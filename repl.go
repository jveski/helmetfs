package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// ---------------------------------------------------------------------------
// PathStateMap: per-file dirty generation and write reference tracking
// ---------------------------------------------------------------------------

// PathInfo tracks the dirty/clean generation and number of open writers for a file.
type PathInfo struct {
	dirtyGen    uint64
	cleanGen    uint64
	writeRefcnt uint32
}

// PathStateMap is a thread-safe map from relative path to per-file state.
type PathStateMap struct {
	mu sync.RWMutex
	m  map[string]*PathInfo
}

// NewPathStateMap creates an empty PathStateMap.
func NewPathStateMap() *PathStateMap {
	return &PathStateMap{m: make(map[string]*PathInfo)}
}

func (ps *PathStateMap) getOrCreate(path string) *PathInfo {
	info, ok := ps.m[path]
	if !ok {
		info = &PathInfo{}
		ps.m[path] = info
	}
	return info
}

// SetDirty increments the dirty generation for the given path.
func (ps *PathStateMap) SetDirty(path string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.getOrCreate(path).dirtyGen++
}

// IsDirty reports whether the file has uncommitted changes.
func (ps *PathStateMap) IsDirty(path string) bool {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	info, ok := ps.m[path]
	return ok && info.dirtyGen > info.cleanGen
}

// ClearDirtyIfGen atomically clears the dirty state only if the current
// dirty generation matches gen, preventing clears from stale snapshots.
func (ps *PathStateMap) ClearDirtyIfGen(path string, gen uint64) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if info, ok := ps.m[path]; ok && info.dirtyGen == gen {
		info.cleanGen = gen
	}
}

// GetDirtyGen returns the current dirty generation for a path.
func (ps *PathStateMap) GetDirtyGen(path string) uint64 {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	if info, ok := ps.m[path]; ok {
		return info.dirtyGen
	}
	return 0
}

// IncWriteRef increments the count of open writers for a path.
func (ps *PathStateMap) IncWriteRef(path string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.getOrCreate(path).writeRefcnt++
}

// DecWriteRef decrements the count of open writers for a path.
func (ps *PathStateMap) DecWriteRef(path string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if info, ok := ps.m[path]; ok && info.writeRefcnt > 0 {
		info.writeRefcnt--
	}
}

// HasWriteRef reports whether the file has any open writers.
func (ps *PathStateMap) HasWriteRef(path string) bool {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	info, ok := ps.m[path]
	return ok && info.writeRefcnt > 0
}

// Remove deletes all tracking state for a path.
func (ps *PathStateMap) Remove(path string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	delete(ps.m, path)
}

// CollectDirtyPaths returns all paths that have uncommitted changes.
func (ps *PathStateMap) CollectDirtyPaths() []string {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	var paths []string
	for p, info := range ps.m {
		if info.dirtyGen > info.cleanGen {
			paths = append(paths, p)
		}
	}
	return paths
}

// ---------------------------------------------------------------------------
// ReplLog: persistent, append-only replication log with put coalescing
// ---------------------------------------------------------------------------

// ReplOp identifies a replication operation type.
type ReplOp int

const (
	ReplPut    ReplOp = iota // Copy file to replica.
	ReplDelete               // Remove file from replica.
)

func (op ReplOp) String() string {
	if op == ReplDelete {
		return "delete"
	}
	return "put"
}

// ReplEntry is a single entry in the replication log.
type ReplEntry struct {
	id        uint64
	op        ReplOp
	path      string
	completed bool
	inFlight  bool
}

// ReplLog is a persistent, mutex-protected replication log that supports
// append, coalescing of consecutive puts, and periodic truncation.
type ReplLog struct {
	mu               sync.Mutex
	cond             *sync.Cond
	entries          []*ReplEntry
	nextID           uint64
	completedCount   int
	lastTruncateTime time.Time
	backingDir       string
	shutdown         *atomic.Bool
}

// NewReplLog creates a ReplLog, loading any persisted entries from disk.
func NewReplLog(backingDir string, shutdown *atomic.Bool) *ReplLog {
	rl := &ReplLog{
		backingDir:       backingDir,
		shutdown:         shutdown,
		lastTruncateTime: time.Now(),
	}
	rl.cond = sync.NewCond(&rl.mu)
	rl.loadFromDisk()
	return rl
}

func (rl *ReplLog) logPath() string {
	return filepath.Join(rl.backingDir, ".helmetfs", "repl.log")
}

const maxLogSize = 16 * 1024 * 1024 // 16 MiB

func (rl *ReplLog) loadFromDisk() {
	f, err := os.Open(rl.logPath())
	if err != nil {
		return
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, maxLogSize))
	if err != nil {
		return
	}
	for _, line := range strings.Split(string(data), "\n") {
		if line == "" {
			continue
		}
		rl.parseLine(line)
	}
}

func (rl *ReplLog) parseLine(line string) {
	idx := strings.IndexByte(line, ' ')
	if idx < 0 {
		return
	}
	var op ReplOp
	switch line[:idx] {
	case "put":
		op = ReplPut
	case "delete":
		op = ReplDelete
	default:
		return
	}
	rl.entries = append(rl.entries, &ReplEntry{
		id:   rl.nextID,
		op:   op,
		path: line[idx+1:],
	})
	rl.nextID++
}

// Enqueue appends a new replication entry, persists it, and wakes a worker.
func (rl *ReplLog) Enqueue(op ReplOp, relPath string) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	entry := &ReplEntry{id: rl.nextID, op: op, path: relPath}
	rl.nextID++
	rl.entries = append(rl.entries, entry)
	rl.appendToDisk(entry)
	rl.cond.Signal()
}

func (rl *ReplLog) appendToDisk(entry *ReplEntry) {
	f, err := os.OpenFile(rl.logPath(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer f.Close()
	fmt.Fprintf(f, "%s %s\n", entry.op, entry.path)
	f.Sync()
}

// DequeueNext blocks until work is available, coalescing duplicate puts.
// Returns nil when shutdown is signaled.
func (rl *ReplLog) DequeueNext() *ReplEntry {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	for !rl.shutdown.Load() {
		for i, e := range rl.entries {
			if e.completed || e.inFlight {
				continue
			}
			if e.op == ReplPut && rl.hasLaterPut(i) {
				e.completed = true
				rl.completedCount++
				continue
			}
			e.inFlight = true
			return e
		}
		rl.cond.Wait()
	}
	return nil
}

// hasLaterPut reports whether a later, non-completed put exists for the same path.
// Caller must hold rl.mu.
func (rl *ReplLog) hasLaterPut(idx int) bool {
	path := rl.entries[idx].path
	for j := idx + 1; j < len(rl.entries); j++ {
		e := rl.entries[j]
		if !e.completed && e.op == ReplPut && e.path == path {
			return true
		}
	}
	return false
}

// MarkCompleted marks an entry as done and may trigger log truncation.
func (rl *ReplLog) MarkCompleted(id uint64) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	for _, e := range rl.entries {
		if e.id == id {
			e.completed = true
			e.inFlight = false
			rl.completedCount++
			break
		}
	}
	rl.maybeTruncate()
}

// HasPendingPut reports whether any incomplete put exists for the path.
func (rl *ReplLog) HasPendingPut(relPath string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	for _, e := range rl.entries {
		if !e.completed && e.op == ReplPut && e.path == relPath {
			return true
		}
	}
	return false
}

// Broadcast wakes all goroutines waiting on the log condition variable.
func (rl *ReplLog) Broadcast() { rl.cond.Broadcast() }

func (rl *ReplLog) maybeTruncate() {
	if len(rl.entries) == 0 || rl.completedCount == 0 {
		return
	}
	halfCompleted := rl.completedCount*2 > len(rl.entries)
	timedOut := time.Since(rl.lastTruncateTime) >= 60*time.Second
	if !halfCompleted && !timedOut {
		return
	}
	newEntries := make([]*ReplEntry, 0, len(rl.entries)-rl.completedCount)
	for _, e := range rl.entries {
		if !e.completed {
			newEntries = append(newEntries, e)
		}
	}
	rl.entries = newEntries
	rl.completedCount = 0
	rl.lastTruncateTime = time.Now()
	rl.rewriteLogAtomic()
}

func (rl *ReplLog) rewriteLogAtomic() {
	tmpPath := rl.logPath() + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return
	}
	for _, e := range rl.entries {
		fmt.Fprintf(f, "%s %s\n", e.op, e.path)
	}
	f.Sync()
	f.Close()
	os.Rename(tmpPath, rl.logPath())
	fsyncDir(filepath.Dir(rl.logPath()))
}

// ---------------------------------------------------------------------------
// Replication workers
// ---------------------------------------------------------------------------

func replWorkerLoop(state *FsState) {
	defer state.replWg.Done()
	for !state.shutdown.Load() {
		work := state.replLog.DequeueNext()
		if work == nil {
			return
		}
		backoff := time.Second
		const maxBackoff = 300 * time.Second
		for !state.shutdown.Load() {
			var err error
			switch work.op {
			case ReplPut:
				err = replicatePut(state, work.path)
			case ReplDelete:
				err = replicateDelete(state, work.path)
			}
			if err == nil {
				state.replLog.MarkCompleted(work.id)
				break
			}
			log.Printf("replication error for %s: %v", work.path, err)
			time.Sleep(backoff)
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

func replicatePut(state *FsState, relPath string) error {
	backingPath := filepath.Join(state.backingDir, relPath)
	replicaPath := filepath.Join(state.replicaDir, "files", relPath)

	var st syscall.Stat_t
	if err := syscall.Lstat(backingPath, &st); err != nil {
		if errors.Is(err, syscall.ENOENT) {
			return nil // File removed before replication; nothing to do.
		}
		return err
	}

	// Handle symlinks: recreate in replica.
	if st.Mode&syscall.S_IFMT == syscall.S_IFLNK {
		if err := ensureParentDir(replicaPath); err != nil {
			return err
		}
		os.Remove(replicaPath)
		target, err := os.Readlink(backingPath)
		if err != nil {
			return err
		}
		return os.Symlink(target, replicaPath)
	}

	// Verify integrity before copying: stored checksum must match computed.
	storedHex, storedErr := readSumFile(backingPath + ".sum")
	computedHex, computedErr := computeBlake3(backingPath)
	if storedErr == nil && computedErr == nil && storedHex != computedHex {
		log.Printf("integrity mismatch for %s, skipping replication", relPath)
		return nil
	}

	if err := ensureParentDir(replicaPath); err != nil {
		return err
	}
	if err := copyFileWithSync(backingPath, replicaPath); err != nil {
		return err
	}

	// Copy the sidecar checksum file as well (best-effort).
	copyFileWithSync(backingPath+".sum", replicaPath+".sum")

	// Preserve mode and ownership (best-effort).
	if err := syscall.Stat(backingPath, &st); err == nil {
		os.Chmod(replicaPath, os.FileMode(st.Mode&0o7777))
		os.Chown(replicaPath, int(st.Uid), int(st.Gid))
	}
	return nil
}

func replicateDelete(state *FsState, relPath string) error {
	replicaPath := filepath.Join(state.replicaDir, "files", relPath)
	os.Remove(replicaPath)
	os.Remove(replicaPath + ".sum")
	if !state.noRemoteMkdir {
		removeEmptyParentDirs(replicaPath, filepath.Join(state.replicaDir, "files"))
	}
	return nil
}
