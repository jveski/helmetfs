package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type ReplOp int

const (
	ReplPut    ReplOp = iota
	ReplDelete
)

type ReplEntry struct {
	id        uint64
	op        ReplOp
	path      string
	completed bool
	inFlight  bool
}

type ReplLog struct {
	mu               sync.Mutex
	cond             *sync.Cond
	entries          []*ReplEntry
	nextID           uint64
	completedCount   int
	lastTruncateTime time.Time
	backingDir       string
	shutdown         *AtomicBool
}

func NewReplLog(backingDir string, shutdown *AtomicBool) *ReplLog {
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

func (rl *ReplLog) loadFromDisk() {
	f, err := os.Open(rl.logPath())
	if err != nil {
		return
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, 16*1024*1024))
	if err != nil {
		return
	}
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
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
	opStr := line[:idx]
	path := line[idx+1:]
	var op ReplOp
	switch opStr {
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
		path: path,
	})
	rl.nextID++
}

func (rl *ReplLog) Enqueue(op ReplOp, relPath string) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	entry := &ReplEntry{
		id:   rl.nextID,
		op:   op,
		path: relPath,
	}
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
	opStr := "put"
	if entry.op == ReplDelete {
		opStr = "delete"
	}
	fmt.Fprintf(f, "%s %s\n", opStr, entry.path)
	f.Sync()
}

func (rl *ReplLog) DequeueNext() *ReplEntry {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	for !rl.shutdown.Load() {
		for i := 0; i < len(rl.entries); i++ {
			e := rl.entries[i]
			if e.completed || e.inFlight {
				continue
			}
			if e.op == ReplPut {
				coalesced := false
				for j := i + 1; j < len(rl.entries); j++ {
					later := rl.entries[j]
					if !later.completed && later.op == ReplPut && later.path == e.path {
						e.completed = true
						rl.completedCount++
						coalesced = true
						break
					}
				}
				if coalesced {
					continue
				}
			}
			e.inFlight = true
			return e
		}
		rl.cond.Wait()
	}
	return nil
}

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

func (rl *ReplLog) MarkCompletedByPath(relPath string) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	for _, e := range rl.entries {
		if !e.completed && e.path == relPath {
			e.completed = true
			e.inFlight = false
			rl.completedCount++
		}
	}
}

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

func (rl *ReplLog) PendingCountLocked() uint64 {
	var count uint64
	for _, e := range rl.entries {
		if !e.completed {
			count++
		}
	}
	return count
}

func (rl *ReplLog) maybeTruncate() {
	if len(rl.entries) == 0 || rl.completedCount == 0 {
		return
	}
	shouldTruncate := rl.completedCount*2 > len(rl.entries) ||
		time.Since(rl.lastTruncateTime) >= 60*time.Second
	if !shouldTruncate {
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
		opStr := "put"
		if e.op == ReplDelete {
			opStr = "delete"
		}
		fmt.Fprintf(f, "%s %s\n", opStr, e.path)
	}
	f.Sync()
	f.Close()
	os.Rename(tmpPath, rl.logPath())
	fsyncDir(filepath.Dir(rl.logPath()))
}

func (rl *ReplLog) Broadcast() {
	rl.cond.Broadcast()
}
