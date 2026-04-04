package main

import (
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	flag "github.com/spf13/pflag"
	"lukechampine.com/blake3"
)

// ---------------------------------------------------------------------------
// FsState: central filesystem state
// ---------------------------------------------------------------------------

// FsState holds all shared state for the helmetfs instance.
type FsState struct {
	backingDir    string
	replicaDir    string
	scrubHour     int
	scrubMinute   int
	replWorkers   int
	noRemoteMkdir bool
	pathState     *PathStateMap
	replLog       *ReplLog
	shutdown      atomic.Bool
	scrubWg       sync.WaitGroup
	replWg        sync.WaitGroup
}

// NewFsState creates and initializes the filesystem state.
func NewFsState(backingDir, replicaDir string, scrubHour, scrubMinute, replWorkers int, noRemoteMkdir bool) *FsState {
	s := &FsState{
		backingDir:    backingDir,
		replicaDir:    replicaDir,
		scrubHour:     scrubHour,
		scrubMinute:   scrubMinute,
		replWorkers:   replWorkers,
		noRemoteMkdir: noRemoteMkdir,
		pathState:     NewPathStateMap(),
	}
	s.replLog = NewReplLog(backingDir, &s.shutdown)
	return s
}

// ---------------------------------------------------------------------------
// CLI entry point
// ---------------------------------------------------------------------------

func main() {
	log.SetFlags(log.Ldate | log.Ltime)
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "mount":
		doMount(os.Args[2:])
	case "unmount":
		doUnmount(os.Args[2:])
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage:\n")
	fmt.Fprintf(os.Stderr, "  helmetfs mount <source-dir> <mountpoint> [flags]\n")
	fmt.Fprintf(os.Stderr, "  helmetfs unmount <mountpoint>\n")
	os.Exit(1)
}

func doMount(args []string) {
	if len(args) < 2 {
		usage()
	}

	sourceDir := args[0]
	mountpoint := args[1]

	f := flag.NewFlagSet("mount", flag.ExitOnError)
	replicaDir := f.String("replica", "", "path to replica directory (required)")
	replWorkers := f.Int("replication-workers", 4, "number of replication workers")
	scrubTime := f.String("scrub-time", "01:00", "daily scrub time in HH:MM format")
	noRemoteMkdir := f.Bool("no-remote-mkdir", false, "do not create directories on the replica")
	f.Parse(args[2:])

	if *replicaDir == "" {
		log.Fatal("--replica is required")
	}

	sourceDir = resolveAbsPath(sourceDir, "source")
	mountpoint = resolveAbsPath(mountpoint, "mountpoint")
	*replicaDir = resolveAbsPath(*replicaDir, "replica")

	scrubHour, scrubMinute := parseScrubTime(*scrubTime)

	state := NewFsState(sourceDir, *replicaDir, scrubHour, scrubMinute, *replWorkers, *noRemoteMkdir)
	os.MkdirAll(filepath.Join(state.backingDir, ".helmetfs"), 0755)

	for range *replWorkers {
		state.replWg.Add(1)
		go replWorkerLoop(state)
	}
	state.scrubWg.Add(1)
	go scrubLoop(state)

	rootNode, err := NewHelmetRoot(sourceDir, state)
	if err != nil {
		log.Fatalf("NewHelmetRoot: %v", err)
	}

	server, err := fs.Mount(mountpoint, rootNode, &fs.Options{
		MountOptions: fuse.MountOptions{
			AllowOther:    false,
			FsName:        "helmetfs",
			Name:          "helmetfs",
			MaxBackground: 10,
		},
		NullPermissions: true,
	})
	if err != nil {
		log.Fatalf("mount failed: %v", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigCh
		state.shutdown.Store(true)
		server.Unmount()
	}()

	log.Printf("helmetfs mounted: source=%s mountpoint=%s replica=%s", sourceDir, mountpoint, *replicaDir)
	server.Wait()

	flushDirtyFiles(state)
	stopWorkers(state)
	log.Println("helmetfs: unmounted")
}

func doUnmount(args []string) {
	if len(args) < 1 {
		usage()
	}
	var cmd *exec.Cmd
	if runtime.GOOS == "darwin" {
		cmd = exec.Command("umount", args[0])
	} else {
		cmd = exec.Command("fusermount3", "-u", args[0])
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		log.Fatalf("unmount failed: %v", err)
	}
}

// resolveAbsPath resolves a path to an absolute, symlink-free path.
func resolveAbsPath(path, label string) string {
	abs, err := filepath.Abs(path)
	if err != nil {
		log.Fatalf("resolve %s path: %v", label, err)
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil {
		log.Fatalf("resolve %s path: %v", label, err)
	}
	return resolved
}

func parseScrubTime(s string) (int, int) {
	parts := strings.SplitN(s, ":", 2)
	if len(parts) != 2 {
		log.Fatalf("invalid scrub time: %s", s)
	}
	h, err := strconv.Atoi(parts[0])
	if err != nil || h < 0 || h > 23 {
		log.Fatalf("invalid scrub hour: %s", parts[0])
	}
	m, err := strconv.Atoi(parts[1])
	if err != nil || m < 0 || m > 59 {
		log.Fatalf("invalid scrub minute: %s", parts[1])
	}
	return h, m
}

// ---------------------------------------------------------------------------
// Checksum-and-enqueue orchestration, shutdown helpers
// ---------------------------------------------------------------------------

// checksumAndEnqueue computes a BLAKE3 checksum and enqueues replication,
// unless the file still has open writers.
func checksumAndEnqueue(state *FsState, relPath string) error {
	if state.pathState.HasWriteRef(relPath) {
		return nil
	}
	return checksumAndEnqueueForced(state, relPath)
}

// checksumAndEnqueueForced computes a BLAKE3 checksum and enqueues replication
// regardless of open writers (used on fsync).
func checksumAndEnqueueForced(state *FsState, relPath string) error {
	gen := state.pathState.GetDirtyGen(relPath)
	backingPath := filepath.Join(state.backingDir, relPath)
	hexDigest, err := computeBlake3(backingPath)
	if err != nil {
		return fmt.Errorf("checksum %s: %w", relPath, err)
	}
	if err := writeSumFile(backingPath+".sum", hexDigest); err != nil {
		return fmt.Errorf("write sum %s: %w", relPath, err)
	}
	state.replLog.Enqueue(ReplPut, relPath)
	state.pathState.ClearDirtyIfGen(relPath, gen)
	return nil
}

func flushDirtyFiles(state *FsState) {
	for _, p := range state.pathState.CollectDirtyPaths() {
		checksumAndEnqueue(state, p)
	}
}

func stopWorkers(state *FsState) {
	state.shutdown.Store(true)
	state.replLog.Broadcast()
	state.replWg.Wait()
	state.scrubWg.Wait()
}

// ---------------------------------------------------------------------------
// BLAKE3 checksum and file utilities
// ---------------------------------------------------------------------------

// computeBlake3 computes the BLAKE3-256 hex digest of a file, holding a
// shared (advisory) file lock during the read.
func computeBlake3(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_SH); err != nil {
		return "", err
	}
	defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN)

	h := blake3.New(32, nil)
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// writeSumFile atomically writes a hex digest to a .sum sidecar file.
func writeSumFile(path, hexDigest string) error {
	return os.WriteFile(path, []byte(hexDigest+"\n"), 0644)
}

// readSumFile reads the hex digest from a .sum sidecar file.
func readSumFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

// fsyncDir fsyncs a directory to ensure metadata durability (e.g. after rename).
func fsyncDir(dirPath string) {
	f, err := os.Open(dirPath)
	if err != nil {
		return
	}
	defer f.Close()
	f.Sync()
}

// ensureParentDir creates all parent directories for the given path.
func ensureParentDir(path string) error {
	return os.MkdirAll(filepath.Dir(path), 0755)
}

// removeEmptyParentDirs removes empty ancestor directories up to (but not
// including) stopAt.
func removeEmptyParentDirs(path, stopAt string) {
	dir := filepath.Dir(path)
	for len(dir) > len(stopAt) {
		if err := os.Remove(dir); err != nil {
			return
		}
		dir = filepath.Dir(dir)
	}
}

// copyFileWithSync atomically copies src to dst by writing to a temporary
// file, fsyncing, and renaming into place.
func copyFileWithSync(src, dst string) error {
	sf, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sf.Close()

	tmpPath := dst + ".tmp"
	df, err := os.Create(tmpPath)
	if err != nil {
		return err
	}

	if _, err := io.Copy(df, sf); err != nil {
		df.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := df.Sync(); err != nil {
		df.Close()
		os.Remove(tmpPath)
		return err
	}
	df.Close()

	if err := os.Rename(tmpPath, dst); err != nil {
		os.Remove(tmpPath)
		return err
	}
	fsyncDir(filepath.Dir(dst))
	return nil
}

// ---------------------------------------------------------------------------
// Scrub: nightly integrity checking and self-healing
// ---------------------------------------------------------------------------

func scrubLoop(state *FsState) {
	defer state.scrubWg.Done()
	if shouldScrubImmediately(state) {
		runScrub(state)
	}
	for !state.shutdown.Load() {
		ns := nsUntilNextScrub(state.scrubHour, state.scrubMinute)
		sleepWithShutdown(state, time.Duration(ns))
		if !state.shutdown.Load() {
			runScrub(state)
		}
	}
}

func sleepWithShutdown(state *FsState, d time.Duration) {
	end := time.Now().Add(d)
	for time.Now().Before(end) && !state.shutdown.Load() {
		remaining := time.Until(end)
		if remaining > time.Second {
			remaining = time.Second
		}
		time.Sleep(remaining)
	}
}

func shouldScrubImmediately(state *FsState) bool {
	tsPath := filepath.Join(state.backingDir, ".helmetfs", "scrub.timestamp")
	data, err := os.ReadFile(tsPath)
	if err != nil {
		return true
	}
	ts, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return true
	}
	const day = 86400
	return time.Now().Unix()-ts > day
}

func nsUntilNextScrub(targetHour, targetMinute int) uint64 {
	now := time.Now()
	next := time.Date(now.Year(), now.Month(), now.Day(), targetHour, targetMinute, 0, 0, now.Location())
	if !next.After(now) {
		next = next.Add(24 * time.Hour)
	}
	return uint64(next.Sub(now))
}

func runScrub(state *FsState) {
	log.Println("scrub: starting")
	var corruptions, repairs int
	err := filepath.Walk(state.backingDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if state.shutdown.Load() {
			return filepath.SkipAll
		}
		relPath, _ := filepath.Rel(state.backingDir, path)
		if relPath == "." {
			return nil
		}
		if info.IsDir() {
			if strings.HasPrefix(relPath, ".helmetfs") {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasPrefix(relPath, ".helmetfs") || strings.HasSuffix(relPath, ".sum") {
			return nil
		}
		if state.pathState.HasWriteRef(relPath) {
			return nil
		}
		scrubFile(state, relPath, &corruptions, &repairs)
		return nil
	})
	if err != nil {
		log.Printf("scrub: walk error: %v", err)
	}
	writeScrubTimestamp(state, time.Now().Unix())
	log.Printf("scrub: completed, corruptions=%d repairs=%d", corruptions, repairs)
}

func scrubFile(state *FsState, relPath string, corruptions, repairs *int) {
	backingPath := filepath.Join(state.backingDir, relPath)
	currentHex, err := computeBlake3(backingPath)
	if err != nil {
		log.Printf("scrub: checksum error for %s: %v", relPath, err)
		return
	}
	storedHex, err := readSumFile(backingPath + ".sum")
	if err != nil {
		if os.IsNotExist(err) {
			// No stored checksum yet; create one and enqueue replication.
			writeSumFile(backingPath+".sum", currentHex)
			state.replLog.Enqueue(ReplPut, relPath)
			return
		}
		log.Printf("scrub: read sum error for %s: %v", relPath, err)
		return
	}

	if currentHex == storedHex {
		return // Integrity check passed.
	}

	*corruptions++
	log.Printf("scrub: corruption detected in %s", relPath)

	hasPending := state.replLog.HasPendingPut(relPath)
	replicaPath := filepath.Join(state.replicaDir, "files", relPath)

	replicaHex, err := readSumFile(replicaPath + ".sum")
	if err != nil {
		log.Printf("scrub: cannot read replica sum for %s: %v", relPath, err)
		return
	}
	replicaComputed, err := computeBlake3(replicaPath)
	if err != nil {
		log.Printf("scrub: cannot compute replica hash for %s: %v", relPath, err)
		return
	}
	if replicaComputed != replicaHex {
		log.Printf("scrub: replica also corrupt for %s", relPath)
		return
	}
	if hasPending {
		log.Printf("scrub: replica is stale for %s, skipping repair", relPath)
		return
	}
	if state.pathState.HasWriteRef(relPath) || state.pathState.IsDirty(relPath) {
		return
	}
	if err := copyFileWithSync(replicaPath, backingPath); err != nil {
		log.Printf("scrub: repair failed for %s: %v", relPath, err)
		return
	}
	writeSumFile(backingPath+".sum", replicaComputed)
	*repairs++
	log.Printf("scrub: repaired %s", relPath)
}

func writeScrubTimestamp(state *FsState, ts int64) {
	tsPath := filepath.Join(state.backingDir, ".helmetfs", "scrub.timestamp")
	os.WriteFile(tsPath, []byte(fmt.Sprintf("%d\n", ts)), 0644)
}
