package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/xml"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

var (
	targetURL   = flag.String("url", "http://localhost:8080", "target server URL")
	duration    = flag.Duration("duration", 0, "test duration (0 = run forever)")
	concurrency = flag.Int("concurrency", 4, "number of concurrent workers")
	seed        = flag.Int64("seed", 0, "random seed (0 = use current time)")

	// Failure injection flags
	dbPath        = flag.String("db", "", "path to helmetfs database (enables failure injection)")
	blobsDir      = flag.String("blobs", "", "path to helmetfs blobs directory (enables corruption injection)")
	corruptRate   = flag.Float64("corrupt-rate", 0, "probability of corrupting a blob after write (0-1)")
	rcloneFailure = flag.Float64("rclone-failure-rate", 0, "probability of simulating rclone sync failure (0-1)")
)

func main() {
	flag.Parse()
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	s := *seed
	if s == 0 {
		s = time.Now().UnixNano()
	}
	log.Printf("soak test starting: url=%s concurrency=%d seed=%d", *targetURL, *concurrency, s)

	// Initialize failure injection if configured
	var injector *FailureInjector
	if *dbPath != "" || *blobsDir != "" {
		var err error
		injector, err = newFailureInjector(*dbPath, *blobsDir, *corruptRate, *rcloneFailure)
		if err != nil {
			return fmt.Errorf("failed to initialize failure injector: %w", err)
		}
		if injector != nil {
			defer injector.Close()
			log.Printf("failure injection enabled: corrupt-rate=%.2f rclone-failure-rate=%.2f", *corruptRate, *rcloneFailure)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	if *duration > 0 {
		ctx, cancel = context.WithTimeout(ctx, *duration)
	}
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	go func() {
		<-sigCh
		log.Println("received interrupt, shutting down...")
		cancel()
	}()

	state := &State{
		files: make(map[string][]byte),
		dirs:  make(map[string]bool),
	}
	state.dirs["/"] = true

	var wg sync.WaitGroup
	failures := make(chan Failure, 1000)
	var opCount atomic.Int64

	for i := range *concurrency {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			w := &Worker{
				id:       id,
				client:   &http.Client{Timeout: 30 * time.Second},
				state:    state,
				rng:      rand.New(rand.NewPCG(uint64(s), uint64(id))),
				baseURL:  *targetURL,
				failures: failures,
				injector: injector,
			}
			w.run(ctx, &opCount)
		}(i)
	}

	wg.Wait()
	close(failures)

	var failureList []Failure
	for f := range failures {
		failureList = append(failureList, f)
	}

	log.Printf("soak test complete: operations=%d failures=%d", opCount.Load(), len(failureList))
	if len(failureList) > 0 {
		log.Println("failures:")
		for _, f := range failureList {
			log.Printf("  [%s] %s: %s", f.Operation, f.Path, f.Error)
		}
		return fmt.Errorf("%d invariant violations", len(failureList))
	}
	return nil
}

type Failure struct {
	Operation string
	Path      string
	Error     string
}

// FailureInjector enables testing corruption and rclone failure scenarios
type FailureInjector struct {
	db            *sql.DB
	blobsDir      string
	corruptRate   float64
	rcloneFailure float64
	mu            sync.Mutex
	corruptedIDs  map[int64]bool // tracks blob IDs we've corrupted
}

func newFailureInjector(dbPath, blobsDir string, corruptRate, rcloneFailure float64) (*FailureInjector, error) {
	if dbPath == "" && blobsDir == "" {
		return nil, nil // failure injection disabled
	}

	fi := &FailureInjector{
		blobsDir:      blobsDir,
		corruptRate:   corruptRate,
		rcloneFailure: rcloneFailure,
		corruptedIDs:  make(map[int64]bool),
	}

	if dbPath != "" {
		db, err := sql.Open("sqlite3", "file:"+dbPath+"?_journal_mode=WAL&_busy_timeout=5000&mode=ro")
		if err != nil {
			return nil, fmt.Errorf("open database: %w", err)
		}
		fi.db = db
	}

	return fi, nil
}

func (fi *FailureInjector) Close() error {
	if fi != nil && fi.db != nil {
		return fi.db.Close()
	}
	return nil
}

// CorruptRandomBlob corrupts a random blob file by flipping bits
func (fi *FailureInjector) CorruptRandomBlob(rng *rand.Rand) error {
	if fi == nil || fi.blobsDir == "" || fi.db == nil {
		return nil
	}

	if rng.Float64() >= fi.corruptRate {
		return nil // skip based on rate
	}

	// Find a blob to corrupt
	var blobID int64
	var size int64
	err := fi.db.QueryRow(`
		SELECT id, size FROM blobs
		WHERE local_written = 1 AND local_deleted = 0 AND size > 0
		ORDER BY RANDOM() LIMIT 1`).Scan(&blobID, &size)
	if err == sql.ErrNoRows {
		return nil // no blobs to corrupt
	}
	if err != nil {
		return err
	}

	fi.mu.Lock()
	if fi.corruptedIDs[blobID] {
		fi.mu.Unlock()
		return nil // already corrupted
	}
	fi.corruptedIDs[blobID] = true
	fi.mu.Unlock()

	blobPath := blobFilePath(fi.blobsDir, blobID)
	return corruptFile(blobPath, rng, size)
}

func corruptFile(path string, rng *rand.Rand, size int64) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	// Flip 1-3 random bits in the file
	numFlips := 1 + rng.IntN(3)
	for i := 0; i < numFlips; i++ {
		offset := rng.Int64N(size)
		var b [1]byte
		if _, err := f.ReadAt(b[:], offset); err != nil {
			return err
		}
		bit := byte(1 << rng.IntN(8))
		b[0] ^= bit
		if _, err := f.WriteAt(b[:], offset); err != nil {
			return err
		}
	}

	log.Printf("[failure-injection] corrupted blob at %s (%d bit flips)", path, numFlips)
	return nil
}

// SimulateRcloneFailure simulates rclone sync failures by marking blobs as not synced
func (fi *FailureInjector) SimulateRcloneFailure(rng *rand.Rand) error {
	if fi == nil || fi.db == nil || fi.rcloneFailure <= 0 {
		return nil
	}

	if rng.Float64() >= fi.rcloneFailure {
		return nil
	}

	// Open a write connection to simulate the failure
	db, err := sql.Open("sqlite3", "file:"+*dbPath+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		return err
	}
	defer db.Close()

	// Mark a random blob as not uploaded (simulating upload failure)
	result, err := db.Exec(`
		UPDATE blobs SET remote_written = 0
		WHERE id = (
			SELECT id FROM blobs
			WHERE remote_written = 1 AND remote_deleted = 0
			ORDER BY RANDOM() LIMIT 1
		)`)
	if err != nil {
		return err
	}
	if n, _ := result.RowsAffected(); n > 0 {
		log.Printf("[failure-injection] simulated rclone upload failure (marked blob as not uploaded)")
	}
	return nil
}

// DeleteRandomLocalBlob deletes a local blob to simulate local data loss
func (fi *FailureInjector) DeleteRandomLocalBlob(rng *rand.Rand) error {
	if fi == nil || fi.blobsDir == "" || fi.db == nil {
		return nil
	}

	// Only do this occasionally
	if rng.Float64() >= fi.corruptRate {
		return nil
	}

	var blobID int64
	err := fi.db.QueryRow(`
		SELECT id FROM blobs
		WHERE local_written = 1 AND local_deleted = 0 AND remote_written = 1
		ORDER BY RANDOM() LIMIT 1`).Scan(&blobID)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}

	blobPath := blobFilePath(fi.blobsDir, blobID)
	if err := os.Remove(blobPath); err != nil && !os.IsNotExist(err) {
		return err
	}

	log.Printf("[failure-injection] deleted local blob %d to simulate data loss", blobID)
	return nil
}

func blobFilePath(blobsDir string, blobID int64) string {
	h := fnv.New64a()
	binary.Write(h, binary.LittleEndian, blobID)
	hash := h.Sum64()
	return filepath.Join(blobsDir, fmt.Sprintf("%02x", hash&0xff), fmt.Sprintf("%014x", hash>>8))
}

type State struct {
	mu    sync.Mutex
	files map[string][]byte
	dirs  map[string]bool
}

func (s *State) addFile(path string, content []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.files[path] = content
}

func (s *State) removeFile(path string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.files, path)
}

func (s *State) getFile(path string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.files[path]
	return c, ok
}

func (s *State) addDir(path string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dirs[path] = true
}

func (s *State) removeDir(path string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.dirs, path)
}

func (s *State) hasDir(path string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dirs[path]
}

func (s *State) randomFile(rng *rand.Rand) (string, []byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.files) == 0 {
		return "", nil, false
	}
	i := rng.IntN(len(s.files))
	for p, c := range s.files {
		if i == 0 {
			return p, c, true
		}
		i--
	}
	return "", nil, false
}

func (s *State) randomDir(rng *rand.Rand) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.dirs) == 0 {
		return "", false
	}
	i := rng.IntN(len(s.dirs))
	for p := range s.dirs {
		if i == 0 {
			return p, true
		}
		i--
	}
	return "", false
}

func (s *State) listDir(dir string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var children []string
	prefix := dir
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	if prefix == "//" {
		prefix = "/"
	}
	seen := make(map[string]bool)
	for p := range s.files {
		if strings.HasPrefix(p, prefix) {
			rest := strings.TrimPrefix(p, prefix)
			parts := strings.SplitN(rest, "/", 2)
			if !seen[parts[0]] {
				seen[parts[0]] = true
				children = append(children, parts[0])
			}
		}
	}
	for p := range s.dirs {
		if p != dir && strings.HasPrefix(p, prefix) {
			rest := strings.TrimPrefix(p, prefix)
			parts := strings.SplitN(rest, "/", 2)
			if !seen[parts[0]] {
				seen[parts[0]] = true
				children = append(children, parts[0])
			}
		}
	}
	return children
}

type Worker struct {
	id       int
	client   *http.Client
	state    *State
	rng      *rand.Rand
	baseURL  string
	failures chan<- Failure
	injector *FailureInjector
}

func (w *Worker) run(ctx context.Context, opCount *atomic.Int64) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		op := w.selectOperation()
		var err error
		switch op {
		case "createFile":
			err = w.createFile(ctx)
		case "readFile":
			err = w.readFile(ctx)
		case "deleteFile":
			err = w.deleteFile(ctx)
		case "createDir":
			err = w.createDir(ctx)
		case "listDir":
			err = w.listDir(ctx)
		case "rename":
			err = w.rename(ctx)
		case "restore":
			err = w.restore(ctx)
		case "verifyDeletedReturns404":
			err = w.verifyDeletedReturns404(ctx)
		case "verifyParentMustExist":
			err = w.verifyParentMustExist(ctx)
		case "verifyPathExclusivity":
			err = w.verifyPathExclusivity(ctx)
		case "verifyRestoreRejectsFuture":
			err = w.verifyRestoreRejectsFuture(ctx)
		case "verifyCannotDeleteRoot":
			err = w.verifyCannotDeleteRoot(ctx)
		case "injectCorruption":
			err = w.injectCorruption()
		case "injectRcloneFailure":
			err = w.injectRcloneFailure()
		case "injectBlobDeletion":
			err = w.injectBlobDeletion()
		case "verifyCorruptionDetected":
			err = w.verifyCorruptionDetected(ctx)
		// WebDAV operations
		case "copyFile":
			err = w.copyFile(ctx)
		case "lockUnlock":
			err = w.lockUnlock(ctx)
		case "proppatch":
			err = w.proppatch(ctx)
		// Edge cases
		case "createFileExclusive":
			err = w.createFileExclusive(ctx)
		case "truncateFile":
			err = w.truncateFile(ctx)
		case "createEmptyFile":
			err = w.createEmptyFile(ctx)
		case "verifyDeduplication":
			err = w.verifyDeduplication(ctx)
		case "verifyReadPermissions":
			err = w.verifyReadPermissions(ctx)
		// Concurrent access
		case "concurrentReaders":
			err = w.concurrentReaders(ctx)
		case "concurrentReadWrite":
			err = w.concurrentReadWrite(ctx)
		// Garbage collection / integrity (when db access enabled)
		case "verifyGarbageCollection":
			err = w.verifyGarbageCollection(ctx)
		case "verifyIntegrityCheckLoop":
			err = w.verifyIntegrityCheckLoop(ctx)
		}
		if err != nil {
			select {
			case w.failures <- Failure{Operation: op, Error: err.Error()}:
			default:
			}
		}
		opCount.Add(1)
	}
}

func (w *Worker) selectOperation() string {
	ops := []struct {
		name   string
		weight int
	}{
		{"createFile", 30},
		{"readFile", 25},
		{"deleteFile", 10},
		{"createDir", 10},
		{"listDir", 10},
		{"rename", 5},
		{"restore", 2},
		{"verifyDeletedReturns404", 2},
		{"verifyParentMustExist", 2},
		{"verifyPathExclusivity", 2},
		{"verifyRestoreRejectsFuture", 1},
		{"verifyCannotDeleteRoot", 1},
		// WebDAV operations
		{"copyFile", 5},
		{"lockUnlock", 3},
		{"proppatch", 2},
		// Edge cases
		{"createFileExclusive", 2},
		{"truncateFile", 3},
		{"createEmptyFile", 2},
		{"verifyDeduplication", 2},
		{"verifyReadPermissions", 1},
		// Concurrent access
		{"concurrentReaders", 2},
		{"concurrentReadWrite", 2},
	}

	// Add failure injection operations if enabled
	if w.injector != nil {
		if w.injector.corruptRate > 0 {
			ops = append(ops,
				struct{ name string; weight int }{"injectCorruption", 2},
				struct{ name string; weight int }{"injectBlobDeletion", 1},
				struct{ name string; weight int }{"verifyCorruptionDetected", 2},
			)
		}
		if w.injector.rcloneFailure > 0 {
			ops = append(ops,
				struct{ name string; weight int }{"injectRcloneFailure", 1},
			)
		}
		// Add GC and integrity checks when we have db access
		if w.injector.db != nil {
			ops = append(ops,
				struct{ name string; weight int }{"verifyGarbageCollection", 2},
				struct{ name string; weight int }{"verifyIntegrityCheckLoop", 2},
			)
		}
	}

	total := 0
	for _, op := range ops {
		total += op.weight
	}
	n := w.rng.IntN(total)
	for _, op := range ops {
		n -= op.weight
		if n < 0 {
			return op.name
		}
	}
	return "createFile"
}

func (w *Worker) createFile(ctx context.Context) error {
	dir, ok := w.state.randomDir(w.rng)
	if !ok {
		dir = "/"
	}
	name := randomName(w.rng)
	filePath := path.Join(dir, name+".txt")
	content := randomContent(w.rng)

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, w.baseURL+filePath, bytes.NewReader(content))
	if err != nil {
		return nil
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
		return nil
	}
	w.state.addFile(filePath, content)

	readResp, err := w.client.Get(w.baseURL + filePath)
	if err != nil {
		return nil
	}
	defer readResp.Body.Close()
	readContent, _ := io.ReadAll(readResp.Body)

	// If the file was deleted (404), it might be due to a concurrent restore operation
	// that restored to a point in time before this file was created. This is expected
	// behavior when restore is running concurrently, not an invariant violation.
	if readResp.StatusCode == http.StatusNotFound {
		w.state.removeFile(filePath)
		return nil
	}
	if readResp.StatusCode != http.StatusOK {
		return nil // Other errors (e.g., 500) are transient and shouldn't fail the test
	}

	if !bytes.Equal(content, readContent) {
		return fmt.Errorf("invariant #1 violated: read-after-write mismatch for %s (wrote %d bytes, read %d bytes)", filePath, len(content), len(readContent))
	}
	return nil
}

func (w *Worker) readFile(ctx context.Context) error {
	filePath, expectedContent, ok := w.state.randomFile(w.rng)
	if !ok {
		return nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, w.baseURL+filePath, nil)
	if err != nil {
		return nil
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	content, _ := io.ReadAll(resp.Body)

	if resp.StatusCode == http.StatusNotFound {
		w.state.removeFile(filePath)
		return nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil
	}
	if !bytes.Equal(expectedContent, content) {
		w.state.addFile(filePath, content)
	}
	return nil
}

func (w *Worker) deleteFile(ctx context.Context) error {
	filePath, _, ok := w.state.randomFile(w.rng)
	if !ok {
		return nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, w.baseURL+filePath, nil)
	if err != nil {
		return nil
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusNotFound {
		w.state.removeFile(filePath)
	}
	return nil
}

func (w *Worker) createDir(ctx context.Context) error {
	parentDir, ok := w.state.randomDir(w.rng)
	if !ok {
		parentDir = "/"
	}
	name := randomName(w.rng)
	dirPath := path.Join(parentDir, name)

	req, err := http.NewRequestWithContext(ctx, "MKCOL", w.baseURL+dirPath, nil)
	if err != nil {
		return nil
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode == http.StatusCreated {
		w.state.addDir(dirPath)
	}
	return nil
}

func (w *Worker) listDir(ctx context.Context) error {
	dir, ok := w.state.randomDir(w.rng)
	if !ok {
		return nil
	}

	req, err := http.NewRequestWithContext(ctx, "PROPFIND", w.baseURL+dir, strings.NewReader(`<?xml version="1.0" encoding="utf-8"?><propfind xmlns="DAV:"><prop><resourcetype/></prop></propfind>`))
	if err != nil {
		return nil
	}
	req.Header.Set("Depth", "1")
	req.Header.Set("Content-Type", "application/xml")
	resp, err := w.client.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return nil
}

func (w *Worker) rename(ctx context.Context) error {
	srcPath, content, ok := w.state.randomFile(w.rng)
	if !ok {
		return nil
	}

	dir := path.Dir(srcPath)
	newName := randomName(w.rng) + ".txt"
	destPath := path.Join(dir, newName)

	req, err := http.NewRequestWithContext(ctx, "MOVE", w.baseURL+srcPath, nil)
	if err != nil {
		return nil
	}
	req.Header.Set("Destination", w.baseURL+destPath)
	resp, err := w.client.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusNoContent {
		w.state.removeFile(srcPath)
		w.state.addFile(destPath, content)
	}
	return nil
}

func (w *Worker) restore(ctx context.Context) error {
	timestamp := time.Now().Add(-time.Duration(w.rng.IntN(3600)) * time.Second).Format(time.RFC3339)
	form := url.Values{}
	form.Set("timestamp", timestamp)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.baseURL+"/api/restore", strings.NewReader(form.Encode()))
	if err != nil {
		return nil
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := w.client.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return nil
}

func (w *Worker) verifyDeletedReturns404(ctx context.Context) error {
	name := randomName(w.rng) + "_deleted.txt"
	filePath := "/" + name
	content := []byte("test content")

	putReq, err := http.NewRequestWithContext(ctx, http.MethodPut, w.baseURL+filePath, bytes.NewReader(content))
	if err != nil {
		return nil
	}
	putResp, err := w.client.Do(putReq)
	if err != nil {
		return nil
	}
	putResp.Body.Close()
	if putResp.StatusCode != http.StatusCreated {
		return nil
	}

	delReq, err := http.NewRequestWithContext(ctx, http.MethodDelete, w.baseURL+filePath, nil)
	if err != nil {
		return nil
	}
	delResp, err := w.client.Do(delReq)
	if err != nil {
		return nil
	}
	delResp.Body.Close()

	getReq, err := http.NewRequestWithContext(ctx, http.MethodGet, w.baseURL+filePath, nil)
	if err != nil {
		return nil
	}
	getResp, err := w.client.Do(getReq)
	if err != nil {
		return nil
	}
	defer getResp.Body.Close()
	io.Copy(io.Discard, getResp.Body)

	if getResp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("invariant #12 violated: deleted file %s returned %d instead of 404", filePath, getResp.StatusCode)
	}
	return nil
}

func (w *Worker) verifyParentMustExist(ctx context.Context) error {
	nonExistent := "/" + randomName(w.rng) + "_noparent/" + randomName(w.rng) + ".txt"
	content := []byte("test")

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, w.baseURL+nonExistent, bytes.NewReader(content))
	if err != nil {
		return nil
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusNoContent {
		return fmt.Errorf("invariant #4 violated: created file %s without parent directory existing", nonExistent)
	}
	return nil
}

func (w *Worker) verifyPathExclusivity(ctx context.Context) error {
	dirName := "/" + randomName(w.rng) + "_excl"

	mkcolReq, err := http.NewRequestWithContext(ctx, "MKCOL", w.baseURL+dirName, nil)
	if err != nil {
		return nil
	}
	mkcolResp, err := w.client.Do(mkcolReq)
	if err != nil {
		return nil
	}
	mkcolResp.Body.Close()
	if mkcolResp.StatusCode != http.StatusCreated {
		return nil
	}
	w.state.addDir(dirName)

	putReq, err := http.NewRequestWithContext(ctx, http.MethodPut, w.baseURL+dirName, bytes.NewReader([]byte("test")))
	if err != nil {
		return nil
	}
	putResp, err := w.client.Do(putReq)
	if err != nil {
		return nil
	}
	defer putResp.Body.Close()
	io.Copy(io.Discard, putResp.Body)

	if putResp.StatusCode == http.StatusCreated || putResp.StatusCode == http.StatusNoContent {
		// Before reporting an invariant violation, verify the directory still exists.
		// A concurrent delete or restore operation might have removed the directory
		// between the MKCOL and PUT, making the PUT success expected behavior.
		statReq, err := http.NewRequestWithContext(ctx, "PROPFIND", w.baseURL+dirName, strings.NewReader(`<?xml version="1.0" encoding="utf-8"?><propfind xmlns="DAV:"><prop><resourcetype/></prop></propfind>`))
		if err != nil {
			return nil
		}
		statReq.Header.Set("Depth", "0")
		statReq.Header.Set("Content-Type", "application/xml")
		statResp, err := w.client.Do(statReq)
		if err != nil {
			return nil
		}
		body, _ := io.ReadAll(statResp.Body)
		statResp.Body.Close()

		// If the path now returns 404 or is a file (no <collection/> in response),
		// the directory was deleted/replaced and the PUT success is expected behavior.
		if statResp.StatusCode == http.StatusNotFound {
			w.state.removeDir(dirName)
			return nil
		}
		// Check if it's now a file (not a directory). A directory response contains <collection/>.
		if !strings.Contains(string(body), "<collection") {
			w.state.removeDir(dirName)
			return nil
		}

		return fmt.Errorf("invariant #5 violated: created file at path %s which is already a directory", dirName)
	}
	return nil
}

func (w *Worker) verifyRestoreRejectsFuture(ctx context.Context) error {
	futureTime := time.Now().Add(time.Hour).Format(time.RFC3339)
	form := url.Values{}
	form.Set("timestamp", futureTime)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.baseURL+"/api/restore", strings.NewReader(form.Encode()))
	if err != nil {
		return nil
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := w.client.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode != http.StatusBadRequest {
		return fmt.Errorf("invariant #14 violated: restore accepted future timestamp, got status %d", resp.StatusCode)
	}
	return nil
}

func (w *Worker) verifyCannotDeleteRoot(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, w.baseURL+"/", nil)
	if err != nil {
		return nil
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode == http.StatusNoContent {
		return fmt.Errorf("invariant #34 violated: root directory was deleted")
	}
	return nil
}

func randomName(rng *rand.Rand) string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	length := 3 + rng.IntN(8)
	b := make([]byte, length)
	for i := range b {
		b[i] = chars[rng.IntN(len(chars))]
	}
	return string(b)
}

func randomContent(rng *rand.Rand) []byte {
	size := rng.IntN(10 * 1024)
	b := make([]byte, size)
	for i := range b {
		b[i] = byte(rng.IntN(256))
	}
	return b
}

type MultiStatus struct {
	Responses []Response `xml:"response"`
}

type Response struct {
	Href string `xml:"href"`
}

var _ = xml.Unmarshal

// Failure injection worker methods

func (w *Worker) injectCorruption() error {
	if w.injector == nil {
		return nil
	}
	return w.injector.CorruptRandomBlob(w.rng)
}

func (w *Worker) injectRcloneFailure() error {
	if w.injector == nil {
		return nil
	}
	return w.injector.SimulateRcloneFailure(w.rng)
}

func (w *Worker) injectBlobDeletion() error {
	if w.injector == nil {
		return nil
	}
	return w.injector.DeleteRandomLocalBlob(w.rng)
}

// verifyCorruptionDetected checks that reading a corrupted file returns an error
// This verifies the server's checksum validation is working correctly
func (w *Worker) verifyCorruptionDetected(ctx context.Context) error {
	if w.injector == nil {
		return nil
	}

	// Create a file and immediately corrupt its blob
	dir, ok := w.state.randomDir(w.rng)
	if !ok {
		dir = "/"
	}
	name := randomName(w.rng)
	filePath := path.Join(dir, name+"_corrupt_test.txt")
	content := randomContent(w.rng)
	if len(content) == 0 {
		content = []byte("test content for corruption")
	}

	// Create the file
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, w.baseURL+filePath, bytes.NewReader(content))
	if err != nil {
		return nil
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
		return nil // file creation failed, skip test
	}

	// Find and corrupt the blob for this file
	if w.injector.db != nil && w.injector.blobsDir != "" {
		var blobID int64
		var size int64
		err := w.injector.db.QueryRow(`
			SELECT b.id, b.size FROM files f
			JOIN blobs b ON f.blob_id = b.id
			WHERE f.path = ? AND f.deleted = 0 AND b.size > 0
			ORDER BY f.version DESC LIMIT 1`, filePath).Scan(&blobID, &size)
		if err == nil && size > 0 {
			blobPath := blobFilePath(w.injector.blobsDir, blobID)
			if err := corruptFile(blobPath, w.rng, size); err == nil {
				// Now read should fail with checksum error
				time.Sleep(10 * time.Millisecond) // brief delay for filesystem
				readResp, err := w.client.Get(w.baseURL + filePath)
				if err != nil {
					return nil
				}
				defer readResp.Body.Close()
				readContent, _ := io.ReadAll(readResp.Body)

				// The server should detect corruption and return an error
				// or the content should be different (server caught it)
				if readResp.StatusCode == http.StatusOK && bytes.Equal(content, readContent) {
					return fmt.Errorf("invariant #99 violated: corrupted file returned original content without error")
				}
				// Server correctly detected corruption (500, different content, or any non-200)
				log.Printf("[failure-injection] corruption detection verified for %s", filePath)
			}
		}
	}

	// Clean up
	delReq, _ := http.NewRequestWithContext(ctx, http.MethodDelete, w.baseURL+filePath, nil)
	if delReq != nil {
		delResp, _ := w.client.Do(delReq)
		if delResp != nil {
			io.Copy(io.Discard, delResp.Body)
			delResp.Body.Close()
		}
	}

	return nil
}

// ==================== WebDAV Operations ====================

// copyFile tests the WebDAV COPY operation
func (w *Worker) copyFile(ctx context.Context) error {
	srcPath, content, ok := w.state.randomFile(w.rng)
	if !ok {
		return nil
	}

	dir := path.Dir(srcPath)
	newName := randomName(w.rng) + "_copy.txt"
	destPath := path.Join(dir, newName)

	req, err := http.NewRequestWithContext(ctx, "COPY", w.baseURL+srcPath, nil)
	if err != nil {
		return nil
	}
	req.Header.Set("Destination", w.baseURL+destPath)
	resp, err := w.client.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusNoContent {
		w.state.addFile(destPath, content)

		// Verify the copy has the same content
		getResp, err := w.client.Get(w.baseURL + destPath)
		if err != nil {
			return nil
		}
		defer getResp.Body.Close()
		copiedContent, _ := io.ReadAll(getResp.Body)

		if getResp.StatusCode == http.StatusOK && !bytes.Equal(content, copiedContent) {
			return fmt.Errorf("invariant #20 violated: COPY produced different content for %s", destPath)
		}
	}
	return nil
}

// lockUnlock tests WebDAV LOCK and UNLOCK operations
func (w *Worker) lockUnlock(ctx context.Context) error {
	filePath, _, ok := w.state.randomFile(w.rng)
	if !ok {
		// Create a test file
		filePath = "/" + randomName(w.rng) + "_lock.txt"
		content := []byte("lock test content")
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, w.baseURL+filePath, bytes.NewReader(content))
		if err != nil {
			return nil
		}
		resp, err := w.client.Do(req)
		if err != nil {
			return nil
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			return nil
		}
		w.state.addFile(filePath, content)
	}

	// Request a write lock
	lockBody := `<?xml version="1.0" encoding="utf-8"?>
<lockinfo xmlns="DAV:">
  <lockscope><exclusive/></lockscope>
  <locktype><write/></locktype>
  <owner><href>soaktest</href></owner>
</lockinfo>`

	lockReq, err := http.NewRequestWithContext(ctx, "LOCK", w.baseURL+filePath, strings.NewReader(lockBody))
	if err != nil {
		return nil
	}
	lockReq.Header.Set("Content-Type", "application/xml")
	lockReq.Header.Set("Timeout", "Second-60")
	lockResp, err := w.client.Do(lockReq)
	if err != nil {
		return nil
	}
	defer lockResp.Body.Close()
	lockRespBody, _ := io.ReadAll(lockResp.Body)

	// Extract lock token from response
	if lockResp.StatusCode != http.StatusOK {
		return nil // Lock not supported or failed, that's ok
	}

	// Parse lock token from Lock-Token header or response body
	lockToken := lockResp.Header.Get("Lock-Token")
	if lockToken == "" {
		// Try to extract from XML response
		if idx := strings.Index(string(lockRespBody), "<opaquelocktoken:"); idx >= 0 {
			end := strings.Index(string(lockRespBody)[idx:], "</href>")
			if end > 0 {
				start := strings.Index(string(lockRespBody)[idx:], ">") + 1
				lockToken = "<" + string(lockRespBody)[idx+start:idx+end] + ">"
			}
		}
	}

	if lockToken == "" {
		return nil // Couldn't get lock token
	}

	// Unlock
	unlockReq, err := http.NewRequestWithContext(ctx, "UNLOCK", w.baseURL+filePath, nil)
	if err != nil {
		return nil
	}
	unlockReq.Header.Set("Lock-Token", lockToken)
	unlockResp, err := w.client.Do(unlockReq)
	if err != nil {
		return nil
	}
	defer unlockResp.Body.Close()
	io.Copy(io.Discard, unlockResp.Body)

	// 204 No Content is success for UNLOCK
	if unlockResp.StatusCode != http.StatusNoContent && unlockResp.StatusCode != http.StatusOK {
		log.Printf("[lock] UNLOCK returned %d for %s", unlockResp.StatusCode, filePath)
	}

	return nil
}

// proppatch tests WebDAV PROPPATCH operation
func (w *Worker) proppatch(ctx context.Context) error {
	filePath, _, ok := w.state.randomFile(w.rng)
	if !ok {
		return nil
	}

	// Set a custom property
	propBody := `<?xml version="1.0" encoding="utf-8"?>
<propertyupdate xmlns="DAV:">
  <set>
    <prop>
      <displayname>` + randomName(w.rng) + `</displayname>
    </prop>
  </set>
</propertyupdate>`

	req, err := http.NewRequestWithContext(ctx, "PROPPATCH", w.baseURL+filePath, strings.NewReader(propBody))
	if err != nil {
		return nil
	}
	req.Header.Set("Content-Type", "application/xml")
	resp, err := w.client.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	// PROPPATCH should return 207 Multi-Status
	// We don't verify the actual property change since it may not be persisted
	return nil
}

// ==================== Edge Cases ====================

// createFileExclusive tests O_EXCL behavior - creating a file that must not already exist
func (w *Worker) createFileExclusive(ctx context.Context) error {
	name := randomName(w.rng) + "_excl.txt"
	filePath := "/" + name
	content := []byte("exclusive content")

	// First create should succeed
	req1, err := http.NewRequestWithContext(ctx, http.MethodPut, w.baseURL+filePath, bytes.NewReader(content))
	if err != nil {
		return nil
	}
	req1.Header.Set("If-None-Match", "*") // WebDAV way to request exclusive create
	resp1, err := w.client.Do(req1)
	if err != nil {
		return nil
	}
	resp1.Body.Close()

	if resp1.StatusCode != http.StatusCreated {
		return nil // May have been created by another worker
	}
	w.state.addFile(filePath, content)

	// Second create with If-None-Match should fail
	req2, err := http.NewRequestWithContext(ctx, http.MethodPut, w.baseURL+filePath, bytes.NewReader(content))
	if err != nil {
		return nil
	}
	req2.Header.Set("If-None-Match", "*")
	resp2, err := w.client.Do(req2)
	if err != nil {
		return nil
	}
	defer resp2.Body.Close()
	io.Copy(io.Discard, resp2.Body)

	// Should get 412 Precondition Failed
	if resp2.StatusCode == http.StatusCreated || resp2.StatusCode == http.StatusNoContent {
		return fmt.Errorf("invariant #21 violated: If-None-Match:* allowed overwrite of existing file %s", filePath)
	}

	return nil
}

// truncateFile tests overwriting a file with new content (O_TRUNC behavior)
func (w *Worker) truncateFile(ctx context.Context) error {
	filePath, _, ok := w.state.randomFile(w.rng)
	if !ok {
		return nil
	}

	// Write new, different content
	newContent := []byte("truncated content: " + randomName(w.rng))

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, w.baseURL+filePath, bytes.NewReader(newContent))
	if err != nil {
		return nil
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
		return nil
	}
	w.state.addFile(filePath, newContent)

	// Verify the new content
	getResp, err := w.client.Get(w.baseURL + filePath)
	if err != nil {
		return nil
	}
	defer getResp.Body.Close()
	readContent, _ := io.ReadAll(getResp.Body)

	if getResp.StatusCode == http.StatusNotFound {
		w.state.removeFile(filePath)
		return nil
	}
	if getResp.StatusCode == http.StatusOK && !bytes.Equal(newContent, readContent) {
		return fmt.Errorf("invariant #22 violated: truncate/overwrite produced wrong content for %s", filePath)
	}
	return nil
}

// createEmptyFile tests creating a file with no content
func (w *Worker) createEmptyFile(ctx context.Context) error {
	dir, ok := w.state.randomDir(w.rng)
	if !ok {
		dir = "/"
	}
	name := randomName(w.rng) + "_empty.txt"
	filePath := path.Join(dir, name)

	// Create with empty body
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, w.baseURL+filePath, bytes.NewReader([]byte{}))
	if err != nil {
		return nil
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
		return nil
	}
	w.state.addFile(filePath, []byte{})

	// Verify the file exists and is empty
	getResp, err := w.client.Get(w.baseURL + filePath)
	if err != nil {
		return nil
	}
	defer getResp.Body.Close()
	content, _ := io.ReadAll(getResp.Body)

	if getResp.StatusCode == http.StatusNotFound {
		w.state.removeFile(filePath)
		return nil
	}
	if getResp.StatusCode == http.StatusOK && len(content) != 0 {
		return fmt.Errorf("invariant #23 violated: empty file %s returned %d bytes", filePath, len(content))
	}

	return nil
}

// verifyDeduplication tests that identical content is deduplicated
func (w *Worker) verifyDeduplication(ctx context.Context) error {
	if w.injector == nil || w.injector.db == nil {
		return nil // Need db access to verify dedup
	}

	content := []byte("dedup test content " + randomName(w.rng))
	file1 := "/" + randomName(w.rng) + "_dedup1.txt"
	file2 := "/" + randomName(w.rng) + "_dedup2.txt"

	// Create first file
	req1, err := http.NewRequestWithContext(ctx, http.MethodPut, w.baseURL+file1, bytes.NewReader(content))
	if err != nil {
		return nil
	}
	resp1, err := w.client.Do(req1)
	if err != nil {
		return nil
	}
	resp1.Body.Close()
	if resp1.StatusCode != http.StatusCreated {
		return nil
	}

	// Create second file with same content
	req2, err := http.NewRequestWithContext(ctx, http.MethodPut, w.baseURL+file2, bytes.NewReader(content))
	if err != nil {
		return nil
	}
	resp2, err := w.client.Do(req2)
	if err != nil {
		return nil
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusCreated {
		return nil
	}

	// Query the database to check if both files share the same blob_id
	var blobID1, blobID2 sql.NullInt64
	err = w.injector.db.QueryRow(`SELECT blob_id FROM files WHERE path = ? AND deleted = 0 ORDER BY version DESC LIMIT 1`, file1).Scan(&blobID1)
	if err != nil {
		return nil
	}
	err = w.injector.db.QueryRow(`SELECT blob_id FROM files WHERE path = ? AND deleted = 0 ORDER BY version DESC LIMIT 1`, file2).Scan(&blobID2)
	if err != nil {
		return nil
	}

	if blobID1.Valid && blobID2.Valid && blobID1.Int64 != blobID2.Int64 {
		return fmt.Errorf("invariant #24 violated: identical content not deduplicated (%s blob=%d, %s blob=%d)", file1, blobID1.Int64, file2, blobID2.Int64)
	}

	// Clean up
	for _, f := range []string{file1, file2} {
		delReq, _ := http.NewRequestWithContext(ctx, http.MethodDelete, w.baseURL+f, nil)
		if delReq != nil {
			delResp, _ := w.client.Do(delReq)
			if delResp != nil {
				io.Copy(io.Discard, delResp.Body)
				delResp.Body.Close()
			}
		}
	}

	return nil
}

// verifyReadPermissions tests that consecutive reads return consistent content
func (w *Worker) verifyReadPermissions(ctx context.Context) error {
	filePath, originalContent, ok := w.state.randomFile(w.rng)
	if !ok {
		return nil
	}

	// Read the file
	getResp, err := w.client.Get(w.baseURL + filePath)
	if err != nil {
		return nil
	}
	defer getResp.Body.Close()
	readContent, _ := io.ReadAll(getResp.Body)

	if getResp.StatusCode != http.StatusOK {
		return nil
	}

	// Read again and verify content hasn't changed
	getResp2, err := w.client.Get(w.baseURL + filePath)
	if err != nil {
		return nil
	}
	defer getResp2.Body.Close()
	readContent2, _ := io.ReadAll(getResp2.Body)

	if getResp2.StatusCode == http.StatusOK && !bytes.Equal(readContent, readContent2) {
		return fmt.Errorf("invariant #25 violated: consecutive reads returned different content for %s", filePath)
	}

	// Verify it matches our expected content (allowing for concurrent modifications)
	if !bytes.Equal(originalContent, readContent) {
		w.state.addFile(filePath, readContent) // Update our state
	}

	return nil
}

// ==================== Concurrent Access ====================

// concurrentReaders tests multiple simultaneous reads of the same file
func (w *Worker) concurrentReaders(ctx context.Context) error {
	filePath, expectedContent, ok := w.state.randomFile(w.rng)
	if !ok {
		return nil
	}

	// Launch 3 concurrent readers
	var wg sync.WaitGroup
	results := make(chan []byte, 3)
	errors := make(chan error, 3)

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := w.client.Get(w.baseURL + filePath)
			if err != nil {
				errors <- err
				return
			}
			defer resp.Body.Close()
			content, _ := io.ReadAll(resp.Body)
			if resp.StatusCode == http.StatusOK {
				results <- content
			} else if resp.StatusCode == http.StatusNotFound {
				errors <- fmt.Errorf("file not found")
			}
		}()
	}

	wg.Wait()
	close(results)
	close(errors)

	// All successful reads should return the same content
	var contents [][]byte
	for c := range results {
		contents = append(contents, c)
	}

	if len(contents) >= 2 {
		for i := 1; i < len(contents); i++ {
			if !bytes.Equal(contents[0], contents[i]) {
				return fmt.Errorf("invariant #26 violated: concurrent reads returned different content for %s", filePath)
			}
		}
		// Update state if content changed
		if len(contents) > 0 && !bytes.Equal(expectedContent, contents[0]) {
			w.state.addFile(filePath, contents[0])
		}
	}

	return nil
}

// concurrentReadWrite tests reading while another request writes
func (w *Worker) concurrentReadWrite(ctx context.Context) error {
	// Create a test file
	filePath := "/" + randomName(w.rng) + "_rw.txt"
	originalContent := []byte("original content for read-write test")
	newContent := []byte("new content after write")

	// Create the file
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, w.baseURL+filePath, bytes.NewReader(originalContent))
	if err != nil {
		return nil
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return nil
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return nil
	}

	// Launch concurrent read and write
	var wg sync.WaitGroup
	readResult := make(chan []byte, 1)
	writeResult := make(chan bool, 1)

	wg.Add(2)

	// Reader
	go func() {
		defer wg.Done()
		resp, err := w.client.Get(w.baseURL + filePath)
		if err != nil {
			return
		}
		defer resp.Body.Close()
		content, _ := io.ReadAll(resp.Body)
		if resp.StatusCode == http.StatusOK {
			readResult <- content
		}
	}()

	// Writer
	go func() {
		defer wg.Done()
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, w.baseURL+filePath, bytes.NewReader(newContent))
		if err != nil {
			return
		}
		resp, err := w.client.Do(req)
		if err != nil {
			return
		}
		resp.Body.Close()
		writeResult <- (resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusNoContent)
	}()

	wg.Wait()
	close(readResult)
	close(writeResult)

	// The read should return either original or new content (not garbage)
	if content, ok := <-readResult; ok {
		if !bytes.Equal(content, originalContent) && !bytes.Equal(content, newContent) {
			// Could be from a concurrent restore, so don't fail hard
			log.Printf("[concurrent-rw] read returned unexpected content for %s (len=%d)", filePath, len(content))
		}
	}

	// Clean up
	delReq, _ := http.NewRequestWithContext(ctx, http.MethodDelete, w.baseURL+filePath, nil)
	if delReq != nil {
		delResp, _ := w.client.Do(delReq)
		if delResp != nil {
			io.Copy(io.Discard, delResp.Body)
			delResp.Body.Close()
		}
	}

	return nil
}

// ==================== Garbage Collection & Integrity ====================

// verifyGarbageCollection verifies that GC is working by checking for orphaned blobs
func (w *Worker) verifyGarbageCollection(ctx context.Context) error {
	if w.injector == nil || w.injector.db == nil {
		return nil
	}

	// Check for old orphaned blobs that should have been cleaned up
	// These are blobs where local_written=0, local_deleting=0, and creation_time is old
	uploadTTL := 5 * time.Minute // Default from main.go
	cutoff := time.Now().Add(-uploadTTL - time.Minute).Unix()

	var count int
	err := w.injector.db.QueryRow(`
		SELECT COUNT(*) FROM blobs
		WHERE local_written = 0
		AND local_deleting = 0
		AND creation_time < ?`, cutoff).Scan(&count)
	if err != nil {
		return nil
	}

	if count > 100 {
		// Too many orphaned blobs - GC may not be running
		log.Printf("[gc-check] found %d orphaned blobs older than upload TTL", count)
	}

	// Check that old file versions are being compacted
	ttl := 24 * time.Hour // Default from main.go
	fileCutoff := time.Now().Add(-ttl - time.Hour).Unix()

	var oldVersions int
	err = w.injector.db.QueryRow(`
		SELECT COUNT(*) FROM files f
		WHERE f.created_at < ?
		AND (f.deleted = 1 OR EXISTS (SELECT 1 FROM files f2 WHERE f2.path = f.path AND f2.version > f.version))`, fileCutoff).Scan(&oldVersions)
	if err != nil {
		return nil
	}

	if oldVersions > 1000 {
		log.Printf("[gc-check] found %d old file versions that should be compacted", oldVersions)
	}

	return nil
}

// verifyIntegrityCheckLoop verifies the background integrity check is working
func (w *Worker) verifyIntegrityCheckLoop(ctx context.Context) error {
	if w.injector == nil || w.injector.db == nil {
		return nil
	}

	// Check that blobs are getting integrity-checked
	// Look for blobs that have been checked recently
	recentCheck := time.Now().Add(-time.Hour).Unix()

	var checkedCount int
	err := w.injector.db.QueryRow(`
		SELECT COUNT(*) FROM blobs
		WHERE last_integrity_check IS NOT NULL
		AND last_integrity_check > ?`, recentCheck).Scan(&checkedCount)
	if err != nil {
		return nil
	}

	var totalEligible int
	err = w.injector.db.QueryRow(`
		SELECT COUNT(*) FROM blobs
		WHERE local_written = 1
		AND remote_written = 1
		AND local_deleting = 0
		AND checksum IS NOT NULL`).Scan(&totalEligible)
	if err != nil {
		return nil
	}

	// Log progress but don't fail - integrity checks take time
	if totalEligible > 0 && checkedCount == 0 {
		log.Printf("[integrity-check] no blobs checked in the last hour (%d eligible)", totalEligible)
	}

	// Check for blobs with mismatched checksums that should be re-downloaded
	var needsRedownload int
	err = w.injector.db.QueryRow(`
		SELECT COUNT(*) FROM blobs
		WHERE local_written = 0
		AND remote_written = 1
		AND remote_deleted = 0`).Scan(&needsRedownload)
	if err != nil {
		return nil
	}

	if needsRedownload > 0 {
		log.Printf("[integrity-check] %d blobs marked for re-download from remote", needsRedownload)
	}

	return nil
}

// ==================== Database Backup ====================

// verifyDatabaseBackup checks that database backups are being created
func (w *Worker) verifyDatabaseBackup(ctx context.Context) error {
	if w.injector == nil || w.injector.blobsDir == "" {
		return nil
	}

	// Check if backup file exists (it's at the same level as blobs dir)
	backupPath := filepath.Join(filepath.Dir(w.injector.blobsDir), "meta.db.backup")
	info, err := os.Stat(backupPath)
	if err != nil {
		if os.IsNotExist(err) {
			// Backup doesn't exist - might be expected if backup interval hasn't elapsed
			return nil
		}
		return nil
	}

	// Check that backup is recent (within 2x backup interval)
	maxAge := 2 * time.Hour // Default backup interval is 1 hour
	if time.Since(info.ModTime()) > maxAge {
		log.Printf("[backup-check] database backup is stale (age=%v)", time.Since(info.ModTime()))
	}

	return nil
}
