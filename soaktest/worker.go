package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Worker runs tests against the server. Each test uses isolated files
// with unique names to avoid interference from other workers.
type Worker struct {
	id       int
	client   *http.Client
	rng      *rand.Rand
	baseURL  string
	failures chan<- Failure
	stats    *Stats
	injector *Injector
}

// Test is a function that verifies a specific invariant.
// Returns an error describing the invariant violation, or nil if the test passed.
type Test struct {
	Name   string
	Weight int
	Fn     func(ctx context.Context, w *Worker) error
}

// tests defines all available tests with their relative weights.
// Higher weight = more frequently selected.
var tests = []Test{
	// Core data integrity
	{"ReadAfterWrite", 30, testReadAfterWrite},
	{"ConsecutiveReads", 20, testConsecutiveReads},
	{"Overwrite", 15, testOverwrite},
	{"EmptyFile", 5, testEmptyFile},

	// Concurrency
	{"ConcurrentReaders", 10, testConcurrentReaders},
	{"ConcurrentWriters", 10, testConcurrentWriters},
	{"ReadDuringWrite", 10, testReadDuringWrite},

	// Directory operations
	{"MkdirRequiresParent", 5, testMkdirRequiresParent},
	{"FileRequiresParent", 5, testFileRequiresParent},
	{"DeleteReturns404", 5, testDeleteReturns404},
	{"CannotDeleteRoot", 2, testCannotDeleteRoot},
	{"PathExclusivity", 5, testPathExclusivity},

	// WebDAV operations
	{"Copy", 10, testCopy},
	{"Move", 10, testMove},

	// Chaos (generates load without strict invariant checks)
	{"Chaos", 30, testChaos},
}

func (w *Worker) Run(ctx context.Context, opCount *atomic.Int64) {
	totalWeight := 0
	for _, t := range tests {
		totalWeight += t.Weight
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Select a test weighted by frequency
		n := w.rng.IntN(totalWeight)
		var test Test
		for _, t := range tests {
			n -= t.Weight
			if n < 0 {
				test = t
				break
			}
		}

		w.stats.Inc(test.Name)
		if err := test.Fn(ctx, w); err != nil {
			select {
			case w.failures <- Failure{Test: test.Name, Error: err.Error(), Timestamp: time.Now()}:
			default:
			}
		}
		opCount.Add(1)
	}
}

// uid returns a unique identifier for this worker's test files.
func (w *Worker) uid() string {
	return fmt.Sprintf("%d_%d", w.id, w.rng.Uint64())
}

// randomContent generates random bytes of the given size.
func (w *Worker) randomContent(size int) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = byte(w.rng.IntN(256))
	}
	return b
}

// put writes content to a path. Returns status code or 0 on error.
func (w *Worker) put(ctx context.Context, path string, content []byte) int {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, w.baseURL+path, bytes.NewReader(content))
	if err != nil {
		return 0
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return 0
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode
}

// get reads a path. Returns content, status code, or nil/0 on error.
func (w *Worker) get(ctx context.Context, path string) ([]byte, int) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, w.baseURL+path, nil)
	if err != nil {
		return nil, 0
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return nil, 0
	}
	defer resp.Body.Close()
	content, _ := io.ReadAll(resp.Body)
	return content, resp.StatusCode
}

// delete removes a path. Returns status code or 0 on error.
func (w *Worker) delete(ctx context.Context, path string) int {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, w.baseURL+path, nil)
	if err != nil {
		return 0
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return 0
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode
}

// mkcol creates a directory. Returns status code or 0 on error.
func (w *Worker) mkcol(ctx context.Context, path string) int {
	req, err := http.NewRequestWithContext(ctx, "MKCOL", w.baseURL+path, nil)
	if err != nil {
		return 0
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return 0
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode
}

// copy copies src to dst. Returns status code or 0 on error.
func (w *Worker) copy(ctx context.Context, src, dst string) int {
	req, err := http.NewRequestWithContext(ctx, "COPY", w.baseURL+src, nil)
	if err != nil {
		return 0
	}
	req.Header.Set("Destination", w.baseURL+dst)
	resp, err := w.client.Do(req)
	if err != nil {
		return 0
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode
}

// move moves src to dst. Returns status code or 0 on error.
func (w *Worker) move(ctx context.Context, src, dst string) int {
	req, err := http.NewRequestWithContext(ctx, "MOVE", w.baseURL+src, nil)
	if err != nil {
		return 0
	}
	req.Header.Set("Destination", w.baseURL+dst)
	resp, err := w.client.Do(req)
	if err != nil {
		return 0
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode
}

// cleanup removes a test file, ignoring errors.
func (w *Worker) cleanup(ctx context.Context, paths ...string) {
	for _, p := range paths {
		w.delete(ctx, p)
	}
}

// ===========================================================================
// Tests
// ===========================================================================

// testReadAfterWrite verifies that content written can be immediately read back.
func testReadAfterWrite(ctx context.Context, w *Worker) error {
	path := "/test_raw_" + w.uid() + ".txt"
	content := w.randomContent(1 + w.rng.IntN(10*1024))
	defer w.cleanup(ctx, path)

	status := w.put(ctx, path, content)
	if status != http.StatusCreated && status != http.StatusNoContent {
		return nil // write failed, not an invariant violation
	}

	got, status := w.get(ctx, path)
	if status != http.StatusOK {
		return fmt.Errorf("GET after PUT returned %d", status)
	}
	if !bytes.Equal(content, got) {
		return fmt.Errorf("content mismatch: wrote %d bytes (sha256=%s), read %d bytes (sha256=%s)",
			len(content), sha256sum(content), len(got), sha256sum(got))
	}
	return nil
}

// testConsecutiveReads verifies that reading the same file twice returns identical content.
func testConsecutiveReads(ctx context.Context, w *Worker) error {
	path := "/test_consec_" + w.uid() + ".txt"
	content := w.randomContent(1 + w.rng.IntN(10*1024))
	defer w.cleanup(ctx, path)

	if w.put(ctx, path, content) == 0 {
		return nil
	}

	read1, s1 := w.get(ctx, path)
	if s1 != http.StatusOK {
		return nil
	}
	read2, s2 := w.get(ctx, path)
	if s2 != http.StatusOK {
		return nil
	}

	if !bytes.Equal(read1, read2) {
		return fmt.Errorf("consecutive reads differ: first=%d bytes, second=%d bytes", len(read1), len(read2))
	}
	return nil
}

// testOverwrite verifies that overwriting a file replaces its content completely.
func testOverwrite(ctx context.Context, w *Worker) error {
	path := "/test_overwrite_" + w.uid() + ".txt"
	content1 := w.randomContent(1000 + w.rng.IntN(5000))
	content2 := w.randomContent(500 + w.rng.IntN(2000)) // different size
	defer w.cleanup(ctx, path)

	if w.put(ctx, path, content1) == 0 {
		return nil
	}
	status := w.put(ctx, path, content2)
	if status != http.StatusCreated && status != http.StatusNoContent {
		return nil
	}

	got, status := w.get(ctx, path)
	if status != http.StatusOK {
		return nil
	}
	if !bytes.Equal(content2, got) {
		// Check if we got content1 (stale read) or garbage
		if bytes.Equal(content1, got) {
			return fmt.Errorf("stale read after overwrite: got original content instead of new")
		}
		return fmt.Errorf("overwrite produced wrong content: expected %d bytes, got %d bytes", len(content2), len(got))
	}
	return nil
}

// testEmptyFile verifies that empty files can be created and read.
func testEmptyFile(ctx context.Context, w *Worker) error {
	path := "/test_empty_" + w.uid() + ".txt"
	defer w.cleanup(ctx, path)

	status := w.put(ctx, path, []byte{})
	if status != http.StatusCreated && status != http.StatusNoContent {
		return nil
	}

	got, status := w.get(ctx, path)
	if status != http.StatusOK {
		return fmt.Errorf("GET empty file returned %d", status)
	}
	if len(got) != 0 {
		return fmt.Errorf("empty file returned %d bytes", len(got))
	}
	return nil
}

// testConcurrentReaders verifies multiple simultaneous reads return identical content.
func testConcurrentReaders(ctx context.Context, w *Worker) error {
	path := "/test_concurrent_read_" + w.uid() + ".txt"
	content := w.randomContent(5000 + w.rng.IntN(10000))
	defer w.cleanup(ctx, path)

	if w.put(ctx, path, content) == 0 {
		return nil
	}

	const numReaders = 5
	results := make(chan []byte, numReaders)
	var wg sync.WaitGroup

	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, status := w.get(ctx, path)
			if status == http.StatusOK {
				results <- got
			}
		}()
	}
	wg.Wait()
	close(results)

	var all [][]byte
	for r := range results {
		all = append(all, r)
	}

	if len(all) < 2 {
		return nil // not enough successful reads
	}

	first := all[0]
	for i, r := range all[1:] {
		if !bytes.Equal(first, r) {
			return fmt.Errorf("concurrent read %d differs: first=%d bytes, this=%d bytes", i+1, len(first), len(r))
		}
	}
	return nil
}

// testConcurrentWriters verifies that concurrent writes to the same path result in
// exactly one of the written values (no torn writes or data mixing).
func testConcurrentWriters(ctx context.Context, w *Worker) error {
	path := "/test_concurrent_write_" + w.uid() + ".txt"
	defer w.cleanup(ctx, path)

	// Create distinct content for each writer
	const numWriters = 5
	contents := make([][]byte, numWriters)
	checksums := make(map[string]int) // checksum -> writer index
	for i := range contents {
		// Use different sizes to make torn writes more detectable
		contents[i] = w.randomContent(1000 + i*500)
		checksums[sha256sum(contents[i])] = i
	}

	var wg sync.WaitGroup
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			w.put(ctx, path, contents[idx])
		}(i)
	}
	wg.Wait()

	// Read the final result
	got, status := w.get(ctx, path)
	if status != http.StatusOK {
		return nil // file might not exist if all writes failed
	}

	// The result must be exactly one of the written values
	gotSum := sha256sum(got)
	if _, ok := checksums[gotSum]; !ok {
		return fmt.Errorf("concurrent writes produced invalid content: got %d bytes (sha256=%s), expected one of %d values",
			len(got), gotSum, numWriters)
	}
	return nil
}

// testReadDuringWrite verifies that reads during concurrent writes return
// either the old content or the new content, never garbage.
func testReadDuringWrite(ctx context.Context, w *Worker) error {
	path := "/test_read_during_write_" + w.uid() + ".txt"
	content1 := w.randomContent(5000)
	content2 := w.randomContent(8000) // different size
	defer w.cleanup(ctx, path)

	// Write initial content
	if w.put(ctx, path, content1) == 0 {
		return nil
	}

	sum1 := sha256sum(content1)
	sum2 := sha256sum(content2)

	// Start concurrent read and write
	readResult := make(chan []byte, 1)
	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		got, status := w.get(ctx, path)
		if status == http.StatusOK {
			readResult <- got
		}
		close(readResult)
	}()
	go func() {
		defer wg.Done()
		w.put(ctx, path, content2)
	}()
	wg.Wait()

	got, ok := <-readResult
	if !ok {
		return nil // read failed
	}

	gotSum := sha256sum(got)
	if gotSum != sum1 && gotSum != sum2 {
		return fmt.Errorf("read during write returned invalid content: got %d bytes (sha256=%s), expected sha256=%s or sha256=%s",
			len(got), gotSum, sum1, sum2)
	}
	return nil
}

// testMkdirRequiresParent verifies that MKCOL fails when parent doesn't exist.
func testMkdirRequiresParent(ctx context.Context, w *Worker) error {
	path := "/nonexistent_" + w.uid() + "/subdir"

	status := w.mkcol(ctx, path)
	if status == http.StatusCreated {
		w.cleanup(ctx, path)
		return fmt.Errorf("MKCOL succeeded without parent directory existing")
	}
	return nil
}

// testFileRequiresParent verifies that PUT fails when parent doesn't exist.
func testFileRequiresParent(ctx context.Context, w *Worker) error {
	path := "/nonexistent_" + w.uid() + "/file.txt"

	status := w.put(ctx, path, []byte("test"))
	if status == http.StatusCreated || status == http.StatusNoContent {
		w.cleanup(ctx, path)
		return fmt.Errorf("PUT succeeded without parent directory existing")
	}
	return nil
}

// testDeleteReturns404 verifies that deleted files return 404.
func testDeleteReturns404(ctx context.Context, w *Worker) error {
	path := "/test_delete404_" + w.uid() + ".txt"

	if w.put(ctx, path, []byte("test")) == 0 {
		return nil
	}
	w.delete(ctx, path)

	_, status := w.get(ctx, path)
	if status != http.StatusNotFound {
		return fmt.Errorf("GET after DELETE returned %d, expected 404", status)
	}
	return nil
}

// testCannotDeleteRoot verifies that the root directory cannot be deleted.
func testCannotDeleteRoot(ctx context.Context, w *Worker) error {
	status := w.delete(ctx, "/")
	if status == http.StatusNoContent || status == http.StatusOK {
		return fmt.Errorf("DELETE / succeeded with status %d", status)
	}
	return nil
}

// testPathExclusivity verifies that a path cannot be both a file and directory.
func testPathExclusivity(ctx context.Context, w *Worker) error {
	path := "/test_excl_" + w.uid()
	defer w.cleanup(ctx, path)

	// Create as directory
	status := w.mkcol(ctx, path)
	if status != http.StatusCreated {
		return nil
	}

	// Try to create as file - should fail
	status = w.put(ctx, path, []byte("test"))
	if status == http.StatusCreated || status == http.StatusNoContent {
		return fmt.Errorf("PUT succeeded at path that is a directory")
	}
	return nil
}

// testCopy verifies that COPY produces an identical file.
func testCopy(ctx context.Context, w *Worker) error {
	src := "/test_copy_src_" + w.uid() + ".txt"
	dst := "/test_copy_dst_" + w.uid() + ".txt"
	content := w.randomContent(1 + w.rng.IntN(10*1024))
	defer w.cleanup(ctx, src, dst)

	if w.put(ctx, src, content) == 0 {
		return nil
	}

	status := w.copy(ctx, src, dst)
	if status != http.StatusCreated && status != http.StatusNoContent {
		return nil // COPY failed, not an invariant violation
	}

	got, status := w.get(ctx, dst)
	if status != http.StatusOK {
		return fmt.Errorf("GET copied file returned %d", status)
	}
	if !bytes.Equal(content, got) {
		return fmt.Errorf("COPY produced different content: src=%d bytes, dst=%d bytes", len(content), len(got))
	}
	return nil
}

// testMove verifies that MOVE relocates a file correctly.
func testMove(ctx context.Context, w *Worker) error {
	src := "/test_move_src_" + w.uid() + ".txt"
	dst := "/test_move_dst_" + w.uid() + ".txt"
	content := w.randomContent(1 + w.rng.IntN(10*1024))
	defer w.cleanup(ctx, src, dst)

	if w.put(ctx, src, content) == 0 {
		return nil
	}

	status := w.move(ctx, src, dst)
	if status != http.StatusCreated && status != http.StatusNoContent {
		return nil
	}

	// Source should be gone
	_, srcStatus := w.get(ctx, src)
	if srcStatus != http.StatusNotFound {
		return fmt.Errorf("source still exists after MOVE (status=%d)", srcStatus)
	}

	// Destination should have content
	got, dstStatus := w.get(ctx, dst)
	if dstStatus != http.StatusOK {
		return fmt.Errorf("GET moved file returned %d", dstStatus)
	}
	if !bytes.Equal(content, got) {
		return fmt.Errorf("MOVE produced different content: original=%d bytes, moved=%d bytes", len(content), len(got))
	}
	return nil
}

// testChaos generates random operations to create background load.
// It doesn't check strict invariants - just exercises the server.
func testChaos(ctx context.Context, w *Worker) error {
	path := "/chaos_" + w.uid() + ".txt"
	defer w.cleanup(ctx, path)

	// Random sequence of operations
	ops := w.rng.IntN(5) + 1
	for i := 0; i < ops; i++ {
		switch w.rng.IntN(4) {
		case 0: // write
			w.put(ctx, path, w.randomContent(w.rng.IntN(50*1024)))
		case 1: // read
			w.get(ctx, path)
		case 2: // overwrite
			w.put(ctx, path, w.randomContent(w.rng.IntN(10*1024)))
		case 3: // delete and recreate
			w.delete(ctx, path)
			w.put(ctx, path, w.randomContent(w.rng.IntN(5*1024)))
		}
	}

	// Optionally inject corruption if configured
	if w.injector != nil && w.rng.Float64() < w.injector.corruptRate {
		w.injector.CorruptRandomBlob(w.rng)
	}

	return nil
}

// sha256sum returns the hex-encoded SHA256 of data.
func sha256sum(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}
