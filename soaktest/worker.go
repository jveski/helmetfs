package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type weightedOp struct {
	name   string
	weight int
}

// baseOps are the core operations always available
var baseOps = []weightedOp{
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

type Worker struct {
	id       int
	client   *http.Client
	state    *State
	rng      *rand.Rand
	baseURL  string
	failures chan<- Failure
	injector *FailureInjector
}

// newRequest creates a new HTTP request with the baseURL prepended to the path.
// Returns nil if request creation fails.
func (w *Worker) newRequest(ctx context.Context, method, path string, body io.Reader) *http.Request {
	req, err := http.NewRequestWithContext(ctx, method, w.baseURL+path, body)
	if err != nil {
		return nil
	}
	return req
}

// doRequest is a helper that performs an HTTP request and returns the response.
// It handles common error cases by returning nil response (caller should check).
// The caller is responsible for closing resp.Body if resp is non-nil.
func (w *Worker) doRequest(ctx context.Context, method, path string, body io.Reader) *http.Response {
	req := w.newRequest(ctx, method, path, body)
	if req == nil {
		return nil
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return nil
	}
	return resp
}

// doRequestDiscard performs an HTTP request and discards the response body.
// Returns the status code, or 0 if the request failed.
func (w *Worker) doRequestDiscard(ctx context.Context, method, path string, body io.Reader) int {
	resp := w.doRequest(ctx, method, path, body)
	if resp == nil {
		return 0
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

// doReq executes a pre-built request and returns the response.
// Returns nil if the request fails.
func (w *Worker) doReq(req *http.Request) *http.Response {
	if req == nil {
		return nil
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return nil
	}
	return resp
}

// doReqDiscard executes a pre-built request and discards the response body.
// Returns the status code, or 0 if the request failed.
func (w *Worker) doReqDiscard(req *http.Request) int {
	resp := w.doReq(req)
	if resp == nil {
		return 0
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

// deleteTestFile is a helper for cleaning up test files.
func (w *Worker) deleteTestFile(ctx context.Context, path string) {
	w.doRequestDiscard(ctx, http.MethodDelete, path, nil)
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
	ops := baseOps

	// Add failure injection operations if enabled
	if w.injector != nil {
		if w.injector.corruptRate > 0 {
			ops = append(ops,
				weightedOp{"injectCorruption", 2},
				weightedOp{"injectBlobDeletion", 1},
				weightedOp{"verifyCorruptionDetected", 2},
			)
		}
		if w.injector.rcloneFailure > 0 {
			ops = append(ops, weightedOp{"injectRcloneFailure", 1})
		}
		// Add GC and integrity checks when we have db access
		if w.injector.db != nil {
			ops = append(ops,
				weightedOp{"verifyGarbageCollection", 2},
				weightedOp{"verifyIntegrityCheckLoop", 2},
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

	status := w.doRequestDiscard(ctx, http.MethodPut, filePath, bytes.NewReader(content))
	if status != http.StatusCreated && status != http.StatusNoContent {
		return nil
	}
	w.state.addFile(filePath, content)

	readResp := w.doRequest(ctx, http.MethodGet, filePath, nil)
	if readResp == nil {
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

	// 500 errors on read-after-write could indicate a real bug (e.g., blob not written correctly).
	// Retry once to distinguish transient failures from persistent issues.
	if readResp.StatusCode == http.StatusInternalServerError {
		time.Sleep(50 * time.Millisecond)
		retryResp := w.doRequest(ctx, http.MethodGet, filePath, nil)
		if retryResp == nil {
			return nil
		}
		defer retryResp.Body.Close()
		retryContent, _ := io.ReadAll(retryResp.Body)

		if retryResp.StatusCode == http.StatusInternalServerError {
			return fmt.Errorf("invariant #3 violated: persistent 500 error on read-after-write for %s", filePath)
		}
		if retryResp.StatusCode == http.StatusOK {
			if !bytes.Equal(content, retryContent) {
				return fmt.Errorf("invariant #1 violated: read-after-write mismatch for %s (wrote %d bytes, read %d bytes)", filePath, len(content), len(retryContent))
			}
			return nil
		}
		if retryResp.StatusCode == http.StatusNotFound {
			w.state.removeFile(filePath)
			return nil
		}
	}

	if readResp.StatusCode != http.StatusOK {
		return nil // Other status codes (e.g., 400-range) are not invariant violations
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

	resp := w.doRequest(ctx, http.MethodGet, filePath, nil)
	if resp == nil {
		return nil
	}
	defer resp.Body.Close()
	content, _ := io.ReadAll(resp.Body)

	if resp.StatusCode == http.StatusNotFound {
		// File was deleted - could be concurrent delete or restore operation
		w.state.removeFile(filePath)
		return nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil
	}

	// Content mismatch check: if content differs from expected, this could be:
	// 1. A concurrent write (legitimate) - we need to verify by re-reading
	// 2. Data corruption (bug) - should be reported
	if !bytes.Equal(expectedContent, content) {
		// Re-read to check if this is a stable state or transient
		time.Sleep(10 * time.Millisecond)
		verifyResp := w.doRequest(ctx, http.MethodGet, filePath, nil)
		if verifyResp == nil {
			return nil
		}
		defer verifyResp.Body.Close()
		verifyContent, _ := io.ReadAll(verifyResp.Body)

		if verifyResp.StatusCode == http.StatusNotFound {
			// File was deleted between reads - concurrent operation
			w.state.removeFile(filePath)
			return nil
		}
		if verifyResp.StatusCode == http.StatusOK {
			if bytes.Equal(content, verifyContent) {
				// Content is stable but different from expected - concurrent write occurred
				// Update our state to reflect the new content
				w.state.addFile(filePath, content)
			} else {
				// Content changed between two reads with no write from us - potential data instability
				return fmt.Errorf("invariant #2 violated: file %s content unstable between reads (first=%d bytes, second=%d bytes)", filePath, len(content), len(verifyContent))
			}
		}
	}
	return nil
}

func (w *Worker) deleteFile(ctx context.Context) error {
	filePath, _, ok := w.state.randomFile(w.rng)
	if !ok {
		return nil
	}

	status := w.doRequestDiscard(ctx, http.MethodDelete, filePath, nil)
	if status == http.StatusNoContent || status == http.StatusNotFound {
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

	if w.doRequestDiscard(ctx, "MKCOL", dirPath, nil) == http.StatusCreated {
		w.state.addDir(dirPath)
	}
	return nil
}

func (w *Worker) listDir(ctx context.Context) error {
	dir, ok := w.state.randomDir(w.rng)
	if !ok {
		return nil
	}

	req := w.newRequest(ctx, "PROPFIND", dir, strings.NewReader(`<?xml version="1.0" encoding="utf-8"?><propfind xmlns="DAV:"><prop><resourcetype/></prop></propfind>`))
	if req == nil {
		return nil
	}
	req.Header.Set("Depth", "1")
	req.Header.Set("Content-Type", "application/xml")
	w.doReqDiscard(req)
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

	req := w.newRequest(ctx, "MOVE", srcPath, nil)
	if req == nil {
		return nil
	}
	req.Header.Set("Destination", w.baseURL+destPath)
	status := w.doReqDiscard(req)

	if status == http.StatusCreated || status == http.StatusNoContent {
		w.state.removeFile(srcPath)
		w.state.addFile(destPath, content)
	}
	return nil
}

func (w *Worker) restore(ctx context.Context) error {
	timestamp := time.Now().Add(-time.Duration(w.rng.IntN(3600)) * time.Second).Format(time.RFC3339)
	form := url.Values{}
	form.Set("timestamp", timestamp)

	req := w.newRequest(ctx, http.MethodPost, "/api/restore", strings.NewReader(form.Encode()))
	if req == nil {
		return nil
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w.doReqDiscard(req)
	return nil
}

func (w *Worker) verifyDeletedReturns404(ctx context.Context) error {
	name := randomName(w.rng) + "_deleted.txt"
	filePath := "/" + name
	content := []byte("test content")

	if w.doRequestDiscard(ctx, http.MethodPut, filePath, bytes.NewReader(content)) != http.StatusCreated {
		return nil
	}

	w.doRequestDiscard(ctx, http.MethodDelete, filePath, nil)

	status := w.doRequestDiscard(ctx, http.MethodGet, filePath, nil)
	if status != http.StatusNotFound {
		return fmt.Errorf("invariant #12 violated: deleted file %s returned %d instead of 404", filePath, status)
	}
	return nil
}

func (w *Worker) verifyParentMustExist(ctx context.Context) error {
	nonExistent := "/" + randomName(w.rng) + "_noparent/" + randomName(w.rng) + ".txt"
	content := []byte("test")

	status := w.doRequestDiscard(ctx, http.MethodPut, nonExistent, bytes.NewReader(content))
	if status == http.StatusCreated || status == http.StatusNoContent {
		return fmt.Errorf("invariant #4 violated: created file %s without parent directory existing", nonExistent)
	}
	return nil
}

func (w *Worker) verifyPathExclusivity(ctx context.Context) error {
	dirName := "/" + randomName(w.rng) + "_excl"

	if w.doRequestDiscard(ctx, "MKCOL", dirName, nil) != http.StatusCreated {
		return nil
	}
	w.state.addDir(dirName)

	putStatus := w.doRequestDiscard(ctx, http.MethodPut, dirName, bytes.NewReader([]byte("test")))
	if putStatus == http.StatusCreated || putStatus == http.StatusNoContent {
		// Before reporting an invariant violation, verify the directory still exists.
		// A concurrent delete or restore operation might have removed the directory
		// between the MKCOL and PUT, making the PUT success expected behavior.
		statReq := w.newRequest(ctx, "PROPFIND", dirName, strings.NewReader(`<?xml version="1.0" encoding="utf-8"?><propfind xmlns="DAV:"><prop><resourcetype/></prop></propfind>`))
		if statReq == nil {
			return nil
		}
		statReq.Header.Set("Depth", "0")
		statReq.Header.Set("Content-Type", "application/xml")
		statResp := w.doReq(statReq)
		if statResp == nil {
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

	req := w.newRequest(ctx, http.MethodPost, "/api/restore", strings.NewReader(form.Encode()))
	if req == nil {
		return nil
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	status := w.doReqDiscard(req)

	if status != http.StatusBadRequest {
		return fmt.Errorf("invariant #14 violated: restore accepted future timestamp, got status %d", status)
	}
	return nil
}

func (w *Worker) verifyCannotDeleteRoot(ctx context.Context) error {
	if w.doRequestDiscard(ctx, http.MethodDelete, "/", nil) == http.StatusNoContent {
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
	status := w.doRequestDiscard(ctx, http.MethodPut, filePath, bytes.NewReader(content))
	if status != http.StatusCreated && status != http.StatusNoContent {
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
				readResp := w.doRequest(ctx, http.MethodGet, filePath, nil)
				if readResp == nil {
					return nil
				}
				defer readResp.Body.Close()
				readContent, _ := io.ReadAll(readResp.Body)

				// The server MUST detect corruption and return an error (non-200).
				// Acceptable responses:
				// - 500 Internal Server Error (checksum validation failed)
				// - 404 Not Found (blob marked as missing, pending re-download)
				// Unacceptable responses:
				// - 200 OK with original content (corruption not detected - bits somehow uncorrupted)
				// - 200 OK with different/garbage content (silent data corruption served to client!)
				if readResp.StatusCode == http.StatusOK {
					if bytes.Equal(content, readContent) {
						// This is actually surprising - we corrupted the file but got original content back.
						// This could happen if:
						// 1. The corruption didn't actually happen (file locked, etc)
						// 2. There's a caching layer returning stale data
						// Either way, not necessarily a bug, so just log it
						log.Printf("[failure-injection] corrupted file %s returned original content (corruption may not have taken effect)", filePath)
					} else {
						// This is the critical bug: server returned 200 OK with corrupted/garbage data!
						// The checksum validation should have caught this and returned an error.
						return fmt.Errorf("invariant #99 violated: corrupted file %s returned 200 OK with garbage content (wrote %d bytes, read %d bytes)", filePath, len(content), len(readContent))
					}
				} else {
					// Non-200 response means server correctly detected the corruption
					log.Printf("[failure-injection] corruption detection verified for %s (status=%d)", filePath, readResp.StatusCode)
				}
			}
		}
	}

	// Clean up
	w.deleteTestFile(ctx, filePath)

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

	req := w.newRequest(ctx, "COPY", srcPath, nil)
	if req == nil {
		return nil
	}
	req.Header.Set("Destination", w.baseURL+destPath)
	status := w.doReqDiscard(req)

	if status == http.StatusCreated || status == http.StatusNoContent {
		w.state.addFile(destPath, content)

		// Verify the copy has the same content
		getResp := w.doRequest(ctx, http.MethodGet, destPath, nil)
		if getResp == nil {
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
		if w.doRequestDiscard(ctx, http.MethodPut, filePath, bytes.NewReader(content)) != http.StatusCreated {
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

	lockReq := w.newRequest(ctx, "LOCK", filePath, strings.NewReader(lockBody))
	if lockReq == nil {
		return nil
	}
	lockReq.Header.Set("Content-Type", "application/xml")
	lockReq.Header.Set("Timeout", "Second-60")
	lockResp := w.doReq(lockReq)
	if lockResp == nil {
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
	unlockReq := w.newRequest(ctx, "UNLOCK", filePath, nil)
	if unlockReq == nil {
		return nil
	}
	unlockReq.Header.Set("Lock-Token", lockToken)
	unlockStatus := w.doReqDiscard(unlockReq)

	// 204 No Content is success for UNLOCK
	if unlockStatus != http.StatusNoContent && unlockStatus != http.StatusOK {
		log.Printf("[lock] UNLOCK returned %d for %s", unlockStatus, filePath)
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

	req := w.newRequest(ctx, "PROPPATCH", filePath, strings.NewReader(propBody))
	if req == nil {
		return nil
	}
	req.Header.Set("Content-Type", "application/xml")
	w.doReqDiscard(req)

	// PROPPATCH should return 207 Multi-Status
	// We don't verify the actual property change since it may not be persisted
	return nil
}

// ==================== Edge Cases ====================

// createFileExclusive tests that creating a file twice works correctly (second write overwrites)
func (w *Worker) createFileExclusive(ctx context.Context) error {
	name := randomName(w.rng) + "_excl.txt"
	filePath := "/" + name
	content1 := []byte("first content")
	content2 := []byte("second content")

	// First create
	if w.doRequestDiscard(ctx, http.MethodPut, filePath, bytes.NewReader(content1)) != http.StatusCreated {
		return nil // May have been created by another worker
	}
	w.state.addFile(filePath, content1)

	// Second create should overwrite
	status2 := w.doRequestDiscard(ctx, http.MethodPut, filePath, bytes.NewReader(content2))
	if status2 != http.StatusCreated && status2 != http.StatusNoContent {
		return nil
	}
	w.state.addFile(filePath, content2)

	// Verify the second content is what we read back
	getResp := w.doRequest(ctx, http.MethodGet, filePath, nil)
	if getResp == nil {
		return nil
	}
	defer getResp.Body.Close()
	readContent, _ := io.ReadAll(getResp.Body)

	if getResp.StatusCode == http.StatusNotFound {
		w.state.removeFile(filePath)
		return nil // Concurrent delete/restore
	}
	if getResp.StatusCode == http.StatusOK && !bytes.Equal(content2, readContent) {
		// Content mismatch - could be concurrent write, verify stability
		if !bytes.Equal(content1, readContent) {
			// Neither content1 nor content2 - concurrent write from another worker
			w.state.addFile(filePath, readContent)
		}
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

	status := w.doRequestDiscard(ctx, http.MethodPut, filePath, bytes.NewReader(newContent))
	if status != http.StatusCreated && status != http.StatusNoContent {
		return nil
	}
	w.state.addFile(filePath, newContent)

	// Verify the new content
	getResp := w.doRequest(ctx, http.MethodGet, filePath, nil)
	if getResp == nil {
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
	status := w.doRequestDiscard(ctx, http.MethodPut, filePath, bytes.NewReader([]byte{}))
	if status != http.StatusCreated && status != http.StatusNoContent {
		return nil
	}
	w.state.addFile(filePath, []byte{})

	// Verify the file exists and is empty
	getResp := w.doRequest(ctx, http.MethodGet, filePath, nil)
	if getResp == nil {
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
	if w.doRequestDiscard(ctx, http.MethodPut, file1, bytes.NewReader(content)) != http.StatusCreated {
		return nil
	}

	// Create second file with same content
	if w.doRequestDiscard(ctx, http.MethodPut, file2, bytes.NewReader(content)) != http.StatusCreated {
		return nil
	}

	// Query the database to check if both files share the same blob_id
	var blobID1, blobID2 sql.NullInt64
	err := w.injector.db.QueryRow(`SELECT blob_id FROM files WHERE path = ? AND deleted = 0 ORDER BY version DESC LIMIT 1`, file1).Scan(&blobID1)
	if err != nil {
		return nil
	}
	err = w.injector.db.QueryRow(`SELECT blob_id FROM files WHERE path = ? AND deleted = 0 ORDER BY version DESC LIMIT 1`, file2).Scan(&blobID2)
	if err != nil {
		return nil
	}

	// Both blob IDs should be valid (non-NULL) for files with content
	if !blobID1.Valid || !blobID2.Valid {
		// Files may have been deleted by concurrent restore - retry query once
		time.Sleep(10 * time.Millisecond)
		err = w.injector.db.QueryRow(`SELECT blob_id FROM files WHERE path = ? AND deleted = 0 ORDER BY version DESC LIMIT 1`, file1).Scan(&blobID1)
		if err != nil || !blobID1.Valid {
			// File was deleted, skip this test
			return nil
		}
		err = w.injector.db.QueryRow(`SELECT blob_id FROM files WHERE path = ? AND deleted = 0 ORDER BY version DESC LIMIT 1`, file2).Scan(&blobID2)
		if err != nil || !blobID2.Valid {
			// File was deleted, skip this test
			return nil
		}
	}

	if blobID1.Int64 != blobID2.Int64 {
		return fmt.Errorf("invariant #24 violated: identical content not deduplicated (%s blob=%d, %s blob=%d)", file1, blobID1.Int64, file2, blobID2.Int64)
	}

	// Clean up
	for _, f := range []string{file1, file2} {
		w.deleteTestFile(ctx, f)
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
	getResp := w.doRequest(ctx, http.MethodGet, filePath, nil)
	if getResp == nil {
		return nil
	}
	defer getResp.Body.Close()
	readContent, _ := io.ReadAll(getResp.Body)

	if getResp.StatusCode != http.StatusOK {
		return nil
	}

	// Read again and verify content hasn't changed
	getResp2 := w.doRequest(ctx, http.MethodGet, filePath, nil)
	if getResp2 == nil {
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
	if w.doRequestDiscard(ctx, http.MethodPut, filePath, bytes.NewReader(originalContent)) != http.StatusCreated {
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

	// The read should return either original or new content (not garbage/torn data)
	// Valid scenarios:
	// - Read got original content (read completed before write took effect)
	// - Read got new content (write completed before read)
	// - Read got empty content (file deleted by concurrent restore then recreated)
	// Invalid scenarios:
	// - Read got partial/torn content (neither original nor new)
	if content, ok := <-readResult; ok {
		if !bytes.Equal(content, originalContent) && !bytes.Equal(content, newContent) && len(content) > 0 {
			// Content is not empty but doesn't match either expected value.
			// This could be a torn read or data corruption.
			// To distinguish from concurrent restore, verify the content is actually garbage:
			// If it's a prefix/suffix of either expected content, it's a torn read (bug).
			isPartialOriginal := len(content) < len(originalContent) && bytes.HasPrefix(originalContent, content)
			isPartialNew := len(content) < len(newContent) && bytes.HasPrefix(newContent, content)

			if isPartialOriginal || isPartialNew {
				return fmt.Errorf("invariant #27 violated: torn read detected for %s (got %d bytes, appears to be partial content)", filePath, len(content))
			}

			// Content doesn't match anything expected - could be from restore or corruption
			// Re-read to check if this is the stable state
			verifyResp := w.doRequest(ctx, http.MethodGet, filePath, nil)
			if verifyResp != nil {
				defer verifyResp.Body.Close()
				verifyContent, _ := io.ReadAll(verifyResp.Body)
				if verifyResp.StatusCode == http.StatusOK && !bytes.Equal(content, verifyContent) {
					return fmt.Errorf("invariant #27 violated: concurrent read returned unstable content for %s", filePath)
				}
			}
		}
	}

	// Clean up
	w.deleteTestFile(ctx, filePath)

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
	// Use 3x the TTL to allow for GC timing jitter and loop intervals
	cutoff := time.Now().Add(-3 * uploadTTL).Unix()

	var orphanedCount int
	err := w.injector.db.QueryRow(`
		SELECT COUNT(*) FROM blobs
		WHERE local_written = 0
		AND local_deleting = 0
		AND creation_time < ?`, cutoff).Scan(&orphanedCount)
	if err != nil {
		return nil
	}

	// If there are many orphaned blobs well past the TTL, GC is not working
	// Threshold: more than 500 orphaned blobs is a strong signal of GC failure
	if orphanedCount > 500 {
		return fmt.Errorf("invariant #30 violated: %d orphaned blobs older than 3x upload TTL, GC may not be running", orphanedCount)
	}

	// Check that old file versions are being compacted
	ttl := 24 * time.Hour // Default from main.go
	// Use 2x the TTL to allow for compaction timing
	fileCutoff := time.Now().Add(-2 * ttl).Unix()

	var oldVersions int
	err = w.injector.db.QueryRow(`
		SELECT COUNT(*) FROM files f
		WHERE f.created_at < ?
		AND (f.deleted = 1 OR EXISTS (SELECT 1 FROM files f2 WHERE f2.path = f.path AND f2.version > f.version))`, fileCutoff).Scan(&oldVersions)
	if err != nil {
		return nil
	}

	// If there are many old versions well past the TTL, compaction is not working
	// Threshold: more than 10000 old file versions is a strong signal
	if oldVersions > 10000 {
		return fmt.Errorf("invariant #31 violated: %d old file versions older than 2x TTL, compaction may not be running", oldVersions)
	}

	return nil
}

// verifyIntegrityCheckLoop verifies the background integrity check is working
func (w *Worker) verifyIntegrityCheckLoop(ctx context.Context) error {
	if w.injector == nil || w.injector.db == nil {
		return nil
	}

	// Check for blobs that should have been integrity-checked but haven't been
	// The default integrity check interval is 24 hours, so we use 3x that
	integrityCheckInterval := 24 * time.Hour
	overdueThreshold := time.Now().Add(-3 * integrityCheckInterval).Unix()

	var overdueCount int
	err := w.injector.db.QueryRow(`
		SELECT COUNT(*) FROM blobs
		WHERE local_written = 1
		AND remote_written = 1
		AND local_deleting = 0
		AND checksum IS NOT NULL
		AND (last_integrity_check IS NULL OR last_integrity_check < ?)`, overdueThreshold).Scan(&overdueCount)
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

	// If most eligible blobs are overdue for integrity check, the loop may not be working
	// Only report if there are enough blobs to make this meaningful
	if totalEligible > 100 && overdueCount > totalEligible*9/10 {
		return fmt.Errorf("invariant #32 violated: %d of %d blobs overdue for integrity check (>3x check interval), integrity loop may not be running", overdueCount, totalEligible)
	}

	// Check for blobs that need re-download (marked as corrupted or missing)
	// These should be getting re-downloaded if remote sync is enabled
	var needsRedownload int
	err = w.injector.db.QueryRow(`
		SELECT COUNT(*) FROM blobs
		WHERE local_written = 0
		AND remote_written = 1
		AND remote_deleted = 0`).Scan(&needsRedownload)
	if err != nil {
		return nil
	}

	// If many blobs need re-download for an extended time, something is wrong
	// This is informational - we can't easily check "how long" they've been waiting
	// without additional timestamp tracking
	if needsRedownload > 100 {
		log.Printf("[integrity-check] %d blobs awaiting re-download from remote", needsRedownload)
	}

	return nil
}
