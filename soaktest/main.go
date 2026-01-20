package main

import (
	"bytes"
	"context"
	"encoding/xml"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	targetURL   = flag.String("url", "http://localhost:8080", "target server URL")
	duration    = flag.Duration("duration", 0, "test duration (0 = run forever)")
	concurrency = flag.Int("concurrency", 4, "number of concurrent workers")
	seed        = flag.Int64("seed", 0, "random seed (0 = use current time)")
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
