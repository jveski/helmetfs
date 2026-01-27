package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWebDAVBasics(t *testing.T) {
	db, blobsDir := initTestState(t)
	server := NewServer(db, blobsDir, nil)
	ts := httptest.NewServer(server)
	t.Cleanup(func() { ts.Close() })

	client := &http.Client{}

	// Create a file
	req, err := http.NewRequest(http.MethodPut, ts.URL+"/test.txt", strings.NewReader("hello world"))
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	// Read the file
	resp, err = client.Get(ts.URL + "/test.txt")
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "hello world", string(body))

	// Update the file
	req, err = http.NewRequest(http.MethodPut, ts.URL+"/test.txt", strings.NewReader("updated content"))
	require.NoError(t, err)
	resp, err = client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	// Verify updated content
	resp, err = client.Get(ts.URL + "/test.txt")
	require.NoError(t, err)
	body, err = io.ReadAll(resp.Body)
	resp.Body.Close()
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "updated content", string(body))

	// Delete the file
	req, err = http.NewRequest(http.MethodDelete, ts.URL+"/test.txt", nil)
	require.NoError(t, err)
	resp, err = client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)

	// Verify file is gone
	resp, err = client.Get(ts.URL + "/test.txt")
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestWebDAVDeleteDirectoryWithFile(t *testing.T) {
	db, blobsDir := initTestState(t)
	server := NewServer(db, blobsDir, nil)
	ts := httptest.NewServer(server)
	t.Cleanup(func() { ts.Close() })

	client := &http.Client{}

	// Create a subdirectory
	req, err := http.NewRequest("MKCOL", ts.URL+"/subdir", nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	// Create a file in the subdirectory
	req, err = http.NewRequest(http.MethodPut, ts.URL+"/subdir/file.txt", strings.NewReader("content in subdir"))
	require.NoError(t, err)
	resp, err = client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	// Verify the file exists
	resp, err = client.Get(ts.URL + "/subdir/file.txt")
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "content in subdir", string(body))

	// Delete the directory
	req, err = http.NewRequest(http.MethodDelete, ts.URL+"/subdir", nil)
	require.NoError(t, err)
	resp, err = client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)

	// Verify directory is gone
	req, err = http.NewRequest("PROPFIND", ts.URL+"/subdir", nil)
	require.NoError(t, err)
	resp, err = client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)

	// Verify file in directory is also gone
	resp, err = client.Get(ts.URL + "/subdir/file.txt")
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestWebDAVCopy(t *testing.T) {
	db, blobsDir := initTestState(t)
	server := NewServer(db, blobsDir, nil)
	ts := httptest.NewServer(server)
	t.Cleanup(func() { ts.Close() })

	client := &http.Client{}

	// Create a file
	req, err := http.NewRequest(http.MethodPut, ts.URL+"/source.txt", strings.NewReader("copy me"))
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	// COPY the file
	req, err = http.NewRequest("COPY", ts.URL+"/source.txt", nil)
	require.NoError(t, err)
	req.Header.Set("Destination", ts.URL+"/dest.txt")
	resp, err = client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	// Verify source still exists
	resp, err = client.Get(ts.URL + "/source.txt")
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "copy me", string(body))

	// Verify destination exists with same content
	resp, err = client.Get(ts.URL + "/dest.txt")
	require.NoError(t, err)
	body, err = io.ReadAll(resp.Body)
	resp.Body.Close()
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "copy me", string(body))
}

func TestWebDAVMove(t *testing.T) {
	db, blobsDir := initTestState(t)
	server := NewServer(db, blobsDir, nil)
	ts := httptest.NewServer(server)
	t.Cleanup(func() { ts.Close() })

	client := &http.Client{}

	// Create a file
	req, err := http.NewRequest(http.MethodPut, ts.URL+"/moveme.txt", strings.NewReader("move me"))
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	// MOVE the file
	req, err = http.NewRequest("MOVE", ts.URL+"/moveme.txt", nil)
	require.NoError(t, err)
	req.Header.Set("Destination", ts.URL+"/moved.txt")
	resp, err = client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	// Verify source is gone
	resp, err = client.Get(ts.URL + "/moveme.txt")
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)

	// Verify destination exists with content
	resp, err = client.Get(ts.URL + "/moved.txt")
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "move me", string(body))
}

func TestWebDAVCopyDirectory(t *testing.T) {
	db, blobsDir := initTestState(t)
	server := NewServer(db, blobsDir, nil)
	ts := httptest.NewServer(server)
	t.Cleanup(func() { ts.Close() })

	client := &http.Client{}

	// Create a directory structure
	req, err := http.NewRequest("MKCOL", ts.URL+"/srcdir", nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	// Create a file in the directory
	req, err = http.NewRequest(http.MethodPut, ts.URL+"/srcdir/file.txt", strings.NewReader("dir content"))
	require.NoError(t, err)
	resp, err = client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	// COPY the directory
	req, err = http.NewRequest("COPY", ts.URL+"/srcdir", nil)
	require.NoError(t, err)
	req.Header.Set("Destination", ts.URL+"/dstdir")
	resp, err = client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	// Verify source still exists
	resp, err = client.Get(ts.URL + "/srcdir/file.txt")
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "dir content", string(body))

	// Verify destination exists
	resp, err = client.Get(ts.URL + "/dstdir/file.txt")
	require.NoError(t, err)
	body, err = io.ReadAll(resp.Body)
	resp.Body.Close()
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "dir content", string(body))
}

func TestWebDAVMoveDirectory(t *testing.T) {
	db, blobsDir := initTestState(t)
	server := NewServer(db, blobsDir, nil)
	ts := httptest.NewServer(server)
	t.Cleanup(func() { ts.Close() })

	client := &http.Client{}

	// Create a directory structure
	req, err := http.NewRequest("MKCOL", ts.URL+"/movedir", nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	// Create a file in the directory
	req, err = http.NewRequest(http.MethodPut, ts.URL+"/movedir/file.txt", strings.NewReader("move dir content"))
	require.NoError(t, err)
	resp, err = client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	// MOVE the directory
	req, err = http.NewRequest("MOVE", ts.URL+"/movedir", nil)
	require.NoError(t, err)
	req.Header.Set("Destination", ts.URL+"/moveddir")
	resp, err = client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	// Verify source is gone
	resp, err = client.Get(ts.URL + "/movedir/file.txt")
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)

	// Verify destination exists
	resp, err = client.Get(ts.URL + "/moveddir/file.txt")
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "move dir content", string(body))
}

func TestWebDAVPropfind(t *testing.T) {
	db, blobsDir := initTestState(t)
	server := NewServer(db, blobsDir, nil)
	ts := httptest.NewServer(server)
	t.Cleanup(func() { ts.Close() })

	client := &http.Client{}

	// Create a directory with files
	req, err := http.NewRequest("MKCOL", ts.URL+"/propdir", nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	// Create files
	for _, name := range []string{"a.txt", "b.txt", "c.txt"} {
		req, err = http.NewRequest(http.MethodPut, ts.URL+"/propdir/"+name, strings.NewReader("content"))
		require.NoError(t, err)
		resp, err = client.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
	}

	// PROPFIND with depth 0 (just the directory)
	req, err = http.NewRequest("PROPFIND", ts.URL+"/propdir", nil)
	require.NoError(t, err)
	req.Header.Set("Depth", "0")
	resp, err = client.Do(req)
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, http.StatusMultiStatus, resp.StatusCode)
	// Should contain the directory but not the files
	assert.Contains(t, string(body), "/propdir")

	// PROPFIND with depth 1 (directory and immediate children)
	req, err = http.NewRequest("PROPFIND", ts.URL+"/propdir", nil)
	require.NoError(t, err)
	req.Header.Set("Depth", "1")
	resp, err = client.Do(req)
	require.NoError(t, err)
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, http.StatusMultiStatus, resp.StatusCode)
	// Should contain all files
	assert.Contains(t, string(body), "a.txt")
	assert.Contains(t, string(body), "b.txt")
	assert.Contains(t, string(body), "c.txt")
}

func TestWebDAVCopyOverwrite(t *testing.T) {
	db, blobsDir := initTestState(t)
	server := NewServer(db, blobsDir, nil)
	ts := httptest.NewServer(server)
	t.Cleanup(func() { ts.Close() })

	client := &http.Client{}

	// Create source file
	req, err := http.NewRequest(http.MethodPut, ts.URL+"/src.txt", strings.NewReader("source"))
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	// Create destination file
	req, err = http.NewRequest(http.MethodPut, ts.URL+"/dst.txt", strings.NewReader("original dest"))
	require.NoError(t, err)
	resp, err = client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	// COPY with Overwrite: T (should overwrite)
	req, err = http.NewRequest("COPY", ts.URL+"/src.txt", nil)
	require.NoError(t, err)
	req.Header.Set("Destination", ts.URL+"/dst.txt")
	req.Header.Set("Overwrite", "T")
	resp, err = client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode) // 204 for overwrite

	// Verify destination was overwritten
	resp, err = client.Get(ts.URL + "/dst.txt")
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, "source", string(body))
}

func TestWebDAVMoveNoOverwrite(t *testing.T) {
	db, blobsDir := initTestState(t)
	server := NewServer(db, blobsDir, nil)
	ts := httptest.NewServer(server)
	t.Cleanup(func() { ts.Close() })

	client := &http.Client{}

	// Create source file
	req, err := http.NewRequest(http.MethodPut, ts.URL+"/movesrc.txt", strings.NewReader("source"))
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	// Create destination file
	req, err = http.NewRequest(http.MethodPut, ts.URL+"/movedst.txt", strings.NewReader("original dest"))
	require.NoError(t, err)
	resp, err = client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	// MOVE with Overwrite: F (should fail)
	req, err = http.NewRequest("MOVE", ts.URL+"/movesrc.txt", nil)
	require.NoError(t, err)
	req.Header.Set("Destination", ts.URL+"/movedst.txt")
	req.Header.Set("Overwrite", "F")
	resp, err = client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusPreconditionFailed, resp.StatusCode)

	// Verify source still exists
	resp, err = client.Get(ts.URL + "/movesrc.txt")
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Verify destination unchanged
	resp, err = client.Get(ts.URL + "/movedst.txt")
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, "original dest", string(body))
}

func TestWebDAVCopyToNonexistentDirectory(t *testing.T) {
	db, blobsDir := initTestState(t)
	server := NewServer(db, blobsDir, nil)
	ts := httptest.NewServer(server)
	t.Cleanup(func() { ts.Close() })

	client := &http.Client{}

	// Create source file
	req, err := http.NewRequest(http.MethodPut, ts.URL+"/copysrc.txt", strings.NewReader("source"))
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	// COPY to nonexistent directory should fail
	req, err = http.NewRequest("COPY", ts.URL+"/copysrc.txt", nil)
	require.NoError(t, err)
	req.Header.Set("Destination", ts.URL+"/nonexistent/dest.txt")
	resp, err = client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusConflict, resp.StatusCode)
}
