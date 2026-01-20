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

func TestCRUD(t *testing.T) {
	db, blobsDir := initTestState(t)
	mux := newRouter(db, blobsDir)
	ts := httptest.NewServer(mux)
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
