package main

import (
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHistoricalBrowse(t *testing.T) {
	db, blobsDir := initTestState(t)
	server := NewServer(db, blobsDir, nil)
	ts := httptest.NewServer(server)
	t.Cleanup(func() { ts.Close() })

	getBrowse := func(path, timestamp string) (int, string) {
		u := ts.URL + "/browse" + path
		if timestamp != "" {
			u += "?at=" + url.QueryEscape(timestamp)
		}
		resp, err := http.Get(u)
		require.NoError(t, err)
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(body)
	}

	t.Run("shows files at historical timestamp", func(t *testing.T) {
		t1 := time.Now().Unix() - 100
		_, err := db.Exec(`INSERT INTO files (created_at, version, path, mode, mod_time, is_dir, deleted) VALUES (?, 1, '/hist-test.txt', 0644, ?, 0, 0)`, t1, t1)
		require.NoError(t, err)

		// Delete file at t2
		t2 := t1 + 50
		_, err = db.Exec(`INSERT INTO files (created_at, version, path, mode, mod_time, is_dir, deleted) VALUES (?, 2, '/hist-test.txt', 0644, ?, 0, 1)`, t2, t2)
		require.NoError(t, err)

		// Browse at t1+25 (before deletion)
		browseTime := time.Unix(t1+25, 0).UTC().Format(time.RFC3339)
		code, body := getBrowse("", browseTime)
		assert.Equal(t, http.StatusOK, code)
		assert.Contains(t, body, "hist-test.txt")
		assert.Contains(t, body, "Viewing:")
	})

	t.Run("does not show files created after timestamp", func(t *testing.T) {
		t1 := time.Now().Unix() - 50
		_, err := db.Exec(`INSERT INTO files (created_at, version, path, mode, mod_time, is_dir, deleted) VALUES (?, 1, '/future-file.txt', 0644, ?, 0, 0)`, t1, t1)
		require.NoError(t, err)

		// Browse at a time before the file was created
		browseTime := time.Unix(t1-100, 0).UTC().Format(time.RFC3339)
		code, body := getBrowse("", browseTime)
		assert.Equal(t, http.StatusOK, code)
		assert.NotContains(t, body, "future-file.txt")
	})

	t.Run("rejects future timestamp", func(t *testing.T) {
		futureTime := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
		code, body := getBrowse("", futureTime)
		assert.Equal(t, http.StatusBadRequest, code)
		assert.Contains(t, body, "cannot browse future time")
	})

	t.Run("rejects timestamp beyond TTL", func(t *testing.T) {
		oldTime := time.Now().Add(-*ttl - time.Hour).UTC().Format(time.RFC3339)
		code, body := getBrowse("", oldTime)
		assert.Equal(t, http.StatusBadRequest, code)
		assert.Contains(t, body, "cannot browse beyond retention period")
	})

	t.Run("rejects invalid timestamp format", func(t *testing.T) {
		code, body := getBrowse("", "not-a-date")
		assert.Equal(t, http.StatusBadRequest, code)
		assert.Contains(t, body, "invalid timestamp format")
	})

	t.Run("shows exit time travel link in historical mode", func(t *testing.T) {
		browseTime := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
		code, body := getBrowse("", browseTime)
		assert.Equal(t, http.StatusOK, code)
		assert.Contains(t, body, "Exit Time Travel")
		assert.NotContains(t, body, "Upload Files")
	})

	t.Run("shows upload button in normal mode", func(t *testing.T) {
		code, body := getBrowse("", "")
		assert.Equal(t, http.StatusOK, code)
		assert.Contains(t, body, "Upload Files")
		assert.NotContains(t, body, "Exit Time Travel")
	})
}

func TestRestoreFileAPI(t *testing.T) {
	db, blobsDir := initTestState(t)
	server := NewServer(db, blobsDir, nil)
	ts := httptest.NewServer(server)
	t.Cleanup(func() { ts.Close() })

	postRestoreFile := func(timestamp, path string) (int, string) {
		resp, err := http.Post(ts.URL+"/api/restore-file", "application/x-www-form-urlencoded",
			strings.NewReader(url.Values{"timestamp": {timestamp}, "path": {path}}.Encode()))
		require.NoError(t, err)
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(body)
	}

	t.Run("restores single deleted file", func(t *testing.T) {
		t1 := time.Now().Unix() - 100
		_, err := db.Exec(`INSERT INTO files (created_at, version, path, mode, mod_time, is_dir, deleted) VALUES (?, 1, '/restore-single.txt', 0644, ?, 0, 0)`, t1, t1)
		require.NoError(t, err)

		t2 := t1 + 10
		_, err = db.Exec(`INSERT INTO files (created_at, version, path, mode, mod_time, is_dir, deleted) VALUES (?, 2, '/restore-single.txt', 0644, ?, 0, 1)`, t2, t2)
		require.NoError(t, err)

		restoreTime := time.Unix(t1+5, 0).UTC().Format(time.RFC3339)
		code, body := postRestoreFile(restoreTime, "/restore-single.txt")
		assert.Equal(t, http.StatusOK, code)
		assert.Contains(t, body, "success")

		var deleted int
		err = db.QueryRow(`SELECT deleted FROM files WHERE path = '/restore-single.txt' ORDER BY version DESC LIMIT 1`).Scan(&deleted)
		require.NoError(t, err)
		assert.Equal(t, 0, deleted, "file should be restored")
	})

	t.Run("requires path parameter", func(t *testing.T) {
		restoreTime := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
		code, body := postRestoreFile(restoreTime, "")
		assert.Equal(t, http.StatusBadRequest, code)
		assert.Contains(t, body, "path is required")
	})

	t.Run("requires timestamp parameter", func(t *testing.T) {
		code, body := postRestoreFile("", "/some/path.txt")
		assert.Equal(t, http.StatusBadRequest, code)
		assert.Contains(t, body, "timestamp is required")
	})

	t.Run("rejects future timestamp", func(t *testing.T) {
		futureTime := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
		code, body := postRestoreFile(futureTime, "/some/path.txt")
		assert.Equal(t, http.StatusBadRequest, code)
		assert.Contains(t, body, "cannot restore from a future time")
	})

	t.Run("rejects timestamp beyond TTL", func(t *testing.T) {
		oldTime := time.Now().Add(-*ttl - time.Hour).UTC().Format(time.RFC3339)
		code, body := postRestoreFile(oldTime, "/some/path.txt")
		assert.Equal(t, http.StatusBadRequest, code)
		assert.Contains(t, body, "cannot restore beyond the retention period")
	})
}

func TestDownloadHistoricalAPI(t *testing.T) {
	db, blobsDir := initTestState(t)
	server := NewServer(db, blobsDir, nil)
	ts := httptest.NewServer(server)
	t.Cleanup(func() { ts.Close() })

	getDownloadHistorical := func(path, timestamp string) (int, string, http.Header) {
		u := ts.URL + "/api/download-historical?path=" + url.QueryEscape(path) + "&at=" + url.QueryEscape(timestamp)
		resp, err := http.Get(u)
		require.NoError(t, err)
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(body), resp.Header
	}

	t.Run("returns unavailable when no remote configured", func(t *testing.T) {
		// Create blob with content (local only, no remote)
		content := []byte("historical content version 1")
		blobID := uuid.New().String()
		now := time.Now().Unix()

		_, err := db.Exec(`INSERT INTO blobs (id, creation_time, modified_time, local_written, size) VALUES (?, ?, ?, 1, ?)`, blobID, now, now, len(content))
		require.NoError(t, err)

		blobPath := blobFilePath(blobsDir, blobID)
		require.NoError(t, os.WriteFile(blobPath, content, 0644))

		t1 := time.Now().Unix() - 100
		_, err = db.Exec(`INSERT INTO files (created_at, version, path, mode, mod_time, is_dir, deleted, blob_id) VALUES (?, 1, '/download-test.txt', 0644, ?, 0, 0, ?)`, t1, t1, blobID)
		require.NoError(t, err)

		// Historical downloads require remote storage; without rclone configured, content is unavailable
		downloadTime := time.Unix(t1+10, 0).UTC().Format(time.RFC3339)
		code, body, _ := getDownloadHistorical("/download-test.txt", downloadTime)
		assert.Equal(t, http.StatusNotFound, code)
		assert.Contains(t, body, "file content not available")
	})

	t.Run("returns 404 for file not found at timestamp", func(t *testing.T) {
		t1 := time.Now().Unix() - 100
		_, err := db.Exec(`INSERT INTO files (created_at, version, path, mode, mod_time, is_dir, deleted) VALUES (?, 1, '/created-later.txt', 0644, ?, 0, 0)`, t1, t1)
		require.NoError(t, err)

		// Try to download before file existed
		downloadTime := time.Unix(t1-50, 0).UTC().Format(time.RFC3339)
		code, body, _ := getDownloadHistorical("/created-later.txt", downloadTime)
		assert.Equal(t, http.StatusNotFound, code)
		assert.Contains(t, body, "file not found at timestamp")
	})

	t.Run("requires path parameter", func(t *testing.T) {
		downloadTime := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
		code, body, _ := getDownloadHistorical("", downloadTime)
		assert.Equal(t, http.StatusBadRequest, code)
		assert.Contains(t, body, "at and path parameters required")
	})

	t.Run("requires timestamp parameter", func(t *testing.T) {
		code, body, _ := getDownloadHistorical("/some/path.txt", "")
		assert.Equal(t, http.StatusBadRequest, code)
		assert.Contains(t, body, "at and path parameters required")
	})

	t.Run("rejects future timestamp", func(t *testing.T) {
		futureTime := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
		code, body, _ := getDownloadHistorical("/some/path.txt", futureTime)
		assert.Equal(t, http.StatusBadRequest, code)
		assert.Contains(t, body, "cannot download from future time")
	})

	t.Run("rejects timestamp beyond TTL", func(t *testing.T) {
		oldTime := time.Now().Add(-*ttl - time.Hour).UTC().Format(time.RFC3339)
		code, body, _ := getDownloadHistorical("/some/path.txt", oldTime)
		assert.Equal(t, http.StatusBadRequest, code)
		assert.Contains(t, body, "cannot download beyond retention period")
	})

	t.Run("streams file from remote storage", func(t *testing.T) {
		rc, remoteDir := initRcloneTest(t, 1)
		db2, _ := initTestState(t)
		server2 := NewServer(db2, blobsDir, rc)
		ts2 := httptest.NewServer(server2)
		t.Cleanup(func() { ts2.Close() })

		content := []byte("historical content from remote")
		blobID := uuid.New().String()
		now := time.Now().Unix()

		// Create blob marked as remote_written (not local)
		_, err := db2.Exec(`INSERT INTO blobs (id, creation_time, modified_time, local_written, remote_written, size) VALUES (?, ?, ?, 0, 1, ?)`, blobID, now, now, len(content))
		require.NoError(t, err)

		// Write blob to the "remote" directory with expected path structure
		blobDir := filepath.Join(remoteDir, blobID[:2])
		require.NoError(t, os.MkdirAll(blobDir, 0755))
		blobPath := filepath.Join(blobDir, blobID[2:])
		require.NoError(t, os.WriteFile(blobPath, content, 0644))

		// Create file record pointing to the blob
		t1 := time.Now().Unix() - 100
		_, err = db2.Exec(`INSERT INTO files (created_at, version, path, mode, mod_time, is_dir, deleted, blob_id) VALUES (?, 1, '/remote-file.txt', 0644, ?, 0, 0, ?)`, t1, t1, blobID)
		require.NoError(t, err)

		// Download historical file
		downloadTime := time.Unix(t1+10, 0).UTC().Format(time.RFC3339)
		u := ts2.URL + "/api/download-historical?path=" + url.QueryEscape("/remote-file.txt") + "&at=" + url.QueryEscape(downloadTime)
		resp, err := http.Get(u)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, "application/octet-stream", resp.Header.Get("Content-Type"))
		assert.Contains(t, resp.Header.Get("Content-Disposition"), "remote-file.txt")

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Equal(t, string(content), string(body))
	})
}

func TestFormatBytes(t *testing.T) {
	testCases := []struct {
		input    int64
		expected string
	}{
		{0, "0 B"},
		{1, "1 B"},
		{512, "512 B"},
		{1023, "1023 B"},
		{1024, "1.0 KB"},
		{1536, "1.5 KB"},
		{2048, "2.0 KB"},
		{1048576, "1.0 MB"},
		{1572864, "1.5 MB"},
		{1073741824, "1.0 GB"},
		{1610612736, "1.5 GB"},
		{1099511627776, "1.0 TB"},
		{1125899906842624, "1.0 PB"},
		{1152921504606846976, "1.0 EB"},
		{math.MaxInt64, "8.0 EB"},
	}

	for _, tc := range testCases {
		t.Run(tc.expected, func(t *testing.T) {
			result := formatBytes(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}
