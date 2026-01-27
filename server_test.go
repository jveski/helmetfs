package main

import (
	"database/sql"
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

func TestQueryDirectoryEntriesPointInTimeConsistency(t *testing.T) {
	db, blobsDir := initTestState(t)
	server := NewServer(db, blobsDir, nil)
	ctx := t.Context()

	// Base timestamp: 1000 seconds ago
	baseTime := time.Now().Unix() - 1000

	// Create a directory with multiple files that have different version histories.
	// This tests that queryDirectoryEntries returns a coherent snapshot where
	// all files reflect the state at the requested timestamp.
	//
	// Timeline:
	//   t=baseTime:      file-a.txt v1 created, file-b.txt v1 created
	//   t=baseTime+100:  file-a.txt v2 (modified), file-c.txt v1 created
	//   t=baseTime+200:  file-b.txt v2 (deleted)
	//   t=baseTime+300:  file-a.txt v3 (modified again)
	//
	// Expected snapshots:
	//   at baseTime+50:  file-a v1, file-b v1
	//   at baseTime+150: file-a v2, file-b v1, file-c v1
	//   at baseTime+250: file-a v2, file-c v1 (file-b deleted)
	//   at baseTime+350: file-a v3, file-c v1

	// Create blobs for each version
	blobA1 := uuid.New().String()
	blobA2 := uuid.New().String()
	blobA3 := uuid.New().String()
	blobB1 := uuid.New().String()
	blobC1 := uuid.New().String()

	// Insert blobs
	for _, blob := range []struct {
		id   string
		size int64
	}{
		{blobA1, 100},
		{blobA2, 200},
		{blobA3, 300},
		{blobB1, 150},
		{blobC1, 250},
	} {
		_, err := db.Exec(`INSERT INTO blobs (id, creation_time, modified_time, local_written, size) VALUES (?, ?, ?, 1, ?)`,
			blob.id, baseTime, baseTime, blob.size)
		require.NoError(t, err)
	}

	// Insert file versions with precise timestamps
	fileVersions := []struct {
		createdAt int64
		version   int
		path      string
		deleted   int
		blobID    string
	}{
		// file-a.txt: created at baseTime, modified at baseTime+100, modified again at baseTime+300
		{baseTime, 1, "/pit-test/file-a.txt", 0, blobA1},
		{baseTime + 100, 2, "/pit-test/file-a.txt", 0, blobA2},
		{baseTime + 300, 3, "/pit-test/file-a.txt", 0, blobA3},

		// file-b.txt: created at baseTime, deleted at baseTime+200
		{baseTime, 1, "/pit-test/file-b.txt", 0, blobB1},
		{baseTime + 200, 2, "/pit-test/file-b.txt", 1, blobB1},

		// file-c.txt: created at baseTime+100
		{baseTime + 100, 1, "/pit-test/file-c.txt", 0, blobC1},
	}

	for _, fv := range fileVersions {
		_, err := db.Exec(`INSERT INTO files (created_at, version, path, mode, mod_time, is_dir, deleted, blob_id)
			VALUES (?, ?, ?, 0644, ?, 0, ?, ?)`,
			fv.createdAt, fv.version, fv.path, fv.createdAt, fv.deleted, fv.blobID)
		require.NoError(t, err)
	}

	t.Run("snapshot at baseTime+50 shows file-a v1 and file-b v1", func(t *testing.T) {
		rows, err := server.queryDirectoryEntries(ctx, "/pit-test/", baseTime+50)
		require.NoError(t, err)
		defer rows.Close()

		entries := collectEntries(t, rows)
		require.Len(t, entries, 2)

		// file-a should be at v1 (size 100)
		assertEntry(t, entries, "/pit-test/file-a.txt", 100, false)
		// file-b should be at v1 (size 150)
		assertEntry(t, entries, "/pit-test/file-b.txt", 150, false)
	})

	t.Run("snapshot at baseTime+150 shows file-a v2, file-b v1, file-c v1", func(t *testing.T) {
		rows, err := server.queryDirectoryEntries(ctx, "/pit-test/", baseTime+150)
		require.NoError(t, err)
		defer rows.Close()

		entries := collectEntries(t, rows)
		require.Len(t, entries, 3)

		// file-a should be at v2 (size 200)
		assertEntry(t, entries, "/pit-test/file-a.txt", 200, false)
		// file-b should still be at v1 (size 150)
		assertEntry(t, entries, "/pit-test/file-b.txt", 150, false)
		// file-c should be at v1 (size 250)
		assertEntry(t, entries, "/pit-test/file-c.txt", 250, false)
	})

	t.Run("snapshot at baseTime+250 shows file-a v2, file-c v1 (file-b deleted)", func(t *testing.T) {
		rows, err := server.queryDirectoryEntries(ctx, "/pit-test/", baseTime+250)
		require.NoError(t, err)
		defer rows.Close()

		entries := collectEntries(t, rows)
		// In historical mode, deleted files are returned but marked as deleted
		// The handler filters them out, but queryDirectoryEntries returns them
		require.Len(t, entries, 3)

		assertEntry(t, entries, "/pit-test/file-a.txt", 200, false)
		assertEntry(t, entries, "/pit-test/file-b.txt", 150, true) // deleted
		assertEntry(t, entries, "/pit-test/file-c.txt", 250, false)
	})

	t.Run("snapshot at baseTime+350 shows file-a v3, file-c v1", func(t *testing.T) {
		rows, err := server.queryDirectoryEntries(ctx, "/pit-test/", baseTime+350)
		require.NoError(t, err)
		defer rows.Close()

		entries := collectEntries(t, rows)
		require.Len(t, entries, 3)

		// file-a should be at v3 (size 300)
		assertEntry(t, entries, "/pit-test/file-a.txt", 300, false)
		assertEntry(t, entries, "/pit-test/file-b.txt", 150, true) // still deleted
		assertEntry(t, entries, "/pit-test/file-c.txt", 250, false)
	})

	t.Run("current view (atUnix=0) shows only latest non-deleted files", func(t *testing.T) {
		rows, err := server.queryDirectoryEntries(ctx, "/pit-test/", 0)
		require.NoError(t, err)
		defer rows.Close()

		entries := collectEntries(t, rows)
		require.Len(t, entries, 2)

		// file-a should be at v3 (size 300)
		assertEntry(t, entries, "/pit-test/file-a.txt", 300, false)
		// file-c should be at v1 (size 250)
		assertEntry(t, entries, "/pit-test/file-c.txt", 250, false)
		// file-b is deleted and should not appear
		for _, e := range entries {
			assert.NotEqual(t, "/pit-test/file-b.txt", e.path)
		}
	})

	t.Run("timestamp before any files exist returns empty", func(t *testing.T) {
		rows, err := server.queryDirectoryEntries(ctx, "/pit-test/", baseTime-100)
		require.NoError(t, err)
		defer rows.Close()

		entries := collectEntries(t, rows)
		assert.Empty(t, entries)
	})

	t.Run("point-in-time view is coherent across entire directory tree", func(t *testing.T) {
		// Create a nested directory structure with files at different timestamps
		// to verify the snapshot is consistent across the entire tree

		// Create blobs
		blobRoot := uuid.New().String()
		blobSub := uuid.New().String()
		_, err := db.Exec(`INSERT INTO blobs (id, creation_time, modified_time, local_written, size) VALUES (?, ?, ?, 1, 500)`,
			blobRoot, baseTime, baseTime)
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO blobs (id, creation_time, modified_time, local_written, size) VALUES (?, ?, ?, 1, 600)`,
			blobSub, baseTime+50, baseTime+50)
		require.NoError(t, err)

		// Create directory
		_, err = db.Exec(`INSERT INTO files (created_at, version, path, mode, mod_time, is_dir, deleted) VALUES (?, 1, '/coherent-test', 0755, ?, 1, 0)`,
			baseTime, baseTime)
		require.NoError(t, err)

		// Create file at root
		_, err = db.Exec(`INSERT INTO files (created_at, version, path, mode, mod_time, is_dir, deleted, blob_id) VALUES (?, 1, '/coherent-test/root.txt', 0644, ?, 0, 0, ?)`,
			baseTime, baseTime, blobRoot)
		require.NoError(t, err)

		// Create subdirectory at baseTime+25
		_, err = db.Exec(`INSERT INTO files (created_at, version, path, mode, mod_time, is_dir, deleted) VALUES (?, 1, '/coherent-test/subdir', 0755, ?, 1, 0)`,
			baseTime+25, baseTime+25)
		require.NoError(t, err)

		// Create file in subdirectory at baseTime+50
		_, err = db.Exec(`INSERT INTO files (created_at, version, path, mode, mod_time, is_dir, deleted, blob_id) VALUES (?, 1, '/coherent-test/subdir/nested.txt', 0644, ?, 0, 0, ?)`,
			baseTime+50, baseTime+50, blobSub)
		require.NoError(t, err)

		// Query at baseTime+30: should see root.txt and subdir, but not nested.txt
		rows, err := server.queryDirectoryEntries(ctx, "/coherent-test/", baseTime+30)
		require.NoError(t, err)
		entries := collectEntries(t, rows)
		rows.Close()

		assert.Len(t, entries, 2) // root.txt and subdir
		assertEntry(t, entries, "/coherent-test/root.txt", 500, false)
		assertEntryIsDir(t, entries, "/coherent-test/subdir", true)

		// Query subdir at baseTime+30: should be empty (nested.txt created at +50)
		rows, err = server.queryDirectoryEntries(ctx, "/coherent-test/subdir/", baseTime+30)
		require.NoError(t, err)
		entries = collectEntries(t, rows)
		rows.Close()
		assert.Empty(t, entries)

		// Query at baseTime+75: should see everything
		rows, err = server.queryDirectoryEntries(ctx, "/coherent-test/", baseTime+75)
		require.NoError(t, err)
		entries = collectEntries(t, rows)
		rows.Close()

		assert.Len(t, entries, 2) // root.txt and subdir (direct children only)
		assertEntry(t, entries, "/coherent-test/root.txt", 500, false)
		assertEntryIsDir(t, entries, "/coherent-test/subdir", true)

		// Query subdir at baseTime+75: should see nested.txt
		rows, err = server.queryDirectoryEntries(ctx, "/coherent-test/subdir/", baseTime+75)
		require.NoError(t, err)
		entries = collectEntries(t, rows)
		rows.Close()
		assert.Len(t, entries, 1)
		assertEntry(t, entries, "/coherent-test/subdir/nested.txt", 600, false)
	})
}

type dirEntry struct {
	path          string
	size          int64
	modTime       int64
	isDir         bool
	remoteWritten bool
	deleted       bool
	localWritten  bool
}

func collectEntries(t *testing.T, rows *sql.Rows) []dirEntry {
	t.Helper()
	var entries []dirEntry
	for rows.Next() {
		var e dirEntry
		err := rows.Scan(&e.path, &e.size, &e.modTime, &e.isDir, &e.remoteWritten, &e.deleted, &e.localWritten)
		require.NoError(t, err)
		entries = append(entries, e)
	}
	return entries
}

func assertEntry(t *testing.T, entries []dirEntry, path string, expectedSize int64, expectedDeleted bool) {
	t.Helper()
	for _, e := range entries {
		if e.path == path {
			assert.Equal(t, expectedSize, e.size, "size mismatch for %s", path)
			assert.Equal(t, expectedDeleted, e.deleted, "deleted mismatch for %s", path)
			return
		}
	}
	t.Errorf("entry not found: %s", path)
}

func assertEntryIsDir(t *testing.T, entries []dirEntry, path string, expectedIsDir bool) {
	t.Helper()
	for _, e := range entries {
		if e.path == path {
			assert.Equal(t, expectedIsDir, e.isDir, "isDir mismatch for %s", path)
			return
		}
	}
	t.Errorf("entry not found: %s", path)
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
