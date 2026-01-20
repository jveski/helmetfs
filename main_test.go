package main

import (
	"database/sql"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDatabaseBackupRestore(t *testing.T) {
	if _, err := exec.LookPath("rclone"); err != nil {
		t.Skip("rclone not installed")
	}

	tmpDir, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	remoteDir, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)

	configPath := filepath.Join(t.TempDir(), "rclone.conf")
	require.NoError(t, os.WriteFile(configPath, []byte("[testremote]\ntype = local\n"), 0644))
	t.Setenv("RCLONE_CONFIG", configPath)

	dbPath := filepath.Join(tmpDir, "test.db")
	backupPath := filepath.Join(tmpDir, "test.db.backup")
	remotePath := "testremote:" + remoteDir

	db, err := sql.Open("sqlite3", dbPath)
	require.NoError(t, err)
	_, err = db.Exec(schema)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO files (created_at, version, path, mode, mod_time, is_dir) VALUES (1, 1, '/test.txt', 0644, 1, 0)`)
	require.NoError(t, err)

	err = backupDatabase(db, backupPath, remotePath, "0")
	require.NoError(t, err)
	db.Close()

	_, err = os.Stat(backupPath)
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(remoteDir, "meta.db.backup"))
	require.NoError(t, err)

	require.NoError(t, os.Remove(dbPath))
	require.NoError(t, os.Remove(backupPath))

	err = restoreDatabase(dbPath, backupPath, remotePath, "0")
	require.NoError(t, err)

	_, err = os.Stat(backupPath)
	require.NoError(t, err)

	restoredDB, err := sql.Open("sqlite3", backupPath)
	require.NoError(t, err)
	t.Cleanup(func() { restoredDB.Close() })

	var path string
	err = restoredDB.QueryRow(`SELECT path FROM files WHERE path = '/test.txt'`).Scan(&path)
	require.NoError(t, err)
	assert.Equal(t, "/test.txt", path)
}

func TestAPIRestore(t *testing.T) {
	db, blobsDir := initTestState(t)
	mux := newRouter(db, blobsDir)
	ts := httptest.NewServer(mux)
	t.Cleanup(func() { ts.Close() })

	client := &http.Client{CheckRedirect: func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}}

	postForm := func(timestamp string) *http.Response {
		resp, err := client.Post(ts.URL+"/api/restore", "application/x-www-form-urlencoded", strings.NewReader(url.Values{"timestamp": {timestamp}}.Encode()))
		require.NoError(t, err)
		return resp
	}

	t.Run("restores file content to previous state", func(t *testing.T) {
		t1 := time.Now().Unix() - 100
		_, err := db.Exec(`INSERT INTO files (created_at, version, path, mode, mod_time, is_dir, blob_id) VALUES (?, 1, '/doc.txt', 0644, ?, 0, NULL)`, t1, t1)
		require.NoError(t, err)

		t2 := t1 + 10
		_, err = db.Exec(`INSERT INTO blobs (creation_time, modified_time, local_written) VALUES (?, ?, 1)`, t2, t2)
		require.NoError(t, err)
		var blobID int64
		require.NoError(t, db.QueryRow(`SELECT last_insert_rowid()`).Scan(&blobID))
		_, err = db.Exec(`INSERT INTO files (created_at, version, path, mode, mod_time, is_dir, blob_id) VALUES (?, 2, '/doc.txt', 0644, ?, 0, ?)`, t2, t2, blobID)
		require.NoError(t, err)

		restoreTime := time.Unix(t1+5, 0).UTC().Format(time.RFC3339)
		resp := postForm(restoreTime)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusSeeOther, resp.StatusCode)
		assert.Equal(t, "/", resp.Header.Get("Location"))

		var blobIDAfter sql.NullInt64
		err = db.QueryRow(`SELECT blob_id FROM files WHERE path = '/doc.txt' ORDER BY version DESC LIMIT 1`).Scan(&blobIDAfter)
		require.NoError(t, err)
		assert.False(t, blobIDAfter.Valid)
	})

	t.Run("restores deleted file", func(t *testing.T) {
		t1 := time.Now().Unix() - 200
		_, err := db.Exec(`INSERT INTO files (created_at, version, path, mode, mod_time, is_dir, deleted) VALUES (?, 1, '/deleted.txt', 0644, ?, 0, 0)`, t1, t1)
		require.NoError(t, err)

		t2 := t1 + 10
		_, err = db.Exec(`INSERT INTO files (created_at, version, path, mode, mod_time, is_dir, deleted) VALUES (?, 2, '/deleted.txt', 0644, ?, 0, 1)`, t2, t2)
		require.NoError(t, err)

		restoreTime := time.Unix(t1+5, 0).UTC().Format(time.RFC3339)
		resp := postForm(restoreTime)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusSeeOther, resp.StatusCode)

		var deleted int
		err = db.QueryRow(`SELECT deleted FROM files WHERE path = '/deleted.txt' ORDER BY version DESC LIMIT 1`).Scan(&deleted)
		require.NoError(t, err)
		assert.Equal(t, 0, deleted)
	})

	t.Run("deletes file that did not exist at timestamp", func(t *testing.T) {
		t1 := time.Now().Unix() - 300
		_, err := db.Exec(`INSERT INTO files (created_at, version, path, mode, mod_time, is_dir, deleted) VALUES (?, 1, '/new.txt', 0644, ?, 0, 0)`, t1, t1)
		require.NoError(t, err)

		restoreTime := time.Unix(t1-10, 0).UTC().Format(time.RFC3339)
		resp := postForm(restoreTime)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusSeeOther, resp.StatusCode)

		var deleted int
		err = db.QueryRow(`SELECT deleted FROM files WHERE path = '/new.txt' ORDER BY version DESC LIMIT 1`).Scan(&deleted)
		require.NoError(t, err)
		assert.Equal(t, 1, deleted)
	})

	t.Run("no changes when already at target state", func(t *testing.T) {
		t1 := time.Now().Unix() - 400
		_, err := db.Exec(`INSERT INTO files (created_at, version, path, mode, mod_time, is_dir, deleted) VALUES (?, 1, '/stable.txt', 0644, ?, 0, 0)`, t1, t1)
		require.NoError(t, err)

		restoreTime := time.Unix(t1+10, 0).UTC().Format(time.RFC3339)
		resp := postForm(restoreTime)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusSeeOther, resp.StatusCode)
	})

	t.Run("error on future timestamp", func(t *testing.T) {
		futureTime := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
		resp := postForm(futureTime)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

		body, _ := io.ReadAll(resp.Body)
		assert.Contains(t, string(body), "cannot restore to a future time")
	})

	t.Run("error on missing timestamp", func(t *testing.T) {
		resp := postForm("")
		defer resp.Body.Close()
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

		body, _ := io.ReadAll(resp.Body)
		assert.Contains(t, string(body), "timestamp is required")
	})

	t.Run("error on invalid timestamp format", func(t *testing.T) {
		resp := postForm("not-a-date")
		defer resp.Body.Close()
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

		body, _ := io.ReadAll(resp.Body)
		assert.Contains(t, string(body), "invalid timestamp format")
	})

	t.Run("error on timestamp beyond retention period", func(t *testing.T) {
		oldTime := time.Now().Add(-*ttl - time.Hour).UTC().Format(time.RFC3339)
		resp := postForm(oldTime)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

		body, _ := io.ReadAll(resp.Body)
		assert.Contains(t, string(body), "cannot restore beyond the retention period")
	})
}

func TestStatusPage(t *testing.T) {
	db, blobsDir := initTestState(t)
	mux := newRouter(db, blobsDir)
	ts := httptest.NewServer(mux)
	t.Cleanup(func() { ts.Close() })

	getStatus := func() (int, string) {
		resp, err := http.Get(ts.URL + "/")
		require.NoError(t, err)
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(body)
	}

	t.Run("returns HTML with correct content type", func(t *testing.T) {
		resp, err := http.Get(ts.URL + "/")
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, "text/html; charset=utf-8", resp.Header.Get("Content-Type"))
	})

	t.Run("shows pending rclone operations", func(t *testing.T) {
		now := time.Now().Unix()
		_, err := db.Exec(`INSERT INTO blobs (creation_time, modified_time, local_written, remote_written) VALUES (?, ?, 0, 1)`, now, now)
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO blobs (creation_time, modified_time, local_written, remote_written, local_deleting) VALUES (?, ?, 1, 0, 0)`, now, now)
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO blobs (creation_time, modified_time, local_written, remote_written, remote_deleting) VALUES (?, ?, 1, 1, 1)`, now, now)
		require.NoError(t, err)

		_, body := getStatus()
		assert.Contains(t, body, "Pending Downloads</span><span")
		assert.Contains(t, body, "Pending Uploads</span><span")
		assert.Contains(t, body, "Pending Deletes</span><span")
	})

	t.Run("shows overdue integrity checks", func(t *testing.T) {
		now := time.Now().Unix()
		oldCheck := now - int64((*integrityCheckInterval).Seconds()) - 3600
		_, err := db.Exec(`INSERT INTO blobs (creation_time, modified_time, local_written, remote_written, checksum, last_integrity_check) VALUES (?, ?, 1, 1, 'abc123', ?)`, now, now, oldCheck)
		require.NoError(t, err)

		_, body := getStatus()
		assert.Contains(t, body, "Overdue Integrity Checks</span><span")
	})

	t.Run("shows active file count", func(t *testing.T) {
		now := time.Now().Unix()
		_, err := db.Exec(`INSERT INTO files (created_at, version, path, mode, mod_time, is_dir, deleted) VALUES (?, 1, '/status-test.txt', 0644, ?, 0, 0)`, now, now)
		require.NoError(t, err)

		_, body := getStatus()
		assert.Contains(t, body, "Active Files</span><span")
	})

	t.Run("shows local and remote space", func(t *testing.T) {
		now := time.Now().Unix()
		_, err := db.Exec(`INSERT INTO blobs (creation_time, modified_time, local_written, remote_written, size) VALUES (?, ?, 1, 1, 1024)`, now, now)
		require.NoError(t, err)

		_, body := getStatus()
		assert.Contains(t, body, "Local Blob Space</span><span")
		assert.Contains(t, body, "Remote Blob Space</span><span")
	})

	t.Run("contains restore form with datetime picker", func(t *testing.T) {
		_, body := getStatus()
		assert.Contains(t, body, `<form action="/api/restore" method="POST"`)
		assert.Contains(t, body, `onsubmit="return confirm(`)
		assert.Contains(t, body, `type="datetime-local"`)
		assert.Contains(t, body, `name="timestamp"`)
		assert.Contains(t, body, `<button type="submit">Restore</button>`)
	})
}
