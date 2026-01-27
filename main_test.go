package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
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

func TestStatusPage(t *testing.T) {
	db, blobsDir := initTestState(t)
	mux := newRouter(db, blobsDir)
	ts := httptest.NewServer(mux)
	t.Cleanup(func() { ts.Close() })

	getStatus := func() (int, string) {
		resp, err := http.Get(ts.URL + "/status")
		require.NoError(t, err)
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(body)
	}

	t.Run("returns HTML with correct content type", func(t *testing.T) {
		resp, err := http.Get(ts.URL + "/status")
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, "text/html; charset=utf-8", resp.Header.Get("Content-Type"))
	})

	t.Run("shows pending rclone operations", func(t *testing.T) {
		now := time.Now().Unix()
		_, err := db.Exec(`INSERT INTO blobs (id, creation_time, modified_time, local_written, remote_written) VALUES (?, ?, ?, 0, 1)`, uuid.New().String(), now, now)
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO blobs (id, creation_time, modified_time, local_written, remote_written, local_deleting) VALUES (?, ?, ?, 1, 0, 0)`, uuid.New().String(), now, now)
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO blobs (id, creation_time, modified_time, local_written, remote_written, remote_deleting) VALUES (?, ?, ?, 1, 1, 1)`, uuid.New().String(), now, now)
		require.NoError(t, err)

		_, body := getStatus()
		assert.Contains(t, body, "Pending Downloads</div>")
		assert.Contains(t, body, "Pending Uploads</div>")
		assert.Contains(t, body, "Pending Deletes</div>")
	})

	t.Run("shows overdue integrity checks", func(t *testing.T) {
		now := time.Now().Unix()
		oldCheck := now - int64((*integrityCheckInterval).Seconds()) - 3600
		_, err := db.Exec(`INSERT INTO blobs (id, creation_time, modified_time, local_written, remote_written, checksum, last_integrity_check) VALUES (?, ?, ?, 1, 1, 'abc123', ?)`, uuid.New().String(), now, now, oldCheck)
		require.NoError(t, err)

		_, body := getStatus()
		assert.Contains(t, body, "Overdue Integrity Checks</div>")
	})

	t.Run("shows active file count", func(t *testing.T) {
		now := time.Now().Unix()
		_, err := db.Exec(`INSERT INTO files (created_at, version, path, mode, mod_time, is_dir, deleted) VALUES (?, 1, '/status-test.txt', 0644, ?, 0, 0)`, now, now)
		require.NoError(t, err)

		_, body := getStatus()
		assert.Contains(t, body, "Active Files</div>")
	})

	t.Run("shows local and remote space", func(t *testing.T) {
		now := time.Now().Unix()
		_, err := db.Exec(`INSERT INTO blobs (id, creation_time, modified_time, local_written, remote_written, size) VALUES (?, ?, ?, 1, 1, 1024)`, uuid.New().String(), now, now)
		require.NoError(t, err)

		_, body := getStatus()
		assert.Contains(t, body, "Local Blob Space</div>")
		assert.Contains(t, body, "Remote Blob Space</div>")
	})
}

func TestGarbageCollection(t *testing.T) {
	db, _ := initTestState(t)

	// Save original TTLs and restore after test
	origTTL := *ttl
	origUploadTTL := *uploadTTL
	t.Cleanup(func() {
		*ttl = origTTL
		*uploadTTL = origUploadTTL
	})

	// Use short TTLs for testing
	*ttl = 100 * time.Millisecond
	*uploadTTL = 100 * time.Millisecond

	t.Run("marks orphaned blobs for deletion", func(t *testing.T) {
		// Create a blob that was never written to (simulates interrupted upload)
		oldTime := time.Now().Add(-time.Hour).Unix()
		orphanedBlobID := uuid.New().String()
		_, err := db.Exec(`INSERT INTO blobs (id, creation_time, modified_time, local_written, local_deleting) VALUES (?, ?, ?, 0, 0)`, orphanedBlobID, oldTime, oldTime)
		require.NoError(t, err)

		// Run garbage collection
		_, err = takeOutTheTrash(db)
		require.NoError(t, err)

		// Check that the orphaned blob is marked for deletion
		var localDeleting int
		err = db.QueryRow(`SELECT local_deleting FROM blobs WHERE id = ?`, orphanedBlobID).Scan(&localDeleting)
		require.NoError(t, err)
		assert.Equal(t, 1, localDeleting, "orphaned blob should be marked for deletion")
	})

	t.Run("does not mark recent orphaned blobs", func(t *testing.T) {
		// Create a recent blob that was never written to
		recentTime := time.Now().Unix()
		recentBlobID := uuid.New().String()
		_, err := db.Exec(`INSERT INTO blobs (id, creation_time, modified_time, local_written, local_deleting) VALUES (?, ?, ?, 0, 0)`, recentBlobID, recentTime, recentTime)
		require.NoError(t, err)

		// Run garbage collection
		_, err = takeOutTheTrash(db)
		require.NoError(t, err)

		// Check that the recent blob is NOT marked for deletion
		var localDeleting int
		err = db.QueryRow(`SELECT local_deleting FROM blobs WHERE id = ?`, recentBlobID).Scan(&localDeleting)
		require.NoError(t, err)
		assert.Equal(t, 0, localDeleting, "recent orphaned blob should not be marked for deletion yet")
	})

	t.Run("compacts old deleted file versions", func(t *testing.T) {
		// Create an old deleted file version
		oldTime := time.Now().Add(-time.Hour).Unix()
		_, err := db.Exec(`INSERT INTO files (created_at, version, path, mode, mod_time, is_dir, deleted) VALUES (?, 1, '/gc-deleted.txt', 0644, ?, 0, 1)`, oldTime, oldTime)
		require.NoError(t, err)

		var deletedFileID int64
		require.NoError(t, db.QueryRow(`SELECT last_insert_rowid()`).Scan(&deletedFileID))

		// Run garbage collection
		_, err = takeOutTheTrash(db)
		require.NoError(t, err)

		// Check that the old deleted file version was compacted
		var count int
		err = db.QueryRow(`SELECT COUNT(*) FROM files WHERE id = ?`, deletedFileID).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 0, count, "old deleted file version should be compacted")
	})

	t.Run("compacts old superseded file versions", func(t *testing.T) {
		// Create two versions of a file, with the old one being past TTL
		oldTime := time.Now().Add(-time.Hour).Unix()
		newTime := time.Now().Unix()

		_, err := db.Exec(`INSERT INTO files (created_at, version, path, mode, mod_time, is_dir, deleted) VALUES (?, 1, '/gc-superseded.txt', 0644, ?, 0, 0)`, oldTime, oldTime)
		require.NoError(t, err)

		var oldFileID int64
		require.NoError(t, db.QueryRow(`SELECT last_insert_rowid()`).Scan(&oldFileID))

		_, err = db.Exec(`INSERT INTO files (created_at, version, path, mode, mod_time, is_dir, deleted) VALUES (?, 2, '/gc-superseded.txt', 0644, ?, 0, 0)`, newTime, newTime)
		require.NoError(t, err)

		// Run garbage collection
		_, err = takeOutTheTrash(db)
		require.NoError(t, err)

		// Check that the old version was compacted
		var count int
		err = db.QueryRow(`SELECT COUNT(*) FROM files WHERE id = ?`, oldFileID).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 0, count, "old superseded file version should be compacted")

		// Check that the new version still exists
		var exists int
		err = db.QueryRow(`SELECT COUNT(*) FROM files WHERE path = '/gc-superseded.txt' AND version = 2`).Scan(&exists)
		require.NoError(t, err)
		assert.Equal(t, 1, exists, "current file version should still exist")
	})

	t.Run("does not compact recent file versions", func(t *testing.T) {
		// Create two versions of a file, both recent
		recentTime := time.Now().Unix()

		_, err := db.Exec(`INSERT INTO files (created_at, version, path, mode, mod_time, is_dir, deleted) VALUES (?, 1, '/gc-recent.txt', 0644, ?, 0, 0)`, recentTime, recentTime)
		require.NoError(t, err)

		var recentFileID int64
		require.NoError(t, db.QueryRow(`SELECT last_insert_rowid()`).Scan(&recentFileID))

		_, err = db.Exec(`INSERT INTO files (created_at, version, path, mode, mod_time, is_dir, deleted) VALUES (?, 2, '/gc-recent.txt', 0644, ?, 0, 0)`, recentTime, recentTime)
		require.NoError(t, err)

		// Run garbage collection
		_, err = takeOutTheTrash(db)
		require.NoError(t, err)

		// Check that the old version still exists (within TTL)
		var count int
		err = db.QueryRow(`SELECT COUNT(*) FROM files WHERE id = ?`, recentFileID).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count, "recent superseded file version should not be compacted yet")
	})
}

func TestDeleteLocalBlobsExtended(t *testing.T) {
	db, blobsDir := initTestState(t)

	t.Run("deletes blob files marked for deletion", func(t *testing.T) {
		// Create a blob and its file
		now := time.Now().Unix()
		blobID := uuid.New().String()
		_, err := db.Exec(`INSERT INTO blobs (id, creation_time, modified_time, local_written, local_deleting, checksum) VALUES (?, ?, ?, 1, 1, 'delete-test-1')`, blobID, now, now)
		require.NoError(t, err)

		blobPath := blobFilePath(blobsDir, blobID)
		require.NoError(t, os.WriteFile(blobPath, []byte("test content"), 0644))

		// Verify blob file exists
		_, err = os.Stat(blobPath)
		require.NoError(t, err)

		// Run local blob deletion
		_, err = deleteLocalBlobs(db, blobsDir)
		require.NoError(t, err)

		// Verify blob file is deleted
		_, err = os.Stat(blobPath)
		assert.True(t, os.IsNotExist(err), "blob file should be deleted")

		// Verify database is updated
		var localDeleted int
		err = db.QueryRow(`SELECT local_deleted FROM blobs WHERE id = ?`, blobID).Scan(&localDeleted)
		require.NoError(t, err)
		assert.Equal(t, 1, localDeleted, "blob should be marked as locally deleted")
	})

	t.Run("handles already deleted blob files", func(t *testing.T) {
		// Create a blob marked for deletion but file doesn't exist
		now := time.Now().Unix()
		blobID := uuid.New().String()
		_, err := db.Exec(`INSERT INTO blobs (id, creation_time, modified_time, local_written, local_deleting, checksum) VALUES (?, ?, ?, 1, 1, 'delete-test-2')`, blobID, now, now)
		require.NoError(t, err)

		// Don't create the file - simulates already deleted

		// Run local blob deletion - should not error
		_, err = deleteLocalBlobs(db, blobsDir)
		require.NoError(t, err)

		// Verify database is updated
		var localDeleted int
		err = db.QueryRow(`SELECT local_deleted FROM blobs WHERE id = ?`, blobID).Scan(&localDeleted)
		require.NoError(t, err)
		assert.Equal(t, 1, localDeleted, "blob should be marked as locally deleted even if file was already gone")
	})

	t.Run("does not delete blobs not marked for deletion", func(t *testing.T) {
		// Create a blob NOT marked for deletion
		now := time.Now().Unix()
		blobID := uuid.New().String()
		_, err := db.Exec(`INSERT INTO blobs (id, creation_time, modified_time, local_written, local_deleting, checksum) VALUES (?, ?, ?, 1, 0, 'nodelete-test')`, blobID, now, now)
		require.NoError(t, err)

		blobPath := blobFilePath(blobsDir, blobID)
		require.NoError(t, os.WriteFile(blobPath, []byte("keep this content"), 0644))

		// Run local blob deletion
		_, err = deleteLocalBlobs(db, blobsDir)
		require.NoError(t, err)

		// Verify blob file still exists
		_, err = os.Stat(blobPath)
		require.NoError(t, err, "blob file should not be deleted")
	})
}

func TestIntegrityCheck(t *testing.T) {
	db, blobsDir := initTestState(t)

	// Save original interval and restore after test
	origInterval := *integrityCheckInterval
	t.Cleanup(func() {
		*integrityCheckInterval = origInterval
	})

	// Use short interval for testing
	*integrityCheckInterval = 100 * time.Millisecond

	t.Run("verifies blob integrity and updates timestamp", func(t *testing.T) {
		// Create a blob with known content and checksum
		content := []byte("integrity test content")
		checksum := sha256.Sum256(content)
		checksumHex := hex.EncodeToString(checksum[:])

		oldCheck := time.Now().Add(-time.Hour).Unix()
		now := time.Now().Unix()
		blobID := uuid.New().String()
		_, err := db.Exec(`INSERT INTO blobs (id, creation_time, modified_time, local_written, remote_written, checksum, last_integrity_check) VALUES (?, ?, ?, 1, 1, ?, ?)`, blobID, now, now, checksumHex, oldCheck)
		require.NoError(t, err)

		blobPath := blobFilePath(blobsDir, blobID)
		require.NoError(t, os.WriteFile(blobPath, content, 0644))

		// Run integrity check
		more, err := checkFileIntegrity(db, blobsDir, *integrityCheckInterval)
		require.NoError(t, err)
		assert.True(t, more, "should indicate more work was done")

		// Verify timestamp was updated
		var lastCheck int64
		err = db.QueryRow(`SELECT last_integrity_check FROM blobs WHERE id = ?`, blobID).Scan(&lastCheck)
		require.NoError(t, err)
		assert.Greater(t, lastCheck, oldCheck, "last_integrity_check should be updated")
	})

	t.Run("detects corrupted blob and marks for re-download", func(t *testing.T) {
		// Create a blob with a checksum that won't match the content
		correctChecksum := sha256.Sum256([]byte("original content"))
		checksumHex := hex.EncodeToString(correctChecksum[:])

		oldCheck := time.Now().Add(-time.Hour).Unix()
		now := time.Now().Unix()
		blobID := uuid.New().String()
		_, err := db.Exec(`INSERT INTO blobs (id, creation_time, modified_time, local_written, remote_written, checksum, last_integrity_check) VALUES (?, ?, ?, 1, 1, ?, ?)`, blobID, now, now, checksumHex, oldCheck)
		require.NoError(t, err)

		// Write corrupted content
		blobPath := blobFilePath(blobsDir, blobID)
		require.NoError(t, os.WriteFile(blobPath, []byte("corrupted content!!!"), 0644))

		// Run integrity check
		more, err := checkFileIntegrity(db, blobsDir, *integrityCheckInterval)
		require.NoError(t, err)
		assert.True(t, more, "should indicate work was done")

		// Verify blob is marked for re-download (local_written = 0)
		var localWritten int
		var lastCheck sql.NullInt64
		err = db.QueryRow(`SELECT local_written, last_integrity_check FROM blobs WHERE id = ?`, blobID).Scan(&localWritten, &lastCheck)
		require.NoError(t, err)
		assert.Equal(t, 0, localWritten, "corrupted blob should be marked for re-download")
		assert.False(t, lastCheck.Valid, "last_integrity_check should be cleared")
	})

	t.Run("detects missing blob and marks for re-download", func(t *testing.T) {
		checksum := sha256.Sum256([]byte("missing blob content"))
		checksumHex := hex.EncodeToString(checksum[:])

		oldCheck := time.Now().Add(-time.Hour).Unix()
		now := time.Now().Unix()
		blobID := uuid.New().String()
		_, err := db.Exec(`INSERT INTO blobs (id, creation_time, modified_time, local_written, remote_written, checksum, last_integrity_check) VALUES (?, ?, ?, 1, 1, ?, ?)`, blobID, now, now, checksumHex, oldCheck)
		require.NoError(t, err)

		// Don't create the blob file - simulates missing file

		// Run integrity check
		more, err := checkFileIntegrity(db, blobsDir, *integrityCheckInterval)
		require.NoError(t, err)
		assert.True(t, more, "should indicate work was done")

		// Verify blob is marked for re-download
		var localWritten int
		err = db.QueryRow(`SELECT local_written FROM blobs WHERE id = ?`, blobID).Scan(&localWritten)
		require.NoError(t, err)
		assert.Equal(t, 0, localWritten, "missing blob should be marked for re-download")
	})

	t.Run("skips recently checked blobs", func(t *testing.T) {
		content := []byte("recently checked content")
		checksum := sha256.Sum256(content)
		checksumHex := hex.EncodeToString(checksum[:])

		// Set last_integrity_check to now (within interval)
		now := time.Now().Unix()
		blobID := uuid.New().String()
		_, err := db.Exec(`INSERT INTO blobs (id, creation_time, modified_time, local_written, remote_written, checksum, last_integrity_check) VALUES (?, ?, ?, 1, 1, ?, ?)`, blobID, now, now, checksumHex, now)
		require.NoError(t, err)

		blobPath := blobFilePath(blobsDir, blobID)
		require.NoError(t, os.WriteFile(blobPath, content, 0644))

		// Clear out any other blobs that might be selected
		_, err = db.Exec(`UPDATE blobs SET last_integrity_check = ? WHERE id != ?`, now, blobID)
		require.NoError(t, err)

		// Run integrity check - should skip this blob
		more, err := checkFileIntegrity(db, blobsDir, *integrityCheckInterval)
		require.NoError(t, err)
		assert.False(t, more, "should indicate no work needed for recently checked blob")
	})

	t.Run("prioritizes blobs never checked", func(t *testing.T) {
		content1 := []byte("never checked content")
		checksum1 := sha256.Sum256(content1)
		checksumHex1 := hex.EncodeToString(checksum1[:])

		content2 := []byte("checked long ago content")
		checksum2 := sha256.Sum256(content2)
		checksumHex2 := hex.EncodeToString(checksum2[:])

		now := time.Now().Unix()
		oldCheck := now - 3600*48 // 48 hours ago

		// Create blob that was checked long ago
		blobID2 := uuid.New().String()
		_, err := db.Exec(`INSERT INTO blobs (id, creation_time, modified_time, local_written, remote_written, checksum, last_integrity_check) VALUES (?, ?, ?, 1, 1, ?, ?)`, blobID2, now, now, checksumHex2, oldCheck)
		require.NoError(t, err)
		blobPath2 := blobFilePath(blobsDir, blobID2)
		require.NoError(t, os.WriteFile(blobPath2, content2, 0644))

		// Create blob that was never checked (NULL)
		blobID1 := uuid.New().String()
		_, err = db.Exec(`INSERT INTO blobs (id, creation_time, modified_time, local_written, remote_written, checksum, last_integrity_check) VALUES (?, ?, ?, 1, 1, ?, NULL)`, blobID1, now, now, checksumHex1)
		require.NoError(t, err)
		blobPath1 := blobFilePath(blobsDir, blobID1)
		require.NoError(t, os.WriteFile(blobPath1, content1, 0644))

		// Run integrity check - should pick the never-checked blob first
		more, err := checkFileIntegrity(db, blobsDir, *integrityCheckInterval)
		require.NoError(t, err)
		assert.True(t, more)

		// Verify the never-checked blob was checked (now has a timestamp)
		var lastCheck sql.NullInt64
		err = db.QueryRow(`SELECT last_integrity_check FROM blobs WHERE id = ?`, blobID1).Scan(&lastCheck)
		require.NoError(t, err)
		assert.True(t, lastCheck.Valid, "never-checked blob should now have a check timestamp")
	})
}

func TestBlobUnreferencedTrigger(t *testing.T) {
	db, blobsDir := initTestState(t)
	ctx := t.Context()

	t.Run("marks blob for deletion when file is overwritten", func(t *testing.T) {
		// Create first file version with blob
		f1, err := openFile(ctx, db, blobsDir, "/trigger-test.txt", os.O_CREATE|os.O_WRONLY, 0644)
		require.NoError(t, err)
		_, err = f1.Write([]byte("first content"))
		require.NoError(t, err)
		require.NoError(t, f1.Close())
		firstBlobID := f1.blobID

		// Mark the first blob as remote_written (simulates upload completion)
		_, err = db.Exec(`UPDATE blobs SET remote_written = 1 WHERE id = ?`, firstBlobID)
		require.NoError(t, err)

		// Overwrite with different content
		f2, err := openFile(ctx, db, blobsDir, "/trigger-test.txt", os.O_WRONLY|os.O_TRUNC, 0644)
		require.NoError(t, err)
		_, err = f2.Write([]byte("second content"))
		require.NoError(t, err)
		require.NoError(t, f2.Close())

		// The first blob should now be marked for deletion (trigger fires on INSERT into files)
		var localDeleting int
		err = db.QueryRow(`SELECT local_deleting FROM blobs WHERE id = ?`, firstBlobID).Scan(&localDeleting)
		require.NoError(t, err)
		assert.Equal(t, 1, localDeleting, "unreferenced blob should be marked for local deletion")
	})

	t.Run("does not mark blob if still referenced", func(t *testing.T) {
		// Create a file
		f1, err := openFile(ctx, db, blobsDir, "/still-ref.txt", os.O_CREATE|os.O_WRONLY, 0644)
		require.NoError(t, err)
		_, err = f1.Write([]byte("still referenced content"))
		require.NoError(t, err)
		require.NoError(t, f1.Close())
		blobID := f1.blobID

		// Mark as remote_written
		_, err = db.Exec(`UPDATE blobs SET remote_written = 1 WHERE id = ?`, blobID)
		require.NoError(t, err)

		// Create a different file (should not affect our blob)
		f2, err := openFile(ctx, db, blobsDir, "/other.txt", os.O_CREATE|os.O_WRONLY, 0644)
		require.NoError(t, err)
		_, err = f2.Write([]byte("other content"))
		require.NoError(t, err)
		require.NoError(t, f2.Close())

		// Our blob should NOT be marked for deletion
		var localDeleting int
		err = db.QueryRow(`SELECT local_deleting FROM blobs WHERE id = ?`, blobID).Scan(&localDeleting)
		require.NoError(t, err)
		assert.Equal(t, 0, localDeleting, "referenced blob should not be marked for deletion")
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

func TestHistoricalBrowse(t *testing.T) {
	db, blobsDir := initTestState(t)
	mux := newRouter(db, blobsDir)
	ts := httptest.NewServer(mux)
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
	mux := newRouter(db, blobsDir)
	ts := httptest.NewServer(mux)
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
	mux := newRouter(db, blobsDir)
	ts := httptest.NewServer(mux)
	t.Cleanup(func() { ts.Close() })

	getDownloadHistorical := func(path, timestamp string) (int, string, http.Header) {
		u := ts.URL + "/api/download-historical?path=" + url.QueryEscape(path) + "&at=" + url.QueryEscape(timestamp)
		resp, err := http.Get(u)
		require.NoError(t, err)
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(body), resp.Header
	}

	t.Run("downloads historical file version", func(t *testing.T) {
		// Create blob with content
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

		downloadTime := time.Unix(t1+10, 0).UTC().Format(time.RFC3339)
		code, body, headers := getDownloadHistorical("/download-test.txt", downloadTime)
		assert.Equal(t, http.StatusOK, code)
		assert.Equal(t, string(content), body)
		assert.Contains(t, headers.Get("Content-Disposition"), "download-test.txt")
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
}
