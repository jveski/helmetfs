package main

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRcloneSync(t *testing.T) {
	rc, remoteDir := initRcloneTest(t, 0)
	db, blobsDir := initTestState(t)
	ctx := t.Context()

	// Resolve symlinks in blobsDir so rclone can find files
	// (macOS has /var -> /private/var symlink)
	blobsDir, err := filepath.EvalSymlinks(blobsDir)
	require.NoError(t, err)

	// Write a test file
	f, err := openFile(ctx, db, blobsDir, "/test.txt", os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)
	_, err = f.Write([]byte("hello remote"))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	blobID := f.blobID

	// Sync once to upload the blob
	_, err = rc.Sync(db, blobsDir)
	require.NoError(t, err)

	// Idempotence check
	_, err = rc.Sync(db, blobsDir)
	require.NoError(t, err)

	// Prove the remote blob was created with relative path structure (not nested under blobsDir)
	// Remote should have: remoteDir/ab/cd1234... NOT remoteDir/full/local/path/blobs/ab/cd1234...
	blobRelPath := blobID[:2] + "/" + blobID[2:]
	remoteBlobPath := filepath.Join(remoteDir, blobRelPath)
	content, err := os.ReadFile(remoteBlobPath)
	require.NoError(t, err)
	assert.Equal(t, "hello remote", string(content))

	// Prove the DB was updated
	var localWritten, remoteWritten, remoteDeleting, remoteDeleted int
	err = db.QueryRow(`SELECT local_written, remote_written, remote_deleting, remote_deleted FROM blobs WHERE id = ?`, blobID).Scan(&localWritten, &remoteWritten, &remoteDeleting, &remoteDeleted)
	require.NoError(t, err)
	assert.Equal(t, 1, localWritten)
	assert.Equal(t, 1, remoteWritten)
	assert.Equal(t, 0, remoteDeleting)
	assert.Equal(t, 0, remoteDeleted)

	// Simulate local blob loss (e.g., disk failure) by deleting local file and marking as not written
	localBlobPath := blobFilePath(blobsDir, blobID)
	require.NoError(t, os.Remove(localBlobPath))
	_, err = db.Exec(`UPDATE blobs SET local_written = 0 WHERE id = ?`, blobID)
	require.NoError(t, err)

	// Sync should download the blob from remote
	_, err = rc.Sync(db, blobsDir)
	require.NoError(t, err)

	// Prove the local blob was restored
	content, err = os.ReadFile(localBlobPath)
	require.NoError(t, err)
	assert.Equal(t, "hello remote", string(content))

	// Prove the DB was updated
	err = db.QueryRow(`SELECT local_written FROM blobs WHERE id = ?`, blobID).Scan(&localWritten)
	require.NoError(t, err)
	assert.Equal(t, 1, localWritten)

	// Mark blob for remote deletion
	_, err = db.Exec(`UPDATE blobs SET remote_deleting = 1 WHERE id = ?`, blobID)
	require.NoError(t, err)

	// Delete blob from remote
	_, err = rc.Sync(db, blobsDir)
	require.NoError(t, err)

	// Prove the remote blob was deleted
	_, err = os.Stat(remoteBlobPath)
	assert.True(t, os.IsNotExist(err))

	// Prove the DB was updated
	err = db.QueryRow(`SELECT remote_written, remote_deleting, remote_deleted FROM blobs WHERE id = ?`, blobID).Scan(&remoteWritten, &remoteDeleting, &remoteDeleted)
	require.NoError(t, err)
	assert.Equal(t, 1, remoteWritten)
	assert.Equal(t, 1, remoteDeleting)
	assert.Equal(t, 1, remoteDeleted)
}

func TestRcloneCopyFile(t *testing.T) {
	rc, remoteDir := initRcloneTest(t, 0)

	// Create a local directory for testing (separate from remote)
	localDir, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)

	// Test upload (toRemote = true)
	localFile := filepath.Join(localDir, "upload.txt")
	require.NoError(t, os.WriteFile(localFile, []byte("upload content"), 0644))

	err = rc.CopyFile(localFile, "uploaded.txt", true)
	require.NoError(t, err)

	content, err := os.ReadFile(filepath.Join(remoteDir, "uploaded.txt"))
	require.NoError(t, err)
	assert.Equal(t, "upload content", string(content))

	// Test download (toRemote = false)
	remoteFile := filepath.Join(remoteDir, "download.txt")
	require.NoError(t, os.WriteFile(remoteFile, []byte("download content"), 0644))

	downloadDest := filepath.Join(localDir, "downloaded.txt")
	err = rc.CopyFile("download.txt", downloadDest, false)
	require.NoError(t, err)

	content, err = os.ReadFile(downloadDest)
	require.NoError(t, err)
	assert.Equal(t, "download content", string(content))
}

func TestRcloneCatBlob(t *testing.T) {
	rc, remoteDir := initRcloneTest(t, 1)
	ctx := t.Context()

	// Create a blob file in the remote with the expected directory structure
	blobID := "abcd1234567890abcdef1234567890abcdef1234567890abcdef1234567890ab"
	blobDir := filepath.Join(remoteDir, blobID[:2])
	require.NoError(t, os.MkdirAll(blobDir, 0755))
	blobPath := filepath.Join(blobDir, blobID[2:])
	require.NoError(t, os.WriteFile(blobPath, []byte("blob content"), 0644))

	// Cat the blob
	var buf bytes.Buffer
	err := rc.CatBlob(ctx, blobID, &buf)
	require.NoError(t, err)
	assert.Equal(t, "blob content", buf.String())
}

func TestRcloneCatBlobConcurrencyLimit(t *testing.T) {
	rc, remoteDir := initRcloneTest(t, 1)

	// Create a blob file
	blobID := "abcd1234567890abcdef1234567890abcdef1234567890abcdef1234567890ab"
	blobDir := filepath.Join(remoteDir, blobID[:2])
	require.NoError(t, os.MkdirAll(blobDir, 0755))
	blobPath := filepath.Join(blobDir, blobID[2:])
	require.NoError(t, os.WriteFile(blobPath, []byte("blob content"), 0644))

	// Fill the download limit channel (simulating an in-progress download)
	rc.downloadLimit <- struct{}{}

	// CatBlob with cancelled context should return context error
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var buf bytes.Buffer
	err := rc.CatBlob(ctx, blobID, &buf)
	assert.ErrorIs(t, err, context.Canceled)
}

// initRcloneTest sets up a test environment for rclone tests.
// It skips the test if rclone is not installed, creates the rclone config,
// sets RCLONE_CONFIG, and returns the Rclone instance and resolved remote directory.
func initRcloneTest(t *testing.T, streamLimit int) (rc *Rclone, remoteDir string) {
	t.Helper()

	if _, err := exec.LookPath("rclone"); err != nil {
		t.Skip("rclone not installed")
	}

	// Resolve symlinks so rclone can find files from /
	// (macOS has /var -> /private/var symlink)
	remoteDir, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)

	// Write an rclone config for testing
	configPath := filepath.Join(t.TempDir(), "rclone.conf")
	require.NoError(t, os.WriteFile(configPath, []byte("[testremote]\ntype = local\n"), 0644))
	t.Setenv("RCLONE_CONFIG", configPath)

	rc = NewRclone("testremote:"+remoteDir, "0", streamLimit)

	return rc, remoteDir
}
