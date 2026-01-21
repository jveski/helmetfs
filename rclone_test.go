package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSyncRemote(t *testing.T) {
	if _, err := exec.LookPath("rclone"); err != nil {
		t.Skip("rclone not installed")
	}

	ctx := t.Context()
	db, blobsDir := initTestState(t)

	// Resolve symlinks in blobsDir so rclone can find files from /
	// (macOS has /var -> /private/var symlink)
	blobsDir, err := filepath.EvalSymlinks(blobsDir)
	require.NoError(t, err)
	remoteDir, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)

	// Write an rclone config for testing
	configPath := filepath.Join(t.TempDir(), "rclone.conf")
	require.NoError(t, os.WriteFile(configPath, []byte("[testremote]\ntype = local\n"), 0644))
	t.Setenv("RCLONE_CONFIG", configPath)

	// Write a test file
	f, err := openFile(ctx, db, blobsDir, "/test.txt", os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)
	_, err = f.Write([]byte("hello remote"))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	blobID := f.blobID

	// Sync once to upload the blob
	_, err = syncRemote(db, blobsDir, "testremote:"+remoteDir, "0")
	require.NoError(t, err)

	// Idempotence check
	_, err = syncRemote(db, blobsDir, "testremote:"+remoteDir, "0")
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
	_, err = syncRemote(db, blobsDir, "testremote:"+remoteDir, "0")
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
	_, err = syncRemote(db, blobsDir, "testremote:"+remoteDir, "0")
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
