package main

import (
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFSBasics(t *testing.T) {
	db, blobsDir := initTestState(t)
	ctx := t.Context()
	fs := &FS{db: db, blobsDir: blobsDir}

	// Mkdir creates a directory
	require.NoError(t, fs.Mkdir(ctx, "/docs", 0755))

	// Stat returns info for existing directory
	info, err := fs.Stat(ctx, "/docs")
	require.NoError(t, err)
	assert.Equal(t, "docs", info.Name())
	assert.True(t, info.IsDir())

	// Mkdir fails if parent doesn't exist
	err = fs.Mkdir(ctx, "/nonexistent/sub", 0755)
	assert.ErrorIs(t, err, os.ErrNotExist)

	// Mkdir fails if already exists
	err = fs.Mkdir(ctx, "/docs", 0755)
	assert.ErrorIs(t, err, os.ErrExist)

	// OpenFile creates and writes a file
	f, err := fs.OpenFile(ctx, "/docs/readme.txt", os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)
	_, err = f.Write([]byte("hello"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Stat returns info for existing file
	info, err = fs.Stat(ctx, "/docs/readme.txt")
	require.NoError(t, err)
	assert.Equal(t, "readme.txt", info.Name())
	assert.False(t, info.IsDir())
	assert.Equal(t, int64(5), info.Size())

	// Stat fails for nonexistent file
	_, err = fs.Stat(ctx, "/nonexistent.txt")
	assert.ErrorIs(t, err, os.ErrNotExist)

	// Rename moves a file
	require.NoError(t, fs.Rename(ctx, "/docs/readme.txt", "/docs/info.txt"))

	_, err = fs.Stat(ctx, "/docs/readme.txt")
	assert.ErrorIs(t, err, os.ErrNotExist)

	info, err = fs.Stat(ctx, "/docs/info.txt")
	require.NoError(t, err)
	assert.Equal(t, "info.txt", info.Name())

	// Rename fails for nonexistent source
	err = fs.Rename(ctx, "/nonexistent.txt", "/other.txt")
	assert.ErrorIs(t, err, os.ErrNotExist)

	// RemoveAll deletes a file
	require.NoError(t, fs.RemoveAll(ctx, "/docs/info.txt"))
	_, err = fs.Stat(ctx, "/docs/info.txt")
	assert.ErrorIs(t, err, os.ErrNotExist)

	// RemoveAll deletes a directory and its contents
	f, err = fs.OpenFile(ctx, "/docs/a.txt", os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.NoError(t, fs.Mkdir(ctx, "/docs/sub", 0755))
	f, err = fs.OpenFile(ctx, "/docs/sub/b.txt", os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.NoError(t, fs.RemoveAll(ctx, "/docs"))

	_, err = fs.Stat(ctx, "/docs")
	assert.ErrorIs(t, err, os.ErrNotExist)
	_, err = fs.Stat(ctx, "/docs/a.txt")
	assert.ErrorIs(t, err, os.ErrNotExist)
	_, err = fs.Stat(ctx, "/docs/sub/b.txt")
	assert.ErrorIs(t, err, os.ErrNotExist)

	// RemoveAll fails for nonexistent path
	err = fs.RemoveAll(ctx, "/nonexistent")
	assert.ErrorIs(t, err, os.ErrNotExist)

	// OpenFile on root returns a directory
	f, err = fs.OpenFile(ctx, "/", os.O_RDONLY, 0)
	require.NoError(t, err)
	entries, err := f.Readdir(-1)
	require.NoError(t, err)
	assert.Empty(t, entries)

	// Mkdir under a file fails
	f, err = fs.OpenFile(ctx, "/file.txt", os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	err = fs.Mkdir(ctx, "/file.txt/sub", 0755)
	assert.ErrorIs(t, err, os.ErrInvalid)

	// Mkdir can recreate a deleted directory
	require.NoError(t, fs.Mkdir(ctx, "/recreate", 0755))
	require.NoError(t, fs.RemoveAll(ctx, "/recreate"))
	require.NoError(t, fs.Mkdir(ctx, "/recreate", 0755))

	info, err = fs.Stat(ctx, "/recreate")
	require.NoError(t, err)
	assert.True(t, info.IsDir())

	// Rename preserves file content
	f, err = fs.OpenFile(ctx, "/original.txt", os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)
	_, err = f.Write([]byte("preserved content"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.NoError(t, fs.Rename(ctx, "/original.txt", "/renamed.txt"))

	f, err = fs.OpenFile(ctx, "/renamed.txt", os.O_RDONLY, 0)
	require.NoError(t, err)
	content, err := io.ReadAll(f)
	require.NoError(t, err)
	assert.Equal(t, "preserved content", string(content))
	require.NoError(t, f.Close())
}

func TestDeleteLocalBlobs(t *testing.T) {
	db, blobsDir := initTestState(t)

	// Deletes blob file and marks as deleted
	now := time.Now().Unix()
	result, err := db.Exec(`INSERT INTO blobs (creation_time, modified_time, local_written, local_deleting) VALUES (?, ?, 1, 1)`, now, now)
	require.NoError(t, err)
	blobID, err := result.LastInsertId()
	require.NoError(t, err)

	blobPath := blobFilePath(blobsDir, blobID)
	require.NoError(t, os.WriteFile(blobPath, []byte("test content"), 0644))

	_, err = deleteLocalBlobs(db, blobsDir)
	require.NoError(t, err)

	_, err = os.Stat(blobPath)
	assert.True(t, os.IsNotExist(err))

	var localDeleted int
	require.NoError(t, db.QueryRow(`SELECT local_deleted FROM blobs WHERE id = ?`, blobID).Scan(&localDeleted))
	assert.Equal(t, 1, localDeleted)

	// Skips blobs with active readers
	result, err = db.Exec(`INSERT INTO blobs (creation_time, modified_time, local_written, local_deleting) VALUES (?, ?, 1, 1)`, now, now)
	require.NoError(t, err)
	blobID, err = result.LastInsertId()
	require.NoError(t, err)

	blobPath = blobFilePath(blobsDir, blobID)
	require.NoError(t, os.WriteFile(blobPath, []byte("test content"), 0644))

	reader, err := open(blobPath, os.O_RDONLY, 0)
	require.NoError(t, err)

	_, err = deleteLocalBlobs(db, blobsDir)
	require.NoError(t, err)

	_, err = os.Stat(blobPath)
	assert.NoError(t, err)

	require.NoError(t, db.QueryRow(`SELECT local_deleted FROM blobs WHERE id = ?`, blobID).Scan(&localDeleted))
	assert.Equal(t, 0, localDeleted)

	reader.Close()

	// Handles missing file gracefully
	result, err = db.Exec(`INSERT INTO blobs (creation_time, modified_time, local_written, local_deleting) VALUES (?, ?, 1, 1)`, now, now)
	require.NoError(t, err)
	blobID, err = result.LastInsertId()
	require.NoError(t, err)

	_, err = deleteLocalBlobs(db, blobsDir)
	require.NoError(t, err)

	require.NoError(t, db.QueryRow(`SELECT local_deleted FROM blobs WHERE id = ?`, blobID).Scan(&localDeleted))
	assert.Equal(t, 1, localDeleted)
}
