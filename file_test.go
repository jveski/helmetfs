package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFile(t *testing.T) {
	db, blobsDir := initTestState(t)
	ctx := t.Context()

	// Create and write to files
	f, err := openFile(ctx, db, blobsDir, "/aaa.txt", os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)
	n, err := f.Write([]byte("hello world"))
	require.NoError(t, err)
	assert.Equal(t, 11, n)
	require.NoError(t, f.Close())

	f, err = openFile(ctx, db, blobsDir, "/bbb.txt", os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)
	_, err = f.Write([]byte("content b"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Read and seek within a file
	f, err = openFile(ctx, db, blobsDir, "/aaa.txt", os.O_RDONLY, 0)
	require.NoError(t, err)

	buf := make([]byte, 5)
	n, err = f.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, "hello", string(buf))

	pos, err := f.Seek(6, io.SeekStart)
	require.NoError(t, err)
	assert.Equal(t, int64(6), pos)

	buf = make([]byte, 5)
	n, err = f.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, "world", string(buf))
	require.NoError(t, f.Close())

	// Readdir with pagination
	root := &file{db: db, path: "/", isDir: true}

	entries, err := root.Readdir(1)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "aaa.txt", entries[0].Name())

	entries, err = root.Readdir(1)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "bbb.txt", entries[0].Name())

	_, err = root.Readdir(1)
	assert.ErrorIs(t, err, io.EOF)

	// Seek back and read all entries
	_, err = root.Seek(0, io.SeekStart)
	require.NoError(t, err)
	entries, err = root.Readdir(-1)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	assert.Equal(t, "aaa.txt", entries[0].Name())
	assert.Equal(t, "bbb.txt", entries[1].Name())
}

func initTestState(t *testing.T) (*sql.DB, string) {
	t.Helper()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	db, err := sql.Open("sqlite3", dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	_, err = db.Exec(schema)
	require.NoError(t, err)

	blobsDir := filepath.Join(tmpDir, "blobs")
	require.NoError(t, os.MkdirAll(blobsDir, 0755))
	require.NoError(t, initBlobDirs(blobsDir))

	return db, blobsDir
}

func TestChecksum(t *testing.T) {
	db, blobsDir := initTestState(t)
	ctx := t.Context()

	// Checksum is stored on write
	content := []byte("test content for checksum")
	expectedHash := sha256.Sum256(content)
	expectedChecksum := hex.EncodeToString(expectedHash[:])

	f, err := openFile(ctx, db, blobsDir, "/checksum.txt", os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)
	_, err = f.Write(content)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	var storedChecksum string
	err = db.QueryRow(`SELECT checksum FROM blobs WHERE id = ?`, f.blobID).Scan(&storedChecksum)
	require.NoError(t, err)
	assert.Equal(t, expectedChecksum, storedChecksum)

	// Checksum is verified on read
	f, err = openFile(ctx, db, blobsDir, "/checksum.txt", os.O_RDONLY, 0)
	require.NoError(t, err)
	readContent, err := io.ReadAll(f)
	require.NoError(t, err)
	assert.Equal(t, content, readContent)
	require.NoError(t, f.Close())

	// Multiple writes produce correct checksum
	multiContent := []byte("first chunksecond chunk")
	multiHash := sha256.Sum256(multiContent)
	multiChecksum := hex.EncodeToString(multiHash[:])

	f, err = openFile(ctx, db, blobsDir, "/multi.txt", os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)
	_, err = f.Write([]byte("first chunk"))
	require.NoError(t, err)
	_, err = f.Write([]byte("second chunk"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	err = db.QueryRow(`SELECT checksum FROM blobs WHERE id = ?`, f.blobID).Scan(&storedChecksum)
	require.NoError(t, err)
	assert.Equal(t, multiChecksum, storedChecksum)

	// Corrupted file fails checksum verification on open
	f, err = openFile(ctx, db, blobsDir, "/corrupt.txt", os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)
	_, err = f.Write([]byte("original content"))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	blobID := f.blobID

	blobPath := blobFilePath(blobsDir, blobID)
	require.NoError(t, os.WriteFile(blobPath, []byte("corrupted content"), 0644))

	_, err = openFile(ctx, db, blobsDir, "/corrupt.txt", os.O_RDONLY, 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "checksum verification failed")
}

func TestChecksumDeduplication(t *testing.T) {
	db, blobsDir := initTestState(t)
	ctx := t.Context()

	content := []byte("duplicate content")

	f1, err := openFile(ctx, db, blobsDir, "/file1.txt", os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)
	_, err = f1.Write(content)
	require.NoError(t, err)
	require.NoError(t, f1.Close())
	firstBlobID := f1.blobID

	f2, err := openFile(ctx, db, blobsDir, "/file2.txt", os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)
	_, err = f2.Write(content)
	require.NoError(t, err)
	require.NoError(t, f2.Close())

	var file1BlobID, file2BlobID string
	err = db.QueryRow(`SELECT blob_id FROM files WHERE path = ? ORDER BY version DESC LIMIT 1`, "/file1.txt").Scan(&file1BlobID)
	require.NoError(t, err)
	err = db.QueryRow(`SELECT blob_id FROM files WHERE path = ? ORDER BY version DESC LIMIT 1`, "/file2.txt").Scan(&file2BlobID)
	require.NoError(t, err)
	assert.Equal(t, file1BlobID, file2BlobID, "both files should reference the same blob")
	assert.Equal(t, firstBlobID, file1BlobID, "should reuse the first blob")

	var blobCount int
	err = db.QueryRow(`SELECT COUNT(*) FROM blobs WHERE local_written = 1`).Scan(&blobCount)
	require.NoError(t, err)
	assert.Equal(t, 1, blobCount, "only one blob should exist")

	f1Read, err := openFile(ctx, db, blobsDir, "/file1.txt", os.O_RDONLY, 0)
	require.NoError(t, err)
	content1, err := io.ReadAll(f1Read)
	require.NoError(t, err)
	f1Read.Close()

	f2Read, err := openFile(ctx, db, blobsDir, "/file2.txt", os.O_RDONLY, 0)
	require.NoError(t, err)
	content2, err := io.ReadAll(f2Read)
	require.NoError(t, err)
	f2Read.Close()

	assert.Equal(t, content, content1)
	assert.Equal(t, content, content2)

	f3, err := openFile(ctx, db, blobsDir, "/file1.txt", os.O_WRONLY|os.O_TRUNC, 0644)
	require.NoError(t, err)
	_, err = f3.Write([]byte("different content"))
	require.NoError(t, err)
	require.NoError(t, f3.Close())

	var newBlobID string
	err = db.QueryRow(`SELECT blob_id FROM files WHERE path = ? ORDER BY version DESC LIMIT 1`, "/file1.txt").Scan(&newBlobID)
	require.NoError(t, err)
	assert.NotEqual(t, firstBlobID, newBlobID, "different content should use different blob")

	f4, err := openFile(ctx, db, blobsDir, "/file1.txt", os.O_WRONLY|os.O_TRUNC, 0644)
	require.NoError(t, err)
	_, err = f4.Write(content)
	require.NoError(t, err)
	require.NoError(t, f4.Close())

	var reusedBlobID string
	err = db.QueryRow(`SELECT blob_id FROM files WHERE path = ? ORDER BY version DESC LIMIT 1`, "/file1.txt").Scan(&reusedBlobID)
	require.NoError(t, err)
	assert.Equal(t, firstBlobID, reusedBlobID, "writing original content again should reuse first blob")
}
