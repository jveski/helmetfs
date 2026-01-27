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

func TestFileChecksum(t *testing.T) {
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

func initTestState(t *testing.T) (*sql.DB, string) {
	t.Helper()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	db, err := sql.Open("sqlite3", "file:"+dbPath+"?_journal_mode=WAL&_busy_timeout=15000&_txlock=immediate")
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	_, err = db.Exec(schema)
	require.NoError(t, err)

	blobsDir := filepath.Join(tmpDir, "blobs")
	require.NoError(t, os.MkdirAll(blobsDir, 0755))
	require.NoError(t, initBlobDirs(blobsDir))

	return db, blobsDir
}
