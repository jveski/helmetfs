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
	f, err := NewFile(ctx, db, blobsDir, "/aaa.txt", os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)
	n, err := f.Write([]byte("hello world"))
	require.NoError(t, err)
	assert.Equal(t, 11, n)
	require.NoError(t, f.Close())

	f, err = NewFile(ctx, db, blobsDir, "/bbb.txt", os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)
	_, err = f.Write([]byte("content b"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Read and seek within a file
	f, err = NewFile(ctx, db, blobsDir, "/aaa.txt", os.O_RDONLY, 0)
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
	root := &File{db: db, path: "/", isDir: true}

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

	// Readdir on a non-directory should return ErrInvalid
	_, err = f.Readdir(-1)
	assert.ErrorIs(t, err, os.ErrInvalid)

	// Readdir on subdirectory (tests prefix != "/" branch)
	_, err = db.Exec(`INSERT INTO files (created_at, version, path, mode, mod_time, is_dir) VALUES (1, 1, ?, ?, 0, 1)`,
		"/subdir", 0755)
	require.NoError(t, err)

	f, err = NewFile(ctx, db, blobsDir, "/subdir/child1.txt", os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	f, err = NewFile(ctx, db, blobsDir, "/subdir/child2.txt", os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Create nested dir and file (should NOT appear in subdir's Readdir)
	_, err = db.Exec(`INSERT INTO files (created_at, version, path, mode, mod_time, is_dir) VALUES (1, 1, ?, ?, 0, 1)`,
		"/subdir/nested", 0755)
	require.NoError(t, err)

	f, err = NewFile(ctx, db, blobsDir, "/subdir/nested/deep.txt", os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	subdir := &File{db: db, path: "/subdir", isDir: true}
	entries, err = subdir.Readdir(-1)
	require.NoError(t, err)
	require.Len(t, entries, 3) // child1.txt, child2.txt, nested (but not nested/deep.txt)

	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	assert.Contains(t, names, "child1.txt")
	assert.Contains(t, names, "child2.txt")
	assert.Contains(t, names, "nested")
	assert.NotContains(t, names, "deep.txt")
}

func TestFileSeek(t *testing.T) {
	db, blobsDir := initTestState(t)
	ctx := t.Context()

	// Create a file with known content
	content := []byte("0123456789")
	f, err := NewFile(ctx, db, blobsDir, "/seek.txt", os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)
	_, err = f.Write(content)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	t.Run("file SeekStart", func(t *testing.T) {
		f, err := NewFile(ctx, db, blobsDir, "/seek.txt", os.O_RDONLY, 0)
		require.NoError(t, err)
		defer f.Close()

		pos, err := f.Seek(3, io.SeekStart)
		require.NoError(t, err)
		assert.Equal(t, int64(3), pos)

		buf := make([]byte, 3)
		n, err := f.Read(buf)
		require.NoError(t, err)
		assert.Equal(t, 3, n)
		assert.Equal(t, "345", string(buf))
	})

	t.Run("file SeekCurrent", func(t *testing.T) {
		f, err := NewFile(ctx, db, blobsDir, "/seek.txt", os.O_RDONLY, 0)
		require.NoError(t, err)
		defer f.Close()

		// Read 3 bytes to advance position
		buf := make([]byte, 3)
		_, err = f.Read(buf)
		require.NoError(t, err)
		assert.Equal(t, "012", string(buf))

		// Seek 2 forward from current position (now at 3, seek to 5)
		pos, err := f.Seek(2, io.SeekCurrent)
		require.NoError(t, err)
		assert.Equal(t, int64(5), pos)

		buf = make([]byte, 3)
		n, err := f.Read(buf)
		require.NoError(t, err)
		assert.Equal(t, 3, n)
		assert.Equal(t, "567", string(buf))

		// Seek backward from current position
		pos, err = f.Seek(-5, io.SeekCurrent)
		require.NoError(t, err)
		assert.Equal(t, int64(3), pos)

		buf = make([]byte, 2)
		n, err = f.Read(buf)
		require.NoError(t, err)
		assert.Equal(t, 2, n)
		assert.Equal(t, "34", string(buf))
	})

	t.Run("file SeekEnd", func(t *testing.T) {
		f, err := NewFile(ctx, db, blobsDir, "/seek.txt", os.O_RDONLY, 0)
		require.NoError(t, err)
		defer f.Close()

		// Seek to 3 bytes before end
		pos, err := f.Seek(-3, io.SeekEnd)
		require.NoError(t, err)
		assert.Equal(t, int64(7), pos)

		buf := make([]byte, 3)
		n, err := f.Read(buf)
		require.NoError(t, err)
		assert.Equal(t, 3, n)
		assert.Equal(t, "789", string(buf))

		// Seek to end
		pos, err = f.Seek(0, io.SeekEnd)
		require.NoError(t, err)
		assert.Equal(t, int64(10), pos)
	})

	t.Run("dir SeekStart", func(t *testing.T) {
		// Create directory entries
		for _, name := range []string{"a.txt", "b.txt", "c.txt", "d.txt"} {
			f, err := NewFile(ctx, db, blobsDir, "/"+name, os.O_CREATE|os.O_WRONLY, 0644)
			require.NoError(t, err)
			require.NoError(t, f.Close())
		}

		dir := &File{db: db, path: "/", isDir: true}

		// Read first 2 entries
		entries, err := dir.Readdir(2)
		require.NoError(t, err)
		require.Len(t, entries, 2)

		// Seek back to start
		pos, err := dir.Seek(0, io.SeekStart)
		require.NoError(t, err)
		assert.Equal(t, int64(0), pos)

		// Read again from beginning
		entries, err = dir.Readdir(2)
		require.NoError(t, err)
		require.Len(t, entries, 2)
		assert.Equal(t, "a.txt", entries[0].Name())

		// Seek to position 2
		pos, err = dir.Seek(2, io.SeekStart)
		require.NoError(t, err)
		assert.Equal(t, int64(2), pos)

		entries, err = dir.Readdir(1)
		require.NoError(t, err)
		assert.Equal(t, "c.txt", entries[0].Name())
	})

	t.Run("dir SeekCurrent", func(t *testing.T) {
		dir := &File{db: db, path: "/", isDir: true}

		// Read 2 entries (position is now 2)
		_, err := dir.Readdir(2)
		require.NoError(t, err)

		// Seek 1 forward from current position (now at 3)
		pos, err := dir.Seek(1, io.SeekCurrent)
		require.NoError(t, err)
		assert.Equal(t, int64(3), pos)

		entries, err := dir.Readdir(1)
		require.NoError(t, err)
		assert.Equal(t, "d.txt", entries[0].Name())

		// Seek backward from current position
		pos, err = dir.Seek(-3, io.SeekCurrent)
		require.NoError(t, err)
		assert.Equal(t, int64(1), pos)

		entries, err = dir.Readdir(1)
		require.NoError(t, err)
		assert.Equal(t, "b.txt", entries[0].Name())
	})

	t.Run("dir SeekEnd", func(t *testing.T) {
		dir := &File{db: db, path: "/", isDir: true}

		// Seek to end (should be total count of entries)
		pos, err := dir.Seek(0, io.SeekEnd)
		require.NoError(t, err)
		// There are 5 files in root: seek.txt, a.txt, b.txt, c.txt, d.txt
		assert.Equal(t, int64(5), pos)

		// Reading should return EOF
		_, err = dir.Readdir(1)
		assert.ErrorIs(t, err, io.EOF)

		// Seek 2 back from end
		pos, err = dir.Seek(-2, io.SeekEnd)
		require.NoError(t, err)
		assert.Equal(t, int64(3), pos)

		entries, err := dir.Readdir(1)
		require.NoError(t, err)
		assert.Equal(t, "d.txt", entries[0].Name())
	})

	t.Run("empty file seek", func(t *testing.T) {
		// Test seek on a file with no readFile (empty/newly created)
		f, err := NewFile(ctx, db, blobsDir, "/empty.txt", os.O_CREATE|os.O_RDONLY, 0644)
		require.NoError(t, err)
		defer f.Close()

		// Seek(0, SeekStart) should succeed on empty file
		pos, err := f.Seek(0, io.SeekStart)
		require.NoError(t, err)
		assert.Equal(t, int64(0), pos)

		// Non-zero seek on empty file returns EOF
		pos, err = f.Seek(5, io.SeekStart)
		assert.ErrorIs(t, err, io.EOF)
	})
}

func TestFileChecksum(t *testing.T) {
	db, blobsDir := initTestState(t)
	ctx := t.Context()

	// Checksum is stored on write
	content := []byte("test content for checksum")
	expectedHash := sha256.Sum256(content)
	expectedChecksum := hex.EncodeToString(expectedHash[:])

	f, err := NewFile(ctx, db, blobsDir, "/checksum.txt", os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)
	_, err = f.Write(content)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	var storedChecksum string
	err = db.QueryRow(`SELECT checksum FROM blobs WHERE id = ?`, f.blobID).Scan(&storedChecksum)
	require.NoError(t, err)
	assert.Equal(t, expectedChecksum, storedChecksum)

	// Checksum is verified on read
	f, err = NewFile(ctx, db, blobsDir, "/checksum.txt", os.O_RDONLY, 0)
	require.NoError(t, err)
	readContent, err := io.ReadAll(f)
	require.NoError(t, err)
	assert.Equal(t, content, readContent)
	require.NoError(t, f.Close())

	// Multiple writes produce correct checksum
	multiContent := []byte("first chunksecond chunk")
	multiHash := sha256.Sum256(multiContent)
	multiChecksum := hex.EncodeToString(multiHash[:])

	f, err = NewFile(ctx, db, blobsDir, "/multi.txt", os.O_CREATE|os.O_WRONLY, 0644)
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
	f, err = NewFile(ctx, db, blobsDir, "/corrupt.txt", os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)
	_, err = f.Write([]byte("original content"))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	blobID := f.blobID

	blobPath := blobFilePath(blobsDir, blobID)
	require.NoError(t, os.WriteFile(blobPath, []byte("corrupted content"), 0644))

	_, err = NewFile(ctx, db, blobsDir, "/corrupt.txt", os.O_RDONLY, 0)
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
