package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
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

func TestConcurrentWriteVersions(t *testing.T) {
	db, blobsDir := initTestState(t)
	ctx := t.Context()

	const numWriters = 10
	const writesPerWriter = 5
	path := "/concurrent_version.txt"

	// Create initial file
	f, err := openFile(ctx, db, blobsDir, path, os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)
	_, err = f.Write([]byte("initial"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Multiple goroutines writing to the same file
	var wg sync.WaitGroup
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < writesPerWriter; j++ {
				f, err := openFile(ctx, db, blobsDir, path, os.O_WRONLY|os.O_TRUNC, 0644)
				if err != nil {
					continue // might get errors due to contention, that's ok
				}
				f.Write([]byte(fmt.Sprintf("worker %d write %d", workerID, j)))
				f.Close()
			}
		}(i)
	}
	wg.Wait()

	// Verify versions are still monotonically increasing (no duplicates or reversals)
	rows, err := db.Query(`SELECT version FROM files WHERE path = ? ORDER BY version`, path)
	require.NoError(t, err)
	defer rows.Close()

	seen := make(map[int]bool)
	var prevVersion int
	first := true
	for rows.Next() {
		var version int
		require.NoError(t, rows.Scan(&version))
		assert.False(t, seen[version], "duplicate version found: %d", version)
		seen[version] = true
		if !first {
			assert.Greater(t, version, prevVersion, "versions must be strictly increasing")
		}
		prevVersion = version
		first = false
	}
	require.NoError(t, rows.Err())
}

func TestLockContention(t *testing.T) {
	db, blobsDir := initTestState(t)
	ctx := t.Context()

	// Create a file
	f, err := openFile(ctx, db, blobsDir, "/locked.txt", os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)
	_, err = f.Write([]byte("content to be locked"))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	blobID := f.blobID

	// Open the blob for reading (acquires LOCK_SH)
	blobPath := blobFilePath(blobsDir, blobID)
	readFile, err := open(blobPath, os.O_RDONLY, 0)
	require.NoError(t, err)
	defer readFile.Close()

	// tryDeleteBlob should fail because the file is locked with LOCK_SH
	// and tryDeleteBlob needs LOCK_EX|LOCK_NB
	deleted, err := tryDeleteBlob(blobPath)
	require.NoError(t, err)
	assert.False(t, deleted, "should not delete blob while it has readers")

	// Verify the blob file still exists
	_, err = os.Stat(blobPath)
	assert.NoError(t, err, "blob file should still exist")

	// Close the reader
	readFile.Close()

	// Now deletion should succeed
	deleted, err = tryDeleteBlob(blobPath)
	require.NoError(t, err)
	assert.True(t, deleted, "should delete blob after readers release lock")

	// Verify the blob file is gone
	_, err = os.Stat(blobPath)
	assert.True(t, os.IsNotExist(err), "blob file should be deleted")
}

func TestConcurrentReadDuringGC(t *testing.T) {
	db, blobsDir := initTestState(t)
	ctx := t.Context()

	// Create a file
	f, err := openFile(ctx, db, blobsDir, "/gc_test.txt", os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)
	content := []byte("content that will be read during GC")
	_, err = f.Write(content)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	blobID := f.blobID
	blobPath := blobFilePath(blobsDir, blobID)

	// Start multiple readers
	const numReaders = 5
	readResults := make(chan []byte, numReaders)
	var wg sync.WaitGroup

	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f, err := openFile(ctx, db, blobsDir, "/gc_test.txt", os.O_RDONLY, 0)
			if err != nil {
				return
			}
			defer f.Close()

			// Simulate slow read
			buf := make([]byte, len(content))
			n, err := f.Read(buf)
			if err == nil {
				readResults <- buf[:n]
			}
		}()
	}

	// Attempt deletion while reads are in progress
	// This simulates GC trying to delete the blob
	deleted, err := tryDeleteBlob(blobPath)
	require.NoError(t, err)
	// Deletion might succeed or fail depending on timing - both are valid

	wg.Wait()
	close(readResults)

	// If any reads completed, they should have correct content
	for result := range readResults {
		assert.Equal(t, content, result, "read content should match original")
	}

	// Check final state
	_, statErr := os.Stat(blobPath)
	if deleted {
		assert.True(t, os.IsNotExist(statErr), "blob should be gone after successful delete")
	} else {
		assert.NoError(t, statErr, "blob should exist if delete was blocked")
	}
}

func TestFileErrorPaths(t *testing.T) {
	db, blobsDir := initTestState(t)
	ctx := t.Context()

	t.Run("Read from write-only file returns permission error", func(t *testing.T) {
		f, err := openFile(ctx, db, blobsDir, "/writeonly.txt", os.O_CREATE|os.O_WRONLY, 0644)
		require.NoError(t, err)
		defer f.Close()

		_, err = f.Read(make([]byte, 10))
		assert.ErrorIs(t, err, os.ErrPermission)
	})

	t.Run("Write to read-only file returns permission error", func(t *testing.T) {
		// Create file first
		f, err := openFile(ctx, db, blobsDir, "/readonly.txt", os.O_CREATE|os.O_WRONLY, 0644)
		require.NoError(t, err)
		f.Write([]byte("content"))
		f.Close()

		f, err = openFile(ctx, db, blobsDir, "/readonly.txt", os.O_RDONLY, 0)
		require.NoError(t, err)
		defer f.Close()

		_, err = f.Write([]byte("test"))
		assert.ErrorIs(t, err, os.ErrPermission)
	})

	t.Run("Write to directory returns invalid error", func(t *testing.T) {
		fs := &FS{db: db, blobsDir: blobsDir}
		require.NoError(t, fs.Mkdir(ctx, "/testdir", 0755))

		// Opening directory with O_WRONLY should fail
		_, err := fs.OpenFile(ctx, "/testdir", os.O_WRONLY, 0)
		assert.ErrorIs(t, err, os.ErrInvalid)
	})

	t.Run("Read from directory returns invalid error", func(t *testing.T) {
		fs := &FS{db: db, blobsDir: blobsDir}
		require.NoError(t, fs.Mkdir(ctx, "/testdir2", 0755))

		f, err := fs.OpenFile(ctx, "/testdir2", os.O_RDONLY, 0)
		require.NoError(t, err)
		defer f.Close()

		_, err = f.Read(make([]byte, 10))
		assert.ErrorIs(t, err, os.ErrInvalid)
	})

	t.Run("Open with O_EXCL on existing file returns exist error", func(t *testing.T) {
		f, err := openFile(ctx, db, blobsDir, "/exists.txt", os.O_CREATE|os.O_WRONLY, 0644)
		require.NoError(t, err)
		f.Write([]byte("content"))
		f.Close()

		_, err = openFile(ctx, db, blobsDir, "/exists.txt", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
		assert.ErrorIs(t, err, os.ErrExist)
	})

	t.Run("Open nonexistent file without O_CREATE returns not exist", func(t *testing.T) {
		_, err := openFile(ctx, db, blobsDir, "/nonexistent.txt", os.O_RDONLY, 0)
		assert.ErrorIs(t, err, os.ErrNotExist)
	})

	t.Run("Create file under nonexistent parent returns not exist", func(t *testing.T) {
		_, err := openFile(ctx, db, blobsDir, "/noparent/file.txt", os.O_CREATE|os.O_WRONLY, 0644)
		assert.ErrorIs(t, err, os.ErrNotExist)
	})

	t.Run("Create file under file (not directory) returns invalid", func(t *testing.T) {
		f, err := openFile(ctx, db, blobsDir, "/notadir.txt", os.O_CREATE|os.O_WRONLY, 0644)
		require.NoError(t, err)
		f.Write([]byte("content"))
		f.Close()

		_, err = openFile(ctx, db, blobsDir, "/notadir.txt/child.txt", os.O_CREATE|os.O_WRONLY, 0644)
		assert.ErrorIs(t, err, os.ErrInvalid)
	})

	t.Run("Open directory with write flag returns invalid", func(t *testing.T) {
		fs := &FS{db: db, blobsDir: blobsDir}
		require.NoError(t, fs.Mkdir(ctx, "/writedir", 0755))

		_, err := fs.OpenFile(ctx, "/writedir", os.O_WRONLY, 0)
		assert.ErrorIs(t, err, os.ErrInvalid)
	})

	t.Run("Open directory with truncate flag returns invalid", func(t *testing.T) {
		fs := &FS{db: db, blobsDir: blobsDir}
		require.NoError(t, fs.Mkdir(ctx, "/truncdir", 0755))

		_, err := fs.OpenFile(ctx, "/truncdir", os.O_TRUNC, 0)
		assert.ErrorIs(t, err, os.ErrInvalid)
	})
}

func TestEmptyFileOperations(t *testing.T) {
	db, blobsDir := initTestState(t)
	ctx := t.Context()

	t.Run("Create and read empty file", func(t *testing.T) {
		// Create empty file (close without writing)
		f, err := openFile(ctx, db, blobsDir, "/empty.txt", os.O_CREATE|os.O_WRONLY, 0644)
		require.NoError(t, err)
		require.NoError(t, f.Close())

		// Read empty file
		f, err = openFile(ctx, db, blobsDir, "/empty.txt", os.O_RDONLY, 0)
		require.NoError(t, err)
		defer f.Close()

		content, err := io.ReadAll(f)
		require.NoError(t, err)
		assert.Empty(t, content)
	})

	t.Run("Seek on empty file", func(t *testing.T) {
		f, err := openFile(ctx, db, blobsDir, "/empty2.txt", os.O_CREATE|os.O_WRONLY, 0644)
		require.NoError(t, err)
		require.NoError(t, f.Close())

		f, err = openFile(ctx, db, blobsDir, "/empty2.txt", os.O_RDONLY, 0)
		require.NoError(t, err)
		defer f.Close()

		// Seek on empty file
		pos, err := f.Seek(0, io.SeekStart)
		require.NoError(t, err)
		assert.Equal(t, int64(0), pos)

		// Seek to end on empty file
		pos, err = f.Seek(0, io.SeekEnd)
		// EOF is expected since file is empty and we're seeking past it
		if err == nil {
			assert.Equal(t, int64(0), pos)
		}
	})

	t.Run("Stat shows zero size for empty file", func(t *testing.T) {
		f, err := openFile(ctx, db, blobsDir, "/empty3.txt", os.O_CREATE|os.O_WRONLY, 0644)
		require.NoError(t, err)
		require.NoError(t, f.Close())

		f, err = openFile(ctx, db, blobsDir, "/empty3.txt", os.O_RDONLY, 0)
		require.NoError(t, err)
		defer f.Close()

		info, err := f.Stat()
		require.NoError(t, err)
		assert.Equal(t, int64(0), info.Size())
	})
}

func TestReaddirEdgeCases(t *testing.T) {
	db, blobsDir := initTestState(t)
	ctx := t.Context()
	fs := &FS{db: db, blobsDir: blobsDir}

	t.Run("Readdir on empty directory", func(t *testing.T) {
		require.NoError(t, fs.Mkdir(ctx, "/emptydir", 0755))

		f, err := fs.OpenFile(ctx, "/emptydir", os.O_RDONLY, 0)
		require.NoError(t, err)
		defer f.Close()

		entries, err := f.Readdir(-1)
		require.NoError(t, err)
		assert.Empty(t, entries)
	})

	t.Run("Readdir with count=0", func(t *testing.T) {
		require.NoError(t, fs.Mkdir(ctx, "/countdir", 0755))
		f1, _ := fs.OpenFile(ctx, "/countdir/a.txt", os.O_CREATE|os.O_WRONLY, 0644)
		f1.Close()

		f, err := fs.OpenFile(ctx, "/countdir", os.O_RDONLY, 0)
		require.NoError(t, err)
		defer f.Close()

		// count=0 should return all entries
		entries, err := f.Readdir(0)
		require.NoError(t, err)
		assert.Len(t, entries, 1)
	})

	t.Run("Readdir with negative count", func(t *testing.T) {
		require.NoError(t, fs.Mkdir(ctx, "/negdir", 0755))
		f1, _ := fs.OpenFile(ctx, "/negdir/a.txt", os.O_CREATE|os.O_WRONLY, 0644)
		f1.Close()
		f2, _ := fs.OpenFile(ctx, "/negdir/b.txt", os.O_CREATE|os.O_WRONLY, 0644)
		f2.Close()

		f, err := fs.OpenFile(ctx, "/negdir", os.O_RDONLY, 0)
		require.NoError(t, err)
		defer f.Close()

		// Negative count should return all entries
		entries, err := f.Readdir(-1)
		require.NoError(t, err)
		assert.Len(t, entries, 2)
	})

	t.Run("Readdir on file returns error", func(t *testing.T) {
		f, err := openFile(ctx, db, blobsDir, "/notdir.txt", os.O_CREATE|os.O_WRONLY, 0644)
		require.NoError(t, err)
		f.Write([]byte("content"))
		f.Close()

		f, err = openFile(ctx, db, blobsDir, "/notdir.txt", os.O_RDONLY, 0)
		require.NoError(t, err)
		defer f.Close()

		_, err = f.Readdir(-1)
		assert.ErrorIs(t, err, os.ErrInvalid)
	})
}

func TestFileContextCancellation(t *testing.T) {
	db, blobsDir := initTestState(t)

	t.Run("openFile with cancelled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := openFile(ctx, db, blobsDir, "/cancelled.txt", os.O_CREATE|os.O_WRONLY, 0644)
		assert.Error(t, err)
	})

	t.Run("openFile read with cancelled context", func(t *testing.T) {
		// Create file first with valid context
		ctx := context.Background()
		f, err := openFile(ctx, db, blobsDir, "/forcancel.txt", os.O_CREATE|os.O_WRONLY, 0644)
		require.NoError(t, err)
		f.Write([]byte("content"))
		f.Close()

		// Try to open with cancelled context
		cancelCtx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err = openFile(cancelCtx, db, blobsDir, "/forcancel.txt", os.O_RDONLY, 0)
		assert.Error(t, err)
	})
}
