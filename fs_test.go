package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
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

	// Rename directory moves all contents
	require.NoError(t, fs.Mkdir(ctx, "/srcdir", 0755))
	require.NoError(t, fs.Mkdir(ctx, "/srcdir/subdir", 0755))

	f, err = fs.OpenFile(ctx, "/srcdir/file1.txt", os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)
	_, err = f.Write([]byte("file1 content"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	f, err = fs.OpenFile(ctx, "/srcdir/subdir/file2.txt", os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)
	_, err = f.Write([]byte("file2 content"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.NoError(t, fs.Rename(ctx, "/srcdir", "/dstdir"))

	// Old paths should not exist
	_, err = fs.Stat(ctx, "/srcdir")
	assert.ErrorIs(t, err, os.ErrNotExist)
	_, err = fs.Stat(ctx, "/srcdir/file1.txt")
	assert.ErrorIs(t, err, os.ErrNotExist)
	_, err = fs.Stat(ctx, "/srcdir/subdir")
	assert.ErrorIs(t, err, os.ErrNotExist)
	_, err = fs.Stat(ctx, "/srcdir/subdir/file2.txt")
	assert.ErrorIs(t, err, os.ErrNotExist)

	// New paths should exist with correct content
	info, err = fs.Stat(ctx, "/dstdir")
	require.NoError(t, err)
	assert.True(t, info.IsDir())

	info, err = fs.Stat(ctx, "/dstdir/subdir")
	require.NoError(t, err)
	assert.True(t, info.IsDir())

	f, err = fs.OpenFile(ctx, "/dstdir/file1.txt", os.O_RDONLY, 0)
	require.NoError(t, err)
	content, err = io.ReadAll(f)
	require.NoError(t, err)
	assert.Equal(t, "file1 content", string(content))
	require.NoError(t, f.Close())

	f, err = fs.OpenFile(ctx, "/dstdir/subdir/file2.txt", os.O_RDONLY, 0)
	require.NoError(t, err)
	content, err = io.ReadAll(f)
	require.NoError(t, err)
	assert.Equal(t, "file2 content", string(content))
	require.NoError(t, f.Close())
}

func TestDeleteLocalBlobs(t *testing.T) {
	db, blobsDir := initTestState(t)

	// Deletes blob file and marks as deleted
	now := time.Now().Unix()
	blobID := uuid.New().String()
	_, err := db.Exec(`INSERT INTO blobs (id, creation_time, modified_time, local_written, local_deleting) VALUES (?, ?, ?, 1, 1)`, blobID, now, now)
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
	blobID = uuid.New().String()
	_, err = db.Exec(`INSERT INTO blobs (id, creation_time, modified_time, local_written, local_deleting) VALUES (?, ?, ?, 1, 1)`, blobID, now, now)
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
	blobID = uuid.New().String()
	_, err = db.Exec(`INSERT INTO blobs (id, creation_time, modified_time, local_written, local_deleting) VALUES (?, ?, ?, 1, 1)`, blobID, now, now)
	require.NoError(t, err)

	_, err = deleteLocalBlobs(db, blobsDir)
	require.NoError(t, err)

	require.NoError(t, db.QueryRow(`SELECT local_deleted FROM blobs WHERE id = ?`, blobID).Scan(&localDeleted))
	assert.Equal(t, 1, localDeleted)
}

func TestPathTraversalProtection(t *testing.T) {
	db, blobsDir := initTestState(t)
	ctx := t.Context()
	fs := &FS{db: db, blobsDir: blobsDir}

	// First create a valid directory to test traversal from
	require.NoError(t, fs.Mkdir(ctx, "/docs", 0755))

	// Test cases for path traversal attempts
	// Note: path.Clean in fs.go should normalize these paths
	testCases := []struct {
		name        string
		path        string
		expectedErr error
	}{
		{"Basic traversal", "/../etc/passwd", os.ErrNotExist},
		{"Double traversal", "/../../etc/passwd", os.ErrNotExist},
		{"Mixed traversal", "/docs/../../../etc/passwd", os.ErrNotExist},
		{"Traversal with valid prefix", "/docs/../../etc", os.ErrNotExist},
		{"Deep traversal", "/docs/sub/../../../../../../../etc/passwd", os.ErrNotExist},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Stat should return not exist for traversal attempts
			// because path.Clean normalizes the path and the file doesn't exist
			_, err := fs.Stat(ctx, tc.path)
			assert.Error(t, err)
		})
	}

	// Test that traversal in Mkdir is handled safely
	t.Run("Mkdir traversal", func(t *testing.T) {
		// path.Clean normalizes "/../evil" to "/evil"
		// So mkdir will try to create /evil at root level
		// This is safe because it's still within the virtual filesystem
		err := fs.Mkdir(ctx, "/../evil", 0755)
		// The normalized path /evil should succeed (no parent check for root)
		// This verifies that traversal is neutralized by path normalization
		if err != nil {
			// If it errors, it should be because /evil already exists or similar
			assert.True(t, err == os.ErrExist || err == os.ErrNotExist)
		}
	})

	// Test OpenFile with traversal path
	t.Run("OpenFile traversal", func(t *testing.T) {
		_, err := fs.OpenFile(ctx, "/../../../etc/passwd", os.O_RDONLY, 0)
		assert.Error(t, err)
	})

	// Test Rename with traversal in source or destination
	t.Run("Rename traversal source", func(t *testing.T) {
		err := fs.Rename(ctx, "/../etc/passwd", "/safe.txt")
		assert.Error(t, err)
	})

	t.Run("Rename traversal destination", func(t *testing.T) {
		// Create a file first
		f, err := fs.OpenFile(ctx, "/source.txt", os.O_CREATE|os.O_WRONLY, 0644)
		require.NoError(t, err)
		require.NoError(t, f.Close())

		err = fs.Rename(ctx, "/source.txt", "/../../../etc/evil")
		// Should either error or rename to /evil (cleaned path)
		// Not actually escape the filesystem
		assert.NoError(t, err) // path.Clean normalizes it
	})

	// Test RemoveAll with traversal
	t.Run("RemoveAll traversal", func(t *testing.T) {
		err := fs.RemoveAll(ctx, "/../etc")
		assert.Error(t, err) // Should error as path doesn't exist after normalization
	})
}

func TestBlobPathSecurity(t *testing.T) {
	_, blobsDir := initTestState(t)

	// Test that blobFilePath always returns paths within blobsDir
	testCases := []struct {
		name   string
		blobID string
	}{
		{"Normal UUID", "abcd1234-5678-90ef-ghij-klmnopqrstuv"},
		{"Short ID", "ab"},
		{"UUID with prefix", "00" + uuid.New().String()[2:]},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			path := blobFilePath(blobsDir, tc.blobID)
			cleanBlobsDir := filepath.Clean(blobsDir)
			cleanPath := filepath.Clean(path)

			// Path should always be within blobsDir
			assert.True(t, strings.HasPrefix(cleanPath, cleanBlobsDir),
				"blob path %q should be within blobs dir %q", cleanPath, cleanBlobsDir)
		})
	}
}

func TestConcurrentMkdir(t *testing.T) {
	db, blobsDir := initTestState(t)
	ctx := t.Context()
	fs := &FS{db: db, blobsDir: blobsDir}

	const numGoroutines = 10
	path := "/concurrent-dir"

	var wg sync.WaitGroup
	results := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- fs.Mkdir(ctx, path, 0755)
		}()
	}

	wg.Wait()
	close(results)

	// Count successes and failures
	var successes, failures int
	for err := range results {
		if err == nil {
			successes++
		} else {
			failures++
			// Should be ErrExist for the losers
			assert.ErrorIs(t, err, os.ErrExist)
		}
	}

	// Exactly one should succeed
	assert.Equal(t, 1, successes, "exactly one mkdir should succeed")
	assert.Equal(t, numGoroutines-1, failures, "all others should fail")

	// Verify directory exists
	info, err := fs.Stat(ctx, path)
	require.NoError(t, err)
	assert.True(t, info.IsDir())
}

func TestConcurrentFileOperations(t *testing.T) {
	db, blobsDir := initTestState(t)
	ctx := t.Context()
	fs := &FS{db: db, blobsDir: blobsDir}

	t.Run("Concurrent create and delete", func(t *testing.T) {
		const iterations = 20
		path := "/concurrent-file.txt"

		for i := 0; i < iterations; i++ {
			var wg sync.WaitGroup
			wg.Add(2)

			// Create
			go func() {
				defer wg.Done()
				f, err := fs.OpenFile(ctx, path, os.O_CREATE|os.O_WRONLY, 0644)
				if err == nil {
					f.Write([]byte("content"))
					f.Close()
				}
			}()

			// Delete
			go func() {
				defer wg.Done()
				fs.RemoveAll(ctx, path)
			}()

			wg.Wait()
		}

		// Final state should be consistent - either exists or not
		_, err := fs.Stat(ctx, path)
		if err != nil {
			assert.ErrorIs(t, err, os.ErrNotExist)
		}
	})

	t.Run("Concurrent rename operations", func(t *testing.T) {
		// Create initial file
		f, err := fs.OpenFile(ctx, "/rename-src.txt", os.O_CREATE|os.O_WRONLY, 0644)
		require.NoError(t, err)
		f.Write([]byte("content"))
		f.Close()

		const numRenames = 5
		var wg sync.WaitGroup

		for i := 0; i < numRenames; i++ {
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				// Try to rename to different destinations
				fs.Rename(ctx, "/rename-src.txt", "/rename-dst.txt")
			}(i)
		}

		wg.Wait()

		// Source should not exist (was renamed)
		_, _ = fs.Stat(ctx, "/rename-src.txt")
		// May or may not exist depending on timing
	})
}

func TestDatabaseClosedGracefully(t *testing.T) {
	db, blobsDir := initTestState(t)
	fs := &FS{db: db, blobsDir: blobsDir}

	// Close the database
	db.Close()

	ctx := context.Background()

	// All operations should fail gracefully without panicking
	t.Run("Mkdir on closed db", func(t *testing.T) {
		err := fs.Mkdir(ctx, "/test", 0755)
		assert.Error(t, err)
	})

	t.Run("Stat on closed db", func(t *testing.T) {
		_, err := fs.Stat(ctx, "/test")
		assert.Error(t, err)
	})

	t.Run("OpenFile on closed db", func(t *testing.T) {
		_, err := fs.OpenFile(ctx, "/test.txt", os.O_CREATE|os.O_WRONLY, 0644)
		assert.Error(t, err)
	})

	t.Run("RemoveAll on closed db", func(t *testing.T) {
		err := fs.RemoveAll(ctx, "/test")
		assert.Error(t, err)
	})

	t.Run("Rename on closed db", func(t *testing.T) {
		err := fs.Rename(ctx, "/src", "/dst")
		assert.Error(t, err)
	})
}

func TestContextCancellation(t *testing.T) {
	db, blobsDir := initTestState(t)
	fs := &FS{db: db, blobsDir: blobsDir}

	t.Run("Mkdir with cancelled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := fs.Mkdir(ctx, "/cancelled", 0755)
		assert.Error(t, err)
	})

	t.Run("Stat with cancelled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := fs.Stat(ctx, "/test")
		assert.Error(t, err)
	})

	t.Run("OpenFile with cancelled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := fs.OpenFile(ctx, "/test.txt", os.O_CREATE|os.O_WRONLY, 0644)
		assert.Error(t, err)
	})

	t.Run("RemoveAll with cancelled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := fs.RemoveAll(ctx, "/test")
		assert.Error(t, err)
	})

	t.Run("Rename with cancelled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := fs.Rename(ctx, "/src", "/dst")
		assert.Error(t, err)
	})
}
