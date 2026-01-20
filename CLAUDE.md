# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Test Commands

```bash
# Build
go build

# Run all tests
go test ./...

# Run a single test
go test -run TestName

# Run with verbose output
go test -v ./...
```

Note: Some tests require `rclone` to be installed and will be skipped if not available.

## Architecture

HelmetFS is a NAS server that exposes a WebDAV interface backed by SQLite metadata and content-addressable blob storage with optional cloud sync via rclone.

### Core Components

- **main.go**: HTTP server setup, WebDAV handler, background loops for garbage collection, integrity checks, and database backup
- **fs.go**: `FS` struct implementing `webdav.FileSystem` - handles Mkdir, RemoveAll, Rename, Stat, OpenFile
- **file.go**: `file` struct implementing `webdav.File` - handles Read, Write, Seek, Close, Readdir with content-addressable blob storage
- **rclone.go**: Cloud storage sync using rclone subprocess for upload/download/delete operations

### Data Model

Two SQLite tables (see schema.sql):
- **files**: Immutable versioned file metadata (path, mode, mod_time, is_dir, deleted, blob_id). Each change creates a new version; old versions retained for TTL period to enable point-in-time restore.
- **blobs**: Content-addressable storage with lifecycle flags (local_written, remote_written, local_deleting, etc.) and SHA-256 checksums for deduplication and integrity verification.

### Platform-Specific Direct I/O

- **direct_darwin.go**: Uses F_NOCACHE syscall for direct I/O on macOS
- **direct_linux.go**: Uses O_DIRECT flag with aligned buffers (aligned_linux.go) for direct I/O on Linux

Files use shared locks (LOCK_SH) during read/write operations; blob deletion requires exclusive lock (LOCK_EX|LOCK_NB).

### Background Operations

`runLoop()` manages periodic tasks with jitter:
- Garbage collection: marks orphaned blobs for deletion, compacts old file versions past TTL
- Local blob deletion: removes blob files marked for deletion (with exclusive lock check)
- Integrity checks: SHA-256 verification of local blobs against stored checksums
- Remote sync: upload/download/delete blobs via rclone batch operations

### Test Helpers

Tests use `initTestState(t)` (in file_test.go) which creates an in-memory SQLite database and temp blob directory.
