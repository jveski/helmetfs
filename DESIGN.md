# helmetfs Design Document

This document contains all information needed to reimplement helmetfs in a
different language. It describes the precise semantics of every component.

## Overview

helmetfs is a FUSE (Filesystem in Userspace) filesystem that provides:

1. **Passthrough** -- all filesystem operations are forwarded to a local
   "backing" directory.
2. **Asynchronous replication** -- file changes are queued and copied to a
   "replica" directory by background worker threads.
3. **Self-healing** -- a nightly scrub detects bitrot via BLAKE3 checksums
   and restores corrupted files from the replica.

## Terminology

| Term | Meaning |
|---|---|
| **Backing directory** | The local directory that stores the real files. All FUSE operations read/write here. |
| **Mountpoint** | Where the FUSE filesystem is mounted. Applications interact with this path. |
| **Replica directory** | A separate directory (potentially remote/NFS-mounted) that receives copies of files. Files live under `<replica>/files/<rel_path>`. |
| **Relative path** (`rel_path`) | A file's path relative to the backing directory root, with no leading `/`. For example, `subdir/file.txt`. |
| **Sum file** (`.sum` sidecar) | A file at `<backing>/<rel_path>.sum` containing a 64-character lowercase hex BLAKE3 digest followed by a newline. |
| **Replication log** | A persistent append-only log at `<backing>/.helmetfs/repl.log` recording pending replication operations. |

## Directory Layout

### Backing Directory

```
<backing>/
  .helmetfs/
    repl.log              # Persistent replication queue (WAL)
    scrub.timestamp        # Unix timestamp of last completed scrub
  <user files>
  <user files>.sum         # BLAKE3 checksum sidecars (hidden from FUSE)
```

### Replica Directory

```
<replica>/
  files/
    <mirrored user files>
    <mirrored user files>.sum   # Copies of the checksum sidecars
```

All replicated files live under `<replica>/files/`. The directory structure
under `files/` mirrors the backing directory (minus `.helmetfs/` and `.sum`
files that have no corresponding data file).

## CLI Interface

### Commands

```
helmetfs mount <source-dir> <mountpoint> --replica <path> [options]
helmetfs unmount <mountpoint>
```

### Mount Options

| Flag | Type | Default | Description |
|---|---|---|---|
| `--replica <path>` | string | (required) | Path to the replica directory |
| `--replication-workers <n>` | u32 | 4 | Number of background replication worker threads |
| `--scrub-time HH:MM` | string | `01:00` | Daily scrub time in 24-hour local time |
| `--no-remote-mkdir` | bool | false | Skip creating/removing subdirectories on the replica. Useful when the replica is an object store that doesn't need explicit directory management. |

### Unmount

On Linux: runs `fusermount3 -u <mountpoint>`.
On macOS: runs `umount <mountpoint>`.

### Startup Sequence

1. Parse CLI arguments.
2. Resolve all three paths (source, mountpoint, replica) to absolute paths
   via `realpath`.
3. Initialize `FsState` (creates `.helmetfs/` directory if missing, loads
   the replication log from disk).
4. Start `repl_workers` replication worker threads.
5. Start one scrub thread.
6. Create the FUSE instance (`fuse_new` with FUSE API version 35).
7. Install signal handlers for SIGTERM and SIGINT that set the shutdown flag
   and call `fuse_exit`.
8. Mount via `fuse_mount`.
9. Enter the FUSE multi-threaded loop (`fuse_loop_mt` with
   `max_idle_threads=10`, `clone_fd=0`).
10. On loop exit: unmount, destroy FUSE, deinit state, exit.

### Shutdown Sequence (FUSE `destroy` callback)

The `destroy` callback ignores its `userdata` parameter.

1. Call `flushDirtyFiles()` -- checksum and enqueue all files that are still
   marked dirty. Note: this uses the non-forced variant, so files with open
   write refs will NOT be checksummed during shutdown.
2. Call `stopWorkers()` -- set shutdown flag, broadcast on the replication log
   condition variable, join all replication threads and the scrub thread.

## Global State (`FsState`)

A single global `FsState` pointer is accessible to all FUSE callbacks and
background threads. Fields:

| Field | Type | Description |
|---|---|---|
| `allocator` | Allocator | Memory allocator |
| `backing_dir` | string | Absolute path to backing directory |
| `replica_dir` | string | Absolute path to replica directory |
| `scrub_hour` | u8 | Hour component of daily scrub time (0-23) |
| `scrub_minute` | u8 | Minute component (0-59) |
| `repl_workers` | u32 | Number of replication worker threads |
| `no_remote_mkdir` | bool | Skip remote directory management |
| `path_state` | PathStateMap | Per-file dirty/write-ref tracking |
| `repl_log` | ReplLog | Persistent replication queue |
| `shutdown` | atomic bool | Shutdown signal |
| `scrub_thread` | Thread handle | The scrub thread |
| `repl_threads` | []Thread | The replication worker threads |

### Initialization

- Create `<backing>/.helmetfs/` if it doesn't exist.
- Initialize the ReplLog (loads pending entries from disk).
- Initialize PathStateMap as empty.
- Set `shutdown` to false.

## PathStateMap (Dirty/Write-Ref Tracking)

A thread-safe map from relative path to `PathInfo`:

```
PathInfo {
    dirty_gen: u64     # Incremented each time the file is modified
    clean_gen: u64     # Set to dirty_gen when a checksum is successfully computed
    write_refcount: u32 # Number of currently open write file descriptors
}
```

### Concurrency

All operations acquire a **read-write lock** (RwLock):
- **Exclusive (write) lock**: `setDirty`, `incWriteRef`, `decWriteRef`,
  `clearDirty`, `clearDirtyIfGen`, `remove`
- **Shared (read) lock**: `isDirty`, `hasWriteRef`, `getDirtyGen`

### Operations

| Method | Semantics |
|---|---|
| `setDirty(path)` | Get-or-create entry; increment `dirty_gen` by 1 |
| `isDirty(path)` | Return `dirty_gen > clean_gen`. Return false if path not in map. |
| `clearDirty(path)` | Set `clean_gen = dirty_gen` |
| `clearDirtyIfGen(path, gen)` | Set `clean_gen = gen` only if `dirty_gen == gen` (CAS-style to avoid masking concurrent writes) |
| `getDirtyGen(path)` | Return current `dirty_gen`, or 0 if not in map |
| `incWriteRef(path)` | Get-or-create entry; increment `write_refcount` by 1 |
| `decWriteRef(path)` | If entry exists and `write_refcount > 0`, decrement by 1 |
| `hasWriteRef(path)` | Return `write_refcount > 0`. Return false if not in map. |
| `remove(path)` | Remove entry entirely from map, freeing the key. |

On OOM, `setDirty` and `incWriteRef` silently fail (the get-or-create
returns null).

## Replication Log (`ReplLog`)

### Entry Structure

```
ReplEntry {
    id: u64             # Monotonically increasing unique ID
    op: enum { put, delete }
    path: string        # Relative path
    completed: bool     # Has been successfully replicated
    in_flight: bool     # Currently being processed by a worker
}
```

### Concurrency

All methods acquire a **mutex** (not an RwLock). A **condition variable** is
used to wake blocked consumer threads when new entries are enqueued.

### Disk Format

The replication log is stored at `<backing>/.helmetfs/repl.log` as a
newline-delimited text file:

```
put rel/path/to/file.txt
delete another/file.txt
```

Each line is: `<op> <space> <rel_path> <newline>`, where `<op>` is the literal
string `put` or `delete`.

### Operations

#### `init(allocator, backing_dir, shutdown_flag)`

1. Set `last_truncate_time` to `now()`.
2. Call `loadFromDisk()`:
   - Open `<backing>/.helmetfs/repl.log`. If not found, return (no entries).
   - Read entire contents (up to 16 MB).
   - Split by `\n`. For each non-empty line, call `parseLine()`.
3. `parseLine(line)`:
   - Split on first space. Left part is the op string, right part is the
     relative path.
   - If op is `"put"` -> `.put`; `"delete"` -> `.delete`; anything else ->
     skip the line (invalid lines are silently ignored).
   - Assign `id = next_id++`, store entry.

#### `enqueue(op, rel_path)`

1. Lock mutex.
2. Duplicate the path string (caller retains ownership of the original).
3. Assign `id = next_id++`.
4. Append entry to in-memory list.
5. Append to disk: open `repl.log` in append mode (create if missing), seek
   to end, write formatted line, `fsync` the file.
6. Signal condition variable (wake one waiting consumer).
7. Unlock mutex.

#### `dequeueNext() -> ?{id, op, path}`

1. Lock mutex.
2. Loop while `!shutdown`:
   - Scan entries from front to back.
   - Skip entries that are `completed` or `in_flight`.
   - **Put coalescing**: if the entry is a `put`, scan all later entries. If
     any later non-completed entry is also a `put` for the same path, mark
     the current entry as `completed` (increment `completed_count`), and
     `continue` scanning.
   - **Delete entries are NOT coalesced** -- every delete is processed
     individually in order.
   - If the entry survives coalescing, mark it `in_flight = true`, unlock
     mutex, return `{id, op, path}`.
   - If no eligible entry found, wait on condition variable.
3. If shutdown, return null.

**Put coalescing semantics**: when multiple puts for the same path are
queued, only the *last* one is actually executed. Earlier puts are skipped
because by the time a worker gets to them, a newer put for the same file
exists. This is correct because `put` always copies the current state of the
file.

#### `markCompleted(id)`

1. Lock mutex.
2. Find entry by ID. Set `completed = true`, `in_flight = false`. Increment
   `completed_count`.
3. Call `maybeTruncate()`.
4. Unlock mutex.

#### `markCompletedByPath(rel_path)`

1. Lock mutex.
2. For every non-completed entry matching the path, set `completed = true`,
   `in_flight = false`, increment `completed_count`.
3. Unlock mutex.

Note: this does NOT call `maybeTruncate()`.

#### `pendingCountLocked() -> u64`

Must be called with the mutex already held. Counts and returns the number of
non-completed entries. Used by tests.

#### `deinitEntries()`

Frees all entry path strings and deinitializes the entry list. Called during
shutdown cleanup. Does NOT acquire the mutex (caller is responsible for
ensuring no concurrent access).

#### `hasPendingPut(rel_path) -> bool`

1. Lock mutex.
2. Return true if any non-completed entry has `op == put` and matching path.
3. Unlock mutex.

#### `maybeTruncate()`

Called with mutex held. Returns immediately if there are no entries or
`completed_count == 0`.

Otherwise, conditions to truncate:

- `completed_count * 2 > total_entries`, OR
- `now() - last_truncate_time >= 60` seconds

If triggered:

1. Build a new list containing only non-completed entries. If OOM occurs
   during this step, abandon the truncation and return with the log intact.
2. Free path strings of all completed entries.
3. Replace the entry list with the new list.
4. Reset `completed_count = 0`, update `last_truncate_time`.
5. Call `rewriteLogAtomic()`.

#### `rewriteLogAtomic()`

1. Write all remaining entries to `<backing>/.helmetfs/repl.log.tmp`.
2. `fsync` the temp file.
3. Rename `repl.log.tmp` -> `repl.log` (atomic on POSIX).
4. `fsync` the parent directory.

## FUSE Operations

All FUSE callbacks access global state via the global `g_state` pointer.
FUSE paths arrive as C strings starting with `/`; the leading `/` is stripped
to produce a relative path (`rel_path`).

### File Handle Encoding

File descriptors are stored in FUSE's 64-bit `fh` field. **Bit 63** is used
as a write flag:

```
fh = fd | (1 << 63)   if opened for writing
fh = fd               if opened for reading only
```

This avoids relying on `fi.flags` in `release()`, which is not guaranteed to
reflect the original open flags.

A file is considered "opened for writing" if:
- The access mode bits (`flags & 0o3`) equal 1 (O_WRONLY) or 2 (O_RDWR), OR
- The O_TRUNC flag is set.

**macOS note**: The macFUSE `fuse_file_info` struct has bitfield members that
are opaque to Zig's C import. The implementation defines an ABI-compatible
extern struct `FuseFileInfo` with explicit fields: `flags: i32`,
`bitfields: u32`, `padding2: u32`, `padding3: u32`, `fh: u64`,
`lock_owner: u64`, `poll_events: u32`, `backing_id: i32`,
`compat_flags: u64`, `reserved: [2]u64`. A `castFi` helper reinterpret-casts
the FUSE-provided `fuse_file_info*` to this layout. If `fi` is null or the
cast fails, operations that require it return `EBADF`.

### Hidden Path Logic (`isHiddenPath`)

A relative path is hidden from the FUSE mount if:

1. It starts with `.helmetfs` (the metadata directory), OR
2. It ends with `.sum` AND a corresponding data file (the path without the
   `.sum` suffix) **exists** in the backing directory. Existence is checked
   via `fstatat` with `AT_SYMLINK_NOFOLLOW`.

Important: a `.sum` file is only hidden if the corresponding data file
exists. A standalone file named `foo.sum` where no `foo` file exists is
**visible**.

If the path join allocation fails during the `.sum` check, the path is
treated as **not hidden** (visible).

### `getattr(path)`

1. If `rel_path` is non-empty and hidden -> return `ENOENT`.
2. Construct backing path. If `rel_path` is empty (root), the backing path
   is just the backing directory itself.
3. `fstatat(AT_FDCWD, backing_path, AT_SYMLINK_NOFOLLOW)`.
4. Return the stat result.

Uses `AT_SYMLINK_NOFOLLOW` so symlinks report their own metadata.

### `readdir(path)`

1. Open the backing directory.
2. Always emit `.` and `..`.
3. Iterate directory entries. For each entry:
   - Compute its relative path (join parent rel_path with entry name).
   - If hidden (`isHiddenPath`), skip.
   - Otherwise emit the entry name.
4. Directory iteration errors and allocation failures are silently ignored
   (the entry is skipped).

### `open(path)`

1. If hidden -> `ENOENT`.
2. Open the backing file with the provided flags.
3. If `fi` is null -> close the fd, return `EBADF`.
4. Determine if opened for writing (check access mode bits and O_TRUNC).
5. Encode fd + write bit into `fh`.
6. If writing: `incWriteRef(rel_path)`.
7. If O_TRUNC is set: `setDirty(rel_path)`.

### `create(path, mode)`

1. If hidden -> `ENOENT`.
2. Open/create the backing file with provided flags and mode.
3. If `fi` is null -> close the fd, return `EBADF`.
4. Encode fd with write bit set (always considered a write open).
5. `incWriteRef(rel_path)`.

### `read(path, buf, size, offset)`

1. Decode fd from `fh`. If `fi` is null -> `EBADF`.
2. `pread(fd, buf, size, offset)`. On any error -> `EIO`.
3. Return bytes read.

### `write(path, data, size, offset)`

1. Decode fd from `fh`. If `fi` is null -> `EBADF`.
2. `pwrite(fd, data, size, offset)`. On any error -> `EIO`.
3. If `rel_path` is non-empty: `setDirty(rel_path)`.
4. Return bytes written.

### `fsync(path, datasync)`

1. Decode fd from `fh`.
2. If `datasync != 0`: call `fdatasync(fd)`. Else: call `fsync(fd)`.
   Errors from `fdatasync`/`fsync` are **silently ignored**.
3. If `rel_path` is non-empty and the file is dirty: call
   `checksumAndEnqueueForced()` (the "forced" variant skips the write-ref
   check because the file is still open but data is synced). If the checksum
   fails, return `EIO`.

### `release(path)` (close)

1. Decode fd and write flag from `fh`.
2. If opened for writing: `decWriteRef(rel_path)` **before** checksumming.
3. Close the fd.
4. If `rel_path` is non-empty and dirty: call `checksumAndEnqueue()` (the
   non-forced variant, which skips files that still have open writers).
   Errors are **logged but not returned** -- `release` always returns 0.

**Critical ordering**: the write ref is decremented *before* the checksum
attempt. This ensures that if this was the last writer, the checksum will
proceed.

### `unlink(path)`

1. If hidden -> `ENOENT`.
2. Delete the backing file.
3. Delete `<backing_path>.sum` (ignore errors if not found).
4. If `rel_path` is non-empty: enqueue a `delete` operation to the
   replication log.
5. `remove(rel_path)` from PathStateMap.

### `rename(from, to, flags)`

On Linux, supports `RENAME_NOREPLACE` flag:
- If `RENAME_NOREPLACE` is set and the destination exists -> `EEXIST`.
- `RENAME_EXCHANGE` -> `EOPNOTSUPP`.
- Any other flags -> `EINVAL`.

On non-Linux, any non-zero flags -> `EOPNOTSUPP`.

1. If source or destination is hidden -> `ENOENT`.
2. Rename the backing file.
3. Rename `<from>.sum` to `<to>.sum` (ignore errors if `.sum` doesn't exist).
4. If both `rel_from` and `rel_to` are non-empty: enqueue `delete` for the
   old path, `put` for the new path.
5. `remove(old_rel_path)` from PathStateMap.

### `mkdir(path, mode)`

1. If hidden -> `ENOENT`.
2. Create the directory in the backing directory.
3. `fchmodat` to apply the requested mode.
4. Unless `no_remote_mkdir` is set and `rel_path` is non-empty:
   `ensureParentDir` on the replica path, then create the corresponding
   directory in `<replica>/files/<rel_path>`. Silently ignore
   `PathAlreadyExists`.

### `rmdir(path)`

Note: `rmdir` does NOT check `isHiddenPath` (unlike `mkdir`).

1. Delete the directory from the backing directory.
2. Unless `no_remote_mkdir` is set and `rel_path` is non-empty: delete the
   corresponding directory from the replica. Silently ignore errors.

### `symlink(target, linkpath)`

1. Create the symlink in the backing directory via `symlinkat`.
2. If `rel_path` is non-empty: enqueue a `put` for the link path.

Note: symlinks do NOT get `.sum` files. They are replicated by reading the
link target and recreating the symlink in the replica.

### `readlink(path)`

1. If hidden -> `ENOENT`.
2. `readlinkat` on the backing path.
3. Null-terminate the result buffer.

### `chmod(path, mode)`

1. `fchmodat` on the backing path. On any error -> `EIO` (does not go
   through the standard error mapping).
2. If `rel_path` is non-empty: enqueue a `put` to replicate the permission
   change.

### `chown(path, uid, gid)`

1. `lchown` on the backing path (uses `lchown`, not `chown`, so symlinks
   themselves can have ownership changed without following). On error,
   returns the raw errno directly (manual errno extraction).
2. If `rel_path` is non-empty: enqueue a `put`.

### `truncate(path, size)`

1. If `fi` is provided (file is open): `ftruncate(fd, size)`. On error ->
   `EIO`.
2. Otherwise: open the file in read-write mode, `ftruncate`, close. Open
   failure -> `ENOENT`; truncate failure -> `EIO`.
3. If `rel_path` is non-empty: `setDirty(rel_path)`, then call
   `checksumAndEnqueue()`. This is called regardless of whether `fi` was
   provided. When the file is open (`fi` path), the `checksumAndEnqueue`
   call will typically be skipped because the file still has a write ref
   (from the original `open`/`create`). For the path-based case (no `fi`),
   the checksum runs immediately because there is no subsequent `release()`.

### `utimens(path, times)`

1. If `tv` (the times pointer) is null: return 0 immediately (no-op).
2. Cast `tv` to an array of 2 timespecs and call `utimensat(AT_FDCWD,
   backing_path, &ts, 0)` using the raw syscall (not a Zig wrapper).
3. Error handling is platform-specific:
   - On **macOS**: extract errno via `_errno().*` and return its negation
     (same pattern as `chown`/`access`).
   - On **Linux**: the return value from the raw syscall is cast to a signed
     int; if negative, return it directly (it is already `-errno`).
4. If `rel_path` is non-empty: enqueue a `put` to replicate the timestamp
   change. (Note: the enqueue is inside the non-null `tv` check, so it only
   happens when times are actually set.)

### `statfs(path)`

Note: `statfs` does NOT check `isHiddenPath`.

1. If `stbuf` is null: return 0 immediately.
2. Call `statvfs(backing_path, stbuf)` using the raw C function. On error ->
   `EIO`.

### `access(path, mask)`

1. If hidden -> `ENOENT`.
2. Call the raw C `access(backing_path, mask)` function (not a Zig wrapper).
   On error, extract errno via `_errno().*` and return its negation (manual
   errno extraction, same pattern as `chown`).

### `init(conn_info, config)`

Returns null (no-op).

### `destroy(userdata)`

1. `flushDirtyFiles()` -- iterates PathStateMap, collects all paths where
   `dirty_gen > clean_gen`, checksums and enqueues each one.
2. `stopWorkers()` -- sets shutdown, broadcasts condition variable, joins all
   threads.

### Unsupported Operations

The following FUSE operations are **not implemented** (their function
pointers are null/zero):
- `link` (hardlinks) -- attempts to hardlink will return `ENOSYS` from FUSE,
  which applications see as an error.
- `mknod`
- `setxattr`, `getxattr`, `listxattr`, `removexattr`
- `opendir`, `releasedir`, `fsyncdir`
- `lock`, `flock`
- `bmap`
- `ioctl`
- `poll`
- `write_buf`, `read_buf`
- `fallocate`
- `copy_file_range`
- `lseek`

## Checksum and Enqueue

### `checksumAndEnqueue(state, rel_path)`

1. If `hasWriteRef(rel_path)` -> return immediately (don't checksum a
   partially-written file).
2. Call `checksumAndEnqueueForced(state, rel_path)`.

### `checksumAndEnqueueForced(state, rel_path)`

1. Snapshot `gen = getDirtyGen(rel_path)` before hashing.
2. Compute BLAKE3 hash of `<backing>/<rel_path>`.
3. Write the hex digest to `<backing>/<rel_path>.sum`.
4. Enqueue a `put` to the replication log.
5. `clearDirtyIfGen(rel_path, gen)` -- only clears dirty if no concurrent
   writes happened during hashing (the generation matches).

The "forced" variant is used by `fsync`, where the file is still open (has a
write ref) but data has been synced to disk.

### `flushDirtyFiles()`

1. Acquire shared lock on PathStateMap.
2. Collect all paths where `dirty_gen > clean_gen`.
3. Release shared lock.
4. For each collected path, call `checksumAndEnqueue()`.

This is called during shutdown (`destroy` callback) to ensure no dirty files
are left unchecksummed.

## BLAKE3 Checksum Computation

### `computeBlake3(backing_path) -> [64]u8`

1. Open the file.
2. Acquire a **shared file lock** (`flock(fd, LOCK_SH)`) to prevent
   concurrent writers from modifying the file during hashing.
3. Read the file in 64 KB chunks using `readAll` (which retries on short
   reads, unlike `read`), feeding each chunk to a BLAKE3 hasher.
4. Finalize the hash to produce a 32-byte digest.
5. Convert to a 64-character lowercase hex string.
6. Release the file lock.

### Sum File Format

The `.sum` file contains exactly: `<64 lowercase hex chars>\n`

`writeSumFile` creates the file, writes the hex digest followed by `\n`,
then calls `fsync` on the file to ensure durability before returning.

When reading (`readSumFile`): reads into a 128-byte stack buffer (using
`readAll` which retries short reads), then trims trailing `\n`, `\r`, and
spaces. The 128-byte limit means `.sum` files larger than this will be
silently truncated.

## Replication Workers

### `replWorkerLoop(state)`

```
while not shutdown:
    work = repl_log.dequeueNext()    # blocks until work available
    if work is null: break           # shutdown

    backoff = 1 second
    max_backoff = 300 seconds (5 minutes)

    while not shutdown:
        result = execute(work.op, work.path)
        if success:
            repl_log.markCompleted(work.id)
            break
        else:
            log error
            sleep(backoff)
            backoff = min(backoff * 2, max_backoff)
```

Exponential backoff starts at 1 second, doubles on each failure, caps at 300
seconds.

### `replicatePut(state, rel_path)`

1. `fstatat(backing_path, AT_SYMLINK_NOFOLLOW)`. If file not found, return
   success (file was deleted between enqueue and replication -- this is fine).
2. **If the file is a symlink**:
   a. Ensure parent directory exists in replica.
   b. Delete any existing file at replica path (ignore not found).
   c. `readlinkat` to get the symlink target.
   d. `symlinkat` to create the symlink in the replica.
   e. Return (no `.sum` handling for symlinks).
3. **Integrity check before replicating**:
   a. Read the stored `.sum` from backing.
   b. Compute BLAKE3 of the backing file.
   c. If they don't match, log a warning and **skip** replication (the
      backing file may be corrupt; don't propagate corruption to the replica).
   d. If either read fails, proceed with replication anyway (best effort).
4. Ensure parent directory exists in replica.
5. `copyFileWithSync(backing_path, replica_path)`.
6. `copyFileWithSync(sum_backing, sum_replica)` -- if `.sum` not found,
   ignore the error.
7. Read file mode and ownership from backing via a **second** `fstatat`
   **without** `AT_SYMLINK_NOFOLLOW` (follows symlinks; gets the actual file
   metadata). If this second `fstatat` fails, **silently return** (skip
   mode/ownership replication).
8. Apply mode via `fchmodat` on replica, using a mask of `0o7777` (preserves
   setuid, setgid, sticky bits in addition to rwx bits). Errors are
   **silently ignored**.
9. Apply ownership via raw C `chown` (NOT `lchown`) on replica. Errors are
   **silently ignored** (the return value of `chown` is discarded).

### `replicateDelete(state, rel_path)`

1. Delete `<replica>/files/<rel_path>` (ignore not found).
2. Delete `<replica>/files/<rel_path>.sum` (ignore not found).
3. Unless `no_remote_mkdir`: call `removeEmptyParentDirs` to clean up empty
   directories up to `<replica>/files/`.

### `copyFileWithSync(src, dst)`

Atomic file copy:

1. Open source for reading.
2. Construct `<dst>.tmp` path in a stack buffer of `max_path_bytes` size
   using `bufPrint`. If the path exceeds this limit, return `NameTooLong`.
3. Create `<dst>.tmp`.
4. Copy data in 64 KB chunks (using `read`, not `readAll` -- short reads are
   not retried, unlike `computeBlake3`).
5. `fsync` the temp file.
6. Close the temp file.
7. Rename `<dst>.tmp` -> `<dst>` (atomic).
8. `fsync` the parent directory.

If any step fails after creating the temp file, delete it before returning
the error. Also, if the rename fails, the temp file is cleaned up.

### `ensureParentDir(path)`

Recursively create parent directories. If `makeDirAbsolute` returns
`FileNotFound`, recursively call `ensureParentDir` on the parent, then retry.
`PathAlreadyExists` is silently ignored.

### `removeEmptyParentDirs(path, stop_at)`

Walk up from `dirname(path)`, deleting each empty directory. Stop when:
- A directory deletion fails (directory not empty or other error), OR
- The current directory path length is <= `stop_at` path length (don't
  delete the `<replica>/files/` root itself).

## Scrub (Integrity Checking and Self-Healing)

### `scrubLoop(state)`

1. On startup: if `shouldScrubImmediately()` -> run scrub now.
2. Loop while not shutdown:
   a. Compute `nsUntilNextScrub(scrub_hour, scrub_minute)`.
   b. Sleep in 1-second chunks (checking shutdown between each).
   c. If not shutdown: run scrub.

### `shouldScrubImmediately(state) -> bool`

1. Read `<backing>/.helmetfs/scrub.timestamp`.
2. Parse as integer (Unix timestamp).
3. If the file doesn't exist, can't be read, or can't be parsed -> return
   true (scrub immediately).
4. If `now - last_scrub > 86400` (24 hours) -> return true.
5. Otherwise -> return false.

### `nsUntilNextScrub(target_hour, target_minute) -> u64`

1. Get current local time (using `localtime_r`).
2. Compute seconds since midnight: `hour*3600 + min*60 + sec`.
3. Compute target seconds: `target_hour*3600 + target_minute*60`.
4. If target is later today: `delta = target - now`.
5. If target has passed today: `delta = 86400 - now + target`.
6. Return `delta * 1_000_000_000` (nanoseconds).

The result is always in the range `(0, 86400 * 10^9]`.

### `runScrub(state)`

1. Log start.
2. Open the backing directory and walk it recursively.
3. For each entry:
   - Skip directories.
   - Skip symlinks.
   - Skip paths starting with `.helmetfs`.
   - Skip paths ending with `.sum`.
   - Skip files with `hasWriteRef` (currently being written).
   - Call `scrubFile()`.
4. Write scrub timestamp.
5. Log completion with stats.

### `scrubFile(state, rel_path, corruptions, repairs)`

1. Compute BLAKE3 of `<backing>/<rel_path>` -> `current_hex`.
2. Read `<backing>/<rel_path>.sum` -> `stored_hex`.
   - If `.sum` not found: **adopt** the file:
     a. Write current hash to `.sum`.
     b. Enqueue `put` for replication.
     c. Return (no corruption).
   - If other error: propagate.
3. Compare `current_hex` to `stored_hex`.
   - If equal: file is clean. Return.
4. **Corruption detected**. Increment `corruptions`.
5. Check if there's a pending `put` for this path in the replication log
   (`hasPendingPut`).
6. Read `<replica>/files/<rel_path>.sum` -> `replica_hex`.
   - If can't read: log error, return (can't repair without replica).
7. Compute BLAKE3 of `<replica>/files/<rel_path>` -> `replica_computed`.
8. Compare `replica_computed` to `replica_hex`.
   - If don't match: **replica is also corrupt**. Log warning, return
     (cannot repair).
9. If `has_pending_put` is true: log warning "replica is stale", **skip
   repair**. (A pending put means the replica might not have the latest
   version; restoring from it could lose data.)
10. Re-check `hasWriteRef(rel_path)` -- a writer may have opened the file
    since the initial check. If true, skip repair.
11. Check `isDirty(rel_path)` -- if dirty, a write completed but hasn't been
    checksummed yet. Skip repair.
12. **Repair**: `copyFileWithSync(replica_path, backing_path)`.
13. Write `replica_computed` to `<backing>/<rel_path>.sum`.
14. Increment `repairs`.

### `writeScrubTimestamp(state, ts)`

Write `<unix_timestamp>\n` to `<backing>/.helmetfs/scrub.timestamp`. Fsync
the file. Errors are silently ignored.

## Error Mapping

FUSE callbacks must return negative errno values on error. The following
Zig/POSIX errors are mapped:

| Error | errno |
|---|---|
| FileNotFound | ENOENT |
| AccessDenied | EACCES |
| NameTooLong | ENAMETOOLONG |
| SymLinkLoop | ELOOP |
| NotDir | ENOTDIR |
| FileTooBig | EFBIG |
| NoSpaceLeft | ENOSPC |
| IsDir | EISDIR |
| ReadOnlyFileSystem | EROFS |
| DiskQuota | EDQUOT |
| FileBusy | EBUSY |
| PathAlreadyExists | EEXIST |
| InvalidArgument | EINVAL |
| NotOpenForWriting | EBADF |
| OperationNotSupported | EOPNOTSUPP |
| BrokenPipe | EPIPE |
| ProcessFdQuotaExceeded | EMFILE |
| SystemFdQuotaExceeded | ENFILE |
| WouldBlock | EAGAIN |
| Unexpected | EIO |
| (all other errors) | EIO |

## fsyncDir Helper

```
fsyncDir(dir_path):
    open directory
    fsync(dir_fd)    # raw syscall; ignores EINVAL (some fs don't support dir fsync)
    close directory
```

Used after atomic renames to ensure directory entries are persisted.

## Signal Handling

SIGTERM and SIGINT handlers:
1. Set `shutdown` flag to true (atomic store with release ordering).
2. Call `fuse_exit()` on the FUSE instance to break the event loop.

## Threading Model

- **FUSE threads**: managed by `fuse_loop_mt`. Multiple threads serve FUSE
  requests concurrently. Max idle threads = 10.
- **Replication worker threads**: `repl_workers` threads (default 4). Each
  runs `replWorkerLoop`. They block on the condition variable when the queue
  is empty.
- **Scrub thread**: 1 thread running `scrubLoop`. Sleeps between scrubs,
  checking shutdown every 1 second.

All shared state access is synchronized via:
- `PathStateMap.rwlock` for per-file tracking.
- `ReplLog.mutex` + `ReplLog.cond` for the replication queue.
- `FsState.shutdown` atomic bool for shutdown signaling.

## Platform-Specific Behavior

### Linux
- FUSE API: links against `libfuse3` (`fuse3/fuse.h`), API version 35.
- Unmount: `fusermount3 -u`.
- Rename: supports `RENAME_NOREPLACE` flag.
- `_GNU_SOURCE` is defined.
- `sys/file.h` is included (for `flock`).

### macOS
- FUSE API: uses macFUSE. `FUSE_DARWIN_ENABLE_EXTENSIONS` is set to 0.
- `fuse_new` is called via `_fuse_new_31` with a version struct
  `{major: 3, minor: 17, hotfix: 0, flags: 0}`.
- Unmount: `umount`.
- Rename: any non-zero flags -> `EOPNOTSUPP`.
- Include paths: `/usr/local/include` and `/usr/local/lib`.

## Build Configuration

- Minimum Zig version: 0.14.0.
- Links: `libc`, `libfuse3` (system library).
- Build modes: Debug (default), ReleaseSafe (for release binaries and fuzz
  targets).
- Fuzz targets use the LLVM backend with sanitizer coverage instrumentation.

## Key Invariants and Race Condition Prevention

1. **Write-ref prevents premature checksumming**: `checksumAndEnqueue` skips
   files with open writers. This ensures the checksum is computed only after
   all data is written.

2. **Dirty generation counter prevents stale checksums**: Before hashing,
   `dirty_gen` is snapshotted. After hashing, `clearDirtyIfGen` only clears
   the dirty flag if no concurrent writes bumped `dirty_gen` during hashing.

3. **Write ref is decremented before checksum in release()**: This ensures
   the last closer's checksum attempt succeeds rather than being skipped.

4. **Scrub checks for pending puts before repair**: If a `put` is pending in
   the replication log, the replica may be stale. Repairing from a stale
   replica would lose data. Scrub skips repair in this case.

5. **Scrub re-checks write ref and dirty state before repair**: Between the
   initial hash computation and the repair decision, a new writer may have
   opened the file or a write may have completed. Scrub checks both
   conditions before overwriting.

6. **Replication verifies checksum before copying**: `replicatePut` computes
   the BLAKE3 hash and compares it to the `.sum` file. If they don't match,
   it skips replication to avoid propagating corruption to the replica.

7. **Put coalescing ensures latest state is replicated**: When multiple puts
   for the same path are queued, only the last one is executed. Since `put`
   copies the current file state, the replica always gets the latest version.

8. **Atomic file copies**: Both replication and repair use write-to-temp +
   rename to ensure the destination file is never in a partially-written
   state.

9. **Log truncation is atomic**: The replication log is rewritten via
   write-to-temp + rename + fsync to prevent data loss on crash.

10. **File lock during hashing**: `computeBlake3` acquires a shared file lock
    (`LOCK_SH`) to prevent concurrent modifications during hashing. Note this
    only works with cooperative locking.
