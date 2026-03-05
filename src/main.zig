// helmetfs - FUSE passthrough filesystem with async replication and self-healing
//
// See DESIGN.md for architecture details.

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;

const c = @cImport({
    @cDefine("FUSE_USE_VERSION", "35");
    if (builtin.os.tag == .macos) {
        @cDefine("FUSE_DARWIN_ENABLE_EXTENSIONS", "0");
    }
    if (builtin.os.tag == .linux) {
        @cDefine("_GNU_SOURCE", "");
    }
    @cInclude("fuse3/fuse.h");
    @cInclude("unistd.h");
    @cInclude("time.h");
    @cInclude("stdlib.h");
    if (builtin.os.tag == .linux) {
        @cInclude("sys/file.h");
        @cInclude("fcntl.h");
    }
});

const log = std.log.scoped(.helmetfs);

// ============================================================================
// FUSE Compatibility Layer
// ============================================================================
//
// macFUSE's fuse_file_info and fuse_conn_info use C bitfields which Zig's
// cImport translates as opaque types. We define ABI-compatible Zig structs
// and @ptrCast when accessing fields from FUSE callbacks.
//

const FuseFileInfo = extern struct {
    flags: i32,
    bitfields: u32 = 0, // writepage:1, direct_io:1, keep_cache:1, etc.
    padding2: u32 = 0,
    padding3: u32 = 0,
    fh: u64 = 0,
    lock_owner: u64 = 0,
    poll_events: u32 = 0,
    backing_id: i32 = 0,
    compat_flags: u64 = 0,
    reserved: [2]u64 = .{ 0, 0 },
};

fn castFi(fi: ?*c.struct_fuse_file_info) ?*FuseFileInfo {
    return @ptrCast(@alignCast(fi));
}

// Encode/decode the "opened for writing" flag in the fh field.
// File descriptors are small non-negative integers; we use bit 63 of the u64
// fh to store whether this descriptor was opened for writing.  This avoids
// relying on fi.flags in release(), which is not guaranteed to reflect the
// original open flags on every FUSE implementation.
const FH_WRITE_BIT: u64 = 1 << 63;

fn encodeFh(fd: posix.fd_t, opened_for_write: bool) u64 {
    const base: u64 = @intCast(fd);
    return if (opened_for_write) base | FH_WRITE_BIT else base;
}

fn decodeFh(fh: u64) struct { fd: posix.fd_t, opened_for_write: bool } {
    return .{
        .fd = @intCast(fh & ~FH_WRITE_BIT),
        .opened_for_write = (fh & FH_WRITE_BIT) != 0,
    };
}

// macFUSE's libfuse_version has bitfields making it opaque to Zig's cImport.
// Layout: { major: u32, minor: u32, hotfix: u32, darwin_ext_and_padding: u32 }
const LibfuseVersion = extern struct {
    major: u32,
    minor: u32,
    hotfix: u32,
    flags: u32, // darwin_extensions_enabled:1 | padding:31
};

// Wrapper for fuse_new that calls _fuse_new_31 directly, bypassing the
// static inline fuse_new_fn that Zig cannot link.
fn fuseNew(args: [*c]c.struct_fuse_args, ops: [*c]const c.struct_fuse_operations, op_size: usize, user_data: ?*anyopaque) ?*c.struct_fuse {
    if (comptime builtin.os.tag == .macos) {
        var version = LibfuseVersion{
            .major = 3,
            .minor = 17,
            .hotfix = 0,
            .flags = 0, // darwin_extensions_enabled = 0
        };
        return c._fuse_new_31(args, ops, op_size, @ptrCast(&version), user_data);
    } else {
        return c.fuse_new(args, ops, op_size, user_data);
    }
}

// ============================================================================
// Global State
// ============================================================================

var g_state: *FsState = undefined;

const FsState = struct {
    allocator: std.mem.Allocator,
    backing_dir: []const u8,
    replica_dir: []const u8,
    verify_reads: bool,
    scrub_hour: u8,
    scrub_minute: u8,
    metrics_addr: ?[]const u8,
    repl_workers: u32,

    // Per-path state (dirty flag + write-descriptor refcount)
    path_state: PathStateMap,

    // Replication log
    repl_log: ReplLog,

    // Metrics
    metrics: Metrics,

    // Shutdown flag
    shutdown: std.atomic.Value(bool),

    // Scrub thread handle
    scrub_thread: ?std.Thread,
    // Replication worker threads
    repl_threads: []std.Thread,
    // Metrics server thread
    metrics_thread: ?std.Thread,

    fn init(
        allocator: std.mem.Allocator,
        backing_dir: []const u8,
        replica_dir: []const u8,
        verify_reads: bool,
        scrub_hour: u8,
        scrub_minute: u8,
        metrics_addr: ?[]const u8,
        repl_workers: u32,
    ) !*FsState {
        const self = try allocator.create(FsState);
        self.* = .{
            .allocator = allocator,
            .backing_dir = backing_dir,
            .replica_dir = replica_dir,
            .verify_reads = verify_reads,
            .scrub_hour = scrub_hour,
            .scrub_minute = scrub_minute,
            .metrics_addr = metrics_addr,
            .repl_workers = repl_workers,
            .path_state = PathStateMap.init(allocator),
            .repl_log = undefined,
            .metrics = Metrics{},
            .shutdown = std.atomic.Value(bool).init(false),
            .scrub_thread = null,
            .repl_threads = &.{},
            .metrics_thread = null,
        };
        // Create .helmetfs directory
        const helmetfs_dir = try std.fs.path.join(allocator, &.{ backing_dir, ".helmetfs" });
        defer allocator.free(helmetfs_dir);
        std.fs.makeDirAbsolute(helmetfs_dir) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };
        self.repl_log = try ReplLog.init(allocator, backing_dir);
        return self;
    }

    /// Free resources owned by FsState.  Workers must already be stopped.
    fn deinit(self: *FsState) void {
        // Free replication log entries
        for (self.repl_log.entries.items) |entry| {
            self.allocator.free(entry.path);
        }
        self.repl_log.entries.deinit(self.allocator);

        // Free path-state map keys
        var it = self.path_state.map.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.path_state.map.deinit();

        // Free thread handle array (only if it was heap-allocated by startWorkers)
        if (self.repl_threads.len > 0) {
            self.allocator.free(self.repl_threads);
        }

        self.allocator.destroy(self);
    }

    fn startWorkers(self: *FsState) !void {
        // Start replication workers
        self.repl_threads = try self.allocator.alloc(std.Thread, self.repl_workers);
        var started: usize = 0;
        errdefer {
            // On failure, signal shutdown and join already-started threads
            self.shutdown.store(true, .release);
            {
                self.repl_log.mutex.lock();
                defer self.repl_log.mutex.unlock();
                self.repl_log.cond.broadcast();
            }
            for (self.repl_threads[0..started]) |t| {
                t.join();
            }
            self.allocator.free(self.repl_threads);
            self.repl_threads = &.{};
        }
        for (self.repl_threads) |*t| {
            t.* = try std.Thread.spawn(.{}, replWorkerLoop, .{self});
            started += 1;
        }
        // Start scrub thread
        self.scrub_thread = try std.Thread.spawn(.{}, scrubLoop, .{self});
        // Start metrics server if configured
        if (self.metrics_addr != null) {
            self.metrics_thread = try std.Thread.spawn(.{}, metricsServerLoop, .{self});
        }
    }

    fn stopWorkers(self: *FsState) void {
        self.shutdown.store(true, .release);
        // Wake waiting workers while holding the mutex to prevent lost wakeups.
        // A worker could be between checking shutdown (false) and calling
        // cond.wait; broadcasting without the mutex would miss that worker.
        {
            self.repl_log.mutex.lock();
            defer self.repl_log.mutex.unlock();
            self.repl_log.cond.broadcast();
        }
        // Join replication workers
        for (self.repl_threads) |t| {
            t.join();
        }
        // Join scrub thread
        if (self.scrub_thread) |t| {
            t.join();
        }
        // Metrics thread will also see shutdown — unblock accept with a self-connect
        if (self.metrics_thread) |t| {
            if (self.metrics_addr) |addr_str| {
                if (parseMetricsAddr(addr_str)) |port| {
                    if (std.net.Address.parseIp4("127.0.0.1", port)) |a| {
                        const stream = std.net.tcpConnectToAddress(a) catch null;
                        if (stream) |s| s.close();
                    } else |_| {}
                } else |_| {}
            }
            t.join();
        }
    }

    /// Flush dirty files to replication log (for shutdown/destroy)
    fn flushDirtyFiles(self: *FsState) void {
        self.path_state.rwlock.lockShared();
        // Collect dirty paths
        var dirty_paths: std.ArrayList([]const u8) = .{};
        defer dirty_paths.deinit(self.allocator);
        var it = self.path_state.map.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.dirty_gen > entry.value_ptr.clean_gen) {
                const path_copy = self.allocator.dupe(u8, entry.key_ptr.*) catch continue;
                dirty_paths.append(self.allocator, path_copy) catch {
                    self.allocator.free(path_copy);
                    continue;
                };
            }
        }
        self.path_state.rwlock.unlockShared();

        for (dirty_paths.items) |rel_path| {
            defer self.allocator.free(rel_path);
            checksumAndEnqueue(self, rel_path) catch |err| {
                log.err("failed to flush dirty file {s}: {}", .{ rel_path, err });
            };
        }
    }
};

// ============================================================================
// Path State Tracking
// ============================================================================

const PathInfo = struct {
    dirty_gen: u64 = 0,
    clean_gen: u64 = 0,
    write_refcount: u32 = 0,
    last_verify_time: i64 = 0,
};

const PathStateMap = struct {
    rwlock: std.Thread.RwLock = .{},
    map: std.StringHashMap(PathInfo),
    allocator: std.mem.Allocator,

    fn init(allocator: std.mem.Allocator) PathStateMap {
        return .{
            .map = std.StringHashMap(PathInfo).init(allocator),
            .allocator = allocator,
        };
    }

    fn setDirty(self: *PathStateMap, rel_path: []const u8) void {
        self.rwlock.lock();
        defer self.rwlock.unlock();
        const key_copy = self.allocator.dupe(u8, rel_path) catch {
            log.err("OOM in setDirty for path: {s}", .{rel_path});
            return;
        };
        const gop = self.map.getOrPut(key_copy) catch {
            self.allocator.free(key_copy);
            log.err("OOM in setDirty map insert for path: {s}", .{rel_path});
            return;
        };
        if (gop.found_existing) {
            self.allocator.free(key_copy);
            gop.value_ptr.dirty_gen += 1;
        } else {
            gop.value_ptr.* = .{ .dirty_gen = 1 };
        }
    }

    fn incWriteRef(self: *PathStateMap, rel_path: []const u8) void {
        self.rwlock.lock();
        defer self.rwlock.unlock();
        const key_copy = self.allocator.dupe(u8, rel_path) catch {
            log.err("OOM in incWriteRef for path: {s}", .{rel_path});
            return;
        };
        const gop = self.map.getOrPut(key_copy) catch {
            self.allocator.free(key_copy);
            log.err("OOM in incWriteRef map insert for path: {s}", .{rel_path});
            return;
        };
        if (!gop.found_existing) {
            gop.value_ptr.* = .{ .write_refcount = 1 };
        } else {
            self.allocator.free(key_copy);
            gop.value_ptr.write_refcount += 1;
        }
    }

    fn decWriteRef(self: *PathStateMap, rel_path: []const u8) void {
        self.rwlock.lock();
        defer self.rwlock.unlock();
        if (self.map.getPtr(rel_path)) |info| {
            if (info.write_refcount > 0) {
                info.write_refcount -= 1;
            }
        }
    }

    fn isDirty(self: *PathStateMap, rel_path: []const u8) bool {
        self.rwlock.lockShared();
        defer self.rwlock.unlockShared();
        if (self.map.get(rel_path)) |info| return info.dirty_gen > info.clean_gen;
        return false;
    }

    fn hasWriteRef(self: *PathStateMap, rel_path: []const u8) bool {
        self.rwlock.lockShared();
        defer self.rwlock.unlockShared();
        if (self.map.get(rel_path)) |info| return info.write_refcount > 0;
        return false;
    }

    fn shouldVerify(self: *PathStateMap, rel_path: []const u8) bool {
        const now = std.time.timestamp();
        self.rwlock.lock();
        defer self.rwlock.unlock();
        if (self.map.getPtr(rel_path)) |info| {
            if (now - info.last_verify_time < 60) return false;
            info.last_verify_time = now;
            return true;
        }
        // Path not in map — no state, allow verification
        // (but we need to create an entry to track it)
        const key_copy = self.allocator.dupe(u8, rel_path) catch return true;
        const gop = self.map.getOrPut(key_copy) catch {
            self.allocator.free(key_copy);
            return true;
        };
        if (gop.found_existing) {
            self.allocator.free(key_copy);
            if (now - gop.value_ptr.last_verify_time < 60) return false;
            gop.value_ptr.last_verify_time = now;
        } else {
            gop.value_ptr.* = .{ .last_verify_time = now };
        }
        return true;
    }

    fn clearDirty(self: *PathStateMap, rel_path: []const u8) void {
        self.rwlock.lock();
        defer self.rwlock.unlock();
        if (self.map.getPtr(rel_path)) |info| {
            info.clean_gen = info.dirty_gen;
        }
    }

    /// Snapshot the current dirty generation for a path.  The caller
    /// computes the checksum and then calls clearDirtyIfGen to
    /// conditionally clear only if no new writes arrived in between.
    fn getDirtyGen(self: *PathStateMap, rel_path: []const u8) u64 {
        self.rwlock.lockShared();
        defer self.rwlock.unlockShared();
        if (self.map.get(rel_path)) |info| return info.dirty_gen;
        return 0;
    }

    /// Clear the dirty flag only if the dirty generation has not advanced
    /// past `gen` since we snapshotted it.  This prevents a concurrent
    /// setDirty from being silently discarded.
    fn clearDirtyIfGen(self: *PathStateMap, rel_path: []const u8, gen: u64) void {
        self.rwlock.lock();
        defer self.rwlock.unlock();
        if (self.map.getPtr(rel_path)) |info| {
            if (info.dirty_gen == gen) {
                info.clean_gen = gen;
            }
        }
    }

    /// Remove a path's state entirely.  Used when a file is deleted
    /// to prevent unbounded growth of the map.
    fn remove(self: *PathStateMap, rel_path: []const u8) void {
        self.rwlock.lock();
        defer self.rwlock.unlock();
        if (self.map.fetchRemove(rel_path)) |kv| {
            self.allocator.free(kv.key);
        }
    }
};

// ============================================================================
// Replication Log
// ============================================================================

const ReplOp = enum { put, delete };

const ReplEntry = struct {
    id: u64 = 0,
    op: ReplOp,
    path: []const u8,
    completed: bool = false,
    in_flight: bool = false,
    /// If set, this entry must not be dequeued until the entry with
    /// this ID has been completed.  Used by enqueuePair to enforce
    /// ordering (e.g. delete-before-put for renames).
    depends_on: ?u64 = null,
};

const ReplLog = struct {
    allocator: std.mem.Allocator,
    backing_dir: []const u8,
    mutex: std.Thread.Mutex = .{},
    cond: std.Thread.Condition = .{},
    entries: std.ArrayList(ReplEntry),
    completed_count: usize = 0,
    last_truncate_time: i64 = 0,
    next_id: u64 = 0,

    fn init(allocator: std.mem.Allocator, backing_dir: []const u8) !ReplLog {
        var self = ReplLog{
            .allocator = allocator,
            .backing_dir = backing_dir,
            .entries = .empty,
        };
        self.last_truncate_time = std.time.timestamp();
        // Load existing log entries from disk
        self.loadFromDisk() catch |err| {
            log.warn("failed to load replication log: {}", .{err});
        };
        return self;
    }

    fn logPath(self: *ReplLog) ![]const u8 {
        return try std.fs.path.join(self.allocator, &.{ self.backing_dir, ".helmetfs", "repl.log" });
    }

    fn loadFromDisk(self: *ReplLog) !void {
        const path = try self.logPath();
        defer self.allocator.free(path);

        const file = std.fs.openFileAbsolute(path, .{}) catch |err| switch (err) {
            error.FileNotFound => return,
            else => return err,
        };
        defer file.close();

        const contents = file.readToEndAlloc(self.allocator, 16 * 1024 * 1024) catch return error.OutOfMemory;
        defer self.allocator.free(contents);

        var iter = std.mem.splitScalar(u8, contents, '\n');
        while (iter.next()) |line| {
            if (line.len == 0) continue;
            self.parseLine(line) catch continue;
        }

        const pending = self.entries.items.len;
        if (pending > 0) {
            log.info("loaded {} pending replication entries from log", .{pending});
        }
    }

    fn parseLine(self: *ReplLog, line: []const u8) !void {
        // Format: <crc32-hex> <operation> <relative-path>
        // Or:     <crc32-hex> <operation> dep:<id> <relative-path>
        const first_space = std.mem.indexOfScalar(u8, line, ' ') orelse return error.InvalidFormat;
        const crc_hex = line[0..first_space];
        const remainder = line[first_space..]; // includes leading space

        // Verify CRC32
        const expected_crc = std.fmt.parseUnsigned(u32, crc_hex, 16) catch return error.InvalidCrc;
        const computed_crc = std.hash.crc.Crc32IsoHdlc.hash(remainder);
        if (expected_crc != computed_crc) {
            log.warn("discarding log entry with CRC mismatch: {s}", .{line});
            return error.CrcMismatch;
        }

        // Parse operation and path (with optional dep:<id>)
        const after_crc = line[first_space + 1 ..];
        const second_space = std.mem.indexOfScalar(u8, after_crc, ' ') orelse return error.InvalidFormat;
        const op_str = after_crc[0..second_space];
        const after_op = after_crc[second_space + 1 ..];

        const op: ReplOp = if (std.mem.eql(u8, op_str, "put"))
            .put
        else if (std.mem.eql(u8, op_str, "delete"))
            .delete
        else
            return error.InvalidOp;

        // Check for optional dep:<id> field
        var depends_on: ?u64 = null;
        var rel_path: []const u8 = after_op;
        if (std.mem.startsWith(u8, after_op, "dep:")) {
            const dep_end = std.mem.indexOfScalar(u8, after_op, ' ') orelse return error.InvalidFormat;
            const dep_str = after_op[4..dep_end]; // skip "dep:"
            depends_on = std.fmt.parseUnsigned(u64, dep_str, 10) catch return error.InvalidFormat;
            rel_path = after_op[dep_end + 1 ..];
        }

        const id = self.next_id;
        self.next_id += 1;
        try self.entries.append(self.allocator, .{
            .id = id,
            .op = op,
            .path = try self.allocator.dupe(u8, rel_path),
            .depends_on = depends_on,
        });
    }

    fn enqueue(self: *ReplLog, op: ReplOp, rel_path: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const path_copy = try self.allocator.dupe(u8, rel_path);
        const id = self.next_id;
        self.next_id += 1;
        self.entries.append(self.allocator, .{ .id = id, .op = op, .path = path_copy }) catch |err| {
            self.allocator.free(path_copy);
            return err;
        };

        // Write to disk
        self.appendToDisk(op, rel_path) catch |err| {
            log.err("failed to append to replication log: {}", .{err});
        };

        // Update pending metric
        g_state.metrics.repl_pending.store(self.pendingCountLocked(), .release);

        self.cond.signal();
    }

    fn enqueuePair(self: *ReplLog, op1: ReplOp, path1: []const u8, op2: ReplOp, path2: []const u8) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const p1 = self.allocator.dupe(u8, path1) catch {
            log.err("OOM in enqueuePair for path: {s}", .{path1});
            return;
        };
        const p2 = self.allocator.dupe(u8, path2) catch {
            self.allocator.free(p1);
            log.err("OOM in enqueuePair for path: {s}", .{path2});
            return;
        };

        const id1 = self.next_id;
        self.next_id += 1;
        const id2 = self.next_id;
        self.next_id += 1;

        self.entries.append(self.allocator, .{ .id = id1, .op = op1, .path = p1 }) catch {
            self.allocator.free(p1);
            self.allocator.free(p2);
            log.err("OOM in enqueuePair append for path: {s}", .{path1});
            return;
        };
        self.entries.append(self.allocator, .{ .id = id2, .op = op2, .path = p2, .depends_on = id1 }) catch {
            // Roll back the first append
            _ = self.entries.pop();
            self.allocator.free(p1);
            self.allocator.free(p2);
            log.err("OOM in enqueuePair append for path: {s}", .{path2});
            return;
        };

        // Write both entries to disk with a single fsync
        self.appendPairToDisk(op1, path1, id1, op2, path2) catch |err| {
            log.err("failed to append pair to replication log: {}", .{err});
        };

        g_state.metrics.repl_pending.store(self.pendingCountLocked(), .release);
        self.cond.broadcast();
    }

    fn appendToDisk(self: *ReplLog, op: ReplOp, rel_path: []const u8) !void {
        const path = try self.logPath();
        defer self.allocator.free(path);

        const file = try std.fs.createFileAbsolute(path, .{ .truncate = false });
        defer file.close();
        try file.seekFromEnd(0);

        const line = try formatLogEntry(self.allocator, op, rel_path, null);
        defer self.allocator.free(line);
        try file.writeAll(line);
        try file.sync();
    }

    fn appendPairToDisk(self: *ReplLog, op1: ReplOp, path1: []const u8, id1: u64, op2: ReplOp, path2: []const u8) !void {
        const path = try self.logPath();
        defer self.allocator.free(path);

        const file = try std.fs.createFileAbsolute(path, .{ .truncate = false });
        defer file.close();
        try file.seekFromEnd(0);

        const line1 = try formatLogEntry(self.allocator, op1, path1, null);
        defer self.allocator.free(line1);
        const line2 = try formatLogEntry(self.allocator, op2, path2, id1);
        defer self.allocator.free(line2);
        try file.writeAll(line1);
        try file.writeAll(line2);
        try file.sync();
    }

    fn dequeueNext(self: *ReplLog) ?struct { id: u64, op: ReplOp, path: []const u8 } {
        self.mutex.lock();
        defer self.mutex.unlock();

        while (!g_state.shutdown.load(.acquire)) {
            // Find next non-completed, non-in-flight entry
            for (self.entries.items, 0..) |*entry, i| {
                if (entry.completed or entry.in_flight) continue;

                // For put entries, check if there's a newer put for the same path
                if (entry.op == .put) {
                    var dominated = false;
                    for (self.entries.items[i + 1 ..]) |*later| {
                        if (later.op == .put and !later.completed and std.mem.eql(u8, later.path, entry.path)) {
                            dominated = true;
                            break;
                        }
                    }
                    if (dominated) {
                        // Skip stale put — mark as completed
                        entry.completed = true;
                        self.completed_count += 1;
                        continue;
                    }
                }

                // Enforce depends_on ordering: skip if dependency not yet completed
                if (entry.depends_on) |dep_id| {
                    const dep_done = for (self.entries.items) |*dep| {
                        if (dep.id == dep_id) break dep.completed;
                    } else true; // dependency already truncated → treat as done
                    if (!dep_done) continue;
                }

                entry.in_flight = true;
                return .{ .id = entry.id, .op = entry.op, .path = entry.path };
            }

            // No work available, wait
            self.cond.wait(&self.mutex);
        }
        return null;
    }

    fn markCompleted(self: *ReplLog, id: u64) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.entries.items) |*entry| {
            if (entry.id == id) {
                entry.completed = true;
                entry.in_flight = false;
                self.completed_count += 1;
                break;
            }
        }

        g_state.metrics.repl_pending.store(self.pendingCountLocked(), .release);

        // Check if truncation is needed
        self.maybeTruncate();
    }

    /// Mark all pending entries for `rel_path` as completed.
    /// Used in tests to simulate the worker completing replication.
    fn markCompletedByPath(self: *ReplLog, rel_path: []const u8) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.entries.items) |*entry| {
            if (!entry.completed and std.mem.eql(u8, entry.path, rel_path)) {
                entry.completed = true;
                entry.in_flight = false;
                self.completed_count += 1;
            }
        }

        g_state.metrics.repl_pending.store(self.pendingCountLocked(), .release);
    }

    fn pendingCountLocked(self: *ReplLog) u64 {
        var count: u64 = 0;
        for (self.entries.items) |entry| {
            if (!entry.completed) count += 1;
        }
        return count;
    }

    fn hasPendingPut(self: *ReplLog, rel_path: []const u8) bool {
        self.mutex.lock();
        defer self.mutex.unlock();
        for (self.entries.items) |entry| {
            if (!entry.completed and entry.op == .put and std.mem.eql(u8, entry.path, rel_path)) {
                return true;
            }
        }
        return false;
    }

    fn maybeTruncate(self: *ReplLog) void {
        const total = self.entries.items.len;
        if (total == 0 or self.completed_count == 0) return;

        const now = std.time.timestamp();
        const should_truncate = (self.completed_count * 2 > total) or
            (now - self.last_truncate_time >= 60);

        if (!should_truncate) return;

        // Collect remaining (non-completed) entries first.  Only free
        // completed entries once we know the new list is fully built,
        // so an OOM here does not corrupt the existing entry list.
        var remaining: std.ArrayList(ReplEntry) = .{};
        for (self.entries.items) |entry| {
            if (!entry.completed) {
                remaining.append(self.allocator, entry) catch {
                    remaining.deinit(self.allocator);
                    log.err("OOM during replication log truncation, skipping", .{});
                    return;
                };
            }
        }

        // Success — now free completed entries' paths
        for (self.entries.items) |entry| {
            if (entry.completed) {
                self.allocator.free(entry.path);
            }
        }

        self.entries.deinit(self.allocator);
        self.entries = remaining;
        self.completed_count = 0;
        self.last_truncate_time = now;

        // Atomic rewrite of log file
        self.rewriteLogAtomic() catch |err| {
            log.err("failed to truncate replication log: {}", .{err});
        };
    }

    fn rewriteLogAtomic(self: *ReplLog) !void {
        const tmp_path = try std.fs.path.join(self.allocator, &.{ self.backing_dir, ".helmetfs", "repl.log.tmp" });
        defer self.allocator.free(tmp_path);
        const log_path = try self.logPath();
        defer self.allocator.free(log_path);

        // Write remaining entries to temp file
        const tmp_file = try std.fs.createFileAbsolute(tmp_path, .{});
        defer tmp_file.close();

        for (self.entries.items) |entry| {
            const line = try formatLogEntry(self.allocator, entry.op, entry.path, entry.depends_on);
            defer self.allocator.free(line);
            try tmp_file.writeAll(line);
        }
        try tmp_file.sync();

        // Rename over the original
        try std.fs.renameAbsolute(tmp_path, log_path);

        // Fsync parent directory to persist the rename
        if (std.fs.path.dirname(log_path)) |dir_path| {
            fsyncDir(dir_path);
        }
    }
};

fn formatLogEntry(allocator: std.mem.Allocator, op: ReplOp, rel_path: []const u8, depends_on: ?u64) ![]const u8 {
    const op_str = switch (op) {
        .put => "put",
        .delete => "delete",
    };
    // Remainder is " <op> [dep:<id> ]<path>"
    const remainder = if (depends_on) |dep_id|
        try std.fmt.allocPrint(allocator, " {s} dep:{d} {s}", .{ op_str, dep_id, rel_path })
    else
        try std.fmt.allocPrint(allocator, " {s} {s}", .{ op_str, rel_path });
    defer allocator.free(remainder);

    const crc_val = std.hash.crc.Crc32IsoHdlc.hash(remainder);
    return try std.fmt.allocPrint(allocator, "{x:0>8}{s}\n", .{ crc_val, remainder });
}

// ============================================================================
// Metrics
// ============================================================================

const Metrics = struct {
    repl_pending: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    repl_completed: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    repl_errors: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    scrub_files_checked: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    scrub_corruptions: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    scrub_repairs: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    scrub_last_completed: std.atomic.Value(i64) = std.atomic.Value(i64).init(0),
    scrub_duration_ms: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
};

fn metricsServerLoop(state: *FsState) void {
    const addr_str = state.metrics_addr orelse return;
    const port = parseMetricsAddr(addr_str) catch |err| {
        log.err("invalid metrics address '{s}': {}", .{ addr_str, err });
        return;
    };

    const addr = std.net.Address.parseIp4("0.0.0.0", port) catch |err| {
        log.err("failed to parse metrics address: {}", .{err});
        return;
    };

    var server = addr.listen(.{ .reuse_address = true }) catch |err| {
        log.err("failed to start metrics server: {}", .{err});
        return;
    };
    defer server.deinit();

    log.info("metrics server listening on :{d}", .{port});

    while (!state.shutdown.load(.acquire)) {
        const conn = server.accept() catch |err| {
            if (state.shutdown.load(.acquire)) break;
            log.err("metrics accept error: {}", .{err});
            continue;
        };

        handleMetricsConn(state, conn.stream) catch |err| {
            log.err("metrics connection error: {}", .{err});
        };
    }
}

fn handleMetricsConn(state: *FsState, stream: std.net.Stream) !void {
    defer stream.close();

    // Read the HTTP request, looping until we see the end-of-headers
    // marker (\r\n\r\n) or the buffer is full.
    var buf: [4096]u8 = undefined;
    var total: usize = 0;
    while (total < buf.len) {
        const n = stream.read(buf[total..]) catch return;
        if (n == 0) break; // client closed connection
        total += n;
        // Check for end of HTTP headers
        if (std.mem.indexOf(u8, buf[0..total], "\r\n\r\n") != null) break;
    }
    const request = buf[0..total];

    if (std.mem.startsWith(u8, request, "GET /metrics ") or std.mem.startsWith(u8, request, "GET /metrics\r")) {
        const body = formatMetrics(state) catch return;
        defer state.allocator.free(body);

        const header = std.fmt.allocPrint(state.allocator, "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n", .{body.len}) catch return;
        defer state.allocator.free(header);

        stream.writeAll(header) catch return;
        stream.writeAll(body) catch return;
    } else {
        const resp = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        stream.writeAll(resp) catch return;
    }
}

fn formatMetrics(state: *FsState) ![]const u8 {
    const m = &state.metrics;
    const duration_ms = m.scrub_duration_ms.load(.acquire);
    const duration: f64 = @as(f64, @floatFromInt(duration_ms)) / 1000.0;

    return try std.fmt.allocPrint(state.allocator,
        \\# HELP helmetfs_replication_pending Pending replication log entries.
        \\# TYPE helmetfs_replication_pending gauge
        \\helmetfs_replication_pending {d}
        \\# HELP helmetfs_replication_completed_total Files replicated.
        \\# TYPE helmetfs_replication_completed_total counter
        \\helmetfs_replication_completed_total {d}
        \\# HELP helmetfs_replication_errors_total Replication errors.
        \\# TYPE helmetfs_replication_errors_total counter
        \\helmetfs_replication_errors_total {d}
        \\# HELP helmetfs_scrub_files_checked_total Files checked across all scrubs.
        \\# TYPE helmetfs_scrub_files_checked_total counter
        \\helmetfs_scrub_files_checked_total {d}
        \\# HELP helmetfs_scrub_corruptions_found_total Corruption detections.
        \\# TYPE helmetfs_scrub_corruptions_found_total counter
        \\helmetfs_scrub_corruptions_found_total {d}
        \\# HELP helmetfs_scrub_repairs_total Successful repairs from replica.
        \\# TYPE helmetfs_scrub_repairs_total counter
        \\helmetfs_scrub_repairs_total {d}
        \\# HELP helmetfs_scrub_last_completed_timestamp Unix timestamp of last scrub.
        \\# TYPE helmetfs_scrub_last_completed_timestamp gauge
        \\helmetfs_scrub_last_completed_timestamp {d}
        \\# HELP helmetfs_scrub_duration_seconds Duration of last scrub.
        \\# TYPE helmetfs_scrub_duration_seconds gauge
        \\helmetfs_scrub_duration_seconds {d:.3}
        \\
    , .{
        m.repl_pending.load(.acquire),
        m.repl_completed.load(.acquire),
        m.repl_errors.load(.acquire),
        m.scrub_files_checked.load(.acquire),
        m.scrub_corruptions.load(.acquire),
        m.scrub_repairs.load(.acquire),
        m.scrub_last_completed.load(.acquire),
        duration,
    });
}

fn parseMetricsAddr(addr_str: []const u8) !u16 {
    // Format: ":9090" or "9090"
    if (addr_str.len == 0) return error.InvalidFormat;
    const port_str = if (addr_str[0] == ':') addr_str[1..] else addr_str;
    return std.fmt.parseUnsigned(u16, port_str, 10);
}

// ============================================================================
// Checksum Computation
// ============================================================================

fn computeBlake3(backing_path: []const u8) ![64]u8 {
    const file = try std.fs.openFileAbsolute(backing_path, .{});
    defer file.close();

    // Advisory read lock
    _ = c.flock(file.handle, c.LOCK_SH);
    defer _ = c.flock(file.handle, c.LOCK_UN);

    var hasher = std.crypto.hash.Blake3.init(.{});
    var buf: [64 * 1024]u8 = undefined; // 64 KB — safe for default thread stacks
    while (true) {
        const n = try file.readAll(&buf);
        if (n == 0) break;
        hasher.update(buf[0..n]);
    }

    var digest: [32]u8 = undefined;
    hasher.final(&digest);
    return std.fmt.bytesToHex(digest, .lower);
}

fn writeSumFile(sum_path: []const u8, hex_digest: []const u8) !void {
    const file = try std.fs.createFileAbsolute(sum_path, .{});
    defer file.close();
    try file.writeAll(hex_digest);
    try file.writeAll("\n");
    try file.sync();
}

fn readSumFile(allocator: std.mem.Allocator, sum_path: []const u8) ![]const u8 {
    const file = std.fs.openFileAbsolute(sum_path, .{}) catch |err| return err;
    defer file.close();
    var buf: [128]u8 = undefined;
    const n = try file.readAll(&buf);
    const content = buf[0..n];
    const trimmed = std.mem.trimRight(u8, content, "\n\r ");
    return try allocator.dupe(u8, trimmed);
}

fn checksumAndEnqueue(state: *FsState, rel_path: []const u8) !void {
    // Skip if the file still has open write descriptors — checksumming a
    // partially-written file would produce a wrong digest.  The dirty flag
    // stays set so that release() will retry once the last writer closes.
    if (state.path_state.hasWriteRef(rel_path)) return;

    try checksumAndEnqueueForced(state, rel_path);
}

/// Like checksumAndEnqueue but bypasses the write-ref check.  Used by
/// fuse_fsync where the caller has already flushed data to disk and
/// wants to trigger replication even though the file is still open.
fn checksumAndEnqueueForced(state: *FsState, rel_path: []const u8) !void {
    // Snapshot the dirty generation before computing the checksum.
    // If a concurrent write bumps the generation while we're hashing,
    // clearDirtyIfGen will leave the dirty flag set so a subsequent
    // release/fsync picks it up.
    const gen = state.path_state.getDirtyGen(rel_path);

    const backing_path = try std.fs.path.join(state.allocator, &.{ state.backing_dir, rel_path });
    defer state.allocator.free(backing_path);
    const sum_path = try std.fmt.allocPrint(state.allocator, "{s}.sum", .{backing_path});
    defer state.allocator.free(sum_path);

    const hex_digest = try computeBlake3(backing_path);

    try writeSumFile(sum_path, &hex_digest);

    try state.repl_log.enqueue(.put, rel_path);
    state.path_state.clearDirtyIfGen(rel_path, gen);
}

// ============================================================================
// Replication Workers
// ============================================================================

fn replWorkerLoop(state: *FsState) void {
    while (!state.shutdown.load(.acquire)) {
        const work = state.repl_log.dequeueNext() orelse break;

        var backoff_ns: u64 = 1_000_000_000; // 1 second
        const max_backoff_ns: u64 = 300_000_000_000; // 5 minutes

        while (!state.shutdown.load(.acquire)) {
            const result = switch (work.op) {
                .put => replicatePut(state, work.path),
                .delete => replicateDelete(state, work.path),
            };

            if (result) |_| {
                state.repl_log.markCompleted(work.id);
                _ = state.metrics.repl_completed.fetchAdd(1, .release);
                break;
            } else |err| {
                _ = state.metrics.repl_errors.fetchAdd(1, .release);
                log.err("replication error for {s}: {}, retrying in {d}s", .{
                    work.path,
                    err,
                    backoff_ns / 1_000_000_000,
                });
                std.Thread.sleep(backoff_ns);
                backoff_ns = @min(backoff_ns * 2, max_backoff_ns);
            }
        }
    }
}

fn replicatePut(state: *FsState, rel_path: []const u8) !void {
    const backing_path = try std.fs.path.join(state.allocator, &.{ state.backing_dir, rel_path });
    defer state.allocator.free(backing_path);
    const replica_path = try std.fs.path.join(state.allocator, &.{ state.replica_dir, "files", rel_path });
    defer state.allocator.free(replica_path);

    // Check if source is a symlink
    const backing_stat = posix.fstatat(posix.AT.FDCWD, backing_path, posix.AT.SYMLINK_NOFOLLOW) catch |err| switch (err) {
        error.FileNotFound => return, // File was deleted before we could replicate
        else => return err,
    };

    if (backing_stat.mode & posix.S.IFMT == posix.S.IFLNK) {
        // Replicate symlink
        try ensureParentDir(replica_path);
        // Remove existing if any
        std.fs.deleteFileAbsolute(replica_path) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };
        // Read symlink target
        const backing_z = try state.allocator.dupeZ(u8, backing_path);
        defer state.allocator.free(backing_z);
        const replica_z = try state.allocator.dupeZ(u8, replica_path);
        defer state.allocator.free(replica_z);
        var link_buf: [std.fs.max_path_bytes]u8 = undefined;
        const target = posix.readlinkat(posix.AT.FDCWD, backing_z, &link_buf) catch return error.ReadLinkFailed;
        // Create symlink on replica
        posix.symlinkat(target, posix.AT.FDCWD, replica_z) catch return error.SymlinkFailed;
        return;
    }

    // Regular file replication
    const sum_backing = try std.fmt.allocPrint(state.allocator, "{s}.sum", .{backing_path});
    defer state.allocator.free(sum_backing);
    const sum_replica = try std.fmt.allocPrint(state.allocator, "{s}.sum", .{replica_path});
    defer state.allocator.free(sum_replica);

    // Copy file content
    try ensureParentDir(replica_path);
    try copyFileWithSync(backing_path, replica_path);

    // Copy .sum sidecar
    copyFileWithSync(sum_backing, sum_replica) catch |err| switch (err) {
        error.FileNotFound => {}, // .sum might not exist yet
        else => return err,
    };

    // Preserve mode bits and ownership
    const stat_info = posix.fstatat(posix.AT.FDCWD, backing_path, 0) catch return;
    const mode: posix.mode_t = stat_info.mode & 0o7777;
    posix.fchmodat(posix.AT.FDCWD, replica_path, mode, 0) catch {};

    // chown (may fail if not root, that's OK)
    const replica_z = state.allocator.dupeZ(u8, replica_path) catch return;
    defer state.allocator.free(replica_z);
    _ = c.chown(replica_z.ptr, stat_info.uid, stat_info.gid);
}

fn replicateDelete(state: *FsState, rel_path: []const u8) !void {
    const replica_path = try std.fs.path.join(state.allocator, &.{ state.replica_dir, "files", rel_path });
    defer state.allocator.free(replica_path);
    const sum_replica = try std.fmt.allocPrint(state.allocator, "{s}.sum", .{replica_path});
    defer state.allocator.free(sum_replica);

    // Idempotent delete
    std.fs.deleteFileAbsolute(replica_path) catch |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    };
    std.fs.deleteFileAbsolute(sum_replica) catch |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    };
}

fn copyFileWithSync(src_path: []const u8, dst_path: []const u8) !void {
    const src = try std.fs.openFileAbsolute(src_path, .{});
    defer src.close();

    // Write to a temporary file next to the destination, then atomically rename.
    // This prevents readers from seeing a partially-written file.
    var tmp_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const tmp_path = std.fmt.bufPrint(&tmp_path_buf, "{s}.tmp", .{dst_path}) catch return error.NameTooLong;

    const dst = std.fs.createFileAbsolute(tmp_path, .{}) catch {
        // Fall back to direct write if we can't create the temp file
        src.seekTo(0) catch {};
        return copyFileDirectWithSync(src, dst_path);
    };

    var ok = false;
    defer {
        if (!ok) {
            dst.close();
            std.fs.deleteFileAbsolute(tmp_path) catch {};
        }
    }

    var buf: [64 * 1024]u8 = undefined; // 64 KB — safe for default thread stacks
    while (true) {
        const n = try src.read(&buf);
        if (n == 0) break;
        try dst.writeAll(buf[0..n]);
    }
    try dst.sync();
    dst.close();
    ok = true;

    std.fs.renameAbsolute(tmp_path, dst_path) catch |err| {
        std.fs.deleteFileAbsolute(tmp_path) catch {};
        return err;
    };

    // Fsync parent directory to persist the rename
    if (std.fs.path.dirname(dst_path)) |dir_path| {
        fsyncDir(dir_path);
    }
}

fn copyFileDirectWithSync(src: std.fs.File, dst_path: []const u8) !void {
    const dst = try std.fs.createFileAbsolute(dst_path, .{});
    defer dst.close();

    var buf: [64 * 1024]u8 = undefined;
    while (true) {
        const n = try src.read(&buf);
        if (n == 0) break;
        try dst.writeAll(buf[0..n]);
    }
    try dst.sync();
}

fn fsyncDir(dir_path: []const u8) void {
    var dir = std.fs.openDirAbsolute(dir_path, .{}) catch return;
    defer dir.close();
    posix.fsync(dir.fd) catch {};
}

fn ensureParentDir(path: []const u8) !void {
    const dir_path = std.fs.path.dirname(path) orelse return;
    std.fs.makeDirAbsolute(dir_path) catch |err| switch (err) {
        error.PathAlreadyExists => return,
        error.FileNotFound => {
            // Parent of parent doesn't exist, recurse
            try ensureParentDir(dir_path);
            std.fs.makeDirAbsolute(dir_path) catch |e| switch (e) {
                error.PathAlreadyExists => return,
                else => return e,
            };
        },
        else => return err,
    };
}

// ============================================================================
// Self-Healing Scrub
// ============================================================================

fn scrubLoop(state: *FsState) void {
    // Check if we need an immediate scrub
    if (shouldScrubImmediately(state)) {
        log.info("scrub overdue, running immediately", .{});
        runScrub(state);
    }

    while (!state.shutdown.load(.acquire)) {
        // Sleep until next scrub time
        const sleep_ns = nsUntilNextScrub(state.scrub_hour, state.scrub_minute);
        // Sleep in small increments to check for shutdown
        var remaining = sleep_ns;
        while (remaining > 0 and !state.shutdown.load(.acquire)) {
            const chunk = @min(remaining, 1_000_000_000); // 1 second
            std.Thread.sleep(chunk);
            remaining -= chunk;
        }
        if (state.shutdown.load(.acquire)) break;
        runScrub(state);
    }
}

fn shouldScrubImmediately(state: *FsState) bool {
    const ts_path = std.fs.path.join(state.allocator, &.{ state.backing_dir, ".helmetfs", "scrub.timestamp" }) catch return true;
    defer state.allocator.free(ts_path);

    const file = std.fs.openFileAbsolute(ts_path, .{}) catch return true;
    defer file.close();

    var buf: [64]u8 = undefined;
    const n = file.read(&buf) catch return true;
    const content = std.mem.trimRight(u8, buf[0..n], "\n\r ");
    const last_scrub = std.fmt.parseInt(i64, content, 10) catch return true;

    const now = std.time.timestamp();
    return (now - last_scrub) > 86400; // 24 hours
}

fn nsUntilNextScrub(target_hour: u8, target_minute: u8) u64 {
    const now_ts = std.time.timestamp();
    var time_val: c.time_t = @intCast(now_ts);
    var tm: c.struct_tm = undefined;
    _ = c.localtime_r(&time_val, &tm);

    const local_day_sec: u64 = @intCast(@as(i64, tm.tm_hour) * 3600 + @as(i64, tm.tm_min) * 60 + @as(i64, tm.tm_sec));
    const target_sec: u64 = @as(u64, target_hour) * 3600 + @as(u64, target_minute) * 60;

    const secs_until = if (target_sec > local_day_sec)
        target_sec - local_day_sec
    else
        86400 - local_day_sec + target_sec;

    return secs_until * 1_000_000_000;
}

fn runScrub(state: *FsState) void {
    log.info("starting scrub", .{});
    const start_ms = std.time.milliTimestamp();
    var files_checked: u64 = 0;
    var corruptions_found: u64 = 0;
    var repairs: u64 = 0;

    // Walk the backing directory
    var dir = std.fs.openDirAbsolute(state.backing_dir, .{ .iterate = true }) catch |err| {
        log.err("scrub: failed to open backing dir: {}", .{err});
        return;
    };
    defer dir.close();

    var walker = dir.walk(state.allocator) catch |err| {
        log.err("scrub: failed to walk backing dir: {}", .{err});
        return;
    };
    defer walker.deinit();

    while (walker.next() catch null) |entry| {
        if (state.shutdown.load(.acquire)) break;

        // Skip directories
        if (entry.kind == .directory) continue;
        // Skip symlinks (no .sum files for symlinks)
        if (entry.kind == .sym_link) continue;
        // Skip .helmetfs directory
        if (std.mem.startsWith(u8, entry.path, ".helmetfs")) continue;
        // Skip .sum sidecar files
        if (std.mem.endsWith(u8, entry.path, ".sum")) continue;

        const rel_path = state.allocator.dupe(u8, entry.path) catch continue;
        defer state.allocator.free(rel_path);

        // Skip files with open write descriptors
        if (state.path_state.hasWriteRef(rel_path)) continue;

        scrubFile(state, rel_path, &corruptions_found, &repairs) catch |err| {
            log.err("scrub: error checking {s}: {}", .{ rel_path, err });
        };
        files_checked += 1;
    }

    const end = std.time.timestamp();
    const end_ms = std.time.milliTimestamp();
    const duration_ms: u64 = @intCast(end_ms - start_ms);

    // Update metrics
    _ = state.metrics.scrub_files_checked.fetchAdd(files_checked, .release);
    _ = state.metrics.scrub_corruptions.fetchAdd(corruptions_found, .release);
    _ = state.metrics.scrub_repairs.fetchAdd(repairs, .release);
    state.metrics.scrub_last_completed.store(end, .release);
    state.metrics.scrub_duration_ms.store(duration_ms, .release);

    // Write scrub timestamp
    writeScrubTimestamp(state, end);

    log.info("scrub complete: checked={d}, corruptions={d}, repairs={d}, duration={d}ms", .{
        files_checked, corruptions_found, repairs, duration_ms,
    });
}

fn scrubFile(state: *FsState, rel_path: []const u8, corruptions: *u64, repairs_count: *u64) !void {
    const backing_path = try std.fs.path.join(state.allocator, &.{ state.backing_dir, rel_path });
    defer state.allocator.free(backing_path);
    const sum_path = try std.fmt.allocPrint(state.allocator, "{s}.sum", .{backing_path});
    defer state.allocator.free(sum_path);

    // Compute current checksum
    const current_hex = computeBlake3(backing_path) catch |err| {
        log.err("scrub: failed to compute checksum for {s}: {}", .{ rel_path, err });
        return err;
    };

    // Try to read existing .sum file
    const stored_hex = readSumFile(state.allocator, sum_path) catch |err| switch (err) {
        error.FileNotFound => {
            // Untracked file — adopt it
            log.info("scrub: adopting untracked file {s}", .{rel_path});
            writeSumFile(sum_path, &current_hex) catch |we| {
                log.err("scrub: failed to write .sum for {s}: {}", .{ rel_path, we });
                return we;
            };
            state.repl_log.enqueue(.put, rel_path) catch |enq_err| {
                log.err("scrub: failed to enqueue replication for {s}: {}", .{ rel_path, enq_err });
            };
            return;
        },
        else => return err,
    };
    defer state.allocator.free(stored_hex);

    // Compare checksums
    if (std.mem.eql(u8, &current_hex, stored_hex)) {
        return; // All good
    }

    // Checksum mismatch — corruption detected
    corruptions.* += 1;
    log.warn("scrub: CORRUPTION detected in {s}", .{rel_path});

    // Check for pending replication (stale replica warning)
    const has_pending = state.repl_log.hasPendingPut(rel_path);

    // Attempt repair from replica
    const replica_path = try std.fs.path.join(state.allocator, &.{ state.replica_dir, "files", rel_path });
    defer state.allocator.free(replica_path);
    const replica_sum_path = try std.fmt.allocPrint(state.allocator, "{s}.sum", .{replica_path});
    defer state.allocator.free(replica_sum_path);

    // Read replica checksum
    const replica_hex = readSumFile(state.allocator, replica_sum_path) catch {
        log.err("scrub: replica unavailable for repair of {s}", .{rel_path});
        return;
    };
    defer state.allocator.free(replica_hex);

    // Verify replica file integrity
    const replica_computed = computeBlake3(replica_path) catch {
        log.err("scrub: cannot read replica file for repair of {s}", .{rel_path});
        return;
    };

    if (!std.mem.eql(u8, &replica_computed, replica_hex)) {
        log.warn("scrub: replica also corrupt for {s}, cannot repair", .{rel_path});
        return;
    }

    // Repair from replica
    if (has_pending) {
        log.warn("scrub: skipping repair of {s} — pending replication means replica is stale", .{rel_path});
        return;
    }

    // Re-check write ref before overwriting.  A writer may have opened the
    // file between the initial hasWriteRef check in runScrub and now; if
    // so the "corruption" is really an in-progress write and we must not
    // clobber the file.
    if (state.path_state.hasWriteRef(rel_path)) {
        log.info("scrub: skipping repair of {s} — file now has open writer", .{rel_path});
        return;
    }

    log.info("scrub: repairing {s} from replica", .{rel_path});

    copyFileWithSync(replica_path, backing_path) catch |err| {
        log.err("scrub: failed to repair {s}: {}", .{ rel_path, err });
        return;
    };
    // Rewrite .sum file
    writeSumFile(sum_path, &replica_computed) catch |err| {
        log.err("scrub: failed to write .sum after repair of {s}: {}", .{ rel_path, err });
        return;
    };
    repairs_count.* += 1;
    log.info("scrub: successfully repaired {s}", .{rel_path});
}

fn writeScrubTimestamp(state: *FsState, ts: i64) void {
    const ts_path = std.fs.path.join(state.allocator, &.{ state.backing_dir, ".helmetfs", "scrub.timestamp" }) catch return;
    defer state.allocator.free(ts_path);

    const file = std.fs.createFileAbsolute(ts_path, .{}) catch return;
    defer file.close();

    const ts_str = std.fmt.allocPrint(state.allocator, "{d}\n", .{ts}) catch return;
    defer state.allocator.free(ts_str);
    file.writeAll(ts_str) catch return;
    file.sync() catch return;
}

// ============================================================================
// Hidden Paths
// ============================================================================

fn isHiddenPath(state: *FsState, rel_path: []const u8) bool {
    // .helmetfs/ directory
    if (std.mem.startsWith(u8, rel_path, ".helmetfs")) return true;

    // .sum sidecar files — only hidden when corresponding data file exists
    if (std.mem.endsWith(u8, rel_path, ".sum")) {
        const data_path_len = rel_path.len - 4; // strip ".sum"
        const data_rel = rel_path[0..data_path_len];
        const data_full = std.fs.path.join(state.allocator, &.{ state.backing_dir, data_rel }) catch return false;
        defer state.allocator.free(data_full);
        // Check if data file exists
        _ = posix.fstatat(posix.AT.FDCWD, data_full, posix.AT.SYMLINK_NOFOLLOW) catch return false;
        return true; // Data file exists, so hide the .sum
    }

    return false;
}

// ============================================================================
// FUSE Operations
// ============================================================================

fn fuseRelPath(path: [*c]const u8) []const u8 {
    const s = std.mem.span(@as([*:0]const u8, @ptrCast(path)));
    // Strip leading '/'
    if (s.len > 0 and s[0] == '/') return s[1..];
    return s;
}

fn fuseSentinel(path: [*c]const u8) [*:0]const u8 {
    return @ptrCast(path);
}

fn backingPath(allocator: std.mem.Allocator, state: *FsState, rel_path: []const u8) ![:0]const u8 {
    if (rel_path.len == 0) {
        return try allocator.dupeZ(u8, state.backing_dir);
    }
    const joined = try std.fs.path.join(allocator, &.{ state.backing_dir, rel_path });
    defer allocator.free(joined);
    return try allocator.dupeZ(u8, joined);
}

fn fuseErr(e: posix.E) c_int {
    return -@as(c_int, @intCast(@intFromEnum(e)));
}

fn posixErr(err: anytype) c_int {
    const name = @errorName(err);
    const map = .{
        .{ "FileNotFound", posix.E.NOENT },
        .{ "AccessDenied", posix.E.ACCES },
        .{ "NameTooLong", posix.E.NAMETOOLONG },
        .{ "SymLinkLoop", posix.E.LOOP },
        .{ "NotDir", posix.E.NOTDIR },
        .{ "FileTooBig", posix.E.FBIG },
        .{ "NoSpaceLeft", posix.E.NOSPC },
        .{ "IsDir", posix.E.ISDIR },
        .{ "ReadOnlyFileSystem", posix.E.ROFS },
        .{ "DiskQuota", posix.E.DQUOT },
        .{ "FileBusy", posix.E.BUSY },
        .{ "PathAlreadyExists", posix.E.EXIST },
        .{ "InvalidArgument", posix.E.INVAL },
        .{ "NotOpenForWriting", posix.E.BADF },
        .{ "OperationNotSupported", posix.E.OPNOTSUPP },
        .{ "BrokenPipe", posix.E.PIPE },
        .{ "ProcessFdQuotaExceeded", posix.E.MFILE },
        .{ "SystemFdQuotaExceeded", posix.E.NFILE },
        .{ "WouldBlock", posix.E.AGAIN },
        .{ "Unexpected", posix.E.IO },
    };
    inline for (map) |entry| {
        if (std.mem.eql(u8, name, entry[0])) {
            return fuseErr(entry[1]);
        }
    }
    return fuseErr(.IO);
}

// --- FUSE callback implementations ---

fn fuse_getattr(path: [*c]const u8, stbuf: [*c]c.struct_stat, fi: ?*c.struct_fuse_file_info) callconv(.c) c_int {
    _ = fi;
    const state = g_state;
    const rel = fuseRelPath(path);

    // Check hidden paths
    if (rel.len > 0 and isHiddenPath(state, rel)) {
        return fuseErr(.NOENT);
    }

    const backing = backingPath(state.allocator, state, rel) catch return fuseErr(.NOMEM);
    defer state.allocator.free(backing);

    const stat_result = posix.fstatat(posix.AT.FDCWD, backing, posix.AT.SYMLINK_NOFOLLOW);
    if (stat_result) |stat_val| {
        const buf: *posix.Stat = @ptrCast(@alignCast(stbuf));
        buf.* = stat_val;
        return 0;
    } else |err| {
        return posixErr(err);
    }
}

fn fuse_readdir(path: [*c]const u8, buf: ?*anyopaque, filler: c.fuse_fill_dir_t, _: c.off_t, _: ?*c.struct_fuse_file_info, _: c.enum_fuse_readdir_flags) callconv(.c) c_int {
    const state = g_state;
    const rel = fuseRelPath(path);

    const backing = backingPath(state.allocator, state, rel) catch return fuseErr(.NOMEM);
    defer state.allocator.free(backing);

    var dir = std.fs.openDirAbsoluteZ(backing, .{ .iterate = true }) catch |err| {
        return posixErr(err);
    };
    defer dir.close();

    // Always add . and ..
    _ = filler.?(buf, ".", null, 0, 0);
    _ = filler.?(buf, "..", null, 0, 0);

    var it = dir.iterate();
    while (it.next() catch null) |entry| {
        // Build the relative path for this entry to check if hidden
        const entry_rel = if (rel.len == 0)
            state.allocator.dupe(u8, entry.name) catch continue
        else
            std.fmt.allocPrint(state.allocator, "{s}/{s}", .{ rel, entry.name }) catch continue;
        defer state.allocator.free(entry_rel);

        if (isHiddenPath(state, entry_rel)) continue;

        const name_z = state.allocator.dupeZ(u8, entry.name) catch continue;
        defer state.allocator.free(name_z);
        _ = filler.?(buf, name_z.ptr, null, 0, 0);
    }

    return 0;
}

fn fuse_open(path: [*c]const u8, fi: ?*c.struct_fuse_file_info) callconv(.c) c_int {
    const state = g_state;
    const rel = fuseRelPath(path);

    if (rel.len > 0 and isHiddenPath(state, rel)) {
        return fuseErr(.NOENT);
    }

    const backing = backingPath(state.allocator, state, rel) catch return fuseErr(.NOMEM);
    defer state.allocator.free(backing);

    const flags: c_int = if (castFi(fi)) |f| f.flags else 0;
    const fd = posix.openZ(backing, @bitCast(flags), 0) catch |err| {
        return posixErr(err);
    };

    if (castFi(fi)) |f| {
        const raw_flags = @as(u32, @bitCast(flags));
        const acc_mode = raw_flags & 0o3;
        const has_trunc = (raw_flags & @as(u32, c.O_TRUNC)) != 0;
        const is_write = (acc_mode == 1 or acc_mode == 2 or has_trunc); // O_WRONLY, O_RDWR, or O_TRUNC
        f.fh = encodeFh(fd, is_write);
        if (is_write) {
            state.path_state.incWriteRef(rel);
        }
        // O_TRUNC modifies the file even without write access mode
        if (has_trunc) {
            state.path_state.setDirty(rel);
        }
    } else {
        // No file_info to store the fd — close to avoid leak
        posix.close(fd);
        return fuseErr(.BADF);
    }

    return 0;
}

fn fuse_read(path: [*c]const u8, buf_ptr: [*c]u8, size: usize, offset: c.off_t, fi: ?*c.struct_fuse_file_info) callconv(.c) c_int {
    const state = g_state;
    const rel = fuseRelPath(path);

    const fd: posix.fd_t = if (castFi(fi)) |f| decodeFh(f.fh).fd else return fuseErr(.BADF);

    // Verify reads if enabled
    if (state.verify_reads and rel.len > 0 and !state.path_state.hasWriteRef(rel) and state.path_state.shouldVerify(rel)) {
        verifyRead(state, rel) catch |err| {
            log.err("read verification failed for {s}: {}", .{ rel, err });
        };
    }

    const n = posix.pread(fd, buf_ptr[0..size], @intCast(offset)) catch {
        return fuseErr(.IO);
    };
    return @intCast(n);
}

fn verifyRead(state: *FsState, rel_path: []const u8) !void {
    const backing_path_str = try std.fs.path.join(state.allocator, &.{ state.backing_dir, rel_path });
    defer state.allocator.free(backing_path_str);
    const sum_path = try std.fmt.allocPrint(state.allocator, "{s}.sum", .{backing_path_str});
    defer state.allocator.free(sum_path);

    const stored_hex = readSumFile(state.allocator, sum_path) catch return; // No .sum = no verification
    defer state.allocator.free(stored_hex);

    const current_hex = computeBlake3(backing_path_str) catch return;
    if (!std.mem.eql(u8, &current_hex, stored_hex)) {
        log.err("READ VERIFICATION FAILED: {s} - checksum mismatch (scrub will attempt repair)", .{rel_path});
    }
}

fn fuse_write(path: [*c]const u8, data: [*c]const u8, size: usize, offset: c.off_t, fi: ?*c.struct_fuse_file_info) callconv(.c) c_int {
    const state = g_state;
    const rel = fuseRelPath(path);

    const fd: posix.fd_t = if (castFi(fi)) |f| decodeFh(f.fh).fd else return fuseErr(.BADF);

    const n = posix.pwrite(fd, @as([*]const u8, @ptrCast(data))[0..size], @intCast(offset)) catch {
        return fuseErr(.IO);
    };

    // Mark dirty
    if (rel.len > 0) {
        state.path_state.setDirty(rel);
    }

    return @intCast(n);
}

fn fuse_fsync(path: [*c]const u8, datasync: c_int, fi: ?*c.struct_fuse_file_info) callconv(.c) c_int {
    const state = g_state;
    const rel = fuseRelPath(path);

    // Forward fsync to backing dir
    if (castFi(fi)) |f| {
        const fd: posix.fd_t = decodeFh(f.fh).fd;
        if (datasync != 0) {
            posix.fdatasync(fd) catch {};
        } else {
            posix.fsync(fd) catch {};
        }
    }

    // If dirty, compute checksum and enqueue replication.
    // Use the forced variant because the file is still open (hasWriteRef is
    // true) but the data has been fsync'd to disk, so the checksum is valid.
    if (rel.len > 0 and state.path_state.isDirty(rel)) {
        checksumAndEnqueueForced(state, rel) catch |err| {
            log.err("fsync checksum failed for {s}: {}", .{ rel, err });
            return fuseErr(.IO);
        };
    }

    return 0;
}

fn fuse_release(path: [*c]const u8, fi: ?*c.struct_fuse_file_info) callconv(.c) c_int {
    const state = g_state;
    const rel = fuseRelPath(path);

    // Decrement write refcount BEFORE checksumming — checksumAndEnqueue skips
    // files that still have open write descriptors (hasWriteRef), so the
    // refcount must be decremented first to allow the checksum to proceed.
    if (castFi(fi)) |f| {
        const decoded = decodeFh(f.fh);
        if (decoded.opened_for_write) {
            state.path_state.decWriteRef(rel);
        }
        posix.close(decoded.fd);
    }

    // If dirty, compute checksum and enqueue replication
    if (rel.len > 0 and state.path_state.isDirty(rel)) {
        checksumAndEnqueue(state, rel) catch |err| {
            log.err("release checksum failed for {s}: {}", .{ rel, err });
        };
    }

    return 0;
}

fn fuse_create(path: [*c]const u8, mode: c.mode_t, fi: ?*c.struct_fuse_file_info) callconv(.c) c_int {
    const state = g_state;
    const rel = fuseRelPath(path);

    if (rel.len > 0 and isHiddenPath(state, rel)) {
        return fuseErr(.NOENT);
    }

    const backing = backingPath(state.allocator, state, rel) catch return fuseErr(.NOMEM);
    defer state.allocator.free(backing);

    const flags: c_int = if (castFi(fi)) |f| f.flags else 0;
    const fd = posix.openZ(backing, @bitCast(flags), mode) catch |err| {
        return posixErr(err);
    };

    if (castFi(fi)) |f| {
        f.fh = encodeFh(fd, true);
        // Create opens for writing
        state.path_state.incWriteRef(rel);
    } else {
        // No file_info to store the fd — close to avoid leak
        posix.close(fd);
        return fuseErr(.BADF);
    }

    return 0;
}

fn fuse_unlink(path: [*c]const u8) callconv(.c) c_int {
    const state = g_state;
    const rel = fuseRelPath(path);

    if (rel.len > 0 and isHiddenPath(state, rel)) {
        return fuseErr(.NOENT);
    }

    const backing = backingPath(state.allocator, state, rel) catch return fuseErr(.NOMEM);
    defer state.allocator.free(backing);

    // Delete the file
    std.fs.deleteFileAbsolute(backing) catch |err| {
        return posixErr(err);
    };

    // Remove .sum sidecar
    const sum_path = std.fmt.allocPrint(state.allocator, "{s}.sum", .{backing}) catch return 0;
    defer state.allocator.free(sum_path);
    std.fs.deleteFileAbsolute(sum_path) catch {};

    // Enqueue delete to replica
    if (rel.len > 0) {
        state.repl_log.enqueue(.delete, rel) catch |err| {
            log.err("failed to enqueue delete for {s}: {}", .{ rel, err });
        };
    }

    // Remove stale path state so the map doesn't grow unboundedly.
    state.path_state.remove(rel);

    return 0;
}

fn fuse_rename(from: [*c]const u8, to: [*c]const u8, flags: c_uint) callconv(.c) c_int {
    const state = g_state;
    const rel_from = fuseRelPath(from);
    const rel_to = fuseRelPath(to);

    // Handle rename flags (RENAME_NOREPLACE, RENAME_EXCHANGE, etc.)
    if (comptime builtin.os.tag == .linux) {
        const RENAME_NOREPLACE = 1;
        const RENAME_EXCHANGE = 2;
        // Reject any unsupported flags (RENAME_EXCHANGE, RENAME_WHITEOUT, etc.)
        if (flags & ~@as(c_uint, RENAME_NOREPLACE) != 0) {
            if (flags & RENAME_EXCHANGE != 0) {
                return fuseErr(.OPNOTSUPP);
            }
            return fuseErr(.INVAL);
        }
        if (flags & RENAME_NOREPLACE != 0) {
            // RENAME_NOREPLACE: fail if destination already exists
            const backing_to_check = backingPath(state.allocator, state, rel_to) catch return fuseErr(.NOMEM);
            defer state.allocator.free(backing_to_check);
            if (posix.fstatat(posix.AT.FDCWD, backing_to_check, posix.AT.SYMLINK_NOFOLLOW)) |_| {
                return fuseErr(.EXIST);
            } else |_| {}
        }
    } else {
        // macOS FUSE does not use rename flags
        if (flags != 0) {
            return fuseErr(.OPNOTSUPP);
        }
    }

    // Check hidden paths
    if (rel_from.len > 0 and isHiddenPath(state, rel_from)) {
        return fuseErr(.NOENT);
    }
    if (rel_to.len > 0 and isHiddenPath(state, rel_to)) {
        return fuseErr(.NOENT);
    }

    const backing_from = backingPath(state.allocator, state, rel_from) catch return fuseErr(.NOMEM);
    defer state.allocator.free(backing_from);
    const backing_to = backingPath(state.allocator, state, rel_to) catch return fuseErr(.NOMEM);
    defer state.allocator.free(backing_to);

    // Rename the data file
    std.fs.renameAbsolute(backing_from, backing_to) catch |err| {
        return posixErr(err);
    };

    // Move .sum sidecar alongside
    const sum_from = std.fmt.allocPrint(state.allocator, "{s}.sum", .{backing_from}) catch return 0;
    defer state.allocator.free(sum_from);
    const sum_to = std.fmt.allocPrint(state.allocator, "{s}.sum", .{backing_to}) catch return 0;
    defer state.allocator.free(sum_to);
    std.fs.renameAbsolute(sum_from, sum_to) catch {};

    // Enqueue delete(old) + put(new) as paired entries
    if (rel_from.len > 0 and rel_to.len > 0) {
        state.repl_log.enqueuePair(.delete, rel_from, .put, rel_to);
    }

    // Remove stale path state for the old name so the map doesn't grow
    // unboundedly.  (The new name will get fresh state on next access.)
    state.path_state.remove(rel_from);

    return 0;
}

fn fuse_mkdir(path: [*c]const u8, mode: c.mode_t) callconv(.c) c_int {
    const state = g_state;
    const rel = fuseRelPath(path);

    if (rel.len > 0 and isHiddenPath(state, rel)) {
        return fuseErr(.NOENT);
    }

    const backing = backingPath(state.allocator, state, rel) catch return fuseErr(.NOMEM);
    defer state.allocator.free(backing);

    std.fs.makeDirAbsolute(backing) catch |err| {
        return posixErr(err);
    };

    // Preserve mode
    posix.fchmodat(posix.AT.FDCWD, backing, mode, 0) catch {};

    return 0;
}

fn fuse_rmdir(path: [*c]const u8) callconv(.c) c_int {
    const state = g_state;
    const rel = fuseRelPath(path);

    const backing = backingPath(state.allocator, state, rel) catch return fuseErr(.NOMEM);
    defer state.allocator.free(backing);

    std.fs.deleteDirAbsolute(backing) catch |err| {
        return posixErr(err);
    };

    return 0;
}

fn fuse_symlink(target: [*c]const u8, linkpath: [*c]const u8) callconv(.c) c_int {
    const state = g_state;
    const rel = fuseRelPath(linkpath);

    const backing = backingPath(state.allocator, state, rel) catch return fuseErr(.NOMEM);
    defer state.allocator.free(backing);

    posix.symlinkat(std.mem.span(@as([*:0]const u8, @ptrCast(target))), posix.AT.FDCWD, backing) catch |err| {
        return posixErr(err);
    };

    // Enqueue for replication (symlinks are replicated)
    if (rel.len > 0) {
        state.repl_log.enqueue(.put, rel) catch |err| {
            log.err("failed to enqueue symlink replication for {s}: {}", .{ rel, err });
        };
    }

    return 0;
}
fn fuse_readlink(path: [*c]const u8, buf_ptr: [*c]u8, size: usize) callconv(.c) c_int {
    const state = g_state;
    const rel = fuseRelPath(path);

    if (rel.len > 0 and isHiddenPath(state, rel)) {
        return fuseErr(.NOENT);
    }

    const backing = backingPath(state.allocator, state, rel) catch return fuseErr(.NOMEM);
    defer state.allocator.free(backing);

    const buf: [*]u8 = @ptrCast(buf_ptr);
    const target = posix.readlinkat(posix.AT.FDCWD, backing, buf[0 .. size - 1]) catch |err| {
        return posixErr(err);
    };
    buf[target.len] = 0;

    return 0;
}

fn fuse_link(_: [*c]const u8, _: [*c]const u8) callconv(.c) c_int {
    return fuseErr(.OPNOTSUPP);
}

fn fuse_chmod(path: [*c]const u8, mode: c.mode_t, fi: ?*c.struct_fuse_file_info) callconv(.c) c_int {
    _ = fi;
    const state = g_state;
    const rel = fuseRelPath(path);

    const backing = backingPath(state.allocator, state, rel) catch return fuseErr(.NOMEM);
    defer state.allocator.free(backing);

    posix.fchmodat(posix.AT.FDCWD, backing, mode, 0) catch {
        return fuseErr(.IO);
    };

    // Enqueue replication for metadata change
    if (rel.len > 0) {
        state.repl_log.enqueue(.put, rel) catch |err| {
            log.err("failed to enqueue chmod replication for {s}: {}", .{ rel, err });
        };
    }

    return 0;
}

fn fuse_chown(path: [*c]const u8, uid: c.uid_t, gid: c.gid_t, fi: ?*c.struct_fuse_file_info) callconv(.c) c_int {
    _ = fi;
    const state = g_state;
    const rel = fuseRelPath(path);

    const backing = backingPath(state.allocator, state, rel) catch return fuseErr(.NOMEM);
    defer state.allocator.free(backing);

    // lchown to not follow symlinks
    const ret = c.lchown(backing.ptr, uid, gid);
    if (ret != 0) {
        const err_val = std.c._errno().*;
        return -@as(c_int, @intCast(err_val));
    }

    // Enqueue replication for metadata change
    if (rel.len > 0) {
        state.repl_log.enqueue(.put, rel) catch |err| {
            log.err("failed to enqueue chown replication for {s}: {}", .{ rel, err });
        };
    }

    return 0;
}

fn fuse_truncate(path: [*c]const u8, size: c.off_t, fi: ?*c.struct_fuse_file_info) callconv(.c) c_int {
    const state = g_state;
    const rel = fuseRelPath(path);

    if (castFi(fi)) |f| {
        const fd: posix.fd_t = decodeFh(f.fh).fd;
        posix.ftruncate(fd, @intCast(size)) catch {
            return fuseErr(.IO);
        };
    } else {
        const backing = backingPath(state.allocator, state, rel) catch return fuseErr(.NOMEM);
        defer state.allocator.free(backing);

        const file = std.fs.openFileAbsolute(backing, .{ .mode = .read_write }) catch {
            return fuseErr(.NOENT);
        };
        defer file.close();
        posix.ftruncate(file.handle, @intCast(size)) catch {
            return fuseErr(.IO);
        };
    }

    // Mark dirty
    if (rel.len > 0) {
        state.path_state.setDirty(rel);
    }

    return 0;
}

fn fuse_utimens(path: [*c]const u8, tv: [*c]const c.struct_timespec, fi: ?*c.struct_fuse_file_info) callconv(.c) c_int {
    _ = fi;
    const state = g_state;
    const rel = fuseRelPath(path);

    const backing = backingPath(state.allocator, state, rel) catch return fuseErr(.NOMEM);
    defer state.allocator.free(backing);

    if (@as(?*const c.struct_timespec, tv)) |_| {
        const times: *const [2]c.struct_timespec = @ptrCast(tv);
        var ts: [2]posix.timespec = .{
            .{ .sec = times[0].tv_sec, .nsec = times[0].tv_nsec },
            .{ .sec = times[1].tv_sec, .nsec = times[1].tv_nsec },
        };
        const ret = posix.system.utimensat(posix.AT.FDCWD, backing.ptr, &ts, 0);
        const signed: c_int = @bitCast(ret);
        if (signed < 0) {
            if (comptime builtin.os.tag == .macos) {
                const err_val = std.c._errno().*;
                return -@as(c_int, @intCast(err_val));
            }
            return signed;
        }
        // Enqueue replication for metadata change
        if (rel.len > 0) {
            state.repl_log.enqueue(.put, rel) catch |err| {
                log.err("failed to enqueue utimens replication for {s}: {}", .{ rel, err });
            };
        }
    }

    return 0;
}

fn fuse_statfs(path: [*c]const u8, stbuf: [*c]c.struct_statvfs) callconv(.c) c_int {
    const state = g_state;
    const rel = fuseRelPath(path);

    const backing = backingPath(state.allocator, state, rel) catch return fuseErr(.NOMEM);
    defer state.allocator.free(backing);

    if (@as(?*c.struct_statvfs, stbuf)) |buf| {
        const ret = c.statvfs(backing.ptr, buf);
        if (ret != 0) {
            return fuseErr(.IO);
        }
    }
    return 0;
}

fn fuse_fallocate(path: [*c]const u8, mode: c_int, offset: c.off_t, length: c.off_t, fi: ?*c.struct_fuse_file_info) callconv(.c) c_int {
    if (comptime builtin.os.tag != .linux) {
        // fallocate is Linux-specific; on macOS return ENOTSUP
        return fuseErr(.OPNOTSUPP);
    }

    const state = g_state;
    const rel = fuseRelPath(path);

    const fd: posix.fd_t = if (castFi(fi)) |f|
        decodeFh(f.fh).fd
    else blk: {
        const backing = backingPath(state.allocator, state, rel) catch return fuseErr(.NOMEM);
        defer state.allocator.free(backing);
        const file = std.fs.openFileAbsolute(backing, .{ .mode = .read_write }) catch |err| {
            return posixErr(err);
        };
        break :blk file.handle;
    };

    const ret = c.fallocate(fd, mode, offset, length);
    // Capture errno immediately, before close() can clobber it.
    const err_val = std.c._errno().*;
    // If we opened the file ourselves (no fi), close it.
    if (castFi(fi) == null) posix.close(fd);

    if (ret != 0) {
        return -@as(c_int, @intCast(err_val));
    }

    // Mark dirty
    if (rel.len > 0) {
        state.path_state.setDirty(rel);
    }

    return 0;
}

fn fuse_access(path: [*c]const u8, mask: c_int) callconv(.c) c_int {
    const state = g_state;
    const rel = fuseRelPath(path);

    if (rel.len > 0 and isHiddenPath(state, rel)) {
        return fuseErr(.NOENT);
    }

    const backing = backingPath(state.allocator, state, rel) catch return fuseErr(.NOMEM);
    defer state.allocator.free(backing);

    const ret = c.access(backing.ptr, mask);
    if (ret != 0) {
        const err_val = std.c._errno().*;
        return -@as(c_int, @intCast(err_val));
    }

    return 0;
}

fn fuse_destroy(_: ?*anyopaque) callconv(.c) void {
    log.info("FUSE destroy — flushing dirty files and stopping workers", .{});
    g_state.flushDirtyFiles();
    g_state.stopWorkers();
}

fn fuse_init(_: ?*c.struct_fuse_conn_info, _: [*c]c.struct_fuse_config) callconv(.c) ?*anyopaque {
    return null;
}

// ============================================================================
// FUSE Operations Table
// ============================================================================
//
// Note on mmap (issue #2): The FUSE high-level API does not expose a separate
// mmap callback, so helmetfs cannot explicitly return ENOTSUP for mmap requests.
// Under the default (non-direct_io) configuration, mmap'd writes go through the
// kernel page cache and may bypass FUSE write() tracking. This is an accepted
// limitation of the high-level API. See DESIGN.md for details.
//

const fuse_ops = std.mem.zeroInit(c.struct_fuse_operations, .{
    .getattr = fuse_getattr,
    .readlink = fuse_readlink,
    .mkdir = fuse_mkdir,
    .unlink = fuse_unlink,
    .rmdir = fuse_rmdir,
    .symlink = fuse_symlink,
    .rename = fuse_rename,
    .link = fuse_link,
    .chmod = fuse_chmod,
    .chown = fuse_chown,
    .truncate = fuse_truncate,
    .open = fuse_open,
    .read = fuse_read,
    .write = fuse_write,
    .statfs = fuse_statfs,
    .release = fuse_release,
    .fsync = fuse_fsync,
    .readdir = fuse_readdir,
    .init = fuse_init,
    .destroy = fuse_destroy,
    .access = fuse_access,
    .create = fuse_create,
    .utimens = fuse_utimens,
    .fallocate = fuse_fallocate,
});

// ============================================================================
// Signal Handling
// ============================================================================

var g_fuse_instance: ?*c.struct_fuse = null;

fn signalHandler(_: c_int) callconv(.c) void {
    g_state.shutdown.store(true, .release);
    if (g_fuse_instance) |fuse| {
        c.fuse_exit(fuse);
    }
}

fn setupSignalHandlers() void {
    const act = posix.Sigaction{
        .handler = .{ .handler = signalHandler },
        .mask = posix.sigemptyset(),
        .flags = 0,
    };
    posix.sigaction(posix.SIG.TERM, &act, null);
    posix.sigaction(posix.SIG.INT, &act, null);
}

// ============================================================================
// CLI
// ============================================================================

const CliArgs = struct {
    command: enum { mount, unmount },
    source: []const u8,
    mountpoint: []const u8,
    replica: ?[]const u8 = null,
    repl_workers: u32 = 4,
    verify_reads: bool = false,
    scrub_time: []const u8 = "01:00",
    metrics_addr: ?[]const u8 = null,
};

fn parseArgs(allocator: std.mem.Allocator) !CliArgs {
    var args_iter = try std.process.argsWithAllocator(allocator);
    defer args_iter.deinit();

    // Skip program name
    _ = args_iter.next();

    const command_str = args_iter.next() orelse {
        printUsage();
        std.process.exit(1);
    };

    if (std.mem.eql(u8, command_str, "unmount")) {
        const mountpoint = args_iter.next() orelse {
            std.debug.print("Usage: helmetfs unmount <mountpoint>\n", .{});
            std.process.exit(1);
        };
        return .{
            .command = .unmount,
            .source = "",
            .mountpoint = mountpoint,
        };
    }

    if (!std.mem.eql(u8, command_str, "mount")) {
        std.debug.print("Unknown command: {s}\n", .{command_str});
        printUsage();
        std.process.exit(1);
    }

    const source = args_iter.next() orelse {
        std.debug.print("Missing source directory\n", .{});
        printUsage();
        std.process.exit(1);
    };

    const mountpoint = args_iter.next() orelse {
        std.debug.print("Missing mountpoint\n", .{});
        printUsage();
        std.process.exit(1);
    };

    var result = CliArgs{
        .command = .mount,
        .source = source,
        .mountpoint = mountpoint,
    };

    while (args_iter.next()) |arg| {
        if (std.mem.eql(u8, arg, "--replica")) {
            result.replica = args_iter.next() orelse {
                std.debug.print("--replica requires a value\n", .{});
                std.process.exit(1);
            };
        } else if (std.mem.eql(u8, arg, "--replication-workers")) {
            const val = args_iter.next() orelse {
                std.debug.print("--replication-workers requires a value\n", .{});
                std.process.exit(1);
            };
            result.repl_workers = std.fmt.parseUnsigned(u32, val, 10) catch {
                std.debug.print("Invalid replication workers count: {s}\n", .{val});
                std.process.exit(1);
            };
        } else if (std.mem.eql(u8, arg, "--verify-reads")) {
            result.verify_reads = true;
        } else if (std.mem.eql(u8, arg, "--scrub-time")) {
            result.scrub_time = args_iter.next() orelse {
                std.debug.print("--scrub-time requires a value (HH:MM)\n", .{});
                std.process.exit(1);
            };
        } else if (std.mem.eql(u8, arg, "--metrics-addr")) {
            result.metrics_addr = args_iter.next() orelse {
                std.debug.print("--metrics-addr requires a value\n", .{});
                std.process.exit(1);
            };
        } else {
            std.debug.print("Unknown option: {s}\n", .{arg});
            printUsage();
            std.process.exit(1);
        }
    }

    if (result.replica == null) {
        std.debug.print("--replica is required\n", .{});
        printUsage();
        std.process.exit(1);
    }

    return result;
}

fn parseScrubTime(time_str: []const u8) !struct { hour: u8, minute: u8 } {
    const colon = std.mem.indexOfScalar(u8, time_str, ':') orelse return error.InvalidFormat;
    const hour = try std.fmt.parseUnsigned(u8, time_str[0..colon], 10);
    const minute = try std.fmt.parseUnsigned(u8, time_str[colon + 1 ..], 10);
    if (hour > 23 or minute > 59) return error.InvalidTime;
    return .{ .hour = hour, .minute = minute };
}

fn printUsage() void {
    std.debug.print(
        \\Usage:
        \\  helmetfs mount <source> <mountpoint> --replica <path> [options]
        \\  helmetfs unmount <mountpoint>
        \\
        \\Options:
        \\  --replica <path>           Replica directory (required)
        \\  --replication-workers <n>  Number of replication workers (default: 4)
        \\  --verify-reads             Enable read-time checksum verification
        \\  --scrub-time HH:MM        Scrub schedule in 24h format (default: 01:00)
        \\  --metrics-addr :PORT      Prometheus metrics endpoint (disabled by default)
        \\
    , .{});
}

// ============================================================================
// Main
// ============================================================================

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    const args = try parseArgs(allocator);

    switch (args.command) {
        .unmount => {
            doUnmount(args.mountpoint);
        },
        .mount => {
            try doMount(allocator, args);
        },
    }
}

fn doUnmount(mountpoint: []const u8) void {
    const is_linux = comptime builtin.os.tag == .linux;
    const cmd: []const u8 = if (is_linux) "fusermount3" else "umount";

    const mountpoint_z = std.heap.page_allocator.dupeZ(u8, mountpoint) catch {
        std.debug.print("Out of memory\n", .{});
        std.process.exit(1);
    };

    var child = if (is_linux)
        std.process.Child.init(&.{ cmd, "-u", mountpoint_z }, std.heap.page_allocator)
    else
        std.process.Child.init(&.{ cmd, mountpoint_z }, std.heap.page_allocator);
    _ = child.spawn() catch |err| {
        std.debug.print("Failed to run {s}: {}\n", .{ cmd, err });
        std.process.exit(1);
    };
    const result = child.wait() catch |err| {
        std.debug.print("Failed to wait for {s}: {}\n", .{ cmd, err });
        std.process.exit(1);
    };
    switch (result) {
        .Exited => |code| {
            if (code != 0) {
                std.debug.print("{s} exited with code {d}\n", .{ cmd, code });
                std.process.exit(1);
            }
        },
        .Signal => |sig| {
            std.debug.print("{s} was killed by signal {d}\n", .{ cmd, sig });
            std.process.exit(1);
        },
        .Stopped => |sig| {
            std.debug.print("{s} was stopped by signal {d}\n", .{ cmd, sig });
            std.process.exit(1);
        },
        .Unknown => |val| {
            std.debug.print("{s} terminated with unknown status {d}\n", .{ cmd, val });
            std.process.exit(1);
        },
    }
}

fn doMount(allocator: std.mem.Allocator, args: CliArgs) !void {
    const scrub = parseScrubTime(args.scrub_time) catch {
        std.debug.print("Invalid scrub time: {s} (expected HH:MM)\n", .{args.scrub_time});
        std.process.exit(1);
    };

    // Resolve source to absolute path
    const source_abs = try std.fs.realpathAlloc(allocator, args.source);
    const replica_abs = try std.fs.realpathAlloc(allocator, args.replica.?);
    const mount_abs = try std.fs.realpathAlloc(allocator, args.mountpoint);

    log.info("helmetfs starting", .{});
    log.info("  backing dir: {s}", .{source_abs});
    log.info("  mountpoint:  {s}", .{mount_abs});
    log.info("  replica dir: {s}", .{replica_abs});
    log.info("  workers:     {d}", .{args.repl_workers});
    log.info("  verify reads: {}", .{args.verify_reads});
    log.info("  scrub time:  {s}", .{args.scrub_time});

    // Initialize global state
    g_state = try FsState.init(
        allocator,
        source_abs,
        replica_abs,
        args.verify_reads,
        scrub.hour,
        scrub.minute,
        args.metrics_addr,
        args.repl_workers,
    );

    // Start background workers
    try g_state.startWorkers();

    // Build FUSE args
    const mount_z = try allocator.dupeZ(u8, mount_abs);

    var fuse_argv = [_][*:0]const u8{
        "helmetfs",
    };
    var fuse_args = c.fuse_args{
        .argc = @intCast(fuse_argv.len),
        .argv = @ptrCast(&fuse_argv),
        .allocated = 0,
    };

    // Create FUSE and run
    const fuse_instance = fuseNew(&fuse_args, &fuse_ops, @sizeOf(c.struct_fuse_operations), null);

    if (fuse_instance == null) {
        log.err("fuse_new failed", .{});
        std.process.exit(1);
    }
    g_fuse_instance = fuse_instance;

    // Setup signal handlers AFTER g_fuse_instance is set, so that if a
    // signal arrives the handler can call fuse_exit on the instance.
    setupSignalHandlers();

    if (c.fuse_mount(fuse_instance, mount_z.ptr) != 0) {
        log.err("fuse_mount failed", .{});
        c.fuse_destroy(fuse_instance);
        std.process.exit(1);
    }

    log.info("mounted, serving requests", .{});

    // Run FUSE main loop (multi-threaded)
    var loop_cfg = c.struct_fuse_loop_config{
        .clone_fd = 0,
        .max_idle_threads = 10,
    };
    const ret = c.fuse_loop_mt(fuse_instance, &loop_cfg);

    log.info("FUSE loop exited with {d}", .{ret});

    // fuse_destroy callback handles flush + stopWorkers; just tear down FUSE.
    c.fuse_unmount(fuse_instance);
    c.fuse_destroy(fuse_instance);

    g_state.deinit();

    log.info("helmetfs shutdown complete", .{});
}

// ============================================================================
// Tests
// ============================================================================

const testing = std.testing;

/// Test harness that creates temp backing/replica dirs and initializes g_state.
const TestHarness = struct {
    allocator: std.mem.Allocator,
    backing_dir: []const u8,
    replica_dir: []const u8,
    state: *FsState,
    tmp_dir_path: []const u8,

    fn init() !TestHarness {
        const allocator = testing.allocator;

        // Create a unique temp directory under /tmp
        const tmp_template = "/tmp/helmetfs-test-XXXXXX";
        var tmp_buf: [tmp_template.len:0]u8 = tmp_template.*;
        const result = c.mkdtemp(&tmp_buf);
        if (result == null) return error.TmpDirFailed;
        const tmp_dir = try allocator.dupe(u8, std.mem.span(result));

        const backing = try std.fs.path.join(allocator, &.{ tmp_dir, "backing" });
        const replica = try std.fs.path.join(allocator, &.{ tmp_dir, "replica" });

        try std.fs.makeDirAbsolute(backing);
        try std.fs.makeDirAbsolute(replica);
        // Create replica/files subdirectory (replication target)
        const replica_files = try std.fs.path.join(allocator, &.{ replica, "files" });
        defer allocator.free(replica_files);
        try std.fs.makeDirAbsolute(replica_files);

        const state = try FsState.init(allocator, backing, replica, false, 1, 0, null, 1);
        g_state = state;

        return .{
            .allocator = allocator,
            .backing_dir = backing,
            .replica_dir = replica,
            .state = state,
            .tmp_dir_path = tmp_dir,
        };
    }

    fn deinit(self: *TestHarness) void {
        // Recursively remove temp dir
        std.fs.deleteTreeAbsolute(self.tmp_dir_path) catch {};
        self.allocator.free(self.backing_dir);
        self.allocator.free(self.replica_dir);
        self.allocator.free(self.tmp_dir_path);
        self.state.deinit();
    }

    /// Create a file in the backing directory with the given contents.
    fn createBackingFile(self: *TestHarness, rel_path: []const u8, contents: []const u8) !void {
        const full = try std.fs.path.join(self.allocator, &.{ self.backing_dir, rel_path });
        defer self.allocator.free(full);
        try ensureParentDir(full);
        const file = try std.fs.createFileAbsolute(full, .{});
        defer file.close();
        try file.writeAll(contents);
    }

    /// Read a file from the backing directory.
    fn readBackingFile(self: *TestHarness, rel_path: []const u8) ![]const u8 {
        const full = try std.fs.path.join(self.allocator, &.{ self.backing_dir, rel_path });
        defer self.allocator.free(full);
        const file = try std.fs.openFileAbsolute(full, .{});
        defer file.close();
        return try file.readToEndAlloc(self.allocator, 1024 * 1024);
    }

    /// Create a file in the replica/files directory with the given contents.
    fn createReplicaFile(self: *TestHarness, rel_path: []const u8, contents: []const u8) !void {
        const full = try std.fs.path.join(self.allocator, &.{ self.replica_dir, "files", rel_path });
        defer self.allocator.free(full);
        try ensureParentDir(full);
        const file = try std.fs.createFileAbsolute(full, .{});
        defer file.close();
        try file.writeAll(contents);
    }

    /// Check if a file exists in the replica/files directory.
    fn replicaFileExists(self: *TestHarness, rel_path: []const u8) bool {
        const full = std.fs.path.join(self.allocator, &.{ self.replica_dir, "files", rel_path }) catch return false;
        defer self.allocator.free(full);
        std.fs.accessAbsolute(full, .{}) catch return false;
        return true;
    }
};

// ---------- formatLogEntry / parseLine round-trip ----------

test "formatLogEntry produces valid CRC-protected line" {
    const allocator = testing.allocator;
    const line = try formatLogEntry(allocator, .put, "foo/bar.txt", null);
    defer allocator.free(line);

    // Should end with newline
    try testing.expect(line[line.len - 1] == '\n');

    // Should contain " put foo/bar.txt"
    try testing.expect(std.mem.indexOf(u8, line, " put foo/bar.txt") != null);
}

test "formatLogEntry/parseLine round-trip for put" {
    var h = try TestHarness.init();
    defer h.deinit();

    const line = try formatLogEntry(h.allocator, .put, "hello/world.txt", null);
    defer h.allocator.free(line);

    // Strip trailing newline for parseLine
    const trimmed = std.mem.trimRight(u8, line, "\n");

    // parseLine should succeed without error
    const count_before = h.state.repl_log.entries.items.len;
    try h.state.repl_log.parseLine(trimmed);
    const count_after = h.state.repl_log.entries.items.len;
    try testing.expectEqual(count_before + 1, count_after);

    const entry = h.state.repl_log.entries.items[count_after - 1];
    try testing.expectEqual(ReplOp.put, entry.op);
    try testing.expectEqualStrings("hello/world.txt", entry.path);
}

test "formatLogEntry/parseLine round-trip for delete" {
    var h = try TestHarness.init();
    defer h.deinit();

    const line = try formatLogEntry(h.allocator, .delete, "gone.txt", null);
    defer h.allocator.free(line);

    const trimmed = std.mem.trimRight(u8, line, "\n");
    try h.state.repl_log.parseLine(trimmed);

    const entry = h.state.repl_log.entries.getLast();
    try testing.expectEqual(ReplOp.delete, entry.op);
    try testing.expectEqualStrings("gone.txt", entry.path);
}

test "parseLine rejects corrupted CRC" {
    var h = try TestHarness.init();
    defer h.deinit();

    const line = try formatLogEntry(h.allocator, .put, "test.txt", null);
    defer h.allocator.free(line);
    const trimmed = std.mem.trimRight(u8, line, "\n");

    // Corrupt the CRC by changing the first character
    var corrupted = try h.allocator.dupe(u8, trimmed);
    defer h.allocator.free(corrupted);
    corrupted[0] = if (corrupted[0] == 'a') 'b' else 'a';

    const result = h.state.repl_log.parseLine(corrupted);
    try testing.expectError(error.CrcMismatch, result);
}

// ---------- PathStateMap ----------

fn deinitPathStateMap(psm: *PathStateMap) void {
    var it = psm.map.iterator();
    while (it.next()) |entry| {
        psm.allocator.free(entry.key_ptr.*);
    }
    psm.map.deinit();
}

test "PathStateMap: setDirty and isDirty" {
    var psm = PathStateMap.init(testing.allocator);
    defer deinitPathStateMap(&psm);

    try testing.expect(!psm.isDirty("foo.txt"));
    psm.setDirty("foo.txt");
    try testing.expect(psm.isDirty("foo.txt"));
}

test "PathStateMap: clearDirty" {
    var psm = PathStateMap.init(testing.allocator);
    defer deinitPathStateMap(&psm);

    psm.setDirty("bar.txt");
    try testing.expect(psm.isDirty("bar.txt"));
    psm.clearDirty("bar.txt");
    try testing.expect(!psm.isDirty("bar.txt"));
}

test "PathStateMap: incWriteRef and hasWriteRef" {
    var psm = PathStateMap.init(testing.allocator);
    defer deinitPathStateMap(&psm);

    try testing.expect(!psm.hasWriteRef("a.txt"));
    psm.incWriteRef("a.txt");
    try testing.expect(psm.hasWriteRef("a.txt"));
    psm.incWriteRef("a.txt");
    try testing.expect(psm.hasWriteRef("a.txt"));
}

test "PathStateMap: decWriteRef" {
    var psm = PathStateMap.init(testing.allocator);
    defer deinitPathStateMap(&psm);

    psm.incWriteRef("b.txt");
    psm.incWriteRef("b.txt");
    psm.decWriteRef("b.txt");
    try testing.expect(psm.hasWriteRef("b.txt")); // refcount=1
    psm.decWriteRef("b.txt");
    try testing.expect(!psm.hasWriteRef("b.txt")); // refcount=0
}

test "PathStateMap: dirty and writeRef are independent" {
    var psm = PathStateMap.init(testing.allocator);
    defer deinitPathStateMap(&psm);

    psm.setDirty("c.txt");
    psm.incWriteRef("c.txt");
    try testing.expect(psm.isDirty("c.txt"));
    try testing.expect(psm.hasWriteRef("c.txt"));

    psm.clearDirty("c.txt");
    try testing.expect(!psm.isDirty("c.txt"));
    try testing.expect(psm.hasWriteRef("c.txt"));
}

// ---------- isHiddenPath ----------

test "isHiddenPath: .helmetfs directory is hidden" {
    var h = try TestHarness.init();
    defer h.deinit();

    try testing.expect(isHiddenPath(h.state, ".helmetfs"));
    try testing.expect(isHiddenPath(h.state, ".helmetfs/repl.log"));
}

test "isHiddenPath: .sum sidecar hidden when data file exists" {
    var h = try TestHarness.init();
    defer h.deinit();

    // Create data file
    try h.createBackingFile("data.txt", "hello");

    // .sum sidecar should be hidden
    try testing.expect(isHiddenPath(h.state, "data.txt.sum"));
}

test "isHiddenPath: .sum not hidden when data file missing" {
    var h = try TestHarness.init();
    defer h.deinit();

    // No data file exists — .sum should be visible (not hidden)
    try testing.expect(!isHiddenPath(h.state, "nodata.txt.sum"));
}

test "isHiddenPath: regular file not hidden" {
    var h = try TestHarness.init();
    defer h.deinit();

    try testing.expect(!isHiddenPath(h.state, "readme.md"));
    try testing.expect(!isHiddenPath(h.state, "subdir/file.txt"));
}

// ---------- parseScrubTime / parseMetricsAddr ----------

test "parseScrubTime: valid inputs" {
    const r1 = try parseScrubTime("01:00");
    try testing.expectEqual(@as(u8, 1), r1.hour);
    try testing.expectEqual(@as(u8, 0), r1.minute);

    const r2 = try parseScrubTime("23:59");
    try testing.expectEqual(@as(u8, 23), r2.hour);
    try testing.expectEqual(@as(u8, 59), r2.minute);

    const r3 = try parseScrubTime("00:00");
    try testing.expectEqual(@as(u8, 0), r3.hour);
    try testing.expectEqual(@as(u8, 0), r3.minute);
}

test "parseScrubTime: invalid inputs" {
    try testing.expectError(error.InvalidTime, parseScrubTime("24:00"));
    try testing.expectError(error.InvalidTime, parseScrubTime("12:60"));
    try testing.expectError(error.InvalidFormat, parseScrubTime("1200"));
}

test "parseMetricsAddr: valid inputs" {
    try testing.expectEqual(@as(u16, 9090), try parseMetricsAddr(":9090"));
    try testing.expectEqual(@as(u16, 8080), try parseMetricsAddr("8080"));
    try testing.expectEqual(@as(u16, 1), try parseMetricsAddr(":1"));
}

test "parseMetricsAddr: invalid inputs" {
    try testing.expectError(error.InvalidFormat, parseMetricsAddr(""));
    try testing.expectError(error.InvalidCharacter, parseMetricsAddr(":abc"));
}

// ---------- computeBlake3 / writeSumFile / readSumFile ----------

test "computeBlake3 produces consistent 64-char hex digest" {
    var h = try TestHarness.init();
    defer h.deinit();

    try h.createBackingFile("hashme.txt", "hello world\n");

    const full = try std.fs.path.join(h.allocator, &.{ h.backing_dir, "hashme.txt" });
    defer h.allocator.free(full);

    const hex1 = try computeBlake3(full);
    const hex2 = try computeBlake3(full);
    try testing.expectEqual(@as(usize, 64), hex1.len);
    try testing.expectEqualSlices(u8, &hex1, &hex2);
}

test "writeSumFile / readSumFile round-trip" {
    var h = try TestHarness.init();
    defer h.deinit();

    const sum_path = try std.fs.path.join(h.allocator, &.{ h.backing_dir, "test.txt.sum" });
    defer h.allocator.free(sum_path);

    const hex = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
    try writeSumFile(sum_path, hex);

    const read_hex = try readSumFile(h.allocator, sum_path);
    defer h.allocator.free(read_hex);

    try testing.expectEqualStrings(hex, read_hex);
}

// ---------- copyFileWithSync / ensureParentDir ----------

test "copyFileWithSync copies file correctly" {
    var h = try TestHarness.init();
    defer h.deinit();

    try h.createBackingFile("original.txt", "file contents here");

    const src = try std.fs.path.join(h.allocator, &.{ h.backing_dir, "original.txt" });
    defer h.allocator.free(src);
    const dst = try std.fs.path.join(h.allocator, &.{ h.replica_dir, "copy.txt" });
    defer h.allocator.free(dst);

    try copyFileWithSync(src, dst);

    const file = try std.fs.openFileAbsolute(dst, .{});
    defer file.close();
    const contents = try file.readToEndAlloc(h.allocator, 1024);
    defer h.allocator.free(contents);
    try testing.expectEqualStrings("file contents here", contents);
}

test "ensureParentDir creates nested directories" {
    var h = try TestHarness.init();
    defer h.deinit();

    const deep_file = try std.fs.path.join(h.allocator, &.{ h.backing_dir, "a", "b", "c", "file.txt" });
    defer h.allocator.free(deep_file);

    try ensureParentDir(deep_file);

    // Parent directory should now exist
    const parent = std.fs.path.dirname(deep_file).?;
    var dir = try std.fs.openDirAbsolute(parent, .{});
    dir.close();
}

// ---------- checksumAndEnqueue ----------

test "checksumAndEnqueue creates .sum and enqueues put" {
    var h = try TestHarness.init();
    defer h.deinit();

    try h.createBackingFile("doc.txt", "important data");

    const entries_before = h.state.repl_log.entries.items.len;
    try checksumAndEnqueue(h.state, "doc.txt");

    // .sum file should exist
    const sum_path = try std.fs.path.join(h.allocator, &.{ h.backing_dir, "doc.txt.sum" });
    defer h.allocator.free(sum_path);
    const hex = try readSumFile(h.allocator, sum_path);
    defer h.allocator.free(hex);
    try testing.expectEqual(@as(usize, 64), hex.len);

    // Should have enqueued a put entry
    try testing.expect(h.state.repl_log.entries.items.len > entries_before);
    const last = h.state.repl_log.entries.getLast();
    try testing.expectEqual(ReplOp.put, last.op);
    try testing.expectEqualStrings("doc.txt", last.path);
}

// ---------- ReplLog enqueue / coalescing ----------

test "ReplLog.enqueue adds entries" {
    var h = try TestHarness.init();
    defer h.deinit();

    const before = h.state.repl_log.entries.items.len;
    try h.state.repl_log.enqueue(.put, "file1.txt");
    try h.state.repl_log.enqueue(.delete, "file2.txt");
    try testing.expectEqual(before + 2, h.state.repl_log.entries.items.len);
}

test "ReplLog.enqueuePair adds two entries atomically" {
    var h = try TestHarness.init();
    defer h.deinit();

    const before = h.state.repl_log.entries.items.len;
    h.state.repl_log.enqueuePair(.delete, "old.txt", .put, "new.txt");
    try testing.expectEqual(before + 2, h.state.repl_log.entries.items.len);
}

test "ReplLog.hasPendingPut detects pending puts" {
    var h = try TestHarness.init();
    defer h.deinit();

    try testing.expect(!h.state.repl_log.hasPendingPut("x.txt"));
    try h.state.repl_log.enqueue(.put, "x.txt");
    try testing.expect(h.state.repl_log.hasPendingPut("x.txt"));
}

// ---------- replicatePut / replicateDelete ----------

test "replicatePut copies file and .sum to replica" {
    var h = try TestHarness.init();
    defer h.deinit();

    // Create backing file and its .sum
    try h.createBackingFile("replme.txt", "replicate this");
    try checksumAndEnqueue(h.state, "replme.txt");

    // Replicate
    try replicatePut(h.state, "replme.txt");

    // Verify replica file exists and has correct contents
    const replica_path = try std.fs.path.join(h.allocator, &.{ h.replica_dir, "files", "replme.txt" });
    defer h.allocator.free(replica_path);
    const file = try std.fs.openFileAbsolute(replica_path, .{});
    defer file.close();
    const contents = try file.readToEndAlloc(h.allocator, 1024);
    defer h.allocator.free(contents);
    try testing.expectEqualStrings("replicate this", contents);

    // Verify replica .sum exists
    const replica_sum = try std.fmt.allocPrint(h.allocator, "{s}.sum", .{replica_path});
    defer h.allocator.free(replica_sum);
    std.fs.accessAbsolute(replica_sum, .{}) catch {
        return error.ReplicaSumMissing;
    };
}

test "replicatePut handles subdirectories" {
    var h = try TestHarness.init();
    defer h.deinit();

    try h.createBackingFile("sub/dir/file.txt", "nested content");
    try checksumAndEnqueue(h.state, "sub/dir/file.txt");
    try replicatePut(h.state, "sub/dir/file.txt");

    try testing.expect(h.replicaFileExists("sub/dir/file.txt"));
}

test "replicateDelete removes file and .sum from replica" {
    var h = try TestHarness.init();
    defer h.deinit();

    // Create replica file and .sum
    try h.createReplicaFile("todelete.txt", "gone soon");
    const replica_sum = try std.fs.path.join(h.allocator, &.{ h.replica_dir, "files", "todelete.txt.sum" });
    defer h.allocator.free(replica_sum);
    try writeSumFile(replica_sum, "0" ** 64);

    try testing.expect(h.replicaFileExists("todelete.txt"));

    try replicateDelete(h.state, "todelete.txt");

    try testing.expect(!h.replicaFileExists("todelete.txt"));
}

test "replicateDelete is idempotent" {
    var h = try TestHarness.init();
    defer h.deinit();

    // Deleting a non-existent file should not error
    try replicateDelete(h.state, "nonexistent.txt");
}

// ---------- scrubFile ----------

test "scrubFile adopts untracked file" {
    var h = try TestHarness.init();
    defer h.deinit();

    // Create a file with no .sum sidecar
    try h.createBackingFile("untracked.txt", "new file");

    var corruptions: u64 = 0;
    var repairs: u64 = 0;
    try scrubFile(h.state, "untracked.txt", &corruptions, &repairs);

    // Should have created a .sum file
    const sum_path = try std.fs.path.join(h.allocator, &.{ h.backing_dir, "untracked.txt.sum" });
    defer h.allocator.free(sum_path);
    const hex = try readSumFile(h.allocator, sum_path);
    defer h.allocator.free(hex);
    try testing.expectEqual(@as(usize, 64), hex.len);

    // No corruption (it was just adopted)
    try testing.expectEqual(@as(u64, 0), corruptions);
}

test "scrubFile detects corruption and repairs from replica" {
    var h = try TestHarness.init();
    defer h.deinit();

    // Create the original file and its checksum
    try h.createBackingFile("important.txt", "correct data");
    try checksumAndEnqueue(h.state, "important.txt");

    // Replicate to replica so we have a good copy
    try replicatePut(h.state, "important.txt");
    h.state.repl_log.markCompletedByPath("important.txt");

    // Now corrupt the backing file
    try h.createBackingFile("important.txt", "CORRUPTED DATA");

    var corruptions: u64 = 0;
    var repairs: u64 = 0;
    try scrubFile(h.state, "important.txt", &corruptions, &repairs);

    // Should detect corruption and repair
    try testing.expectEqual(@as(u64, 1), corruptions);
    try testing.expectEqual(@as(u64, 1), repairs);

    // File should be restored to original content
    const restored = try h.readBackingFile("important.txt");
    defer h.allocator.free(restored);
    try testing.expectEqualStrings("correct data", restored);
}

test "scrubFile passes clean file" {
    var h = try TestHarness.init();
    defer h.deinit();

    try h.createBackingFile("clean.txt", "all good");
    try checksumAndEnqueue(h.state, "clean.txt");

    var corruptions: u64 = 0;
    var repairs: u64 = 0;
    try scrubFile(h.state, "clean.txt", &corruptions, &repairs);

    try testing.expectEqual(@as(u64, 0), corruptions);
    try testing.expectEqual(@as(u64, 0), repairs);
}

// ---------- nsUntilNextScrub ----------

test "nsUntilNextScrub returns positive value" {
    const ns = nsUntilNextScrub(3, 0);
    try testing.expect(ns > 0);
    // Should be at most ~24 hours in nanoseconds
    try testing.expect(ns <= 86400 * 1_000_000_000);
}

test "nsUntilNextScrub returns at most 24 hours" {
    // Test several different target times
    for ([_]u8{ 0, 6, 12, 18, 23 }) |hour| {
        const ns = nsUntilNextScrub(hour, 0);
        try testing.expect(ns > 0);
        try testing.expect(ns <= 86400 * 1_000_000_000);
    }
}

// ---------- ReplLog disk persistence ----------

test "ReplLog persists to disk and reloads" {
    var h = try TestHarness.init();
    defer h.deinit();

    // Enqueue some entries
    try h.state.repl_log.enqueue(.put, "persist1.txt");
    try h.state.repl_log.enqueue(.delete, "persist2.txt");

    // Create a fresh ReplLog from the same backing dir — it should load entries
    var log2 = try ReplLog.init(h.allocator, h.backing_dir);
    defer {
        for (log2.entries.items) |entry| {
            h.allocator.free(entry.path);
        }
        log2.entries.deinit(h.allocator);
    }

    try testing.expectEqual(@as(usize, 2), log2.entries.items.len);
    try testing.expectEqual(ReplOp.put, log2.entries.items[0].op);
    try testing.expectEqualStrings("persist1.txt", log2.entries.items[0].path);
    try testing.expectEqual(ReplOp.delete, log2.entries.items[1].op);
    try testing.expectEqualStrings("persist2.txt", log2.entries.items[1].path);
}

// ---------- formatMetrics ----------

test "formatMetrics produces Prometheus-format output" {
    var h = try TestHarness.init();
    defer h.deinit();

    const body = try formatMetrics(h.state);
    defer h.allocator.free(body);

    try testing.expect(std.mem.indexOf(u8, body, "helmetfs_replication_pending") != null);
    try testing.expect(std.mem.indexOf(u8, body, "helmetfs_scrub_files_checked_total") != null);
    try testing.expect(std.mem.indexOf(u8, body, "# HELP") != null);
    try testing.expect(std.mem.indexOf(u8, body, "# TYPE") != null);
}

// ---------- ReplLog put coalescing ----------

test "dequeueNext coalesces duplicate puts, returning only the latest" {
    var h = try TestHarness.init();
    defer h.deinit();

    // Manually add entries without disk I/O (directly into the list)
    const p1 = try h.allocator.dupe(u8, "dup.txt");
    const p2 = try h.allocator.dupe(u8, "dup.txt");
    const p3 = try h.allocator.dupe(u8, "unique.txt");
    try h.state.repl_log.entries.append(h.allocator, .{ .id = 0, .op = .put, .path = p1 });
    try h.state.repl_log.entries.append(h.allocator, .{ .id = 1, .op = .put, .path = p3 });
    try h.state.repl_log.entries.append(h.allocator, .{ .id = 2, .op = .put, .path = p2 });
    h.state.repl_log.next_id = 3;

    // First dequeue should skip stale dup.txt (index 0) and return unique.txt (index 1)
    const first = h.state.repl_log.dequeueNext();
    try testing.expect(first != null);
    try testing.expectEqualStrings("unique.txt", first.?.path);

    // The stale put at index 0 should have been marked completed by coalescing
    try testing.expect(h.state.repl_log.entries.items[0].completed);

    // Mark unique.txt as completed (may trigger truncation, compacting entries)
    h.state.repl_log.markCompleted(first.?.id);

    // Next dequeue should return the latest dup.txt (the only remaining entry)
    const second = h.state.repl_log.dequeueNext();
    try testing.expect(second != null);
    try testing.expectEqualStrings("dup.txt", second.?.path);
    h.state.repl_log.markCompleted(second.?.id);

    // All consumed — set shutdown so dequeueNext returns null instead of blocking
    h.state.shutdown.store(true, .release);
    h.state.repl_log.cond.broadcast();
    const third = h.state.repl_log.dequeueNext();
    try testing.expect(third == null);
}

// ---------- ReplLog markCompleted + maybeTruncate ----------

test "markCompleted triggers truncation and removes completed entries" {
    var h = try TestHarness.init();
    defer h.deinit();

    // Add 4 entries
    try h.state.repl_log.enqueue(.put, "a.txt");
    try h.state.repl_log.enqueue(.put, "b.txt");
    try h.state.repl_log.enqueue(.delete, "c.txt");
    try h.state.repl_log.enqueue(.put, "d.txt");

    try testing.expectEqual(@as(usize, 4), h.state.repl_log.entries.items.len);

    // Set last_truncate_time to now so only ratio-based truncation applies
    h.state.repl_log.last_truncate_time = std.time.timestamp();

    // Mark 3 of 4 completed (75% > 50% — triggers ratio-based truncation)
    h.state.repl_log.markCompleted(0);
    // After first: completed_count=1, total=4, 1*2=2 > 4? No. No truncation yet.
    h.state.repl_log.markCompleted(1);
    // After second: completed_count=2, total=4, 2*2=4 > 4? No. No truncation yet.
    h.state.repl_log.markCompleted(2);
    // After third: completed_count=3, total=4, 3*2=6 > 4? Yes. Truncation fires.

    // After truncation, only the uncompleted entry (d.txt) should remain
    try testing.expectEqual(@as(usize, 1), h.state.repl_log.entries.items.len);
    try testing.expectEqualStrings("d.txt", h.state.repl_log.entries.items[0].path);
    try testing.expectEqual(@as(usize, 0), h.state.repl_log.completed_count);
}

// ---------- PathStateMap.shouldVerify ----------

test "PathStateMap.shouldVerify throttles within 60s window" {
    var psm = PathStateMap.init(testing.allocator);
    defer deinitPathStateMap(&psm);

    // First call should return true (no prior verification)
    try testing.expect(psm.shouldVerify("throttle.txt"));

    // Immediately calling again should return false (within 60s)
    try testing.expect(!psm.shouldVerify("throttle.txt"));

    // Different path should still allow verification
    try testing.expect(psm.shouldVerify("other.txt"));
}

// ---------- scrubFile: replica also corrupt ----------

test "scrubFile does not repair when replica is also corrupt" {
    var h = try TestHarness.init();
    defer h.deinit();

    // Create original file and checksum
    try h.createBackingFile("both-bad.txt", "original data");
    try checksumAndEnqueue(h.state, "both-bad.txt");

    // Replicate to get a good copy in replica
    try replicatePut(h.state, "both-bad.txt");

    // Now corrupt BOTH the backing file and the replica file
    try h.createBackingFile("both-bad.txt", "CORRUPTED BACKING");
    try h.createReplicaFile("both-bad.txt", "CORRUPTED REPLICA");

    var corruptions: u64 = 0;
    var repairs: u64 = 0;
    try scrubFile(h.state, "both-bad.txt", &corruptions, &repairs);

    // Should detect corruption but NOT repair (replica checksum won't match)
    try testing.expectEqual(@as(u64, 1), corruptions);
    try testing.expectEqual(@as(u64, 0), repairs);

    // File should still be corrupted (not overwritten with bad replica)
    const contents = try h.readBackingFile("both-bad.txt");
    defer h.allocator.free(contents);
    try testing.expectEqualStrings("CORRUPTED BACKING", contents);
}

// ---------- replicatePut: source deleted before replication ----------

test "replicatePut silently handles source file deleted before replication" {
    var h = try TestHarness.init();
    defer h.deinit();

    // replicatePut for a non-existent file should not error (FileNotFound is handled)
    try replicatePut(h.state, "ghost.txt");

    // Replica should not have the file
    try testing.expect(!h.replicaFileExists("ghost.txt"));
}

// ---------- checksumAndEnqueue clears dirty flag ----------

test "checksumAndEnqueue clears dirty flag on success" {
    var h = try TestHarness.init();
    defer h.deinit();

    try h.createBackingFile("dirty.txt", "some content");

    // Mark dirty
    h.state.path_state.setDirty("dirty.txt");
    try testing.expect(h.state.path_state.isDirty("dirty.txt"));

    // checksumAndEnqueue should clear it
    try checksumAndEnqueue(h.state, "dirty.txt");
    try testing.expect(!h.state.path_state.isDirty("dirty.txt"));
}

// ---------- End-to-end: write + checksum + replicate + corrupt + scrub repair ----------

test "end-to-end: file goes through checksum, replication, corruption, and scrub repair" {
    var h = try TestHarness.init();
    defer h.deinit();

    // Step 1: "Write" a file (simulate what fuse_write + fuse_release would do)
    try h.createBackingFile("e2e.txt", "precious data");
    h.state.path_state.setDirty("e2e.txt");
    try testing.expect(h.state.path_state.isDirty("e2e.txt"));

    // Step 2: Checksum and enqueue (what release/fsync does)
    try checksumAndEnqueue(h.state, "e2e.txt");
    try testing.expect(!h.state.path_state.isDirty("e2e.txt"));

    // Verify .sum sidecar was created
    const sum_path = try std.fs.path.join(h.allocator, &.{ h.backing_dir, "e2e.txt.sum" });
    defer h.allocator.free(sum_path);
    const original_sum = try readSumFile(h.allocator, sum_path);
    defer h.allocator.free(original_sum);
    try testing.expectEqual(@as(usize, 64), original_sum.len);

    // Step 3: Replicate to replica
    try replicatePut(h.state, "e2e.txt");
    h.state.repl_log.markCompletedByPath("e2e.txt");
    try testing.expect(h.replicaFileExists("e2e.txt"));

    // Verify replica contents match
    const replica_path = try std.fs.path.join(h.allocator, &.{ h.replica_dir, "files", "e2e.txt" });
    defer h.allocator.free(replica_path);
    const replica_file = try std.fs.openFileAbsolute(replica_path, .{});
    defer replica_file.close();
    const replica_contents = try replica_file.readToEndAlloc(h.allocator, 1024);
    defer h.allocator.free(replica_contents);
    try testing.expectEqualStrings("precious data", replica_contents);

    // Step 4: Simulate corruption
    try h.createBackingFile("e2e.txt", "CORRUPTED DATA!!");

    // Step 5: Scrub detects and repairs
    var corruptions: u64 = 0;
    var repairs: u64 = 0;
    try scrubFile(h.state, "e2e.txt", &corruptions, &repairs);
    try testing.expectEqual(@as(u64, 1), corruptions);
    try testing.expectEqual(@as(u64, 1), repairs);

    // Step 6: Verify file was restored
    const restored = try h.readBackingFile("e2e.txt");
    defer h.allocator.free(restored);
    try testing.expectEqualStrings("precious data", restored);

    // Verify .sum was also updated to match the repaired file
    const repaired_sum = try readSumFile(h.allocator, sum_path);
    defer h.allocator.free(repaired_sum);
    try testing.expectEqualStrings(original_sum, repaired_sum);
}

// ---------- ReplLog loadFromDisk: corrupted lines ----------

test "ReplLog loadFromDisk gracefully skips corrupted lines" {
    var h = try TestHarness.init();
    defer h.deinit();

    // Write a valid entry and a corrupted entry directly to the log file
    const log_path = try std.fs.path.join(h.allocator, &.{ h.backing_dir, ".helmetfs", "repl.log" });
    defer h.allocator.free(log_path);

    const valid_line = try formatLogEntry(h.allocator, .put, "good.txt", null);
    defer h.allocator.free(valid_line);

    {
        const file = try std.fs.createFileAbsolute(log_path, .{ .truncate = true });
        defer file.close();
        try file.writeAll(valid_line);
        try file.writeAll("deadbeef put corrupted.txt\n"); // bad CRC
        try file.sync();
    }

    // Load from disk into a fresh ReplLog
    var log2 = try ReplLog.init(h.allocator, h.backing_dir);
    defer {
        for (log2.entries.items) |entry| {
            h.allocator.free(entry.path);
        }
        log2.entries.deinit(h.allocator);
    }

    // Only the valid entry should have been loaded
    try testing.expectEqual(@as(usize, 1), log2.entries.items.len);
    try testing.expectEqualStrings("good.txt", log2.entries.items[0].path);
}

// ---------- flushDirtyFiles ----------

test "flushDirtyFiles processes all dirty paths" {
    var h = try TestHarness.init();
    defer h.deinit();

    // Create files and mark them dirty
    try h.createBackingFile("flush1.txt", "data one");
    try h.createBackingFile("flush2.txt", "data two");

    h.state.path_state.setDirty("flush1.txt");
    h.state.path_state.setDirty("flush2.txt");

    try testing.expect(h.state.path_state.isDirty("flush1.txt"));
    try testing.expect(h.state.path_state.isDirty("flush2.txt"));

    const entries_before = h.state.repl_log.entries.items.len;

    // Flush dirty files (simulates what FUSE destroy does)
    h.state.flushDirtyFiles();

    // Both files should now have .sum sidecars
    const sum1 = try std.fs.path.join(h.allocator, &.{ h.backing_dir, "flush1.txt.sum" });
    defer h.allocator.free(sum1);
    const sum2 = try std.fs.path.join(h.allocator, &.{ h.backing_dir, "flush2.txt.sum" });
    defer h.allocator.free(sum2);
    std.fs.accessAbsolute(sum1, .{}) catch return error.Sum1Missing;
    std.fs.accessAbsolute(sum2, .{}) catch return error.Sum2Missing;

    // Both should have been enqueued for replication
    try testing.expect(h.state.repl_log.entries.items.len >= entries_before + 2);

    // Dirty flags should be cleared
    try testing.expect(!h.state.path_state.isDirty("flush1.txt"));
    try testing.expect(!h.state.path_state.isDirty("flush2.txt"));
}

// ---------- formatMetrics reflects actual values ----------

test "formatMetrics reflects actual metric values" {
    var h = try TestHarness.init();
    defer h.deinit();

    // Set specific metric values
    h.state.metrics.repl_completed.store(42, .release);
    h.state.metrics.repl_errors.store(3, .release);
    h.state.metrics.scrub_corruptions.store(7, .release);
    h.state.metrics.scrub_repairs.store(5, .release);

    const body = try formatMetrics(h.state);
    defer h.allocator.free(body);

    // Verify the specific values appear in the output
    try testing.expect(std.mem.indexOf(u8, body, "helmetfs_replication_completed_total 42") != null);
    try testing.expect(std.mem.indexOf(u8, body, "helmetfs_replication_errors_total 3") != null);
    try testing.expect(std.mem.indexOf(u8, body, "helmetfs_scrub_corruptions_found_total 7") != null);
    try testing.expect(std.mem.indexOf(u8, body, "helmetfs_scrub_repairs_total 5") != null);
}

// ---------- ReplLog: delete entries are not coalesced ----------

test "dequeueNext does not coalesce delete entries" {
    var h = try TestHarness.init();
    defer h.deinit();

    // Two deletes for the same path should both be processed
    const p1 = try h.allocator.dupe(u8, "del.txt");
    const p2 = try h.allocator.dupe(u8, "del.txt");
    try h.state.repl_log.entries.append(h.allocator, .{ .id = 0, .op = .delete, .path = p1 });
    try h.state.repl_log.entries.append(h.allocator, .{ .id = 1, .op = .delete, .path = p2 });
    h.state.repl_log.next_id = 2;

    const first = h.state.repl_log.dequeueNext();
    try testing.expect(first != null);
    try testing.expectEqual(@as(u64, 0), first.?.id);
    try testing.expectEqual(ReplOp.delete, first.?.op);

    h.state.repl_log.markCompleted(first.?.id);

    const second = h.state.repl_log.dequeueNext();
    try testing.expect(second != null);
    try testing.expectEqual(@as(u64, 1), second.?.id);
    try testing.expectEqual(ReplOp.delete, second.?.op);

    h.state.repl_log.markCompleted(second.?.id);

    h.state.shutdown.store(true, .release);
    h.state.repl_log.cond.broadcast();
    try testing.expect(h.state.repl_log.dequeueNext() == null);
}

// ---------- ReplLog atomic rewrite preserves pending entries on disk ----------

test "ReplLog atomic rewrite preserves only pending entries on disk" {
    var h = try TestHarness.init();
    defer h.deinit();

    try h.state.repl_log.enqueue(.put, "keep.txt");
    try h.state.repl_log.enqueue(.delete, "remove.txt");
    try h.state.repl_log.enqueue(.put, "also-keep.txt");

    // Mark the middle entry as completed and force truncation
    h.state.repl_log.last_truncate_time = 0;
    h.state.repl_log.markCompleted(1);

    // Reload from disk to verify persistence
    var log2 = try ReplLog.init(h.allocator, h.backing_dir);
    defer {
        for (log2.entries.items) |entry| {
            h.allocator.free(entry.path);
        }
        log2.entries.deinit(h.allocator);
    }

    try testing.expectEqual(@as(usize, 2), log2.entries.items.len);
    try testing.expectEqualStrings("keep.txt", log2.entries.items[0].path);
    try testing.expectEqualStrings("also-keep.txt", log2.entries.items[1].path);
}

// ---------- End-to-end: rename triggers delete+put pair ----------

test "end-to-end: rename enqueues delete+put pair and replication works" {
    var h = try TestHarness.init();
    defer h.deinit();

    // Create original file, checksum, and replicate
    try h.createBackingFile("old-name.txt", "rename me");
    try checksumAndEnqueue(h.state, "old-name.txt");
    try replicatePut(h.state, "old-name.txt");
    try testing.expect(h.replicaFileExists("old-name.txt"));

    // Simulate rename: move file in backing, move .sum, enqueue pair
    const old_backing = try std.fs.path.join(h.allocator, &.{ h.backing_dir, "old-name.txt" });
    defer h.allocator.free(old_backing);
    const new_backing = try std.fs.path.join(h.allocator, &.{ h.backing_dir, "new-name.txt" });
    defer h.allocator.free(new_backing);
    try std.fs.renameAbsolute(old_backing, new_backing);

    const old_sum = try std.fmt.allocPrint(h.allocator, "{s}.sum", .{old_backing});
    defer h.allocator.free(old_sum);
    const new_sum = try std.fmt.allocPrint(h.allocator, "{s}.sum", .{new_backing});
    defer h.allocator.free(new_sum);
    std.fs.renameAbsolute(old_sum, new_sum) catch {};

    const entries_before = h.state.repl_log.entries.items.len;
    h.state.repl_log.enqueuePair(.delete, "old-name.txt", .put, "new-name.txt");
    try testing.expectEqual(entries_before + 2, h.state.repl_log.entries.items.len);

    // Process the delete
    try replicateDelete(h.state, "old-name.txt");
    try testing.expect(!h.replicaFileExists("old-name.txt"));

    // Process the put
    try replicatePut(h.state, "new-name.txt");
    try testing.expect(h.replicaFileExists("new-name.txt"));

    // Verify replica content
    const replica_path = try std.fs.path.join(h.allocator, &.{ h.replica_dir, "files", "new-name.txt" });
    defer h.allocator.free(replica_path);
    const file = try std.fs.openFileAbsolute(replica_path, .{});
    defer file.close();
    const content = try file.readToEndAlloc(h.allocator, 1024);
    defer h.allocator.free(content);
    try testing.expectEqualStrings("rename me", content);
}

// ---------- Multiple files: scrub handles mixed state ----------

test "end-to-end: scrub handles mix of clean, untracked, and corrupt files" {
    var h = try TestHarness.init();
    defer h.deinit();

    // File A: clean (has valid .sum)
    try h.createBackingFile("clean.txt", "all good");
    try checksumAndEnqueue(h.state, "clean.txt");
    try replicatePut(h.state, "clean.txt");
    h.state.repl_log.markCompletedByPath("clean.txt");

    // File B: untracked (no .sum)
    try h.createBackingFile("untracked.txt", "new arrival");

    // File C: corrupted (has .sum but content changed)
    try h.createBackingFile("corrupt.txt", "original");
    try checksumAndEnqueue(h.state, "corrupt.txt");
    try replicatePut(h.state, "corrupt.txt");
    h.state.repl_log.markCompletedByPath("corrupt.txt");
    try h.createBackingFile("corrupt.txt", "DAMAGED");

    var corruptions: u64 = 0;
    var repairs: u64 = 0;

    // Scrub all three files
    try scrubFile(h.state, "clean.txt", &corruptions, &repairs);
    try scrubFile(h.state, "untracked.txt", &corruptions, &repairs);
    try scrubFile(h.state, "corrupt.txt", &corruptions, &repairs);

    // 1 corruption (corrupt.txt), 1 repair (from replica)
    try testing.expectEqual(@as(u64, 1), corruptions);
    try testing.expectEqual(@as(u64, 1), repairs);

    // Untracked file should now have a .sum
    const untracked_sum = try std.fs.path.join(h.allocator, &.{ h.backing_dir, "untracked.txt.sum" });
    defer h.allocator.free(untracked_sum);
    std.fs.accessAbsolute(untracked_sum, .{}) catch return error.UntrackedSumMissing;

    // Corrupt file should be repaired
    const restored = try h.readBackingFile("corrupt.txt");
    defer h.allocator.free(restored);
    try testing.expectEqualStrings("original", restored);

    // Clean file should be unchanged
    const clean = try h.readBackingFile("clean.txt");
    defer h.allocator.free(clean);
    try testing.expectEqualStrings("all good", clean);
}
