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
    }
});

const log = std.log.scoped(.helmetfs);

// macFUSE bitfields are opaque to cImport; we define ABI-compatible structs.
const FuseFileInfo = extern struct {
    flags: i32,
    bitfields: u32 = 0,
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

// Bit 63 of fh encodes "opened for writing" — avoids relying on fi.flags in
// release(), which is not guaranteed to reflect original open flags.
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

const LibfuseVersion = extern struct {
    major: u32,
    minor: u32,
    hotfix: u32,
    flags: u32,
};

fn fuseNew(args: [*c]c.struct_fuse_args, ops: [*c]const c.struct_fuse_operations, op_size: usize, user_data: ?*anyopaque) ?*c.struct_fuse {
    if (comptime builtin.os.tag == .macos) {
        var version = LibfuseVersion{ .major = 3, .minor = 17, .hotfix = 0, .flags = 0 };
        return c._fuse_new_31(args, ops, op_size, @ptrCast(&version), user_data);
    }
    return c.fuse_new(args, ops, op_size, user_data);
}

var g_state: *FsState = undefined;

const FsState = struct {
    allocator: std.mem.Allocator,
    backing_dir: []const u8,
    replica_dir: []const u8,
    scrub_hour: u8,
    scrub_minute: u8,
    repl_workers: u32,
    no_remote_mkdir: bool,
    path_state: PathStateMap,
    repl_log: ReplLog,
    shutdown: std.atomic.Value(bool),
    scrub_thread: ?std.Thread,
    repl_threads: []std.Thread,

    fn init(
        allocator: std.mem.Allocator,
        backing_dir: []const u8,
        replica_dir: []const u8,
        scrub_hour: u8,
        scrub_minute: u8,
        repl_workers: u32,
        no_remote_mkdir: bool,
    ) !*FsState {
        const self = try allocator.create(FsState);
        self.* = .{
            .allocator = allocator,
            .backing_dir = backing_dir,
            .replica_dir = replica_dir,
            .scrub_hour = scrub_hour,
            .scrub_minute = scrub_minute,
            .repl_workers = repl_workers,
            .no_remote_mkdir = no_remote_mkdir,
            .path_state = PathStateMap.init(allocator),
            .repl_log = undefined,
            .shutdown = std.atomic.Value(bool).init(false),
            .scrub_thread = null,
            .repl_threads = &.{},
        };
        const helmetfs_dir = try std.fs.path.join(allocator, &.{ backing_dir, ".helmetfs" });
        defer allocator.free(helmetfs_dir);
        std.fs.makeDirAbsolute(helmetfs_dir) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };
        self.repl_log = try ReplLog.init(allocator, backing_dir);
        return self;
    }

    fn deinit(self: *FsState) void {
        for (self.repl_log.entries.items) |entry| {
            self.allocator.free(entry.path);
        }
        self.repl_log.entries.deinit(self.allocator);
        self.path_state.deinit();
        if (self.repl_threads.len > 0) {
            self.allocator.free(self.repl_threads);
        }
        self.allocator.destroy(self);
    }

    fn startWorkers(self: *FsState) !void {
        self.repl_threads = try self.allocator.alloc(std.Thread, self.repl_workers);
        var started: usize = 0;
        errdefer {
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
        self.scrub_thread = try std.Thread.spawn(.{}, scrubLoop, .{self});
    }

    fn stopWorkers(self: *FsState) void {
        self.shutdown.store(true, .release);
        {
            self.repl_log.mutex.lock();
            defer self.repl_log.mutex.unlock();
            self.repl_log.cond.broadcast();
        }
        for (self.repl_threads) |t| t.join();
        if (self.scrub_thread) |t| t.join();
    }

    fn flushDirtyFiles(self: *FsState) void {
        self.path_state.rwlock.lockShared();
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

const PathInfo = struct {
    dirty_gen: u64 = 0,
    clean_gen: u64 = 0,
    write_refcount: u32 = 0,
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

    fn deinit(self: *PathStateMap) void {
        var it = self.map.iterator();
        while (it.next()) |entry| self.allocator.free(entry.key_ptr.*);
        self.map.deinit();
    }

    fn getOrCreate(self: *PathStateMap, rel_path: []const u8) ?*PathInfo {
        const key_copy = self.allocator.dupe(u8, rel_path) catch return null;
        const gop = self.map.getOrPut(key_copy) catch {
            self.allocator.free(key_copy);
            return null;
        };
        if (gop.found_existing) {
            self.allocator.free(key_copy);
        } else {
            gop.value_ptr.* = .{};
        }
        return gop.value_ptr;
    }

    fn setDirty(self: *PathStateMap, rel_path: []const u8) void {
        self.rwlock.lock();
        defer self.rwlock.unlock();
        if (self.getOrCreate(rel_path)) |info| info.dirty_gen += 1;
    }

    fn incWriteRef(self: *PathStateMap, rel_path: []const u8) void {
        self.rwlock.lock();
        defer self.rwlock.unlock();
        if (self.getOrCreate(rel_path)) |info| info.write_refcount += 1;
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

    fn clearDirty(self: *PathStateMap, rel_path: []const u8) void {
        self.rwlock.lock();
        defer self.rwlock.unlock();
        if (self.map.getPtr(rel_path)) |info| info.clean_gen = info.dirty_gen;
    }

    fn getDirtyGen(self: *PathStateMap, rel_path: []const u8) u64 {
        self.rwlock.lockShared();
        defer self.rwlock.unlockShared();
        if (self.map.get(rel_path)) |info| return info.dirty_gen;
        return 0;
    }

    fn clearDirtyIfGen(self: *PathStateMap, rel_path: []const u8, gen: u64) void {
        self.rwlock.lock();
        defer self.rwlock.unlock();
        if (self.map.getPtr(rel_path)) |info| {
            if (info.dirty_gen == gen) {
                info.clean_gen = gen;
            }
        }
    }

    fn remove(self: *PathStateMap, rel_path: []const u8) void {
        self.rwlock.lock();
        defer self.rwlock.unlock();
        if (self.map.fetchRemove(rel_path)) |kv| {
            self.allocator.free(kv.key);
        }
    }
};

const ReplOp = enum { put, delete };

const ReplEntry = struct {
    id: u64 = 0,
    op: ReplOp,
    path: []const u8,
    completed: bool = false,
    in_flight: bool = false,
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
        self.loadFromDisk() catch |err| {
            log.warn("failed to load replication log: {}", .{err});
        };
        return self;
    }

    fn deinitEntries(self: *ReplLog) void {
        for (self.entries.items) |entry| self.allocator.free(entry.path);
        self.entries.deinit(self.allocator);
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
        const first_space = std.mem.indexOfScalar(u8, line, ' ') orelse return error.InvalidFormat;
        const op_str = line[0..first_space];
        const rel_path = line[first_space + 1 ..];

        const op: ReplOp = if (std.mem.eql(u8, op_str, "put"))
            .put
        else if (std.mem.eql(u8, op_str, "delete"))
            .delete
        else
            return error.InvalidOp;

        const id = self.next_id;
        self.next_id += 1;
        try self.entries.append(self.allocator, .{
            .id = id,
            .op = op,
            .path = try self.allocator.dupe(u8, rel_path),
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

        self.appendToDisk(op, rel_path) catch |err| {
            log.err("failed to append to replication log: {}", .{err});
        };

        self.cond.signal();
    }

    fn appendToDisk(self: *ReplLog, op: ReplOp, rel_path: []const u8) !void {
        const path = try self.logPath();
        defer self.allocator.free(path);

        const file = try std.fs.createFileAbsolute(path, .{ .truncate = false });
        defer file.close();
        try file.seekFromEnd(0);

        const line = try formatLogEntry(self.allocator, op, rel_path);
        defer self.allocator.free(line);
        try file.writeAll(line);
        try file.sync();
    }

    fn dequeueNext(self: *ReplLog) ?struct { id: u64, op: ReplOp, path: []const u8 } {
        self.mutex.lock();
        defer self.mutex.unlock();

        while (!g_state.shutdown.load(.acquire)) {
            for (self.entries.items, 0..) |*entry, i| {
                if (entry.completed or entry.in_flight) continue;

                if (entry.op == .put) {
                    var dominated = false;
                    for (self.entries.items[i + 1 ..]) |*later| {
                        if (later.op == .put and !later.completed and std.mem.eql(u8, later.path, entry.path)) {
                            dominated = true;
                            break;
                        }
                    }
                    if (dominated) {
                        entry.completed = true;
                        self.completed_count += 1;
                        continue;
                    }
                }

                entry.in_flight = true;
                return .{ .id = entry.id, .op = entry.op, .path = entry.path };
            }
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

        self.maybeTruncate();
    }

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

        for (self.entries.items) |entry| {
            if (entry.completed) {
                self.allocator.free(entry.path);
            }
        }

        self.entries.deinit(self.allocator);
        self.entries = remaining;
        self.completed_count = 0;
        self.last_truncate_time = now;

        self.rewriteLogAtomic() catch |err| {
            log.err("failed to truncate replication log: {}", .{err});
        };
    }

    fn rewriteLogAtomic(self: *ReplLog) !void {
        const tmp_path = try std.fs.path.join(self.allocator, &.{ self.backing_dir, ".helmetfs", "repl.log.tmp" });
        defer self.allocator.free(tmp_path);
        const log_path = try self.logPath();
        defer self.allocator.free(log_path);

        const tmp_file = try std.fs.createFileAbsolute(tmp_path, .{});
        defer tmp_file.close();

        for (self.entries.items) |entry| {
            const line = try formatLogEntry(self.allocator, entry.op, entry.path);
            defer self.allocator.free(line);
            try tmp_file.writeAll(line);
        }
        try tmp_file.sync();

        try std.fs.renameAbsolute(tmp_path, log_path);

        if (std.fs.path.dirname(log_path)) |dir_path| {
            fsyncDir(dir_path);
        }
    }
};

fn formatLogEntry(allocator: std.mem.Allocator, op: ReplOp, rel_path: []const u8) ![]const u8 {
    return try std.fmt.allocPrint(allocator, "{s} {s}\n", .{ @tagName(op), rel_path });
}

fn computeBlake3(backing_path: []const u8) ![64]u8 {
    const file = try std.fs.openFileAbsolute(backing_path, .{});
    defer file.close();

    _ = c.flock(file.handle, c.LOCK_SH);
    defer _ = c.flock(file.handle, c.LOCK_UN);

    var hasher = std.crypto.hash.Blake3.init(.{});
    var buf: [64 * 1024]u8 = undefined; // 64 KB
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
    const trimmed = std.mem.trimRight(u8, buf[0..n], "\n\r ");
    return try allocator.dupe(u8, trimmed);
}

fn checksumAndEnqueue(state: *FsState, rel_path: []const u8) !void {
    // Skip if file still has open write descriptors — checksumming a
    // partially-written file would produce a wrong digest.
    if (state.path_state.hasWriteRef(rel_path)) return;
    try checksumAndEnqueueForced(state, rel_path);
}

fn checksumAndEnqueueForced(state: *FsState, rel_path: []const u8) !void {
    // Snapshot dirty_gen before hashing so concurrent writes don't get lost.
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

fn replWorkerLoop(state: *FsState) void {
    while (!state.shutdown.load(.acquire)) {
        const work = state.repl_log.dequeueNext() orelse break;

        var backoff_ns: u64 = 1_000_000_000;
        const max_backoff_ns: u64 = 300_000_000_000;

        while (!state.shutdown.load(.acquire)) {
            const result = switch (work.op) {
                .put => replicatePut(state, work.path),
                .delete => replicateDelete(state, work.path),
            };

            if (result) |_| {
                state.repl_log.markCompleted(work.id);
                break;
            } else |err| {
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

    const backing_stat = posix.fstatat(posix.AT.FDCWD, backing_path, posix.AT.SYMLINK_NOFOLLOW) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };

    if (backing_stat.mode & posix.S.IFMT == posix.S.IFLNK) {
        try ensureParentDir(replica_path);
        std.fs.deleteFileAbsolute(replica_path) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };
        const backing_z = try state.allocator.dupeZ(u8, backing_path);
        defer state.allocator.free(backing_z);
        const replica_z = try state.allocator.dupeZ(u8, replica_path);
        defer state.allocator.free(replica_z);
        var link_buf: [std.fs.max_path_bytes]u8 = undefined;
        const target = posix.readlinkat(posix.AT.FDCWD, backing_z, &link_buf) catch return error.ReadLinkFailed;
        posix.symlinkat(target, posix.AT.FDCWD, replica_z) catch return error.SymlinkFailed;
        return;
    }

    const sum_backing = try std.fmt.allocPrint(state.allocator, "{s}.sum", .{backing_path});
    defer state.allocator.free(sum_backing);
    const sum_replica = try std.fmt.allocPrint(state.allocator, "{s}.sum", .{replica_path});
    defer state.allocator.free(sum_replica);

    if (readSumFile(state.allocator, sum_backing)) |stored_hex| {
        defer state.allocator.free(stored_hex);
        if (computeBlake3(backing_path)) |computed_hex| {
            if (!std.mem.eql(u8, &computed_hex, stored_hex)) {
                log.warn("replication: skipping {s} — backing file does not match .sum (possible corruption)", .{rel_path});
                return;
            }
        } else |_| {}
    } else |_| {}

    try ensureParentDir(replica_path);
    try copyFileWithSync(backing_path, replica_path);

    copyFileWithSync(sum_backing, sum_replica) catch |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    };

    const stat_info = posix.fstatat(posix.AT.FDCWD, backing_path, 0) catch return;
    const mode: posix.mode_t = stat_info.mode & 0o7777;
    posix.fchmodat(posix.AT.FDCWD, replica_path, mode, 0) catch {};

    const replica_z = state.allocator.dupeZ(u8, replica_path) catch return;
    defer state.allocator.free(replica_z);
    _ = c.chown(replica_z.ptr, stat_info.uid, stat_info.gid);
}

fn replicateDelete(state: *FsState, rel_path: []const u8) !void {
    const replica_path = try std.fs.path.join(state.allocator, &.{ state.replica_dir, "files", rel_path });
    defer state.allocator.free(replica_path);
    const sum_replica = try std.fmt.allocPrint(state.allocator, "{s}.sum", .{replica_path});
    defer state.allocator.free(sum_replica);

    std.fs.deleteFileAbsolute(replica_path) catch |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    };
    std.fs.deleteFileAbsolute(sum_replica) catch |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    };

    if (!state.no_remote_mkdir) {
        const replica_files_root = try std.fs.path.join(state.allocator, &.{ state.replica_dir, "files" });
        defer state.allocator.free(replica_files_root);
        removeEmptyParentDirs(replica_path, replica_files_root);
    }
}

fn copyFileWithSync(src_path: []const u8, dst_path: []const u8) !void {
    const src = try std.fs.openFileAbsolute(src_path, .{});
    defer src.close();

    var tmp_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const tmp_path = std.fmt.bufPrint(&tmp_path_buf, "{s}.tmp", .{dst_path}) catch return error.NameTooLong;

    const dst = try std.fs.createFileAbsolute(tmp_path, .{});

    var ok = false;
    defer {
        if (!ok) {
            dst.close();
            std.fs.deleteFileAbsolute(tmp_path) catch {};
        }
    }

    var buf: [64 * 1024]u8 = undefined; // 64 KB
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

    if (std.fs.path.dirname(dst_path)) |dir_path| {
        fsyncDir(dir_path);
    }
}

fn fsyncDir(dir_path: []const u8) void {
    var dir = std.fs.openDirAbsolute(dir_path, .{}) catch return;
    defer dir.close();
    // Raw syscall avoids unreachable panics on filesystems that return EINVAL for dir fsync.
    _ = std.posix.system.fsync(dir.fd);
}

fn ensureParentDir(path: []const u8) !void {
    const dir_path = std.fs.path.dirname(path) orelse return;
    std.fs.makeDirAbsolute(dir_path) catch |err| switch (err) {
        error.PathAlreadyExists => return,
        error.FileNotFound => {
            try ensureParentDir(dir_path);
            std.fs.makeDirAbsolute(dir_path) catch |e| switch (e) {
                error.PathAlreadyExists => return,
                else => return e,
            };
        },
        else => return err,
    };
}

fn removeEmptyParentDirs(path: []const u8, stop_at: []const u8) void {
    var current = std.fs.path.dirname(path);
    while (current) |dir| {
        if (dir.len <= stop_at.len) break;
        std.fs.deleteDirAbsolute(dir) catch break;
        current = std.fs.path.dirname(dir);
    }
}

fn scrubLoop(state: *FsState) void {
    if (shouldScrubImmediately(state)) {
        log.info("scrub overdue, running immediately", .{});
        runScrub(state);
    }

    while (!state.shutdown.load(.acquire)) {
        const sleep_ns = nsUntilNextScrub(state.scrub_hour, state.scrub_minute);
        var remaining = sleep_ns;
        while (remaining > 0 and !state.shutdown.load(.acquire)) {
            const chunk = @min(remaining, 1_000_000_000);
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
    return (now - last_scrub) > 86400;
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
        if (entry.kind == .directory) continue;
        if (entry.kind == .sym_link) continue;
        if (std.mem.startsWith(u8, entry.path, ".helmetfs")) continue;
        if (std.mem.endsWith(u8, entry.path, ".sum")) continue;

        const rel_path = state.allocator.dupe(u8, entry.path) catch continue;
        defer state.allocator.free(rel_path);

        if (state.path_state.hasWriteRef(rel_path)) continue;

        scrubFile(state, rel_path, &corruptions_found, &repairs) catch |err| {
            log.err("scrub: error checking {s}: {}", .{ rel_path, err });
        };
        files_checked += 1;
    }

    const end = std.time.timestamp();
    const end_ms = std.time.milliTimestamp();
    const duration_ms: u64 = @intCast(end_ms - start_ms);

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

    const current_hex = computeBlake3(backing_path) catch |err| {
        log.err("scrub: failed to compute checksum for {s}: {}", .{ rel_path, err });
        return err;
    };

    const stored_hex = readSumFile(state.allocator, sum_path) catch |err| switch (err) {
        error.FileNotFound => {
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

    if (std.mem.eql(u8, &current_hex, stored_hex)) {
        return;
    }

    corruptions.* += 1;
    log.warn("scrub: CORRUPTION detected in {s}", .{rel_path});

    const has_pending = state.repl_log.hasPendingPut(rel_path);

    const replica_path = try std.fs.path.join(state.allocator, &.{ state.replica_dir, "files", rel_path });
    defer state.allocator.free(replica_path);
    const replica_sum_path = try std.fmt.allocPrint(state.allocator, "{s}.sum", .{replica_path});
    defer state.allocator.free(replica_sum_path);

    const replica_hex = readSumFile(state.allocator, replica_sum_path) catch {
        log.err("scrub: replica unavailable for repair of {s}", .{rel_path});
        return;
    };
    defer state.allocator.free(replica_hex);

    const replica_computed = computeBlake3(replica_path) catch {
        log.err("scrub: cannot read replica file for repair of {s}", .{rel_path});
        return;
    };

    if (!std.mem.eql(u8, &replica_computed, replica_hex)) {
        log.warn("scrub: replica also corrupt for {s}, cannot repair", .{rel_path});
        return;
    }

    if (has_pending) {
        log.warn("scrub: skipping repair of {s} — pending replication means replica is stale", .{rel_path});
        return;
    }

    // Re-check write ref before overwriting — a writer may have opened the
    // file between the initial check and now.
    if (state.path_state.hasWriteRef(rel_path)) {
        log.info("scrub: skipping repair of {s} — file now has open writer", .{rel_path});
        return;
    }

    // Skip if dirty — write completed but hasn't been checksummed yet.
    // (See TLA+ ScrubRepair precondition: dirty_gen = clean_gen.)
    if (state.path_state.isDirty(rel_path)) {
        log.info("scrub: skipping repair of {s} — file is dirty (pending checksum)", .{rel_path});
        return;
    }

    log.info("scrub: repairing {s} from replica", .{rel_path});

    copyFileWithSync(replica_path, backing_path) catch |err| {
        log.err("scrub: failed to repair {s}: {}", .{ rel_path, err });
        return;
    };
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

fn isHiddenPath(state: *FsState, rel_path: []const u8) bool {
    if (std.mem.startsWith(u8, rel_path, ".helmetfs")) return true;

    if (std.mem.endsWith(u8, rel_path, ".sum")) {
        const data_rel = rel_path[0 .. rel_path.len - 4];
        const data_full = std.fs.path.join(state.allocator, &.{ state.backing_dir, data_rel }) catch return false;
        defer state.allocator.free(data_full);
        _ = posix.fstatat(posix.AT.FDCWD, data_full, posix.AT.SYMLINK_NOFOLLOW) catch return false;
        return true;
    }

    return false;
}

fn fuseRelPath(path: [*c]const u8) []const u8 {
    const s = std.mem.span(@as([*:0]const u8, @ptrCast(path)));
    if (s.len > 0 and s[0] == '/') return s[1..];
    return s;
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

fn fuse_getattr(path: [*c]const u8, stbuf: [*c]c.struct_stat, fi: ?*c.struct_fuse_file_info) callconv(.c) c_int {
    _ = fi;
    const state = g_state;
    const rel = fuseRelPath(path);

    if (rel.len > 0 and isHiddenPath(state, rel)) {
        return fuseErr(.NOENT);
    }

    const backing = backingPath(state.allocator, state, rel) catch return fuseErr(.NOMEM);
    defer state.allocator.free(backing);

    const stat_val = posix.fstatat(posix.AT.FDCWD, backing, posix.AT.SYMLINK_NOFOLLOW) catch |err| return posixErr(err);
    const buf: *posix.Stat = @ptrCast(@alignCast(stbuf));
    buf.* = stat_val;
    return 0;
}

fn fuse_readdir(path: [*c]const u8, buf: ?*anyopaque, filler: c.fuse_fill_dir_t, _: c.off_t, _: ?*c.struct_fuse_file_info, _: c.enum_fuse_readdir_flags) callconv(.c) c_int {
    const state = g_state;
    const rel = fuseRelPath(path);

    const backing = backingPath(state.allocator, state, rel) catch return fuseErr(.NOMEM);
    defer state.allocator.free(backing);

    var dir = std.fs.openDirAbsoluteZ(backing, .{ .iterate = true }) catch |err| return posixErr(err);
    defer dir.close();

    _ = filler.?(buf, ".", null, 0, 0);
    _ = filler.?(buf, "..", null, 0, 0);

    var it = dir.iterate();
    while (it.next() catch null) |entry| {
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
    const fd = posix.openZ(backing, @bitCast(flags), 0) catch |err| return posixErr(err);

    if (castFi(fi)) |f| {
        const raw_flags = @as(u32, @bitCast(flags));
        const acc_mode = raw_flags & 0o3;
        const has_trunc = (raw_flags & @as(u32, c.O_TRUNC)) != 0;
        const is_write = (acc_mode == 1 or acc_mode == 2 or has_trunc);
        f.fh = encodeFh(fd, is_write);
        if (is_write) {
            state.path_state.incWriteRef(rel);
        }
        if (has_trunc) {
            state.path_state.setDirty(rel);
        }
    } else {
        posix.close(fd);
        return fuseErr(.BADF);
    }
    return 0;
}

fn fuse_read(path: [*c]const u8, buf_ptr: [*c]u8, size: usize, offset: c.off_t, fi: ?*c.struct_fuse_file_info) callconv(.c) c_int {
    _ = path;
    const fd: posix.fd_t = if (castFi(fi)) |f| decodeFh(f.fh).fd else return fuseErr(.BADF);
    const n = posix.pread(fd, buf_ptr[0..size], @intCast(offset)) catch {
        return fuseErr(.IO);
    };
    return @intCast(n);
}

fn fuse_write(path: [*c]const u8, data: [*c]const u8, size: usize, offset: c.off_t, fi: ?*c.struct_fuse_file_info) callconv(.c) c_int {
    const state = g_state;
    const rel = fuseRelPath(path);
    const fd: posix.fd_t = if (castFi(fi)) |f| decodeFh(f.fh).fd else return fuseErr(.BADF);
    const n = posix.pwrite(fd, @as([*]const u8, @ptrCast(data))[0..size], @intCast(offset)) catch {
        return fuseErr(.IO);
    };
    if (rel.len > 0) {
        state.path_state.setDirty(rel);
    }
    return @intCast(n);
}

fn fuse_fsync(path: [*c]const u8, datasync: c_int, fi: ?*c.struct_fuse_file_info) callconv(.c) c_int {
    const state = g_state;
    const rel = fuseRelPath(path);

    if (castFi(fi)) |f| {
        const fd: posix.fd_t = decodeFh(f.fh).fd;
        if (datasync != 0) {
            posix.fdatasync(fd) catch {};
        } else {
            posix.fsync(fd) catch {};
        }
    }

    // Use forced variant because the file is still open but data is fsync'd.
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

    // Decrement write refcount BEFORE checksumming so checksumAndEnqueue
    // doesn't skip this file due to hasWriteRef.
    if (castFi(fi)) |f| {
        const decoded = decodeFh(f.fh);
        if (decoded.opened_for_write) {
            state.path_state.decWriteRef(rel);
        }
        posix.close(decoded.fd);
    }

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
    const fd = posix.openZ(backing, @bitCast(flags), mode) catch |err| return posixErr(err);

    if (castFi(fi)) |f| {
        f.fh = encodeFh(fd, true);
        state.path_state.incWriteRef(rel);
    } else {
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

    std.fs.deleteFileAbsolute(backing) catch |err| return posixErr(err);

    const sum_path = std.fmt.allocPrint(state.allocator, "{s}.sum", .{backing}) catch return 0;
    defer state.allocator.free(sum_path);
    std.fs.deleteFileAbsolute(sum_path) catch {};

    if (rel.len > 0) {
        state.repl_log.enqueue(.delete, rel) catch |err| {
            log.err("failed to enqueue delete for {s}: {}", .{ rel, err });
        };
    }

    state.path_state.remove(rel);
    return 0;
}

fn fuse_rename(from: [*c]const u8, to: [*c]const u8, flags: c_uint) callconv(.c) c_int {
    const state = g_state;
    const rel_from = fuseRelPath(from);
    const rel_to = fuseRelPath(to);

    if (comptime builtin.os.tag == .linux) {
        const RENAME_NOREPLACE = 1;
        const RENAME_EXCHANGE = 2;
        if (flags & ~@as(c_uint, RENAME_NOREPLACE) != 0) {
            if (flags & RENAME_EXCHANGE != 0) {
                return fuseErr(.OPNOTSUPP);
            }
            return fuseErr(.INVAL);
        }
        if (flags & RENAME_NOREPLACE != 0) {
            const backing_to_check = backingPath(state.allocator, state, rel_to) catch return fuseErr(.NOMEM);
            defer state.allocator.free(backing_to_check);
            if (posix.fstatat(posix.AT.FDCWD, backing_to_check, posix.AT.SYMLINK_NOFOLLOW)) |_| {
                return fuseErr(.EXIST);
            } else |_| {}
        }
    } else {
        if (flags != 0) {
            return fuseErr(.OPNOTSUPP);
        }
    }

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

    std.fs.renameAbsolute(backing_from, backing_to) catch |err| return posixErr(err);

    const sum_from = std.fmt.allocPrint(state.allocator, "{s}.sum", .{backing_from}) catch return 0;
    defer state.allocator.free(sum_from);
    const sum_to = std.fmt.allocPrint(state.allocator, "{s}.sum", .{backing_to}) catch return 0;
    defer state.allocator.free(sum_to);
    std.fs.renameAbsolute(sum_from, sum_to) catch {};

    if (rel_from.len > 0 and rel_to.len > 0) {
        state.repl_log.enqueue(.delete, rel_from) catch |err| {
            log.err("failed to enqueue rename delete for {s}: {}", .{ rel_from, err });
        };
        state.repl_log.enqueue(.put, rel_to) catch |err| {
            log.err("failed to enqueue rename put for {s}: {}", .{ rel_to, err });
        };
    }

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

    std.fs.makeDirAbsolute(backing) catch |err| return posixErr(err);

    posix.fchmodat(posix.AT.FDCWD, backing, mode, 0) catch {};

    if (!state.no_remote_mkdir and rel.len > 0) {
        const replica_path = std.fs.path.join(state.allocator, &.{ state.replica_dir, "files", rel }) catch return 0;
        defer state.allocator.free(replica_path);
        ensureParentDir(replica_path) catch {};
        std.fs.makeDirAbsolute(replica_path) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => {
                log.warn("failed to mkdir on replica for {s}: {}", .{ rel, err });
            },
        };
    }
    return 0;
}

fn fuse_rmdir(path: [*c]const u8) callconv(.c) c_int {
    const state = g_state;
    const rel = fuseRelPath(path);

    const backing = backingPath(state.allocator, state, rel) catch return fuseErr(.NOMEM);
    defer state.allocator.free(backing);

    std.fs.deleteDirAbsolute(backing) catch |err| return posixErr(err);

    if (!state.no_remote_mkdir and rel.len > 0) {
        const replica_path = std.fs.path.join(state.allocator, &.{ state.replica_dir, "files", rel }) catch return 0;
        defer state.allocator.free(replica_path);
        std.fs.deleteDirAbsolute(replica_path) catch {};
    }
    return 0;
}

fn fuse_symlink(target: [*c]const u8, linkpath: [*c]const u8) callconv(.c) c_int {
    const state = g_state;
    const rel = fuseRelPath(linkpath);

    const backing = backingPath(state.allocator, state, rel) catch return fuseErr(.NOMEM);
    defer state.allocator.free(backing);

    posix.symlinkat(std.mem.span(@as([*:0]const u8, @ptrCast(target))), posix.AT.FDCWD, backing) catch |err| return posixErr(err);

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
    const target = posix.readlinkat(posix.AT.FDCWD, backing, buf[0 .. size - 1]) catch |err| return posixErr(err);
    buf[target.len] = 0;

    return 0;
}

fn fuse_chmod(path: [*c]const u8, mode: c.mode_t, fi: ?*c.struct_fuse_file_info) callconv(.c) c_int {
    _ = fi;
    const state = g_state;
    const rel = fuseRelPath(path);

    const backing = backingPath(state.allocator, state, rel) catch return fuseErr(.NOMEM);
    defer state.allocator.free(backing);

    posix.fchmodat(posix.AT.FDCWD, backing, mode, 0) catch return fuseErr(.IO);

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

    const ret = c.lchown(backing.ptr, uid, gid);
    if (ret != 0) {
        const err_val = std.c._errno().*;
        return -@as(c_int, @intCast(err_val));
    }

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
        posix.ftruncate(fd, @intCast(size)) catch return fuseErr(.IO);
    } else {
        const backing = backingPath(state.allocator, state, rel) catch return fuseErr(.NOMEM);
        defer state.allocator.free(backing);
        const file = std.fs.openFileAbsolute(backing, .{ .mode = .read_write }) catch return fuseErr(.NOENT);
        defer file.close();
        posix.ftruncate(file.handle, @intCast(size)) catch return fuseErr(.IO);
    }

    // For the non-fi path (truncate without an open fd), there is no
    // subsequent fuse_release, so we must checksum here.
    if (rel.len > 0) {
        state.path_state.setDirty(rel);
        checksumAndEnqueue(state, rel) catch |err| {
            log.err("truncate checksum failed for {s}: {}", .{ rel, err });
        };
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

const fuse_ops = std.mem.zeroInit(c.struct_fuse_operations, .{
    .getattr = fuse_getattr,
    .readlink = fuse_readlink,
    .mkdir = fuse_mkdir,
    .unlink = fuse_unlink,
    .rmdir = fuse_rmdir,
    .symlink = fuse_symlink,
    .rename = fuse_rename,
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
});

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

const CliArgs = struct {
    command: enum { mount, unmount },
    source: []const u8,
    mountpoint: []const u8,
    replica: ?[]const u8 = null,
    repl_workers: u32 = 4,
    scrub_time: []const u8 = "01:00",
    no_remote_mkdir: bool = false,
};

fn parseArgs(allocator: std.mem.Allocator) !CliArgs {
    var args_iter = try std.process.argsWithAllocator(allocator);
    defer args_iter.deinit();

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
        } else if (std.mem.eql(u8, arg, "--scrub-time")) {
            result.scrub_time = args_iter.next() orelse {
                std.debug.print("--scrub-time requires a value (HH:MM)\n", .{});
                std.process.exit(1);
            };
        } else if (std.mem.eql(u8, arg, "--no-remote-mkdir")) {
            result.no_remote_mkdir = true;
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
        \\  --scrub-time HH:MM        Scrub schedule in 24h format (default: 01:00)
        \\  --no-remote-mkdir          Skip creating/removing subdirectories on the
        \\                             replica. Use when the replica is an object store
        \\                             or filesystem that does not require explicit
        \\                             directory management.
        \\
    , .{});
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    const args = try parseArgs(allocator);

    switch (args.command) {
        .unmount => doUnmount(args.mountpoint),
        .mount => try doMount(allocator, args),
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
        else => {
            std.debug.print("{s} terminated abnormally\n", .{cmd});
            std.process.exit(1);
        },
    }
}

fn doMount(allocator: std.mem.Allocator, args: CliArgs) !void {
    const scrub = parseScrubTime(args.scrub_time) catch {
        std.debug.print("Invalid scrub time: {s} (expected HH:MM)\n", .{args.scrub_time});
        std.process.exit(1);
    };

    const source_abs = try std.fs.realpathAlloc(allocator, args.source);
    const replica_abs = try std.fs.realpathAlloc(allocator, args.replica.?);
    const mount_abs = try std.fs.realpathAlloc(allocator, args.mountpoint);

    log.info("helmetfs starting", .{});
    log.info("  backing dir: {s}", .{source_abs});
    log.info("  mountpoint:  {s}", .{mount_abs});
    log.info("  replica dir: {s}", .{replica_abs});
    log.info("  workers:     {d}", .{args.repl_workers});
    log.info("  scrub time:  {s}", .{args.scrub_time});
    log.info("  remote mkdir: {s}", .{if (args.no_remote_mkdir) "disabled" else "enabled"});

    g_state = try FsState.init(
        allocator,
        source_abs,
        replica_abs,
        scrub.hour,
        scrub.minute,
        args.repl_workers,
        args.no_remote_mkdir,
    );

    try g_state.startWorkers();

    const mount_z = try allocator.dupeZ(u8, mount_abs);

    var fuse_argv = [_][*:0]const u8{"helmetfs"};
    var fuse_args = c.fuse_args{
        .argc = @intCast(fuse_argv.len),
        .argv = @ptrCast(&fuse_argv),
        .allocated = 0,
    };

    const fuse_instance = fuseNew(&fuse_args, &fuse_ops, @sizeOf(c.struct_fuse_operations), null);

    if (fuse_instance == null) {
        log.err("fuse_new failed", .{});
        std.process.exit(1);
    }
    g_fuse_instance = fuse_instance;

    setupSignalHandlers();

    if (c.fuse_mount(fuse_instance, mount_z.ptr) != 0) {
        log.err("fuse_mount failed", .{});
        c.fuse_destroy(fuse_instance);
        std.process.exit(1);
    }

    log.info("mounted, serving requests", .{});

    var loop_cfg = c.struct_fuse_loop_config{
        .clone_fd = 0,
        .max_idle_threads = 10,
    };
    const ret = c.fuse_loop_mt(fuse_instance, &loop_cfg);

    log.info("FUSE loop exited with {d}", .{ret});

    c.fuse_unmount(fuse_instance);
    c.fuse_destroy(fuse_instance);

    g_state.deinit();

    log.info("helmetfs shutdown complete", .{});
}

const testing = std.testing;

const TestHarness = struct {
    allocator: std.mem.Allocator,
    backing_dir: []const u8,
    replica_dir: []const u8,
    state: *FsState,
    tmp_dir_path: []const u8,

    fn init() !TestHarness {
        return initWithFlags(false);
    }

    fn initWithFlags(no_remote_mkdir: bool) !TestHarness {
        const allocator = testing.allocator;

        const tmp_template = "/tmp/helmetfs-test-XXXXXX";
        var tmp_buf: [tmp_template.len:0]u8 = tmp_template.*;
        const result = c.mkdtemp(&tmp_buf);
        if (result == null) return error.TmpDirFailed;
        const tmp_dir = try allocator.dupe(u8, std.mem.span(result));

        const backing = try std.fs.path.join(allocator, &.{ tmp_dir, "backing" });
        const replica = try std.fs.path.join(allocator, &.{ tmp_dir, "replica" });

        try std.fs.makeDirAbsolute(backing);
        try std.fs.makeDirAbsolute(replica);
        const replica_files = try std.fs.path.join(allocator, &.{ replica, "files" });
        defer allocator.free(replica_files);
        try std.fs.makeDirAbsolute(replica_files);

        const state = try FsState.init(allocator, backing, replica, 1, 0, 1, no_remote_mkdir);
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
        std.fs.deleteTreeAbsolute(self.tmp_dir_path) catch {};
        self.allocator.free(self.backing_dir);
        self.allocator.free(self.replica_dir);
        self.allocator.free(self.tmp_dir_path);
        self.state.deinit();
    }

    fn createBackingFile(self: *TestHarness, rel_path: []const u8, contents: []const u8) !void {
        const full = try std.fs.path.join(self.allocator, &.{ self.backing_dir, rel_path });
        defer self.allocator.free(full);
        try ensureParentDir(full);
        const file = try std.fs.createFileAbsolute(full, .{});
        defer file.close();
        try file.writeAll(contents);
    }

    fn readBackingFile(self: *TestHarness, rel_path: []const u8) ![]const u8 {
        const full = try std.fs.path.join(self.allocator, &.{ self.backing_dir, rel_path });
        defer self.allocator.free(full);
        const file = try std.fs.openFileAbsolute(full, .{});
        defer file.close();
        return try file.readToEndAlloc(self.allocator, 1024 * 1024);
    }

    fn createReplicaFile(self: *TestHarness, rel_path: []const u8, contents: []const u8) !void {
        const full = try std.fs.path.join(self.allocator, &.{ self.replica_dir, "files", rel_path });
        defer self.allocator.free(full);
        try ensureParentDir(full);
        const file = try std.fs.createFileAbsolute(full, .{});
        defer file.close();
        try file.writeAll(contents);
    }

    fn replicaFileExists(self: *TestHarness, rel_path: []const u8) bool {
        const full = std.fs.path.join(self.allocator, &.{ self.replica_dir, "files", rel_path }) catch return false;
        defer self.allocator.free(full);
        std.fs.accessAbsolute(full, .{}) catch return false;
        return true;
    }
};

test "formatLogEntry/parseLine round-trip" {
    var h = try TestHarness.init();
    defer h.deinit();

    const cases = .{
        .{ .op = ReplOp.put, .path = "hello/world.txt" },
        .{ .op = ReplOp.delete, .path = "gone.txt" },
    };
    inline for (cases) |tc| {
        const line = try formatLogEntry(h.allocator, tc.op, tc.path);
        defer h.allocator.free(line);
        const trimmed = std.mem.trimRight(u8, line, "\n");
        const before = h.state.repl_log.entries.items.len;
        try h.state.repl_log.parseLine(trimmed);
        try testing.expectEqual(before + 1, h.state.repl_log.entries.items.len);
        const entry = h.state.repl_log.entries.getLast();
        try testing.expectEqual(tc.op, entry.op);
        try testing.expectEqualStrings(tc.path, entry.path);
    }
}

test "PathStateMap: dirty, writeRef, clearDirtyIfGen, and remove" {
    var psm = PathStateMap.init(testing.allocator);
    defer psm.deinit();

    try testing.expect(!psm.isDirty("foo.txt"));
    psm.setDirty("foo.txt");
    try testing.expect(psm.isDirty("foo.txt"));
    psm.clearDirty("foo.txt");
    try testing.expect(!psm.isDirty("foo.txt"));

    try testing.expect(!psm.hasWriteRef("a.txt"));
    psm.incWriteRef("a.txt");
    psm.incWriteRef("a.txt");
    try testing.expect(psm.hasWriteRef("a.txt"));
    psm.decWriteRef("a.txt");
    try testing.expect(psm.hasWriteRef("a.txt"));
    psm.decWriteRef("a.txt");
    try testing.expect(!psm.hasWriteRef("a.txt"));

    psm.setDirty("c.txt");
    psm.incWriteRef("c.txt");
    psm.clearDirty("c.txt");
    try testing.expect(!psm.isDirty("c.txt"));
    try testing.expect(psm.hasWriteRef("c.txt"));

    psm.setDirty("g.txt");
    const gen = psm.getDirtyGen("g.txt");
    psm.clearDirtyIfGen("g.txt", gen -% 1);
    try testing.expect(psm.isDirty("g.txt"));
    psm.clearDirtyIfGen("g.txt", gen);
    try testing.expect(!psm.isDirty("g.txt"));

    psm.setDirty("r.txt");
    psm.remove("r.txt");
    try testing.expect(!psm.isDirty("r.txt"));
}

test "isHiddenPath" {
    var h = try TestHarness.init();
    defer h.deinit();

    try testing.expect(isHiddenPath(h.state, ".helmetfs"));
    try testing.expect(isHiddenPath(h.state, ".helmetfs/repl.log"));

    try h.createBackingFile("data.txt", "hello");
    try testing.expect(isHiddenPath(h.state, "data.txt.sum"));

    try testing.expect(!isHiddenPath(h.state, "nodata.txt.sum"));

    try testing.expect(!isHiddenPath(h.state, "readme.md"));
    try testing.expect(!isHiddenPath(h.state, "subdir/file.txt"));
}

test "parseScrubTime" {
    const valid_cases = .{
        .{ "01:00", 1, 0 },
        .{ "23:59", 23, 59 },
        .{ "00:00", 0, 0 },
    };
    inline for (valid_cases) |tc| {
        const r = try parseScrubTime(tc[0]);
        try testing.expectEqual(@as(u8, tc[1]), r.hour);
        try testing.expectEqual(@as(u8, tc[2]), r.minute);
    }

    const error_cases = .{
        .{ "24:00", error.InvalidTime },
        .{ "12:60", error.InvalidTime },
        .{ "1200", error.InvalidFormat },
    };
    inline for (error_cases) |tc| {
        try testing.expectError(tc[1], parseScrubTime(tc[0]));
    }
}

test "nsUntilNextScrub returns value in (0, 24h]" {
    for ([_]u8{ 0, 3, 6, 12, 18, 23 }) |hour| {
        const ns = nsUntilNextScrub(hour, 0);
        try testing.expect(ns > 0);
        try testing.expect(ns <= 86400 * 1_000_000_000);
    }
}

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

    const parent = std.fs.path.dirname(deep_file).?;
    var dir = try std.fs.openDirAbsolute(parent, .{});
    dir.close();
}

test "checksumAndEnqueue creates .sum and enqueues put" {
    var h = try TestHarness.init();
    defer h.deinit();

    try h.createBackingFile("doc.txt", "important data");

    const entries_before = h.state.repl_log.entries.items.len;
    try checksumAndEnqueue(h.state, "doc.txt");

    const sum_path = try std.fs.path.join(h.allocator, &.{ h.backing_dir, "doc.txt.sum" });
    defer h.allocator.free(sum_path);
    const hex = try readSumFile(h.allocator, sum_path);
    defer h.allocator.free(hex);
    try testing.expectEqual(@as(usize, 64), hex.len);

    try testing.expect(h.state.repl_log.entries.items.len > entries_before);
    const last = h.state.repl_log.entries.getLast();
    try testing.expectEqual(ReplOp.put, last.op);
    try testing.expectEqualStrings("doc.txt", last.path);
}

test "ReplLog enqueue and hasPendingPut" {
    var h = try TestHarness.init();
    defer h.deinit();

    const before = h.state.repl_log.entries.items.len;
    try h.state.repl_log.enqueue(.put, "file1.txt");
    try h.state.repl_log.enqueue(.delete, "file2.txt");
    try testing.expectEqual(before + 2, h.state.repl_log.entries.items.len);

    try testing.expect(!h.state.repl_log.hasPendingPut("nope.txt"));
    try h.state.repl_log.enqueue(.put, "x.txt");
    try testing.expect(h.state.repl_log.hasPendingPut("x.txt"));
}

test "replicatePut copies file and .sum to replica" {
    var h = try TestHarness.init();
    defer h.deinit();

    try h.createBackingFile("replme.txt", "replicate this");
    try checksumAndEnqueue(h.state, "replme.txt");
    try replicatePut(h.state, "replme.txt");

    const replica_path = try std.fs.path.join(h.allocator, &.{ h.replica_dir, "files", "replme.txt" });
    defer h.allocator.free(replica_path);
    const file = try std.fs.openFileAbsolute(replica_path, .{});
    defer file.close();
    const contents = try file.readToEndAlloc(h.allocator, 1024);
    defer h.allocator.free(contents);
    try testing.expectEqualStrings("replicate this", contents);

    const replica_sum = try std.fmt.allocPrint(h.allocator, "{s}.sum", .{replica_path});
    defer h.allocator.free(replica_sum);
    std.fs.accessAbsolute(replica_sum, .{}) catch {
        return error.ReplicaSumMissing;
    };

    try replicatePut(h.state, "ghost.txt");
    try testing.expect(!h.replicaFileExists("ghost.txt"));
}

test "replicateDelete removes files and is idempotent" {
    var h = try TestHarness.init();
    defer h.deinit();

    try h.createReplicaFile("todelete.txt", "gone soon");
    const replica_sum = try std.fs.path.join(h.allocator, &.{ h.replica_dir, "files", "todelete.txt.sum" });
    defer h.allocator.free(replica_sum);
    try writeSumFile(replica_sum, "0" ** 64);

    try testing.expect(h.replicaFileExists("todelete.txt"));

    try replicateDelete(h.state, "todelete.txt");
    try testing.expect(!h.replicaFileExists("todelete.txt"));

    try replicateDelete(h.state, "todelete.txt");
}

test "ReplLog persists to disk and reloads" {
    var h = try TestHarness.init();
    defer h.deinit();

    try h.state.repl_log.enqueue(.put, "persist1.txt");
    try h.state.repl_log.enqueue(.delete, "persist2.txt");

    var log2 = try ReplLog.init(h.allocator, h.backing_dir);
    defer log2.deinitEntries();

    try testing.expectEqual(@as(usize, 2), log2.entries.items.len);
    try testing.expectEqual(ReplOp.put, log2.entries.items[0].op);
    try testing.expectEqualStrings("persist1.txt", log2.entries.items[0].path);
    try testing.expectEqual(ReplOp.delete, log2.entries.items[1].op);
    try testing.expectEqualStrings("persist2.txt", log2.entries.items[1].path);

    const log_path = try std.fs.path.join(h.allocator, &.{ h.backing_dir, ".helmetfs", "repl.log" });
    defer h.allocator.free(log_path);

    const valid_line = try formatLogEntry(h.allocator, .put, "good.txt");
    defer h.allocator.free(valid_line);

    {
        const file = try std.fs.createFileAbsolute(log_path, .{ .truncate = true });
        defer file.close();
        try file.writeAll(valid_line);
        try file.writeAll("badop corrupted.txt\n");
        try file.sync();
    }

    var log3 = try ReplLog.init(h.allocator, h.backing_dir);
    defer log3.deinitEntries();

    try testing.expectEqual(@as(usize, 1), log3.entries.items.len);
    try testing.expectEqualStrings("good.txt", log3.entries.items[0].path);
}

test "dequeueNext coalesces puts but not deletes" {
    var h = try TestHarness.init();
    defer h.deinit();

    const p1 = try h.allocator.dupe(u8, "dup.txt");
    const p2 = try h.allocator.dupe(u8, "dup.txt");
    const p3 = try h.allocator.dupe(u8, "unique.txt");
    try h.state.repl_log.entries.append(h.allocator, .{ .id = 0, .op = .put, .path = p1 });
    try h.state.repl_log.entries.append(h.allocator, .{ .id = 1, .op = .put, .path = p3 });
    try h.state.repl_log.entries.append(h.allocator, .{ .id = 2, .op = .put, .path = p2 });
    h.state.repl_log.next_id = 3;

    const first = h.state.repl_log.dequeueNext();
    try testing.expect(first != null);
    try testing.expectEqualStrings("unique.txt", first.?.path);
    try testing.expect(h.state.repl_log.entries.items[0].completed);

    h.state.repl_log.markCompleted(first.?.id);

    const second = h.state.repl_log.dequeueNext();
    try testing.expect(second != null);
    try testing.expectEqualStrings("dup.txt", second.?.path);
    h.state.repl_log.markCompleted(second.?.id);

    h.state.shutdown.store(true, .release);
    h.state.repl_log.cond.broadcast();
    try testing.expect(h.state.repl_log.dequeueNext() == null);

    h.state.shutdown.store(false, .release);

    for (h.state.repl_log.entries.items) |entry| {
        if (!entry.completed) h.allocator.free(entry.path);
    }
    h.state.repl_log.entries.clearRetainingCapacity();
    h.state.repl_log.completed_count = 0;

    const d1 = try h.allocator.dupe(u8, "del.txt");
    const d2 = try h.allocator.dupe(u8, "del.txt");
    try h.state.repl_log.entries.append(h.allocator, .{ .id = 10, .op = .delete, .path = d1 });
    try h.state.repl_log.entries.append(h.allocator, .{ .id = 11, .op = .delete, .path = d2 });
    h.state.repl_log.next_id = 12;

    const del_first = h.state.repl_log.dequeueNext();
    try testing.expect(del_first != null);
    try testing.expectEqual(@as(u64, 10), del_first.?.id);
    try testing.expectEqual(ReplOp.delete, del_first.?.op);
    h.state.repl_log.markCompleted(del_first.?.id);

    const del_second = h.state.repl_log.dequeueNext();
    try testing.expect(del_second != null);
    try testing.expectEqual(@as(u64, 11), del_second.?.id);
    try testing.expectEqual(ReplOp.delete, del_second.?.op);
    h.state.repl_log.markCompleted(del_second.?.id);

    h.state.shutdown.store(true, .release);
    h.state.repl_log.cond.broadcast();
    try testing.expect(h.state.repl_log.dequeueNext() == null);
}

test "markCompleted triggers truncation and removes completed entries" {
    var h = try TestHarness.init();
    defer h.deinit();

    try h.state.repl_log.enqueue(.put, "a.txt");
    try h.state.repl_log.enqueue(.put, "b.txt");
    try h.state.repl_log.enqueue(.delete, "c.txt");
    try h.state.repl_log.enqueue(.put, "d.txt");

    try testing.expectEqual(@as(usize, 4), h.state.repl_log.entries.items.len);

    h.state.repl_log.last_truncate_time = std.time.timestamp();

    h.state.repl_log.markCompleted(0);
    h.state.repl_log.markCompleted(1);
    h.state.repl_log.markCompleted(2);

    try testing.expectEqual(@as(usize, 1), h.state.repl_log.entries.items.len);
    try testing.expectEqualStrings("d.txt", h.state.repl_log.entries.items[0].path);
    try testing.expectEqual(@as(usize, 0), h.state.repl_log.completed_count);
}

test "scrubFile does not repair when replica is also corrupt" {
    var h = try TestHarness.init();
    defer h.deinit();

    try h.createBackingFile("both-bad.txt", "original data");
    try checksumAndEnqueue(h.state, "both-bad.txt");
    try replicatePut(h.state, "both-bad.txt");

    try h.createBackingFile("both-bad.txt", "CORRUPTED BACKING");
    try h.createReplicaFile("both-bad.txt", "CORRUPTED REPLICA");

    var corruptions: u64 = 0;
    var repairs: u64 = 0;
    try scrubFile(h.state, "both-bad.txt", &corruptions, &repairs);

    try testing.expectEqual(@as(u64, 1), corruptions);
    try testing.expectEqual(@as(u64, 0), repairs);

    const contents = try h.readBackingFile("both-bad.txt");
    defer h.allocator.free(contents);
    try testing.expectEqualStrings("CORRUPTED BACKING", contents);
}

test "end-to-end: file goes through checksum, replication, corruption, and scrub repair" {
    var h = try TestHarness.init();
    defer h.deinit();

    try h.createBackingFile("e2e.txt", "precious data");
    h.state.path_state.setDirty("e2e.txt");
    try testing.expect(h.state.path_state.isDirty("e2e.txt"));

    try checksumAndEnqueue(h.state, "e2e.txt");
    try testing.expect(!h.state.path_state.isDirty("e2e.txt"));

    const sum_path = try std.fs.path.join(h.allocator, &.{ h.backing_dir, "e2e.txt.sum" });
    defer h.allocator.free(sum_path);
    const original_sum = try readSumFile(h.allocator, sum_path);
    defer h.allocator.free(original_sum);
    try testing.expectEqual(@as(usize, 64), original_sum.len);

    try replicatePut(h.state, "e2e.txt");
    h.state.repl_log.markCompletedByPath("e2e.txt");
    try testing.expect(h.replicaFileExists("e2e.txt"));

    const replica_path = try std.fs.path.join(h.allocator, &.{ h.replica_dir, "files", "e2e.txt" });
    defer h.allocator.free(replica_path);
    const replica_file = try std.fs.openFileAbsolute(replica_path, .{});
    defer replica_file.close();
    const replica_contents = try replica_file.readToEndAlloc(h.allocator, 1024);
    defer h.allocator.free(replica_contents);
    try testing.expectEqualStrings("precious data", replica_contents);

    try h.createBackingFile("e2e.txt", "CORRUPTED DATA!!");

    var corruptions: u64 = 0;
    var repairs: u64 = 0;
    try scrubFile(h.state, "e2e.txt", &corruptions, &repairs);
    try testing.expectEqual(@as(u64, 1), corruptions);
    try testing.expectEqual(@as(u64, 1), repairs);

    const restored = try h.readBackingFile("e2e.txt");
    defer h.allocator.free(restored);
    try testing.expectEqualStrings("precious data", restored);

    const repaired_sum = try readSumFile(h.allocator, sum_path);
    defer h.allocator.free(repaired_sum);
    try testing.expectEqualStrings(original_sum, repaired_sum);
}

test "flushDirtyFiles processes all dirty paths" {
    var h = try TestHarness.init();
    defer h.deinit();

    try h.createBackingFile("flush1.txt", "data one");
    try h.createBackingFile("flush2.txt", "data two");

    h.state.path_state.setDirty("flush1.txt");
    h.state.path_state.setDirty("flush2.txt");

    try testing.expect(h.state.path_state.isDirty("flush1.txt"));
    try testing.expect(h.state.path_state.isDirty("flush2.txt"));

    const entries_before = h.state.repl_log.entries.items.len;

    h.state.flushDirtyFiles();

    const sum1 = try std.fs.path.join(h.allocator, &.{ h.backing_dir, "flush1.txt.sum" });
    defer h.allocator.free(sum1);
    const sum2 = try std.fs.path.join(h.allocator, &.{ h.backing_dir, "flush2.txt.sum" });
    defer h.allocator.free(sum2);
    std.fs.accessAbsolute(sum1, .{}) catch return error.Sum1Missing;
    std.fs.accessAbsolute(sum2, .{}) catch return error.Sum2Missing;

    try testing.expect(h.state.repl_log.entries.items.len >= entries_before + 2);

    try testing.expect(!h.state.path_state.isDirty("flush1.txt"));
    try testing.expect(!h.state.path_state.isDirty("flush2.txt"));
}

test "ReplLog atomic rewrite preserves only pending entries on disk" {
    var h = try TestHarness.init();
    defer h.deinit();

    try h.state.repl_log.enqueue(.put, "keep.txt");
    try h.state.repl_log.enqueue(.delete, "remove.txt");
    try h.state.repl_log.enqueue(.put, "also-keep.txt");

    h.state.repl_log.last_truncate_time = 0;
    h.state.repl_log.markCompleted(1);

    var log2 = try ReplLog.init(h.allocator, h.backing_dir);
    defer log2.deinitEntries();

    try testing.expectEqual(@as(usize, 2), log2.entries.items.len);
    try testing.expectEqualStrings("keep.txt", log2.entries.items[0].path);
    try testing.expectEqualStrings("also-keep.txt", log2.entries.items[1].path);
}

test "end-to-end: rename enqueues delete+put pair and replication works" {
    var h = try TestHarness.init();
    defer h.deinit();

    try h.createBackingFile("old-name.txt", "rename me");
    try checksumAndEnqueue(h.state, "old-name.txt");
    try replicatePut(h.state, "old-name.txt");
    try testing.expect(h.replicaFileExists("old-name.txt"));

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
    try h.state.repl_log.enqueue(.delete, "old-name.txt");
    try h.state.repl_log.enqueue(.put, "new-name.txt");
    try testing.expectEqual(entries_before + 2, h.state.repl_log.entries.items.len);

    try replicateDelete(h.state, "old-name.txt");
    try testing.expect(!h.replicaFileExists("old-name.txt"));

    try replicatePut(h.state, "new-name.txt");
    try testing.expect(h.replicaFileExists("new-name.txt"));

    const replica_path = try std.fs.path.join(h.allocator, &.{ h.replica_dir, "files", "new-name.txt" });
    defer h.allocator.free(replica_path);
    const file = try std.fs.openFileAbsolute(replica_path, .{});
    defer file.close();
    const content = try file.readToEndAlloc(h.allocator, 1024);
    defer h.allocator.free(content);
    try testing.expectEqualStrings("rename me", content);
}

test "end-to-end: scrub handles mix of clean, untracked, and corrupt files" {
    var h = try TestHarness.init();
    defer h.deinit();

    try h.createBackingFile("clean.txt", "all good");
    try checksumAndEnqueue(h.state, "clean.txt");
    try replicatePut(h.state, "clean.txt");
    h.state.repl_log.markCompletedByPath("clean.txt");

    try h.createBackingFile("untracked.txt", "new arrival");

    try h.createBackingFile("corrupt.txt", "original");
    try checksumAndEnqueue(h.state, "corrupt.txt");
    try replicatePut(h.state, "corrupt.txt");
    h.state.repl_log.markCompletedByPath("corrupt.txt");
    try h.createBackingFile("corrupt.txt", "DAMAGED");

    var corruptions: u64 = 0;
    var repairs: u64 = 0;

    try scrubFile(h.state, "clean.txt", &corruptions, &repairs);
    try scrubFile(h.state, "untracked.txt", &corruptions, &repairs);
    try scrubFile(h.state, "corrupt.txt", &corruptions, &repairs);

    try testing.expectEqual(@as(u64, 1), corruptions);
    try testing.expectEqual(@as(u64, 1), repairs);

    const untracked_sum = try std.fs.path.join(h.allocator, &.{ h.backing_dir, "untracked.txt.sum" });
    defer h.allocator.free(untracked_sum);
    std.fs.accessAbsolute(untracked_sum, .{}) catch return error.UntrackedSumMissing;

    const restored = try h.readBackingFile("corrupt.txt");
    defer h.allocator.free(restored);
    try testing.expectEqualStrings("original", restored);

    const clean = try h.readBackingFile("clean.txt");
    defer h.allocator.free(clean);
    try testing.expectEqualStrings("all good", clean);
}

test "removeEmptyParentDirs" {
    var h = try TestHarness.init();
    defer h.deinit();

    const replica_files = try std.fs.path.join(h.allocator, &.{ h.replica_dir, "files" });
    defer h.allocator.free(replica_files);

    // Case 1: removes empty parents up to stop_at
    {
        const dir_c = try std.fs.path.join(h.allocator, &.{ replica_files, "a", "b", "c" });
        defer h.allocator.free(dir_c);
        try ensureParentDir(dir_c);
        try std.fs.makeDirAbsolute(dir_c);

        const file_path = try std.fs.path.join(h.allocator, &.{ dir_c, "file.txt" });
        defer h.allocator.free(file_path);
        const f = try std.fs.createFileAbsolute(file_path, .{});
        f.close();
        try std.fs.deleteFileAbsolute(file_path);

        removeEmptyParentDirs(file_path, replica_files);

        const dir_a = try std.fs.path.join(h.allocator, &.{ replica_files, "a" });
        defer h.allocator.free(dir_a);
        std.fs.accessAbsolute(dir_a, .{}) catch |err| switch (err) {
            error.FileNotFound => {
                // Expected — all empty dirs removed
            },
            else => return err,
        };
    }

    // Case 2: stops at non-empty directory
    {
        const file_path = try std.fs.path.join(h.allocator, &.{ replica_files, "x", "y", "z", "file.txt" });
        defer h.allocator.free(file_path);
        try ensureParentDir(file_path);
        const f1 = try std.fs.createFileAbsolute(file_path, .{});
        f1.close();

        const sibling = try std.fs.path.join(h.allocator, &.{ replica_files, "x", "sibling.txt" });
        defer h.allocator.free(sibling);
        const f2 = try std.fs.createFileAbsolute(sibling, .{});
        f2.close();

        try std.fs.deleteFileAbsolute(file_path);
        removeEmptyParentDirs(file_path, replica_files);

        const dir_y = try std.fs.path.join(h.allocator, &.{ replica_files, "x", "y" });
        defer h.allocator.free(dir_y);
        std.fs.accessAbsolute(dir_y, .{}) catch |err| switch (err) {
            error.FileNotFound => {
                const dir_x = try std.fs.path.join(h.allocator, &.{ replica_files, "x" });
                defer h.allocator.free(dir_x);
                try std.fs.accessAbsolute(dir_x, .{});
                // Clean up for next case
                try std.fs.deleteFileAbsolute(sibling);
                std.fs.deleteTreeAbsolute(dir_x) catch {};
            },
            else => return err,
        };
    }

    // Case 3: does not remove stop_at directory itself
    {
        const file_path = try std.fs.path.join(h.allocator, &.{ replica_files, "root-file.txt" });
        defer h.allocator.free(file_path);
        const f = try std.fs.createFileAbsolute(file_path, .{});
        f.close();
        try std.fs.deleteFileAbsolute(file_path);

        removeEmptyParentDirs(file_path, replica_files);

        try std.fs.accessAbsolute(replica_files, .{});
    }
}

test "replicateDelete skips dir cleanup when no_remote_mkdir is set" {
    var h = try TestHarness.initWithFlags(true);
    defer h.deinit();

    try h.createBackingFile("sub/file.txt", "data");
    try checksumAndEnqueue(h.state, "sub/file.txt");
    try replicatePut(h.state, "sub/file.txt");

    try replicateDelete(h.state, "sub/file.txt");

    const replica_files = try std.fs.path.join(h.allocator, &.{ h.replica_dir, "files" });
    defer h.allocator.free(replica_files);
    const replica_sub = try std.fs.path.join(h.allocator, &.{ replica_files, "sub" });
    defer h.allocator.free(replica_sub);
    try std.fs.accessAbsolute(replica_sub, .{});
}
