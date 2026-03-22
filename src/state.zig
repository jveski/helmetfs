const std = @import("std");
const helpers = @import("helpers.zig");
const repl_log = @import("repl_log.zig");
const log = helpers.log;

pub const ReplLog = repl_log.ReplLog;
pub const ReplOp = repl_log.ReplOp;

pub var g_state: *FsState = undefined;

pub const PathInfo = struct {
    dirty_gen: u64 = 0,
    clean_gen: u64 = 0,
    write_refcount: u32 = 0,
};

pub const PathStateMap = struct {
    rwlock: std.Thread.RwLock = .{},
    map: std.StringHashMap(PathInfo),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) PathStateMap {
        return .{
            .map = std.StringHashMap(PathInfo).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *PathStateMap) void {
        var it = self.map.iterator();
        while (it.next()) |entry| self.allocator.free(entry.key_ptr.*);
        self.map.deinit();
    }

    pub fn getOrCreate(self: *PathStateMap, rel_path: []const u8) ?*PathInfo {
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

    pub fn setDirty(self: *PathStateMap, rel_path: []const u8) void {
        self.rwlock.lock();
        defer self.rwlock.unlock();
        if (self.getOrCreate(rel_path)) |info| info.dirty_gen += 1;
    }

    pub fn incWriteRef(self: *PathStateMap, rel_path: []const u8) void {
        self.rwlock.lock();
        defer self.rwlock.unlock();
        if (self.getOrCreate(rel_path)) |info| info.write_refcount += 1;
    }

    pub fn decWriteRef(self: *PathStateMap, rel_path: []const u8) void {
        self.rwlock.lock();
        defer self.rwlock.unlock();
        if (self.map.getPtr(rel_path)) |info| {
            if (info.write_refcount > 0) {
                info.write_refcount -= 1;
            }
        }
    }

    pub fn isDirty(self: *PathStateMap, rel_path: []const u8) bool {
        self.rwlock.lockShared();
        defer self.rwlock.unlockShared();
        if (self.map.get(rel_path)) |info| return info.dirty_gen > info.clean_gen;
        return false;
    }

    pub fn hasWriteRef(self: *PathStateMap, rel_path: []const u8) bool {
        self.rwlock.lockShared();
        defer self.rwlock.unlockShared();
        if (self.map.get(rel_path)) |info| return info.write_refcount > 0;
        return false;
    }

    pub fn clearDirty(self: *PathStateMap, rel_path: []const u8) void {
        self.rwlock.lock();
        defer self.rwlock.unlock();
        if (self.map.getPtr(rel_path)) |info| info.clean_gen = info.dirty_gen;
    }

    pub fn getDirtyGen(self: *PathStateMap, rel_path: []const u8) u64 {
        self.rwlock.lockShared();
        defer self.rwlock.unlockShared();
        if (self.map.get(rel_path)) |info| return info.dirty_gen;
        return 0;
    }

    pub fn clearDirtyIfGen(self: *PathStateMap, rel_path: []const u8, gen: u64) void {
        self.rwlock.lock();
        defer self.rwlock.unlock();
        if (self.map.getPtr(rel_path)) |info| {
            if (info.dirty_gen == gen) {
                info.clean_gen = gen;
            }
        }
    }

    pub fn remove(self: *PathStateMap, rel_path: []const u8) void {
        self.rwlock.lock();
        defer self.rwlock.unlock();
        if (self.map.fetchRemove(rel_path)) |kv| {
            self.allocator.free(kv.key);
        }
    }
};

pub const FsState = struct {
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

    pub fn init(
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
        self.repl_log = try ReplLog.init(allocator, backing_dir, &self.shutdown);
        return self;
    }

    pub fn deinit(self: *FsState) void {
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

    pub fn stopWorkers(self: *FsState) void {
        self.shutdown.store(true, .release);
        {
            self.repl_log.mutex.lock();
            defer self.repl_log.mutex.unlock();
            self.repl_log.cond.broadcast();
        }
        for (self.repl_threads) |t| t.join();
        if (self.scrub_thread) |t| t.join();
    }

    pub fn flushDirtyFiles(self: *FsState) void {
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

pub fn checksumAndEnqueue(state: *FsState, rel_path: []const u8) !void {
    // Skip if file still has open write descriptors — checksumming a
    // partially-written file would produce a wrong digest.
    if (state.path_state.hasWriteRef(rel_path)) return;
    try checksumAndEnqueueForced(state, rel_path);
}

pub fn checksumAndEnqueueForced(state: *FsState, rel_path: []const u8) !void {
    // Snapshot dirty_gen before hashing so concurrent writes don't get lost.
    const gen = state.path_state.getDirtyGen(rel_path);

    const backing_path = try std.fs.path.join(state.allocator, &.{ state.backing_dir, rel_path });
    defer state.allocator.free(backing_path);
    const sum_path = try std.fmt.allocPrint(state.allocator, "{s}.sum", .{backing_path});
    defer state.allocator.free(sum_path);

    const hex_digest = try helpers.computeBlake3(backing_path);
    try helpers.writeSumFile(sum_path, &hex_digest);
    try state.repl_log.enqueue(.put, rel_path);
    state.path_state.clearDirtyIfGen(rel_path, gen);
}

test "PathStateMap: dirty, writeRef, clearDirtyIfGen, and remove" {
    const testing = std.testing;

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
