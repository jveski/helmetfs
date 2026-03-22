const std = @import("std");
const helpers = @import("helpers.zig");
const log = helpers.log;

pub const ReplOp = enum { put, delete };

pub const ReplEntry = struct {
    id: u64 = 0,
    op: ReplOp,
    path: []const u8,
    completed: bool = false,
    in_flight: bool = false,
};

pub const ReplLog = struct {
    allocator: std.mem.Allocator,
    backing_dir: []const u8,
    mutex: std.Thread.Mutex = .{},
    cond: std.Thread.Condition = .{},
    entries: std.ArrayList(ReplEntry),
    completed_count: usize = 0,
    last_truncate_time: i64 = 0,
    next_id: u64 = 0,
    shutdown: *std.atomic.Value(bool),

    pub fn init(allocator: std.mem.Allocator, backing_dir: []const u8, shutdown: *std.atomic.Value(bool)) !ReplLog {
        var self = ReplLog{
            .allocator = allocator,
            .backing_dir = backing_dir,
            .entries = .empty,
            .shutdown = shutdown,
        };
        self.last_truncate_time = std.time.timestamp();
        self.loadFromDisk() catch |err| {
            log.warn("failed to load replication log: {}", .{err});
        };
        return self;
    }

    pub fn deinitEntries(self: *ReplLog) void {
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

    pub fn parseLine(self: *ReplLog, line: []const u8) !void {
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

    pub fn enqueue(self: *ReplLog, op: ReplOp, rel_path: []const u8) !void {
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

    pub fn dequeueNext(self: *ReplLog) ?struct { id: u64, op: ReplOp, path: []const u8 } {
        self.mutex.lock();
        defer self.mutex.unlock();

        while (!self.shutdown.load(.acquire)) {
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

    pub fn markCompleted(self: *ReplLog, id: u64) void {
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

    pub fn markCompletedByPath(self: *ReplLog, rel_path: []const u8) void {
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

    pub fn pendingCountLocked(self: *ReplLog) u64 {
        var count: u64 = 0;
        for (self.entries.items) |entry| {
            if (!entry.completed) count += 1;
        }
        return count;
    }

    pub fn hasPendingPut(self: *ReplLog, rel_path: []const u8) bool {
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
            helpers.fsyncDir(dir_path);
        }
    }
};

pub fn formatLogEntry(allocator: std.mem.Allocator, op: ReplOp, rel_path: []const u8) ![]const u8 {
    return try std.fmt.allocPrint(allocator, "{s} {s}\n", .{ @tagName(op), rel_path });
}

test "formatLogEntry/parseLine round-trip" {
    const testing = std.testing;
    const allocator = testing.allocator;

    // Create a minimal ReplLog for parseLine testing
    var shutdown = std.atomic.Value(bool).init(false);
    var tmp_dir_buf: ["/tmp/helmetfs-rl-test-XXXXXX".len:0]u8 = "/tmp/helmetfs-rl-test-XXXXXX".*;
    const result = helpers.c.mkdtemp(&tmp_dir_buf);
    if (result == null) return error.TmpDirFailed;
    const tmp_dir = try allocator.dupe(u8, std.mem.span(result));
    defer allocator.free(tmp_dir);
    defer std.fs.deleteTreeAbsolute(tmp_dir) catch {};

    const helmetfs_dir = try std.fs.path.join(allocator, &.{ tmp_dir, ".helmetfs" });
    defer allocator.free(helmetfs_dir);
    try std.fs.makeDirAbsolute(helmetfs_dir);

    var rl = try ReplLog.init(allocator, tmp_dir, &shutdown);
    defer rl.deinitEntries();

    const cases = .{
        .{ .op = ReplOp.put, .path = "hello/world.txt" },
        .{ .op = ReplOp.delete, .path = "gone.txt" },
    };
    inline for (cases) |tc| {
        const line = try formatLogEntry(allocator, tc.op, tc.path);
        defer allocator.free(line);
        const trimmed = std.mem.trimRight(u8, line, "\n");
        const before = rl.entries.items.len;
        try rl.parseLine(trimmed);
        try testing.expectEqual(before + 1, rl.entries.items.len);
        const entry = rl.entries.getLast();
        try testing.expectEqual(tc.op, entry.op);
        try testing.expectEqualStrings(tc.path, entry.path);
    }
}
