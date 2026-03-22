const std = @import("std");
const builtin = @import("builtin");
const helpers = @import("helpers.zig");
const repl_log = @import("repl_log.zig");
const state_mod = @import("state.zig");
const replication = @import("replication.zig");
const scrub = @import("scrub.zig");
const fuse_ops_mod = @import("fuse_ops.zig");

const posix = helpers.posix;
const c = helpers.c;
const log = helpers.log;
const FsState = state_mod.FsState;
const ReplLog = repl_log.ReplLog;
const ReplOp = repl_log.ReplOp;

var g_fuse_instance: ?*c.struct_fuse = null;

fn signalHandler(_: c_int) callconv(.c) void {
    state_mod.g_state.shutdown.store(true, .release);
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

fn startWorkers(state: *FsState) !void {
    state.repl_threads = try state.allocator.alloc(std.Thread, state.repl_workers);
    var started: usize = 0;
    errdefer {
        state.shutdown.store(true, .release);
        {
            state.repl_log.mutex.lock();
            defer state.repl_log.mutex.unlock();
            state.repl_log.cond.broadcast();
        }
        for (state.repl_threads[0..started]) |t| {
            t.join();
        }
        state.allocator.free(state.repl_threads);
        state.repl_threads = &.{};
    }
    for (state.repl_threads) |*t| {
        t.* = try std.Thread.spawn(.{}, replication.replWorkerLoop, .{state});
        started += 1;
    }
    state.scrub_thread = try std.Thread.spawn(.{}, scrub.scrubLoop, .{state});
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
    const scrub_time = parseScrubTime(args.scrub_time) catch {
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

    const state = try FsState.init(
        allocator,
        source_abs,
        replica_abs,
        scrub_time.hour,
        scrub_time.minute,
        args.repl_workers,
        args.no_remote_mkdir,
    );
    state_mod.g_state = state;

    try startWorkers(state);

    const mount_z = try allocator.dupeZ(u8, mount_abs);

    var fuse_argv = [_][*:0]const u8{"helmetfs"};
    var fuse_args = c.fuse_args{
        .argc = @intCast(fuse_argv.len),
        .argv = @ptrCast(&fuse_argv),
        .allocated = 0,
    };

    const fuse_instance = fuse_ops_mod.fuseNew(&fuse_args, &fuse_ops_mod.fuse_ops, @sizeOf(c.struct_fuse_operations), null);

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

    state.deinit();

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
        state_mod.g_state = state;

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
        try replication.ensureParentDir(full);
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
        try replication.ensureParentDir(full);
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

test "isHiddenPath" {
    var h = try TestHarness.init();
    defer h.deinit();

    try testing.expect(helpers.isHiddenPath(h.allocator, h.backing_dir, ".helmetfs"));
    try testing.expect(helpers.isHiddenPath(h.allocator, h.backing_dir, ".helmetfs/repl.log"));

    try h.createBackingFile("data.txt", "hello");
    try testing.expect(helpers.isHiddenPath(h.allocator, h.backing_dir, "data.txt.sum"));

    try testing.expect(!helpers.isHiddenPath(h.allocator, h.backing_dir, "nodata.txt.sum"));

    try testing.expect(!helpers.isHiddenPath(h.allocator, h.backing_dir, "readme.md"));
    try testing.expect(!helpers.isHiddenPath(h.allocator, h.backing_dir, "subdir/file.txt"));
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
        const ns = scrub.nsUntilNextScrub(hour, 0);
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

    const hex1 = try helpers.computeBlake3(full);
    const hex2 = try helpers.computeBlake3(full);
    try testing.expectEqual(@as(usize, 64), hex1.len);
    try testing.expectEqualSlices(u8, &hex1, &hex2);
}

test "writeSumFile / readSumFile round-trip" {
    var h = try TestHarness.init();
    defer h.deinit();

    const sum_path = try std.fs.path.join(h.allocator, &.{ h.backing_dir, "test.txt.sum" });
    defer h.allocator.free(sum_path);

    const hex = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
    try helpers.writeSumFile(sum_path, hex);

    const read_hex = try helpers.readSumFile(h.allocator, sum_path);
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

    try replication.copyFileWithSync(src, dst);

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

    try replication.ensureParentDir(deep_file);

    const parent = std.fs.path.dirname(deep_file).?;
    var dir = try std.fs.openDirAbsolute(parent, .{});
    dir.close();
}

test "checksumAndEnqueue creates .sum and enqueues put" {
    var h = try TestHarness.init();
    defer h.deinit();

    try h.createBackingFile("doc.txt", "important data");

    const entries_before = h.state.repl_log.entries.items.len;
    try state_mod.checksumAndEnqueue(h.state, "doc.txt");

    const sum_path = try std.fs.path.join(h.allocator, &.{ h.backing_dir, "doc.txt.sum" });
    defer h.allocator.free(sum_path);
    const hex = try helpers.readSumFile(h.allocator, sum_path);
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
    try state_mod.checksumAndEnqueue(h.state, "replme.txt");
    try replication.replicatePut(h.state, "replme.txt");

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

    try replication.replicatePut(h.state, "ghost.txt");
    try testing.expect(!h.replicaFileExists("ghost.txt"));
}

test "replicateDelete removes files and is idempotent" {
    var h = try TestHarness.init();
    defer h.deinit();

    try h.createReplicaFile("todelete.txt", "gone soon");
    const replica_sum = try std.fs.path.join(h.allocator, &.{ h.replica_dir, "files", "todelete.txt.sum" });
    defer h.allocator.free(replica_sum);
    try helpers.writeSumFile(replica_sum, "0" ** 64);

    try testing.expect(h.replicaFileExists("todelete.txt"));

    try replication.replicateDelete(h.state, "todelete.txt");
    try testing.expect(!h.replicaFileExists("todelete.txt"));

    try replication.replicateDelete(h.state, "todelete.txt");
}

test "ReplLog persists to disk and reloads" {
    var h = try TestHarness.init();
    defer h.deinit();

    try h.state.repl_log.enqueue(.put, "persist1.txt");
    try h.state.repl_log.enqueue(.delete, "persist2.txt");

    var log2 = try ReplLog.init(h.allocator, h.backing_dir, &h.state.shutdown);
    defer log2.deinitEntries();

    try testing.expectEqual(@as(usize, 2), log2.entries.items.len);
    try testing.expectEqual(ReplOp.put, log2.entries.items[0].op);
    try testing.expectEqualStrings("persist1.txt", log2.entries.items[0].path);
    try testing.expectEqual(ReplOp.delete, log2.entries.items[1].op);
    try testing.expectEqualStrings("persist2.txt", log2.entries.items[1].path);

    const log_path = try std.fs.path.join(h.allocator, &.{ h.backing_dir, ".helmetfs", "repl.log" });
    defer h.allocator.free(log_path);

    const valid_line = try repl_log.formatLogEntry(h.allocator, .put, "good.txt");
    defer h.allocator.free(valid_line);

    {
        const file = try std.fs.createFileAbsolute(log_path, .{ .truncate = true });
        defer file.close();
        try file.writeAll(valid_line);
        try file.writeAll("badop corrupted.txt\n");
        try file.sync();
    }

    var log3 = try ReplLog.init(h.allocator, h.backing_dir, &h.state.shutdown);
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
    try state_mod.checksumAndEnqueue(h.state, "both-bad.txt");
    try replication.replicatePut(h.state, "both-bad.txt");

    try h.createBackingFile("both-bad.txt", "CORRUPTED BACKING");
    try h.createReplicaFile("both-bad.txt", "CORRUPTED REPLICA");

    var corruptions: u64 = 0;
    var repairs: u64 = 0;
    try scrub.scrubFile(h.state, "both-bad.txt", &corruptions, &repairs);

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

    try state_mod.checksumAndEnqueue(h.state, "e2e.txt");
    try testing.expect(!h.state.path_state.isDirty("e2e.txt"));

    const sum_path = try std.fs.path.join(h.allocator, &.{ h.backing_dir, "e2e.txt.sum" });
    defer h.allocator.free(sum_path);
    const original_sum = try helpers.readSumFile(h.allocator, sum_path);
    defer h.allocator.free(original_sum);
    try testing.expectEqual(@as(usize, 64), original_sum.len);

    try replication.replicatePut(h.state, "e2e.txt");
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
    try scrub.scrubFile(h.state, "e2e.txt", &corruptions, &repairs);
    try testing.expectEqual(@as(u64, 1), corruptions);
    try testing.expectEqual(@as(u64, 1), repairs);

    const restored = try h.readBackingFile("e2e.txt");
    defer h.allocator.free(restored);
    try testing.expectEqualStrings("precious data", restored);

    const repaired_sum = try helpers.readSumFile(h.allocator, sum_path);
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

    var log2 = try ReplLog.init(h.allocator, h.backing_dir, &h.state.shutdown);
    defer log2.deinitEntries();

    try testing.expectEqual(@as(usize, 2), log2.entries.items.len);
    try testing.expectEqualStrings("keep.txt", log2.entries.items[0].path);
    try testing.expectEqualStrings("also-keep.txt", log2.entries.items[1].path);
}

test "end-to-end: rename enqueues delete+put pair and replication works" {
    var h = try TestHarness.init();
    defer h.deinit();

    try h.createBackingFile("old-name.txt", "rename me");
    try state_mod.checksumAndEnqueue(h.state, "old-name.txt");
    try replication.replicatePut(h.state, "old-name.txt");
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

    try replication.replicateDelete(h.state, "old-name.txt");
    try testing.expect(!h.replicaFileExists("old-name.txt"));

    try replication.replicatePut(h.state, "new-name.txt");
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
    try state_mod.checksumAndEnqueue(h.state, "clean.txt");
    try replication.replicatePut(h.state, "clean.txt");
    h.state.repl_log.markCompletedByPath("clean.txt");

    try h.createBackingFile("untracked.txt", "new arrival");

    try h.createBackingFile("corrupt.txt", "original");
    try state_mod.checksumAndEnqueue(h.state, "corrupt.txt");
    try replication.replicatePut(h.state, "corrupt.txt");
    h.state.repl_log.markCompletedByPath("corrupt.txt");
    try h.createBackingFile("corrupt.txt", "DAMAGED");

    var corruptions: u64 = 0;
    var repairs: u64 = 0;

    try scrub.scrubFile(h.state, "clean.txt", &corruptions, &repairs);
    try scrub.scrubFile(h.state, "untracked.txt", &corruptions, &repairs);
    try scrub.scrubFile(h.state, "corrupt.txt", &corruptions, &repairs);

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
        try replication.ensureParentDir(dir_c);
        try std.fs.makeDirAbsolute(dir_c);

        const file_path = try std.fs.path.join(h.allocator, &.{ dir_c, "file.txt" });
        defer h.allocator.free(file_path);
        const f = try std.fs.createFileAbsolute(file_path, .{});
        f.close();
        try std.fs.deleteFileAbsolute(file_path);

        replication.removeEmptyParentDirs(file_path, replica_files);

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
        try replication.ensureParentDir(file_path);
        const f1 = try std.fs.createFileAbsolute(file_path, .{});
        f1.close();

        const sibling = try std.fs.path.join(h.allocator, &.{ replica_files, "x", "sibling.txt" });
        defer h.allocator.free(sibling);
        const f2 = try std.fs.createFileAbsolute(sibling, .{});
        f2.close();

        try std.fs.deleteFileAbsolute(file_path);
        replication.removeEmptyParentDirs(file_path, replica_files);

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

        replication.removeEmptyParentDirs(file_path, replica_files);

        try std.fs.accessAbsolute(replica_files, .{});
    }
}

test "replicateDelete skips dir cleanup when no_remote_mkdir is set" {
    var h = try TestHarness.initWithFlags(true);
    defer h.deinit();

    try h.createBackingFile("sub/file.txt", "data");
    try state_mod.checksumAndEnqueue(h.state, "sub/file.txt");
    try replication.replicatePut(h.state, "sub/file.txt");

    try replication.replicateDelete(h.state, "sub/file.txt");

    const replica_files = try std.fs.path.join(h.allocator, &.{ h.replica_dir, "files" });
    defer h.allocator.free(replica_files);
    const replica_sub = try std.fs.path.join(h.allocator, &.{ replica_files, "sub" });
    defer h.allocator.free(replica_sub);
    try std.fs.accessAbsolute(replica_sub, .{});
}

// ---------------------------------------------------------------------------
// Fuzz-style concurrency stress tests
// ---------------------------------------------------------------------------

/// Worker function for PathStateMap fuzz test.  Each byte of `input` selects
/// an operation and a path from a small pool, exercising every PathStateMap
/// method under contention from many threads.
fn fuzzPathStateWorker(psm: *state_mod.PathStateMap, input: []const u8) void {
    const paths = [_][]const u8{ "a.txt", "b.txt", "c.txt", "d/e.txt" };

    for (input) |byte| {
        const path = paths[byte % paths.len];
        switch ((byte / 4) % 8) {
            0 => psm.setDirty(path),
            1 => _ = psm.isDirty(path),
            2 => psm.incWriteRef(path),
            3 => psm.decWriteRef(path),
            4 => _ = psm.hasWriteRef(path),
            5 => psm.clearDirty(path),
            6 => psm.clearDirtyIfGen(path, psm.getDirtyGen(path)),
            7 => psm.remove(path),
            else => unreachable,
        }
    }
}

fn fuzzTestOnePathState(_: void, input: []const u8) anyerror!void {
    const allocator = testing.allocator;
    var psm = state_mod.PathStateMap.init(allocator);
    defer psm.deinit();

    const thread_count = 8;
    var threads: [thread_count]std.Thread = undefined;
    var started: usize = 0;

    // Give each thread a different slice of the input (or the whole thing if
    // the input is short) so they hit overlapping paths with varying ops.
    for (0..thread_count) |i| {
        const chunk_start = if (input.len > 0) (i * input.len / thread_count) else 0;
        const chunk_end = if (input.len > 0) ((i + 1) * input.len / thread_count) else 0;
        const chunk = if (chunk_end > chunk_start) input[chunk_start..chunk_end] else input[0..0];
        threads[i] = std.Thread.spawn(.{}, fuzzPathStateWorker, .{ &psm, chunk }) catch
            return error.ThreadSpawnFailed;
        started += 1;
    }

    for (threads[0..started]) |t| t.join();
}

test "fuzz: PathStateMap concurrent access" {
    try std.testing.fuzz({}, fuzzTestOnePathState, .{
        .corpus = &.{ "abcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*()", "\x00\x04\x08\x0c\x10\x14\x18\x1c\x03\x07\x0b\x0f" },
    });
}

/// Worker function for ReplLog fuzz test — producer side.  Enqueues entries
/// whose op and path are derived from fuzz input bytes.
fn fuzzReplLogProducer(rl: *ReplLog, input: []const u8) void {
    const paths = [_][]const u8{ "p.txt", "q.txt", "r/s.txt" };
    for (input) |byte| {
        const op: ReplOp = if (byte & 1 == 0) .put else .delete;
        const path = paths[byte % paths.len];
        rl.enqueue(op, path) catch continue;
    }
}

/// Worker function for ReplLog fuzz test — consumer side.  Dequeues and
/// marks entries completed until shutdown is signalled.
fn fuzzReplLogConsumer(rl: *ReplLog) void {
    while (!rl.shutdown.load(.acquire)) {
        const work = rl.dequeueNext() orelse break;
        rl.markCompleted(work.id);
    }
}

fn fuzzTestOneReplLog(_: void, input: []const u8) anyerror!void {
    var h = try TestHarness.init();
    defer h.deinit();

    // Reset shutdown so consumers will block
    h.state.shutdown.store(false, .release);

    const producer_count = 4;
    const consumer_count = 4;
    const total = producer_count + consumer_count;
    var threads: [total]std.Thread = undefined;
    var started: usize = 0;

    // Spawn consumers first (they will block on the condition variable)
    for (0..consumer_count) |i| {
        threads[started] = std.Thread.spawn(.{}, fuzzReplLogConsumer, .{&h.state.repl_log}) catch
            return error.ThreadSpawnFailed;
        started += 1;
        _ = i;
    }

    // Spawn producers — split fuzz input among them
    for (0..producer_count) |i| {
        const chunk_start = if (input.len > 0) (i * input.len / producer_count) else 0;
        const chunk_end = if (input.len > 0) ((i + 1) * input.len / producer_count) else 0;
        const chunk = if (chunk_end > chunk_start) input[chunk_start..chunk_end] else input[0..0];
        threads[started] = std.Thread.spawn(.{}, fuzzReplLogProducer, .{ &h.state.repl_log, chunk }) catch
            return error.ThreadSpawnFailed;
        started += 1;
    }

    // Wait for producers to finish
    for (threads[consumer_count..started]) |t| t.join();

    // Signal shutdown and wake consumers so they exit
    h.state.shutdown.store(true, .release);
    {
        h.state.repl_log.mutex.lock();
        defer h.state.repl_log.mutex.unlock();
        h.state.repl_log.cond.broadcast();
    }

    // Wait for consumers
    for (threads[0..consumer_count]) |t| t.join();
}

test "fuzz: ReplLog concurrent enqueue/dequeue/complete" {
    try std.testing.fuzz({}, fuzzTestOneReplLog, .{
        .corpus = &.{ "abcdefghijklmnopqrstuvwxyz", "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b" },
    });
}

/// Worker for the end-to-end fuzz test.  Each byte drives a file operation
/// (create + checksum+enqueue, replicate, or scrub) on a small set of paths.
fn fuzzE2eWorker(state: *FsState, backing_dir: []const u8, input: []const u8) void {
    const paths = [_][]const u8{ "f1.txt", "f2.txt", "d/f3.txt" };
    const allocator = state.allocator;

    for (input) |byte| {
        const rel_path = paths[byte % paths.len];
        switch ((byte / 3) % 4) {
            0 => {
                // Create / overwrite a backing file, mark dirty, checksum+enqueue
                const full = std.fs.path.join(allocator, &.{ backing_dir, rel_path }) catch continue;
                defer allocator.free(full);
                replication.ensureParentDir(full) catch continue;
                const f = std.fs.createFileAbsolute(full, .{}) catch continue;
                // Write a few bytes so the checksum is non-trivial
                f.writeAll("fuzz-data") catch {};
                f.close();
                state.path_state.setDirty(rel_path);
                state_mod.checksumAndEnqueue(state, rel_path) catch {};
            },
            1 => {
                // Attempt replicatePut (may legitimately fail if the file
                // doesn't exist yet — that's fine)
                replication.replicatePut(state, rel_path) catch {};
            },
            2 => {
                // Attempt replicateDelete
                replication.replicateDelete(state, rel_path) catch {};
            },
            3 => {
                // Probe path-state methods
                state.path_state.setDirty(rel_path);
                _ = state.path_state.isDirty(rel_path);
                state.path_state.clearDirty(rel_path);
            },
            else => unreachable,
        }
    }
}

fn fuzzTestOneE2e(_: void, input: []const u8) anyerror!void {
    var h = try TestHarness.init();
    defer h.deinit();

    const thread_count = 6;
    var threads: [thread_count]std.Thread = undefined;
    var started: usize = 0;

    for (0..thread_count) |i| {
        const chunk_start = if (input.len > 0) (i * input.len / thread_count) else 0;
        const chunk_end = if (input.len > 0) ((i + 1) * input.len / thread_count) else 0;
        const chunk = if (chunk_end > chunk_start) input[chunk_start..chunk_end] else input[0..0];
        threads[i] = std.Thread.spawn(.{}, fuzzE2eWorker, .{ h.state, h.backing_dir, chunk }) catch
            return error.ThreadSpawnFailed;
        started += 1;
    }

    for (threads[0..started]) |t| t.join();
}

test "fuzz: end-to-end concurrent file operations" {
    try std.testing.fuzz({}, fuzzTestOneE2e, .{
        .corpus = &.{ "abcdefghijklmnopqrstuvwxyz0123456789", "\x00\x03\x06\x09\x01\x04\x07\x0a\x02\x05\x08\x0b" },
    });
}
