const std = @import("std");
const helpers = @import("helpers.zig");
const state_mod = @import("state.zig");
const posix = helpers.posix;
const c = helpers.c;
const log = helpers.log;
const FsState = state_mod.FsState;

pub fn replWorkerLoop(state: *FsState) void {
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

pub fn replicatePut(state: *FsState, rel_path: []const u8) !void {
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

    if (helpers.readSumFile(state.allocator, sum_backing)) |stored_hex| {
        defer state.allocator.free(stored_hex);
        if (helpers.computeBlake3(backing_path)) |computed_hex| {
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

pub fn replicateDelete(state: *FsState, rel_path: []const u8) !void {
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

pub fn copyFileWithSync(src_path: []const u8, dst_path: []const u8) !void {
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
        helpers.fsyncDir(dir_path);
    }
}

pub fn ensureParentDir(path: []const u8) !void {
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

pub fn removeEmptyParentDirs(path: []const u8, stop_at: []const u8) void {
    var current = std.fs.path.dirname(path);
    while (current) |dir| {
        if (dir.len <= stop_at.len) break;
        std.fs.deleteDirAbsolute(dir) catch break;
        current = std.fs.path.dirname(dir);
    }
}
