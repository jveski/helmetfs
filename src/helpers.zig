const std = @import("std");
const builtin = @import("builtin");
pub const posix = std.posix;

pub const c = @cImport({
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

pub const log = std.log.scoped(.helmetfs);

pub fn computeBlake3(backing_path: []const u8) ![64]u8 {
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

pub fn writeSumFile(sum_path: []const u8, hex_digest: []const u8) !void {
    const file = try std.fs.createFileAbsolute(sum_path, .{});
    defer file.close();
    try file.writeAll(hex_digest);
    try file.writeAll("\n");
    try file.sync();
}

pub fn readSumFile(allocator: std.mem.Allocator, sum_path: []const u8) ![]const u8 {
    const file = std.fs.openFileAbsolute(sum_path, .{}) catch |err| return err;
    defer file.close();
    var buf: [128]u8 = undefined;
    const n = try file.readAll(&buf);
    const trimmed = std.mem.trimRight(u8, buf[0..n], "\n\r ");
    return try allocator.dupe(u8, trimmed);
}

pub fn isHiddenPath(allocator: std.mem.Allocator, backing_dir: []const u8, rel_path: []const u8) bool {
    if (std.mem.startsWith(u8, rel_path, ".helmetfs")) return true;

    if (std.mem.endsWith(u8, rel_path, ".sum")) {
        const data_rel = rel_path[0 .. rel_path.len - 4];
        const data_full = std.fs.path.join(allocator, &.{ backing_dir, data_rel }) catch return false;
        defer allocator.free(data_full);
        _ = posix.fstatat(posix.AT.FDCWD, data_full, posix.AT.SYMLINK_NOFOLLOW) catch return false;
        return true;
    }

    return false;
}

pub fn fuseRelPath(path: [*c]const u8) []const u8 {
    const s = std.mem.span(@as([*:0]const u8, @ptrCast(path)));
    if (s.len > 0 and s[0] == '/') return s[1..];
    return s;
}

pub fn backingPath(allocator: std.mem.Allocator, backing_dir: []const u8, rel_path: []const u8) ![:0]const u8 {
    if (rel_path.len == 0) {
        return try allocator.dupeZ(u8, backing_dir);
    }
    const joined = try std.fs.path.join(allocator, &.{ backing_dir, rel_path });
    defer allocator.free(joined);
    return try allocator.dupeZ(u8, joined);
}

pub fn fuseErr(e: posix.E) c_int {
    return -@as(c_int, @intCast(@intFromEnum(e)));
}

pub fn posixErr(err: anytype) c_int {
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

pub fn fsyncDir(dir_path: []const u8) void {
    var dir = std.fs.openDirAbsolute(dir_path, .{}) catch return;
    defer dir.close();
    // Raw syscall avoids unreachable panics on filesystems that return EINVAL for dir fsync.
    _ = std.posix.system.fsync(dir.fd);
}
