const std = @import("std");
const builtin = @import("builtin");
const helpers = @import("helpers.zig");
const state_mod = @import("state.zig");
const replication = @import("replication.zig");
const posix = helpers.posix;
const c = helpers.c;
const log = helpers.log;
const FsState = state_mod.FsState;

// macFUSE bitfields are opaque to cImport; we define ABI-compatible structs.
pub const FuseFileInfo = extern struct {
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

pub const LibfuseVersion = extern struct {
    major: u32,
    minor: u32,
    hotfix: u32,
    flags: u32,
};

pub fn fuseNew(args: [*c]c.struct_fuse_args, ops: [*c]const c.struct_fuse_operations, op_size: usize, user_data: ?*anyopaque) ?*c.struct_fuse {
    if (comptime builtin.os.tag == .macos) {
        var version = LibfuseVersion{ .major = 3, .minor = 17, .hotfix = 0, .flags = 0 };
        return c._fuse_new_31(args, ops, op_size, @ptrCast(&version), user_data);
    }
    return c.fuse_new(args, ops, op_size, user_data);
}

fn fuse_getattr(path: [*c]const u8, stbuf: [*c]c.struct_stat, fi: ?*c.struct_fuse_file_info) callconv(.c) c_int {
    _ = fi;
    const state = state_mod.g_state;
    const rel = helpers.fuseRelPath(path);

    if (rel.len > 0 and helpers.isHiddenPath(state.allocator, state.backing_dir, rel)) {
        return helpers.fuseErr(.NOENT);
    }

    const backing = helpers.backingPath(state.allocator, state.backing_dir, rel) catch return helpers.fuseErr(.NOMEM);
    defer state.allocator.free(backing);

    const stat_val = posix.fstatat(posix.AT.FDCWD, backing, posix.AT.SYMLINK_NOFOLLOW) catch |err| return helpers.posixErr(err);
    const buf: *posix.Stat = @ptrCast(@alignCast(stbuf));
    buf.* = stat_val;
    return 0;
}

fn fuse_readdir(path: [*c]const u8, buf: ?*anyopaque, filler: c.fuse_fill_dir_t, _: c.off_t, _: ?*c.struct_fuse_file_info, _: c.enum_fuse_readdir_flags) callconv(.c) c_int {
    const state = state_mod.g_state;
    const rel = helpers.fuseRelPath(path);

    const backing = helpers.backingPath(state.allocator, state.backing_dir, rel) catch return helpers.fuseErr(.NOMEM);
    defer state.allocator.free(backing);

    var dir = std.fs.openDirAbsoluteZ(backing, .{ .iterate = true }) catch |err| return helpers.posixErr(err);
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

        if (helpers.isHiddenPath(state.allocator, state.backing_dir, entry_rel)) continue;

        const name_z = state.allocator.dupeZ(u8, entry.name) catch continue;
        defer state.allocator.free(name_z);
        _ = filler.?(buf, name_z.ptr, null, 0, 0);
    }
    return 0;
}

fn fuse_open(path: [*c]const u8, fi: ?*c.struct_fuse_file_info) callconv(.c) c_int {
    const state = state_mod.g_state;
    const rel = helpers.fuseRelPath(path);

    if (rel.len > 0 and helpers.isHiddenPath(state.allocator, state.backing_dir, rel)) {
        return helpers.fuseErr(.NOENT);
    }

    const backing = helpers.backingPath(state.allocator, state.backing_dir, rel) catch return helpers.fuseErr(.NOMEM);
    defer state.allocator.free(backing);

    const flags: c_int = if (castFi(fi)) |f| f.flags else 0;
    const fd = posix.openZ(backing, @bitCast(flags), 0) catch |err| return helpers.posixErr(err);

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
        return helpers.fuseErr(.BADF);
    }
    return 0;
}

fn fuse_read(path: [*c]const u8, buf_ptr: [*c]u8, size: usize, offset: c.off_t, fi: ?*c.struct_fuse_file_info) callconv(.c) c_int {
    _ = path;
    const fd: posix.fd_t = if (castFi(fi)) |f| decodeFh(f.fh).fd else return helpers.fuseErr(.BADF);
    const n = posix.pread(fd, buf_ptr[0..size], @intCast(offset)) catch {
        return helpers.fuseErr(.IO);
    };
    return @intCast(n);
}

fn fuse_write(path: [*c]const u8, data: [*c]const u8, size: usize, offset: c.off_t, fi: ?*c.struct_fuse_file_info) callconv(.c) c_int {
    const state = state_mod.g_state;
    const rel = helpers.fuseRelPath(path);
    const fd: posix.fd_t = if (castFi(fi)) |f| decodeFh(f.fh).fd else return helpers.fuseErr(.BADF);
    const n = posix.pwrite(fd, @as([*]const u8, @ptrCast(data))[0..size], @intCast(offset)) catch {
        return helpers.fuseErr(.IO);
    };
    if (rel.len > 0) {
        state.path_state.setDirty(rel);
    }
    return @intCast(n);
}

fn fuse_fsync(path: [*c]const u8, datasync: c_int, fi: ?*c.struct_fuse_file_info) callconv(.c) c_int {
    const state = state_mod.g_state;
    const rel = helpers.fuseRelPath(path);

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
        state_mod.checksumAndEnqueueForced(state, rel) catch |err| {
            log.err("fsync checksum failed for {s}: {}", .{ rel, err });
            return helpers.fuseErr(.IO);
        };
    }
    return 0;
}

fn fuse_release(path: [*c]const u8, fi: ?*c.struct_fuse_file_info) callconv(.c) c_int {
    const state = state_mod.g_state;
    const rel = helpers.fuseRelPath(path);

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
        state_mod.checksumAndEnqueue(state, rel) catch |err| {
            log.err("release checksum failed for {s}: {}", .{ rel, err });
        };
    }
    return 0;
}

fn fuse_create(path: [*c]const u8, mode: c.mode_t, fi: ?*c.struct_fuse_file_info) callconv(.c) c_int {
    const state = state_mod.g_state;
    const rel = helpers.fuseRelPath(path);

    if (rel.len > 0 and helpers.isHiddenPath(state.allocator, state.backing_dir, rel)) {
        return helpers.fuseErr(.NOENT);
    }

    const backing = helpers.backingPath(state.allocator, state.backing_dir, rel) catch return helpers.fuseErr(.NOMEM);
    defer state.allocator.free(backing);

    const flags: c_int = if (castFi(fi)) |f| f.flags else 0;
    const fd = posix.openZ(backing, @bitCast(flags), mode) catch |err| return helpers.posixErr(err);

    if (castFi(fi)) |f| {
        f.fh = encodeFh(fd, true);
        state.path_state.incWriteRef(rel);
    } else {
        posix.close(fd);
        return helpers.fuseErr(.BADF);
    }
    return 0;
}

fn fuse_unlink(path: [*c]const u8) callconv(.c) c_int {
    const state = state_mod.g_state;
    const rel = helpers.fuseRelPath(path);

    if (rel.len > 0 and helpers.isHiddenPath(state.allocator, state.backing_dir, rel)) {
        return helpers.fuseErr(.NOENT);
    }

    const backing = helpers.backingPath(state.allocator, state.backing_dir, rel) catch return helpers.fuseErr(.NOMEM);
    defer state.allocator.free(backing);

    std.fs.deleteFileAbsolute(backing) catch |err| return helpers.posixErr(err);

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
    const state = state_mod.g_state;
    const rel_from = helpers.fuseRelPath(from);
    const rel_to = helpers.fuseRelPath(to);

    if (comptime builtin.os.tag == .linux) {
        const RENAME_NOREPLACE = 1;
        const RENAME_EXCHANGE = 2;
        if (flags & ~@as(c_uint, RENAME_NOREPLACE) != 0) {
            if (flags & RENAME_EXCHANGE != 0) {
                return helpers.fuseErr(.OPNOTSUPP);
            }
            return helpers.fuseErr(.INVAL);
        }
        if (flags & RENAME_NOREPLACE != 0) {
            const backing_to_check = helpers.backingPath(state.allocator, state.backing_dir, rel_to) catch return helpers.fuseErr(.NOMEM);
            defer state.allocator.free(backing_to_check);
            if (posix.fstatat(posix.AT.FDCWD, backing_to_check, posix.AT.SYMLINK_NOFOLLOW)) |_| {
                return helpers.fuseErr(.EXIST);
            } else |_| {}
        }
    } else {
        if (flags != 0) {
            return helpers.fuseErr(.OPNOTSUPP);
        }
    }

    if (rel_from.len > 0 and helpers.isHiddenPath(state.allocator, state.backing_dir, rel_from)) {
        return helpers.fuseErr(.NOENT);
    }
    if (rel_to.len > 0 and helpers.isHiddenPath(state.allocator, state.backing_dir, rel_to)) {
        return helpers.fuseErr(.NOENT);
    }

    const backing_from = helpers.backingPath(state.allocator, state.backing_dir, rel_from) catch return helpers.fuseErr(.NOMEM);
    defer state.allocator.free(backing_from);
    const backing_to = helpers.backingPath(state.allocator, state.backing_dir, rel_to) catch return helpers.fuseErr(.NOMEM);
    defer state.allocator.free(backing_to);

    std.fs.renameAbsolute(backing_from, backing_to) catch |err| return helpers.posixErr(err);

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
    const state = state_mod.g_state;
    const rel = helpers.fuseRelPath(path);

    if (rel.len > 0 and helpers.isHiddenPath(state.allocator, state.backing_dir, rel)) {
        return helpers.fuseErr(.NOENT);
    }

    const backing = helpers.backingPath(state.allocator, state.backing_dir, rel) catch return helpers.fuseErr(.NOMEM);
    defer state.allocator.free(backing);

    std.fs.makeDirAbsolute(backing) catch |err| return helpers.posixErr(err);

    posix.fchmodat(posix.AT.FDCWD, backing, mode, 0) catch {};

    if (!state.no_remote_mkdir and rel.len > 0) {
        const replica_path = std.fs.path.join(state.allocator, &.{ state.replica_dir, "files", rel }) catch return 0;
        defer state.allocator.free(replica_path);
        replication.ensureParentDir(replica_path) catch {};
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
    const state = state_mod.g_state;
    const rel = helpers.fuseRelPath(path);

    const backing = helpers.backingPath(state.allocator, state.backing_dir, rel) catch return helpers.fuseErr(.NOMEM);
    defer state.allocator.free(backing);

    std.fs.deleteDirAbsolute(backing) catch |err| return helpers.posixErr(err);

    if (!state.no_remote_mkdir and rel.len > 0) {
        const replica_path = std.fs.path.join(state.allocator, &.{ state.replica_dir, "files", rel }) catch return 0;
        defer state.allocator.free(replica_path);
        std.fs.deleteDirAbsolute(replica_path) catch {};
    }
    return 0;
}

fn fuse_symlink(target: [*c]const u8, linkpath: [*c]const u8) callconv(.c) c_int {
    const state = state_mod.g_state;
    const rel = helpers.fuseRelPath(linkpath);

    const backing = helpers.backingPath(state.allocator, state.backing_dir, rel) catch return helpers.fuseErr(.NOMEM);
    defer state.allocator.free(backing);

    posix.symlinkat(std.mem.span(@as([*:0]const u8, @ptrCast(target))), posix.AT.FDCWD, backing) catch |err| return helpers.posixErr(err);

    if (rel.len > 0) {
        state.repl_log.enqueue(.put, rel) catch |err| {
            log.err("failed to enqueue symlink replication for {s}: {}", .{ rel, err });
        };
    }
    return 0;
}

fn fuse_readlink(path: [*c]const u8, buf_ptr: [*c]u8, size: usize) callconv(.c) c_int {
    const state = state_mod.g_state;
    const rel = helpers.fuseRelPath(path);

    if (rel.len > 0 and helpers.isHiddenPath(state.allocator, state.backing_dir, rel)) {
        return helpers.fuseErr(.NOENT);
    }

    const backing = helpers.backingPath(state.allocator, state.backing_dir, rel) catch return helpers.fuseErr(.NOMEM);
    defer state.allocator.free(backing);

    const buf: [*]u8 = @ptrCast(buf_ptr);
    const target = posix.readlinkat(posix.AT.FDCWD, backing, buf[0 .. size - 1]) catch |err| return helpers.posixErr(err);
    buf[target.len] = 0;

    return 0;
}

fn fuse_chmod(path: [*c]const u8, mode: c.mode_t, fi: ?*c.struct_fuse_file_info) callconv(.c) c_int {
    _ = fi;
    const state = state_mod.g_state;
    const rel = helpers.fuseRelPath(path);

    const backing = helpers.backingPath(state.allocator, state.backing_dir, rel) catch return helpers.fuseErr(.NOMEM);
    defer state.allocator.free(backing);

    posix.fchmodat(posix.AT.FDCWD, backing, mode, 0) catch return helpers.fuseErr(.IO);

    if (rel.len > 0) {
        state.repl_log.enqueue(.put, rel) catch |err| {
            log.err("failed to enqueue chmod replication for {s}: {}", .{ rel, err });
        };
    }
    return 0;
}

fn fuse_chown(path: [*c]const u8, uid: c.uid_t, gid: c.gid_t, fi: ?*c.struct_fuse_file_info) callconv(.c) c_int {
    _ = fi;
    const state = state_mod.g_state;
    const rel = helpers.fuseRelPath(path);

    const backing = helpers.backingPath(state.allocator, state.backing_dir, rel) catch return helpers.fuseErr(.NOMEM);
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
    const state = state_mod.g_state;
    const rel = helpers.fuseRelPath(path);

    if (castFi(fi)) |f| {
        const fd: posix.fd_t = decodeFh(f.fh).fd;
        posix.ftruncate(fd, @intCast(size)) catch return helpers.fuseErr(.IO);
    } else {
        const backing = helpers.backingPath(state.allocator, state.backing_dir, rel) catch return helpers.fuseErr(.NOMEM);
        defer state.allocator.free(backing);
        const file = std.fs.openFileAbsolute(backing, .{ .mode = .read_write }) catch return helpers.fuseErr(.NOENT);
        defer file.close();
        posix.ftruncate(file.handle, @intCast(size)) catch return helpers.fuseErr(.IO);
    }

    // For the non-fi path (truncate without an open fd), there is no
    // subsequent fuse_release, so we must checksum here.
    if (rel.len > 0) {
        state.path_state.setDirty(rel);
        state_mod.checksumAndEnqueue(state, rel) catch |err| {
            log.err("truncate checksum failed for {s}: {}", .{ rel, err });
        };
    }
    return 0;
}

fn fuse_utimens(path: [*c]const u8, tv: [*c]const c.struct_timespec, fi: ?*c.struct_fuse_file_info) callconv(.c) c_int {
    _ = fi;
    const state = state_mod.g_state;
    const rel = helpers.fuseRelPath(path);

    const backing = helpers.backingPath(state.allocator, state.backing_dir, rel) catch return helpers.fuseErr(.NOMEM);
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
    const state = state_mod.g_state;
    const rel = helpers.fuseRelPath(path);

    const backing = helpers.backingPath(state.allocator, state.backing_dir, rel) catch return helpers.fuseErr(.NOMEM);
    defer state.allocator.free(backing);

    if (@as(?*c.struct_statvfs, stbuf)) |buf| {
        const ret = c.statvfs(backing.ptr, buf);
        if (ret != 0) {
            return helpers.fuseErr(.IO);
        }
    }
    return 0;
}

fn fuse_access(path: [*c]const u8, mask: c_int) callconv(.c) c_int {
    const state = state_mod.g_state;
    const rel = helpers.fuseRelPath(path);

    if (rel.len > 0 and helpers.isHiddenPath(state.allocator, state.backing_dir, rel)) {
        return helpers.fuseErr(.NOENT);
    }

    const backing = helpers.backingPath(state.allocator, state.backing_dir, rel) catch return helpers.fuseErr(.NOMEM);
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
    state_mod.g_state.flushDirtyFiles();
    state_mod.g_state.stopWorkers();
}

fn fuse_init(_: ?*c.struct_fuse_conn_info, _: [*c]c.struct_fuse_config) callconv(.c) ?*anyopaque {
    return null;
}

pub const fuse_ops = std.mem.zeroInit(c.struct_fuse_operations, .{
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
