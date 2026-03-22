const std = @import("std");
const helpers = @import("helpers.zig");
const state_mod = @import("state.zig");
const replication = @import("replication.zig");
const c = helpers.c;
const log = helpers.log;
const FsState = state_mod.FsState;

pub fn scrubLoop(state: *FsState) void {
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

pub fn shouldScrubImmediately(state: *FsState) bool {
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

pub fn nsUntilNextScrub(target_hour: u8, target_minute: u8) u64 {
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

pub fn runScrub(state: *FsState) void {
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

pub fn scrubFile(state: *FsState, rel_path: []const u8, corruptions: *u64, repairs_count: *u64) !void {
    const backing_path = try std.fs.path.join(state.allocator, &.{ state.backing_dir, rel_path });
    defer state.allocator.free(backing_path);
    const sum_path = try std.fmt.allocPrint(state.allocator, "{s}.sum", .{backing_path});
    defer state.allocator.free(sum_path);

    const current_hex = helpers.computeBlake3(backing_path) catch |err| {
        log.err("scrub: failed to compute checksum for {s}: {}", .{ rel_path, err });
        return err;
    };

    const stored_hex = helpers.readSumFile(state.allocator, sum_path) catch |err| switch (err) {
        error.FileNotFound => {
            log.info("scrub: adopting untracked file {s}", .{rel_path});
            helpers.writeSumFile(sum_path, &current_hex) catch |we| {
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

    const replica_hex = helpers.readSumFile(state.allocator, replica_sum_path) catch {
        log.err("scrub: replica unavailable for repair of {s}", .{rel_path});
        return;
    };
    defer state.allocator.free(replica_hex);

    const replica_computed = helpers.computeBlake3(replica_path) catch {
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

    replication.copyFileWithSync(replica_path, backing_path) catch |err| {
        log.err("scrub: failed to repair {s}: {}", .{ rel_path, err });
        return;
    };
    helpers.writeSumFile(sum_path, &replica_computed) catch |err| {
        log.err("scrub: failed to write .sum after repair of {s}: {}", .{ rel_path, err });
        return;
    };
    repairs_count.* += 1;
    log.info("scrub: successfully repaired {s}", .{rel_path});
}

pub fn writeScrubTimestamp(state: *FsState, ts: i64) void {
    const ts_path = std.fs.path.join(state.allocator, &.{ state.backing_dir, ".helmetfs", "scrub.timestamp" }) catch return;
    defer state.allocator.free(ts_path);
    const file = std.fs.createFileAbsolute(ts_path, .{}) catch return;
    defer file.close();
    const ts_str = std.fmt.allocPrint(state.allocator, "{d}\n", .{ts}) catch return;
    defer state.allocator.free(ts_str);
    file.writeAll(ts_str) catch return;
    file.sync() catch return;
}

test "nsUntilNextScrub returns value in (0, 24h]" {
    const testing = std.testing;
    for ([_]u8{ 0, 3, 6, 12, 18, 23 }) |hour| {
        const ns = nsUntilNextScrub(hour, 0);
        try testing.expect(ns > 0);
        try testing.expect(ns <= 86400 * 1_000_000_000);
    }
}
