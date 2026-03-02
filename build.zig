const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const root_module = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });

    // Link libfuse3 (Linux) or macFUSE (macOS)
    const os_tag = target.result.os.tag;
    if (os_tag == .macos) {
        root_module.addIncludePath(.{ .cwd_relative = "/usr/local/include" });
        root_module.addLibraryPath(.{ .cwd_relative = "/usr/local/lib" });
        root_module.linkSystemLibrary("fuse3", .{});
    } else {
        root_module.linkSystemLibrary("fuse3", .{});
    }

    const exe = b.addExecutable(.{
        .name = "helmetfs",
        .root_module = root_module,
    });

    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const run_step = b.step("run", "Run helmetfs");
    run_step.dependOn(&run_cmd.step);

    // Unit / integration tests
    const test_mod = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    if (os_tag == .macos) {
        test_mod.addIncludePath(.{ .cwd_relative = "/usr/local/include" });
        test_mod.addLibraryPath(.{ .cwd_relative = "/usr/local/lib" });
        test_mod.linkSystemLibrary("fuse3", .{});
    } else {
        test_mod.linkSystemLibrary("fuse3", .{});
    }

    const unit_tests = b.addTest(.{
        .root_module = test_mod,
    });

    const run_unit_tests = b.addRunArtifact(unit_tests);
    const test_step = b.step("test", "Run unit and integration tests");
    test_step.dependOn(&run_unit_tests.step);
}
