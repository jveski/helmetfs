const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const root_module = createModule(b, target, optimize);

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
    const unit_tests = b.addTest(.{
        .root_module = createModule(b, target, optimize),
    });

    const run_unit_tests = b.addRunArtifact(unit_tests);
    const test_step = b.step("test", "Run unit and integration tests");
    test_step.dependOn(&run_unit_tests.step);
}

fn createModule(b: *std.Build, target: std.Build.ResolvedTarget, optimize: std.builtin.OptimizeMode) *std.Build.Module {
    const module = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });

    if (target.result.os.tag == .macos) {
        module.addIncludePath(.{ .cwd_relative = "/usr/local/include" });
        module.addLibraryPath(.{ .cwd_relative = "/usr/local/lib" });
    }
    module.linkSystemLibrary("fuse3", .{});

    return module;
}
