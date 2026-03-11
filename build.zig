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

    // Fuzz tests — must use the LLVM backend so that sanitizer-coverage
    // instrumentation (__sancov_pcs1 / __sancov_cntrs) is emitted.  The
    // self-hosted x86 backend does not provide these sections, which causes
    // the build-runner's fuzz infrastructure to crash (ziglang/zig#23423).
    // ReleaseSafe also gives much better fuzzing throughput.
    const fuzz_tests = b.addTest(.{
        .root_module = createModule(b, target, .ReleaseSafe),
        .filters = &.{"fuzz:"},
        .use_llvm = true,
    });

    const run_fuzz_tests = b.addRunArtifact(fuzz_tests);
    const fuzz_step = b.step("fuzz", "Run fuzz tests (use with --fuzz)");
    fuzz_step.dependOn(&run_fuzz_tests.step);
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
