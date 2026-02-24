const std = @import("std");
const tokamak = @import("tokamak");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // --- Executable ---
    const exe = b.addExecutable(.{
        .name = "bleepstore",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        }),
    });

    exe.linkSystemLibrary("sqlite3");
    tokamak.setup(exe, .{});

    b.installArtifact(exe);

    // --- Run step ---
    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const run_step = b.step("run", "Run the BleepStore server");
    run_step.dependOn(&run_cmd.step);

    // --- Tests ---
    const unit_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        }),
    });

    unit_tests.linkSystemLibrary("sqlite3");
    tokamak.setup(unit_tests, .{});

    const run_unit_tests = b.addRunArtifact(unit_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_unit_tests.step);

    // --- E2E Integration Tests ---
    // Builds and runs a standalone HTTP client that exercises the S3 API
    // against a running server on port 9013. Start the server first!
    const e2e_exe = b.addExecutable(.{
        .name = "e2e_test",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/e2e_test.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const run_e2e = b.addRunArtifact(e2e_exe);
    run_e2e.step.dependOn(&e2e_exe.step);

    const e2e_step = b.step("e2e", "Run E2E integration tests (server must be running on port 9013)");
    e2e_step.dependOn(&run_e2e.step);

}
