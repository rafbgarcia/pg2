const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Shared pg2 module — all source files import through this.
    const pg2_mod = b.createModule(.{
        .root_source_file = b.path("src/pg2.zig"),
        .target = target,
        .optimize = optimize,
    });
    const test_shared_mod = b.createModule(.{
        .root_source_file = b.path("test/harness/feature_env.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "pg2", .module = pg2_mod },
        },
    });

    // --- Main executable ---
    const exe_mod = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "pg2", .module = pg2_mod },
        },
    });
    const exe = b.addExecutable(.{
        .name = "pg2",
        .root_module = exe_mod,
    });
    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }
    const run_step = b.step("run", "Run pg2");
    run_step.dependOn(&run_cmd.step);

    // --- Fast test lane (features + internals + sim) ---
    const test_step = b.step("test", "Run fast lane tests (features, internals, sim)");

    const all_tests_mod = b.createModule(.{
        .root_source_file = b.path("test/all_tests_test.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "pg2", .module = pg2_mod },
            .{ .name = "test_shared", .module = test_shared_mod },
        },
    });
    const t = b.addTest(.{
        .root_module = all_tests_mod,
    });
    const run_t = b.addRunArtifact(t);
    test_step.dependOn(&run_t.step);

    // --- Stress tests ---
    const stress_tests_mod = b.createModule(.{
        .root_source_file = b.path("test/stress/stress_specs_test.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "pg2", .module = pg2_mod },
            .{ .name = "test_shared", .module = test_shared_mod },
        },
    });
    const stress_t = b.addTest(.{
        .root_module = stress_tests_mod,
    });
    const run_stress_t = b.addRunArtifact(stress_t);
    const stress_step = b.step("stress", "Run stress tests");
    stress_step.dependOn(&run_stress_t.step);

    // --- Simulation tests ---
    const sim_test_mod = b.createModule(.{
        .root_source_file = b.path("test/sim/sim_specs_test.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "pg2", .module = pg2_mod },
        },
    });
    const sim_t = b.addTest(.{
        .root_module = sim_test_mod,
    });
    const run_sim_t = b.addRunArtifact(sim_t);
    const sim_step = b.step("sim", "Run deterministic simulation tests");
    sim_step.dependOn(&run_sim_t.step);
    test_step.dependOn(&run_sim_t.step);

    // --- Simulation executable ---
    const sim_mod = b.createModule(.{
        .root_source_file = b.path("src/simulator/main.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "pg2", .module = pg2_mod },
        },
    });
    const sim = b.addExecutable(.{
        .name = "pg2-sim",
        .root_module = sim_mod,
    });
    b.installArtifact(sim);

    const sim_cmd = b.addRunArtifact(sim);
    sim_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        sim_cmd.addArgs(args);
    }
    const sim_run_step = b.step("sim-run", "Run simulator executable");
    sim_run_step.dependOn(&sim_cmd.step);
}
