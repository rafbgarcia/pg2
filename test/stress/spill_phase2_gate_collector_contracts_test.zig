//! Phase 2 gate integration tests for spill collector semantics.
const std = @import("std");
const spill = @import("spill_phase2_gate_helpers.zig");

const FeatureEnv = spill.FeatureEnv;
const spill_boundary_row_count = spill.spill_boundary_row_count;
const runWithBuffer = spill.runWithBuffer;
const insertRows = spill.insertRows;
const insertRowsWithScore = spill.insertRowsWithScore;

test "collector-backed spill path applies limit correctly" {
    var env: FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\LimitSpillTable {
        \\  field(id, i64, notNull, primaryKey)
        \\}
    );

    try insertRows(executor, "LimitSpillTable", spill_boundary_row_count);

    const result = try executor.run(
        "LimitSpillTable |> limit(10) |> inspect {}",
    );

    try std.testing.expect(std.mem.startsWith(u8, result, "OK returned_rows=10 "));
    try std.testing.expect(
        std.mem.indexOf(
            u8,
            result,
            "spill_triggered=true",
        ) != null,
    );
    try std.testing.expect(std.mem.indexOf(u8, result, "\n1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n") != null);
}

test "collector-backed external sort spill applies limit correctly" {
    var env: FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\SortLimitSpillTable {
        \\  field(id, i64, notNull, primaryKey)
        \\}
    );

    try insertRows(executor, "SortLimitSpillTable", spill_boundary_row_count);

    const result = try executor.run(
        "SortLimitSpillTable |> sort(id desc) |> limit(10) |> inspect {}",
    );

    try std.testing.expect(std.mem.startsWith(u8, result, "OK returned_rows=10 "));
    try std.testing.expect(
        std.mem.indexOf(
            u8,
            result,
            "spill_triggered=true",
        ) != null,
    );
    try std.testing.expect(std.mem.indexOf(u8, result, "\n4097\n4096\n4095\n4094\n4093\n4092\n4091\n4090\n4089\n4088\n") != null);
}

test "collector-backed spill path applies offset then limit correctly" {
    var env: FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\OffsetLimitSpillTable {
        \\  field(id, i64, notNull, primaryKey)
        \\}
    );

    try insertRows(executor, "OffsetLimitSpillTable", spill_boundary_row_count);

    const result = try executor.run(
        "OffsetLimitSpillTable |> offset(100) |> limit(5) |> inspect {}",
    );

    try std.testing.expect(std.mem.startsWith(u8, result, "OK returned_rows=5 "));
    try std.testing.expect(std.mem.indexOf(u8, result, "\n101\n102\n103\n104\n105\n") != null);
}

test "collector-backed spill path applies flat column projection correctly" {
    var env: FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\ProjectionSpillTable {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(score, i64, notNull)
        \\}
    );

    try insertRowsWithScore(
        executor,
        "ProjectionSpillTable",
        spill_boundary_row_count,
        2,
    );

    const result = try executor.run(
        "ProjectionSpillTable |> limit(3) |> inspect { score }",
    );

    try std.testing.expect(std.mem.startsWith(u8, result, "OK returned_rows=3 "));
    try std.testing.expect(std.mem.indexOf(u8, result, "\n2\n4\n6\n") != null);
}

test "collector-backed spill path applies computed projection correctly" {
    var env: FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\ComputedProjectionSpillTable {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(score, i64, notNull)
        \\}
    );

    try insertRowsWithScore(
        executor,
        "ComputedProjectionSpillTable",
        spill_boundary_row_count,
        10,
    );

    const result = try executor.run(
        "ComputedProjectionSpillTable |> offset(2) |> limit(2) |> inspect { plus_one: score + 1 }",
    );

    try std.testing.expect(std.mem.startsWith(u8, result, "OK returned_rows=2 "));
    try std.testing.expect(std.mem.indexOf(u8, result, "\n31\n41\n") != null);
}

test "collector-backed spill path applies having correctly" {
    var env: FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\HavingSpillTable {
        \\  field(id, i64, notNull, primaryKey)
        \\}
    );

    try insertRows(executor, "HavingSpillTable", spill_boundary_row_count);

    var large_buf: [64 * 1024]u8 = undefined;
    const result = try runWithBuffer(
        executor,
        "HavingSpillTable |> having(id > 100) |> inspect { id }",
        &large_buf,
    );

    try std.testing.expect(std.mem.startsWith(u8, result, "OK returned_rows=3997 "));
    try std.testing.expect(
        std.mem.indexOf(
            u8,
            result,
            "\n101\n102\n103\n",
        ) != null,
    );
    try std.testing.expect(std.mem.indexOf(u8, result, "\n4097\n") != null);
}

test "collector-backed spill path preserves having-limit order semantics" {
    var env: FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\HavingLimitOrderTable {
        \\  field(id, i64, notNull, primaryKey)
        \\}
    );

    try insertRows(executor, "HavingLimitOrderTable", spill_boundary_row_count);

    const result_a = try executor.run(
        "HavingLimitOrderTable |> having(id > 4000) |> limit(3) |> inspect { id }",
    );
    try std.testing.expect(std.mem.startsWith(u8, result_a, "OK returned_rows=3 "));
    try std.testing.expect(std.mem.indexOf(u8, result_a, "\n4001\n4002\n4003\n") != null);

    const result_b = try executor.run(
        "HavingLimitOrderTable |> limit(3) |> having(id > 4000) |> inspect { id }",
    );
    try std.testing.expect(std.mem.startsWith(u8, result_b, "OK returned_rows=0 "));
}

test "collector-backed external sort spill applies having correctly" {
    var env: FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\SortHavingSpillTable {
        \\  field(id, i64, notNull, primaryKey)
        \\}
    );

    try insertRows(executor, "SortHavingSpillTable", spill_boundary_row_count);

    const result = try executor.run(
        "SortHavingSpillTable |> sort(id desc) |> having(id > 4094) |> inspect { id }",
    );

    try std.testing.expect(std.mem.startsWith(u8, result, "OK returned_rows=3 "));
    try std.testing.expect(std.mem.indexOf(u8, result, "\n4097\n4096\n4095\n") != null);
}
