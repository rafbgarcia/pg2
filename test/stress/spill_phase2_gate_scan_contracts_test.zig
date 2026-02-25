//! Phase 2 gate integration tests for spill path scan/completeness contracts.
const std = @import("std");
const spill = @import("spill_phase2_gate_helpers.zig");

const FeatureEnv = spill.FeatureEnv;
const spill_boundary_row_count = spill.spill_boundary_row_count;
const runWithBuffer = spill.runWithBuffer;
const insertRows = spill.insertRows;
const insertArenaRows = spill.insertArenaRows;
const countLines = spill.countLines;

test "select all on table with more than 4096 rows returns complete results" {
    var env: FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\BigTable {
        \\  field(id, i64, notNull, primaryKey)
        \\}
    );

    // Insert 4097 rows (exceeds scan_batch_size of 4096, forcing a 2-chunk scan).
    try insertRows(executor, "BigTable", spill_boundary_row_count);

    // Query all rows with a large buffer.
    var large_buf: [64 * 1024]u8 = undefined;
    const result = try runWithBuffer(executor, "BigTable |> inspect { id }", &large_buf);

    // Header reports all rows.
    var header_buf: [64]u8 = undefined;
    const expected_header = try std.fmt.bufPrint(
        &header_buf,
        "OK returned_rows={d} ",
        .{spill_boundary_row_count},
    );
    try std.testing.expect(std.mem.startsWith(u8, result, expected_header));

    // Count data rows between header and INSPECT block.
    const header_end = (std.mem.indexOf(u8, result, "\n") orelse unreachable) + 1;
    const inspect_start = std.mem.indexOf(u8, result, "INSPECT ") orelse result.len;
    const body = result[header_end..inspect_start];
    try std.testing.expectEqual(spill_boundary_row_count, countLines(body));

    // First and last ids present (rows come in insertion order across spill).
    try std.testing.expect(std.mem.startsWith(u8, body, "1\n"));
    var tail_buf: [32]u8 = undefined;
    const expected_tail = try std.fmt.bufPrint(&tail_buf, "{d}\n", .{spill_boundary_row_count});
    try std.testing.expect(std.mem.indexOf(u8, body, expected_tail) != null);

    // Spill triggered (hot batch filled at 4096 rows, forcing a flush).
    try std.testing.expect(std.mem.indexOf(u8, result, "spill_triggered=true") != null);
}

test "query exceeding string arena completes via arena safety valve" {
    // 1 MB per-slot query arena is split by RequestState: 75% statement strings,
    // 25% variable arena. With 180-byte strings, one scan chunk (4096 rows)
    // consumes ~94% of the statement arena, leaving < 10% free. The safety
    // valve force-flushes the collector's hot batch and resets the arena before
    // the second chunk, preventing OutOfMemory during string materialization.
    var env: FeatureEnv = undefined;
    try env.initWithConfig(.{
        .max_query_slots = 1,
        .query_string_arena_bytes_per_slot = 1 * 1024 * 1024,
    });
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\ArenaTable {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(data, string, notNull)
        \\  field(flag, bool, notNull)
        \\}
    );

    const padding = "A" ** 180;

    // Insert 4097 rows (2 scan chunks) with 180-byte strings.
    // Mark the first 4 rows with flag=true, rest with flag=false.
    try insertArenaRows(executor, spill_boundary_row_count, padding);

    // Filter on non-PK column to force a full table scan that fills the arena,
    // while keeping the result set small (4 rows × ~260 bytes).
    const result = try executor.run(
        "ArenaTable |> where(flag == true) |> inspect {}",
    );

    // Query completed without error — the safety valve prevented arena exhaustion.
    try std.testing.expect(std.mem.startsWith(u8, result, "OK "));
    try std.testing.expect(std.mem.indexOf(u8, result, "returned_rows=4 ") != null);

    // The arena safety valve forced a spill flush between scan chunks.
    try std.testing.expect(std.mem.indexOf(u8, result, "spill_triggered=true") != null);
}

test "selective where on large table does not spill" {
    var env: FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\FilterTable {
        \\  field(id, i64, notNull, primaryKey)
        \\}
    );

    // Insert 4097 rows — requires a multi-chunk scan.
    try insertRows(executor, "FilterTable", spill_boundary_row_count);

    // Selective WHERE: only 50 of 4097 rows survive.
    const result = try executor.run(
        "FilterTable |> where(id < 51) |> inspect { id }",
    );

    // Only 50 rows returned.
    try std.testing.expect(std.mem.indexOf(u8, result, "returned_rows=50 ") != null);

    // No spill — 50 narrow rows fit in the 4 MB memory budget and 4096-slot batch.
    try std.testing.expect(std.mem.indexOf(u8, result, "spill_triggered=false") != null);
}

test "spill replay from same initial state produces identical results" {
    const row_count: u32 = spill_boundary_row_count;
    const schema =
        \\ReplayTable {
        \\  field(id, i64, notNull, primaryKey)
        \\}
    ;
    const query = "ReplayTable |> inspect { id }";

    var result_a: [64 * 1024]u8 = undefined;
    var result_b: [64 * 1024]u8 = undefined;
    var len_a: usize = 0;
    var len_b: usize = 0;

    // --- Run A ---
    {
        var env: FeatureEnv = undefined;
        try env.init();
        defer env.deinit();
        const executor = &env.executor;
        try executor.applyDefinitions(schema);
        try insertRows(executor, "ReplayTable", row_count);
        const r = try runWithBuffer(executor, query, &result_a);
        len_a = r.len;
    }

    // --- Run B (independent environment, same operations) ---
    {
        var env: FeatureEnv = undefined;
        try env.init();
        defer env.deinit();
        const executor = &env.executor;
        try executor.applyDefinitions(schema);
        try insertRows(executor, "ReplayTable", row_count);
        const r = try runWithBuffer(executor, query, &result_b);
        len_b = r.len;
    }

    // Byte-identical output proves deterministic spill path.
    try std.testing.expectEqual(len_a, len_b);
    try std.testing.expectEqualSlices(u8, result_a[0..len_a], result_b[0..len_b]);
}
