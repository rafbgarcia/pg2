//! Phase 2 gate integration tests for the degrade-first spill path (Workfront 03).
//!
//! Validates end-to-end correctness of the chunked scan + spill pipeline:
//! - Large table completeness: >4096 rows returned without truncation.
//! - Arena safety valve: string-heavy queries degrade instead of failing.
//! - Selective WHERE avoidance: narrow result sets stay in memory.
//! - Deterministic replay: identical setup produces byte-identical output.

const std = @import("std");
const pg2 = @import("pg2");
const internal = @import("../../features/test_env_test.zig");

const FeatureEnv = internal.FeatureEnv;
const TestExecutor = internal.TestExecutor;
const mutation_mod = pg2.executor.mutation;

/// Execute a query with a caller-provided response buffer, for tests whose
/// output exceeds the default 16 KB TestExecutor response buffer.
fn runWithBuffer(executor: *TestExecutor, request: []const u8, buf: []u8) ![]const u8 {
    var pool_conn = try executor.pool.checkout();
    const result = try executor.session.handleRequest(
        &executor.pool,
        &pool_conn,
        request,
        buf,
    );

    if (result.is_query_error) {
        mutation_mod.rollbackOverflowReclaimEntriesForTx(
            executor.catalog,
            pool_conn.tx_id,
        );
        try executor.pool.abortCheckin(&pool_conn);
        return buf[0..result.bytes_written];
    }

    const tx_id = pool_conn.tx_id;
    if (result.had_mutation) {
        mutation_mod.commitOverflowReclaimEntriesForTx(
            executor.catalog,
            &executor.runtime.pool,
            &executor.runtime.wal,
            tx_id,
            1,
        ) catch |err| {
            mutation_mod.rollbackOverflowReclaimEntriesForTx(
                executor.catalog,
                tx_id,
            );
            try executor.pool.abortCheckin(&pool_conn);
            return err;
        };
    }
    try executor.pool.checkin(&pool_conn);
    return buf[0..result.bytes_written];
}

/// Insert `count` rows with sequential i64 ids starting at 1.
fn insertRows(executor: *TestExecutor, model: []const u8, count: u32) !void {
    var i: u32 = 1;
    while (i <= count) : (i += 1) {
        var query_buf: [128]u8 = undefined;
        const query = std.fmt.bufPrint(
            &query_buf,
            "{s} |> insert(id = {d}) {{}}",
            .{ model, i },
        ) catch unreachable;
        _ = try executor.run(query);
    }
}

/// Count newline characters in a byte slice.
fn countLines(data: []const u8) u32 {
    var n: u32 = 0;
    for (data) |c| {
        if (c == '\n') n += 1;
    }
    return n;
}

// ---------------------------------------------------------------------------
// Gate tests
// ---------------------------------------------------------------------------

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

    // Insert 4200 rows (exceeds scan_batch_size of 4096, forcing a 2-chunk scan).
    try insertRows(executor, "BigTable", 4200);

    // Query all rows with a large buffer (response is ~20 KB for 4200 ids).
    var large_buf: [64 * 1024]u8 = undefined;
    const result = try runWithBuffer(executor, "BigTable |> inspect { id }", &large_buf);

    // Header reports all 4200 rows.
    try std.testing.expect(std.mem.startsWith(u8, result, "OK returned_rows=4200 "));

    // Count data rows between header and INSPECT block.
    const header_end = (std.mem.indexOf(u8, result, "\n") orelse unreachable) + 1;
    const inspect_start = std.mem.indexOf(u8, result, "INSPECT ") orelse result.len;
    const body = result[header_end..inspect_start];
    try std.testing.expectEqual(@as(u32, 4200), countLines(body));

    // First and last ids present (rows come in insertion order across spill).
    try std.testing.expect(std.mem.startsWith(u8, body, "1\n"));
    try std.testing.expect(std.mem.indexOf(u8, body, "4200\n") != null);

    // Spill triggered (hot batch filled at 4096 rows, forcing a flush).
    try std.testing.expect(std.mem.indexOf(u8, result, "spill_triggered=true") != null);
}

test "query exceeding string arena completes via arena safety valve" {
    // 1 MB arena: a single scan batch of 4096 rows × 250-byte strings fills
    // ~97.7% of the arena, leaving < 10% free. The safety valve force-flushes
    // the collector's hot batch and resets the arena before the second chunk,
    // preventing an OutOfMemory failure during string materialization.
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
        \\}
    );

    const padding = "A" ** 250;

    // Insert 4200 rows (2 scan chunks) with 250-byte strings.
    var i: i64 = 1;
    while (i <= 4200) : (i += 1) {
        var query_buf: [512]u8 = undefined;
        const query = std.fmt.bufPrint(
            &query_buf,
            "ArenaTable |> insert(id = {d}, data = \"{s}\") {{}}",
            .{ i, padding },
        ) catch unreachable;
        _ = try executor.run(query);
    }

    // Use a selective WHERE so the response stays small (4 rows × ~260 bytes)
    // while still forcing a full table scan that fills the arena.
    const result = try executor.run(
        "ArenaTable |> where(id < 5) |> inspect {}",
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

    // Insert 4200 rows — requires a multi-chunk scan.
    try insertRows(executor, "FilterTable", 4200);

    // Selective WHERE: only 50 of 4200 rows survive.
    const result = try executor.run(
        "FilterTable |> where(id < 51) |> inspect { id }",
    );

    // Only 50 rows returned.
    try std.testing.expect(std.mem.indexOf(u8, result, "returned_rows=50 ") != null);

    // No spill — 50 narrow rows fit in the 4 MB memory budget and 4096-slot batch.
    try std.testing.expect(std.mem.indexOf(u8, result, "spill_triggered=false") != null);
}

test "spill replay from same initial state produces identical results" {
    const row_count: u32 = 4200;
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
