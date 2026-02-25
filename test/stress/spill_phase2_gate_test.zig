//! Phase 2 gate integration tests for the degrade-first spill path.
//!
//! Validates end-to-end correctness of the chunked scan + spill pipeline:
//! - Large table completeness: >4096 rows returned without truncation.
//! - Arena safety valve: string-heavy queries degrade instead of failing.
//! - Selective WHERE avoidance: narrow result sets stay in memory.
//! - Deterministic replay: identical setup produces byte-identical output.

const std = @import("std");
const pg2 = @import("pg2");
const internal = @import("test_env_test.zig");

const FeatureEnv = internal.FeatureEnv;
const TestExecutor = internal.TestExecutor;
const mutation_mod = pg2.executor.mutation;

const spill_boundary_row_count: u32 = 4097;
const insert_batch_size_simple: u32 = 128;
const insert_batch_size_string_heavy: u32 = 8;

/// Execute a query with a caller-provided response buffer, for tests whose
/// output exceeds the default 16 KB TestExecutor response buffer.
fn runWithBuffer(executor: *TestExecutor, request: []const u8, buf: []u8) ![]const u8 {
    var pool_conn = try executor.pool.checkout();
    const result = try executor.session.handleRequest(
        &executor.pool,
        &pool_conn,
        request,
        null,
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
    var start_id: u32 = 1;
    while (start_id <= count) {
        const remaining = count - start_id + 1;
        const batch = @min(insert_batch_size_simple, remaining);

        var query_buf: [16 * 1024]u8 = undefined;
        var stream = std.io.fixedBufferStream(query_buf[0..]);
        const writer = stream.writer();

        try writer.print("{s} |> insert(", .{model});
        var i: u32 = 0;
        while (i < batch) : (i += 1) {
            if (i > 0) try writer.writeAll(", ");
            const id = start_id + i;
            try writer.print("(id = {d})", .{id});
        }
        try writer.writeAll(") {}");
        _ = try executor.runSeed(stream.getWritten());

        start_id += batch;
    }
}

fn insertRowsWithScore(
    executor: *TestExecutor,
    model: []const u8,
    count: u32,
    score_scale: u32,
) !void {
    var start_id: u32 = 1;
    while (start_id <= count) {
        const remaining = count - start_id + 1;
        const batch = @min(insert_batch_size_simple, remaining);

        var query_buf: [16 * 1024]u8 = undefined;
        var stream = std.io.fixedBufferStream(query_buf[0..]);
        const writer = stream.writer();

        try writer.print("{s} |> insert(", .{model});
        var i: u32 = 0;
        while (i < batch) : (i += 1) {
            if (i > 0) try writer.writeAll(", ");
            const id = start_id + i;
            try writer.print("(id = {d}, score = {d})", .{ id, id * score_scale });
        }
        try writer.writeAll(") {}");
        _ = try executor.runSeed(stream.getWritten());

        start_id += batch;
    }
}

fn insertArenaRows(
    executor: *TestExecutor,
    count: u32,
    payload: []const u8,
) !void {
    var start_id: u32 = 1;
    while (start_id <= count) {
        const remaining = count - start_id + 1;
        const batch = @min(insert_batch_size_string_heavy, remaining);

        var query_buf: [16 * 1024]u8 = undefined;
        var stream = std.io.fixedBufferStream(query_buf[0..]);
        const writer = stream.writer();

        try writer.writeAll("ArenaTable |> insert(");
        var i: u32 = 0;
        while (i < batch) : (i += 1) {
            if (i > 0) try writer.writeAll(", ");
            const id = start_id + i;
            const flag_str = if (id <= 4) "true" else "false";
            try writer.print(
                "(id = {d}, data = \"{s}\", flag = {s})",
                .{ id, payload, flag_str },
            );
        }
        try writer.writeAll(") {}");
        _ = try executor.runSeed(stream.getWritten());

        start_id += batch;
    }
}

fn insertDistributedPosts(
    executor: *TestExecutor,
    post_count: u32,
    owner_count: u32,
) !void {
    var start_id: u32 = 1;
    while (start_id <= post_count) {
        const remaining = post_count - start_id + 1;
        const batch = @min(insert_batch_size_simple, remaining);

        var query_buf: [16 * 1024]u8 = undefined;
        var stream = std.io.fixedBufferStream(query_buf[0..]);
        const writer = stream.writer();

        try writer.writeAll("Post |> insert(");
        var i: u32 = 0;
        while (i < batch) : (i += 1) {
            if (i > 0) try writer.writeAll(", ");
            const post_id = start_id + i;
            const owner_id: u32 = ((post_id - 1) % owner_count) + 1;
            try writer.print(
                "(id = {d}, user_id = {d})",
                .{ post_id, owner_id },
            );
        }
        try writer.writeAll(") {}");
        _ = try executor.runSeed(stream.getWritten());

        start_id += batch;
    }
}

fn insertPostsForSingleUser(
    executor: *TestExecutor,
    post_count: u32,
    with_title: bool,
) !void {
    var start_id: u32 = 1;
    while (start_id <= post_count) {
        const remaining = post_count - start_id + 1;
        const batch = @min(insert_batch_size_simple, remaining);

        var query_buf: [16 * 1024]u8 = undefined;
        var stream = std.io.fixedBufferStream(query_buf[0..]);
        const writer = stream.writer();

        try writer.writeAll("Post |> insert(");
        var i: u32 = 0;
        while (i < batch) : (i += 1) {
            if (i > 0) try writer.writeAll(", ");
            const post_id = start_id + i;
            if (with_title) {
                try writer.print(
                    "(id = {d}, user_id = 1, title = \"p{d}\")",
                    .{ post_id, post_id },
                );
            } else {
                try writer.print("(id = {d}, user_id = 1)", .{post_id});
            }
        }
        try writer.writeAll(") {}");
        _ = try executor.runSeed(stream.getWritten());

        start_id += batch;
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

fn runMixedRootAndNestedHashSpillScenario(out_buf: []u8) ![]const u8 {
    var env: FeatureEnv = undefined;
    try env.initWithConfig(.{
        .max_query_slots = 1,
        .work_memory_bytes_per_slot = 256,
        .temp_pages_per_query_slot = 256,
    });
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  reference(posts, id, Post.user_id, withoutReferentialIntegrity)
        \\}
        \\Post {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(user_id, i64, notNull)
        \\}
    );

    try insertRows(executor, "User", spill_boundary_row_count);
    try insertDistributedPosts(
        executor,
        spill_boundary_row_count,
        spill_boundary_row_count,
    );

    const query =
        "User |> sort(id asc) |> inspect { id posts |> where(id > 0) |> sort(id asc) |> limit(1) { id } }";
    const result = try runWithBuffer(executor, query, out_buf);
    return result;
}

fn runRootSortSpillAndNestedHashSpillScenario(out_buf: []u8) ![]const u8 {
    var env: FeatureEnv = undefined;
    try env.initWithConfig(.{
        .max_query_slots = 1,
        .work_memory_bytes_per_slot = 256,
        .temp_pages_per_query_slot = 256,
    });
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  reference(posts, id, Post.user_id, withoutReferentialIntegrity)
        \\}
        \\Post {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(user_id, i64, notNull)
        \\}
    );

    try insertRows(executor, "User", spill_boundary_row_count);
    try insertDistributedPosts(
        executor,
        spill_boundary_row_count,
        spill_boundary_row_count,
    );

    const query =
        "User |> sort(id desc) |> limit(3) |> inspect { id posts |> where(id > 0) |> sort(id asc) |> limit(1) { id } }";
    const result = try runWithBuffer(executor, query, out_buf);
    return result;
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

test "collector-backed spill path supports nested selection with empty children" {
    var env: FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  reference(posts, id, Post.user_id, withoutReferentialIntegrity)
        \\}
        \\Post {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(user_id, i64, notNull)
        \\  field(title, string, notNull)
        \\}
    );

    try insertRows(executor, "User", spill_boundary_row_count);
    const seed_insert = try executor.run("Post |> insert(id = 1, user_id = 99999, title = \"seed\") {}");
    try std.testing.expect(!std.mem.startsWith(u8, seed_insert, "ERR query: "));

    var large_buf: [256 * 1024]u8 = undefined;
    const result = try runWithBuffer(
        executor,
        "User |> inspect { id posts |> sort(id asc) { id title } }",
        &large_buf,
    );

    try std.testing.expect(!std.mem.startsWith(u8, result, "ERR query: "));
    try std.testing.expect(std.mem.indexOf(u8, result, "{id:i64,posts:[{id:i64,title:str}]}") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "1,[]\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "4097,[]\n") != null);
}

test "nested selection fails explicitly when child scan exceeds in-memory batch" {
    // Under WF03 Option A, per-parent child subsets must degrade/spill and
    // preserve exact semantics instead of failing at the in-memory cap.
    var env: FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\  reference(posts, id, Post.user_id, withoutReferentialIntegrity)
        \\}
        \\Post {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(user_id, i64, notNull)
        \\  field(title, string, notNull)
        \\}
    );

    _ = try executor.run("User |> insert(id = 1, name = \"Alice\") {}");
    try insertPostsForSingleUser(executor, spill_boundary_row_count, true);

    const result = try executor.run(
        "User |> inspect { name posts |> sort(id desc) |> limit(1) { id } }",
    );
    try std.testing.expect(!std.mem.startsWith(u8, result, "ERR query: "));
    try std.testing.expect(std.mem.indexOf(u8, result, "{name:str,posts:[{id:i64}]}") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"Alice\",[[4097]]\n") != null);
}

test "mixed root spill and nested hash spill preserves per-parent results under tight temp budgets" {
    var result_buf: [512 * 1024]u8 = undefined;
    const result = try runMixedRootAndNestedHashSpillScenario(&result_buf);

    try std.testing.expect(std.mem.startsWith(u8, result, "OK returned_rows=4097 "));
    try std.testing.expect(std.mem.indexOf(u8, result, "{id:i64,posts:[{id:i64}]}") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\n1,[[1]]\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\n4097,[[4097]]\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "spill_triggered=true") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "join_strategy=hash_spill") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "nested_join_hash_spill=1") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "nested_join_hash_in_memory=0") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "nested_join_breakdown=nested_loop:0,hash_in_memory:0,hash_spill:1") != null);
}

test "mixed root spill and nested hash spill is deterministic under tight temp budgets" {
    var run1_buf: [512 * 1024]u8 = undefined;
    var run2_buf: [512 * 1024]u8 = undefined;
    const run1 = try runMixedRootAndNestedHashSpillScenario(&run1_buf);
    const run2 = try runMixedRootAndNestedHashSpillScenario(&run2_buf);
    try std.testing.expectEqualStrings(run1, run2);
}

test "root sort spill and nested hash spill compose correctly under tight budget" {
    var result_buf: [128 * 1024]u8 = undefined;
    const result = try runRootSortSpillAndNestedHashSpillScenario(&result_buf);

    try std.testing.expect(std.mem.startsWith(u8, result, "OK returned_rows=3 "));
    try std.testing.expect(std.mem.indexOf(u8, result, "{id:i64,posts:[{id:i64}]}") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\n4097,[[4097]]\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\n4096,[[4096]]\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\n4095,[[4095]]\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "spill_triggered=true") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "join_strategy=hash_spill") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "sort_strategy=external_merge") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "nested_join_breakdown=nested_loop:0,hash_in_memory:0,hash_spill:1") != null);
}

test "nested hash spill fails closed when temp page budget is exhausted" {
    var env: FeatureEnv = undefined;
    try env.initWithConfig(.{
        .max_query_slots = 1,
        .work_memory_bytes_per_slot = 256,
        .temp_pages_per_query_slot = 1,
    });
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  reference(posts, id, Post.user_id, withoutReferentialIntegrity)
        \\}
        \\Post {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(user_id, i64, notNull)
        \\}
    );

    _ = try executor.run("User |> insert(id = 1) {}");

    try insertPostsForSingleUser(executor, spill_boundary_row_count, false);

    const result = try executor.run(
        "User |> where(id == 1) |> inspect { id posts |> limit(1) { id } }",
    );
    try std.testing.expect(std.mem.startsWith(u8, result, "ERR query: "));
    try std.testing.expect(std.mem.indexOf(
        u8,
        result,
        "nested relation hash spill temp page budget exhausted",
    ) != null);
}
