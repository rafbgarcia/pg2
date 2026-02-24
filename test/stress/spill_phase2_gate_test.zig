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

    var user_id: u32 = 1;
    while (user_id <= 4200) : (user_id += 1) {
        var user_query_buf: [128]u8 = undefined;
        const user_query = std.fmt.bufPrint(
            &user_query_buf,
            "User |> insert(id = {d}) {{}}",
            .{user_id},
        ) catch unreachable;
        _ = try executor.run(user_query);
    }

    var post_id: u32 = 1;
    while (post_id <= 5000) : (post_id += 1) {
        const owner_id: u32 = ((post_id - 1) % 4200) + 1;
        var post_query_buf: [160]u8 = undefined;
        const post_query = std.fmt.bufPrint(
            &post_query_buf,
            "Post |> insert(id = {d}, user_id = {d}) {{}}",
            .{ post_id, owner_id },
        ) catch unreachable;
        _ = try executor.run(post_query);
    }

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

    var user_id: u32 = 1;
    while (user_id <= 4200) : (user_id += 1) {
        var user_query_buf: [128]u8 = undefined;
        const user_query = std.fmt.bufPrint(
            &user_query_buf,
            "User |> insert(id = {d}) {{}}",
            .{user_id},
        ) catch unreachable;
        _ = try executor.run(user_query);
    }

    var post_id: u32 = 1;
    while (post_id <= 5000) : (post_id += 1) {
        const owner_id: u32 = ((post_id - 1) % 4200) + 1;
        var post_query_buf: [160]u8 = undefined;
        const post_query = std.fmt.bufPrint(
            &post_query_buf,
            "Post |> insert(id = {d}, user_id = {d}) {{}}",
            .{ post_id, owner_id },
        ) catch unreachable;
        _ = try executor.run(post_query);
    }

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
        \\  field(flag, bool, notNull)
        \\}
    );

    const padding = "A" ** 250;

    // Insert 4200 rows (2 scan chunks) with 250-byte strings.
    // Mark the first 4 rows with flag=true, rest with flag=false.
    var i: i64 = 1;
    while (i <= 4200) : (i += 1) {
        var query_buf: [512]u8 = undefined;
        const flag_str = if (i <= 4) "true" else "false";
        const query = std.fmt.bufPrint(
            &query_buf,
            "ArenaTable |> insert(id = {d}, data = \"{s}\", flag = {s}) {{}}",
            .{ i, padding, flag_str },
        ) catch unreachable;
        _ = try executor.run(query);
    }

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

    try insertRows(executor, "LimitSpillTable", 4200);

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

    try insertRows(executor, "SortLimitSpillTable", 4200);

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
    try std.testing.expect(std.mem.indexOf(u8, result, "\n4200\n4199\n4198\n4197\n4196\n4195\n4194\n4193\n4192\n4191\n") != null);
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

    try insertRows(executor, "OffsetLimitSpillTable", 4200);

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

    var i: u32 = 1;
    while (i <= 4200) : (i += 1) {
        var query_buf: [160]u8 = undefined;
        const query = std.fmt.bufPrint(
            &query_buf,
            "ProjectionSpillTable |> insert(id = {d}, score = {d}) {{}}",
            .{ i, i * 2 },
        ) catch unreachable;
        _ = try executor.run(query);
    }

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

    var i: u32 = 1;
    while (i <= 4200) : (i += 1) {
        var query_buf: [192]u8 = undefined;
        const query = std.fmt.bufPrint(
            &query_buf,
            "ComputedProjectionSpillTable |> insert(id = {d}, score = {d}) {{}}",
            .{ i, i * 10 },
        ) catch unreachable;
        _ = try executor.run(query);
    }

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

    try insertRows(executor, "HavingSpillTable", 4200);

    var large_buf: [64 * 1024]u8 = undefined;
    const result = try runWithBuffer(
        executor,
        "HavingSpillTable |> having(id > 100) |> inspect { id }",
        &large_buf,
    );

    try std.testing.expect(std.mem.startsWith(u8, result, "OK returned_rows=4100 "));
    try std.testing.expect(
        std.mem.indexOf(
            u8,
            result,
            "\n101\n102\n103\n",
        ) != null,
    );
    try std.testing.expect(std.mem.indexOf(u8, result, "\n4200\n") != null);
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

    try insertRows(executor, "HavingLimitOrderTable", 4200);

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

    try insertRows(executor, "SortHavingSpillTable", 4200);

    const result = try executor.run(
        "SortHavingSpillTable |> sort(id desc) |> having(id > 4197) |> inspect { id }",
    );

    try std.testing.expect(std.mem.startsWith(u8, result, "OK returned_rows=3 "));
    try std.testing.expect(std.mem.indexOf(u8, result, "\n4200\n4199\n4198\n") != null);
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

    var i: u32 = 1;
    while (i <= 4200) : (i += 1) {
        var query_buf: [256]u8 = undefined;
        const query = std.fmt.bufPrint(
            &query_buf,
            "User |> insert(id = {d}) {{}}",
            .{i},
        ) catch unreachable;
        _ = try executor.run(query);
    }
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
    try std.testing.expect(std.mem.indexOf(u8, result, "4200,[]\n") != null);
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
    var i: u32 = 1;
    while (i <= 4200) : (i += 1) {
        var query_buf: [256]u8 = undefined;
        const query = std.fmt.bufPrint(
            &query_buf,
            "Post |> insert(id = {d}, user_id = 1, title = \"p{d}\") {{}}",
            .{ i, i },
        ) catch unreachable;
        _ = try executor.run(query);
    }

    const result = try executor.run(
        "User |> inspect { name posts |> sort(id desc) |> limit(1) { id } }",
    );
    try std.testing.expect(!std.mem.startsWith(u8, result, "ERR query: "));
    try std.testing.expect(std.mem.indexOf(u8, result, "{name:str,posts:[{id:i64}]}") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"Alice\",[[4200]]\n") != null);
}

test "mixed root spill and nested hash spill preserves per-parent results under tight temp budgets" {
    var result_buf: [512 * 1024]u8 = undefined;
    const result = try runMixedRootAndNestedHashSpillScenario(&result_buf);

    try std.testing.expect(std.mem.startsWith(u8, result, "OK returned_rows=4200 "));
    try std.testing.expect(std.mem.indexOf(u8, result, "{id:i64,posts:[{id:i64}]}") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\n1,[[1]]\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\n4200,[[4200]]\n") != null);
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
    try std.testing.expect(std.mem.indexOf(u8, result, "\n4200,[[4200]]\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\n4199,[[4199]]\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\n4198,[[4198]]\n") != null);
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

    var post_id: u32 = 1;
    while (post_id <= 5000) : (post_id += 1) {
        var post_query_buf: [160]u8 = undefined;
        const post_query = std.fmt.bufPrint(
            &post_query_buf,
            "Post |> insert(id = {d}, user_id = 1) {{}}",
            .{post_id},
        ) catch unreachable;
        _ = try executor.run(post_query);
    }

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
