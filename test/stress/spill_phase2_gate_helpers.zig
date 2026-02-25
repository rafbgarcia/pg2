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

pub const FeatureEnv = internal.FeatureEnv;
pub const TestExecutor = internal.TestExecutor;
const mutation_mod = pg2.executor.mutation;

pub const spill_boundary_row_count: u32 = 4097;
const insert_batch_size_simple: u32 = 128;
const insert_batch_size_string_heavy: u32 = 8;

/// Execute a query with a caller-provided response buffer, for tests whose
/// output exceeds the default 16 KB TestExecutor response buffer.
pub fn runWithBuffer(executor: *TestExecutor, request: []const u8, buf: []u8) ![]const u8 {
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
pub fn insertRows(executor: *TestExecutor, model: []const u8, count: u32) !void {
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

pub fn insertRowsWithScore(
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

pub fn insertArenaRows(
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

pub fn insertDistributedPosts(
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

pub fn insertPostsForSingleUser(
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
pub fn countLines(data: []const u8) u32 {
    var n: u32 = 0;
    for (data) |c| {
        if (c == '\n') n += 1;
    }
    return n;
}

pub fn runMixedRootAndNestedHashSpillScenario(out_buf: []u8) ![]const u8 {
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

pub fn runRootSortSpillAndNestedHashSpillScenario(out_buf: []u8) ![]const u8 {
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
