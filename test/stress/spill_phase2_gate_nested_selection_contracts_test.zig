//! Phase 2 gate integration tests for nested selection spill behavior.
const std = @import("std");
const spill = @import("spill_phase2_gate_helpers.zig");

const FeatureEnv = spill.FeatureEnv;
const spill_boundary_row_count = spill.spill_boundary_row_count;
const runWithBuffer = spill.runWithBuffer;
const insertRows = spill.insertRows;
const insertPostsForSingleUser = spill.insertPostsForSingleUser;

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
