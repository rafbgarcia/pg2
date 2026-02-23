//! Contract test for per-parent nested spill semantics.
//!
//! This test captures the target behavior for Phase 5:
//! a single parent with child cardinality above in-memory subset capacity must
//! still return correct nested results (degrade/spill first, no truncation).
const std = @import("std");

const internal = @import("../../features/test_env_test.zig");

test "nested one-parent-many-children degrades instead of failing" {
    var env: internal.FeatureEnv = undefined;
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

test "nested per-parent spill preserves where-group-having-sort-offset-limit order" {
    var env: internal.FeatureEnv = undefined;
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
        \\  field(bucket, i64, notNull)
        \\  field(points, i64, notNull)
        \\}
    );

    _ = try executor.run("User |> insert(id = 1, name = \"Alice\") {}");

    var i: u32 = 1;
    while (i <= 4200) : (i += 1) {
        var query_buf: [256]u8 = undefined;
        const query = std.fmt.bufPrint(
            &query_buf,
            "Post |> insert(id = {d}, user_id = 1, bucket = {d}, points = {d}) {{}}",
            .{ i, i % 4, i },
        ) catch unreachable;
        _ = try executor.run(query);
    }

    const result = try executor.run(
        "User |> inspect { name posts |> where(points >= 0) |> group(bucket) |> having(sum(points) > 0) |> sort(bucket asc) |> offset(1) |> limit(1) { bucket } }",
    );

    try std.testing.expect(!std.mem.startsWith(u8, result, "ERR query: "));
    try std.testing.expect(std.mem.indexOf(u8, result, "{name:str,posts:[{bucket:i64}]}") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"Alice\",[[1]]\n") != null);
}
