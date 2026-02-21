//! Internal coverage for tree-shaped query protocol serialization.
const std = @import("std");
const feature = @import("../../features/test_env_test.zig");

test "internal query protocol emits shape once and grouped root rows" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\  field(active, bool, notNull)
        \\  reference(posts, id, Post.user_id, withoutReferentialIntegrity)
        \\}
        \\Post {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(user_id, i64, notNull)
        \\  field(title, string, notNull)
        \\}
    );

    var result = try executor.run(
        "User |> insert(id = 1, name = \"Alice\", active = true) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );
    result = try executor.run(
        "User |> insert(id = 2, name = \"Bob\", active = false) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    const post_inserts = [_][]const u8{
        "Post |> insert(id = 10, user_id = 1, title = \"Hello\") {}",
        "Post |> insert(id = 20, user_id = 1, title = \"World\") {}",
    };
    for (post_inserts) |stmt| {
        result = try executor.run(stmt);
        try std.testing.expectEqualStrings(
            "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
            result,
        );
    }

    result = try executor.run(
        "User |> sort(id asc) { name posts |> sort(id asc) { id title } active }",
    );
    const expected =
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n" ++
        "{name:str,posts:[{id:i64,title:str}],active:bool}\n" ++
        "\"Alice\",[[10,\"Hello\"],[20,\"World\"]],true\n" ++
        "\"Bob\",[],false\n";
    try std.testing.expectEqualStrings(expected, result);
}

test "internal query protocol keeps quoted string framing for punctuation" {
    var env: feature.FeatureEnv = undefined;
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

    var result = try executor.run(
        "User |> insert(id = 1, name = \"Alice\") {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run(
        "Post |> insert(id = 10, user_id = 1, title = \"Hello, world\") {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run(
        "User |> where(id == 1) { name posts |> sort(id asc) { id title } }",
    );
    const expected =
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n" ++
        "{name:str,posts:[{id:i64,title:str}]}\n" ++
        "\"Alice\",[[10,\"Hello, world\"]]\n";
    try std.testing.expectEqualStrings(expected, result);
}
