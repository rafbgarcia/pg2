//! Feature coverage for select/query behavior through server session path.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature query returns deterministic rows via session path" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\  field(active, bool, notNull)
        \\}
    );

    var result = try executor.run(
        "User |> insert(id = 1, name = \"Charlie\", active = true) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run(
        "User |> insert(id = 2, name = \"Alice\", active = true) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run(
        "User |> insert(id = 3, name = \"Bob\", active = false) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run(
        "User |> where(active == true) |> sort(name asc) { id name active }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n2,Alice,true\n1,Charlie,true\n",
        result,
    );
}

test "feature query projection returns only requested columns via session path" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\  field(active, bool, notNull)
        \\}
    );

    var result = try executor.run(
        "User |> insert(id = 1, name = \"Alice\", active = true) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run("User |> where(id == 1) { id name }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n" ++
            "1,Alice\n",
        result,
    );
}

test "feature query nested projection returns tree-shaped roots with nested lists including empty list" {
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
        "User |> insert(id = 2, name = \"Bob\") {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run(
        "Post |> insert(id = 10, user_id = 1, title = \"Hello\") {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run(
        "Post |> insert(id = 20, user_id = 1, title = \"World\") {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run(
        "Post |> insert(id = 30, user_id = 1, title = \"Ignored\") {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run(
        "User |> insert(id = 3, name = \"Trish\") {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run(
        "User |> sort(id asc) { name posts |> sort(id asc) { id title } }",
    );
    const expected =
        "OK returned_rows=3 inserted_rows=0 updated_rows=0 deleted_rows=0\n" ++
        "{name:str,posts:[{id:i64,title:str}]}\n" ++
        "\"Alice\",[[10,\"Hello\"],[20,\"World\"],[30,\"Ignored\"]]\n" ++
        "\"Bob\",[]\n" ++
        "\"Trish\",[]\n";
    try std.testing.expectEqualStrings(expected, result);
}

test "feature nested child pipeline applies limit and offset per parent" {
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

    _ = try executor.run("User |> insert(id = 1, name = \"Alice\") {}");
    _ = try executor.run("User |> insert(id = 2, name = \"Bob\") {}");

    _ = try executor.run("Post |> insert(id = 10, user_id = 1, title = \"A10\") {}");
    _ = try executor.run("Post |> insert(id = 8, user_id = 1, title = \"A08\") {}");
    _ = try executor.run("Post |> insert(id = 6, user_id = 1, title = \"A06\") {}");
    _ = try executor.run("Post |> insert(id = 9, user_id = 2, title = \"B09\") {}");
    _ = try executor.run("Post |> insert(id = 7, user_id = 2, title = \"B07\") {}");
    _ = try executor.run("Post |> insert(id = 5, user_id = 2, title = \"B05\") {}");

    const result = try executor.run(
        "User |> sort(id asc) { name posts |> sort(id desc) |> offset(1) |> limit(1) { id } }",
    );
    const expected =
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n" ++
        "{name:str,posts:[{id:i64}]}\n" ++
        "\"Alice\",[[8]]\n" ++
        "\"Bob\",[[7]]\n";
    try std.testing.expectEqualStrings(expected, result);
}
