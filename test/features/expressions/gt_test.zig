//! Feature coverage for greater-than operator behavior through server session path.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature greater-than supports nested having predicates" {
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
    _ = try executor.run("Post |> insert(id = 9, user_id = 2, title = \"B09\") {}");
    _ = try executor.run("Post |> insert(id = 7, user_id = 2, title = \"B07\") {}");

    const result = try executor.run(
        "User |> sort(id asc) { name posts |> having(id > 8) |> sort(id desc) { id } }",
    );
    const expected =
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n" ++
        "{name:str,posts:[{id:i64}]}\n" ++
        "\"Alice\",[[10]]\n" ++
        "\"Bob\",[[9]]\n";
    try std.testing.expectEqualStrings(expected, result);
}
