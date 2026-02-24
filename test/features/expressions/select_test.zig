//! Feature coverage for select/query behavior through server session path.
const std = @import("std");
const feature = @import("../test_env_test.zig");

fn expectContains(haystack: []const u8, needle: []const u8) !void {
    try std.testing.expect(std.mem.indexOf(u8, haystack, needle) != null);
}

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

test "feature nested child pipeline applies having per parent" {
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

test "feature query supports operator-keyword field names in where sort and projection" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\KeywordFields {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(offset, i64, notNull)
        \\}
    );

    _ = try executor.run("KeywordFields |> insert(id = 1, offset = 20) {}");
    _ = try executor.run("KeywordFields |> insert(id = 2, offset = 10) {}");

    const result = try executor.run(
        "KeywordFields |> where(offset >= 10) |> sort(offset asc) { id offset }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n2,10\n1,20\n",
        result,
    );
}

test "feature computed select mirrors where predicate outcomes for composed expressions" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\ComputedSelectParity {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(base, i64, notNull)
        \\  field(bonus, i64, notNull)
        \\  field(active, bool, nullable)
        \\  field(status, string, nullable)
        \\}
    );

    _ = try executor.run("ComputedSelectParity |> insert(id = 1, base = 7, bonus = 3, active = false, status = \"open\") {}");
    _ = try executor.run("ComputedSelectParity |> insert(id = 2, base = 2, bonus = 1, active = true, status = \"archived\") {}");
    _ = try executor.run("ComputedSelectParity |> insert(id = 3, base = 5, bonus = 0, active = false, status = \"open\") {}");
    _ = try executor.run("ComputedSelectParity |> insert(id = 4, base = 9, bonus = 2, active = false, status = \"archived\") {}");

    var result = try executor.run(
        "ComputedSelectParity |> where(active == true || base + bonus >= 10 && !in(status, [\"archived\"])) |> sort(id asc) { id }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n1\n2\n",
        result,
    );

    result = try executor.run(
        "ComputedSelectParity |> sort(id asc) { id in_scope: active == true || base + bonus >= 10 && !in(status, [\"archived\"]) }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=4 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,true\n2,true\n3,false\n4,false\n",
        result,
    );
}

test "feature computed select preserves null equality and inequality semantics" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\ComputedSelectNullEq {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(status, string, nullable)
        \\}
    );

    _ = try executor.run("ComputedSelectNullEq |> insert(id = 1, status = \"open\") {}");
    _ = try executor.run("ComputedSelectNullEq |> insert(id = 2, status = \"closed\") {}");
    _ = try executor.run("ComputedSelectNullEq |> insert(id = 3, status = null) {}");

    const result = try executor.run(
        "ComputedSelectNullEq |> sort(id asc) { id probe: status == null || status != null }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=3 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,true\n2,true\n3,true\n",
        result,
    );
}

test "feature computed select fails closed on incompatible comparison types" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\ComputedSelectTypeMismatch {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(status, string, notNull)
        \\}
    );

    _ = try executor.run("ComputedSelectTypeMismatch |> insert(id = 1, status = \"1\") {}");

    const result = try executor.run(
        "ComputedSelectTypeMismatch |> sort(id asc) { id bad: status == 1 }",
    );
    try expectContains(result, "ERR query: select computed expression evaluation failed");
}

test "feature nested child where fails closed for non-boolean predicate outputs" {
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
    _ = try executor.run("Post |> insert(id = 10, user_id = 1, title = \"A10\") {}");

    const result = try executor.run(
        "User |> sort(id asc) { name posts |> where(id + user_id) { id } }",
    );
    try std.testing.expectEqualStrings(
        "ERR query: where/having predicate must evaluate to boolean (true or false)\n",
        result,
    );
}

test "feature nested child having fails closed for non-boolean predicate outputs" {
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
    _ = try executor.run("Post |> insert(id = 10, user_id = 1, title = \"A10\") {}");

    const result = try executor.run(
        "User |> sort(id asc) { name posts |> having(id + user_id) { id } }",
    );
    try std.testing.expectEqualStrings(
        "ERR query: where/having predicate must evaluate to boolean (true or false)\n",
        result,
    );
}
