//! Feature coverage for membership expression semantics (`in` and `!in`).
const std = @import("std");
const feature = @import("../test_env_test.zig");

fn expectContains(haystack: []const u8, needle: []const u8) !void {
    try std.testing.expect(std.mem.indexOf(u8, haystack, needle) != null);
}

test "feature where supports in(value, list) and !in(value, list)" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\Post {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(status, string, nullable)
        \\}
    );

    _ = try executor.run("Post |> insert(id = 1, status = \"draft\") {}");
    _ = try executor.run("Post |> insert(id = 2, status = \"published\") {}");
    _ = try executor.run("Post |> insert(id = 3, status = \"archived\") {}");

    var result = try executor.run(
        "Post |> where(in(status, [\"draft\", \"published\"])) |> sort(id asc) { id status }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,draft\n2,published\n",
        result,
    );

    result = try executor.run(
        "Post |> where(!in(status, [\"draft\", \"published\"])) |> sort(id asc) { id status }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n3,archived\n",
        result,
    );
}

test "feature where membership with null result does not match predicate" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\Ticket {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(status, string, nullable)
        \\}
    );

    _ = try executor.run("Ticket |> insert(id = 1, status = \"archived\") {}");
    _ = try executor.run("Ticket |> insert(id = 2, status = null) {}");

    var result = try executor.run(
        "Ticket |> where(in(status, [\"active\", null])) |> sort(id asc) { id status }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run(
        "Ticket |> where(!in(status, [\"active\", null])) |> sort(id asc) { id status }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=0 deleted_rows=0\n",
        result,
    );
}

test "feature update supports membership assignment expressions" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\Review {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(status, string, nullable)
        \\  field(is_open, bool, nullable)
        \\}
    );

    _ = try executor.run("Review |> insert(id = 1, status = \"pending\", is_open = null) {}");
    _ = try executor.run("Review |> insert(id = 2, status = \"closed\", is_open = null) {}");

    var result = try executor.run(
        "Review |> update(is_open = in(status, [\"pending\", \"in_progress\"])) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=2 deleted_rows=0\n",
        result,
    );

    result = try executor.run("Review |> sort(id asc) { id is_open }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,true\n2,false\n",
        result,
    );

    result = try executor.run(
        "Review |> update(is_open = !in(status, [\"pending\", \"in_progress\"])) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=2 deleted_rows=0\n",
        result,
    );

    result = try executor.run("Review |> sort(id asc) { id is_open }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,false\n2,true\n",
        result,
    );
}

test "feature membership assignment fails closed on type mismatch" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\TypeMismatchMembership {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(status, string, notNull)
        \\  field(in_scope, bool, nullable)
        \\}
    );

    _ = try executor.run("TypeMismatchMembership |> insert(id = 1, status = \"1\", in_scope = null) {}");
    const result = try executor.run(
        "TypeMismatchMembership |> where(id == 1) |> update(in_scope = in(status, [1, 2])) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: message=\"update failed\" phase=execution code=TypeMismatch path=query line=1 col=1\n",
        result,
    );
}

test "feature where membership handles null needle and null element semantics" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\StatusProbe {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(status, string, nullable)
        \\}
    );

    _ = try executor.run("StatusProbe |> insert(id = 1, status = \"draft\") {}");
    _ = try executor.run("StatusProbe |> insert(id = 2, status = \"archived\") {}");
    _ = try executor.run("StatusProbe |> insert(id = 3, status = null) {}");

    var result = try executor.run(
        "StatusProbe |> where(in(status, [null, \"draft\"])) |> sort(id asc) { id status }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,draft\n",
        result,
    );

    result = try executor.run(
        "StatusProbe |> where(!in(status, [null, \"draft\"])) |> sort(id asc) { id status }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=0 deleted_rows=0\n",
        result,
    );
}

test "feature membership supports where and sort expression contexts" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\PostStatus {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(status, string, nullable)
        \\}
    );

    _ = try executor.run("PostStatus |> insert(id = 1, status = \"draft\") {}");
    _ = try executor.run("PostStatus |> insert(id = 2, status = \"published\") {}");

    var result = try executor.run(
        "PostStatus |> sort(in(status, [\"draft\", \"published\"]) desc, id asc) { id status }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,draft\n2,published\n",
        result,
    );

    result = try executor.run(
        "PostStatus |> where(!in(status, [\"draft\"])) |> sort(id asc) { id status }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n2,published\n",
        result,
    );
}

test "feature membership fails closed for invalid shape and arity" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\InvalidMembership {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(status, string, nullable)
        \\  field(status_list, string, nullable)
        \\}
    );

    _ = try executor.run("InvalidMembership |> insert(id = 1, status = \"draft\", status_list = \"draft\") {}");

    var result = try executor.run(
        "InvalidMembership |> where(in(status, status_list)) { id }",
    );
    try expectContains(result, "ERR query:");

    result = try executor.run(
        "InvalidMembership |> where(in(status)) { id }",
    );
    try expectContains(result, "ERR parse:");
}
