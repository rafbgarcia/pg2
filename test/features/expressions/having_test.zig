//! Feature coverage for having expression parity with aggregates.
const std = @import("std");
const feature = @import("../test_env_test.zig");

fn expectContains(haystack: []const u8, needle: []const u8) !void {
    try std.testing.expect(std.mem.indexOf(u8, haystack, needle) != null);
}

test "feature having supports composed aggregate predicates with membership" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\Ticket {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(status, string, nullable)
        \\  field(points, i64, notNull)
        \\}
    );

    _ = try executor.run("Ticket |> insert(id = 1, status = \"open\", points = 5) {}");
    _ = try executor.run("Ticket |> insert(id = 2, status = \"open\", points = 6) {}");
    _ = try executor.run("Ticket |> insert(id = 3, status = \"closed\", points = 4) {}");
    _ = try executor.run("Ticket |> insert(id = 4, status = \"closed\", points = 3) {}");
    _ = try executor.run("Ticket |> insert(id = 5, status = \"closed\", points = 2) {}");
    _ = try executor.run("Ticket |> insert(id = 6, status = null, points = 9) {}");

    const result = try executor.run(
        "Ticket |> group(status) |> having(count(*) >= 2 && sum(points) >= 9 && !in(status, [\"closed\"])) |> sort(status asc) { status }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\nopen\n",
        result,
    );
}

test "feature having preserves null equality semantics with aggregates" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\Probe {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(status, string, nullable)
        \\}
    );

    _ = try executor.run("Probe |> insert(id = 1, status = \"open\") {}");
    _ = try executor.run("Probe |> insert(id = 2, status = \"closed\") {}");
    _ = try executor.run("Probe |> insert(id = 3, status = null) {}");

    const result = try executor.run(
        "Probe |> group(status) |> having(count(*) > 0 && (status == null || status != null)) |> sort(status asc) { status }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=3 inserted_rows=0 updated_rows=0 deleted_rows=0\nclosed\nopen\nnull\n",
        result,
    );
}

test "feature having fails closed on invalid aggregate operand type" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\BadAggregate {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(status, string, notNull)
        \\}
    );

    _ = try executor.run("BadAggregate |> insert(id = 1, status = \"open\") {}");

    const result = try executor.run(
        "BadAggregate |> group(status) |> having(sum(status) > 0) { status }",
    );
    try expectContains(result, "message=\"aggregate evaluation failed\"");
}

test "feature having fails closed for non-boolean predicate outputs" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\HavingTypeMismatch {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(status, string, notNull)
        \\  field(points, i64, notNull)
        \\}
    );

    _ = try executor.run("HavingTypeMismatch |> insert(id = 1, status = \"open\", points = 5) {}");
    _ = try executor.run("HavingTypeMismatch |> insert(id = 2, status = \"open\", points = 7) {}");

    const result = try executor.run(
        "HavingTypeMismatch |> group(status) |> having(sum(points)) { status }",
    );
    try std.testing.expectEqualStrings(
        "ERR query: message=\"having expression must evaluate to boolean\" phase=execution code=QueryExecutionError path=query line=1 col=1\n",
        result,
    );
}
