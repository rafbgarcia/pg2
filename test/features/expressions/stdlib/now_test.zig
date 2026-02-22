//! Feature coverage for now() builtin deterministic statement semantics.
const std = @import("std");
const feature = @import("../../test_env_test.zig");

fn parseTimestampPair(row_line: []const u8) !struct { created_at: i64, updated_at: i64 } {
    var parts = std.mem.splitScalar(u8, row_line, ',');
    _ = parts.next() orelse return error.TestExpectedEqual;
    const created_text = parts.next() orelse return error.TestExpectedEqual;
    const updated_text = parts.next() orelse return error.TestExpectedEqual;
    if (parts.next() != null) return error.TestExpectedEqual;
    return .{
        .created_at = try std.fmt.parseInt(i64, created_text, 10),
        .updated_at = try std.fmt.parseInt(i64, updated_text, 10),
    };
}

test "feature now is statement-stable per callsite and independent across callsites" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\NowSemantics {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(created_at, timestamp, nullable)
        \\  field(updated_at, timestamp, nullable)
        \\}
    );

    _ = try executor.run("NowSemantics |> insert(id = 1, created_at = null, updated_at = null) {}");
    _ = try executor.run("NowSemantics |> insert(id = 2, created_at = null, updated_at = null) {}");
    _ = try executor.run("NowSemantics |> insert(id = 3, created_at = null, updated_at = null) {}");

    var result = try executor.run(
        "NowSemantics |> update(created_at = now(), updated_at = now()) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=3 deleted_rows=0\n",
        result,
    );

    result = try executor.run("NowSemantics |> sort(id asc) { id created_at updated_at }");

    var lines = std.mem.splitScalar(u8, result, '\n');
    _ = lines.next() orelse return error.TestExpectedEqual;
    const row_1 = lines.next() orelse return error.TestExpectedEqual;
    const row_2 = lines.next() orelse return error.TestExpectedEqual;
    const row_3 = lines.next() orelse return error.TestExpectedEqual;

    const pair_1 = try parseTimestampPair(row_1);
    const pair_2 = try parseTimestampPair(row_2);
    const pair_3 = try parseTimestampPair(row_3);
    try std.testing.expectEqual(pair_1.created_at, pair_2.created_at);
    try std.testing.expectEqual(pair_1.created_at, pair_3.created_at);
    try std.testing.expectEqual(pair_1.updated_at, pair_2.updated_at);
    try std.testing.expectEqual(pair_1.updated_at, pair_3.updated_at);
    try std.testing.expect(pair_1.updated_at > pair_1.created_at);

    result = try executor.run(
        "NowSemantics |> where(now() < now()) |> sort(id asc) { id }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=3 inserted_rows=0 updated_rows=0 deleted_rows=0\n1\n2\n3\n",
        result,
    );
}

test "feature now fails closed on invalid arity" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\NowArity {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(created_at, timestamp, nullable)
        \\}
    );

    _ = try executor.run("NowArity |> insert(id = 1, created_at = null) {}");

    const result = try executor.run(
        "NowArity |> where(id == 1) |> update(created_at = now(1)) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: update failed; class=fatal; code=TypeMismatch\n",
        result,
    );
}
