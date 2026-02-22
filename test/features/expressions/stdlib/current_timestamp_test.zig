//! Feature coverage for CurrentTimestamp deterministic statement semantics.
//!
//! CurrentTimestamp returns the statement-level timestamp: every reference
//! within a single statement yields the same microsecond value, matching
//! PostgreSQL's transaction-timestamp behavior.
//!
//! Unlike now() in PostgreSQL, CurrentTimestamp is a keyword (not a function
//! call), so it does not use parentheses.
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

test "feature CurrentTimestamp returns identical value for all references in a statement" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\CurrentTsSemantics {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(created_at, timestamp, nullable)
        \\  field(updated_at, timestamp, nullable)
        \\}
    );

    _ = try executor.run("CurrentTsSemantics |> insert(id = 1, created_at = null, updated_at = null) {}");
    _ = try executor.run("CurrentTsSemantics |> insert(id = 2, created_at = null, updated_at = null) {}");
    _ = try executor.run("CurrentTsSemantics |> insert(id = 3, created_at = null, updated_at = null) {}");

    var result = try executor.run(
        "CurrentTsSemantics |> update(created_at = CurrentTimestamp, updated_at = CurrentTimestamp) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=3 deleted_rows=0\n",
        result,
    );

    result = try executor.run("CurrentTsSemantics |> sort(id asc) { id created_at updated_at }");

    var lines = std.mem.splitScalar(u8, result, '\n');
    _ = lines.next() orelse return error.TestExpectedEqual;
    const row_1 = lines.next() orelse return error.TestExpectedEqual;
    const row_2 = lines.next() orelse return error.TestExpectedEqual;
    const row_3 = lines.next() orelse return error.TestExpectedEqual;

    const pair_1 = try parseTimestampPair(row_1);
    const pair_2 = try parseTimestampPair(row_2);
    const pair_3 = try parseTimestampPair(row_3);

    // All rows get the same created_at (same statement, same reference).
    try std.testing.expectEqual(pair_1.created_at, pair_2.created_at);
    try std.testing.expectEqual(pair_1.created_at, pair_3.created_at);

    // All rows get the same updated_at (same statement, same reference).
    try std.testing.expectEqual(pair_1.updated_at, pair_2.updated_at);
    try std.testing.expectEqual(pair_1.updated_at, pair_3.updated_at);

    // Both references in the same statement yield the same timestamp
    // (PostgreSQL semantics: statement-level clock, not per-reference).
    try std.testing.expectEqual(pair_1.created_at, pair_1.updated_at);

    // CurrentTimestamp < CurrentTimestamp is always false (both sides
    // are the same value), so no rows should match the predicate.
    result = try executor.run(
        "CurrentTsSemantics |> where(CurrentTimestamp < CurrentTimestamp) |> sort(id asc) { id }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=0 deleted_rows=0\n",
        result,
    );
}
