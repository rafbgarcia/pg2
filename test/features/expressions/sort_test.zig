//! Feature coverage for sort expression behavior through server session path.
const std = @import("std");
const feature = @import("../test_env_test.zig");

fn expectContains(haystack: []const u8, needle: []const u8) !void {
    try std.testing.expect(std.mem.indexOf(u8, haystack, needle) != null);
}

test "feature sort supports arithmetic expression keys with deterministic tie-breaks" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\ScoreSort {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(base, i64, notNull)
        \\  field(extra, i64, notNull)
        \\}
    );

    _ = try executor.run("ScoreSort |> insert(id = 1, base = 7, extra = 3) {}");
    _ = try executor.run("ScoreSort |> insert(id = 2, base = 4, extra = 2) {}");
    _ = try executor.run("ScoreSort |> insert(id = 3, base = 6, extra = 6) {}");
    _ = try executor.run("ScoreSort |> insert(id = 4, base = 5, extra = 5) {}");

    var result = try executor.run(
        "ScoreSort |> sort(base + extra desc, id asc) { id base extra }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=4 inserted_rows=0 updated_rows=0 deleted_rows=0\n3,6,6\n1,7,3\n4,5,5\n2,4,2\n",
        result,
    );

    result = try executor.run(
        "ScoreSort |> sort(base + extra asc, id desc) { id }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=4 inserted_rows=0 updated_rows=0 deleted_rows=0\n2\n4\n1\n3\n",
        result,
    );
}

test "feature sort supports function-based expression keys" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\TicketSort {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(status, string, notNull)
        \\}
    );

    _ = try executor.run("TicketSort |> insert(id = 1, status = \"open\") {}");
    _ = try executor.run("TicketSort |> insert(id = 2, status = \"closed\") {}");
    _ = try executor.run("TicketSort |> insert(id = 3, status = \"triage\") {}");
    _ = try executor.run("TicketSort |> insert(id = 4, status = \"open\") {}");

    const result = try executor.run(
        "TicketSort |> sort(in(status, [\"open\", \"triage\"]) desc, id asc) { id status }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=4 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,open\n3,triage\n4,open\n2,closed\n",
        result,
    );
}

test "feature sort fails closed when key expression evaluation fails" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\SortEvalFailure {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(value, i64, nullable)
        \\}
    );

    _ = try executor.run("SortEvalFailure |> insert(id = 1, value = 2) {}");
    _ = try executor.run("SortEvalFailure |> insert(id = 2, value = null) {}");

    const result = try executor.run(
        "SortEvalFailure |> sort(value + 1 asc) { id value }",
    );
    try expectContains(result, "ERR query: sort key evaluation failed");
}
