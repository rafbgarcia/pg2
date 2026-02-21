//! Feature coverage for u64 field behavior through server session path.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature u64 fields preserve 64-bit unsigned values across insert and update" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\EventLog {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(sequence, u64, notNull)
        \\}
    );

    _ = try executor.run("EventLog |> insert(id = 1, sequence = 0) {}");
    _ = try executor.run("EventLog |> insert(id = 2, sequence = 18446744073709551615) {}");

    var result = try executor.run("EventLog |> sort(id asc) { id sequence }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,0\n2,18446744073709551615\n",
        result,
    );

    result = try executor.run("EventLog |> where(id = 1) |> update(sequence = 42) {}");
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=1 deleted_rows=0\n",
        result,
    );

    result = try executor.run("EventLog |> where(id = 1) { id sequence }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,42\n",
        result,
    );
}

test "feature u64 fields fail closed when insert value is out of range" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\EventLogValidation {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(sequence, u64, notNull)
        \\}
    );

    const result = try executor.run(
        "EventLogValidation |> insert(id = 1, sequence = -1) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: phase=mutation code=IntegerOutOfRange path=insert.sequence line=1 col=49 message=\"value is out of range (0 to 18446744073709551615)\"\n",
        result,
    );
}
