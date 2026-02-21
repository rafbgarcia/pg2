//! Feature coverage for u64 field behavior through server session path.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature u64 fields preserve 64-bit unsigned values" {
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

    const result = try executor.run("EventLog |> sort(sequence asc) { id sequence }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,0\n2,18446744073709551615\n",
        result,
    );
}
