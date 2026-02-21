//! Feature coverage for timestamp field behavior through server session path.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature timestamp fields preserve epoch microseconds" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\AuditEntry {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(recorded_at, timestamp, notNull)
        \\}
    );

    _ = try executor.run("AuditEntry |> insert(id = 1, recorded_at = 1700000000123456) {}");

    const result = try executor.run("AuditEntry |> where(id = 1) { id recorded_at }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,1700000000123456\n",
        result,
    );
}
