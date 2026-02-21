//! Feature coverage for u16 field behavior through server session path.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature u16 fields preserve 16-bit unsigned values" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\OrderStats {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(daily_orders, u16, notNull)
        \\}
    );

    _ = try executor.run("OrderStats |> insert(id = 1, daily_orders = 0) {}");
    _ = try executor.run("OrderStats |> insert(id = 2, daily_orders = 65535) {}");

    const result = try executor.run("OrderStats |> sort(daily_orders asc) { id daily_orders }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,0\n2,65535\n",
        result,
    );
}
