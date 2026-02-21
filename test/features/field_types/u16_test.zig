//! Feature coverage for u16 field behavior through server session path.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature u16 fields preserve 16-bit unsigned values across insert and update" {
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

    var result = try executor.run("OrderStats |> sort(id asc) { id daily_orders }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,0\n2,65535\n",
        result,
    );

    result = try executor.run("OrderStats |> where(id = 1) |> update(daily_orders = 12345) {}");
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=1 deleted_rows=0\n",
        result,
    );

    result = try executor.run("OrderStats |> where(id = 1) { id daily_orders }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,12345\n",
        result,
    );
}

test "feature u16 fields fail closed when insert value is out of range" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\OrderStatsValidation {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(daily_orders, u16, notNull)
        \\}
    );

    const result = try executor.run(
        "OrderStatsValidation |> insert(id = 1, daily_orders = 65536) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: phase=mutation code=IntegerOutOfRange path=insert.daily_orders line=1 col=55 message=\"value is out of range (0 to 65535)\"\n",
        result,
    );
}
