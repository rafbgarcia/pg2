//! Feature coverage for u8 field behavior through server session path.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature u8 fields preserve 8-bit unsigned values" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\Packet {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(hops, u8, notNull)
        \\}
    );

    _ = try executor.run("Packet |> insert(id = 1, hops = 0) {}");
    _ = try executor.run("Packet |> insert(id = 2, hops = 255) {}");

    const result = try executor.run("Packet |> sort(hops asc) { id hops }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,0\n2,255\n",
        result,
    );
}
