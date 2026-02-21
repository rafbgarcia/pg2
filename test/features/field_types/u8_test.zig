//! Feature coverage for u8 field behavior through server session path.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature u8 fields preserve 8-bit unsigned values across insert and update" {
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

    var result = try executor.run("Packet |> sort(id asc) { id hops }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,0\n2,255\n",
        result,
    );

    result = try executor.run("Packet |> where(id = 1) |> update(hops = 200) {}");
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=1 deleted_rows=0\n",
        result,
    );

    result = try executor.run("Packet |> where(id = 1) { id hops }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,200\n",
        result,
    );
}

test "feature u8 fields fail closed when insert value is out of range" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\PacketValidation {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(hops, u8, notNull)
        \\}
    );

    const result = try executor.run(
        "PacketValidation |> insert(id = 1, hops = 256) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: phase=mutation code=IntegerOutOfRange path=insert.hops line=1 col=43 message=\"value is out of range (0 to 255)\"\n",
        result,
    );
}
