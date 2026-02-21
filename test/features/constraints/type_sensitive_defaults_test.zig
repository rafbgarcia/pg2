//! Feature coverage for type-sensitive default and nullability constraint behavior.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature insert applies typed defaults across representative field types" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\Settings {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(enabled, bool, notNull, default, true)
        \\  field(retry_budget, i64, notNull, default, 3)
        \\  field(label, string, nullable, default, "anon")
        \\  field(created_at, timestamp, notNull, default, 1700000000123456)
        \\}
    );

    var result = try executor.run("Settings |> insert(id = 1) {}");
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run(
        "Settings |> where(id == 1) { id enabled retry_budget label created_at }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,true,3,anon,1700000000123456\n",
        result,
    );
}

test "feature insert explicit null bypasses nullable bool default" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\Preference {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(email_opt_in, bool, nullable, default, true)
        \\}
    );

    var result = try executor.run("Preference |> insert(id = 1, email_opt_in = null) {}");
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run("Preference |> where(id == 1) { id email_opt_in }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,null\n",
        result,
    );
}

test "feature schema rejects out-of-range default for constrained integer type" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try std.testing.expectError(
        error.InvalidSchema,
        executor.applyDefinitions(
            \\Packet {
            \\  field(id, i64, notNull, primaryKey)
            \\  field(hops, u8, notNull, default, 256)
            \\}
        ),
    );
}

test "feature insert explicit out-of-range integer does not fallback to default" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\Packet {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(hops, u8, notNull, default, 8)
        \\}
    );

    const result = try executor.run(
        "Packet |> insert(id = 1, hops = 300) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: phase=mutation code=IntegerOutOfRange path=insert.hops line=1 col=33 message=\"value is out of range (0 to 255)\"\n",
        result,
    );
}
