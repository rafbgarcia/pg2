//! Feature coverage for insert defaults on omitted fields.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature insert applies schema defaults for omitted fields" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, bigint, notNull, primaryKey)
        \\  field(tier, string, notNull, default, "free")
        \\  field(marketing_opt_in, boolean, notNull, default, false)
        \\}
    );

    var result = try executor.run("User |> insert(id = 1) {}");
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run("User |> where(id = 1) { id tier marketing_opt_in }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,free,false\n",
        result,
    );
}

test "feature insert keeps explicit null semantics even when default exists" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, bigint, notNull, primaryKey)
        \\  field(display_name, string, notNull, default, "guest")
        \\}
    );

    const result = try executor.run(
        "User |> insert(id = 1, display_name = null) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: insert failed; class=fatal; code=NullNotAllowed\n",
        result,
    );
}
