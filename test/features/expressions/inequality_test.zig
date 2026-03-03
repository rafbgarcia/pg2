//! Feature coverage for inequality operator behavior through server session path.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature inequality preserves null semantics in computed select context" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\ComputedSelectNullEq {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(status, string, nullable)
        \\}
    );

    _ = try executor.run("ComputedSelectNullEq |> insert(id = 1, status = \"open\") {}");
    _ = try executor.run("ComputedSelectNullEq |> insert(id = 2, status = \"closed\") {}");
    _ = try executor.run("ComputedSelectNullEq |> insert(id = 3, status = null) {}");

    const result = try executor.run(
        "ComputedSelectNullEq |> sort(id asc) { id probe: status == null || status != null }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=3 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,true\n2,true\n3,true\n",
        result,
    );
}
