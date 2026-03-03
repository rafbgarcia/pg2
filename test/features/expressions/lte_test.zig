//! Feature coverage for less-than-or-equal operator behavior through server session path.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature less-than-or-equal supports where expression contexts" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\Score {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(base, i64, notNull)
        \\  field(extra, i64, notNull)
        \\}
    );

    _ = try executor.run("Score |> insert(id = 1, base = 5, extra = 3) {}");
    _ = try executor.run("Score |> insert(id = 2, base = 2, extra = 4) {}");
    _ = try executor.run("Score |> insert(id = 3, base = 7, extra = 1) {}");

    const result = try executor.run(
        "Score |> where(-(base + extra) <= -6) |> sort(-(base + extra) desc, id asc) { id base extra }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=3 inserted_rows=0 updated_rows=0 deleted_rows=0\n2,2,4\n1,5,3\n3,7,1\n",
        result,
    );
}
