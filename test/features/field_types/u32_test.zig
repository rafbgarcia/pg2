//! Feature coverage for u32 field behavior through server session path.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature u32 fields preserve 32-bit unsigned values" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\PageStats {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(bytes_written, u32, notNull)
        \\}
    );

    _ = try executor.run("PageStats |> insert(id = 1, bytes_written = 0) {}");
    _ = try executor.run("PageStats |> insert(id = 2, bytes_written = 4294967295) {}");

    const result = try executor.run("PageStats |> sort(bytes_written asc) { id bytes_written }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,0\n2,4294967295\n",
        result,
    );
}
