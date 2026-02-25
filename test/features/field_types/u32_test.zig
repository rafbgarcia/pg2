//! Feature coverage for u32 field behavior through server session path.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature u32 fields preserve 32-bit unsigned values across insert and update" {
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

    var result = try executor.run("PageStats |> sort(id asc) { id bytes_written }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,0\n2,4294967295\n",
        result,
    );

    result = try executor.run("PageStats |> where(id == 1) |> update(bytes_written = 4096) {}");
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=1 deleted_rows=0\n",
        result,
    );

    result = try executor.run("PageStats |> where(id == 1) { id bytes_written }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,4096\n",
        result,
    );
}

test "feature u32 fields fail closed when insert value is out of range" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\PageStatsValidation {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(bytes_written, u32, notNull)
        \\}
    );

    const result = try executor.run(
        "PageStatsValidation |> insert(id = 1, bytes_written = 4294967296) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: message=\"value is out of range (0 to 4294967295)\" phase=mutation code=IntegerOutOfRange path=insert.bytes_written line=1 col=55\n",
        result,
    );
}
