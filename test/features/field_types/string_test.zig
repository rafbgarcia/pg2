//! E2E coverage for string field behavior through server session path.
const std = @import("std");
const e2e = @import("../test_env_test.zig");

test "e2e string fields preserve user-facing text values" {
    var env: e2e.E2EEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\CustomerProfile {
        \\  field(id, bigint, notNull, primaryKey)
        \\  field(display_name, string, notNull)
        \\}
    );

    var result = try executor.run(
        "CustomerProfile |> insert(id = 1, display_name = \"Ada Lovelace\") {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run(
        "CustomerProfile |> where(id = 1) { id display_name }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,Ada Lovelace\n",
        result,
    );
}
