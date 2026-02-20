//! Feature coverage for bigint field behavior through server session path.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature bigint fields preserve large integer values end-to-end" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\LedgerEntry {
        \\  field(id, bigint, notNull, primaryKey)
        \\  field(balance_cents, bigint, notNull)
        \\}
    );

    var result = try executor.run(
        "LedgerEntry |> insert(id = 9007199254740991, balance_cents = 1250000000000) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run(
        "LedgerEntry |> where(id = 9007199254740991) { id balance_cents }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n9007199254740991,1250000000000\n",
        result,
    );
}
