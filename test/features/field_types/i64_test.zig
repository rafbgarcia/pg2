//! Feature coverage for i64 field behavior through server session path.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature i64 fields preserve large integer values across insert and update" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\LedgerEntry {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(balance_cents, i64, notNull)
        \\}
    );

    _ = try executor.run(
        "LedgerEntry |> insert(id = 1, balance_cents = -9223372036854775808) {}",
    );
    _ = try executor.run(
        "LedgerEntry |> insert(id = 2, balance_cents = 9223372036854775807) {}",
    );

    var result = try executor.run("LedgerEntry |> sort(id asc) { id balance_cents }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,-9223372036854775808\n2,9223372036854775807\n",
        result,
    );

    result = try executor.run(
        "LedgerEntry |> where(id = 1) |> update(balance_cents = 1250000000000) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=1 deleted_rows=0\n",
        result,
    );

    result = try executor.run(
        "LedgerEntry |> where(id = 1) { id balance_cents }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,1250000000000\n",
        result,
    );
}

test "feature i64 fields fail closed when insert value is out of range" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\LedgerEntryValidation {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(balance_cents, i64, notNull)
        \\}
    );

    const result = try executor.run(
        "LedgerEntryValidation |> insert(id = 1, balance_cents = -9223372036854775809) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: phase=mutation code=IntegerOutOfRange path=insert.balance_cents line=1 col=57 message=\"value is out of range (-9223372036854775808 to 9223372036854775807)\"\n",
        result,
    );
}
