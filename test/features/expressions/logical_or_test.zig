//! Feature coverage for logical-or operator behavior through server session path.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature logical-or supports mixed arithmetic and membership predicates" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\WhereParity {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(base, i64, notNull)
        \\  field(bonus, i64, nullable)
        \\  field(active, bool, nullable)
        \\  field(status, string, nullable)
        \\}
    );

    _ = try executor.run("WhereParity |> insert(id = 1, base = 7, bonus = 3, active = false, status = \"open\") {}");
    _ = try executor.run("WhereParity |> insert(id = 2, base = 2, bonus = 1, active = true, status = \"archived\") {}");
    _ = try executor.run("WhereParity |> insert(id = 3, base = 5, bonus = null, active = false, status = \"open\") {}");
    _ = try executor.run("WhereParity |> insert(id = 4, base = 9, bonus = 2, active = false, status = \"archived\") {}");
    _ = try executor.run("WhereParity |> insert(id = 5, base = 1, bonus = 9, active = null, status = \"open\") {}");
    _ = try executor.run("WhereParity |> insert(id = 6, base = 3, bonus = 3, active = null, status = null) {}");

    const result = try executor.run(
        "WhereParity |> where(active == true || base + bonus >= 10 && !in(status, [\"archived\"])) |> sort(id asc) { id }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=3 inserted_rows=0 updated_rows=0 deleted_rows=0\n1\n2\n5\n",
        result,
    );
}
