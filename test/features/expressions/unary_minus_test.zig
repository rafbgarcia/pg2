//! Feature coverage for unary minus operator behavior through server session path.
const std = @import("std");
const feature = @import("../test_env_test.zig");
const assertions = @import("../assertions.zig");

test "feature update supports unary minus on representative numeric types" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\Counter {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(v_i64, i64, notNull)
        \\  field(v_u8, u8, notNull)
        \\  field(v_u16, u16, notNull)
        \\  field(v_u32, u32, notNull)
        \\  field(v_u64, u64, notNull)
        \\  field(v_f64, f64, notNull)
        \\  field(neg_i64, i64, notNull)
        \\  field(neg_from_u8, i64, notNull)
        \\  field(neg_from_u16, i64, notNull)
        \\  field(neg_from_u32, i64, notNull)
        \\  field(neg_from_u64, i64, notNull)
        \\  field(neg_f64, f64, notNull)
        \\}
    );

    _ = try executor.run(
        "Counter |> insert(id = 1, v_i64 = 41, v_u8 = 8, v_u16 = 16, v_u32 = 32, v_u64 = 9, v_f64 = 3.5, neg_i64 = 0, neg_from_u8 = 0, neg_from_u16 = 0, neg_from_u32 = 0, neg_from_u64 = 0, neg_f64 = 0.0) {}",
    );

    var result = try executor.run(
        "Counter |> where(id == 1) |> update(neg_i64 = -v_i64, neg_from_u8 = -v_u8, neg_from_u16 = -v_u16, neg_from_u32 = -v_u32, neg_from_u64 = -v_u64, neg_f64 = -(v_f64)) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=1 deleted_rows=0\n",
        result,
    );

    result = try executor.run("Counter |> where(id == 1) { id neg_i64 neg_from_u8 neg_from_u16 neg_from_u32 neg_from_u64 neg_f64 }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,-41,-8,-16,-32,-9,-3.5\n",
        result,
    );
}

test "feature update unary minus fails closed on type mismatch" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\CounterMismatch {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\}
    );

    _ = try executor.run("CounterMismatch |> insert(id = 1, name = \"41\") {}");
    const result = try executor.run(
        "CounterMismatch |> where(id == 1) |> update(name = -name) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: message=\"update failed\" phase=execution code=TypeMismatch path=query line=1 col=1\n",
        result,
    );
}

test "feature update unary minus fails closed on out-of-range constrained integer target" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\CounterOutOfRange {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(source, u64, notNull)
        \\  field(value, u8, notNull)
        \\}
    );

    _ = try executor.run("CounterOutOfRange |> insert(id = 1, source = 5, value = 5) {}");
    const result = try executor.run(
        "CounterOutOfRange |> where(id == 1) |> update(value = -source) {}",
    );

    try assertions.expectContains(result, "phase=mutation code=IntegerOutOfRange");
    try assertions.expectContains(result, "path=update.value");
    try assertions.expectContains(result, "message=\"value is out of range (0 to 255)\"");
}

test "feature unary minus propagates null operand for nullable assignment" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\CounterNull {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(value, i64, nullable)
        \\}
    );

    _ = try executor.run("CounterNull |> insert(id = 1, value = null) {}");
    const result = try executor.run(
        "CounterNull |> where(id == 1) |> update(value = -value) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=1 deleted_rows=0\n",
        result,
    );

    const verify = try executor.run("CounterNull |> where(id == 1) { id value }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,null\n",
        verify,
    );
}

test "feature update unary minus supports mixed numeric coercion into f64 target" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\MixedUnaryMinus {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(i_value, i64, notNull)
        \\  field(total, f64, notNull)
        \\}
    );

    _ = try executor.run("MixedUnaryMinus |> insert(id = 1, i_value = 9, total = 0.0) {}");
    const result = try executor.run(
        "MixedUnaryMinus |> where(id == 1) |> update(total = -i_value) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=1 deleted_rows=0\n",
        result,
    );

    const verify = try executor.run("MixedUnaryMinus |> where(id == 1) { id total }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,-9\n",
        verify,
    );
}
