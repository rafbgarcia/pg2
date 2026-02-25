//! Feature coverage for deterministic variable materialization memory boundaries.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature let list spills beyond in-memory list capacity and remains query-correct" {
    var env: feature.FeatureEnv = undefined;
    try env.initWithConfig(.{
        .max_query_slots = 1,
        .undo_max_entries = 16 * 1024,
        .undo_max_data_bytes = 2 * 1024 * 1024,
        .wal_buffer_capacity_bytes = 8 * 1024 * 1024,
        .wal_flush_threshold_bytes = 1 * 1024 * 1024,
        .query_string_arena_bytes_per_slot = 256 * 1024,
    });
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\SpillVarUser {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(active, bool, notNull)
        \\}
    );

    try executor.seedActiveRows("SpillVarUser", 1, 8205, 256);

    const result = try executor.run(
        \\let ids = SpillVarUser |> where(active == true) { id }
        \\SpillVarUser |> where(in(id, ids)) |> update(active = false) {}
        \\SpillVarUser |> where(active == false) |> sort(id asc) |> limit(1) { id }
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=8205 deleted_rows=0\n1\n",
        result,
    );
}
