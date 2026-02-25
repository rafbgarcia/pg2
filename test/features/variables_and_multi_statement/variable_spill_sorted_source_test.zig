//! Regression coverage for sorted-source let spill feeding cross-statement in(...).
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature spilled sorted let list remains query-correct across statements" {
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
        \\SpillSortUser {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(active, bool, notNull)
        \\}
    );

    var i: u32 = 1;
    while (i <= 8205) : (i += 1) {
        var req_buf: [128]u8 = undefined;
        const req = try std.fmt.bufPrint(
            req_buf[0..],
            "SpillSortUser |> insert(id = {d}, active = true) {{}}",
            .{i},
        );
        _ = try executor.run(req);
    }

    const result = try executor.run(
        \\let ids = SpillSortUser |> where(active == true) |> sort(id asc) { id }
        \\SpillSortUser |> where(in(id, ids)) |> update(active = false) {}
        \\SpillSortUser |> where(active == false) |> sort(id asc) |> limit(1) { id }
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=8205 deleted_rows=0\n1\n",
        result,
    );
}
