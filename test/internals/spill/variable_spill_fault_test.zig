//! Internal fault-injection coverage for request-variable spill materialization.
const std = @import("std");
const pg2 = @import("pg2");
const internal = @import("../../features/test_env_test.zig");
const mutation_mod = pg2.executor.mutation;

test "internal variable spill write fault fails closed deterministically" {
    var env: internal.FeatureEnv = undefined;
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
        \\SpillFaultUser {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(active, bool, notNull)
        \\}
    );

    var i: u32 = 1;
    while (i <= 8205) : (i += 1) {
        var req_buf: [128]u8 = undefined;
        const req = try std.fmt.bufPrint(
            req_buf[0..],
            "SpillFaultUser |> insert(id = {d}, active = true) {{}}",
            .{i},
        );
        _ = try executor.run(req);
    }

    env.disk.failWriteAt(env.disk.writes + 1);
    var pool_conn = try executor.pool.checkout();
    const failed_result = try executor.session.handleRequest(
        &executor.pool,
        &pool_conn,
        \\let ids = SpillFaultUser |> where(active == true) { id }
        \\SpillFaultUser |> where(in(id, ids)) |> update(active = false) {}
        \\SpillFaultUser |> where(active == false) |> sort(id asc) |> limit(1) { id }
        ,
        null,
        executor.response_buf[0..],
    );
    const failed = executor.response_buf[0..failed_result.bytes_written];
    if (failed_result.is_query_error) {
        mutation_mod.rollbackOverflowReclaimEntriesForTx(
            executor.catalog,
            pool_conn.tx_id,
        );
        _ = executor.pool.abortCheckin(&pool_conn) catch {};
    } else {
        try executor.pool.checkin(&pool_conn);
    }
    try std.testing.expect(std.mem.indexOf(u8, failed, "ERR query: statement_index=0") != null);
    try std.testing.expect(std.mem.indexOf(u8, failed, "spill") != null);

    const recovered = try executor.run(
        \\let ids = SpillFaultUser |> where(active == true) { id }
        \\SpillFaultUser |> where(in(id, ids)) |> update(active = false) {}
        \\SpillFaultUser |> where(active == false) |> sort(id asc) |> limit(1) { id }
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=8205 deleted_rows=0\n1\n",
        recovered,
    );
}
