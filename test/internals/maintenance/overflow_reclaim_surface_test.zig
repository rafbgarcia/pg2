//! Internal reclaim-surface checks for overflow lifecycle bookkeeping.
//!
//! Responsibilities in this file:
//! - Verifies reclaim queue drain behavior after overflow-backed deletes.
//! - Verifies inspect counters expose reclaim queue depth and throughput.
const std = @import("std");
const pg2 = @import("pg2");
const overflow_mod = pg2.storage.overflow;
const wal_mod = pg2.storage.wal;
const internal = @import("../../harness/internal_env.zig");

test "internal overflow delete drains reclaim queue deterministically" {
    var env: internal.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\}
    );
    env.catalog.overflow_page_allocator = try overflow_mod.PageIdAllocator.initWithBounds(12_000, 8);

    var long_name: [1200]u8 = undefined;
    @memset(long_name[0..], 'x');
    var insert_req_buf: [1500]u8 = undefined;
    const insert_req = try std.fmt.bufPrint(
        insert_req_buf[0..],
        "User |> insert(id = 1, name = \"{s}\") {{}}",
        .{long_name[0..]},
    );
    _ = try executor.run(insert_req);

    var result = try executor.run("User |> where(id == 1) |> delete {}");
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=0 deleted_rows=1\n",
        result,
    );
    result = try executor.run("User |> where(id == 1) { id name }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=0 deleted_rows=0\n",
        result,
    );
    try std.testing.expect(env.catalog.overflow_reclaim_queue.isEmpty());
}

test "internal inspect exposes overflow reclaim backlog and throughput counters" {
    var env: internal.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\  field(bio, string, notNull)
        \\}
    );
    env.catalog.overflow_page_allocator = try overflow_mod.PageIdAllocator.initWithBounds(13_000, 16);

    var long_name_a: [1200]u8 = undefined;
    @memset(long_name_a[0..], 'a');
    var long_bio_a: [1200]u8 = undefined;
    @memset(long_bio_a[0..], 'b');
    var insert_req_buf: [2800]u8 = undefined;
    const insert_req = try std.fmt.bufPrint(
        insert_req_buf[0..],
        "User |> insert(id = 1, name = \"{s}\", bio = \"{s}\") {{}}",
        .{ long_name_a[0..], long_bio_a[0..] },
    );
    var result = try executor.run(insert_req);
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    var long_name_b: [1200]u8 = undefined;
    @memset(long_name_b[0..], 'x');
    var long_bio_b: [1200]u8 = undefined;
    @memset(long_bio_b[0..], 'y');
    var update_req_buf: [3000]u8 = undefined;
    const update_req = try std.fmt.bufPrint(
        update_req_buf[0..],
        "User |> where(id == 1) |> update(name = \"{s}\", bio = \"{s}\") {{}}",
        .{ long_name_b[0..], long_bio_b[0..] },
    );
    result = try executor.run(update_req);
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=1 deleted_rows=0\n",
        result,
    );

    result = try executor.run("User |> inspect {}");
    try std.testing.expect(
        std.mem.indexOf(
            u8,
            result,
            "INSPECT overflow reclaim_queue_depth=1 reclaim_enqueued_total=2 reclaim_dequeued_total=1 reclaim_chains_total=1 reclaim_pages_total=1 reclaim_failures_total=0\n",
        ) != null,
    );
    try std.testing.expect(
        std.mem.indexOf(
            u8,
            result,
            "INSPECT heap_reclaim queue_depth=0 pinned_by_snapshot=0 reclaim_enqueued_total=0 reclaim_dequeued_total=0 reclaimed_slots_total=0 reclaim_failures_total=0\n",
        ) != null,
    );
}

test "internal overflow allocator reuses reclaimed pages in LIFO order under churn" {
    var env: internal.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\}
    );
    env.catalog.overflow_page_allocator = try overflow_mod.PageIdAllocator.initWithBounds(18_000, 12);

    var long_a: [1200]u8 = undefined;
    @memset(long_a[0..], 'a');
    var insert_a_buf: [1500]u8 = undefined;
    const insert_a = try std.fmt.bufPrint(
        insert_a_buf[0..],
        "User |> insert(id = 1, name = \"{s}\") {{}}",
        .{long_a[0..]},
    );
    var result = try executor.run(insert_a);
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run("User |> where(id == 1) |> delete {}");
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=0 deleted_rows=1\n",
        result,
    );
    try std.testing.expect(env.catalog.overflow_reclaim_queue.isEmpty());

    var long_b: [1200]u8 = undefined;
    @memset(long_b[0..], 'b');
    var insert_b_buf: [1500]u8 = undefined;
    const insert_b = try std.fmt.bufPrint(
        insert_b_buf[0..],
        "User |> insert(id = 2, name = \"{s}\") {{}}",
        .{long_b[0..]},
    );
    result = try executor.run(insert_b);
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    var wal_records: [256]wal_mod.Record = undefined;
    var wal_payload: [128 * 1024]u8 = undefined;
    const decoded = try env.runtime.wal.readFromInto(1, &wal_records, &wal_payload);
    var roots: [8]u64 = undefined;
    var roots_len: usize = 0;
    for (wal_records[0..decoded.records_len]) |rec| {
        if (rec.record_type != .overflow_chain_create) continue;
        try std.testing.expectEqual(@as(usize, 16), rec.payload.len);
        roots[roots_len] = std.mem.littleToNative(u64, std.mem.bytesAsValue(u64, rec.payload[0..8]).*);
        roots_len += 1;
    }
    try std.testing.expectEqual(@as(usize, 2), roots_len);
    try std.testing.expectEqual(roots[0], roots[1]);
}
