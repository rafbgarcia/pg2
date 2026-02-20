//! E2E coverage for overflow-string lifecycle through server session path.
//!
//! Responsibilities in this file:
//! - Verifies insert/update/read behavior for oversized string values.
//! - Validates reclaim queue accounting and inspect surface output.
//! - Exercises crash-recovery WAL replay behavior for overflow reclaim paths.
const std = @import("std");
const buffer_pool_mod = @import("../../storage/buffer_pool.zig");
const overflow_mod = @import("../../storage/overflow.zig");
const page_mod = @import("../../storage/page.zig");
const recovery_mod = @import("../../storage/recovery.zig");
const wal_mod = @import("../../storage/wal.zig");
const e2e = @import("test_env.zig");

test "e2e overflow insert update and read via session path" {
    var env: e2e.E2EEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, bigint, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\}
    );
    env.catalog.overflow_page_allocator = try overflow_mod.PageIdAllocator.initWithBounds(11_000, 12);

    var long_name_a: [1200]u8 = undefined;
    @memset(long_name_a[0..], 'a');
    var insert_req_buf: [1500]u8 = undefined;
    const insert_req = try std.fmt.bufPrint(
        insert_req_buf[0..],
        "User |> insert(id = 1, name = \"{s}\")",
        .{long_name_a[0..]},
    );
    var result = try executor.run(insert_req);
    try std.testing.expectEqualStrings("OK rows=0\n", result);

    result = try executor.run("User |> where(id = 1)");
    var expected_select_a: [1300]u8 = undefined;
    const expected_a = try std.fmt.bufPrint(
        expected_select_a[0..],
        "OK rows=1\n1,{s}\n",
        .{long_name_a[0..]},
    );
    try std.testing.expectEqualStrings(expected_a, result);

    var long_name_b: [1200]u8 = undefined;
    @memset(long_name_b[0..], 'b');
    var update_req_buf: [1700]u8 = undefined;
    const update_req = try std.fmt.bufPrint(
        update_req_buf[0..],
        "User |> where(id = 1) |> update(name = \"{s}\")",
        .{long_name_b[0..]},
    );
    result = try executor.run(update_req);
    try std.testing.expectEqualStrings("OK rows=0\n", result);

    result = try executor.run("User |> where(id = 1)");
    var expected_select_b: [1300]u8 = undefined;
    const expected_b = try std.fmt.bufPrint(
        expected_select_b[0..],
        "OK rows=1\n1,{s}\n",
        .{long_name_b[0..]},
    );
    try std.testing.expectEqualStrings(expected_b, result);
}

test "e2e overflow delete drains reclaim queue deterministically" {
    var env: e2e.E2EEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, bigint, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\}
    );
    env.catalog.overflow_page_allocator = try overflow_mod.PageIdAllocator.initWithBounds(12_000, 8);

    var long_name: [1200]u8 = undefined;
    @memset(long_name[0..], 'x');
    var insert_req_buf: [1500]u8 = undefined;
    const insert_req = try std.fmt.bufPrint(
        insert_req_buf[0..],
        "User |> insert(id = 1, name = \"{s}\")",
        .{long_name[0..]},
    );
    _ = try executor.run(insert_req);

    var result = try executor.run("User |> where(id = 1) |> delete");
    try std.testing.expectEqualStrings("OK rows=0\n", result);
    result = try executor.run("User |> where(id = 1)");
    try std.testing.expectEqualStrings("OK rows=0\n", result);
    try std.testing.expect(env.catalog.overflow_reclaim_queue.isEmpty());
}

test "e2e inspect exposes overflow reclaim backlog and throughput counters" {
    var env: e2e.E2EEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, bigint, notNull, primaryKey)
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
        "User |> insert(id = 1, name = \"{s}\", bio = \"{s}\")",
        .{ long_name_a[0..], long_bio_a[0..] },
    );
    var result = try executor.run(insert_req);
    try std.testing.expectEqualStrings("OK rows=0\n", result);

    var long_name_b: [1200]u8 = undefined;
    @memset(long_name_b[0..], 'x');
    var long_bio_b: [1200]u8 = undefined;
    @memset(long_bio_b[0..], 'y');
    var update_req_buf: [3000]u8 = undefined;
    const update_req = try std.fmt.bufPrint(
        update_req_buf[0..],
        "User |> where(id = 1) |> update(name = \"{s}\", bio = \"{s}\")",
        .{ long_name_b[0..], long_bio_b[0..] },
    );
    result = try executor.run(update_req);
    try std.testing.expectEqualStrings("OK rows=0\n", result);

    result = try executor.run("User |> inspect");
    try std.testing.expect(
        std.mem.indexOf(
            u8,
            result,
            "INSPECT overflow reclaim_queue_depth=1 reclaim_enqueued_total=2 reclaim_dequeued_total=1 reclaim_chains_total=1 reclaim_pages_total=1 reclaim_failures_total=0\n",
        ) != null,
    );
}

test "e2e overflow reclaim WAL replay restores page state after crash and is idempotent" {
    var env: e2e.E2EEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, bigint, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\}
    );
    env.catalog.overflow_page_allocator = try overflow_mod.PageIdAllocator.initWithBounds(14_000, 12);

    var long_name: [1200]u8 = undefined;
    @memset(long_name[0..], 'z');
    var insert_req_buf: [1500]u8 = undefined;
    const insert_req = try std.fmt.bufPrint(
        insert_req_buf[0..],
        "User |> insert(id = 1, name = \"{s}\")",
        .{long_name[0..]},
    );
    var result = try executor.run(insert_req);
    try std.testing.expectEqualStrings("OK rows=0\n", result);

    // Persist overflow chain pages before delete so replay must reclaim them.
    try env.runtime.pool.flushAll();

    result = try executor.run("User |> where(id = 1) |> delete");
    try std.testing.expectEqualStrings("OK rows=0\n", result);
    try env.runtime.wal.flush();

    env.disk.crash();

    var recovered_pool = try buffer_pool_mod.BufferPool.init(
        std.testing.allocator,
        env.disk.storage(),
        16,
    );
    defer recovered_pool.deinit();

    var recovered_wal = wal_mod.Wal.init(std.testing.allocator, env.disk.storage());
    defer recovered_wal.deinit();
    try recovered_wal.recover();

    var wal_records: [128]wal_mod.Record = undefined;
    var wal_payloads: [64 * 1024]u8 = undefined;
    const decoded = try recovered_wal.readFromInto(1, &wal_records, &wal_payloads);
    var overflow_root: u64 = 0;
    for (wal_records[0..decoded.records_len]) |rec| {
        if (rec.record_type != .overflow_chain_create) continue;
        overflow_root = std.mem.littleToNative(u64, std.mem.bytesAsValue(u64, rec.payload[0..8]).*);
        break;
    }
    try std.testing.expect(overflow_root != 0);

    {
        const page = try recovered_pool.pin(overflow_root);
        defer recovered_pool.unpin(overflow_root, false);
        try std.testing.expectEqual(page_mod.PageType.overflow, page.header.page_type);
    }

    var replay_records_a: [128]wal_mod.Record = undefined;
    var replay_payload_a: [64 * 1024]u8 = undefined;
    const first_replay = try recovery_mod.replayCommittedOverflowLifecycle(
        &recovered_pool,
        &recovered_wal,
        &env.catalog.overflow_page_allocator,
        replay_records_a[0..],
        replay_payload_a[0..],
    );
    try std.testing.expectEqual(@as(usize, 1), first_replay.overflow_reclaim_applied);
    try std.testing.expectEqual(@as(usize, 0), first_replay.overflow_reclaim_idempotent_skips);
    try recovered_pool.flushAll();

    {
        const page = try recovered_pool.pin(overflow_root);
        defer recovered_pool.unpin(overflow_root, false);
        try std.testing.expectEqual(page_mod.PageType.free, page.header.page_type);
    }

    var replay_records_b: [128]wal_mod.Record = undefined;
    var replay_payload_b: [64 * 1024]u8 = undefined;
    const second_replay = try recovery_mod.replayCommittedOverflowLifecycle(
        &recovered_pool,
        &recovered_wal,
        &env.catalog.overflow_page_allocator,
        replay_records_b[0..],
        replay_payload_b[0..],
    );
    try std.testing.expectEqual(first_replay.total_records, second_replay.total_records);
    try std.testing.expectEqual(
        first_replay.overflow_reclaim_records_seen,
        second_replay.overflow_reclaim_records_seen,
    );
    try std.testing.expectEqual(@as(usize, 0), second_replay.overflow_reclaim_applied);
    try std.testing.expectEqual(@as(usize, 1), second_replay.overflow_reclaim_idempotent_skips);
}
