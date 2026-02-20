//! Internal crash/fault matrix for commit-hook overflow reclaim behavior.
//!
//! Responsibilities in this file:
//! - Verifies replay applies only committed reclaim records after crash.
//! - Validates deterministic page-state outcomes across crash checkpoints.
const std = @import("std");
const pg2 = @import("pg2");
const buffer_pool_mod = pg2.storage.buffer_pool;
const overflow_mod = pg2.storage.overflow;
const page_mod = pg2.storage.page;
const recovery_mod = pg2.storage.recovery;
const wal_mod = pg2.storage.wal;
const internal = @import("../../features/test_env_test.zig");

const PageType = page_mod.PageType;
const Record = wal_mod.Record;

const OverflowChainRecordMeta = struct {
    first_page_id: u64,
    page_count: u32,
    payload_bytes: u32,
};

const CrashPoint = enum {
    after_update_commit,
    after_followup_commit,
};

const ScenarioOutcome = struct {
    replay_stats: recovery_mod.ReplayStats,
    pre_crash_reclaim_len: usize,
    pre_crash_tx_begin_len: usize,
    pre_crash_tx_commit_len: usize,
    unlink_roots: [8]u64,
    unlink_len: usize,
    reclaim_roots: [8]u64,
    reclaim_len: usize,
    page_types_after_replay: [8]PageType,
};

fn decodeOverflowChainRecordMeta(payload: []const u8) !OverflowChainRecordMeta {
    if (payload.len != 16) return error.Corruption;
    return .{
        .first_page_id = std.mem.littleToNative(u64, std.mem.bytesAsValue(u64, payload[0..8]).*),
        .page_count = std.mem.littleToNative(u32, std.mem.bytesAsValue(u32, payload[8..12]).*),
        .payload_bytes = std.mem.littleToNative(u32, std.mem.bytesAsValue(u32, payload[12..16]).*),
    };
}

fn containsRoot(roots: []const u64, root: u64) bool {
    for (roots) |item| {
        if (item == root) return true;
    }
    return false;
}

fn runCrashScenario(crash_point: CrashPoint) !ScenarioOutcome {
    var env: internal.FeatureEnv = undefined;
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
    env.catalog.overflow_page_allocator = try overflow_mod.PageIdAllocator.initWithBounds(16_000, 32);

    var name_a: [1200]u8 = undefined;
    @memset(name_a[0..], 'a');
    var bio_a: [1200]u8 = undefined;
    @memset(bio_a[0..], 'b');
    var insert_buf: [2800]u8 = undefined;
    const insert_req = try std.fmt.bufPrint(
        insert_buf[0..],
        "User |> insert(id = 1, name = \"{s}\", bio = \"{s}\") {{}}",
        .{ name_a[0..], bio_a[0..] },
    );
    var result = try executor.run(insert_req);
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    // Persist initial overflow chains so replay has durable pre-reclaim page state.
    try env.runtime.pool.flushAll();

    var name_b: [1200]u8 = undefined;
    @memset(name_b[0..], 'x');
    var bio_b: [1200]u8 = undefined;
    @memset(bio_b[0..], 'y');
    var update_buf: [3000]u8 = undefined;
    const update_req = try std.fmt.bufPrint(
        update_buf[0..],
        "User |> where(id = 1) |> update(name = \"{s}\", bio = \"{s}\") {{}}",
        .{ name_b[0..], bio_b[0..] },
    );
    result = try executor.run(update_req);
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=1 deleted_rows=0\n",
        result,
    );
    try std.testing.expectEqual(@as(usize, 1), env.catalog.overflow_reclaim_queue.len);
    try std.testing.expectEqual(
        overflow_mod.ReclaimEntryState.committed,
        env.catalog.overflow_reclaim_queue.entries[env.catalog.overflow_reclaim_queue.head].state,
    );

    if (crash_point == .after_followup_commit) {
        // Follow-up successful mutation creates another commit boundary that drains one more chain.
        result = try executor.run("User |> insert(id = 2, name = \"n\", bio = \"b\") {}");
        try std.testing.expectEqualStrings(
            "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
            result,
        );
        try std.testing.expect(env.catalog.overflow_reclaim_queue.isEmpty());
    }

    var pre_records: [512]Record = undefined;
    var pre_payloads: [512 * 1024]u8 = undefined;
    const pre_decoded = try env.runtime.wal.readFromInto(1, &pre_records, &pre_payloads);
    var pre_crash_reclaim_len: usize = 0;
    var pre_crash_tx_begin_len: usize = 0;
    var pre_crash_tx_commit_len: usize = 0;
    for (pre_records[0..pre_decoded.records_len]) |rec| {
        if (rec.record_type == .overflow_chain_reclaim) pre_crash_reclaim_len += 1;
        if (rec.record_type == .tx_begin) pre_crash_tx_begin_len += 1;
        if (rec.record_type == .tx_commit) pre_crash_tx_commit_len += 1;
    }

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

    var wal_records: [512]Record = undefined;
    var wal_payloads: [512 * 1024]u8 = undefined;
    const decoded = try recovered_wal.readFromInto(1, &wal_records, &wal_payloads);

    var unlink_roots: [8]u64 = undefined;
    var unlink_len: usize = 0;
    var reclaim_roots: [8]u64 = undefined;
    var reclaim_len: usize = 0;
    for (wal_records[0..decoded.records_len]) |rec| {
        if (rec.record_type != .overflow_chain_unlink and rec.record_type != .overflow_chain_reclaim) continue;
        const meta = try decodeOverflowChainRecordMeta(rec.payload);
        if (rec.record_type == .overflow_chain_unlink) {
            std.debug.assert(unlink_len < unlink_roots.len);
            unlink_roots[unlink_len] = meta.first_page_id;
            unlink_len += 1;
        } else {
            std.debug.assert(reclaim_len < reclaim_roots.len);
            reclaim_roots[reclaim_len] = meta.first_page_id;
            reclaim_len += 1;
        }
    }

    var replay_records: [512]Record = undefined;
    var replay_payload: [512 * 1024]u8 = undefined;
    const replay_stats = try recovery_mod.replayCommittedOverflowLifecycle(
        &recovered_pool,
        &recovered_wal,
        &env.catalog.overflow_page_allocator,
        replay_records[0..],
        replay_payload[0..],
    );

    var page_types_after_replay: [8]PageType = undefined;
    for (unlink_roots[0..unlink_len], 0..) |root, idx| {
        const page = try recovered_pool.pin(root);
        defer recovered_pool.unpin(root, false);
        page_types_after_replay[idx] = page.header.page_type;
    }

    return .{
        .replay_stats = replay_stats,
        .pre_crash_reclaim_len = pre_crash_reclaim_len,
        .pre_crash_tx_begin_len = pre_crash_tx_begin_len,
        .pre_crash_tx_commit_len = pre_crash_tx_commit_len,
        .unlink_roots = unlink_roots,
        .unlink_len = unlink_len,
        .reclaim_roots = reclaim_roots,
        .reclaim_len = reclaim_len,
        .page_types_after_replay = page_types_after_replay,
    };
}

test "internal crash matrix: crash after update commit replays one committed reclaim and keeps unrecorded unlink overflow" {
    const outcome = try runCrashScenario(.after_update_commit);

    try std.testing.expectEqual(@as(usize, 1), outcome.pre_crash_reclaim_len);
    try std.testing.expectEqual(@as(usize, 2), outcome.pre_crash_tx_begin_len);
    try std.testing.expectEqual(@as(usize, 2), outcome.pre_crash_tx_commit_len);
    try std.testing.expectEqual(@as(usize, 2), outcome.unlink_len);
    try std.testing.expectEqual(@as(usize, 1), outcome.reclaim_len);
    try std.testing.expectEqual(@as(usize, 1), outcome.replay_stats.overflow_reclaim_records_seen);

    for (outcome.unlink_roots[0..outcome.unlink_len], 0..) |root, idx| {
        const expected: PageType = if (containsRoot(outcome.reclaim_roots[0..outcome.reclaim_len], root)) .free else .overflow;
        try std.testing.expectEqual(expected, outcome.page_types_after_replay[idx]);
    }
}

test "internal crash matrix: follow-up write commit drains backlog and replay reclaims all unlinked roots" {
    const outcome = try runCrashScenario(.after_followup_commit);

    try std.testing.expectEqual(@as(usize, 2), outcome.pre_crash_reclaim_len);
    try std.testing.expectEqual(@as(usize, 3), outcome.pre_crash_tx_begin_len);
    try std.testing.expectEqual(@as(usize, 3), outcome.pre_crash_tx_commit_len);
    try std.testing.expectEqual(@as(usize, 2), outcome.unlink_len);
    try std.testing.expectEqual(@as(usize, 2), outcome.reclaim_len);
    try std.testing.expectEqual(@as(usize, 2), outcome.replay_stats.overflow_reclaim_records_seen);

    for (outcome.page_types_after_replay[0..outcome.unlink_len]) |page_type| {
        try std.testing.expectEqual(PageType.free, page_type);
    }
}

test "internal crash matrix: repeated replay cycles remain idempotent after durable multi-chain reclaim" {
    var env: internal.FeatureEnv = undefined;
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
    env.catalog.overflow_page_allocator = try overflow_mod.PageIdAllocator.initWithBounds(17_000, 32);

    var name_a: [1200]u8 = undefined;
    @memset(name_a[0..], 'a');
    var bio_a: [1200]u8 = undefined;
    @memset(bio_a[0..], 'b');
    var insert_buf: [2800]u8 = undefined;
    const insert_req = try std.fmt.bufPrint(
        insert_buf[0..],
        "User |> insert(id = 1, name = \"{s}\", bio = \"{s}\") {{}}",
        .{ name_a[0..], bio_a[0..] },
    );
    var result = try executor.run(insert_req);
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );
    try env.runtime.pool.flushAll();

    var name_b: [1200]u8 = undefined;
    @memset(name_b[0..], 'x');
    var bio_b: [1200]u8 = undefined;
    @memset(bio_b[0..], 'y');
    var update_buf: [3000]u8 = undefined;
    const update_req = try std.fmt.bufPrint(
        update_buf[0..],
        "User |> where(id = 1) |> update(name = \"{s}\", bio = \"{s}\") {{}}",
        .{ name_b[0..], bio_b[0..] },
    );
    result = try executor.run(update_req);
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=1 deleted_rows=0\n",
        result,
    );

    // A second write commit boundary drains the second committed chain.
    result = try executor.run("User |> insert(id = 2, name = \"n\", bio = \"b\") {}");
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );
    try std.testing.expect(env.catalog.overflow_reclaim_queue.isEmpty());

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

    var replay_records_a: [512]Record = undefined;
    var replay_payload_a: [512 * 1024]u8 = undefined;
    const first_replay = try recovery_mod.replayCommittedOverflowLifecycle(
        &recovered_pool,
        &recovered_wal,
        &env.catalog.overflow_page_allocator,
        replay_records_a[0..],
        replay_payload_a[0..],
    );
    try std.testing.expectEqual(@as(usize, 2), first_replay.overflow_reclaim_records_seen);
    try std.testing.expectEqual(@as(usize, 2), first_replay.overflow_reclaim_applied);
    try std.testing.expectEqual(@as(usize, 0), first_replay.overflow_reclaim_idempotent_skips);
    try recovered_pool.flushAll();

    var replay_records_b: [512]Record = undefined;
    var replay_payload_b: [512 * 1024]u8 = undefined;
    const second_replay = try recovery_mod.replayCommittedOverflowLifecycle(
        &recovered_pool,
        &recovered_wal,
        &env.catalog.overflow_page_allocator,
        replay_records_b[0..],
        replay_payload_b[0..],
    );
    try std.testing.expectEqual(first_replay.total_records, second_replay.total_records);
    try std.testing.expectEqual(@as(usize, 2), second_replay.overflow_reclaim_records_seen);
    try std.testing.expectEqual(@as(usize, 0), second_replay.overflow_reclaim_applied);
    try std.testing.expectEqual(@as(usize, 2), second_replay.overflow_reclaim_idempotent_skips);
}
