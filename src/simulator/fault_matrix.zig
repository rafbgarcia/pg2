const std = @import("std");
const disk_mod = @import("disk.zig");
const wal_mod = @import("../storage/wal.zig");
const buffer_pool_mod = @import("../storage/buffer_pool.zig");
const btree_mod = @import("../storage/btree.zig");
const heap_mod = @import("../storage/heap.zig");
const tx_mod = @import("../mvcc/transaction.zig");
const undo_mod = @import("../mvcc/undo.zig");

const SimulatedDisk = disk_mod.SimulatedDisk;
const Wal = wal_mod.Wal;
const BufferPool = buffer_pool_mod.BufferPool;
const BTree = btree_mod.BTree;
const HeapPage = heap_mod.HeapPage;
const RowId = heap_mod.RowId;
const TxManager = tx_mod.TxManager;
const UndoLog = undo_mod.UndoLog;

const ScenarioOutcome = struct {
    signature: u64,
};

fn splitMix64(state: *u64) u64 {
    state.* +%= 0x9E3779B97F4A7C15;
    var z = state.*;
    z = (z ^ (z >> 30)) *% 0xBF58476D1CE4E5B9;
    z = (z ^ (z >> 27)) *% 0x94D049BB133111EB;
    return z ^ (z >> 31);
}

fn buildSeedSet(comptime count: usize, seed: u64) [count]u64 {
    var out: [count]u64 = undefined;
    var state = seed;
    var i: usize = 0;
    while (i < count) : (i += 1) {
        out[i] = splitMix64(&state);
    }
    return out;
}

fn expectReplayDeterministicAcrossSeeds(
    scenario_name: []const u8,
    seeds: []const u64,
    comptime run_scenario: fn (u64) anyerror!ScenarioOutcome,
) !void {
    for (seeds, 0..) |seed, seed_index| {
        const first = try run_scenario(seed);
        const second = try run_scenario(seed);
        if (first.signature != second.signature) {
            std.debug.print(
                "determinism mismatch scenario={s} seed_index={} seed=0x{x} first=0x{x} second=0x{x}\n",
                .{ scenario_name, seed_index, seed, first.signature, second.signature },
            );
        }
        try std.testing.expectEqual(first.signature, second.signature);
    }
}

fn runWalPartialWriteRecovery(seed: u64) !ScenarioOutcome {
    var prng = std.Random.DefaultPrng.init(seed);
    const rand = prng.random();

    var disk = SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    var wal = Wal.init(std.testing.allocator, disk.storage());
    defer wal.deinit();

    var payload: [128]u8 = undefined;
    const payload_len = 24 + rand.uintLessThan(usize, 64);
    rand.bytes(payload[0..payload_len]);

    const partial_prefix = 1 + rand.uintLessThan(usize, wal_mod.Record.header_size);
    disk.partialWriteAt(1, partial_prefix);

    _ = try wal.beginTx(1);
    _ = try wal.append(1, .insert, rand.uintLessThan(u64, 1000), payload[0..payload_len]);
    _ = try wal.commitTx(1);

    disk.crash();

    var recovered = Wal.init(std.testing.allocator, disk.storage());
    defer recovered.deinit();
    try recovered.recover();

    var records_buf: [8]wal_mod.Record = undefined;
    var payload_buf: [256]u8 = undefined;
    const decoded = try recovered.readFromInto(1, &records_buf, &payload_buf);
    const records = records_buf[0..decoded.records_len];
    try std.testing.expect(records.len <= 3);

    var h = std.hash.Wyhash.init(seed ^ 0xA11CE001);
    const len_u64: u64 = @intCast(records.len);
    h.update(std.mem.asBytes(&len_u64));
    for (records) |rec| {
        h.update(std.mem.asBytes(&rec.lsn));
        h.update(&[_]u8{@intFromEnum(rec.record_type)});
    }
    return .{ .signature = h.final() };
}

fn runPageBitflipChecksum(seed: u64) !ScenarioOutcome {
    var prng = std.Random.DefaultPrng.init(seed);
    const rand = prng.random();

    var disk = SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    var pool = try BufferPool.init(std.testing.allocator, disk.storage(), 2);
    defer pool.deinit();

    const page_id = 10 + rand.uintLessThan(u64, 16);
    const fill = rand.int(u8);
    const bitflip_offset = rand.uintLessThan(usize, 64);
    const mask: u8 = @as(u8, 1) << @as(u3, @intCast(rand.uintLessThan(u8, 7)));

    const page = try pool.pin(page_id);
    page.header.page_type = .heap;
    page.header.lsn = 1;
    @memset(&page.content, fill);
    pool.unpin(page_id, true);

    disk.bitflipWriteAt(1, bitflip_offset, mask);
    try pool.flush(page_id);
    try disk.storage().fsync();

    var pool2 = try BufferPool.init(std.testing.allocator, disk.storage(), 2);
    defer pool2.deinit();

    try std.testing.expectError(error.ChecksumMismatch, pool2.pin(page_id));

    var h = std.hash.Wyhash.init(seed ^ 0xA11CE002);
    h.update(std.mem.asBytes(&page_id));
    h.update(&[_]u8{fill});
    const off_u64: u64 = @intCast(bitflip_offset);
    h.update(std.mem.asBytes(&off_u64));
    h.update(&[_]u8{mask});
    return .{ .signature = h.final() };
}

fn runWalFsyncThenPageFlushGate(seed: u64) !ScenarioOutcome {
    var prng = std.Random.DefaultPrng.init(seed);
    const rand = prng.random();

    var disk = SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    var wal = Wal.init(std.testing.allocator, disk.storage());
    defer wal.deinit();

    var pool = try BufferPool.init(std.testing.allocator, disk.storage(), 2);
    defer pool.deinit();
    pool.wal = &wal;

    const page_id = 50 + rand.uintLessThan(u64, 16);
    var payload: [32]u8 = undefined;
    rand.bytes(&payload);
    _ = try wal.append(1, .insert, page_id, &payload);

    const page = try pool.pin(page_id);
    page.header.page_type = .heap;
    page.header.lsn = 1;
    @memset(&page.content, 0xCD);
    pool.unpin(page_id, true);

    disk.failFsyncAt(1);
    try std.testing.expectError(error.WalFsyncError, wal.flush());
    try std.testing.expectEqual(@as(u64, 0), wal.flushed_lsn);
    try std.testing.expectError(error.WalNotFlushed, pool.flush(page_id));

    var h = std.hash.Wyhash.init(seed ^ 0xA11CE003);
    h.update(std.mem.asBytes(&page_id));
    h.update(std.mem.asBytes(&wal.flushed_lsn));
    return .{ .signature = h.final() };
}

fn runWalMultiFaultInterleaving(seed: u64) !ScenarioOutcome {
    var prng = std.Random.DefaultPrng.init(seed);
    const rand = prng.random();

    var disk = SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    var wal = Wal.init(std.testing.allocator, disk.storage());
    defer wal.deinit();

    var payload1: [96]u8 = undefined;
    const payload1_len = 16 + rand.uintLessThan(usize, 48);
    rand.bytes(payload1[0..payload1_len]);
    const partial_prefix = 1 + rand.uintLessThan(usize, wal_mod.Record.header_size);
    disk.partialWriteAt(disk.writes + 1, partial_prefix);
    _ = try wal.beginTx(1);
    _ = try wal.append(1, .insert, 100 + rand.uintLessThan(u64, 32), payload1[0..payload1_len]);
    _ = try wal.commitTx(1);

    disk.crash();

    var recovered = Wal.init(std.testing.allocator, disk.storage());
    defer recovered.deinit();
    try recovered.recover();

    var first_records_buf: [8]wal_mod.Record = undefined;
    var first_payload_buf: [256]u8 = undefined;
    const first_decoded = try recovered.readFromInto(1, &first_records_buf, &first_payload_buf);
    const first_records = first_records_buf[0..first_decoded.records_len];
    try std.testing.expect(first_records.len <= 3);

    var payload2: [96]u8 = undefined;
    const payload2_len = 16 + rand.uintLessThan(usize, 48);
    rand.bytes(payload2[0..payload2_len]);
    _ = try recovered.beginTx(2);
    _ = try recovered.append(2, .insert, 200 + rand.uintLessThan(u64, 32), payload2[0..payload2_len]);
    const before_failed_flush = recovered.flushed_lsn;
    disk.failFsyncAt(disk.fsyncs + 1);
    try std.testing.expectError(error.WalFsyncError, recovered.commitTx(2));
    try std.testing.expectEqual(before_failed_flush, recovered.flushed_lsn);
    try recovered.flush();
    try std.testing.expect(recovered.flushed_lsn > before_failed_flush);

    disk.crash();

    var final_wal = Wal.init(std.testing.allocator, disk.storage());
    defer final_wal.deinit();
    try final_wal.recover();

    var final_records_buf: [16]wal_mod.Record = undefined;
    var final_payload_buf: [512]u8 = undefined;
    const final_decoded = try final_wal.readFromInto(1, &final_records_buf, &final_payload_buf);
    const final_records = final_records_buf[0..final_decoded.records_len];
    try std.testing.expect(final_records.len <= final_records_buf.len);

    var h = std.hash.Wyhash.init(seed ^ 0xA11CE004);
    const first_len_u64: u64 = @intCast(first_records.len);
    const final_len_u64: u64 = @intCast(final_records.len);
    h.update(std.mem.asBytes(&first_len_u64));
    h.update(std.mem.asBytes(&final_len_u64));
    h.update(std.mem.asBytes(&recovered.flushed_lsn));
    for (final_records) |rec| {
        h.update(std.mem.asBytes(&rec.lsn));
        h.update(std.mem.asBytes(&rec.tx_id));
        h.update(&[_]u8{@intFromEnum(rec.record_type)});
    }
    return .{ .signature = h.final() };
}

fn runCombinedWalAndPageCorruption(seed: u64) !ScenarioOutcome {
    var prng = std.Random.DefaultPrng.init(seed);
    const rand = prng.random();

    var disk = SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    var wal = Wal.init(std.testing.allocator, disk.storage());
    defer wal.deinit();

    var pool = try BufferPool.init(std.testing.allocator, disk.storage(), 2);
    defer pool.deinit();
    pool.wal = &wal;

    const page_id = 80 + rand.uintLessThan(u64, 16);
    var payload: [48]u8 = undefined;
    rand.bytes(&payload);
    _ = try wal.append(1, .insert, page_id, &payload);

    const page = try pool.pin(page_id);
    page.header.page_type = .heap;
    page.header.lsn = 1;
    @memset(&page.content, 0x7C);
    pool.unpin(page_id, true);

    disk.failFsyncAt(disk.fsyncs + 1);
    try std.testing.expectError(error.WalFsyncError, wal.flush());
    try std.testing.expectEqual(@as(u64, 0), wal.flushed_lsn);
    try std.testing.expectError(error.WalNotFlushed, pool.flush(page_id));

    try wal.flush();
    try std.testing.expect(wal.flushed_lsn >= 1);

    const bitflip_offset = rand.uintLessThan(usize, 64);
    const bitflip_mask: u8 = @as(u8, 1) << @as(u3, @intCast(rand.uintLessThan(u8, 7)));
    disk.bitflipWriteAt(disk.writes + 1, bitflip_offset, bitflip_mask);
    try pool.flush(page_id);
    try disk.storage().fsync();

    var pool2 = try BufferPool.init(std.testing.allocator, disk.storage(), 2);
    defer pool2.deinit();
    try std.testing.expectError(error.ChecksumMismatch, pool2.pin(page_id));

    disk.crash();

    var recovered = Wal.init(std.testing.allocator, disk.storage());
    defer recovered.deinit();
    try recovered.recover();

    var records_buf: [8]wal_mod.Record = undefined;
    var payload_buf: [256]u8 = undefined;
    const decoded = try recovered.readFromInto(1, &records_buf, &payload_buf);
    const records = records_buf[0..decoded.records_len];
    try std.testing.expect(records.len <= records_buf.len);

    var h = std.hash.Wyhash.init(seed ^ 0xA11CE005);
    h.update(std.mem.asBytes(&page_id));
    h.update(std.mem.asBytes(&wal.flushed_lsn));
    const records_len_u64: u64 = @intCast(records.len);
    h.update(std.mem.asBytes(&records_len_u64));
    const offset_u64: u64 = @intCast(bitflip_offset);
    h.update(std.mem.asBytes(&offset_u64));
    h.update(&[_]u8{bitflip_mask});
    for (records) |rec| {
        h.update(std.mem.asBytes(&rec.lsn));
        h.update(&[_]u8{@intFromEnum(rec.record_type)});
    }
    return .{ .signature = h.final() };
}

fn runWalRepeatedCrashRecoveryCycles(seed: u64) !ScenarioOutcome {
    var prng = std.Random.DefaultPrng.init(seed);
    const rand = prng.random();

    var disk = SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    {
        var wal = Wal.init(std.testing.allocator, disk.storage());
        defer wal.deinit();

        var payload1: [80]u8 = undefined;
        const payload1_len = 20 + rand.uintLessThan(usize, 32);
        rand.bytes(payload1[0..payload1_len]);

        _ = try wal.beginTx(1);
        _ = try wal.append(1, .insert, 300 + rand.uintLessThan(u64, 32), payload1[0..payload1_len]);
        disk.failFsyncAt(disk.fsyncs + 1);
        try std.testing.expectError(error.WalFsyncError, wal.commitTx(1));
        const failed_flush_lsn = wal.flushed_lsn;
        try wal.flush();
        try std.testing.expect(wal.flushed_lsn > failed_flush_lsn);
    }

    disk.crash();

    {
        var wal = Wal.init(std.testing.allocator, disk.storage());
        defer wal.deinit();
        try wal.recover();

        var payload2: [80]u8 = undefined;
        const payload2_len = 20 + rand.uintLessThan(usize, 32);
        rand.bytes(payload2[0..payload2_len]);

        const partial_prefix = 1 + rand.uintLessThan(usize, wal_mod.Record.header_size);
        disk.partialWriteAt(disk.writes + 1, partial_prefix);

        _ = try wal.beginTx(2);
        _ = try wal.append(2, .insert, 400 + rand.uintLessThan(u64, 32), payload2[0..payload2_len]);
        _ = try wal.commitTx(2);
    }

    disk.crash();

    {
        var wal = Wal.init(std.testing.allocator, disk.storage());
        defer wal.deinit();
        try wal.recover();

        var payload3: [80]u8 = undefined;
        const payload3_len = 20 + rand.uintLessThan(usize, 32);
        rand.bytes(payload3[0..payload3_len]);

        _ = try wal.beginTx(3);
        _ = try wal.append(3, .insert, 500 + rand.uintLessThan(u64, 32), payload3[0..payload3_len]);
        _ = try wal.commitTx(3);
    }

    disk.crash();

    var recovered = Wal.init(std.testing.allocator, disk.storage());
    defer recovered.deinit();
    try recovered.recover();

    var records_buf: [24]wal_mod.Record = undefined;
    var payload_buf: [1024]u8 = undefined;
    const decoded = try recovered.readFromInto(1, &records_buf, &payload_buf);
    const records = records_buf[0..decoded.records_len];
    try std.testing.expect(records.len <= records_buf.len);
    try std.testing.expect(recovered.flushed_lsn > 0);

    var h = std.hash.Wyhash.init(seed ^ 0xA11CE006);
    const records_len_u64: u64 = @intCast(records.len);
    h.update(std.mem.asBytes(&records_len_u64));
    h.update(std.mem.asBytes(&decoded.payload_bytes_used));
    h.update(std.mem.asBytes(&recovered.flushed_lsn));
    for (records) |rec| {
        h.update(std.mem.asBytes(&rec.lsn));
        h.update(std.mem.asBytes(&rec.tx_id));
        h.update(&[_]u8{@intFromEnum(rec.record_type)});
    }
    return .{ .signature = h.final() };
}

fn runLongWalPageInterleaving(seed: u64) !ScenarioOutcome {
    var prng = std.Random.DefaultPrng.init(seed);
    const rand = prng.random();

    var disk = SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    const page_a = 600 + rand.uintLessThan(u64, 32);
    const page_b = 700 + rand.uintLessThan(u64, 32);

    {
        var wal = Wal.init(std.testing.allocator, disk.storage());
        defer wal.deinit();

        var pool = try BufferPool.init(std.testing.allocator, disk.storage(), 4);
        defer pool.deinit();
        pool.wal = &wal;

        var payload_a: [64]u8 = undefined;
        rand.bytes(&payload_a);
        const lsn_a = try wal.append(1, .insert, page_a, &payload_a);

        const page = try pool.pin(page_a);
        page.header.page_type = .heap;
        page.header.lsn = lsn_a;
        @memset(&page.content, 0x3A);
        pool.unpin(page_a, true);

        disk.failFsyncAt(disk.fsyncs + 1);
        try std.testing.expectError(error.WalFsyncError, wal.flush());
        try std.testing.expectError(error.WalNotFlushed, pool.flush(page_a));
        try wal.flush();

        const bitflip_offset = rand.uintLessThan(usize, 64);
        const bitflip_mask: u8 = @as(u8, 1) << @as(u3, @intCast(rand.uintLessThan(u8, 7)));
        disk.bitflipWriteAt(disk.writes + 1, bitflip_offset, bitflip_mask);
        try pool.flush(page_a);
        try disk.storage().fsync();
    }

    var verify_pool_a = try BufferPool.init(std.testing.allocator, disk.storage(), 2);
    defer verify_pool_a.deinit();
    try std.testing.expectError(error.ChecksumMismatch, verify_pool_a.pin(page_a));

    disk.crash();

    {
        var wal = Wal.init(std.testing.allocator, disk.storage());
        defer wal.deinit();
        try wal.recover();

        var pool = try BufferPool.init(std.testing.allocator, disk.storage(), 4);
        defer pool.deinit();
        pool.wal = &wal;

        var payload_b: [64]u8 = undefined;
        rand.bytes(&payload_b);
        const lsn_b = try wal.append(2, .insert, page_b, &payload_b);

        const page = try pool.pin(page_b);
        page.header.page_type = .heap;
        page.header.lsn = lsn_b;
        @memset(&page.content, 0x4B);
        pool.unpin(page_b, true);

        try wal.flush();

        const partial_prefix = 16 + rand.uintLessThan(usize, 128);
        disk.partialWriteAt(disk.writes + 1, partial_prefix);
        try pool.flush(page_b);
        try disk.storage().fsync();
    }

    var verify_pool_b = try BufferPool.init(std.testing.allocator, disk.storage(), 2);
    defer verify_pool_b.deinit();
    try std.testing.expectError(error.ChecksumMismatch, verify_pool_b.pin(page_b));

    disk.crash();

    var recovered = Wal.init(std.testing.allocator, disk.storage());
    defer recovered.deinit();
    try recovered.recover();

    var records_buf: [32]wal_mod.Record = undefined;
    var payload_buf: [2048]u8 = undefined;
    const decoded = try recovered.readFromInto(1, &records_buf, &payload_buf);
    const records = records_buf[0..decoded.records_len];
    try std.testing.expect(records.len <= records_buf.len);
    try std.testing.expect(recovered.flushed_lsn > 0);

    var h = std.hash.Wyhash.init(seed ^ 0xA11CE007);
    h.update(std.mem.asBytes(&page_a));
    h.update(std.mem.asBytes(&page_b));
    h.update(std.mem.asBytes(&disk.writes));
    h.update(std.mem.asBytes(&disk.fsyncs));
    h.update(std.mem.asBytes(&recovered.flushed_lsn));
    const records_len_u64: u64 = @intCast(records.len);
    h.update(std.mem.asBytes(&records_len_u64));
    for (records) |rec| {
        h.update(std.mem.asBytes(&rec.lsn));
        h.update(std.mem.asBytes(&rec.page_id));
        h.update(&[_]u8{@intFromEnum(rec.record_type)});
    }
    return .{ .signature = h.final() };
}

fn runBufferPoolIoFaultInterleaving(seed: u64) !ScenarioOutcome {
    var prng = std.Random.DefaultPrng.init(seed);
    const rand = prng.random();

    var disk = SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    var pool = try BufferPool.init(std.testing.allocator, disk.storage(), 3);
    defer pool.deinit();

    const page_a = 900 + rand.uintLessThan(u64, 16);
    const page_b = 950 + rand.uintLessThan(u64, 16);

    // 1) Deterministic read failure then successful retry.
    disk.failReadAt(disk.reads + 1);
    try std.testing.expectError(error.StorageRead, pool.pin(page_a));
    const page_a_ptr = try pool.pin(page_a);
    page_a_ptr.header.page_type = .heap;
    page_a_ptr.header.lsn = 1;
    @memset(&page_a_ptr.content, 0x2D);
    pool.unpin(page_a, true);

    // 2) Deterministic write failure during flush, then retry.
    disk.failWriteAt(disk.writes + 1);
    try std.testing.expectError(error.StorageWrite, pool.flush(page_a));
    try pool.flush(page_a);

    // 3) Dirty second page and deterministically fail fsync in flushAll.
    const page_b_ptr = try pool.pin(page_b);
    page_b_ptr.header.page_type = .heap;
    page_b_ptr.header.lsn = 2;
    @memset(&page_b_ptr.content, 0x5E);
    pool.unpin(page_b, true);

    disk.failFsyncAt(disk.fsyncs + 1);
    try std.testing.expectError(error.StorageFsync, pool.flushAll());
    try pool.flushAll();

    var h = std.hash.Wyhash.init(seed ^ 0xA11CE008);
    h.update(std.mem.asBytes(&page_a));
    h.update(std.mem.asBytes(&page_b));
    h.update(std.mem.asBytes(&disk.reads));
    h.update(std.mem.asBytes(&disk.writes));
    h.update(std.mem.asBytes(&disk.fsyncs));
    h.update(std.mem.asBytes(&pool.flushes));
    return .{ .signature = h.final() };
}

fn runBTreeSplitFlushCrashInterleaving(seed: u64) !ScenarioOutcome {
    var prng = std.Random.DefaultPrng.init(seed);
    const rand = prng.random();

    var disk = SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    var pool = try BufferPool.init(std.testing.allocator, disk.storage(), 64);
    defer pool.deinit();

    var tree = try BTree.init(&pool, null, 0);

    var key_buf: [8]u8 = undefined;
    const insert_count: u64 = 420 + rand.uintLessThan(u64, 16);
    var i: u64 = 0;
    while (i < insert_count) : (i += 1) {
        const key_val = std.mem.nativeToBig(u64, i);
        @memcpy(&key_buf, std.mem.asBytes(&key_val));
        try tree.insert(&key_buf, RowId{ .page_id = i, .slot = 0 });
    }
    std.debug.assert(tree.root_page_id >= 2);

    const fail_target = rand.uintLessThan(u8, 3);
    const flush_page_id: u64 = switch (fail_target) {
        0 => 0, // left split page
        1 => 1, // right split page
        else => tree.root_page_id, // promoted/new root path
    };

    disk.failWriteAt(disk.writes + 1);
    try std.testing.expectError(error.StorageWrite, pool.flush(flush_page_id));

    // Best-effort flush for remaining pages before crash.
    _ = pool.flushAll() catch {};
    disk.crash();

    var verify_pool = try BufferPool.init(std.testing.allocator, disk.storage(), 64);
    defer verify_pool.deinit();
    var verify_tree = BTree{
        .root_page_id = tree.root_page_id,
        .next_page_id = tree.next_page_id,
        .pool = &verify_pool,
        .wal = null,
    };

    const probe = rand.uintLessThan(u64, insert_count);
    const probe_key_val = std.mem.nativeToBig(u64, probe);
    @memcpy(&key_buf, std.mem.asBytes(&probe_key_val));
    const lookup_outcome: u8 = blk: {
        const found = verify_tree.find(&key_buf) catch |err| {
            break :blk switch (err) {
                error.Corruption => 1,
                error.StorageRead => 2,
                error.InvalidPage => 3,
                else => 4,
            };
        };
        if (found) |row_id| {
            break :blk if (row_id.page_id == probe) 5 else 6;
        }
        break :blk 0;
    };

    var h = std.hash.Wyhash.init(seed ^ 0xA11CE00A);
    h.update(std.mem.asBytes(&insert_count));
    h.update(std.mem.asBytes(&flush_page_id));
    h.update(std.mem.asBytes(&disk.writes));
    h.update(std.mem.asBytes(&disk.fsyncs));
    h.update(std.mem.asBytes(&probe));
    h.update(&[_]u8{lookup_outcome});
    return .{ .signature = h.final() };
}

fn classifyBTreeLookup(
    tree: *BTree,
    key_buf: *[8]u8,
    expected_page_id: u64,
) u8 {
    const found = tree.find(key_buf) catch |err| {
        return switch (err) {
            error.Corruption => 1,
            error.StorageRead => 2,
            error.InvalidPage => 3,
            else => 4,
        };
    };
    if (found) |row_id| {
        return if (row_id.page_id == expected_page_id) 5 else 6;
    }
    return 0;
}

fn runBTreeSplitProtocolCutScenario(
    seed: u64,
    durable_flush_steps: u8,
    fault_mode: u8,
    fault_step: u8,
) !u64 {
    std.debug.assert(durable_flush_steps <= 3);
    std.debug.assert(fault_mode <= 3);
    std.debug.assert(fault_step <= 2);

    var prng = std.Random.DefaultPrng.init(
        seed ^ (@as(u64, durable_flush_steps) << 16) ^
            (@as(u64, fault_mode) << 8) ^ fault_step,
    );
    const rand = prng.random();

    var disk = SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    var pool = try BufferPool.init(std.testing.allocator, disk.storage(), 64);
    defer pool.deinit();

    var tree = try BTree.init(&pool, null, 0);
    var key_buf: [8]u8 = undefined;
    const insert_count: u64 = 420 + rand.uintLessThan(u64, 16);
    var i: u64 = 0;
    while (i < insert_count) : (i += 1) {
        const key_val = std.mem.nativeToBig(u64, i);
        @memcpy(&key_buf, std.mem.asBytes(&key_val));
        try tree.insert(&key_buf, RowId{ .page_id = i, .slot = 0 });
    }
    std.debug.assert(tree.root_page_id >= 2);

    const flush_plan = [3]u64{ 0, 1, tree.root_page_id };
    var step_idx: u8 = 0;
    while (step_idx < durable_flush_steps) : (step_idx += 1) {
        try pool.flush(flush_plan[step_idx]);
        try disk.storage().fsync();
    }

    const target_page = flush_plan[fault_step];
    switch (fault_mode) {
        0 => {
            disk.failWriteAt(disk.writes + 1);
            try std.testing.expectError(error.StorageWrite, pool.flush(target_page));
        },
        1 => {
            const keep_prefix = 16 + rand.uintLessThan(usize, 192);
            disk.partialWriteAt(disk.writes + 1, keep_prefix);
            try pool.flush(target_page);
            try disk.storage().fsync();
        },
        2 => {
            const bitflip_offset = rand.uintLessThan(usize, 128);
            const bitflip_mask: u8 = @as(u8, 1) << @as(u3, @intCast(rand.uintLessThan(u8, 7)));
            disk.bitflipWriteAt(disk.writes + 1, bitflip_offset, bitflip_mask);
            try pool.flush(target_page);
            try disk.storage().fsync();
        },
        3 => {},
        else => unreachable,
    }

    disk.crash();

    var verify_pool = try BufferPool.init(std.testing.allocator, disk.storage(), 64);
    defer verify_pool.deinit();
    var verify_tree = BTree{
        .root_page_id = tree.root_page_id,
        .next_page_id = tree.next_page_id,
        .pool = &verify_pool,
        .wal = null,
    };

    const probe_low: u64 = 0;
    const probe_mid: u64 = insert_count / 2;
    const probe_high: u64 = insert_count - 1;

    const low_key = std.mem.nativeToBig(u64, probe_low);
    @memcpy(&key_buf, std.mem.asBytes(&low_key));
    const low_outcome = classifyBTreeLookup(&verify_tree, &key_buf, probe_low);

    const mid_key = std.mem.nativeToBig(u64, probe_mid);
    @memcpy(&key_buf, std.mem.asBytes(&mid_key));
    const mid_outcome = classifyBTreeLookup(&verify_tree, &key_buf, probe_mid);

    const high_key = std.mem.nativeToBig(u64, probe_high);
    @memcpy(&key_buf, std.mem.asBytes(&high_key));
    const high_outcome = classifyBTreeLookup(&verify_tree, &key_buf, probe_high);

    var h = std.hash.Wyhash.init(
        seed ^ 0xA11CE00B ^ (@as(u64, durable_flush_steps) << 16) ^
            (@as(u64, fault_mode) << 8) ^ fault_step,
    );
    h.update(std.mem.asBytes(&insert_count));
    h.update(std.mem.asBytes(&tree.root_page_id));
    h.update(std.mem.asBytes(&target_page));
    h.update(std.mem.asBytes(&disk.writes));
    h.update(std.mem.asBytes(&disk.fsyncs));
    h.update(&[_]u8{ low_outcome, mid_outcome, high_outcome });
    return h.final();
}

fn runBTreeSplitProtocolCrashMatrix(seed: u64) !ScenarioOutcome {
    var h = std.hash.Wyhash.init(seed ^ 0xA11CE00C);

    var clean_step: u8 = 0;
    while (clean_step <= 3) : (clean_step += 1) {
        const sig = try runBTreeSplitProtocolCutScenario(
            seed,
            clean_step,
            3, // clean crash cut after N durable flushes
            @min(clean_step, 2),
        );
        h.update(std.mem.asBytes(&sig));
    }

    var fault_mode: u8 = 1;
    while (fault_mode <= 2) : (fault_mode += 1) {
        var step: u8 = 0;
        while (step < 3) : (step += 1) {
            const sig = try runBTreeSplitProtocolCutScenario(
                seed,
                step,
                fault_mode,
                step,
            );
            h.update(std.mem.asBytes(&sig));
        }
    }

    return .{ .signature = h.final() };
}

fn runExtendedWalBufferPoolCycles(seed: u64) !ScenarioOutcome {
    var prng = std.Random.DefaultPrng.init(seed);
    const rand = prng.random();

    var disk = SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    const page_c = 1000 + rand.uintLessThan(u64, 32);
    const page_d = 1100 + rand.uintLessThan(u64, 32);

    {
        var wal = Wal.init(std.testing.allocator, disk.storage());
        defer wal.deinit();

        var pool = try BufferPool.init(std.testing.allocator, disk.storage(), 4);
        defer pool.deinit();
        pool.wal = &wal;

        var payload_c: [64]u8 = undefined;
        rand.bytes(&payload_c);
        const lsn_c = try wal.append(1, .insert, page_c, &payload_c);

        const page = try pool.pin(page_c);
        page.header.page_type = .heap;
        page.header.lsn = lsn_c;
        @memset(&page.content, 0x6D);
        pool.unpin(page_c, true);

        disk.failFsyncAt(disk.fsyncs + 1);
        try std.testing.expectError(error.WalFsyncError, wal.flush());
        try std.testing.expectError(error.WalNotFlushed, pool.flush(page_c));
        try wal.flush();

        const partial_prefix = 8 + rand.uintLessThan(usize, 96);
        disk.partialWriteAt(disk.writes + 1, partial_prefix);
        try pool.flush(page_c);
        try disk.storage().fsync();
    }

    var verify_pool_c = try BufferPool.init(std.testing.allocator, disk.storage(), 2);
    defer verify_pool_c.deinit();
    try std.testing.expectError(error.ChecksumMismatch, verify_pool_c.pin(page_c));

    disk.crash();

    {
        var wal = Wal.init(std.testing.allocator, disk.storage());
        defer wal.deinit();
        try wal.recover();

        var pool = try BufferPool.init(std.testing.allocator, disk.storage(), 4);
        defer pool.deinit();
        pool.wal = &wal;

        var payload_d: [64]u8 = undefined;
        rand.bytes(&payload_d);
        const lsn_d = try wal.append(2, .insert, page_d, &payload_d);

        const page = try pool.pin(page_d);
        page.header.page_type = .heap;
        page.header.lsn = lsn_d;
        @memset(&page.content, 0x93);
        pool.unpin(page_d, true);

        try wal.flush();
        const bitflip_offset = rand.uintLessThan(usize, 96);
        const bitflip_mask: u8 = @as(u8, 1) << @as(u3, @intCast(rand.uintLessThan(u8, 7)));
        disk.bitflipWriteAt(disk.writes + 1, bitflip_offset, bitflip_mask);
        try pool.flush(page_d);
        try disk.storage().fsync();
    }

    var verify_pool_d = try BufferPool.init(std.testing.allocator, disk.storage(), 2);
    defer verify_pool_d.deinit();
    try std.testing.expectError(error.ChecksumMismatch, verify_pool_d.pin(page_d));

    disk.crash();

    var recovered = Wal.init(std.testing.allocator, disk.storage());
    defer recovered.deinit();
    try recovered.recover();

    var records_buf: [40]wal_mod.Record = undefined;
    var payload_buf: [3072]u8 = undefined;
    const decoded = try recovered.readFromInto(1, &records_buf, &payload_buf);
    const records = records_buf[0..decoded.records_len];
    try std.testing.expect(records.len <= records_buf.len);
    try std.testing.expect(recovered.flushed_lsn > 0);

    var h = std.hash.Wyhash.init(seed ^ 0xA11CE009);
    h.update(std.mem.asBytes(&page_c));
    h.update(std.mem.asBytes(&page_d));
    h.update(std.mem.asBytes(&disk.reads));
    h.update(std.mem.asBytes(&disk.writes));
    h.update(std.mem.asBytes(&disk.fsyncs));
    h.update(std.mem.asBytes(&recovered.flushed_lsn));
    const records_len_u64: u64 = @intCast(records.len);
    h.update(std.mem.asBytes(&records_len_u64));
    h.update(std.mem.asBytes(&decoded.payload_bytes_used));
    for (records) |rec| {
        h.update(std.mem.asBytes(&rec.lsn));
        h.update(std.mem.asBytes(&rec.page_id));
        h.update(&[_]u8{@intFromEnum(rec.record_type)});
    }
    return .{ .signature = h.final() };
}

fn runRollbackVisibilityEdge(seed: u64) !ScenarioOutcome {
    var prng = std.Random.DefaultPrng.init(seed);
    const rand = prng.random();

    var tm = TxManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo_log = try UndoLog.init(std.testing.allocator, 128, 8 * 1024);
    defer undo_log.deinit();

    const page_id: u64 = 1200 + rand.uintLessThan(u64, 32);
    const slot: u16 = rand.uintLessThan(u16, 16);

    // Baseline committed transaction.
    const tx_base = try tm.begin();
    try tm.commit(tx_base);

    // Reader starts before writer transactions.
    const tx_reader_before = try tm.begin();
    var snap_before = try tm.snapshot(tx_reader_before);
    defer snap_before.deinit();

    var old_v1: [24]u8 = undefined;
    const old_v1_len = 8 + rand.uintLessThan(usize, 8);
    rand.bytes(old_v1[0..old_v1_len]);

    var old_v2: [24]u8 = undefined;
    const old_v2_len = 8 + rand.uintLessThan(usize, 8);
    rand.bytes(old_v2[0..old_v2_len]);

    // First update commits.
    const tx_writer_1 = try tm.begin();
    _ = try undo_log.push(tx_writer_1, page_id, slot, old_v1[0..old_v1_len]);
    try tm.commit(tx_writer_1);

    // Head update aborts (rollback edge).
    const tx_writer_2 = try tm.begin();
    _ = try undo_log.push(tx_writer_2, page_id, slot, old_v2[0..old_v2_len]);
    try tm.abort(tx_writer_2);

    // Reader starts after the aborted head transaction.
    const tx_reader_after = try tm.begin();
    var snap_after = try tm.snapshot(tx_reader_after);
    defer snap_after.deinit();

    const before_visible = undo_log.findVisible(page_id, slot, &snap_before, &tm);
    try std.testing.expect(before_visible != null);
    try std.testing.expectEqualSlices(u8, old_v1[0..old_v1_len], before_visible.?);

    const after_visible = undo_log.findVisible(page_id, slot, &snap_after, &tm);
    try std.testing.expect(after_visible != null);
    try std.testing.expectEqualSlices(u8, old_v2[0..old_v2_len], after_visible.?);

    try tm.commit(tx_reader_before);
    try tm.commit(tx_reader_after);

    const old_len = undo_log.len();
    undo_log.truncate(tm.getOldestActive());
    try std.testing.expect(undo_log.len() <= old_len);
    try std.testing.expectEqual(@as(u32, 0), undo_log.len());

    var h = std.hash.Wyhash.init(seed ^ 0xA11CE00D);
    h.update(std.mem.asBytes(&page_id));
    h.update(std.mem.asBytes(&slot));
    h.update(std.mem.asBytes(&old_len));
    h.update(std.mem.asBytes(&undo_log.len()));
    h.update(old_v1[0..old_v1_len]);
    h.update(old_v2[0..old_v2_len]);
    h.update(before_visible.?);
    h.update(after_visible.?);
    return .{ .signature = h.final() };
}

fn runWalUndoCrashVisibilityConsistency(seed: u64) !ScenarioOutcome {
    var prng = std.Random.DefaultPrng.init(seed);
    const rand = prng.random();

    const page_id: u64 = 1400 + rand.uintLessThan(u64, 32);
    const wal_fixed_capacity: usize = 32;

    var old_row: [48]u8 = undefined;
    const old_row_len = 12 + rand.uintLessThan(usize, 12);
    rand.bytes(old_row[0..old_row_len]);

    var failed_new_row: [48]u8 = undefined;
    const failed_new_row_len = if (old_row_len > 1)
        1 + rand.uintLessThan(usize, old_row_len)
    else
        @as(usize, 1);
    rand.bytes(failed_new_row[0..failed_new_row_len]);

    var slot: u16 = 0;
    var visible_before_crash_len: usize = 0;
    var visible_before_crash: [48]u8 = undefined;

    var disk = SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    {
        var pool = try BufferPool.init(std.testing.allocator, disk.storage(), 8);
        defer pool.deinit();

        var wal = Wal.init(std.testing.allocator, disk.storage());
        defer wal.deinit();
        pool.wal = &wal;

        var tm = TxManager.init(std.testing.allocator);
        defer tm.deinit();
        var undo_log = try UndoLog.init(std.testing.allocator, 128, 8 * 1024);
        defer undo_log.deinit();

        const page = try pool.pin(page_id);
        HeapPage.init(page);
        pool.unpin(page_id, true);

        const tx_insert = try tm.begin();
        _ = try wal.beginTx(tx_insert);
        const insert_lsn = try wal.append(tx_insert, .insert, page_id, old_row[0..old_row_len]);
        {
            const insert_page = try pool.pin(page_id);
            slot = try HeapPage.insert(insert_page, old_row[0..old_row_len]);
            insert_page.header.lsn = insert_lsn;
            pool.unpin(page_id, true);
        }
        try tm.commit(tx_insert);
        _ = try wal.commitTx(tx_insert);
        try pool.flushAll();

        try wal.reserveBufferCapacity(wal_fixed_capacity);

        const tx_failed_update = try tm.begin();
        _ = try wal.beginTx(tx_failed_update);
        {
            const update_page = try pool.pin(page_id);
            defer pool.unpin(page_id, true);
            const old_visible = HeapPage.read(update_page, slot) catch return error.StorageRead;
            _ = try undo_log.push(tx_failed_update, page_id, slot, old_visible);
            try HeapPage.update(update_page, slot, failed_new_row[0..failed_new_row_len]);
            try std.testing.expectError(
                error.OutOfMemory,
                wal.append(tx_failed_update, .update, page_id, failed_new_row[0..failed_new_row_len]),
            );
        }
        try tm.abort(tx_failed_update);

        const tx_reader = try tm.begin();
        defer tm.commit(tx_reader) catch {};
        var snap = try tm.snapshot(tx_reader);
        defer snap.deinit();
        const visible = undo_log.findVisible(page_id, slot, &snap, &tm) orelse {
            return error.Corruption;
        };
        visible_before_crash_len = visible.len;
        @memcpy(
            visible_before_crash[0..visible_before_crash_len],
            visible,
        );

        try std.testing.expectEqualSlices(
            u8,
            old_row[0..old_row_len],
            visible_before_crash[0..visible_before_crash_len],
        );
    }

    disk.crash();

    var recovered_row_len: usize = 0;
    var recovered_row: [48]u8 = undefined;
    var recovered_records_len: usize = 0;
    var recovered_tx2_records: usize = 0;
    var recovered_flushed_lsn: u64 = 0;

    {
        var recovered_wal = Wal.init(std.testing.allocator, disk.storage());
        defer recovered_wal.deinit();
        try recovered_wal.recover();
        recovered_flushed_lsn = recovered_wal.flushed_lsn;

        var recovered_records_buf: [8]wal_mod.Record = undefined;
        var recovered_payload_buf: [256]u8 = undefined;
        const decoded = try recovered_wal.readFromInto(
            1,
            &recovered_records_buf,
            &recovered_payload_buf,
        );
        recovered_records_len = decoded.records_len;
        for (recovered_records_buf[0..decoded.records_len]) |rec| {
            if (rec.tx_id == 2) recovered_tx2_records += 1;
        }

        var recovered_pool = try BufferPool.init(std.testing.allocator, disk.storage(), 8);
        defer recovered_pool.deinit();
        const page = try recovered_pool.pin(page_id);
        const row = HeapPage.read(page, slot) catch return error.StorageRead;
        recovered_row_len = row.len;
        @memcpy(recovered_row[0..recovered_row_len], row);
        recovered_pool.unpin(page_id, false);
    }

    try std.testing.expectEqualSlices(
        u8,
        visible_before_crash[0..visible_before_crash_len],
        recovered_row[0..recovered_row_len],
    );
    try std.testing.expectEqual(@as(usize, 0), recovered_tx2_records);

    var h = std.hash.Wyhash.init(seed ^ 0xA11CE00E);
    h.update(std.mem.asBytes(&page_id));
    h.update(std.mem.asBytes(&slot));
    h.update(std.mem.asBytes(&recovered_records_len));
    h.update(std.mem.asBytes(&recovered_tx2_records));
    h.update(std.mem.asBytes(&recovered_flushed_lsn));
    h.update(visible_before_crash[0..visible_before_crash_len]);
    h.update(recovered_row[0..recovered_row_len]);
    return .{ .signature = h.final() };
}

const seed_set = [_]u64{
    0xC0FFEE01,
    0xC0FFEEA5,
    0xD15EA5E1,
    0xFEED1234,
    0x1234ABCD,
    0x0BADF00D,
    0xABAD1DEA,
    0x51515151,
};

const ci_short_seed_budget: usize = 28;
const ci_long_seed_budget: usize = 14;
const ci_short_seed_set = buildSeedSet(ci_short_seed_budget, 0xC1F00D55);
const ci_long_seed_set = buildSeedSet(ci_long_seed_budget, 0xC1F00D66);

test "seeded schedule: WAL partial write recovery is replay-deterministic across seed set" {
    for (seed_set) |seed| {
        const first = try runWalPartialWriteRecovery(seed);
        const second = try runWalPartialWriteRecovery(seed);
        try std.testing.expectEqual(first.signature, second.signature);
    }
}

test "seeded schedule: page bitflip checksum path is replay-deterministic across seed set" {
    for (seed_set) |seed| {
        const first = try runPageBitflipChecksum(seed);
        const second = try runPageBitflipChecksum(seed);
        try std.testing.expectEqual(first.signature, second.signature);
    }
}

test "seeded schedule: WAL fsync failure gates page flush deterministically across seed set" {
    for (seed_set) |seed| {
        const first = try runWalFsyncThenPageFlushGate(seed);
        const second = try runWalFsyncThenPageFlushGate(seed);
        try std.testing.expectEqual(first.signature, second.signature);
    }
}

test "seeded schedule: multi-fault WAL interleaving is replay-deterministic across seed set" {
    for (seed_set) |seed| {
        const first = try runWalMultiFaultInterleaving(seed);
        const second = try runWalMultiFaultInterleaving(seed);
        try std.testing.expectEqual(first.signature, second.signature);
    }
}

test "seeded schedule: combined WAL and page corruption path is replay-deterministic across seed set" {
    for (seed_set) |seed| {
        const first = try runCombinedWalAndPageCorruption(seed);
        const second = try runCombinedWalAndPageCorruption(seed);
        try std.testing.expectEqual(first.signature, second.signature);
    }
}

test "seeded schedule: repeated crash and recover cycles remain replay-deterministic" {
    for (seed_set) |seed| {
        const first = try runWalRepeatedCrashRecoveryCycles(seed);
        const second = try runWalRepeatedCrashRecoveryCycles(seed);
        try std.testing.expectEqual(first.signature, second.signature);
    }
}

test "seeded schedule: long WAL and page interleaving remains replay-deterministic" {
    for (seed_set) |seed| {
        const first = try runLongWalPageInterleaving(seed);
        const second = try runLongWalPageInterleaving(seed);
        try std.testing.expectEqual(first.signature, second.signature);
    }
}

test "seeded schedule: buffer-pool I/O fault interleaving remains replay-deterministic" {
    for (seed_set) |seed| {
        const first = try runBufferPoolIoFaultInterleaving(seed);
        const second = try runBufferPoolIoFaultInterleaving(seed);
        try std.testing.expectEqual(first.signature, second.signature);
    }
}

test "seeded schedule: extended WAL + buffer-pool cycles remain replay-deterministic" {
    for (seed_set) |seed| {
        const first = try runExtendedWalBufferPoolCycles(seed);
        const second = try runExtendedWalBufferPoolCycles(seed);
        try std.testing.expectEqual(first.signature, second.signature);
    }
}

test "seeded schedule: rollback visibility edge remains replay-deterministic" {
    for (seed_set) |seed| {
        const first = try runRollbackVisibilityEdge(seed);
        const second = try runRollbackVisibilityEdge(seed);
        try std.testing.expectEqual(first.signature, second.signature);
    }
}

test "seeded schedule: WAL+undo crash visibility consistency remains replay-deterministic" {
    for (seed_set) |seed| {
        const first = try runWalUndoCrashVisibilityConsistency(seed);
        const second = try runWalUndoCrashVisibilityConsistency(seed);
        try std.testing.expectEqual(first.signature, second.signature);
    }
}

test "seeded schedule: btree split flush crash interleaving remains replay-deterministic" {
    for (seed_set) |seed| {
        const first = try runBTreeSplitFlushCrashInterleaving(seed);
        const second = try runBTreeSplitFlushCrashInterleaving(seed);
        try std.testing.expectEqual(first.signature, second.signature);
    }
}

test "seeded schedule: btree split protocol crash matrix is replay-deterministic" {
    for (seed_set) |seed| {
        const first = try runBTreeSplitProtocolCrashMatrix(seed);
        const second = try runBTreeSplitProtocolCrashMatrix(seed);
        try std.testing.expectEqual(first.signature, second.signature);
    }
}

test "ci seed sweep: extended deterministic replay coverage for short schedules" {
    std.debug.assert(ci_short_seed_set.len == ci_short_seed_budget);
    try expectReplayDeterministicAcrossSeeds(
        "wal_partial_write_recovery",
        ci_short_seed_set[0..],
        runWalPartialWriteRecovery,
    );
    try expectReplayDeterministicAcrossSeeds(
        "page_bitflip_checksum",
        ci_short_seed_set[0..],
        runPageBitflipChecksum,
    );
    try expectReplayDeterministicAcrossSeeds(
        "wal_fsync_then_page_flush_gate",
        ci_short_seed_set[0..],
        runWalFsyncThenPageFlushGate,
    );
    try expectReplayDeterministicAcrossSeeds(
        "wal_multi_fault_interleaving",
        ci_short_seed_set[0..],
        runWalMultiFaultInterleaving,
    );
    try expectReplayDeterministicAcrossSeeds(
        "combined_wal_and_page_corruption",
        ci_short_seed_set[0..],
        runCombinedWalAndPageCorruption,
    );
    try expectReplayDeterministicAcrossSeeds(
        "buffer_pool_io_fault_interleaving",
        ci_short_seed_set[0..],
        runBufferPoolIoFaultInterleaving,
    );
    try expectReplayDeterministicAcrossSeeds(
        "btree_split_flush_crash_interleaving",
        ci_short_seed_set[0..],
        runBTreeSplitFlushCrashInterleaving,
    );
    try expectReplayDeterministicAcrossSeeds(
        "btree_split_protocol_crash_matrix",
        ci_short_seed_set[0..],
        runBTreeSplitProtocolCrashMatrix,
    );
    try expectReplayDeterministicAcrossSeeds(
        "rollback_visibility_edge",
        ci_short_seed_set[0..],
        runRollbackVisibilityEdge,
    );
    try expectReplayDeterministicAcrossSeeds(
        "wal_undo_crash_visibility_consistency",
        ci_short_seed_set[0..],
        runWalUndoCrashVisibilityConsistency,
    );
}

test "ci seed sweep: bounded deterministic replay coverage for long schedules" {
    std.debug.assert(ci_long_seed_set.len == ci_long_seed_budget);
    try expectReplayDeterministicAcrossSeeds(
        "wal_repeated_crash_recovery_cycles",
        ci_long_seed_set[0..],
        runWalRepeatedCrashRecoveryCycles,
    );
    try expectReplayDeterministicAcrossSeeds(
        "long_wal_page_interleaving",
        ci_long_seed_set[0..],
        runLongWalPageInterleaving,
    );
    try expectReplayDeterministicAcrossSeeds(
        "extended_wal_buffer_pool_cycles",
        ci_long_seed_set[0..],
        runExtendedWalBufferPoolCycles,
    );
}
