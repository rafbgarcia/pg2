const std = @import("std");
const disk_mod = @import("disk.zig");
const wal_mod = @import("../storage/wal.zig");
const buffer_pool_mod = @import("../storage/buffer_pool.zig");

const SimulatedDisk = disk_mod.SimulatedDisk;
const Wal = wal_mod.Wal;
const BufferPool = buffer_pool_mod.BufferPool;

const ScenarioOutcome = struct {
    signature: u64,
};

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
