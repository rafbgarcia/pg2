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

    const records = try recovered.readFrom(1, std.testing.allocator);
    defer Wal.freeRecords(records, std.testing.allocator);
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

test "seeded schedule: WAL partial write recovery is replay-deterministic" {
    const seed: u64 = 0xC0FFEE01;
    const first = try runWalPartialWriteRecovery(seed);
    const second = try runWalPartialWriteRecovery(seed);
    try std.testing.expectEqual(first.signature, second.signature);
}

test "seeded schedule: page bitflip checksum path is replay-deterministic" {
    const seed: u64 = 0xC0FFEE02;
    const first = try runPageBitflipChecksum(seed);
    const second = try runPageBitflipChecksum(seed);
    try std.testing.expectEqual(first.signature, second.signature);
}

test "seeded schedule: WAL fsync failure gates page flush deterministically" {
    const seed: u64 = 0xC0FFEE03;
    const first = try runWalFsyncThenPageFlushGate(seed);
    const second = try runWalFsyncThenPageFlushGate(seed);
    try std.testing.expectEqual(first.signature, second.signature);
}
