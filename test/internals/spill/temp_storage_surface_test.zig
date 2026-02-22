//! Integration tests for temp/spill storage primitives.
//!
//! Responsibilities in this file:
//! - Verifies temp pages bypass the buffer pool (frame count unchanged).
//! - Verifies per-slot page-id isolation.
//! - Verifies checksum corruption detection via SimulatedDisk fault injection.
//! - Verifies temp pages are lost on crash (no durability).
//! - Verifies stats accumulation across multiple allocations and reset.
//! - Verifies region exhaustion under constrained pages_per_slot.
const std = @import("std");
const pg2 = @import("pg2");

const disk_mod = pg2.simulator.disk;
const temp_mod = pg2.storage.temp;
const buffer_pool_mod = pg2.storage.buffer_pool;
const io_mod = pg2.storage.io;

const SimulatedDisk = disk_mod.SimulatedDisk;
const TempStorageManager = temp_mod.TempStorageManager;
const TempPage = temp_mod.TempPage;
const BufferPool = buffer_pool_mod.BufferPool;

test "temp pages bypass buffer pool" {
    var disk = SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    // Create a buffer pool to verify it is NOT used by temp writes.
    var pool = try BufferPool.init(std.testing.allocator, disk.storage(), 8);
    defer pool.deinit();

    const hits_before = pool.hits;
    const misses_before = pool.misses;

    // Write temp pages directly through Storage (bypassing buffer pool).
    var mgr = try TempStorageManager.init(0, disk.storage(), 8, 30_000);
    _ = try mgr.allocateAndWrite("payload-a", TempPage.null_page_id);
    _ = try mgr.allocateAndWrite("payload-b", TempPage.null_page_id);

    // Buffer pool stats unchanged — temp I/O does not touch the pool.
    try std.testing.expectEqual(hits_before, pool.hits);
    try std.testing.expectEqual(misses_before, pool.misses);
}

test "temp page allocator per-slot isolation" {
    var disk = SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    const pages_per_slot: u64 = 4;
    const region_start: u64 = 40_000;

    var mgr0 = try TempStorageManager.init(0, disk.storage(), pages_per_slot, region_start);
    var mgr1 = try TempStorageManager.init(1, disk.storage(), pages_per_slot, region_start);

    // Allocate pages from both slots.
    const id0a = try mgr0.allocateAndWrite("slot-0-a", TempPage.null_page_id);
    const id0b = try mgr0.allocateAndWrite("slot-0-b", TempPage.null_page_id);
    const id1a = try mgr1.allocateAndWrite("slot-1-a", TempPage.null_page_id);

    // Slot 0: [40000, 40003], Slot 1: [40004, 40007].
    try std.testing.expectEqual(@as(u64, 40_000), id0a);
    try std.testing.expectEqual(@as(u64, 40_001), id0b);
    try std.testing.expectEqual(@as(u64, 40_004), id1a);

    // Verify ownership is disjoint.
    try std.testing.expect(!mgr0.allocator.ownsPageId(id1a));
    try std.testing.expect(!mgr1.allocator.ownsPageId(id0a));

    // Reset slot 0 — slot 1 unaffected.
    mgr0.reset();
    try std.testing.expectEqual(@as(u64, 0), mgr0.pagesInUse());
    try std.testing.expectEqual(@as(u64, 1), mgr1.pagesInUse());

    // Slot 0 can allocate again from the same region.
    const id0c = try mgr0.allocateAndWrite("slot-0-c", TempPage.null_page_id);
    try std.testing.expectEqual(@as(u64, 40_000), id0c);

    // Slot 1 read still works.
    const r1 = try mgr1.readPage(id1a);
    try std.testing.expectEqualSlices(u8, "slot-1-a", r1.payload);
}

test "temp page checksum detects corruption" {
    var disk = SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    var mgr = try TempStorageManager.init(0, disk.storage(), 8, 50_000);
    _ = try mgr.allocateAndWrite("clean-data", TempPage.null_page_id);

    // Inject bitflip on the next write.
    disk.bitflipWriteAt(disk.writes + 1, 30, 0xFF);
    _ = try mgr.allocateAndWrite("corrupt-data", TempPage.null_page_id);

    // Reading the corrupted page should fail checksum validation.
    const read_result = mgr.readPage(50_001);
    try std.testing.expectError(error.ChecksumMismatch, read_result);

    // The clean page is still readable.
    const clean = try mgr.readPage(50_000);
    try std.testing.expectEqualSlices(u8, "clean-data", clean.payload);
}

test "temp pages lost on crash (no durability)" {
    var disk = SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    var mgr = try TempStorageManager.init(0, disk.storage(), 8, 60_000);
    _ = try mgr.allocateAndWrite("ephemeral", TempPage.null_page_id);

    // Verify the page exists before crash.
    const pre_crash = try mgr.readPage(60_000);
    try std.testing.expectEqualSlices(u8, "ephemeral", pre_crash.payload);

    // Crash discards unfsynced writes.
    disk.crash();

    // Page is gone — read returns zero-filled page which fails format check.
    const post_crash = mgr.readPage(60_000);
    try std.testing.expect(std.meta.isError(post_crash));
}

test "temp stats accumulate across multiple allocations" {
    var disk = SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    var mgr = try TempStorageManager.init(0, disk.storage(), 16, 70_000);

    // Write 3 pages, read 2 back.
    const id0 = try mgr.allocateAndWrite("a", TempPage.null_page_id);
    _ = try mgr.allocateAndWrite("b", TempPage.null_page_id);
    _ = try mgr.allocateAndWrite("c", TempPage.null_page_id);
    _ = try mgr.readPage(id0);
    _ = try mgr.readPage(id0);

    var stats = mgr.snapshotStats();
    try std.testing.expectEqual(@as(u32, 3), stats.temp_pages_allocated);
    try std.testing.expectEqual(@as(u64, 3 * io_mod.page_size), stats.temp_bytes_written);
    try std.testing.expectEqual(@as(u64, 2 * io_mod.page_size), stats.temp_bytes_read);
    try std.testing.expectEqual(@as(u32, 0), stats.temp_pages_reclaimed);

    // Reset and verify reclaim counter.
    mgr.reset();
    stats = mgr.snapshotStats();
    try std.testing.expectEqual(@as(u32, 3), stats.temp_pages_reclaimed);
    try std.testing.expectEqual(@as(u64, 0), mgr.pagesInUse());
}

test "temp region exhaustion returns RegionExhausted" {
    var disk = SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    var mgr = try TempStorageManager.init(0, disk.storage(), 2, 80_000);
    _ = try mgr.allocateAndWrite("first", TempPage.null_page_id);
    _ = try mgr.allocateAndWrite("second", TempPage.null_page_id);

    // Third allocation exceeds the 2-page region.
    const result = mgr.allocateAndWrite("third", TempPage.null_page_id);
    try std.testing.expectError(error.RegionExhausted, result);

    // After reset, allocation succeeds again.
    mgr.reset();
    _ = try mgr.allocateAndWrite("reused", TempPage.null_page_id);
    try std.testing.expectEqual(@as(u64, 1), mgr.pagesInUse());
}

test "inspect includes spill stats at zero when no spilling occurs" {
    const internal = @import("../../features/test_env_test.zig");

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

    const result = try executor.run("User |> inspect {}");
    try std.testing.expect(
        std.mem.indexOf(
            u8,
            result,
            "INSPECT spill temp_pages_allocated=0 temp_pages_reclaimed=0 temp_bytes_written=0 temp_bytes_read=0\n",
        ) != null,
    );
}
