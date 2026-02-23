//! Integration tests for temp/spill storage primitives.
//!
//! Responsibilities in this file:
//! - Verifies temp pages bypass the buffer pool (frame count unchanged).
//! - Verifies per-slot page-id isolation.
//! - Verifies checksum corruption detection via SimulatedDisk fault injection.
//! - Verifies temp pages are lost on crash (no durability).
//! - Verifies stats accumulation across multiple allocations and reset.
//! - Verifies region exhaustion under constrained pages_per_slot.
//! - Verifies long chain traversal (production-scale page chains).
//! - Verifies page reuse after reset correctly overwrites old content.
//! - Verifies multiple reset cycles with stats accumulation.
//! - Verifies interleaved multi-slot writes don't cross-contaminate.
//! - Verifies temp writes never trigger fsync.
//! - Verifies high slot-index overflow protection.
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
            "INSPECT spill spill_triggered=false result_bytes_accumulated=0 temp_pages_allocated=0 temp_pages_reclaimed=0 temp_bytes_written=0 temp_bytes_read=0\n",
        ) != null,
    );
}

test "inspect reports non-zero spill telemetry when spill is triggered" {
    const internal = @import("../../features/test_env_test.zig");

    // Use a tiny work-memory budget so that a few rows exceed it and trigger spill.
    var env: internal.FeatureEnv = undefined;
    try env.initWithConfig(.{
        .max_query_slots = 1,
        .work_memory_bytes_per_slot = 50,
    });
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\SpillUser {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\}
    );

    // Insert several rows. Each row serializes to more than ~20 bytes,
    // so 3-4 rows will exceed the 50-byte budget and force a spill.
    _ = try executor.run("SpillUser |> insert(id = 1, name = \"Alice\") {}");
    _ = try executor.run("SpillUser |> insert(id = 2, name = \"Bob\") {}");
    _ = try executor.run("SpillUser |> insert(id = 3, name = \"Charlie\") {}");
    _ = try executor.run("SpillUser |> insert(id = 4, name = \"Diana\") {}");

    const result = try executor.run("SpillUser |> inspect {}");

    // spill_triggered must be true.
    try std.testing.expect(
        std.mem.indexOf(u8, result, "spill_triggered=true") != null,
    );

    // result_bytes_accumulated must be non-zero (rows were scanned).
    // Verify it does NOT say result_bytes_accumulated=0.
    try std.testing.expect(
        std.mem.indexOf(u8, result, "result_bytes_accumulated=0 ") == null,
    );

    // temp_bytes_written must be non-zero (data was spilled to disk).
    try std.testing.expect(
        std.mem.indexOf(u8, result, "temp_bytes_written=0 ") == null,
    );

    // temp_pages_allocated must be non-zero.
    try std.testing.expect(
        std.mem.indexOf(u8, result, "temp_pages_allocated=0 ") == null,
    );
}

test "temp long chain traversal" {
    var disk = SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    const chain_len = 12;
    var mgr = try TempStorageManager.init(0, disk.storage(), 16, 100_000);

    // Build a forward chain: page[i] points to page[i-1], page[0] → null.
    var page_ids: [chain_len]u64 = undefined;
    var prev_id: u64 = TempPage.null_page_id;
    for (0..chain_len) |i| {
        // Each page has a unique payload so we can verify traversal order.
        var payload: [32]u8 = undefined;
        const label = std.fmt.bufPrint(&payload, "chunk-{d:0>4}", .{i}) catch unreachable;
        page_ids[i] = try mgr.allocateAndWrite(label, prev_id);
        prev_id = page_ids[i];
    }

    // Traverse from tail to head, verifying every payload and link.
    var current_id = page_ids[chain_len - 1];
    var traversed: usize = 0;
    while (current_id != TempPage.null_page_id) {
        const r = try mgr.readPage(current_id);
        const expected_index = chain_len - 1 - traversed;
        var expected: [32]u8 = undefined;
        const expected_label = std.fmt.bufPrint(&expected, "chunk-{d:0>4}", .{expected_index}) catch unreachable;
        try std.testing.expectEqualSlices(u8, expected_label, r.payload);
        current_id = r.next_page_id;
        traversed += 1;
    }
    try std.testing.expectEqual(chain_len, traversed);
}

test "temp page reuse after reset overwrites old content" {
    var disk = SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    var mgr = try TempStorageManager.init(0, disk.storage(), 8, 110_000);

    // Write a long payload to page 0.
    const old_payload = "old-data-that-is-quite-long-and-fills-many-bytes";
    const old_id = try mgr.allocateAndWrite(old_payload, 42);
    try std.testing.expectEqual(@as(u64, 110_000), old_id);

    // Verify it reads back.
    const old_read = try mgr.readPage(old_id);
    try std.testing.expectEqualSlices(u8, old_payload, old_read.payload);
    try std.testing.expectEqual(@as(u64, 42), old_read.next_page_id);

    // Reset and write a shorter, different payload to the same page ID.
    mgr.reset();
    const new_payload = "short";
    const new_id = try mgr.allocateAndWrite(new_payload, TempPage.null_page_id);
    try std.testing.expectEqual(@as(u64, 110_000), new_id); // same page ID reused

    // New read must return the short payload, not remnants of the old one.
    const new_read = try mgr.readPage(new_id);
    try std.testing.expectEqualSlices(u8, new_payload, new_read.payload);
    try std.testing.expectEqual(TempPage.null_page_id, new_read.next_page_id);
}

test "temp multiple reset cycles accumulate stats correctly" {
    var disk = SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    var mgr = try TempStorageManager.init(0, disk.storage(), 16, 120_000);

    // Cycle 1: allocate 3, reset.
    _ = try mgr.allocateAndWrite("c1-a", TempPage.null_page_id);
    _ = try mgr.allocateAndWrite("c1-b", TempPage.null_page_id);
    _ = try mgr.allocateAndWrite("c1-c", TempPage.null_page_id);
    mgr.reset();

    var stats = mgr.snapshotStats();
    try std.testing.expectEqual(@as(u32, 3), stats.temp_pages_allocated);
    try std.testing.expectEqual(@as(u32, 3), stats.temp_pages_reclaimed);

    // Cycle 2: allocate 2, reset.
    _ = try mgr.allocateAndWrite("c2-a", TempPage.null_page_id);
    _ = try mgr.allocateAndWrite("c2-b", TempPage.null_page_id);
    mgr.reset();

    stats = mgr.snapshotStats();
    try std.testing.expectEqual(@as(u32, 5), stats.temp_pages_allocated);
    try std.testing.expectEqual(@as(u32, 5), stats.temp_pages_reclaimed);

    // Cycle 3: allocate 1, don't reset — verify in-use count.
    const id = try mgr.allocateAndWrite("c3-a", TempPage.null_page_id);
    try std.testing.expectEqual(@as(u64, 1), mgr.pagesInUse());

    stats = mgr.snapshotStats();
    try std.testing.expectEqual(@as(u32, 6), stats.temp_pages_allocated);
    try std.testing.expectEqual(@as(u32, 5), stats.temp_pages_reclaimed);

    // Data from cycle 3 is still readable.
    const r = try mgr.readPage(id);
    try std.testing.expectEqualSlices(u8, "c3-a", r.payload);
}

test "temp interleaved multi-slot writes don't cross-contaminate" {
    var disk = SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    const pages_per_slot: u64 = 8;
    const region_start: u64 = 130_000;

    var mgr0 = try TempStorageManager.init(0, disk.storage(), pages_per_slot, region_start);
    var mgr1 = try TempStorageManager.init(1, disk.storage(), pages_per_slot, region_start);

    // Interleave writes: slot0, slot1, slot0, slot1, slot0.
    const id0a = try mgr0.allocateAndWrite("s0-alpha", TempPage.null_page_id);
    const id1a = try mgr1.allocateAndWrite("s1-alpha", TempPage.null_page_id);
    const id0b = try mgr0.allocateAndWrite("s0-beta", id0a);
    const id1b = try mgr1.allocateAndWrite("s1-beta", id1a);
    const id0c = try mgr0.allocateAndWrite("s0-gamma", id0b);

    // Verify all slot 0 pages have correct content.
    const r0a = try mgr0.readPage(id0a);
    try std.testing.expectEqualSlices(u8, "s0-alpha", r0a.payload);
    const r0b = try mgr0.readPage(id0b);
    try std.testing.expectEqualSlices(u8, "s0-beta", r0b.payload);
    try std.testing.expectEqual(id0a, r0b.next_page_id);
    const r0c = try mgr0.readPage(id0c);
    try std.testing.expectEqualSlices(u8, "s0-gamma", r0c.payload);
    try std.testing.expectEqual(id0b, r0c.next_page_id);

    // Verify all slot 1 pages have correct content.
    const r1a = try mgr1.readPage(id1a);
    try std.testing.expectEqualSlices(u8, "s1-alpha", r1a.payload);
    const r1b = try mgr1.readPage(id1b);
    try std.testing.expectEqualSlices(u8, "s1-beta", r1b.payload);
    try std.testing.expectEqual(id1a, r1b.next_page_id);

    // Verify page IDs are in their respective regions.
    try std.testing.expect(mgr0.allocator.ownsPageId(id0a));
    try std.testing.expect(mgr0.allocator.ownsPageId(id0b));
    try std.testing.expect(mgr0.allocator.ownsPageId(id0c));
    try std.testing.expect(mgr1.allocator.ownsPageId(id1a));
    try std.testing.expect(mgr1.allocator.ownsPageId(id1b));

    // Cross-reads via the other manager should also work (same underlying disk).
    const cross_read = try mgr0.readPage(id1a);
    try std.testing.expectEqualSlices(u8, "s1-alpha", cross_read.payload);
}

test "temp writes never trigger fsync" {
    var disk = SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    const fsyncs_before = disk.fsyncs;

    var mgr = try TempStorageManager.init(0, disk.storage(), 16, 140_000);
    _ = try mgr.allocateAndWrite("a", TempPage.null_page_id);
    _ = try mgr.allocateAndWrite("b", TempPage.null_page_id);
    _ = try mgr.allocateAndWrite("c", TempPage.null_page_id);
    _ = try mgr.readPage(140_000);
    _ = try mgr.readPage(140_001);
    mgr.reset();

    // Temp storage must never call fsync — pages are ephemeral.
    try std.testing.expectEqual(fsyncs_before, disk.fsyncs);
}

test "temp allocator rejects high slot index that would overflow region" {
    const TempPageAllocator = temp_mod.TempPageAllocator;

    // With max slot index (65535) and large pages_per_slot, the region
    // calculation would overflow u64. Checked arithmetic must catch this.
    const result = TempPageAllocator.initForSlot(
        std.math.maxInt(u16), // slot 65535
        std.math.maxInt(u64) / 2, // huge pages_per_slot
        std.math.maxInt(u64) / 2, // huge region_start
    );
    try std.testing.expectError(error.InvalidRegion, result);

    // Slightly less extreme but still overflowing: slot_offset overflows.
    const result2 = TempPageAllocator.initForSlot(
        std.math.maxInt(u16),
        std.math.maxInt(u64) / 65534, // just enough to overflow when multiplied by 65535
        1,
    );
    try std.testing.expectError(error.InvalidRegion, result2);
}
