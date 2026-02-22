//! Simulation determinism tests for temp/spill storage.
//!
//! Responsibilities in this file:
//! - Verifies temp spill operations produce identical results under replay.
//! - Verifies fault injection replays deterministically at the same operation.
const std = @import("std");
const pg2 = @import("pg2");

const disk_mod = pg2.simulator.disk;
const temp_mod = pg2.storage.temp;
const io_mod = pg2.storage.io;
const page_mod = pg2.storage.page;

const SimulatedDisk = disk_mod.SimulatedDisk;
const TempStorageManager = temp_mod.TempStorageManager;
const TempPage = temp_mod.TempPage;

/// Run a deterministic spill sequence and return stats + page contents.
fn runSpillSequence(
    disk: *SimulatedDisk,
    payloads: []const []const u8,
) !struct {
    stats: temp_mod.TempSpillStats,
    contents: [16][io_mod.page_size]u8,
    page_count: usize,
} {
    var mgr = try TempStorageManager.init(0, disk.storage(), 16, 90_000);
    var contents: [16][io_mod.page_size]u8 = undefined;
    var page_count: usize = 0;

    for (payloads) |payload| {
        const page_id = try mgr.allocateAndWrite(payload, TempPage.null_page_id);
        // Read back the raw page to capture full content.
        disk.storage().read(page_id, &contents[page_count]) catch unreachable;
        page_count += 1;
    }

    mgr.reset();
    return .{
        .stats = mgr.snapshotStats(),
        .contents = contents,
        .page_count = page_count,
    };
}

test "temp spill operations are deterministic under replay" {
    const payloads = [_][]const u8{
        "sort-run-0",
        "sort-run-1",
        "sort-run-2",
        "join-partition-0",
    };

    // Run 1.
    var disk1 = SimulatedDisk.init(std.testing.allocator);
    defer disk1.deinit();
    const run1 = try runSpillSequence(&disk1, payloads[0..]);

    // Run 2 (fresh disk, same sequence).
    var disk2 = SimulatedDisk.init(std.testing.allocator);
    defer disk2.deinit();
    const run2 = try runSpillSequence(&disk2, payloads[0..]);

    // Stats must be identical.
    try std.testing.expectEqual(run1.stats.temp_pages_allocated, run2.stats.temp_pages_allocated);
    try std.testing.expectEqual(run1.stats.temp_pages_reclaimed, run2.stats.temp_pages_reclaimed);
    try std.testing.expectEqual(run1.stats.temp_bytes_written, run2.stats.temp_bytes_written);
    try std.testing.expectEqual(run1.stats.temp_bytes_read, run2.stats.temp_bytes_read);

    // Page contents must be byte-identical.
    try std.testing.expectEqual(run1.page_count, run2.page_count);
    for (0..run1.page_count) |i| {
        try std.testing.expectEqualSlices(u8, &run1.contents[i], &run2.contents[i]);
    }
}

test "temp fault injection replays deterministically" {
    const payloads = [_][]const u8{
        "chunk-a",
        "chunk-b",
        "chunk-c",
    };

    // Run 1: inject write fault on the 3rd write (3rd payload).
    var disk1 = SimulatedDisk.init(std.testing.allocator);
    defer disk1.deinit();
    disk1.failWriteAt(3);

    var mgr1 = try TempStorageManager.init(0, disk1.storage(), 16, 91_000);
    _ = try mgr1.allocateAndWrite(payloads[0], TempPage.null_page_id);
    _ = try mgr1.allocateAndWrite(payloads[1], TempPage.null_page_id);
    const err1 = mgr1.allocateAndWrite(payloads[2], TempPage.null_page_id);
    try std.testing.expectError(error.WriteError, err1);
    const stats1 = mgr1.snapshotStats();

    // Run 2: same sequence, same fault point.
    var disk2 = SimulatedDisk.init(std.testing.allocator);
    defer disk2.deinit();
    disk2.failWriteAt(3);

    var mgr2 = try TempStorageManager.init(0, disk2.storage(), 16, 91_000);
    _ = try mgr2.allocateAndWrite(payloads[0], TempPage.null_page_id);
    _ = try mgr2.allocateAndWrite(payloads[1], TempPage.null_page_id);
    const err2 = mgr2.allocateAndWrite(payloads[2], TempPage.null_page_id);
    try std.testing.expectError(error.WriteError, err2);
    const stats2 = mgr2.snapshotStats();

    // Both runs fail at the same point with identical stats.
    try std.testing.expectEqual(stats1.temp_pages_allocated, stats2.temp_pages_allocated);
    try std.testing.expectEqual(stats1.temp_bytes_written, stats2.temp_bytes_written);
}
