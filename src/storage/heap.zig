const std = @import("std");
const io = @import("io.zig");
const page_mod = @import("page.zig");

const Page = page_mod.Page;
const content_size = page_mod.content_size;

/// A row is identified by (page_id, slot_index).
pub const RowId = struct {
    page_id: u64,
    slot: u16,
};

/// Slotted page header at the start of the content area.
///
/// Layout within content area:
///   slot_count: u16    (2 bytes)
///   free_start: u16    (2 bytes) — offset where row data can grow up from
///   free_end:   u16    (2 bytes) — offset where slot array grows down to
///
/// Then:
///   Slot array grows downward from offset 6 (each slot = 4 bytes)
///   Row data grows upward from the end of the content area
///
/// ```
/// ┌──────────────────────────────────────┐
/// │ SlottedHeader (6 bytes)              │
/// ├──────────────────────────────────────┤
/// │ Slot Array → (grows down)            │
/// │  [offset:u16, len:u16] ...           │
/// ├──────── free space ─────────────────┤
/// │              ← Row Data (grows up)   │
/// └──────────────────────────────────────┘
/// ```
const SlottedHeader = struct {
    slot_count: u16,
    free_start: u16, // end of slot array (next free byte for slots)
    free_end: u16, // start of row data region (rows grow from end toward start)

    const size = 6;

    fn read(content: *const [content_size]u8) SlottedHeader {
        return .{
            .slot_count = std.mem.littleToNative(u16, std.mem.bytesAsValue(u16, content[0..2]).*),
            .free_start = std.mem.littleToNative(u16, std.mem.bytesAsValue(u16, content[2..4]).*),
            .free_end = std.mem.littleToNative(u16, std.mem.bytesAsValue(u16, content[4..6]).*),
        };
    }

    fn write(self: SlottedHeader, content: *[content_size]u8) void {
        @memcpy(content[0..2], std.mem.asBytes(&std.mem.nativeToLittle(u16, self.slot_count)));
        @memcpy(content[2..4], std.mem.asBytes(&std.mem.nativeToLittle(u16, self.free_start)));
        @memcpy(content[4..6], std.mem.asBytes(&std.mem.nativeToLittle(u16, self.free_end)));
    }
};

/// A slot entry: offset and length of a row within the content area.
/// A length of 0 indicates a deleted slot (tombstone).
const Slot = struct {
    offset: u16,
    len: u16,

    const size = 4;
    /// Sentinel value for deleted slots.
    const deleted_len: u16 = 0;

    fn read(content: *const [content_size]u8, index: u16) Slot {
        const base = SlottedHeader.size + @as(usize, index) * Slot.size;
        return .{
            .offset = std.mem.littleToNative(u16, std.mem.bytesAsValue(u16, content[base..][0..2]).*),
            .len = std.mem.littleToNative(u16, std.mem.bytesAsValue(u16, content[base + 2 ..][0..2]).*),
        };
    }

    fn writeAt(content: *[content_size]u8, index: u16, slot: Slot) void {
        const base = SlottedHeader.size + @as(usize, index) * Slot.size;
        @memcpy(content[base..][0..2], std.mem.asBytes(&std.mem.nativeToLittle(u16, slot.offset)));
        @memcpy(content[base + 2 ..][0..2], std.mem.asBytes(&std.mem.nativeToLittle(u16, slot.len)));
    }
};

pub const HeapError = error{
    PageFull,
    InvalidSlot,
    RowTooLarge,
};

/// Operations on a heap page (slotted page format).
///
/// These operate directly on the content area of a `Page`.
/// The caller is responsible for pinning/unpinning the page via the buffer pool.
pub const HeapPage = struct {

    /// Initialize a page's content area for heap use.
    pub fn init(page: *Page) void {
        page.header.page_type = .heap;
        @memset(&page.content, 0);
        const hdr = SlottedHeader{
            .slot_count = 0,
            .free_start = SlottedHeader.size,
            .free_end = content_size,
        };
        hdr.write(&page.content);
    }

    /// Insert a row into the page. Returns the slot index.
    pub fn insert(page: *Page, data: []const u8) HeapError!u16 {
        if (data.len > std.math.maxInt(u16)) return error.RowTooLarge;
        const row_len: u16 = @intCast(data.len);

        var hdr = SlottedHeader.read(&page.content);

        // Check if there's enough space: we need room for the slot entry + row data.
        const needed = Slot.size + row_len;
        if (hdr.free_end - hdr.free_start < needed) return error.PageFull;

        // Allocate row data from the end.
        hdr.free_end -= row_len;
        const row_offset = hdr.free_end;

        // Write row data.
        @memcpy(page.content[row_offset..][0..row_len], data);

        // Write slot entry.
        const slot_idx = hdr.slot_count;
        Slot.writeAt(&page.content, slot_idx, .{ .offset = row_offset, .len = row_len });

        // Update header.
        hdr.slot_count += 1;
        hdr.free_start += Slot.size;
        hdr.write(&page.content);

        return slot_idx;
    }

    /// Read a row from the page. Returns a slice into the page's content.
    pub fn read(page: *const Page, slot_idx: u16) HeapError![]const u8 {
        const hdr = SlottedHeader.read(&page.content);
        if (slot_idx >= hdr.slot_count) return error.InvalidSlot;

        const slot = Slot.read(&page.content, slot_idx);
        if (slot.len == Slot.deleted_len) return error.InvalidSlot;

        return page.content[slot.offset..][0..slot.len];
    }

    /// Update a row in-place. The new data must fit in the existing slot's space.
    /// For simplicity, in-place update requires the new row to be <= old row size.
    /// Returns the old row data (caller should copy if needed for undo log).
    ///
    /// If the new row is larger, returns error.RowTooLarge.
    pub fn update(page: *Page, slot_idx: u16, new_data: []const u8) HeapError!void {
        const hdr = SlottedHeader.read(&page.content);
        if (slot_idx >= hdr.slot_count) return error.InvalidSlot;

        var slot = Slot.read(&page.content, slot_idx);
        if (slot.len == Slot.deleted_len) return error.InvalidSlot;

        if (new_data.len > slot.len) return error.RowTooLarge;

        const new_len: u16 = @intCast(new_data.len);

        // Write new data at the same offset.
        @memcpy(page.content[slot.offset..][0..new_len], new_data);
        // Zero out any remaining bytes if new data is shorter.
        if (new_len < slot.len) {
            @memset(page.content[slot.offset + new_len ..][0 .. slot.len - new_len], 0);
        }
        // Update slot length.
        slot.len = new_len;
        Slot.writeAt(&page.content, slot_idx, slot);
    }

    /// Delete a row by marking its slot as deleted (tombstone).
    /// Does not reclaim space — that requires compaction.
    pub fn delete(page: *Page, slot_idx: u16) HeapError!void {
        const hdr = SlottedHeader.read(&page.content);
        if (slot_idx >= hdr.slot_count) return error.InvalidSlot;

        var slot = Slot.read(&page.content, slot_idx);
        if (slot.len == Slot.deleted_len) return error.InvalidSlot;

        slot.len = Slot.deleted_len;
        Slot.writeAt(&page.content, slot_idx, slot);
    }

    /// Returns the number of live (non-deleted) rows in the page.
    pub fn liveCount(page: *const Page) u16 {
        const hdr = SlottedHeader.read(&page.content);
        var count: u16 = 0;
        for (0..hdr.slot_count) |i| {
            const slot = Slot.read(&page.content, @intCast(i));
            if (slot.len != Slot.deleted_len) count += 1;
        }
        return count;
    }

    /// Returns the amount of free space available for new inserts
    /// (contiguous free space between slot array and row data).
    pub fn freeSpace(page: *const Page) u16 {
        const hdr = SlottedHeader.read(&page.content);
        if (hdr.free_end <= hdr.free_start) return 0;
        return hdr.free_end - hdr.free_start;
    }

    /// Returns the total slot count (including deleted slots).
    pub fn slotCount(page: *const Page) u16 {
        return SlottedHeader.read(&page.content).slot_count;
    }
};

// --- Tests ---

test "init creates empty heap page" {
    var page = Page.init(0, .free);
    HeapPage.init(&page);

    try std.testing.expectEqual(page_mod.PageType.heap, page.header.page_type);
    try std.testing.expectEqual(@as(u16, 0), HeapPage.slotCount(&page));
    try std.testing.expectEqual(@as(u16, 0), HeapPage.liveCount(&page));
    // Free space should be content_size minus the slotted header.
    try std.testing.expectEqual(@as(u16, content_size - SlottedHeader.size), HeapPage.freeSpace(&page));
}

test "insert and read single row" {
    var page = Page.init(0, .free);
    HeapPage.init(&page);

    const data = "hello, world!";
    const slot = try HeapPage.insert(&page, data);
    try std.testing.expectEqual(@as(u16, 0), slot);

    const result = try HeapPage.read(&page, slot);
    try std.testing.expectEqualSlices(u8, data, result);
}

test "insert multiple rows" {
    var page = Page.init(0, .free);
    HeapPage.init(&page);

    const s0 = try HeapPage.insert(&page, "row zero");
    const s1 = try HeapPage.insert(&page, "row one");
    const s2 = try HeapPage.insert(&page, "row two");

    try std.testing.expectEqual(@as(u16, 0), s0);
    try std.testing.expectEqual(@as(u16, 1), s1);
    try std.testing.expectEqual(@as(u16, 2), s2);

    try std.testing.expectEqualSlices(u8, "row zero", try HeapPage.read(&page, s0));
    try std.testing.expectEqualSlices(u8, "row one", try HeapPage.read(&page, s1));
    try std.testing.expectEqualSlices(u8, "row two", try HeapPage.read(&page, s2));

    try std.testing.expectEqual(@as(u16, 3), HeapPage.liveCount(&page));
}

test "delete marks slot as tombstone" {
    var page = Page.init(0, .free);
    HeapPage.init(&page);

    const s0 = try HeapPage.insert(&page, "data");
    try std.testing.expectEqual(@as(u16, 1), HeapPage.liveCount(&page));

    try HeapPage.delete(&page, s0);
    try std.testing.expectEqual(@as(u16, 0), HeapPage.liveCount(&page));
    try std.testing.expectEqual(@as(u16, 1), HeapPage.slotCount(&page));

    // Reading deleted slot should fail.
    const result = HeapPage.read(&page, s0);
    try std.testing.expectError(HeapError.InvalidSlot, result);
}

test "update in-place with smaller data" {
    var page = Page.init(0, .free);
    HeapPage.init(&page);

    const s0 = try HeapPage.insert(&page, "long original data");
    try HeapPage.update(&page, s0, "short");

    const result = try HeapPage.read(&page, s0);
    try std.testing.expectEqualSlices(u8, "short", result);
}

test "update rejects larger data" {
    var page = Page.init(0, .free);
    HeapPage.init(&page);

    const s0 = try HeapPage.insert(&page, "small");
    const result = HeapPage.update(&page, s0, "this is much larger than the original");
    try std.testing.expectError(HeapError.RowTooLarge, result);
}

test "page full returns error" {
    var page = Page.init(0, .free);
    HeapPage.init(&page);

    // Fill the page. Each insert takes Slot.size (4) + data_len bytes.
    // content_size = 8168. Header = 6. Available = 8162.
    // Insert 100-byte rows: each needs 104 bytes. 8162/104 = 78 rows.
    var row: [100]u8 = undefined;
    @memset(&row, 0x42);

    var count: u16 = 0;
    while (true) {
        _ = HeapPage.insert(&page, &row) catch |err| {
            try std.testing.expectEqual(HeapError.PageFull, err);
            break;
        };
        count += 1;
    }
    try std.testing.expect(count > 0);
    try std.testing.expectEqual(count, HeapPage.liveCount(&page));
}

test "read invalid slot returns error" {
    var page = Page.init(0, .free);
    HeapPage.init(&page);

    const result = HeapPage.read(&page, 0);
    try std.testing.expectError(HeapError.InvalidSlot, result);
}

test "free space decreases after insert" {
    var page = Page.init(0, .free);
    HeapPage.init(&page);

    const before = HeapPage.freeSpace(&page);
    _ = try HeapPage.insert(&page, "test data");
    const after = HeapPage.freeSpace(&page);

    // Should decrease by slot size (4) + data length (9).
    try std.testing.expectEqual(before - after, Slot.size + 9);
}

test "insert and read with buffer pool" {
    const disk_mod = @import("../simulator/disk.zig");
    const bp_mod = @import("buffer_pool.zig");

    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    var pool = try bp_mod.BufferPool.init(std.testing.allocator, disk.storage(), 4);
    defer pool.deinit();

    // Pin page, init as heap, insert rows.
    const page = try pool.pin(0);
    HeapPage.init(page);
    const s0 = try HeapPage.insert(page, "row via buffer pool");
    pool.unpin(0, true);

    // Re-pin and verify.
    const page2 = try pool.pin(0);
    const data = try HeapPage.read(page2, s0);
    try std.testing.expectEqualSlices(u8, "row via buffer pool", data);
    pool.unpin(0, false);
}
