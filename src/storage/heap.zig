const std = @import("std");
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
///   format_magic: u16  (2 bytes)
///   format_version: u8 (1 byte)
///   _reserved: u8      (1 byte)
///   slot_count: u16    (2 bytes)
///   free_start: u16    (2 bytes) — offset where row data can grow up from
///   free_end:   u16    (2 bytes) — offset where slot array grows down to
///
/// Then:
///   Slot array grows downward from offset 10 (each slot = 4 bytes)
///   Row data grows upward from the end of the content area
///
/// Header then slot array, then free space, then row data.
const SlottedHeader = struct {
    format_magic: u16,
    format_version: u8,
    reserved: u8,
    slot_count: u16,
    free_start: u16, // end of slot array (next free byte for slots)
    free_end: u16, // start of row data region (rows grow from end toward start)

    const size = 10;
    const format_magic_value: u16 = 0x4832; // "H2"
    const format_version_value: u8 = 1;

    fn read(content: *const [content_size]u8) SlottedHeader {
        std.debug.assert(content_size >= SlottedHeader.size);
        const header = SlottedHeader{
            .format_magic = std.mem.littleToNative(
                u16,
                std.mem.bytesAsValue(u16, content[0..2]).*,
            ),
            .format_version = content[2],
            .reserved = content[3],
            .slot_count = std.mem.littleToNative(
                u16,
                std.mem.bytesAsValue(u16, content[4..6]).*,
            ),
            .free_start = std.mem.littleToNative(
                u16,
                std.mem.bytesAsValue(u16, content[6..8]).*,
            ),
            .free_end = std.mem.littleToNative(
                u16,
                std.mem.bytesAsValue(u16, content[8..10]).*,
            ),
        };
        assert_header_valid(header);
        return header;
    }

    fn write(self: SlottedHeader, content: *[content_size]u8) void {
        assert_header_valid(self);
        std.debug.assert(content_size >= SlottedHeader.size);
        @memcpy(
            content[0..2],
            std.mem.asBytes(&std.mem.nativeToLittle(u16, self.format_magic)),
        );
        content[2] = self.format_version;
        content[3] = self.reserved;
        @memcpy(
            content[4..6],
            std.mem.asBytes(&std.mem.nativeToLittle(u16, self.slot_count)),
        );
        @memcpy(
            content[6..8],
            std.mem.asBytes(&std.mem.nativeToLittle(u16, self.free_start)),
        );
        @memcpy(content[8..10], std.mem.asBytes(&std.mem.nativeToLittle(u16, self.free_end)));
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
        std.debug.assert(index < max_slot_count);
        const base = SlottedHeader.size + @as(usize, index) * Slot.size;
        std.debug.assert(base + Slot.size <= content_size);
        return Slot{
            .offset = std.mem.littleToNative(
                u16,
                std.mem.bytesAsValue(u16, content[base..][0..2]).*,
            ),
            .len = std.mem.littleToNative(
                u16,
                std.mem.bytesAsValue(u16, content[base + 2 ..][0..2]).*,
            ),
        };
    }

    fn write_at(content: *[content_size]u8, index: u16, slot: Slot) void {
        std.debug.assert(index < max_slot_count);
        const base = SlottedHeader.size + @as(usize, index) * Slot.size;
        std.debug.assert(base + Slot.size <= content_size);
        @memcpy(
            content[base..][0..2],
            std.mem.asBytes(&std.mem.nativeToLittle(u16, slot.offset)),
        );
        @memcpy(
            content[base + 2 ..][0..2],
            std.mem.asBytes(&std.mem.nativeToLittle(u16, slot.len)),
        );
    }
};

const max_slot_count = (content_size - SlottedHeader.size) / Slot.size;

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
        std.debug.assert(page.header.page_type == .free or page.header.page_type == .heap);
        std.debug.assert(content_size > SlottedHeader.size);
        page.header.page_type = .heap;
        @memset(&page.content, 0);
        const header = SlottedHeader{
            .format_magic = SlottedHeader.format_magic_value,
            .format_version = SlottedHeader.format_version_value,
            .reserved = 0,
            .slot_count = 0,
            .free_start = SlottedHeader.size,
            .free_end = content_size,
        };
        header.write(&page.content);
        const written = SlottedHeader.read(&page.content);
        std.debug.assert(written.slot_count == 0);
        std.debug.assert(written.free_start == SlottedHeader.size);
    }

    /// Insert a row into the page. Returns the slot index.
    pub fn insert(page: *Page, data: []const u8) HeapError!u16 {
        std.debug.assert(page.header.page_type == .heap);
        std.debug.assert(data.len <= content_size);
        if (data.len > std.math.maxInt(u16)) return error.RowTooLarge;
        const row_len: u16 = @intCast(data.len);

        var header = SlottedHeader.read(&page.content);
        std.debug.assert(header.slot_count < max_slot_count);

        // Check if there's enough space: we need room for the slot entry + row data.
        const needed = Slot.size + row_len;
        if (header.free_end - header.free_start < needed) {
            _ = maybe_compact_for_required_space(page, needed);
            header = SlottedHeader.read(&page.content);
            if (header.free_end - header.free_start < needed) return error.PageFull;
        }

        // Allocate row data from the end.
        header.free_end -= row_len;
        const row_offset = header.free_end;

        // Write row data.
        @memcpy(page.content[row_offset..][0..row_len], data);

        // Write slot entry.
        const slot_index = header.slot_count;
        Slot.write_at(&page.content, slot_index, .{ .offset = row_offset, .len = row_len });

        // Update header.
        header.slot_count += 1;
        header.free_start += Slot.size;
        header.write(&page.content);

        std.debug.assert(header.free_start <= header.free_end);
        std.debug.assert(slot_index < header.slot_count);
        return slot_index;
    }

    /// Read a row from the page. Returns a slice into the page's content.
    pub fn read(page: *const Page, slot_idx: u16) HeapError![]const u8 {
        std.debug.assert(page.header.page_type == .heap);
        const header = SlottedHeader.read(&page.content);
        if (slot_idx >= header.slot_count) return error.InvalidSlot;

        const slot = Slot.read(&page.content, slot_idx);
        if (slot.len == Slot.deleted_len) return error.InvalidSlot;
        assert_live_slot_valid(slot, header);

        const row_end = @as(usize, slot.offset) + slot.len;
        std.debug.assert(row_end <= content_size);
        std.debug.assert(slot.len > 0);
        return page.content[slot.offset..][0..slot.len];
    }

    /// Update a row.
    ///
    /// If the new payload fits in the current slot, update in-place.
    /// If it does not fit, relocate the row to fresh space in the page and
    /// repoint the slot. If contiguous free space is insufficient but
    /// reclaimable fragmented bytes can cover the shortfall, this performs an
    /// automatic in-page compaction and retries.
    pub fn update(page: *Page, slot_idx: u16, new_data: []const u8) HeapError!void {
        std.debug.assert(page.header.page_type == .heap);
        var header = SlottedHeader.read(&page.content);
        if (slot_idx >= header.slot_count) return error.InvalidSlot;

        var slot = Slot.read(&page.content, slot_idx);
        if (slot.len == Slot.deleted_len) return error.InvalidSlot;
        assert_live_slot_valid(slot, header);

        if (new_data.len > std.math.maxInt(u16)) return error.RowTooLarge;
        const new_len: u16 = @intCast(new_data.len);

        if (new_len > slot.len) {
            // Grow by relocating to the current free region.
            if (header.free_end - header.free_start < new_len) {
                _ = maybe_compact_for_required_space(page, new_len);
                header = SlottedHeader.read(&page.content);
                if (header.free_end - header.free_start < new_len) return error.PageFull;
            }

            header.free_end -= new_len;
            const new_offset = header.free_end;
            @memcpy(page.content[new_offset..][0..new_len], new_data);

            slot.offset = new_offset;
            slot.len = new_len;
            Slot.write_at(&page.content, slot_idx, slot);
            header.write(&page.content);

            const row_end = @as(usize, slot.offset) + slot.len;
            std.debug.assert(slot.len == new_len);
            std.debug.assert(row_end <= content_size);
            return;
        }

        // Write new data at the same offset.
        @memcpy(page.content[slot.offset..][0..new_len], new_data);
        // Zero out any remaining bytes if new data is shorter.
        if (new_len < slot.len) {
            @memset(page.content[slot.offset + new_len ..][0 .. slot.len - new_len], 0);
        }
        // Update slot length.
        slot.len = new_len;
        Slot.write_at(&page.content, slot_idx, slot);
        const row_end = @as(usize, slot.offset) + slot.len;
        std.debug.assert(slot.len == new_len);
        std.debug.assert(row_end <= content_size);
    }

    /// Delete a row by marking its slot as deleted (tombstone).
    /// Does not reclaim space — that requires compaction.
    pub fn delete(page: *Page, slot_idx: u16) HeapError!void {
        std.debug.assert(page.header.page_type == .heap);
        const header = SlottedHeader.read(&page.content);
        if (slot_idx >= header.slot_count) return error.InvalidSlot;

        var slot = Slot.read(&page.content, slot_idx);
        if (slot.len == Slot.deleted_len) return error.InvalidSlot;
        assert_live_slot_valid(slot, header);

        slot.len = Slot.deleted_len;
        Slot.write_at(&page.content, slot_idx, slot);
        std.debug.assert(slot.len == Slot.deleted_len);
        std.debug.assert(Slot.read(&page.content, slot_idx).len == Slot.deleted_len);
    }

    /// Returns the number of live (non-deleted) rows in the page.
    pub fn live_count(page: *const Page) u16 {
        std.debug.assert(page.header.page_type == .heap);
        const header = SlottedHeader.read(&page.content);
        var count: u16 = 0;
        for (0..header.slot_count) |i| {
            const slot = Slot.read(&page.content, @intCast(i));
            if (slot.len != Slot.deleted_len) count += 1;
        }
        std.debug.assert(count <= header.slot_count);
        std.debug.assert(header.slot_count <= max_slot_count);
        return count;
    }

    /// Returns the amount of free space available for new inserts
    /// (contiguous free space between slot array and row data).
    pub fn free_space(page: *const Page) u16 {
        std.debug.assert(page.header.page_type == .heap);
        const header = SlottedHeader.read(&page.content);
        std.debug.assert(header.free_start <= content_size);
        std.debug.assert(header.free_end <= content_size);
        if (header.free_end <= header.free_start) return 0;
        return header.free_end - header.free_start;
    }

    /// Returns reclaimable bytes currently stranded as internal fragmentation.
    pub fn fragmented_bytes(page: *const Page) u16 {
        std.debug.assert(page.header.page_type == .heap);
        const header = SlottedHeader.read(&page.content);
        return fragmented_bytes_with_header(page, header);
    }

    /// Compacts live row payloads into a contiguous region at the end of page.
    /// Slot indexes are preserved.
    pub fn compact(page: *Page) void {
        std.debug.assert(page.header.page_type == .heap);
        var header = SlottedHeader.read(&page.content);

        var scratch: [content_size]u8 = undefined;
        var write_end: u16 = content_size;
        var slot_idx: u16 = 0;
        while (slot_idx < header.slot_count) : (slot_idx += 1) {
            var slot = Slot.read(&page.content, slot_idx);
            if (slot.len == Slot.deleted_len) continue;

            assert_live_slot_valid(slot, header);
            write_end -= slot.len;
            const dst_off = write_end;

            @memcpy(
                scratch[dst_off..][0..slot.len],
                page.content[slot.offset..][0..slot.len],
            );
            slot.offset = dst_off;
            Slot.write_at(&page.content, slot_idx, slot);
        }

        @memcpy(
            page.content[write_end..content_size],
            scratch[write_end..content_size],
        );
        @memset(page.content[header.free_start..write_end], 0);

        header.free_end = write_end;
        header.write(&page.content);
    }

    /// Returns the total slot count (including deleted slots).
    pub fn slot_count(page: *const Page) u16 {
        std.debug.assert(page.header.page_type == .heap);
        const count = SlottedHeader.read(&page.content).slot_count;
        std.debug.assert(count <= max_slot_count);
        const slot_array_end = @as(usize, count) * Slot.size + SlottedHeader.size;
        std.debug.assert(slot_array_end <= content_size);
        return count;
    }
};

fn maybe_compact_for_required_space(page: *Page, required_space: u16) bool {
    std.debug.assert(page.header.page_type == .heap);
    const before = HeapPage.free_space(page);
    if (before >= required_space) return false;

    const header = SlottedHeader.read(&page.content);
    const fragmented = fragmented_bytes_with_header(page, header);
    const shortfall = required_space - before;
    if (fragmented < shortfall) return false;

    HeapPage.compact(page);
    const after = HeapPage.free_space(page);
    std.debug.assert(after >= before);
    std.debug.assert(after >= required_space);
    return true;
}

fn fragmented_bytes_with_header(page: *const Page, header: SlottedHeader) u16 {
    const live_bytes = live_row_bytes_with_header(page, header);
    const max_contiguous_after = max_contiguous_after_compaction(header, live_bytes);
    const current = HeapPage.free_space(page);
    std.debug.assert(max_contiguous_after >= current);
    return max_contiguous_after - current;
}

fn live_row_bytes_with_header(page: *const Page, header: SlottedHeader) u16 {
    var live_bytes: u16 = 0;
    var slot_idx: u16 = 0;
    while (slot_idx < header.slot_count) : (slot_idx += 1) {
        const slot = Slot.read(&page.content, slot_idx);
        if (slot.len == Slot.deleted_len) continue;
        assert_live_slot_valid(slot, header);
        live_bytes += slot.len;
    }
    return live_bytes;
}

fn max_contiguous_after_compaction(header: SlottedHeader, live_bytes: u16) u16 {
    const used = @as(usize, header.free_start) + live_bytes;
    std.debug.assert(used <= content_size);
    return @intCast(content_size - used);
}

fn assert_header_valid(header: SlottedHeader) void {
    std.debug.assert(header.format_magic == SlottedHeader.format_magic_value);
    std.debug.assert(header.format_version == SlottedHeader.format_version_value);
    std.debug.assert(header.free_start >= SlottedHeader.size);
    std.debug.assert(header.free_start <= content_size);
    std.debug.assert(header.free_end <= content_size);
    std.debug.assert(header.free_start <= header.free_end);
    std.debug.assert((header.free_start - SlottedHeader.size) % Slot.size == 0);
    std.debug.assert(header.slot_count <= max_slot_count);
    const slot_bytes = @as(usize, header.slot_count) * Slot.size + SlottedHeader.size;
    std.debug.assert(slot_bytes <= header.free_start);
}

fn assert_live_slot_valid(slot: Slot, header: SlottedHeader) void {
    std.debug.assert(slot.len > Slot.deleted_len);
    std.debug.assert(slot.offset >= header.free_end);
    const row_end = @as(usize, slot.offset) + slot.len;
    std.debug.assert(row_end <= content_size);
}

// --- Tests ---

test "init creates empty heap page" {
    var page = Page.init(0, .free);
    HeapPage.init(&page);

    try std.testing.expectEqual(page_mod.PageType.heap, page.header.page_type);
    try std.testing.expectEqual(@as(u16, 0), HeapPage.slot_count(&page));
    try std.testing.expectEqual(@as(u16, 0), HeapPage.live_count(&page));
    // Free space should be content_size minus the slotted header.
    const expected = @as(u16, content_size - SlottedHeader.size);
    try std.testing.expectEqual(expected, HeapPage.free_space(&page));
}

test "heap header format metadata is initialized" {
    var page = Page.init(0, .free);
    HeapPage.init(&page);
    const header = SlottedHeader.read(&page.content);
    try std.testing.expectEqual(
        SlottedHeader.format_magic_value,
        header.format_magic,
    );
    try std.testing.expectEqual(
        SlottedHeader.format_version_value,
        header.format_version,
    );
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

    try std.testing.expectEqual(@as(u16, 3), HeapPage.live_count(&page));
}

test "delete marks slot as tombstone" {
    var page = Page.init(0, .free);
    HeapPage.init(&page);

    const s0 = try HeapPage.insert(&page, "data");
    try std.testing.expectEqual(@as(u16, 1), HeapPage.live_count(&page));

    try HeapPage.delete(&page, s0);
    try std.testing.expectEqual(@as(u16, 0), HeapPage.live_count(&page));
    try std.testing.expectEqual(@as(u16, 1), HeapPage.slot_count(&page));

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

test "update supports larger data when page has space" {
    var page = Page.init(0, .free);
    HeapPage.init(&page);

    const s0 = try HeapPage.insert(&page, "small");
    const before = HeapPage.free_space(&page);
    try HeapPage.update(&page, s0, "this is much larger than the original");
    const after = HeapPage.free_space(&page);

    const result = try HeapPage.read(&page, s0);
    try std.testing.expectEqualSlices(u8, "this is much larger than the original", result);
    try std.testing.expect(after < before);
}

test "update larger data returns page full when no contiguous free space" {
    var page = Page.init(0, .free);
    HeapPage.init(&page);

    var row: [100]u8 = undefined;
    @memset(&row, 0x44);

    var first_slot: ?u16 = null;
    while (true) {
        const inserted = HeapPage.insert(&page, &row) catch |err| {
            try std.testing.expectEqual(HeapError.PageFull, err);
            break;
        };
        if (first_slot == null) first_slot = inserted;
    }

    const grow = [_]u8{0x45} ** 120;
    const result = HeapPage.update(&page, first_slot.?, &grow);
    try std.testing.expectError(HeapError.PageFull, result);
}

test "update auto-compacts when fragmented bytes cover growth shortfall" {
    var page = Page.init(0, .free);
    HeapPage.init(&page);

    const large = [_]u8{0x41} ** 1800;
    const tiny = [_]u8{0x42} ** 32;
    const grown = [_]u8{0x43} ** 1200;
    const filler = [_]u8{0x44} ** 256;

    const s0 = try HeapPage.insert(&page, large[0..]);
    try HeapPage.update(&page, s0, tiny[0..]);
    try std.testing.expect(HeapPage.fragmented_bytes(&page) > 0);

    const needed: u16 = @intCast(grown.len);
    var attempts: u16 = 0;
    while (attempts < 128 and HeapPage.free_space(&page) >= needed) : (attempts += 1) {
        _ = HeapPage.insert(&page, filler[0..]) catch break;
    }
    try std.testing.expect(HeapPage.free_space(&page) < needed);

    try HeapPage.update(&page, s0, grown[0..]);
    const result = try HeapPage.read(&page, s0);
    try std.testing.expectEqualSlices(u8, grown[0..], result);
}

test "insert auto-compacts when fragmented bytes cover insert shortfall" {
    var page = Page.init(0, .free);
    HeapPage.init(&page);

    const large = [_]u8{0x51} ** 1700;
    const tiny = [_]u8{0x52} ** 24;
    const inserted = [_]u8{0x53} ** 900;
    const filler = [_]u8{0x54} ** 240;

    const s0 = try HeapPage.insert(&page, large[0..]);
    try HeapPage.update(&page, s0, tiny[0..]);
    try std.testing.expect(HeapPage.fragmented_bytes(&page) > 0);

    const needed: u16 = Slot.size + @as(u16, @intCast(inserted.len));
    var attempts: u16 = 0;
    while (attempts < 128 and HeapPage.free_space(&page) >= needed) : (attempts += 1) {
        _ = HeapPage.insert(&page, filler[0..]) catch break;
    }
    try std.testing.expect(HeapPage.free_space(&page) < needed);

    const slot = try HeapPage.insert(&page, inserted[0..]);
    const result = try HeapPage.read(&page, slot);
    try std.testing.expectEqualSlices(u8, inserted[0..], result);
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
    const max_attempts = (content_size - SlottedHeader.size) / (Slot.size + row.len) + 1;
    var saw_page_full = false;
    while (count < max_attempts) {
        _ = HeapPage.insert(&page, &row) catch |err| {
            try std.testing.expectEqual(HeapError.PageFull, err);
            saw_page_full = true;
            break;
        };
        count += 1;
    }
    try std.testing.expect(saw_page_full);
    try std.testing.expect(count > 0);
    try std.testing.expectEqual(count, HeapPage.live_count(&page));
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

    const before = HeapPage.free_space(&page);
    _ = try HeapPage.insert(&page, "test data");
    const after = HeapPage.free_space(&page);

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
