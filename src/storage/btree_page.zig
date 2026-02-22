//! B+ tree leaf and internal page node operations.
//!
//! Extracted from btree.zig — contains on-page cell layouts, binary search,
//! insert/delete at the page level, and byte read/write helpers.
const std = @import("std");
const page_mod = @import("page.zig");
const heap_mod = @import("heap.zig");
const btree = @import("btree.zig");

const content_size = page_mod.content_size;
const Page = page_mod.Page;
const RowId = heap_mod.RowId;
const BTreeError = btree.BTreeError;

// ============================================================================
// Byte read/write helpers (little-endian)
// ============================================================================

pub fn readU16(content: *const [content_size]u8, offset: anytype) u16 {
    const off: usize = @intCast(offset);
    return std.mem.littleToNative(u16, std.mem.bytesAsValue(u16, content[off..][0..2]).*);
}

pub fn writeU16(content: *[content_size]u8, offset: anytype, value: u16) void {
    const off: usize = @intCast(offset);
    @memcpy(content[off..][0..2], std.mem.asBytes(&std.mem.nativeToLittle(u16, value)));
}

pub fn readU64(content: *const [content_size]u8, offset: anytype) u64 {
    const off: usize = @intCast(offset);
    return std.mem.littleToNative(u64, std.mem.bytesAsValue(u64, content[off..][0..8]).*);
}

pub fn writeU64(content: *[content_size]u8, offset: anytype, value: u64) void {
    const off: usize = @intCast(offset);
    @memcpy(content[off..][0..8], std.mem.asBytes(&std.mem.nativeToLittle(u64, value)));
}

// ============================================================================
// Leaf Node
// ============================================================================

/// Operations on a B+ tree leaf page.
///
/// Layout within content area (8168 bytes):
///
/// ```
/// ┌───────────────────────────────────────────────────┐
/// │ LeafHeader (18 bytes)                             │
/// │   cell_count: u16, free_start: u16, free_end: u16 │
/// │   right_sibling: u64                              │
/// │   format_magic: u16, format_version: u8           │
/// ├───────────────────────────────────────────────────┤
/// │ Cell Pointer Array → (2 bytes each, grows right)  │
/// ├───────────── free space ──────────────────────────┤
/// │            ← Cell Data (grows left from end)       │
/// │  Each cell: [key_len:u16][key][page_id:u64][slot:u16] │
/// └───────────────────────────────────────────────────┘
/// ```
pub const LeafNode = struct {
    pub const header_size: u16 = 18;
    pub const cell_ptr_size: u16 = 2;
    pub const format_magic: u16 = 0x4C32; // "L2"
    pub const format_version: u8 = 1;
    /// No sibling. Uses maxInt to avoid collision with valid page IDs.
    pub const no_sibling: u64 = std.math.maxInt(u64);

    /// Initialize a page as an empty leaf node.
    pub fn init(page: *Page) void {
        page.header.page_type = .btree_leaf;
        @memset(&page.content, 0);
        writeU16(&page.content, 0, 0); // cell_count
        writeU16(&page.content, 2, header_size); // free_start
        writeU16(&page.content, 4, content_size); // free_end
        writeU64(&page.content, 6, no_sibling); // right_sibling
        writeU16(&page.content, 14, format_magic);
        page.content[16] = format_version;
        page.content[17] = 0;
    }

    pub fn cellCount(content: *const [content_size]u8) u16 {
        return readU16(content, 0);
    }

    pub fn freeSpace(content: *const [content_size]u8) u16 {
        const free_start = readU16(content, 2);
        const free_end = readU16(content, 4);
        if (free_end <= free_start) return 0;
        return free_end - free_start;
    }

    pub fn rightSibling(content: *const [content_size]u8) u64 {
        return readU64(content, 6);
    }

    pub fn setRightSibling(content: *[content_size]u8, sibling: u64) void {
        writeU64(content, 6, sibling);
    }

    /// Returns the offset within the content area where cell `index` data starts.
    fn cellOffset(content: *const [content_size]u8, index: u16) u16 {
        const ptr_pos = header_size + @as(usize, index) * cell_ptr_size;
        return readU16(content, ptr_pos);
    }

    /// Returns the key stored in cell `index`.
    pub fn getKey(content: *const [content_size]u8, index: u16) []const u8 {
        const offset = cellOffset(content, index);
        const key_len = readU16(content, offset);
        return content[offset + 2 ..][0..key_len];
    }

    /// Returns the RowId stored in cell `index`.
    pub fn getRowId(content: *const [content_size]u8, index: u16) RowId {
        const offset = cellOffset(content, index);
        const key_len = readU16(content, offset);
        const rid_offset = offset + 2 + key_len;
        return .{
            .page_id = readU64(content, rid_offset),
            .slot = readU16(content, rid_offset + 8),
        };
    }

    /// Binary search for `key`. Returns the index where the key is or should be inserted.
    /// If found, returns .{ .index = i, .found = true }.
    pub fn search(content: *const [content_size]u8, key: []const u8) struct { index: u16, found: bool } {
        const count = cellCount(content);
        if (count == 0) return .{ .index = 0, .found = false };

        var lo: u16 = 0;
        var hi: u16 = count;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const mid_key = getKey(content, mid);
            const ord = std.mem.order(u8, mid_key, key);
            switch (ord) {
                .lt => lo = mid + 1,
                .gt => hi = mid,
                .eq => return .{ .index = mid, .found = true },
            }
        }
        return .{ .index = lo, .found = false };
    }

    /// Insert a key/RowId pair into the leaf page. Maintains sorted order.
    /// Returns error.PageFull if not enough space, error.DuplicateKey if key exists.
    pub fn insert(content: *[content_size]u8, key: []const u8, row_id: RowId) BTreeError!void {
        const result = search(content, key);
        if (result.found) return error.DuplicateKey;

        // Cell data = key_len(2) + key + page_id(8) + slot(2). Ensure no u16 overflow.
        if (key.len > std.math.maxInt(u16) - 12) return error.PageFull;
        const key_len: u16 = @intCast(key.len);
        // Cell data: key_len(2) + key + page_id(8) + slot(2) = 12 + key_len
        const cell_data_size: u16 = 2 + key_len + 8 + 2;
        const needed: u16 = cell_ptr_size + cell_data_size;

        const free_start = readU16(content, 2);
        const free_end = readU16(content, 4);
        if (free_end - free_start < needed) return error.PageFull;

        const count = cellCount(content);
        const insert_pos = result.index;

        // Allocate cell data from the end.
        const new_free_end = free_end - cell_data_size;
        const cell_off = new_free_end;

        // Write cell data.
        writeU16(content, cell_off, key_len);
        @memcpy(content[cell_off + 2 ..][0..key_len], key);
        writeU64(content, cell_off + 2 + key_len, row_id.page_id);
        writeU16(content, cell_off + 2 + key_len + 8, row_id.slot);

        // Shift cell pointers right to make room at insert_pos.
        // Move pointers [insert_pos..count] to [insert_pos+1..count+1]
        var i: u16 = count;
        while (i > insert_pos) {
            i -= 1;
            const old_ptr = readU16(content, header_size + @as(usize, i) * cell_ptr_size);
            writeU16(content, header_size + @as(usize, i + 1) * cell_ptr_size, old_ptr);
        }

        // Write new cell pointer.
        writeU16(content, header_size + @as(usize, insert_pos) * cell_ptr_size, cell_off);

        // Update header.
        writeU16(content, 0, count + 1); // cell_count
        writeU16(content, 2, free_start + cell_ptr_size); // free_start
        writeU16(content, 4, new_free_end); // free_end
    }

    /// Delete a key from the leaf. Returns error.KeyNotFound if not present.
    pub fn delete(content: *[content_size]u8, key: []const u8) BTreeError!void {
        const result = search(content, key);
        if (!result.found) return error.KeyNotFound;

        const count = cellCount(content);
        const del_pos = result.index;

        // Shift cell pointers left to fill the gap.
        var i: u16 = del_pos;
        while (i + 1 < count) : (i += 1) {
            const next_ptr = readU16(content, header_size + @as(usize, i + 1) * cell_ptr_size);
            writeU16(content, header_size + @as(usize, i) * cell_ptr_size, next_ptr);
        }

        // Update header. Note: free_end doesn't reclaim space (no compaction).
        const free_start = readU16(content, 2);
        writeU16(content, 0, count - 1); // cell_count
        writeU16(content, 2, free_start - cell_ptr_size); // free_start
    }
};

// ============================================================================
// Internal Node
// ============================================================================

/// Operations on a B+ tree internal page.
///
/// Layout within content area (8168 bytes):
///
/// ```
/// ┌───────────────────────────────────────────────────┐
/// │ InternalHeader (20 bytes)                         │
/// │   cell_count: u16, free_start: u16, free_end: u16 │
/// │   left_child: u64                                  │
/// │   format_magic: u16, format_version: u8           │
/// ├───────────────────────────────────────────────────┤
/// │ Cell Pointer Array → (2 bytes each, grows right)  │
/// ├───────────── free space ──────────────────────────┤
/// │            ← Cell Data (grows left from end)       │
/// │  Each cell: [key_len:u16][key][right_child:u64]   │
/// └───────────────────────────────────────────────────┘
/// ```
pub const InternalNode = struct {
    pub const header_size: u16 = 20;
    pub const cell_ptr_size: u16 = 2;
    pub const format_magic: u16 = 0x4932; // "I2"
    pub const format_version: u8 = 1;

    /// Initialize a page as an empty internal node.
    pub fn init(page: *Page) void {
        page.header.page_type = .btree_internal;
        @memset(&page.content, 0);
        writeU16(&page.content, 0, 0); // cell_count
        writeU16(&page.content, 2, header_size); // free_start
        writeU16(&page.content, 4, content_size); // free_end
        writeU64(&page.content, 6, 0); // left_child
        writeU16(&page.content, 14, format_magic);
        page.content[16] = format_version;
        page.content[17] = 0;
        writeU16(&page.content, 18, 0);
    }

    pub fn cellCount(content: *const [content_size]u8) u16 {
        return readU16(content, 0);
    }

    pub fn freeSpace(content: *const [content_size]u8) u16 {
        const free_start = readU16(content, 2);
        const free_end = readU16(content, 4);
        if (free_end <= free_start) return 0;
        return free_end - free_start;
    }

    pub fn leftChild(content: *const [content_size]u8) u64 {
        return readU64(content, 6);
    }

    pub fn setLeftChild(content: *[content_size]u8, child: u64) void {
        writeU64(content, 6, child);
    }

    fn cellOffset(content: *const [content_size]u8, index: u16) u16 {
        const ptr_pos = header_size + @as(usize, index) * cell_ptr_size;
        return readU16(content, ptr_pos);
    }

    /// Returns the key stored in cell `index`.
    pub fn getKey(content: *const [content_size]u8, index: u16) []const u8 {
        const offset = cellOffset(content, index);
        const key_len = readU16(content, offset);
        return content[offset + 2 ..][0..key_len];
    }

    /// Returns the right child page_id of cell `index`.
    pub fn getRightChild(content: *const [content_size]u8, index: u16) u64 {
        const offset = cellOffset(content, index);
        const key_len = readU16(content, offset);
        return readU64(content, offset + 2 + key_len);
    }

    /// Find which child to follow for a given key.
    /// Returns the page_id of the child that may contain `key`.
    pub fn findChild(content: *const [content_size]u8, key: []const u8) u64 {
        const count = cellCount(content);
        // Binary search for the first key > search key.
        var lo: u16 = 0;
        var hi: u16 = count;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const mid_key = getKey(content, mid);
            const ord = std.mem.order(u8, mid_key, key);
            switch (ord) {
                .lt => lo = mid + 1,
                .eq => lo = mid + 1, // go right on equal (key is in right subtree)
                .gt => hi = mid,
            }
        }
        // lo = index of first key > search key.
        // If lo == 0, go to left_child. Otherwise go to right_child of (lo-1).
        if (lo == 0) return leftChild(content);
        return getRightChild(content, lo - 1);
    }

    /// Find the index of the child pointer that was followed for a given key.
    /// Returns 0 for left_child, or cell_index + 1 for right_child of that cell.
    /// This is used during splits to know where to insert the promoted key.
    pub fn findChildIndex(content: *const [content_size]u8, key: []const u8) u16 {
        const count = cellCount(content);
        var lo: u16 = 0;
        var hi: u16 = count;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const mid_key = getKey(content, mid);
            const ord = std.mem.order(u8, mid_key, key);
            switch (ord) {
                .lt => lo = mid + 1,
                .eq => lo = mid + 1,
                .gt => hi = mid,
            }
        }
        return lo;
    }

    /// Insert a key + right_child pointer. Maintains sorted order.
    /// The new key separates the existing child (at insert_pos) from the new right_child.
    pub fn insert(content: *[content_size]u8, key: []const u8, right_child: u64) BTreeError!void {
        // Cell data = key_len(2) + key + right_child(8). Ensure no u16 overflow.
        if (key.len > std.math.maxInt(u16) - 10) return error.PageFull;
        const key_len: u16 = @intCast(key.len);
        // Cell data: key_len(2) + key + right_child(8)
        const cell_data_size: u16 = 2 + key_len + 8;
        const needed: u16 = cell_ptr_size + cell_data_size;

        const free_start = readU16(content, 2);
        const free_end = readU16(content, 4);
        if (free_end - free_start < needed) return error.PageFull;

        const count = cellCount(content);

        // Find insert position (sorted order).
        var insert_pos: u16 = 0;
        while (insert_pos < count) : (insert_pos += 1) {
            const existing = getKey(content, insert_pos);
            if (std.mem.order(u8, key, existing) == .lt) break;
        }

        // Allocate cell data from the end.
        const new_free_end = free_end - cell_data_size;

        // Write cell data.
        writeU16(content, new_free_end, key_len);
        @memcpy(content[new_free_end + 2 ..][0..key_len], key);
        writeU64(content, new_free_end + 2 + key_len, right_child);

        // Shift cell pointers right.
        var i: u16 = count;
        while (i > insert_pos) {
            i -= 1;
            const old_ptr = readU16(content, header_size + @as(usize, i) * cell_ptr_size);
            writeU16(content, header_size + @as(usize, i + 1) * cell_ptr_size, old_ptr);
        }

        // Write new cell pointer.
        writeU16(content, header_size + @as(usize, insert_pos) * cell_ptr_size, new_free_end);

        // Update header.
        writeU16(content, 0, count + 1);
        writeU16(content, 2, free_start + cell_ptr_size);
        writeU16(content, 4, new_free_end);
    }
};
