const std = @import("std");
const io = @import("io.zig");
const page_mod = @import("page.zig");
const heap_mod = @import("heap.zig");
const buffer_pool_mod = @import("buffer_pool.zig");
const wal_mod = @import("wal.zig");

const Page = page_mod.Page;
const content_size = page_mod.content_size;
const RowId = heap_mod.RowId;
const BufferPool = buffer_pool_mod.BufferPool;
const Wal = wal_mod.Wal;

pub const BTreeError = error{
    PageFull,
    DuplicateKey,
    KeyNotFound,
    InvalidPage,
    Corruption,
    TreeEmpty,
    AllFramesPinned,
    ChecksumMismatch,
    StorageRead,
    StorageWrite,
    StorageFsync,
    WalNotFlushed,
    WalWriteError,
    WalFsyncError,
    OutOfMemory,
};

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
    const header_size: u16 = 18;
    const cell_ptr_size: u16 = 2;
    const format_magic: u16 = 0x4C32; // "L2"
    const format_version: u8 = 1;
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
    const header_size: u16 = 20;
    const cell_ptr_size: u16 = 2;
    const format_magic: u16 = 0x4932; // "I2"
    const format_version: u8 = 1;

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

// ============================================================================
// B+ Tree
// ============================================================================

/// Max number of internal levels traversed from root to leaf.
/// 16 levels with fanout ~400 can index far more entries than any reasonable dataset.
const max_btree_depth = 16;
/// Max sibling-page hops while advancing a range scan iterator.
/// This guards against malformed cyclic sibling chains.
const max_leaf_sibling_hops = 1024;
/// Fixed split scratch capacities derived from page-size bounds.
const max_leaf_split_entries = (content_size - LeafNode.header_size) / 14 + 1;
const max_internal_split_entries = (content_size - InternalNode.header_size) / 12 + 1;

/// A path entry tracks the page_id and the child index followed at each level.
const PathEntry = struct {
    page_id: u64,
    /// The child index we descended into. For internal nodes: 0 = left_child,
    /// N = right_child of cell N-1.
    child_index: u16,
};

pub const BTree = struct {
    root_page_id: u64,
    next_page_id: u64,
    pool: *BufferPool,
    wal: ?*Wal,

    /// Create a new B+ tree, allocating an empty root leaf page.
    pub fn init(pool: *BufferPool, wal: ?*Wal, start_page_id: u64) BTreeError!BTree {
        const page = pool.pin(start_page_id) catch |e| return mapPoolError(e);
        LeafNode.init(page);
        pool.unpin(start_page_id, true);

        return .{
            .root_page_id = start_page_id,
            .next_page_id = start_page_id + 1,
            .pool = pool,
            .wal = wal,
        };
    }

    fn allocPage(self: *BTree) u64 {
        const id = self.next_page_id;
        self.next_page_id += 1;
        return id;
    }

    /// Look up a key. Returns the RowId if found, null otherwise.
    pub fn find(self: *BTree, key: []const u8) BTreeError!?RowId {
        var page_id = self.root_page_id;
        var depth: usize = 0;

        // Traverse internal nodes to find the leaf.
        while (true) {
            if (depth > max_btree_depth) return error.Corruption;
            const page = self.pool.pin(page_id) catch |e| return mapPoolError(e);
            defer self.pool.unpin(page_id, false);

            switch (page.header.page_type) {
                .btree_leaf => {
                    if (!validateLeafStructure(&page.content)) return error.Corruption;
                    const result = LeafNode.search(&page.content, key);
                    if (result.found) {
                        return LeafNode.getRowId(&page.content, result.index);
                    }
                    return null;
                },
                .btree_internal => {
                    if (!validateInternalStructure(&page.content)) return error.Corruption;
                    const next = InternalNode.findChild(&page.content, key);
                    if (next == page_id) return error.Corruption;
                    page_id = next;
                    depth += 1;
                },
                else => return error.InvalidPage,
            }
        }
    }

    /// Insert a key/RowId pair. Handles splits automatically.
    pub fn insert(self: *BTree, key: []const u8, row_id: RowId) BTreeError!void {
        // Build path from root to leaf.
        var path: [max_btree_depth]PathEntry = undefined;
        var depth: usize = 0;
        var page_id = self.root_page_id;

        while (true) {
            const page = self.pool.pin(page_id) catch |e| return mapPoolError(e);

            switch (page.header.page_type) {
                .btree_leaf => {
                    if (!validateLeafStructure(&page.content)) {
                        self.pool.unpin(page_id, false);
                        return error.Corruption;
                    }
                    self.pool.unpin(page_id, false);
                    break;
                },
                .btree_internal => {
                    if (!validateInternalStructure(&page.content)) {
                        self.pool.unpin(page_id, false);
                        return error.Corruption;
                    }
                    const child_index = InternalNode.findChildIndex(&page.content, key);
                    const next = if (child_index == 0)
                        InternalNode.leftChild(&page.content)
                    else
                        InternalNode.getRightChild(&page.content, child_index - 1);
                    if (next == page_id) {
                        self.pool.unpin(page_id, false);
                        return error.Corruption;
                    }
                    if (depth >= max_btree_depth) {
                        self.pool.unpin(page_id, false);
                        return error.Corruption;
                    }
                    path[depth] = .{ .page_id = page_id, .child_index = child_index };
                    depth += 1;
                    self.pool.unpin(page_id, false);

                    // Follow the child pointer.
                    page_id = next;
                },
                else => {
                    self.pool.unpin(page_id, false);
                    return error.InvalidPage;
                },
            }
        }

        // Try to insert into the leaf.
        const leaf_page = self.pool.pin(page_id) catch |e| return mapPoolError(e);
        if (!validateLeafStructure(&leaf_page.content)) {
            self.pool.unpin(page_id, false);
            return error.Corruption;
        }

        const leaf_result = LeafNode.insert(&leaf_page.content, key, row_id);
        if (leaf_result) |_| {
            // Log WAL record after successful modification.
            if (self.wal) |wal| {
                const lsn = wal.append(0, .btree_insert, page_id, key) catch |e|
                    return mapWalAppendError(e);
                leaf_page.header.lsn = lsn;
            }
            self.pool.unpin(page_id, true);
            return;
        } else |err| switch (err) {
            error.PageFull => {
                self.pool.unpin(page_id, false);
                // Need to split — split path handles its own WAL records.
                try self.splitAndInsert(page_id, key, row_id, path[0..depth]);
            },
            error.DuplicateKey => {
                self.pool.unpin(page_id, false);
                return error.DuplicateKey;
            },
            else => {
                self.pool.unpin(page_id, false);
                return err;
            },
        }
    }

    /// Delete a key from the tree. No rebalancing (PostgreSQL-style).
    pub fn delete(self: *BTree, key: []const u8) BTreeError!void {
        var page_id = self.root_page_id;
        var depth: usize = 0;

        // Traverse to the leaf.
        while (true) {
            if (depth > max_btree_depth) return error.Corruption;
            const page = self.pool.pin(page_id) catch |e| return mapPoolError(e);

            switch (page.header.page_type) {
                .btree_leaf => {
                    if (!validateLeafStructure(&page.content)) {
                        self.pool.unpin(page_id, false);
                        return error.Corruption;
                    }
                    LeafNode.delete(&page.content, key) catch |err| {
                        self.pool.unpin(page_id, false);
                        return err;
                    };
                    // Log WAL record after successful modification.
                    if (self.wal) |wal| {
                        const lsn = wal.append(0, .btree_delete, page_id, key) catch |e|
                            return mapWalAppendError(e);
                        page.header.lsn = lsn;
                    }
                    self.pool.unpin(page_id, true);
                    return;
                },
                .btree_internal => {
                    if (!validateInternalStructure(&page.content)) {
                        self.pool.unpin(page_id, false);
                        return error.Corruption;
                    }
                    const next = InternalNode.findChild(&page.content, key);
                    if (next == page_id) {
                        self.pool.unpin(page_id, false);
                        return error.Corruption;
                    }
                    self.pool.unpin(page_id, false);
                    page_id = next;
                    depth += 1;
                },
                else => {
                    self.pool.unpin(page_id, false);
                    return error.InvalidPage;
                },
            }
        }
    }

    /// Range scan from `lo` (inclusive) to `hi` (exclusive). Either bound can be null
    /// for open-ended scans.
    pub fn rangeScan(self: *BTree, lo: ?[]const u8, hi: ?[]const u8) BTreeError!RangeScanIterator {
        // Find the starting leaf.
        var page_id = self.root_page_id;
        var depth: usize = 0;

        while (true) {
            if (depth > max_btree_depth) return error.Corruption;
            const page = self.pool.pin(page_id) catch |e| return mapPoolError(e);

            switch (page.header.page_type) {
                .btree_leaf => {
                    if (!validateLeafStructure(&page.content)) {
                        self.pool.unpin(page_id, false);
                        return error.Corruption;
                    }
                    // Find starting position within the leaf.
                    var start_idx: u16 = 0;
                    if (lo) |lo_key| {
                        const result = LeafNode.search(&page.content, lo_key);
                        start_idx = result.index;
                    }
                    self.pool.unpin(page_id, false);

                    return .{
                        .pool = self.pool,
                        .current_page_id = page_id,
                        .current_index = start_idx,
                        .hi = hi,
                        .done = false,
                        .has_pinned_page = false,
                        .sibling_hops = 0,
                    };
                },
                .btree_internal => {
                    if (!validateInternalStructure(&page.content)) {
                        self.pool.unpin(page_id, false);
                        return error.Corruption;
                    }
                    const next = if (lo) |lo_key|
                        InternalNode.findChild(&page.content, lo_key)
                    else
                        // No lower bound — start from leftmost child.
                        InternalNode.leftChild(&page.content);
                    if (next == page_id) {
                        self.pool.unpin(page_id, false);
                        return error.Corruption;
                    }
                    self.pool.unpin(page_id, false);
                    page_id = next;
                    depth += 1;
                },
                else => {
                    self.pool.unpin(page_id, false);
                    return error.InvalidPage;
                },
            }
        }
    }

    // --- Split logic ---

    fn splitAndInsert(self: *BTree, leaf_id: u64, key: []const u8, row_id: RowId, path: []const PathEntry) BTreeError!void {
        // Collect all existing cells + the new one, then redistribute.
        // Keys point into page content, so we must copy them into a contiguous
        // buffer BEFORE re-initializing the page.
        const leaf_page = self.pool.pin(leaf_id) catch |e| return mapPoolError(e);
        if (!validateLeafStructure(&leaf_page.content)) {
            self.pool.unpin(leaf_id, false);
            return error.Corruption;
        }
        const old_count = LeafNode.cellCount(&leaf_page.content);
        const total: usize = @as(usize, old_count) + 1;

        // First pass: compute total key bytes needed.
        var total_key_bytes: usize = 0;
        for (0..old_count) |idx_usize| {
            total_key_bytes += LeafNode.getKey(&leaf_page.content, @intCast(idx_usize)).len;
        }
        total_key_bytes += key.len;

        // Allocate entries array and key data buffer.
        if (total > max_leaf_split_entries) return error.Corruption;
        if (total_key_bytes > content_size) return error.Corruption;
        var entries: [max_leaf_split_entries]TempLeafEntry = undefined;
        var key_buf: [content_size]u8 = undefined;

        // Second pass: copy keys into owned buffer and build entries array.
        var buf_offset: usize = 0;
        var inserted = false;
        var ki: usize = 0;
        for (0..old_count) |idx_usize| {
            const idx: u16 = @intCast(idx_usize);
            const existing_key = LeafNode.getKey(&leaf_page.content, idx);
            if (!inserted) {
                if (std.mem.order(u8, key, existing_key) == .lt) {
                    const owned = key_buf[buf_offset..][0..key.len];
                    @memcpy(owned, key);
                    buf_offset += key.len;
                    entries[ki] = .{ .key = owned, .row_id = row_id };
                    ki += 1;
                    inserted = true;
                }
            }
            const owned = key_buf[buf_offset..][0..existing_key.len];
            @memcpy(owned, existing_key);
            buf_offset += existing_key.len;
            entries[ki] = .{ .key = owned, .row_id = LeafNode.getRowId(&leaf_page.content, idx) };
            ki += 1;
        }
        if (!inserted) {
            const owned = key_buf[buf_offset..][0..key.len];
            @memcpy(owned, key);
            buf_offset += key.len;
            entries[ki] = .{ .key = owned, .row_id = row_id };
            ki += 1;
        }

        const old_right_sibling = LeafNode.rightSibling(&leaf_page.content);
        self.pool.unpin(leaf_id, false);

        // Split at midpoint.
        const mid = total / 2;

        // Allocate new right page.
        const right_id = self.allocPage();
        const right_page = self.pool.pin(right_id) catch |e| return mapPoolError(e);
        LeafNode.init(right_page);

        // Re-pin left page and reinitialize it.
        const left_page = self.pool.pin(leaf_id) catch |e| {
            self.pool.unpin(right_id, false);
            return mapPoolError(e);
        };

        // Log WAL for the split.
        if (self.wal) |wal| {
            const lsn = wal.append(0, .btree_split_leaf, leaf_id, key) catch |e|
                return mapWalAppendError(e);
            left_page.header.lsn = lsn;
            right_page.header.lsn = lsn;
        }

        LeafNode.init(left_page);

        // Fill left page with [0..mid).
        for (0..mid) |i| {
            LeafNode.insert(&left_page.content, entries[i].key, entries[i].row_id) catch
                @panic("leaf split redistribution failed");
        }

        // Fill right page with [mid..total).
        for (mid..total) |i| {
            LeafNode.insert(&right_page.content, entries[i].key, entries[i].row_id) catch
                @panic("leaf split redistribution failed");
        }

        // Set sibling pointers: left -> right -> old_right_sibling.
        LeafNode.setRightSibling(&left_page.content, right_id);
        LeafNode.setRightSibling(&right_page.content, old_right_sibling);

        self.pool.unpin(leaf_id, true);
        self.pool.unpin(right_id, true);

        // Promote separator key (first key of right page) to parent.
        // entries[mid].key is already in our owned key_buf, safe to use.
        try self.insertIntoParent(leaf_id, entries[mid].key, right_id, path);
    }

    fn insertIntoParent(self: *BTree, left_id: u64, key: []const u8, right_id: u64, path: []const PathEntry) BTreeError!void {
        if (path.len == 0) {
            // Splitting the root — create a new root.
            try self.splitRoot(left_id, key, right_id);
            return;
        }

        const parent_entry = path[path.len - 1];
        const parent_id = parent_entry.page_id;
        const parent_page = self.pool.pin(parent_id) catch |e| return mapPoolError(e);

        if (self.wal) |wal| {
            const lsn = wal.append(0, .btree_split_internal, parent_id, key) catch |e|
                return mapWalAppendError(e);
            parent_page.header.lsn = lsn;
        }

        const result = InternalNode.insert(&parent_page.content, key, right_id);
        if (result) |_| {
            self.pool.unpin(parent_id, true);
            return;
        } else |err| switch (err) {
            error.PageFull => {
                self.pool.unpin(parent_id, false);
                try self.splitInternal(parent_id, key, right_id, path[0 .. path.len - 1]);
            },
            else => {
                self.pool.unpin(parent_id, false);
                return err;
            },
        }
    }

    fn splitInternal(self: *BTree, node_id: u64, new_key: []const u8, new_right_child: u64, path: []const PathEntry) BTreeError!void {
        const node_page = self.pool.pin(node_id) catch |e| return mapPoolError(e);
        if (!validateInternalStructure(&node_page.content)) {
            self.pool.unpin(node_id, false);
            return error.Corruption;
        }
        const old_count = InternalNode.cellCount(&node_page.content);
        const old_left_child = InternalNode.leftChild(&node_page.content);

        const total: usize = @as(usize, old_count) + 1;
        if (total > max_internal_split_entries) return error.Corruption;

        // Compute total key bytes for owned copy.
        var total_key_bytes: usize = 0;
        for (0..old_count) |idx_usize| {
            total_key_bytes += InternalNode.getKey(&node_page.content, @intCast(idx_usize)).len;
        }
        total_key_bytes += new_key.len;
        if (total_key_bytes > content_size) return error.Corruption;

        var entries: [max_internal_split_entries]TempInternalEntry = undefined;
        var key_buf: [content_size]u8 = undefined;

        var buf_offset: usize = 0;
        var inserted = false;
        var ki: usize = 0;
        for (0..old_count) |idx_usize| {
            const idx: u16 = @intCast(idx_usize);
            const existing_key = InternalNode.getKey(&node_page.content, idx);
            if (!inserted) {
                if (std.mem.order(u8, new_key, existing_key) == .lt) {
                    const owned = key_buf[buf_offset..][0..new_key.len];
                    @memcpy(owned, new_key);
                    buf_offset += new_key.len;
                    entries[ki] = .{ .key = owned, .right_child = new_right_child };
                    ki += 1;
                    inserted = true;
                }
            }
            const owned = key_buf[buf_offset..][0..existing_key.len];
            @memcpy(owned, existing_key);
            buf_offset += existing_key.len;
            entries[ki] = .{ .key = owned, .right_child = InternalNode.getRightChild(&node_page.content, idx) };
            ki += 1;
        }
        if (!inserted) {
            const owned = key_buf[buf_offset..][0..new_key.len];
            @memcpy(owned, new_key);
            buf_offset += new_key.len;
            entries[ki] = .{ .key = owned, .right_child = new_right_child };
            ki += 1;
        }

        self.pool.unpin(node_id, false);

        // Split: left gets [0..mid), promoted key is entries[mid], right gets [mid+1..total).
        const mid = total / 2;

        const new_right_id = self.allocPage();
        const right_page = self.pool.pin(new_right_id) catch |e| return mapPoolError(e);
        InternalNode.init(right_page);

        const left_page = self.pool.pin(node_id) catch |e| {
            self.pool.unpin(new_right_id, false);
            return mapPoolError(e);
        };

        if (self.wal) |wal| {
            const lsn = wal.append(0, .btree_split_internal, node_id, new_key) catch |e|
                return mapWalAppendError(e);
            left_page.header.lsn = lsn;
            right_page.header.lsn = lsn;
        }

        InternalNode.init(left_page);
        InternalNode.setLeftChild(&left_page.content, old_left_child);

        // Fill left with [0..mid).
        for (0..mid) |i| {
            InternalNode.insert(&left_page.content, entries[i].key, entries[i].right_child) catch
                @panic("internal split redistribution failed");
        }

        // Promoted key is entries[mid]. Right child's left_child = entries[mid].right_child.
        InternalNode.setLeftChild(&right_page.content, entries[mid].right_child);

        // Fill right with [mid+1..total).
        for (mid + 1..total) |i| {
            InternalNode.insert(&right_page.content, entries[i].key, entries[i].right_child) catch
                @panic("internal split redistribution failed");
        }

        self.pool.unpin(node_id, true);
        self.pool.unpin(new_right_id, true);

        // entries[mid].key is in our owned key_buf, safe to use.
        try self.insertIntoParent(node_id, entries[mid].key, new_right_id, path);
    }

    fn splitRoot(self: *BTree, left_id: u64, key: []const u8, right_id: u64) BTreeError!void {
        const new_root_id = self.allocPage();
        const root_page = self.pool.pin(new_root_id) catch |e| return mapPoolError(e);
        InternalNode.init(root_page);

        if (self.wal) |wal| {
            const lsn = wal.append(0, .btree_new_root, new_root_id, key) catch |e|
                return mapWalAppendError(e);
            root_page.header.lsn = lsn;
        }

        InternalNode.setLeftChild(&root_page.content, left_id);
        InternalNode.insert(&root_page.content, key, right_id) catch
            @panic("root split insertion failed");

        self.pool.unpin(new_root_id, true);
        self.root_page_id = new_root_id;
    }
};

// ============================================================================
// Range Scan Iterator
// ============================================================================

pub const RangeScanIterator = struct {
    pool: *BufferPool,
    current_page_id: u64,
    current_index: u16,
    hi: ?[]const u8,
    done: bool,
    /// True if the current leaf page is pinned by this iterator.
    /// The page stays pinned so returned key slices remain valid
    /// until the next call to `next()` or `close()`.
    has_pinned_page: bool,
    sibling_hops: usize,

    pub const Entry = struct {
        key: []const u8,
        row_id: RowId,
    };

    /// Advance the iterator. Returns the next key/RowId pair, or null when exhausted.
    /// The returned key slice is valid until the next call to `next()` or `close()`.
    pub fn next(self: *RangeScanIterator) BTreeError!?Entry {
        while (!self.done) {
            // Release the pin from the previous next() call.
            if (self.has_pinned_page) {
                self.pool.unpin(self.current_page_id, false);
                self.has_pinned_page = false;
            }

            const page = self.pool.pin(self.current_page_id) catch |e| return mapPoolError(e);
            if (page.header.page_type != .btree_leaf or !validateLeafStructure(&page.content)) {
                self.pool.unpin(self.current_page_id, false);
                return if (page.header.page_type != .btree_leaf)
                    error.InvalidPage
                else
                    error.Corruption;
            }
            const count = LeafNode.cellCount(&page.content);

            if (self.current_index >= count) {
                // Move to right sibling.
                const sibling = LeafNode.rightSibling(&page.content);
                self.pool.unpin(self.current_page_id, false);

                if (sibling == LeafNode.no_sibling) {
                    self.done = true;
                    return null;
                }
                if (sibling == self.current_page_id) return error.Corruption;
                if (self.sibling_hops >= max_leaf_sibling_hops) return error.Corruption;
                self.sibling_hops += 1;

                self.current_page_id = sibling;
                self.current_index = 0;
                continue;
            }

            const entry_key = LeafNode.getKey(&page.content, self.current_index);

            // Check upper bound.
            if (self.hi) |hi_key| {
                if (std.mem.order(u8, entry_key, hi_key) != .lt) {
                    self.pool.unpin(self.current_page_id, false);
                    self.done = true;
                    return null;
                }
            }

            const entry_row_id = LeafNode.getRowId(&page.content, self.current_index);
            self.current_index += 1;

            // Keep the page pinned — the returned key slice points into it.
            self.has_pinned_page = true;

            return .{ .key = entry_key, .row_id = entry_row_id };
        }
        return null;
    }

    /// Close the iterator and release any pinned page.
    pub fn close(self: *RangeScanIterator) void {
        if (self.has_pinned_page) {
            self.pool.unpin(self.current_page_id, false);
            self.has_pinned_page = false;
        }
        self.done = true;
    }
};

// ============================================================================
// Temp entry types for splits
// ============================================================================

const TempLeafEntry = struct {
    key: []const u8,
    row_id: RowId,
};

const TempInternalEntry = struct {
    key: []const u8,
    right_child: u64,
};

// ============================================================================
// Byte read/write helpers (little-endian)
// ============================================================================

fn readU16(content: *const [content_size]u8, offset: anytype) u16 {
    const off: usize = @intCast(offset);
    return std.mem.littleToNative(u16, std.mem.bytesAsValue(u16, content[off..][0..2]).*);
}

fn writeU16(content: *[content_size]u8, offset: anytype, value: u16) void {
    const off: usize = @intCast(offset);
    @memcpy(content[off..][0..2], std.mem.asBytes(&std.mem.nativeToLittle(u16, value)));
}

fn readU64(content: *const [content_size]u8, offset: anytype) u64 {
    const off: usize = @intCast(offset);
    return std.mem.littleToNative(u64, std.mem.bytesAsValue(u64, content[off..][0..8]).*);
}

fn writeU64(content: *[content_size]u8, offset: anytype, value: u64) void {
    const off: usize = @intCast(offset);
    @memcpy(content[off..][0..8], std.mem.asBytes(&std.mem.nativeToLittle(u64, value)));
}

fn validateLeafStructure(content: *const [content_size]u8) bool {
    if (readU16(content, 14) != LeafNode.format_magic) return false;
    if (content[16] != LeafNode.format_version) return false;
    const count = LeafNode.cellCount(content);
    const free_start = readU16(content, 2);
    const free_end = readU16(content, 4);
    if (free_start < LeafNode.header_size) return false;
    if (free_start > content_size) return false;
    if (free_end > content_size) return false;
    if (free_start > free_end) return false;

    const ptr_bytes = free_start - LeafNode.header_size;
    if (ptr_bytes % LeafNode.cell_ptr_size != 0) return false;
    if (count != ptr_bytes / LeafNode.cell_ptr_size) return false;

    var i: usize = 0;
    while (i < @as(usize, count)) : (i += 1) {
        const ptr_pos = LeafNode.header_size + i * LeafNode.cell_ptr_size;
        if (ptr_pos + LeafNode.cell_ptr_size > free_start) return false;

        const cell_off = readU16(content, ptr_pos);
        const cell_off_usize: usize = cell_off;
        if (cell_off < free_end) return false;
        if (cell_off_usize > content_size) return false;
        if (cell_off_usize + 2 > content_size) return false;

        const key_len = readU16(content, cell_off_usize);
        const cell_end = addMany(&[_]usize{ cell_off_usize, 2, key_len, 8, 2 }) orelse
            return false;
        if (cell_end > content_size) return false;
    }
    return true;
}

fn validateInternalStructure(content: *const [content_size]u8) bool {
    if (readU16(content, 14) != InternalNode.format_magic) return false;
    if (content[16] != InternalNode.format_version) return false;
    const count = InternalNode.cellCount(content);
    const free_start = readU16(content, 2);
    const free_end = readU16(content, 4);
    if (free_start < InternalNode.header_size) return false;
    if (free_start > content_size) return false;
    if (free_end > content_size) return false;
    if (free_start > free_end) return false;

    const ptr_bytes = free_start - InternalNode.header_size;
    if (ptr_bytes % InternalNode.cell_ptr_size != 0) return false;
    if (count != ptr_bytes / InternalNode.cell_ptr_size) return false;

    var i: usize = 0;
    while (i < @as(usize, count)) : (i += 1) {
        const ptr_pos = InternalNode.header_size + i * InternalNode.cell_ptr_size;
        if (ptr_pos + InternalNode.cell_ptr_size > free_start) return false;

        const cell_off = readU16(content, ptr_pos);
        const cell_off_usize: usize = cell_off;
        if (cell_off < free_end) return false;
        if (cell_off_usize > content_size) return false;
        if (cell_off_usize + 2 > content_size) return false;

        const key_len = readU16(content, cell_off_usize);
        const cell_end = addMany(&[_]usize{ cell_off_usize, 2, key_len, 8 }) orelse
            return false;
        if (cell_end > content_size) return false;
    }
    return true;
}

fn addMany(parts: []const usize) ?usize {
    var total: usize = 0;
    for (parts) |part| {
        total = std.math.add(usize, total, part) catch return null;
    }
    return total;
}

// ============================================================================
// Map buffer pool errors to BTreeError
// ============================================================================

fn mapPoolError(err: buffer_pool_mod.BufferPoolError) BTreeError {
    return switch (err) {
        error.AllFramesPinned => error.AllFramesPinned,
        error.ChecksumMismatch => error.ChecksumMismatch,
        error.StorageRead => error.StorageRead,
        error.StorageWrite => error.StorageWrite,
        error.StorageFsync => error.StorageFsync,
        error.WalNotFlushed => error.WalNotFlushed,
    };
}

fn mapWalAppendError(err: wal_mod.WalError) BTreeError {
    return switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        error.PayloadTooLarge => error.WalWriteError,
        error.WalReadError => error.WalWriteError,
        error.WalWriteError => error.WalWriteError,
        error.WalFsyncError => error.WalFsyncError,
        error.InvalidEnvelope => error.WalWriteError,
        error.CorruptEnvelope => error.WalWriteError,
        error.UnsupportedEnvelopeVersion => error.WalWriteError,
    };
}

// ============================================================================
// Tests
// ============================================================================

const testing = std.testing;

// --- Leaf Node Tests ---

test "leaf: init creates empty leaf page" {
    var page = Page.init(0, .free);
    LeafNode.init(&page);

    try testing.expectEqual(page_mod.PageType.btree_leaf, page.header.page_type);
    try testing.expectEqual(@as(u16, 0), LeafNode.cellCount(&page.content));
    try testing.expectEqual(
        @as(u16, content_size - LeafNode.header_size),
        LeafNode.freeSpace(&page.content),
    );
    try testing.expectEqual(LeafNode.no_sibling, LeafNode.rightSibling(&page.content));
}

test "leaf: insert and read single entry" {
    var page = Page.init(0, .free);
    LeafNode.init(&page);

    const rid = RowId{ .page_id = 5, .slot = 3 };
    try LeafNode.insert(&page.content, "hello", rid);

    try testing.expectEqual(@as(u16, 1), LeafNode.cellCount(&page.content));
    try testing.expectEqualSlices(u8, "hello", LeafNode.getKey(&page.content, 0));
    const got = LeafNode.getRowId(&page.content, 0);
    try testing.expectEqual(@as(u64, 5), got.page_id);
    try testing.expectEqual(@as(u16, 3), got.slot);
}

test "leaf: insert maintains sorted order" {
    var page = Page.init(0, .free);
    LeafNode.init(&page);

    try LeafNode.insert(&page.content, "cherry", RowId{ .page_id = 1, .slot = 0 });
    try LeafNode.insert(&page.content, "apple", RowId{ .page_id = 2, .slot = 0 });
    try LeafNode.insert(&page.content, "banana", RowId{ .page_id = 3, .slot = 0 });

    try testing.expectEqual(@as(u16, 3), LeafNode.cellCount(&page.content));
    try testing.expectEqualSlices(u8, "apple", LeafNode.getKey(&page.content, 0));
    try testing.expectEqualSlices(u8, "banana", LeafNode.getKey(&page.content, 1));
    try testing.expectEqualSlices(u8, "cherry", LeafNode.getKey(&page.content, 2));
}

test "leaf: binary search finds existing keys" {
    var page = Page.init(0, .free);
    LeafNode.init(&page);

    try LeafNode.insert(&page.content, "a", RowId{ .page_id = 0, .slot = 0 });
    try LeafNode.insert(&page.content, "c", RowId{ .page_id = 0, .slot = 1 });
    try LeafNode.insert(&page.content, "e", RowId{ .page_id = 0, .slot = 2 });

    const r1 = LeafNode.search(&page.content, "a");
    try testing.expect(r1.found);
    try testing.expectEqual(@as(u16, 0), r1.index);

    const r2 = LeafNode.search(&page.content, "c");
    try testing.expect(r2.found);
    try testing.expectEqual(@as(u16, 1), r2.index);

    const r3 = LeafNode.search(&page.content, "e");
    try testing.expect(r3.found);
    try testing.expectEqual(@as(u16, 2), r3.index);
}

test "leaf: binary search returns insert position for missing keys" {
    var page = Page.init(0, .free);
    LeafNode.init(&page);

    try LeafNode.insert(&page.content, "b", RowId{ .page_id = 0, .slot = 0 });
    try LeafNode.insert(&page.content, "d", RowId{ .page_id = 0, .slot = 1 });

    const r1 = LeafNode.search(&page.content, "a");
    try testing.expect(!r1.found);
    try testing.expectEqual(@as(u16, 0), r1.index); // before "b"

    const r2 = LeafNode.search(&page.content, "c");
    try testing.expect(!r2.found);
    try testing.expectEqual(@as(u16, 1), r2.index); // between "b" and "d"

    const r3 = LeafNode.search(&page.content, "z");
    try testing.expect(!r3.found);
    try testing.expectEqual(@as(u16, 2), r3.index); // after "d"
}

test "leaf: duplicate key returns error" {
    var page = Page.init(0, .free);
    LeafNode.init(&page);

    try LeafNode.insert(&page.content, "key", RowId{ .page_id = 0, .slot = 0 });
    const result = LeafNode.insert(&page.content, "key", RowId{ .page_id = 1, .slot = 1 });
    try testing.expectError(error.DuplicateKey, result);
}

test "leaf: page full returns error" {
    var page = Page.init(0, .free);
    LeafNode.init(&page);

    // Fill the leaf with 8-byte keys. Each cell: ptr(2) + key_len(2) + key(8) + page_id(8) + slot(2) = 22 bytes.
    // Available: 8168 - 14 = 8154 bytes. 8154 / 22 = 370 entries.
    var buf: [8]u8 = undefined;
    var count: u32 = 0;
    while (count < 500) : (count += 1) {
        // Generate unique sorted key.
        const key_val = std.mem.nativeToLittle(u64, @as(u64, count) * 10);
        @memcpy(&buf, std.mem.asBytes(&key_val));
        LeafNode.insert(&page.content, &buf, RowId{ .page_id = 0, .slot = @intCast(count) }) catch |err| {
            try testing.expectEqual(BTreeError.PageFull, err);
            break;
        };
    }
    try testing.expect(count > 100); // Should fit many entries.
}

test "leaf: delete removes entry" {
    var page = Page.init(0, .free);
    LeafNode.init(&page);

    try LeafNode.insert(&page.content, "a", RowId{ .page_id = 0, .slot = 0 });
    try LeafNode.insert(&page.content, "b", RowId{ .page_id = 0, .slot = 1 });
    try LeafNode.insert(&page.content, "c", RowId{ .page_id = 0, .slot = 2 });

    try LeafNode.delete(&page.content, "b");

    try testing.expectEqual(@as(u16, 2), LeafNode.cellCount(&page.content));
    try testing.expectEqualSlices(u8, "a", LeafNode.getKey(&page.content, 0));
    try testing.expectEqualSlices(u8, "c", LeafNode.getKey(&page.content, 1));

    const result = LeafNode.search(&page.content, "b");
    try testing.expect(!result.found);
}

test "leaf: delete non-existent key returns error" {
    var page = Page.init(0, .free);
    LeafNode.init(&page);

    try LeafNode.insert(&page.content, "a", RowId{ .page_id = 0, .slot = 0 });
    const result = LeafNode.delete(&page.content, "z");
    try testing.expectError(error.KeyNotFound, result);
}

test "leaf: sibling pointer roundtrip" {
    var page = Page.init(0, .free);
    LeafNode.init(&page);

    try testing.expectEqual(LeafNode.no_sibling, LeafNode.rightSibling(&page.content));
    LeafNode.setRightSibling(&page.content, 42);
    try testing.expectEqual(@as(u64, 42), LeafNode.rightSibling(&page.content));
}

// --- Internal Node Tests ---

test "internal: init creates empty internal page" {
    var page = Page.init(0, .free);
    InternalNode.init(&page);

    try testing.expectEqual(page_mod.PageType.btree_internal, page.header.page_type);
    try testing.expectEqual(@as(u16, 0), InternalNode.cellCount(&page.content));
    try testing.expectEqual(
        @as(u16, content_size - InternalNode.header_size),
        InternalNode.freeSpace(&page.content),
    );
}

test "internal: insert and read" {
    var page = Page.init(0, .free);
    InternalNode.init(&page);
    InternalNode.setLeftChild(&page.content, 100);

    try InternalNode.insert(&page.content, "mid", 200);

    try testing.expectEqual(@as(u16, 1), InternalNode.cellCount(&page.content));
    try testing.expectEqualSlices(u8, "mid", InternalNode.getKey(&page.content, 0));
    try testing.expectEqual(@as(u64, 200), InternalNode.getRightChild(&page.content, 0));
    try testing.expectEqual(@as(u64, 100), InternalNode.leftChild(&page.content));
}

test "internal: child routing" {
    var page = Page.init(0, .free);
    InternalNode.init(&page);
    InternalNode.setLeftChild(&page.content, 10);

    try InternalNode.insert(&page.content, "m", 20);
    try InternalNode.insert(&page.content, "t", 30);

    // key < "m" -> left child (10)
    try testing.expectEqual(@as(u64, 10), InternalNode.findChild(&page.content, "a"));
    // "m" <= key < "t" -> right child of "m" (20)
    try testing.expectEqual(@as(u64, 20), InternalNode.findChild(&page.content, "m"));
    try testing.expectEqual(@as(u64, 20), InternalNode.findChild(&page.content, "p"));
    // key >= "t" -> right child of "t" (30)
    try testing.expectEqual(@as(u64, 30), InternalNode.findChild(&page.content, "t"));
    try testing.expectEqual(@as(u64, 30), InternalNode.findChild(&page.content, "z"));
}

test "internal: page full returns error" {
    var page = Page.init(0, .free);
    InternalNode.init(&page);
    InternalNode.setLeftChild(&page.content, 0);

    var buf: [8]u8 = undefined;
    var count: u32 = 0;
    while (count < 600) : (count += 1) {
        const key_val = std.mem.nativeToLittle(u64, @as(u64, count) * 10);
        @memcpy(&buf, std.mem.asBytes(&key_val));
        InternalNode.insert(&page.content, &buf, count) catch |err| {
            try testing.expectEqual(BTreeError.PageFull, err);
            break;
        };
    }
    try testing.expect(count > 100);
}

// --- BTree Tests ---

test "btree: empty find returns null" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    var pool = try BufferPool.init(testing.allocator, disk.storage(), 16);
    defer pool.deinit();

    var tree = try BTree.init(&pool, null, 0);
    const result = try tree.find("anything");
    try testing.expect(result == null);
}

test "btree: corrupted leaf structure returns Corruption" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    var pool = try BufferPool.init(testing.allocator, disk.storage(), 16);
    defer pool.deinit();

    var tree = try BTree.init(&pool, null, 0);
    const root = try pool.pin(tree.root_page_id);
    // Break invariant: free_start must be >= leaf header size.
    writeU16(&root.content, 2, 0);
    pool.unpin(tree.root_page_id, true);

    const result = tree.find("anything");
    try testing.expectError(error.Corruption, result);
}

test "btree: invalid leaf format version returns Corruption" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    var pool = try BufferPool.init(testing.allocator, disk.storage(), 16);
    defer pool.deinit();

    var tree = try BTree.init(&pool, null, 0);
    const root = try pool.pin(tree.root_page_id);
    root.content[16] = LeafNode.format_version + 1;
    pool.unpin(tree.root_page_id, true);

    try testing.expectError(error.Corruption, tree.find("anything"));
}

test "btree: corrupted leaf cell pointer returns Corruption" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    var pool = try BufferPool.init(testing.allocator, disk.storage(), 16);
    defer pool.deinit();

    var tree = try BTree.init(&pool, null, 0);
    try tree.insert("k", RowId{ .page_id = 1, .slot = 1 });

    const root = try pool.pin(tree.root_page_id);
    const free_end = readU16(&root.content, 4);
    writeU16(&root.content, LeafNode.header_size, free_end - 1);
    pool.unpin(tree.root_page_id, true);

    try testing.expectError(error.Corruption, tree.find("k"));
}

test "btree: corrupted internal key length returns Corruption" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    var pool = try BufferPool.init(testing.allocator, disk.storage(), 16);
    defer pool.deinit();

    var tree = try BTree.init(&pool, null, 0);
    const root = try pool.pin(tree.root_page_id);
    InternalNode.init(root);
    InternalNode.setLeftChild(&root.content, 1);
    try InternalNode.insert(&root.content, "m", 1);
    const cell_off = readU16(&root.content, InternalNode.header_size);
    writeU16(&root.content, cell_off, content_size);
    pool.unpin(tree.root_page_id, true);

    const child = try pool.pin(1);
    LeafNode.init(child);
    pool.unpin(1, true);

    try testing.expectError(error.Corruption, tree.find("z"));
}

test "btree: invalid internal format magic returns Corruption" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    var pool = try BufferPool.init(testing.allocator, disk.storage(), 16);
    defer pool.deinit();

    var tree = try BTree.init(&pool, null, 0);
    const root = try pool.pin(tree.root_page_id);
    InternalNode.init(root);
    InternalNode.setLeftChild(&root.content, 1);
    root.content[14] = 0;
    root.content[15] = 0;
    pool.unpin(tree.root_page_id, true);

    const child = try pool.pin(1);
    LeafNode.init(child);
    pool.unpin(1, true);

    try testing.expectError(error.Corruption, tree.find("anything"));
}

test "btree: traversal depth guard returns Corruption" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    var pool = try BufferPool.init(testing.allocator, disk.storage(), 32);
    defer pool.deinit();

    var tree = try BTree.init(&pool, null, 0);

    var page_id: u64 = tree.root_page_id;
    var level: usize = 0;
    while (level <= max_btree_depth) : (level += 1) {
        const internal = try pool.pin(page_id);
        InternalNode.init(internal);
        InternalNode.setLeftChild(&internal.content, page_id + 1);
        pool.unpin(page_id, true);
        page_id += 1;
    }

    const leaf = try pool.pin(page_id);
    LeafNode.init(leaf);
    pool.unpin(page_id, true);

    try testing.expectError(error.Corruption, tree.find("anything"));
}

test "btree: range scan sibling cycle returns Corruption" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    var pool = try BufferPool.init(testing.allocator, disk.storage(), 16);
    defer pool.deinit();

    var tree = try BTree.init(&pool, null, 0);

    const leaf0 = try pool.pin(tree.root_page_id);
    LeafNode.init(leaf0);
    LeafNode.setRightSibling(&leaf0.content, 1);
    pool.unpin(tree.root_page_id, true);

    const leaf1 = try pool.pin(1);
    LeafNode.init(leaf1);
    LeafNode.setRightSibling(&leaf1.content, tree.root_page_id);
    pool.unpin(1, true);

    var iter = try tree.rangeScan(null, null);
    defer iter.close();
    try testing.expectError(error.Corruption, iter.next());
}

test "btree: single insert and find" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    var pool = try BufferPool.init(testing.allocator, disk.storage(), 16);
    defer pool.deinit();

    var tree = try BTree.init(&pool, null, 0);

    const rid = RowId{ .page_id = 10, .slot = 5 };
    try tree.insert("mykey", rid);

    const found = try tree.find("mykey");
    try testing.expect(found != null);
    try testing.expectEqual(@as(u64, 10), found.?.page_id);
    try testing.expectEqual(@as(u16, 5), found.?.slot);
}

test "btree: multiple inserts and finds" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    var pool = try BufferPool.init(testing.allocator, disk.storage(), 16);
    defer pool.deinit();

    var tree = try BTree.init(&pool, null, 0);

    try tree.insert("cherry", RowId{ .page_id = 1, .slot = 0 });
    try tree.insert("apple", RowId{ .page_id = 2, .slot = 0 });
    try tree.insert("banana", RowId{ .page_id = 3, .slot = 0 });

    const r1 = try tree.find("apple");
    try testing.expectEqual(@as(u64, 2), r1.?.page_id);
    const r2 = try tree.find("banana");
    try testing.expectEqual(@as(u64, 3), r2.?.page_id);
    const r3 = try tree.find("cherry");
    try testing.expectEqual(@as(u64, 1), r3.?.page_id);
    const r4 = try tree.find("date");
    try testing.expect(r4 == null);
}

test "btree: duplicate key returns error" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    var pool = try BufferPool.init(testing.allocator, disk.storage(), 16);
    defer pool.deinit();

    var tree = try BTree.init(&pool, null, 0);

    try tree.insert("key", RowId{ .page_id = 0, .slot = 0 });
    const result = tree.insert("key", RowId{ .page_id = 1, .slot = 1 });
    try testing.expectError(error.DuplicateKey, result);
}

// --- Split Tests ---

test "btree: leaf split on insert" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    var pool = try BufferPool.init(testing.allocator, disk.storage(), 32);
    defer pool.deinit();

    var tree = try BTree.init(&pool, null, 0);

    // Insert enough keys to trigger a split. With 8-byte keys, ~370 fit per leaf.
    var buf: [8]u8 = undefined;
    var i: u64 = 0;
    while (i < 400) : (i += 1) {
        // Use big-endian so lexicographic order matches numeric order.
        const key_val = std.mem.nativeToBig(u64, i);
        @memcpy(&buf, std.mem.asBytes(&key_val));
        try tree.insert(&buf, RowId{ .page_id = i, .slot = 0 });
    }

    // Verify all keys are findable.
    i = 0;
    while (i < 400) : (i += 1) {
        const key_val = std.mem.nativeToBig(u64, i);
        @memcpy(&buf, std.mem.asBytes(&key_val));
        const found = try tree.find(&buf);
        try testing.expect(found != null);
        try testing.expectEqual(i, found.?.page_id);
    }
}

test "btree: multi-level splits with sequential inserts" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    // Need enough buffer pool frames for the tree pages.
    var pool = try BufferPool.init(testing.allocator, disk.storage(), 64);
    defer pool.deinit();

    var tree = try BTree.init(&pool, null, 0);

    const n: u64 = 2000;
    var buf: [8]u8 = undefined;
    var i: u64 = 0;
    while (i < n) : (i += 1) {
        const key_val = std.mem.nativeToBig(u64, i);
        @memcpy(&buf, std.mem.asBytes(&key_val));
        try tree.insert(&buf, RowId{ .page_id = i, .slot = 0 });
    }

    // Verify all keys.
    i = 0;
    while (i < n) : (i += 1) {
        const key_val = std.mem.nativeToBig(u64, i);
        @memcpy(&buf, std.mem.asBytes(&key_val));
        const found = try tree.find(&buf);
        try testing.expect(found != null);
        try testing.expectEqual(i, found.?.page_id);
    }
}

test "btree: reverse order inserts" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    var pool = try BufferPool.init(testing.allocator, disk.storage(), 64);
    defer pool.deinit();

    var tree = try BTree.init(&pool, null, 0);

    const n: u64 = 1000;
    var buf: [8]u8 = undefined;

    // Insert in reverse.
    var i: u64 = n;
    while (i > 0) {
        i -= 1;
        const key_val = std.mem.nativeToBig(u64, i);
        @memcpy(&buf, std.mem.asBytes(&key_val));
        try tree.insert(&buf, RowId{ .page_id = i, .slot = 0 });
    }

    // Verify all keys.
    i = 0;
    while (i < n) : (i += 1) {
        const key_val = std.mem.nativeToBig(u64, i);
        @memcpy(&buf, std.mem.asBytes(&key_val));
        const found = try tree.find(&buf);
        try testing.expect(found != null);
        try testing.expectEqual(i, found.?.page_id);
    }
}

// --- Delete Tests ---

test "btree: insert then delete" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    var pool = try BufferPool.init(testing.allocator, disk.storage(), 16);
    defer pool.deinit();

    var tree = try BTree.init(&pool, null, 0);

    try tree.insert("key1", RowId{ .page_id = 1, .slot = 0 });
    try tree.insert("key2", RowId{ .page_id = 2, .slot = 0 });

    try tree.delete("key1");
    try testing.expect((try tree.find("key1")) == null);
    try testing.expect((try tree.find("key2")) != null);
}

test "btree: delete non-existent key returns error" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    var pool = try BufferPool.init(testing.allocator, disk.storage(), 16);
    defer pool.deinit();

    var tree = try BTree.init(&pool, null, 0);

    const result = tree.delete("nope");
    try testing.expectError(error.KeyNotFound, result);
}

test "btree: insert delete reinsert" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    var pool = try BufferPool.init(testing.allocator, disk.storage(), 16);
    defer pool.deinit();

    var tree = try BTree.init(&pool, null, 0);

    try tree.insert("key", RowId{ .page_id = 1, .slot = 0 });
    try tree.delete("key");
    try tree.insert("key", RowId{ .page_id = 2, .slot = 1 });

    const found = try tree.find("key");
    try testing.expect(found != null);
    try testing.expectEqual(@as(u64, 2), found.?.page_id);
    try testing.expectEqual(@as(u16, 1), found.?.slot);
}

// --- Range Scan Tests ---

test "btree: full range scan" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    var pool = try BufferPool.init(testing.allocator, disk.storage(), 16);
    defer pool.deinit();

    var tree = try BTree.init(&pool, null, 0);

    try tree.insert("a", RowId{ .page_id = 1, .slot = 0 });
    try tree.insert("b", RowId{ .page_id = 2, .slot = 0 });
    try tree.insert("c", RowId{ .page_id = 3, .slot = 0 });

    var iter = try tree.rangeScan(null, null);
    defer iter.close();

    const e1 = (try iter.next()).?;
    try testing.expectEqualSlices(u8, "a", e1.key);
    const e2 = (try iter.next()).?;
    try testing.expectEqualSlices(u8, "b", e2.key);
    const e3 = (try iter.next()).?;
    try testing.expectEqualSlices(u8, "c", e3.key);
    try testing.expect((try iter.next()) == null);
}

test "btree: bounded range scan" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    var pool = try BufferPool.init(testing.allocator, disk.storage(), 16);
    defer pool.deinit();

    var tree = try BTree.init(&pool, null, 0);

    try tree.insert("a", RowId{ .page_id = 1, .slot = 0 });
    try tree.insert("b", RowId{ .page_id = 2, .slot = 0 });
    try tree.insert("c", RowId{ .page_id = 3, .slot = 0 });
    try tree.insert("d", RowId{ .page_id = 4, .slot = 0 });

    var iter = try tree.rangeScan("b", "d"); // [b, d)
    defer iter.close();

    const e1 = (try iter.next()).?;
    try testing.expectEqualSlices(u8, "b", e1.key);
    const e2 = (try iter.next()).?;
    try testing.expectEqualSlices(u8, "c", e2.key);
    try testing.expect((try iter.next()) == null);
}

test "btree: empty range scan" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    var pool = try BufferPool.init(testing.allocator, disk.storage(), 16);
    defer pool.deinit();

    var tree = try BTree.init(&pool, null, 0);

    try tree.insert("a", RowId{ .page_id = 1, .slot = 0 });
    try tree.insert("c", RowId{ .page_id = 3, .slot = 0 });

    var iter = try tree.rangeScan("d", "z"); // no keys in [d, z)
    defer iter.close();

    try testing.expect((try iter.next()) == null);
}

test "btree: range scan across split pages" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    var pool = try BufferPool.init(testing.allocator, disk.storage(), 64);
    defer pool.deinit();

    var tree = try BTree.init(&pool, null, 0);

    // Insert enough to cause splits.
    const n: u64 = 500;
    var buf: [8]u8 = undefined;
    var i: u64 = 0;
    while (i < n) : (i += 1) {
        const key_val = std.mem.nativeToBig(u64, i);
        @memcpy(&buf, std.mem.asBytes(&key_val));
        try tree.insert(&buf, RowId{ .page_id = i, .slot = 0 });
    }

    // Full scan should return all entries in order.
    var iter = try tree.rangeScan(null, null);
    defer iter.close();

    var count: u64 = 0;
    var prev_key: ?[8]u8 = null;
    while (try iter.next()) |entry| {
        if (prev_key) |pk| {
            // Verify sorted order.
            try testing.expect(std.mem.order(u8, &pk, entry.key) == .lt);
        }
        var cur: [8]u8 = undefined;
        @memcpy(&cur, entry.key[0..8]);
        prev_key = cur;
        count += 1;
    }
    try testing.expectEqual(n, count);
}

test "btree: open-ended range scans" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    var pool = try BufferPool.init(testing.allocator, disk.storage(), 16);
    defer pool.deinit();

    var tree = try BTree.init(&pool, null, 0);

    try tree.insert("a", RowId{ .page_id = 1, .slot = 0 });
    try tree.insert("b", RowId{ .page_id = 2, .slot = 0 });
    try tree.insert("c", RowId{ .page_id = 3, .slot = 0 });

    // lo=null, hi="b" => just "a"
    {
        var iter = try tree.rangeScan(null, "b");
        defer iter.close();
        const e1 = (try iter.next()).?;
        try testing.expectEqualSlices(u8, "a", e1.key);
        try testing.expect((try iter.next()) == null);
    }

    // lo="b", hi=null => "b", "c"
    {
        var iter = try tree.rangeScan("b", null);
        defer iter.close();
        const e1 = (try iter.next()).?;
        try testing.expectEqualSlices(u8, "b", e1.key);
        const e2 = (try iter.next()).?;
        try testing.expectEqualSlices(u8, "c", e2.key);
        try testing.expect((try iter.next()) == null);
    }
}

// --- WAL Integration Tests ---

test "btree: WAL records logged on insert" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    var wal = Wal.init(testing.allocator, disk.storage());
    defer wal.deinit();

    var pool = try BufferPool.init(testing.allocator, disk.storage(), 16);
    defer pool.deinit();

    var tree = try BTree.init(&pool, &wal, 0);

    try tree.insert("key1", RowId{ .page_id = 1, .slot = 0 });
    try tree.insert("key2", RowId{ .page_id = 2, .slot = 0 });

    try testing.expect(wal.records_written >= 2);
}

test "btree: page LSN updated on insert" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    var wal = Wal.init(testing.allocator, disk.storage());
    defer wal.deinit();

    var pool = try BufferPool.init(testing.allocator, disk.storage(), 16);
    defer pool.deinit();

    var tree = try BTree.init(&pool, &wal, 0);

    try tree.insert("key1", RowId{ .page_id = 1, .slot = 0 });

    // Pin root page and check LSN > 0.
    const page = try pool.pin(tree.root_page_id);
    try testing.expect(page.header.lsn > 0);
    pool.unpin(tree.root_page_id, false);
}

// --- Buffer Pool Integration Test ---

test "btree: survives buffer pool eviction and reload" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    // Buffer pool smaller than the number of pages the tree will use.
    // Splits pin at most 3 pages at once (old leaf, new leaf, parent), so
    // we need at least that many. Use 8 frames — tree will allocate more
    // pages than that, forcing evictions.
    var pool = try BufferPool.init(testing.allocator, disk.storage(), 8);
    defer pool.deinit();

    var tree = try BTree.init(&pool, null, 0);

    // Insert enough keys to use many pages (triggering evictions).
    var buf: [8]u8 = undefined;
    const n: u64 = 2000;
    var i: u64 = 0;
    while (i < n) : (i += 1) {
        const key_val = std.mem.nativeToBig(u64, i);
        @memcpy(&buf, std.mem.asBytes(&key_val));
        try tree.insert(&buf, RowId{ .page_id = i, .slot = 0 });
    }

    // Flush everything to disk.
    try pool.flushAll();

    // Verify evictions happened.
    try testing.expect(pool.evictions > 0);

    // Verify all keys are still findable (pages must be reloaded from disk).
    i = 0;
    while (i < n) : (i += 1) {
        const key_val = std.mem.nativeToBig(u64, i);
        @memcpy(&buf, std.mem.asBytes(&key_val));
        const found = try tree.find(&buf);
        try testing.expect(found != null);
        try testing.expectEqual(i, found.?.page_id);
    }
}

// --- Bug regression tests (TDD: written before fixes) ---

test "btree: range scan keeps current leaf pinned" {
    // The key returned by next() is a slice into the buffer pool frame.
    // If the page is unpinned before returning, eviction could corrupt it.
    // The iterator must keep the current leaf pinned between next() calls.
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    var pool = try BufferPool.init(testing.allocator, disk.storage(), 16);
    defer pool.deinit();

    var tree = try BTree.init(&pool, null, 0);

    try tree.insert("a", RowId{ .page_id = 1, .slot = 0 });
    try tree.insert("b", RowId{ .page_id = 2, .slot = 0 });

    var iter = try tree.rangeScan(null, null);
    defer iter.close();

    const entry = (try iter.next()).?;
    try testing.expectEqualSlices(u8, "a", entry.key);

    // The leaf page holding "a" must still be pinned.
    try testing.expect(pool.isPinned(iter.current_page_id));
}

test "btree: range scan close unpins page" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    var pool = try BufferPool.init(testing.allocator, disk.storage(), 16);
    defer pool.deinit();

    var tree = try BTree.init(&pool, null, 0);

    try tree.insert("a", RowId{ .page_id = 1, .slot = 0 });

    var iter = try tree.rangeScan(null, null);
    _ = try iter.next();
    const page_id = iter.current_page_id;

    iter.close();
    // After close, the page should no longer be pinned by the iterator.
    try testing.expect(!pool.isPinned(page_id));
}

test "btree: no WAL record on failed delete" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    var wal = Wal.init(testing.allocator, disk.storage());
    defer wal.deinit();

    var pool = try BufferPool.init(testing.allocator, disk.storage(), 16);
    defer pool.deinit();

    var tree = try BTree.init(&pool, &wal, 0);

    const before = wal.records_written;
    const result = tree.delete("nonexistent");
    try testing.expectError(error.KeyNotFound, result);
    // No WAL record should have been written for a failed delete.
    try testing.expectEqual(before, wal.records_written);
}

test "btree: no spurious WAL record on split" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    var wal = Wal.init(testing.allocator, disk.storage());
    defer wal.deinit();

    var pool = try BufferPool.init(testing.allocator, disk.storage(), 32);
    defer pool.deinit();

    var tree = try BTree.init(&pool, &wal, 0);

    // Fill a leaf to capacity. Each insert produces exactly one btree_insert WAL record.
    var buf: [8]u8 = undefined;
    var i: u64 = 0;
    while (i < 370) : (i += 1) {
        const key_val = std.mem.nativeToBig(u64, i);
        @memcpy(&buf, std.mem.asBytes(&key_val));
        try tree.insert(&buf, RowId{ .page_id = i, .slot = 0 });
    }
    try testing.expectEqual(@as(u64, 370), wal.records_written);

    // This insert triggers a split. It should NOT produce a spurious
    // btree_insert WAL record before the split records.
    const before_split = wal.records_written;
    const split_key = std.mem.nativeToBig(u64, @as(u64, 370));
    @memcpy(&buf, std.mem.asBytes(&split_key));
    try tree.insert(&buf, RowId{ .page_id = 370, .slot = 0 });

    // Split produces: btree_split_leaf + btree_new_root = 2 records.
    // A spurious btree_insert before the split would make it 3.
    const split_records = wal.records_written - before_split;
    try testing.expectEqual(@as(u64, 2), split_records);
}

test "btree: sentinel no_sibling is not a valid page id" {
    // The no_sibling sentinel must not collide with any page_id the tree could use.
    // Page 0 is the initial root, so no_sibling must not be 0.
    try testing.expect(LeafNode.no_sibling != 0);
}
