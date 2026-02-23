//! B+ tree split and parent promotion logic.
//!
//! Extracted from btree.zig — handles leaf splits, internal node splits,
//! root splits, and key redistribution during overflow.
const std = @import("std");
const page_mod = @import("page.zig");
const heap_mod = @import("heap.zig");
const btree = @import("btree.zig");
const btree_page = @import("btree_page.zig");

const content_size = page_mod.content_size;
const RowId = heap_mod.RowId;
const BTreeError = btree.BTreeError;
const BTree = btree.BTree;
const PathEntry = btree.PathEntry;
const validateLeafStructure = btree.validateLeafStructure;
const validateInternalStructure = btree.validateInternalStructure;
const mapPoolError = btree.mapPoolError;
const mapWalAppendError = btree.mapWalAppendError;

const LeafNode = btree_page.LeafNode;
const InternalNode = btree_page.InternalNode;

/// Fixed split scratch capacities derived from page-size bounds.
const max_leaf_split_entries = (content_size - LeafNode.header_size) / 14 + 1;
const max_internal_split_entries = (content_size - InternalNode.header_size) / 12 + 1;

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

const SplitKeyPlan = struct {
    insert_pos: usize,
    total_key_bytes: usize,
};

fn planSplitKeyMerge(
    comptime ContentPtr: type,
    content: ContentPtr,
    old_count: u16,
    new_key: []const u8,
    comptime getKeyFn: fn (ContentPtr, u16) []const u8,
) SplitKeyPlan {
    var insert_pos: usize = @as(usize, old_count);
    var total_key_bytes: usize = new_key.len;
    for (0..old_count) |idx_usize| {
        const idx: u16 = @intCast(idx_usize);
        const existing_key = getKeyFn(content, idx);
        total_key_bytes += existing_key.len;
        if (insert_pos == @as(usize, old_count) and std.mem.order(u8, new_key, existing_key) == .lt) {
            insert_pos = idx_usize;
        }
    }
    return .{
        .insert_pos = insert_pos,
        .total_key_bytes = total_key_bytes,
    };
}

fn copyOwnedExistingKeys(
    comptime ContentPtr: type,
    content: ContentPtr,
    old_count: u16,
    key_buf: *[content_size]u8,
    owned_keys: [][]const u8,
    comptime getKeyFn: fn (ContentPtr, u16) []const u8,
) usize {
    std.debug.assert(owned_keys.len >= old_count);

    var buf_offset: usize = 0;
    for (0..old_count) |idx_usize| {
        const idx: u16 = @intCast(idx_usize);
        const existing_key = getKeyFn(content, idx);
        const owned = key_buf[buf_offset..][0..existing_key.len];
        @memcpy(owned, existing_key);
        owned_keys[idx_usize] = owned;
        buf_offset += existing_key.len;
    }
    return buf_offset;
}

// ============================================================================
// Split operations
// ============================================================================

pub fn splitAndInsert(self: *BTree, leaf_id: u64, key: []const u8, row_id: RowId, path: []const PathEntry) BTreeError!u64 {
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

    const key_plan = planSplitKeyMerge(
        *const [content_size]u8,
        &leaf_page.content,
        old_count,
        key,
        LeafNode.getKey,
    );

    // Allocate entries array and key data buffer.
    if (total > max_leaf_split_entries) return error.Corruption;
    if (key_plan.total_key_bytes > content_size) return error.Corruption;
    var entries: [max_leaf_split_entries]TempLeafEntry = undefined;
    var key_buf: [content_size]u8 = undefined;
    var owned_old_keys: [max_leaf_split_entries - 1][]const u8 = undefined;
    const buf_offset = copyOwnedExistingKeys(
        *const [content_size]u8,
        &leaf_page.content,
        old_count,
        &key_buf,
        owned_old_keys[0..old_count],
        LeafNode.getKey,
    );
    const owned_new_key = key_buf[buf_offset..][0..key.len];
    @memcpy(owned_new_key, key);

    for (0..total) |entry_idx| {
        if (entry_idx == key_plan.insert_pos) {
            entries[entry_idx] = .{ .key = owned_new_key, .row_id = row_id };
            continue;
        }
        const old_idx: usize = if (entry_idx < key_plan.insert_pos) entry_idx else entry_idx - 1;
        entries[entry_idx] = .{
            .key = owned_old_keys[old_idx],
            .row_id = LeafNode.getRowId(&leaf_page.content, @intCast(old_idx)),
        };
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
    errdefer {
        self.pool.unpin(leaf_id, false);
        self.pool.unpin(right_id, false);
    }

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
        LeafNode.insert(&left_page.content, entries[i].key, entries[i].row_id) catch |err| {
            std.log.err("btree leaf split redistribution failed on left page: leaf_id={d} right_id={d} insert_idx={d} err={s}", .{ leaf_id, right_id, i, @errorName(err) });
            return error.Corruption;
        };
    }

    // Fill right page with [mid..total).
    for (mid..total) |i| {
        LeafNode.insert(&right_page.content, entries[i].key, entries[i].row_id) catch |err| {
            std.log.err("btree leaf split redistribution failed on right page: leaf_id={d} right_id={d} insert_idx={d} err={s}", .{ leaf_id, right_id, i, @errorName(err) });
            return error.Corruption;
        };
    }

    // Set sibling pointers: left -> right -> old_right_sibling.
    LeafNode.setRightSibling(&left_page.content, right_id);
    LeafNode.setRightSibling(&right_page.content, old_right_sibling);

    self.pool.unpin(leaf_id, true);
    self.pool.unpin(right_id, true);

    const inserted_leaf_id = if (key_plan.insert_pos < mid) leaf_id else right_id;

    // Promote separator key (first key of right page) to parent.
    // entries[mid].key is already in our owned key_buf, safe to use.
    try insertIntoParent(self, leaf_id, entries[mid].key, right_id, path);
    return inserted_leaf_id;
}

pub fn insertIntoParent(self: *BTree, left_id: u64, key: []const u8, right_id: u64, path: []const PathEntry) BTreeError!void {
    if (path.len == 0) {
        // Splitting the root — create a new root.
        try splitRoot(self, left_id, key, right_id);
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
            try splitInternal(self, parent_id, key, right_id, path[0 .. path.len - 1]);
        },
        else => {
            self.pool.unpin(parent_id, false);
            return err;
        },
    }
}

pub fn splitInternal(self: *BTree, node_id: u64, new_key: []const u8, new_right_child: u64, path: []const PathEntry) BTreeError!void {
    const node_page = self.pool.pin(node_id) catch |e| return mapPoolError(e);
    if (!validateInternalStructure(&node_page.content)) {
        self.pool.unpin(node_id, false);
        return error.Corruption;
    }
    const old_count = InternalNode.cellCount(&node_page.content);
    const old_left_child = InternalNode.leftChild(&node_page.content);

    const total: usize = @as(usize, old_count) + 1;
    if (total > max_internal_split_entries) return error.Corruption;
    const key_plan = planSplitKeyMerge(
        *const [content_size]u8,
        &node_page.content,
        old_count,
        new_key,
        InternalNode.getKey,
    );
    if (key_plan.total_key_bytes > content_size) return error.Corruption;

    var entries: [max_internal_split_entries]TempInternalEntry = undefined;
    var key_buf: [content_size]u8 = undefined;
    var owned_old_keys: [max_internal_split_entries - 1][]const u8 = undefined;
    const buf_offset = copyOwnedExistingKeys(
        *const [content_size]u8,
        &node_page.content,
        old_count,
        &key_buf,
        owned_old_keys[0..old_count],
        InternalNode.getKey,
    );
    const owned_new_key = key_buf[buf_offset..][0..new_key.len];
    @memcpy(owned_new_key, new_key);

    for (0..total) |entry_idx| {
        if (entry_idx == key_plan.insert_pos) {
            entries[entry_idx] = .{ .key = owned_new_key, .right_child = new_right_child };
            continue;
        }
        const old_idx: usize = if (entry_idx < key_plan.insert_pos) entry_idx else entry_idx - 1;
        entries[entry_idx] = .{
            .key = owned_old_keys[old_idx],
            .right_child = InternalNode.getRightChild(&node_page.content, @intCast(old_idx)),
        };
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
    errdefer {
        self.pool.unpin(node_id, false);
        self.pool.unpin(new_right_id, false);
    }

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
        InternalNode.insert(&left_page.content, entries[i].key, entries[i].right_child) catch |err| {
            std.log.err("btree internal split redistribution failed on left page: node_id={d} new_right_id={d} insert_idx={d} err={s}", .{ node_id, new_right_id, i, @errorName(err) });
            return error.Corruption;
        };
    }

    // Promoted key is entries[mid]. Right child's left_child = entries[mid].right_child.
    InternalNode.setLeftChild(&right_page.content, entries[mid].right_child);

    // Fill right with [mid+1..total).
    for (mid + 1..total) |i| {
        InternalNode.insert(&right_page.content, entries[i].key, entries[i].right_child) catch |err| {
            std.log.err("btree internal split redistribution failed on right page: node_id={d} new_right_id={d} insert_idx={d} err={s}", .{ node_id, new_right_id, i, @errorName(err) });
            return error.Corruption;
        };
    }

    self.pool.unpin(node_id, true);
    self.pool.unpin(new_right_id, true);

    // entries[mid].key is in our owned key_buf, safe to use.
    try insertIntoParent(self, node_id, entries[mid].key, new_right_id, path);
}

pub fn splitRoot(self: *BTree, left_id: u64, key: []const u8, right_id: u64) BTreeError!void {
    const new_root_id = self.allocPage();
    const root_page = self.pool.pin(new_root_id) catch |e| return mapPoolError(e);
    errdefer self.pool.unpin(new_root_id, false);
    InternalNode.init(root_page);

    if (self.wal) |wal| {
        const lsn = wal.append(0, .btree_new_root, new_root_id, key) catch |e|
            return mapWalAppendError(e);
        root_page.header.lsn = lsn;
    }

    InternalNode.setLeftChild(&root_page.content, left_id);
    InternalNode.insert(&root_page.content, key, right_id) catch |err| {
        std.log.err("btree root split insertion failed: new_root_id={d} left_id={d} right_id={d} key_len={d} err={s}", .{ new_root_id, left_id, right_id, key.len, @errorName(err) });
        return switch (err) {
            error.PageFull, error.DuplicateKey => error.Corruption,
            else => err,
        };
    };

    self.pool.unpin(new_root_id, true);
    self.root_page_id = new_root_id;
}
