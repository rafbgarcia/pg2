//! B+ tree index maintenance for mutation operations.
//!
//! Responsibilities in this file:
//! - Constructs transient BTree handles from catalog metadata (any index).
//! - Provides insert/delete helpers that encode key values and sync state.
//! - Maps BTreeError to MutationError.
//! - Primary-key wrappers delegate to generic index functions.
//!
//! Why this exists:
//! - Keeps B+ tree plumbing out of mutation.zig's already-large codepath.
//! - Follows the existing submodule pattern (constraints.zig, referential_integrity.zig).
const std = @import("std");
const btree_mod = @import("../storage/btree.zig");
const index_key_mod = @import("../storage/index_key.zig");
const catalog_mod = @import("../catalog/catalog.zig");
const buffer_pool_mod = @import("../storage/buffer_pool.zig");
const wal_mod = @import("../storage/wal.zig");
const row_mod = @import("../storage/row.zig");
const heap_mod = @import("../storage/heap.zig");
const undo_mod = @import("../mvcc/undo.zig");
const tx_mod = @import("../mvcc/transaction.zig");

const BTree = btree_mod.BTree;
const BTreeError = btree_mod.BTreeError;
pub const LeafHint = btree_mod.LeafHint;
const Value = row_mod.Value;
const RowId = heap_mod.RowId;
const Catalog = catalog_mod.Catalog;
const ModelId = catalog_mod.ModelId;
const IndexId = catalog_mod.IndexId;
const BufferPool = buffer_pool_mod.BufferPool;
const Wal = wal_mod.Wal;
const HeapPage = heap_mod.HeapPage;
const UndoLog = undo_mod.UndoLog;
const Snapshot = tx_mod.Snapshot;
const TxManager = tx_mod.TxManager;

const mutation = @import("mutation.zig");
const MutationError = mutation.MutationError;
const max_row_buf_size = mutation.max_row_buf_size;

// ---------------------------------------------------------------------------
// Generic index functions (work with any index by index_id)
// ---------------------------------------------------------------------------

/// Construct a transient BTree from catalog IndexInfo + runtime pool/wal.
/// Returns null if the index's btree_root_page_id is 0 (tree not yet allocated).
pub fn openIndex(
    catalog: *const Catalog,
    pool: *BufferPool,
    wal: *Wal,
    model_id: ModelId,
    index_id: IndexId,
) ?BTree {
    std.debug.assert(model_id < catalog.model_count);
    const model = &catalog.models[model_id];
    std.debug.assert(index_id < model.index_count);
    const idx = &model.indexes[index_id];
    if (idx.btree_root_page_id == 0) return null;
    return BTree{
        .root_page_id = @as(u64, idx.btree_root_page_id),
        .next_page_id = @as(u64, idx.btree_next_page_id),
        .pool = pool,
        .wal = wal,
    };
}

/// Persist the BTree's current root_page_id and next_page_id back to the
/// catalog for the given index_id. Must be called after any BTree mutation
/// that may have allocated pages (insert with split).
pub fn syncIndexBTreeState(
    catalog: *Catalog,
    model_id: ModelId,
    index_id: IndexId,
    btree: *const BTree,
) void {
    std.debug.assert(model_id < catalog.model_count);
    const model = &catalog.models[model_id];
    std.debug.assert(index_id < model.index_count);
    model.indexes[index_id].btree_root_page_id = @intCast(btree.root_page_id);
    model.indexes[index_id].btree_next_page_id = @intCast(btree.next_page_id);
}

/// Encode a key Value and insert into the B+ tree for any index.
/// Returns DuplicateKey on conflict.
pub fn insertIndexKey(
    catalog: *Catalog,
    btree: *BTree,
    model_id: ModelId,
    index_id: IndexId,
    key_value: Value,
    row_id: RowId,
) MutationError!void {
    try insertIndexKeyNoSync(btree, key_value, row_id);
    syncIndexBTreeState(catalog, model_id, index_id, btree);
}

/// Encode a key Value and insert into the B+ tree for any index without
/// syncing catalog state. Call `syncIndexBTreeState` once after batching.
pub fn insertIndexKeyNoSync(
    btree: *BTree,
    key_value: Value,
    row_id: RowId,
) MutationError!void {
    var key_buf: [max_row_buf_size]u8 = undefined;
    const key = index_key_mod.encodeValue(key_value, &key_buf);
    btree.insert(key, row_id) catch |e| return mapBTreeError(e);
}

/// Encode a key Value and insert into the B+ tree using a threaded leaf hint.
pub fn insertIndexKeyWithHintNoSync(
    btree: *BTree,
    key_value: Value,
    row_id: RowId,
    hint: *LeafHint,
) MutationError!void {
    var key_buf: [max_row_buf_size]u8 = undefined;
    const key = index_key_mod.encodeValue(key_value, &key_buf);
    btree.insertWithHint(key, row_id, hint) catch |e| return mapBTreeError(e);
}

/// Delete a key from any index's B+ tree. Silently succeeds if the key is
/// not found (e.g., rows inserted before the index existed).
pub fn deleteIndexKey(
    catalog: *Catalog,
    btree: *BTree,
    model_id: ModelId,
    index_id: IndexId,
    key_value: Value,
) MutationError!void {
    var key_buf: [max_row_buf_size]u8 = undefined;
    const key = index_key_mod.encodeValue(key_value, &key_buf);
    btree.delete(key) catch |e| switch (e) {
        error.KeyNotFound => return, // Tolerate missing entries.
        else => return mapBTreeError(e),
    };
    syncIndexBTreeState(catalog, model_id, index_id, btree);
}

/// MVCC-aware uniqueness check for any index.
///
/// Looks up the key in the B+ tree. If found, follows the pointer to the
/// heap and checks MVCC visibility. Returns `true` when the key belongs to
/// a row that is visible to the current snapshot (genuine duplicate).
/// Returns `false` when the key is absent or belongs to a deleted/invisible
/// row — in the latter case the dead B+ tree entry is cleaned up so that
/// future inserts don't collide.
///
/// When any of the optional MVCC parameters is null, falls back to the
/// non-MVCC behavior: key present in B+ tree = duplicate.
pub fn indexKeyVisibleInIndex(
    catalog: *Catalog,
    btree: *BTree,
    model_id: ModelId,
    index_id: IndexId,
    key_value: Value,
    pool: *BufferPool,
    undo_log: ?*const UndoLog,
    snapshot: ?*const Snapshot,
    tx_manager: ?*const TxManager,
) MutationError!bool {
    var key_buf: [max_row_buf_size]u8 = undefined;
    const key = index_key_mod.encodeValue(key_value, &key_buf);
    const row_id_opt = btree.find(key) catch |e| return mapBTreeError(e);
    const row_id = row_id_opt orelse return false;

    // Without full MVCC context, fall back to: key exists = duplicate.
    if (undo_log == null or snapshot == null or tx_manager == null) {
        return true;
    }

    // Pin the heap page and check MVCC visibility.
    const page = pool.pin(row_id.page_id) catch |e| return mapPoolError(e);
    defer pool.unpin(row_id.page_id, false);

    const heap_data_opt: ?[]const u8 = HeapPage.read(page, row_id.slot) catch null;
    const head = undo_log.?.getHead(row_id.page_id, row_id.slot);
    const visible: ?[]const u8 = if (head == null)
        heap_data_opt
    else
        undo_log.?.findVisible(
            row_id.page_id,
            row_id.slot,
            snapshot.?,
            tx_manager.?,
        ) orelse heap_data_opt;

    if (visible != null) {
        // Row is visible — genuine duplicate.
        return true;
    }

    // Row is invisible (committed delete). Clean up the dead B+ tree entry
    // so future inserts don't collide with the stale key.
    deleteIndexKey(catalog, btree, model_id, index_id, key_value) catch {};
    return false;
}

// ---------------------------------------------------------------------------
// Primary-key wrappers (delegate to generic functions)
// ---------------------------------------------------------------------------

/// Construct a transient BTree for the model's primary-key index.
/// Returns null if the model has no PK index with an allocated B+ tree.
pub fn openPrimaryKeyIndex(
    catalog: *const Catalog,
    pool: *BufferPool,
    wal: *Wal,
    model_id: ModelId,
) ?BTree {
    const idx_id = catalog_mod.findPrimaryKeyIndex(catalog, model_id) orelse return null;
    return openIndex(catalog, pool, wal, model_id, idx_id);
}

/// Persist the BTree's current root_page_id and next_page_id back to the
/// catalog for the model's primary-key index.
pub fn syncBTreeState(
    catalog: *Catalog,
    model_id: ModelId,
    btree: *const BTree,
) void {
    const idx_id = catalog_mod.findPrimaryKeyIndex(catalog, model_id) orelse return;
    syncIndexBTreeState(catalog, model_id, idx_id, btree);
}

/// Encode a PK Value and insert into the B+ tree. Returns DuplicateKey on conflict.
pub fn insertPrimaryKey(
    catalog: *Catalog,
    btree: *BTree,
    model_id: ModelId,
    pk_value: Value,
    row_id: RowId,
) MutationError!void {
    const idx_id = catalog_mod.findPrimaryKeyIndex(catalog, model_id) orelse return error.Corruption;
    try insertPrimaryKeyNoSync(catalog, model_id, btree, pk_value, row_id);
    syncIndexBTreeState(catalog, model_id, idx_id, btree);
}

/// Encode a PK Value and insert into the B+ tree without syncing catalog state.
/// Call `syncBTreeState` once after batching.
pub fn insertPrimaryKeyNoSync(
    catalog: *const Catalog,
    model_id: ModelId,
    btree: *BTree,
    pk_value: Value,
    row_id: RowId,
) MutationError!void {
    _ = catalog_mod.findPrimaryKeyIndex(catalog, model_id) orelse return error.Corruption;
    return insertIndexKeyNoSync(btree, pk_value, row_id);
}

/// Encode a PK Value and insert into the B+ tree using a threaded leaf hint.
pub fn insertPrimaryKeyWithHintNoSync(
    catalog: *const Catalog,
    model_id: ModelId,
    btree: *BTree,
    pk_value: Value,
    row_id: RowId,
    hint: *LeafHint,
) MutationError!void {
    _ = catalog_mod.findPrimaryKeyIndex(catalog, model_id) orelse return error.Corruption;
    return insertIndexKeyWithHintNoSync(btree, pk_value, row_id, hint);
}

/// Delete a PK key from the B+ tree. Silently succeeds if the key is not
/// found (e.g., rows inserted before the index existed, or after crash
/// recovery where the B+ tree was not yet rebuilt).
pub fn deletePrimaryKey(
    catalog: *Catalog,
    btree: *BTree,
    model_id: ModelId,
    pk_value: Value,
) MutationError!void {
    var key_buf: [max_row_buf_size]u8 = undefined;
    const key = index_key_mod.encodeValue(pk_value, &key_buf);
    btree.delete(key) catch |e| switch (e) {
        error.KeyNotFound => return, // Tolerate missing entries.
        else => return mapBTreeError(e),
    };
    syncBTreeState(catalog, model_id, btree);
}

/// Check whether a PK value exists in the B+ tree. Returns true if found.
pub fn primaryKeyExists(
    btree: *BTree,
    pk_value: Value,
) MutationError!bool {
    var key_buf: [max_row_buf_size]u8 = undefined;
    const key = index_key_mod.encodeValue(pk_value, &key_buf);
    const found = btree.find(key) catch |e| return mapBTreeError(e);
    return found != null;
}

/// MVCC-aware PK uniqueness check.
///
/// Looks up the key in the B+ tree. If found, follows the pointer to the
/// heap and checks MVCC visibility. Returns `true` when the key belongs to
/// a row that is visible to the current snapshot (genuine duplicate).
/// Returns `false` when the key is absent or belongs to a deleted/invisible
/// row — in the latter case the dead B+ tree entry is also cleaned up.
///
/// When any of the optional MVCC parameters is null, falls back to the
/// non-MVCC behavior: key present in B+ tree = duplicate.
pub fn primaryKeyVisibleInIndex(
    catalog: *Catalog,
    btree: *BTree,
    model_id: ModelId,
    pk_value: Value,
    pool: *BufferPool,
    undo_log: ?*const UndoLog,
    snapshot: ?*const Snapshot,
    tx_manager: ?*const TxManager,
) MutationError!bool {
    var key_buf: [max_row_buf_size]u8 = undefined;
    const key = index_key_mod.encodeValue(pk_value, &key_buf);
    const row_id_opt = btree.find(key) catch |e| return mapBTreeError(e);
    const row_id = row_id_opt orelse return false;

    // Without full MVCC context, fall back to: key exists = duplicate.
    if (undo_log == null or snapshot == null or tx_manager == null) {
        return true;
    }

    // Pin the heap page and check MVCC visibility.
    const page = pool.pin(row_id.page_id) catch |e| return mapPoolError(e);
    defer pool.unpin(row_id.page_id, false);

    const heap_data_opt: ?[]const u8 = HeapPage.read(page, row_id.slot) catch null;
    const head = undo_log.?.getHead(row_id.page_id, row_id.slot);
    const visible: ?[]const u8 = if (head == null)
        heap_data_opt
    else
        undo_log.?.findVisible(
            row_id.page_id,
            row_id.slot,
            snapshot.?,
            tx_manager.?,
        ) orelse heap_data_opt;

    if (visible != null) {
        // Row is visible — genuine duplicate.
        return true;
    }

    // Row is invisible (committed delete). Clean up the dead B+ tree entry
    // so future lookups skip the extra heap check.
    deletePrimaryKey(catalog, btree, model_id, pk_value) catch {};
    return false;
}

fn mapPoolError(err: buffer_pool_mod.BufferPoolError) MutationError {
    return switch (err) {
        error.AllFramesPinned => error.AllFramesPinned,
        error.OutOfMemory => error.OutOfMemory,
        error.ChecksumMismatch => error.ChecksumMismatch,
        error.StorageRead => error.StorageRead,
        error.StorageWrite => error.StorageWrite,
        error.StorageFsync => error.StorageFsync,
        error.WalNotFlushed => error.WalNotFlushed,
    };
}

pub fn mapBTreeError(err: BTreeError) MutationError {
    return switch (err) {
        error.DuplicateKey => error.DuplicateKey,
        error.Corruption => error.Corruption,
        error.InvalidPage => error.Corruption,
        error.TreeEmpty => error.Corruption,
        error.KeyNotFound => error.Corruption,
        error.AllFramesPinned => error.AllFramesPinned,
        error.ChecksumMismatch => error.ChecksumMismatch,
        error.PageFull => error.PageFull,
        error.StorageRead => error.StorageRead,
        error.StorageWrite => error.StorageWrite,
        error.StorageFsync => error.StorageFsync,
        error.WalNotFlushed => error.WalNotFlushed,
        error.WalWriteError => error.WalWriteError,
        error.WalFsyncError => error.WalFsyncError,
        error.OutOfMemory => error.OutOfMemory,
    };
}
