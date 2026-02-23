//! Primary-key B+ tree maintenance for mutation operations.
//!
//! Responsibilities in this file:
//! - Constructs transient BTree handles from catalog metadata.
//! - Provides insert/delete helpers that encode PK values and sync state.
//! - Maps BTreeError to MutationError.
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

const BTree = btree_mod.BTree;
const BTreeError = btree_mod.BTreeError;
const Value = row_mod.Value;
const RowId = heap_mod.RowId;
const Catalog = catalog_mod.Catalog;
const ModelId = catalog_mod.ModelId;
const BufferPool = buffer_pool_mod.BufferPool;
const Wal = wal_mod.Wal;

const mutation = @import("mutation.zig");
const MutationError = mutation.MutationError;
const max_row_buf_size = mutation.max_row_buf_size;

/// Construct a transient BTree from catalog IndexInfo + runtime pool/wal.
/// Returns null if the model has no PK index with an allocated B+ tree.
pub fn openPrimaryKeyIndex(
    catalog: *const Catalog,
    pool: *BufferPool,
    wal: *Wal,
    model_id: ModelId,
) ?BTree {
    const idx_id = catalog_mod.findPrimaryKeyIndex(catalog, model_id) orelse return null;
    const idx = &catalog.models[model_id].indexes[idx_id];
    return BTree{
        .root_page_id = @as(u64, idx.btree_root_page_id),
        .next_page_id = @as(u64, idx.btree_next_page_id),
        .pool = pool,
        .wal = wal,
    };
}

/// Persist the BTree's current next_page_id back to the catalog.
/// Must be called after any BTree mutation that may have allocated pages (insert with split).
pub fn syncBTreeState(
    catalog: *Catalog,
    model_id: ModelId,
    btree: *const BTree,
) void {
    const idx_id = catalog_mod.findPrimaryKeyIndex(catalog, model_id) orelse return;
    catalog.models[model_id].indexes[idx_id].btree_next_page_id =
        @intCast(btree.next_page_id);
}

/// Encode a PK Value and insert into the B+ tree. Returns DuplicateKey on conflict.
pub fn insertPrimaryKey(
    catalog: *Catalog,
    btree: *BTree,
    model_id: ModelId,
    pk_value: Value,
    row_id: RowId,
) MutationError!void {
    var key_buf: [max_row_buf_size]u8 = undefined;
    const key = index_key_mod.encodeValue(pk_value, &key_buf);
    btree.insert(key, row_id) catch |e| return mapBTreeError(e);
    syncBTreeState(catalog, model_id, btree);
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
