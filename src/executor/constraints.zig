//! Primary-key and unique-index constraint enforcement.
//!
//! Checks that INSERT operations do not violate primary-key or
//! unique-index constraints by scanning existing heap pages for
//! duplicate key values.
const std = @import("std");
const heap_mod = @import("../storage/heap.zig");
const buffer_pool_mod = @import("../storage/buffer_pool.zig");
const row_mod = @import("../storage/row.zig");
const catalog_mod = @import("../catalog/catalog.zig");

const index_maintenance_mod = @import("index_maintenance.zig");
const index_key_mod = @import("../storage/index_key.zig");
const wal_mod = @import("../storage/wal.zig");
const btree_mod = @import("../storage/btree.zig");
const undo_mod = @import("../mvcc/undo.zig");
const tx_mod = @import("../mvcc/transaction.zig");
const Wal = wal_mod.Wal;
const UndoLog = undo_mod.UndoLog;
const Snapshot = tx_mod.Snapshot;
const TxManager = tx_mod.TxManager;

const mutation = @import("mutation.zig");
const MutationError = mutation.MutationError;
const max_assignments = mutation.max_assignments;

const referential_integrity_mod = @import("referential_integrity.zig");
const rowExistsForValue = referential_integrity_mod.rowExistsForValue;

const HeapPage = heap_mod.HeapPage;
const BufferPool = buffer_pool_mod.BufferPool;
const Value = row_mod.Value;
const RowSchema = row_mod.RowSchema;
const Catalog = catalog_mod.Catalog;
const ModelId = catalog_mod.ModelId;

pub fn enforceInsertUniqueness(
    catalog: *Catalog,
    pool: *BufferPool,
    wal: *Wal,
    model_id: ModelId,
    values: []const Value,
    undo_log: ?*const UndoLog,
    snapshot: ?*const Snapshot,
    tx_manager: ?*const TxManager,
) MutationError!void {
    const model = &catalog.models[model_id];

    var col_id: catalog_mod.ColumnId = 0;
    while (col_id < model.column_count) : (col_id += 1) {
        if (model.columns[col_id].is_primary_key) {
            if (rowExistsForValue(catalog, pool, model_id, col_id, values[col_id])) {
                return error.DuplicateKey;
            }
            break;
        }
    }

    try enforceNonPkUniqueness(catalog, pool, wal, model_id, values, undo_log, snapshot, tx_manager);
}

/// Enforce uniqueness for non-PK unique indexes only. Used when the PK is
/// already checked via B+ tree lookup, so only secondary unique indexes
/// need the heap scan fallback.
pub fn enforceNonPkUniqueness(
    catalog: *Catalog,
    pool: *BufferPool,
    wal: *Wal,
    model_id: ModelId,
    values: []const Value,
    undo_log: ?*const UndoLog,
    snapshot: ?*const Snapshot,
    tx_manager: ?*const TxManager,
) MutationError!void {
    const model = &catalog.models[model_id];
    const pk_col = catalog_mod.findPrimaryKeyColumnId(catalog, model_id);

    var idx_id: u16 = 0;
    while (idx_id < model.index_count) : (idx_id += 1) {
        const idx = model.indexes[idx_id];
        if (!idx.is_unique or idx.column_count == 0) continue;
        // Skip the PK index — it's already enforced via B+ tree.
        if (idx.column_count == 1 and pk_col != null and idx.column_ids[0] == pk_col.?) continue;
        if (uniqueKeyHasNull(values, idx.column_ids[0..idx.column_count])) continue;
        // B+ tree fast path for single-column unique indexes with allocated trees.
        if (idx.column_count == 1 and idx.btree_root_page_id != 0) {
            var btree = index_maintenance_mod.openIndex(
                catalog,
                pool,
                wal,
                model_id,
                idx_id,
            );
            if (btree != null) {
                const is_visible = try index_maintenance_mod.indexKeyVisibleInIndex(
                    catalog,
                    &btree.?,
                    model_id,
                    idx_id,
                    values[idx.column_ids[0]],
                    pool,
                    undo_log,
                    snapshot,
                    tx_manager,
                );
                if (is_visible) return error.DuplicateKey;
                continue;
            }
        }
        if (rowExistsForUniqueIndex(catalog, pool, model_id, &idx, values)) {
            return error.DuplicateKey;
        }
    }
}

fn uniqueKeyHasNull(
    values: []const Value,
    key_column_ids: []const catalog_mod.ColumnId,
) bool {
    for (key_column_ids) |col_id| {
        if (col_id >= values.len) return true;
        if (values[col_id] == .null_value) return true;
    }
    return false;
}

fn rowExistsForUniqueIndex(
    catalog: *const Catalog,
    pool: *BufferPool,
    model_id: ModelId,
    index: *const catalog_mod.IndexInfo,
    key_values: []const Value,
) bool {
    const model = &catalog.models[model_id];
    const schema = &model.row_schema;

    var page_idx: u32 = 0;
    while (page_idx < model.total_pages) : (page_idx += 1) {
        const page_id: u64 = @as(u64, model.heap_first_page_id) + page_idx;
        const page = pool.pin(page_id) catch return false;
        defer pool.unpin(page_id, false);

        const slot_count = HeapPage.slot_count(page);
        var slot_idx: u16 = 0;
        while (slot_idx < slot_count) : (slot_idx += 1) {
            const row_data = HeapPage.read(page, slot_idx) catch continue;
            var decoded: [max_assignments]Value = undefined;
            row_mod.decodeRowChecked(schema, row_data, decoded[0..schema.column_count]) catch
                return false;

            var all_match = true;
            for (index.column_ids[0..index.column_count]) |col_id| {
                if (col_id >= schema.column_count or col_id >= key_values.len) {
                    return false;
                }
                if (row_mod.compareValues(decoded[col_id], key_values[col_id]) != .eq) {
                    all_match = false;
                    break;
                }
            }
            if (all_match) return true;
        }
    }
    return false;
}
