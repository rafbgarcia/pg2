//! Referential-integrity enforcement for mutation operations.
//!
//! Validates outgoing foreign-key references (INSERT/UPDATE must point
//! to an existing parent row), enforces incoming constraints when a
//! parent row is deleted or its key is updated, and executes cascade /
//! set-null referential actions.
const std = @import("std");
const heap_mod = @import("../storage/heap.zig");
const buffer_pool_mod = @import("../storage/buffer_pool.zig");
const wal_mod = @import("../storage/wal.zig");
const row_mod = @import("../storage/row.zig");
const catalog_mod = @import("../catalog/catalog.zig");
const tx_mod = @import("../mvcc/transaction.zig");
const undo_mod = @import("../mvcc/undo.zig");
const scan_mod = @import("scan.zig");
const index_maintenance_mod = @import("index_maintenance.zig");

const mutation = @import("mutation.zig");
const MutationError = mutation.MutationError;
const mapPoolError = mutation.mapPoolError;
const max_assignments = mutation.max_assignments;
const deleteSingleRow = mutation.deleteSingleRow;
const updateRowWithValues = mutation.updateRowWithValues;

const HeapPage = heap_mod.HeapPage;
const RowId = heap_mod.RowId;
const BufferPool = buffer_pool_mod.BufferPool;
const Wal = wal_mod.Wal;
const Value = row_mod.Value;
const RowSchema = row_mod.RowSchema;
const Catalog = catalog_mod.Catalog;
const ModelId = catalog_mod.ModelId;
const TxId = tx_mod.TxId;
const UndoLog = undo_mod.UndoLog;

pub fn enforceOutgoingReferentialIntegrity(
    catalog: *const Catalog,
    pool: *BufferPool,
    wal: *Wal,
    model_id: ModelId,
    row_values: []const Value,
) MutationError!void {
    const model = &catalog.models[model_id];
    var assoc_idx: u16 = 0;
    while (assoc_idx < model.association_count) : (assoc_idx += 1) {
        const assoc = &model.associations[assoc_idx];
        if (assoc.referential_integrity_mode != .with_referential_integrity) continue;
        if (assoc.target_model_id == catalog_mod.null_model) {
            return error.ReferentialIntegrityViolation;
        }
        if (assoc.local_column_id == catalog_mod.null_column or
            assoc.foreign_key_column_id == catalog_mod.null_column)
        {
            return error.ReferentialIntegrityViolation;
        }

        const local_idx: usize = assoc.local_column_id;
        if (local_idx >= row_values.len) return error.ReferentialIntegrityViolation;
        const fk_value = row_values[local_idx];
        if (fk_value == .null_value) continue;

        // Try B+ tree lookup when FK references the target's PK column.
        const target_pk_col = catalog_mod.findPrimaryKeyColumnId(
            catalog,
            assoc.target_model_id,
        );
        if (target_pk_col != null and assoc.foreign_key_column_id == target_pk_col.?) {
            // Target FK column is the PK — use B+ tree if available.
            var pk_btree = index_maintenance_mod.openPrimaryKeyIndex(
                catalog,
                pool,
                wal,
                assoc.target_model_id,
            );
            if (pk_btree != null) {
                const exists = index_maintenance_mod.primaryKeyExists(
                    &pk_btree.?,
                    fk_value,
                ) catch |e| return e;
                if (!exists) return error.ReferentialIntegrityViolation;
                continue;
            }
        }
        // Fallback: heap scan.
        if (!rowExistsForValue(
            catalog,
            pool,
            assoc.target_model_id,
            assoc.foreign_key_column_id,
            fk_value,
        )) {
            return error.ReferentialIntegrityViolation;
        }
    }
}

pub fn enforceIncomingDeleteReferentialIntegrity(
    catalog: *const Catalog,
    pool: *BufferPool,
    wal: *Wal,
    undo_log: *UndoLog,
    tx_id: TxId,
    target_model_id: ModelId,
    deleted_row_values: []const Value,
) MutationError!void {
    var source_model_id: ModelId = 0;
    while (source_model_id < catalog.model_count) : (source_model_id += 1) {
        const source_model = &catalog.models[source_model_id];
        var assoc_idx: u16 = 0;
        while (assoc_idx < source_model.association_count) : (assoc_idx += 1) {
            const assoc = &source_model.associations[assoc_idx];
            if (assoc.referential_integrity_mode != .with_referential_integrity) continue;
            if (assoc.target_model_id != target_model_id) continue;
            if (assoc.local_column_id == catalog_mod.null_column or
                assoc.foreign_key_column_id == catalog_mod.null_column)
            {
                return error.ReferentialIntegrityViolation;
            }

            const key_idx: usize = assoc.foreign_key_column_id;
            if (key_idx >= deleted_row_values.len) return error.ReferentialIntegrityViolation;
            const key = deleted_row_values[key_idx];

            switch (assoc.on_delete) {
                .restrict => {
                    if (hasReferencingRows(
                        catalog,
                        pool,
                        source_model_id,
                        assoc.local_column_id,
                        key,
                    )) return error.ReferentialIntegrityViolation;
                },
                .cascade => try cascadeDeleteReferencingRows(
                    catalog,
                    pool,
                    wal,
                    undo_log,
                    tx_id,
                    source_model_id,
                    assoc.local_column_id,
                    key,
                ),
                .set_null => try setReferencingRowsValue(
                    catalog,
                    pool,
                    wal,
                    undo_log,
                    tx_id,
                    source_model_id,
                    assoc.local_column_id,
                    key,
                    .{ .null_value = {} },
                ),
                .set_default => return error.UnsupportedReferentialAction,
                .unspecified => return error.ReferentialIntegrityViolation,
            }
        }
    }
}

pub fn enforceIncomingUpdateReferentialIntegrity(
    catalog: *const Catalog,
    pool: *BufferPool,
    wal: *Wal,
    undo_log: *UndoLog,
    tx_id: TxId,
    target_model_id: ModelId,
    old_row_values: []const Value,
    new_row_values: []const Value,
) MutationError!void {
    var source_model_id: ModelId = 0;
    while (source_model_id < catalog.model_count) : (source_model_id += 1) {
        const source_model = &catalog.models[source_model_id];
        var assoc_idx: u16 = 0;
        while (assoc_idx < source_model.association_count) : (assoc_idx += 1) {
            const assoc = &source_model.associations[assoc_idx];
            if (assoc.referential_integrity_mode != .with_referential_integrity) continue;
            if (assoc.target_model_id != target_model_id) continue;
            if (assoc.local_column_id == catalog_mod.null_column or
                assoc.foreign_key_column_id == catalog_mod.null_column)
            {
                return error.ReferentialIntegrityViolation;
            }

            const key_idx: usize = assoc.foreign_key_column_id;
            if (key_idx >= old_row_values.len or key_idx >= new_row_values.len) {
                return error.ReferentialIntegrityViolation;
            }
            const old_key = old_row_values[key_idx];
            const new_key = new_row_values[key_idx];
            if (row_mod.compareValues(old_key, new_key) == .eq) continue;

            switch (assoc.on_update) {
                .restrict => {
                    if (hasReferencingRows(
                        catalog,
                        pool,
                        source_model_id,
                        assoc.local_column_id,
                        old_key,
                    )) return error.ReferentialIntegrityViolation;
                },
                .cascade => try setReferencingRowsValue(
                    catalog,
                    pool,
                    wal,
                    undo_log,
                    tx_id,
                    source_model_id,
                    assoc.local_column_id,
                    old_key,
                    new_key,
                ),
                .set_null => try setReferencingRowsValue(
                    catalog,
                    pool,
                    wal,
                    undo_log,
                    tx_id,
                    source_model_id,
                    assoc.local_column_id,
                    old_key,
                    .{ .null_value = {} },
                ),
                .set_default => return error.UnsupportedReferentialAction,
                .unspecified => return error.ReferentialIntegrityViolation,
            }
        }
    }
}

fn cascadeDeleteReferencingRows(
    catalog: *const Catalog,
    pool: *BufferPool,
    wal: *Wal,
    undo_log: *UndoLog,
    tx_id: TxId,
    source_model_id: ModelId,
    source_column_id: catalog_mod.ColumnId,
    key: Value,
) MutationError!void {
    const source_model = &catalog.models[source_model_id];
    var row_ids: [scan_mod.scan_batch_size]RowId = undefined;
    const count = try collectReferencingRows(
        catalog,
        pool,
        source_model_id,
        source_column_id,
        key,
        &row_ids,
    );
    var i: u16 = 0;
    while (i < count) : (i += 1) {
        try deleteSingleRow(
            @constCast(catalog),
            pool,
            wal,
            undo_log,
            tx_id,
            source_model_id,
            &source_model.row_schema,
            row_ids[i],
        );
    }
}

fn setReferencingRowsValue(
    catalog: *const Catalog,
    pool: *BufferPool,
    wal: *Wal,
    undo_log: *UndoLog,
    tx_id: TxId,
    source_model_id: ModelId,
    source_column_id: catalog_mod.ColumnId,
    key: Value,
    new_value: Value,
) MutationError!void {
    const source_model = &catalog.models[source_model_id];
    const schema = &source_model.row_schema;
    const col_idx: usize = source_column_id;
    if (col_idx >= schema.column_count) return error.ReferentialIntegrityViolation;
    if (new_value == .null_value and !schema.columns[col_idx].nullable) {
        return error.ReferentialIntegrityViolation;
    }
    if (new_value != .null_value) {
        const expected_type = schema.columns[col_idx].column_type;
        const got_type = new_value.columnType() orelse return error.ReferentialIntegrityViolation;
        if (expected_type != got_type) return error.ReferentialIntegrityViolation;
    }

    const total_pages = source_model.total_pages;
    const first_page = source_model.heap_first_page_id;
    var page_idx: u32 = 0;
    while (page_idx < total_pages) : (page_idx += 1) {
        const page_id: u64 = @as(u64, first_page) + page_idx;
        const page = pool.pin(page_id) catch |e| return mapPoolError(e);
        defer pool.unpin(page_id, false);

        const slot_count = HeapPage.slot_count(page);
        var slot_idx: u16 = 0;
        while (slot_idx < slot_count) : (slot_idx += 1) {
            const row_data = HeapPage.read(page, slot_idx) catch continue;
            var decoded: [max_assignments]Value = undefined;
            row_mod.decodeRowChecked(schema, row_data, decoded[0..schema.column_count]) catch
                return error.Corruption;

            if (row_mod.compareValues(decoded[col_idx], key) != .eq) continue;

            decoded[col_idx] = new_value;
            try updateRowWithValues(
                @constCast(catalog),
                pool,
                wal,
                undo_log,
                tx_id,
                schema,
                .{ .page_id = page_id, .slot = slot_idx },
                decoded[0..schema.column_count],
            );
        }
    }
}

pub fn hasReferencingRows(
    catalog: *const Catalog,
    pool: *BufferPool,
    source_model_id: ModelId,
    source_column_id: catalog_mod.ColumnId,
    key: Value,
) bool {
    if (key == .null_value) return false;
    return rowExistsForValue(
        catalog,
        pool,
        source_model_id,
        source_column_id,
        key,
    );
}

pub fn rowExistsForValue(
    catalog: *const Catalog,
    pool: *BufferPool,
    model_id: ModelId,
    column_id: catalog_mod.ColumnId,
    key: Value,
) bool {
    if (key == .null_value) return false;
    const model = &catalog.models[model_id];
    const schema = &model.row_schema;
    const col_idx: usize = column_id;
    if (col_idx >= schema.column_count) return false;

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
            if (row_mod.compareValues(decoded[col_idx], key) == .eq) {
                return true;
            }
        }
    }
    return false;
}

fn collectReferencingRows(
    catalog: *const Catalog,
    pool: *BufferPool,
    source_model_id: ModelId,
    source_column_id: catalog_mod.ColumnId,
    key: Value,
    out_row_ids: *[scan_mod.scan_batch_size]RowId,
) MutationError!u16 {
    if (key == .null_value) return 0;
    const source_model = &catalog.models[source_model_id];
    const schema = &source_model.row_schema;
    const col_idx: usize = source_column_id;
    if (col_idx >= schema.column_count) return error.ReferentialIntegrityViolation;

    var count: u16 = 0;
    var page_idx: u32 = 0;
    while (page_idx < source_model.total_pages) : (page_idx += 1) {
        const page_id: u64 = @as(u64, source_model.heap_first_page_id) + page_idx;
        const page = pool.pin(page_id) catch |e| return mapPoolError(e);
        defer pool.unpin(page_id, false);

        const slot_count = HeapPage.slot_count(page);
        var slot_idx: u16 = 0;
        while (slot_idx < slot_count) : (slot_idx += 1) {
            const row_data = HeapPage.read(page, slot_idx) catch continue;
            var decoded: [max_assignments]Value = undefined;
            row_mod.decodeRowChecked(schema, row_data, decoded[0..schema.column_count]) catch
                return error.Corruption;
            if (row_mod.compareValues(decoded[col_idx], key) != .eq) continue;
            if (count >= scan_mod.scan_batch_size) return error.ResultOverflow;
            out_row_ids[count] = .{ .page_id = page_id, .slot = slot_idx };
            count += 1;
        }
    }
    return count;
}
