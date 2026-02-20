const std = @import("std");
const ast_mod = @import("../parser/ast.zig");
const tokenizer_mod = @import("../parser/tokenizer.zig");
const page_mod = @import("../storage/page.zig");
const heap_mod = @import("../storage/heap.zig");
const buffer_pool_mod = @import("../storage/buffer_pool.zig");
const wal_mod = @import("../storage/wal.zig");
const row_mod = @import("../storage/row.zig");
const btree_mod = @import("../storage/btree.zig");
const catalog_mod = @import("../catalog/catalog.zig");
const tx_mod = @import("../mvcc/transaction.zig");
const undo_mod = @import("../mvcc/undo.zig");
const filter_mod = @import("filter.zig");
const scan_mod = @import("scan.zig");

const Allocator = std.mem.Allocator;
const Ast = ast_mod.Ast;
const NodeIndex = ast_mod.NodeIndex;
const null_node = ast_mod.null_node;
const TokenType = tokenizer_mod.TokenType;
const TokenizeResult = tokenizer_mod.TokenizeResult;
const Page = page_mod.Page;
const HeapPage = heap_mod.HeapPage;
const RowId = heap_mod.RowId;
const BufferPool = buffer_pool_mod.BufferPool;
const Wal = wal_mod.Wal;
const Value = row_mod.Value;
const RowSchema = row_mod.RowSchema;
const BTree = btree_mod.BTree;
const Catalog = catalog_mod.Catalog;
const ModelId = catalog_mod.ModelId;
const TxId = tx_mod.TxId;
const Snapshot = tx_mod.Snapshot;
const TxManager = tx_mod.TxManager;
const UndoLog = undo_mod.UndoLog;

/// Maximum encoded row size.
pub const max_row_buf_size = 8000;
/// Maximum number of field assignments in a single mutation.
pub const max_assignments = 128;

pub const MutationError = error{
    // Storage errors
    AllFramesPinned,
    ChecksumMismatch,
    Corruption,
    StorageRead,
    StorageWrite,
    StorageFsync,
    WalNotFlushed,
    PageFull,
    RowTooLarge,
    // Encode errors
    BufferTooSmall,
    TypeMismatch,
    NullNotAllowed,
    // Filter/scan errors
    ColumnNotFound,
    InvalidLiteral,
    StackOverflow,
    StackUnderflow,
    DivisionByZero,
    NumericOverflow,
    UnknownFunction,
    NullInPredicate,
    ResultOverflow,
    // WAL errors
    WalWriteError,
    WalFsyncError,
    OutOfMemory,
    // Undo errors
    UndoLogFull,
    ReferentialIntegrityViolation,
    UnsupportedReferentialAction,
};

const PinnedMutationPage = struct {
    pool: *BufferPool,
    page_id: u64,
    page: *Page,
    dirty: bool = false,
    active: bool = true,

    fn pin(pool: *BufferPool, page_id: u64) MutationError!PinnedMutationPage {
        const page = pool.pin(page_id) catch |e| return mapPoolError(e);
        return .{
            .pool = pool,
            .page_id = page_id,
            .page = page,
        };
    }

    fn markDirty(self: *PinnedMutationPage) void {
        self.dirty = true;
    }

    fn release(self: *PinnedMutationPage) void {
        if (!self.active) return;
        self.pool.unpin(self.page_id, self.dirty);
        self.active = false;
    }
};

/// Execute an INSERT operation.
///
/// Walks the assignment linked list to build a Value array, encodes the row,
/// finds a heap page with space (or uses the model's first page), inserts the
/// row, appends a WAL record, and updates catalog stats.
pub fn executeInsert(
    catalog: *Catalog,
    pool: *BufferPool,
    wal: *Wal,
    tx_id: TxId,
    model_id: ModelId,
    tree: *const Ast,
    tokens: *const TokenizeResult,
    source: []const u8,
    first_assignment_node: NodeIndex,
) MutationError!RowId {
    std.debug.assert(model_id < catalog.model_count);
    const model = &catalog.models[model_id];
    const schema = &model.row_schema;
    std.debug.assert(schema.column_count > 0);

    // Build values from assignments.
    var values: [max_assignments]Value =
        [_]Value{.{ .null_value = {} }} ** max_assignments;
    try buildRowFromAssignments(
        tree,
        tokens,
        source,
        schema,
        first_assignment_node,
        &values,
    );
    try enforceOutgoingReferentialIntegrity(
        catalog,
        pool,
        model_id,
        values[0..schema.column_count],
    );

    // Encode row.
    var row_buf: [max_row_buf_size]u8 = undefined;
    const row_len = row_mod.encodeRow(
        schema,
        values[0..schema.column_count],
        &row_buf,
    ) catch |e| return mapEncodeError(e);
    std.debug.assert(row_len > 0);

    // Find a page with space.
    const page_id = try findPageWithSpace(
        pool,
        model.heap_first_page_id,
        model.total_pages,
        row_len,
    );

    // Insert into heap page.
    var pinned = try PinnedMutationPage.pin(pool, page_id);
    defer pinned.release();

    if (pinned.page.header.page_type == .free) {
        HeapPage.init(pinned.page);
    }
    const slot = HeapPage.insert(pinned.page, row_buf[0..row_len]) catch |e|
        return mapHeapError(e);
    pinned.markDirty();

    // WAL append.
    const lsn = wal.append(
        tx_id,
        .insert,
        page_id,
        row_buf[0..row_len],
    ) catch |e| return mapWalAppendError(e);
    pinned.page.header.lsn = lsn;

    // Update catalog stats.
    catalog.incrementRowCount(model_id, 1);
    const current_pages: u32 =
        @intCast(page_id - model.heap_first_page_id + 1);
    if (current_pages > model.total_pages) {
        catalog.models[model_id].total_pages = current_pages;
    }

    const result = RowId{ .page_id = page_id, .slot = slot };
    std.debug.assert(result.page_id >= model.heap_first_page_id);
    return result;
}

/// Execute an UPDATE operation.
///
/// Scans the table, filters by predicate, then for each matching row:
/// reads old data, pushes to undo log, applies assignments, re-encodes,
/// updates the heap page in-place, and appends a WAL record.
pub fn executeUpdate(
    catalog: *Catalog,
    pool: *BufferPool,
    wal: *Wal,
    undo_log: *UndoLog,
    tx_id: TxId,
    snapshot: *const Snapshot,
    tx_manager: *const TxManager,
    model_id: ModelId,
    tree: *const Ast,
    tokens: *const TokenizeResult,
    source: []const u8,
    predicate_node: NodeIndex,
    first_assignment_node: NodeIndex,
    allocator: Allocator,
) MutationError!u32 {
    std.debug.assert(model_id < catalog.model_count);
    _ = allocator;
    const model = &catalog.models[model_id];
    const schema = &model.row_schema;

    var updated_count: u32 = 0;
    const first_page = model.heap_first_page_id;
    const total_pages = model.total_pages;

    var page_idx: u32 = 0;
    while (page_idx < total_pages) : (page_idx += 1) {
        const page_id: u64 = @as(u64, first_page) + page_idx;
        const page = pool.pin(page_id) catch |e| return mapPoolError(e);
        defer pool.unpin(page_id, false);

        const slot_count = HeapPage.slot_count(page);
        var slot_idx: u16 = 0;
        while (slot_idx < slot_count) : (slot_idx += 1) {
            const row_data = HeapPage.read(page, slot_idx) catch continue;
            const data_to_decode = resolveVisibleVersion(
                undo_log,
                page_id,
                slot_idx,
                snapshot,
                tx_manager,
                row_data,
            );

            var row = scan_mod.ResultRow.init();
            row.row_id = .{ .page_id = page_id, .slot = slot_idx };
            row.column_count = schema.column_count;
            row_mod.decodeRowChecked(schema, data_to_decode, &row.values) catch
                return error.Corruption;

            // Apply predicate filter.
            if (predicate_node != null_node) {
                const matches = filter_mod.evaluatePredicate(
                    tree,
                    tokens,
                    source,
                    predicate_node,
                    row.values[0..row.column_count],
                    schema,
                ) catch continue;
                if (!matches) continue;
            }

            var new_values: [max_assignments]Value = undefined;
            for (0..row.column_count) |c| {
                new_values[c] = row.values[c];
            }
            applyAssignments(
                tree,
                tokens,
                source,
                schema,
                first_assignment_node,
                new_values[0..row.column_count],
            ) catch |e| return e;

            try enforceOutgoingReferentialIntegrity(
                catalog,
                pool,
                model_id,
                new_values[0..row.column_count],
            );
            try enforceIncomingUpdateReferentialIntegrity(
                catalog,
                pool,
                wal,
                undo_log,
                tx_id,
                model_id,
                row.values[0..row.column_count],
                new_values[0..row.column_count],
            );
            try updateRowWithValues(
                pool,
                wal,
                undo_log,
                tx_id,
                schema,
                row.row_id,
                new_values[0..row.column_count],
            );
            updated_count += 1;
        }
    }

    return updated_count;
}

/// Execute a DELETE operation.
///
/// Scans the table, filters by predicate, then for each matching row:
/// reads old data, pushes to undo log, marks the slot as deleted,
/// appends a WAL record, and decrements catalog row count.
pub fn executeDelete(
    catalog: *Catalog,
    pool: *BufferPool,
    wal: *Wal,
    undo_log: *UndoLog,
    tx_id: TxId,
    snapshot: *const Snapshot,
    tx_manager: *const TxManager,
    model_id: ModelId,
    tree: *const Ast,
    tokens: *const TokenizeResult,
    source: []const u8,
    predicate_node: NodeIndex,
    allocator: Allocator,
) MutationError!u32 {
    std.debug.assert(model_id < catalog.model_count);
    _ = allocator;

    const model = &catalog.models[model_id];
    const schema = &model.row_schema;

    var deleted_count: u32 = 0;
    const first_page = model.heap_first_page_id;
    const total_pages = model.total_pages;

    var page_idx: u32 = 0;
    while (page_idx < total_pages) : (page_idx += 1) {
        const page_id: u64 = @as(u64, first_page) + page_idx;
        const page = pool.pin(page_id) catch |e| return mapPoolError(e);
        defer pool.unpin(page_id, false);

        const slot_count = HeapPage.slot_count(page);
        var slot_idx: u16 = 0;
        while (slot_idx < slot_count) : (slot_idx += 1) {
            const row_data = HeapPage.read(page, slot_idx) catch continue;
            const data_to_decode = resolveVisibleVersion(
                undo_log,
                page_id,
                slot_idx,
                snapshot,
                tx_manager,
                row_data,
            );

            var row = scan_mod.ResultRow.init();
            row.row_id = .{ .page_id = page_id, .slot = slot_idx };
            row.column_count = schema.column_count;
            row_mod.decodeRowChecked(schema, data_to_decode, &row.values) catch
                return error.Corruption;

            // Apply predicate filter.
            if (predicate_node != null_node) {
                const matches = filter_mod.evaluatePredicate(
                    tree,
                    tokens,
                    source,
                    predicate_node,
                    row.values[0..row.column_count],
                    schema,
                ) catch continue;
                if (!matches) continue;
            }

            try enforceIncomingDeleteReferentialIntegrity(
                catalog,
                pool,
                wal,
                undo_log,
                tx_id,
                model_id,
                row.values[0..row.column_count],
            );
            try deleteSingleRow(pool, wal, undo_log, tx_id, row.row_id);
            deleted_count += 1;
        }
    }

    if (deleted_count > 0) {
        catalog.decrementRowCount(model_id, deleted_count);
    }

    return deleted_count;
}

/// Delete a single matched row: undo push, tombstone, WAL append.
fn deleteSingleRow(
    pool: *BufferPool,
    wal: *Wal,
    undo_log: *UndoLog,
    tx_id: TxId,
    row_id: RowId,
) MutationError!void {
    var pinned = try PinnedMutationPage.pin(pool, row_id.page_id);
    defer pinned.release();

    // Read old data for undo.
    const old_data = HeapPage.read(pinned.page, row_id.slot) catch
        return error.StorageRead;

    _ = undo_log.push(
        tx_id,
        row_id.page_id,
        row_id.slot,
        old_data,
    ) catch return error.UndoLogFull;

    // Tombstone the slot.
    HeapPage.delete(pinned.page, row_id.slot) catch return error.StorageRead;
    pinned.markDirty();

    // WAL append.
    const lsn = wal.append(
        tx_id,
        .delete,
        row_id.page_id,
        &.{},
    ) catch |e| return mapWalAppendError(e);
    pinned.page.header.lsn = lsn;
}

fn updateRowWithValues(
    pool: *BufferPool,
    wal: *Wal,
    undo_log: *UndoLog,
    tx_id: TxId,
    schema: *const RowSchema,
    row_id: RowId,
    new_values: []const Value,
) MutationError!void {
    var pinned = try PinnedMutationPage.pin(pool, row_id.page_id);
    defer pinned.release();

    const old_data = HeapPage.read(pinned.page, row_id.slot) catch
        return error.StorageRead;
    _ = undo_log.push(tx_id, row_id.page_id, row_id.slot, old_data) catch
        return error.UndoLogFull;

    var row_buf: [max_row_buf_size]u8 = undefined;
    const row_len = row_mod.encodeRow(schema, new_values, &row_buf) catch |e|
        return mapEncodeError(e);
    HeapPage.update(pinned.page, row_id.slot, row_buf[0..row_len]) catch |e|
        return mapHeapError(e);
    pinned.markDirty();

    const lsn = wal.append(tx_id, .update, row_id.page_id, row_buf[0..row_len]) catch |e|
        return mapWalAppendError(e);
    pinned.page.header.lsn = lsn;
}

fn enforceOutgoingReferentialIntegrity(
    catalog: *const Catalog,
    pool: *BufferPool,
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

fn enforceIncomingDeleteReferentialIntegrity(
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

fn enforceIncomingUpdateReferentialIntegrity(
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
    var row_ids: [scan_mod.max_result_rows]RowId = undefined;
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
        try deleteSingleRow(pool, wal, undo_log, tx_id, row_ids[i]);
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

fn hasReferencingRows(
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

fn collectReferencingRows(
    catalog: *const Catalog,
    pool: *BufferPool,
    source_model_id: ModelId,
    source_column_id: catalog_mod.ColumnId,
    key: Value,
    out_row_ids: *[scan_mod.max_result_rows]RowId,
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
            if (count >= scan_mod.max_result_rows) return error.ResultOverflow;
            out_row_ids[count] = .{ .page_id = page_id, .slot = slot_idx };
            count += 1;
        }
    }
    return count;
}

fn rowExistsForValue(
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

fn resolveVisibleVersion(
    undo_log: *const UndoLog,
    page_id: u64,
    slot_idx: u16,
    snapshot: *const Snapshot,
    tx_manager: *const TxManager,
    heap_data: []const u8,
) []const u8 {
    const head = undo_log.getHead(page_id, slot_idx);
    if (head == null) return heap_data;
    const vis = undo_log.findVisible(
        page_id,
        slot_idx,
        snapshot,
        tx_manager,
    );
    return vis orelse heap_data;
}

/// Build a Value array from an assignment linked list.
///
/// Each assignment node has: extra = field name token,
/// data.unary = expression node. Walks the linked list, evaluates each
/// expression, and places the result in the correct column position.
pub fn buildRowFromAssignments(
    tree: *const Ast,
    tokens: *const TokenizeResult,
    source: []const u8,
    schema: *const RowSchema,
    first_assignment: NodeIndex,
    out_values: []Value,
) MutationError!void {
    std.debug.assert(out_values.len >= schema.column_count);

    var current = first_assignment;
    while (current != null_node) {
        const node = tree.getNode(current);
        std.debug.assert(node.tag == .assignment);

        const field_name = tokens.getText(node.extra, source);
        const col_idx = schema.findColumn(field_name) orelse
            return error.ColumnNotFound;

        const expr_node = node.data.unary;
        const val = filter_mod.evaluateExpression(
            tree,
            tokens,
            source,
            expr_node,
            &.{},
            schema,
        ) catch |e| return mapFilterError(e);

        out_values[col_idx] = val;
        current = node.next;
    }
}

/// Apply assignment values on top of existing values (for UPDATE).
fn applyAssignments(
    tree: *const Ast,
    tokens: *const TokenizeResult,
    source: []const u8,
    schema: *const RowSchema,
    first_assignment: NodeIndex,
    values: []Value,
) MutationError!void {
    std.debug.assert(values.len >= schema.column_count);

    var current = first_assignment;
    while (current != null_node) {
        const node = tree.getNode(current);
        std.debug.assert(node.tag == .assignment);

        const field_name = tokens.getText(node.extra, source);
        const col_idx = schema.findColumn(field_name) orelse
            return error.ColumnNotFound;

        // Evaluate expression with current row values for context.
        const expr_node = node.data.unary;
        const val = filter_mod.evaluateExpression(
            tree,
            tokens,
            source,
            expr_node,
            values,
            schema,
        ) catch |e| return mapFilterError(e);

        values[col_idx] = val;
        current = node.next;
    }
}

/// Find a heap page with enough free space, or allocate the next page.
fn findPageWithSpace(
    pool: *BufferPool,
    first_page_id: u32,
    total_pages: u32,
    row_len: u16,
) MutationError!u64 {
    std.debug.assert(row_len > 0);

    // Try existing pages (scan from last to first for locality).
    if (total_pages > 0) {
        var p: u32 = total_pages;
        while (p > 0) {
            p -= 1;
            const page_id: u64 = @as(u64, first_page_id) + p;
            const page = pool.pin(page_id) catch |e|
                return mapPoolError(e);
            const free = HeapPage.free_space(page);
            pool.unpin(page_id, false);

            // Need space for slot entry (4 bytes) + row data.
            if (free >= row_len + 4) return page_id;
        }
    }

    // No existing page has space — use next page.
    return @as(u64, first_page_id) + total_pages;
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

fn mapEncodeError(err: row_mod.EncodeError) MutationError {
    return switch (err) {
        error.BufferTooSmall => error.BufferTooSmall,
        error.TypeMismatch => error.TypeMismatch,
        error.NullNotAllowed => error.NullNotAllowed,
    };
}

fn mapHeapError(err: heap_mod.HeapError) MutationError {
    return switch (err) {
        error.PageFull => error.PageFull,
        error.InvalidSlot => error.StorageRead,
        error.RowTooLarge => error.RowTooLarge,
    };
}

fn mapScanError(err: scan_mod.ScanError) MutationError {
    return switch (err) {
        error.AllFramesPinned => error.AllFramesPinned,
        error.ChecksumMismatch => error.ChecksumMismatch,
        error.Corruption => error.Corruption,
        error.StorageRead => error.StorageRead,
        error.StorageWrite => error.StorageWrite,
        error.StorageFsync => error.StorageFsync,
        error.WalNotFlushed => error.WalNotFlushed,
        error.ResultOverflow => error.ResultOverflow,
        error.OutOfMemory => error.OutOfMemory,
    };
}

fn mapFilterError(err: filter_mod.EvalError) MutationError {
    return switch (err) {
        error.StackOverflow => error.StackOverflow,
        error.StackUnderflow => error.StackUnderflow,
        error.TypeMismatch => error.TypeMismatch,
        error.DivisionByZero => error.DivisionByZero,
        error.NumericOverflow => error.NumericOverflow,
        error.ColumnNotFound => error.ColumnNotFound,
        error.InvalidLiteral => error.InvalidLiteral,
        error.UnknownFunction => error.UnknownFunction,
        error.NullInPredicate => error.NullInPredicate,
    };
}

fn mapWalAppendError(err: wal_mod.WalError) MutationError {
    return switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        error.PayloadTooLarge => error.RowTooLarge,
        error.RecordBufferTooSmall => error.WalWriteError,
        error.PayloadBufferTooSmall => error.WalWriteError,
        error.WalReadError => error.WalWriteError,
        error.WalWriteError => error.WalWriteError,
        error.WalFsyncError => error.WalFsyncError,
        error.InvalidEnvelope => error.WalWriteError,
        error.CorruptEnvelope => error.WalWriteError,
        error.UnsupportedEnvelopeVersion => error.WalWriteError,
    };
}

// --- Tests ---

const testing = std.testing;
const disk_mod = @import("../simulator/disk.zig");
const parser_mod = @import("../parser/parser.zig");

const TestEnv = struct {
    disk: disk_mod.SimulatedDisk,
    pool: BufferPool,
    wal: Wal,
    tm: TxManager,
    undo_log: UndoLog,
    catalog: Catalog,
    model_id: ModelId,

    /// Initialize in-place so that disk.storage() captures a stable pointer.
    fn init(self: *TestEnv) !void {
        self.disk = disk_mod.SimulatedDisk.init(testing.allocator);
        self.pool = try BufferPool.init(
            testing.allocator,
            self.disk.storage(),
            16,
        );
        self.wal = Wal.init(testing.allocator, self.disk.storage());
        self.tm = TxManager.init(testing.allocator);
        self.undo_log = try UndoLog.init(testing.allocator, 1024, 64 * 1024);

        self.catalog = Catalog{};
        self.model_id = try self.catalog.addModel("User");
        _ = try self.catalog.addColumn(self.model_id, "id", .bigint, false);
        _ = try self.catalog.addColumn(
            self.model_id,
            "name",
            .string,
            true,
        );
        self.catalog.models[self.model_id].heap_first_page_id = 100;
        self.catalog.models[self.model_id].total_pages = 0;

        const page = try self.pool.pin(100);
        HeapPage.init(page);
        self.pool.unpin(100, true);
        self.catalog.models[self.model_id].total_pages = 1;
    }

    fn deinit(self: *TestEnv) void {
        self.undo_log.deinit();
        self.tm.deinit();
        self.wal.deinit();
        self.pool.deinit();
        self.disk.deinit();
    }
};

test "insert and scan back" {
    var env: TestEnv = undefined;
    try env.init();
    defer env.deinit();

    const tx = try env.tm.begin();
    var snap = try env.tm.snapshot(tx);
    defer snap.deinit();

    const src = "User |> insert(id = 1, name = \"Alice\")";
    const tok = tokenizer_mod.tokenize(src);
    const parsed = parser_mod.parse(&tok, src);
    std.debug.assert(!parsed.has_error);

    const root = parsed.ast.getNode(parsed.ast.root);
    const pipeline = parsed.ast.getNode(root.data.unary);
    const insert_op = parsed.ast.getNode(pipeline.data.binary.rhs);

    const row_id = try executeInsert(
        &env.catalog,
        &env.pool,
        &env.wal,
        tx,
        env.model_id,
        &parsed.ast,
        &tok,
        src,
        insert_op.data.unary,
    );

    try testing.expectEqual(@as(u64, 100), row_id.page_id);
    try testing.expectEqual(@as(u16, 0), row_id.slot);
    try testing.expectEqual(
        @as(u64, 1),
        env.catalog.models[env.model_id].row_count,
    );

    var result = try scan_mod.tableScan(
        &env.catalog,
        &env.pool,
        &env.undo_log,
        &snap,
        &env.tm,
        env.model_id,
        testing.allocator,
    );
    defer result.deinit();

    try testing.expectEqual(@as(u16, 1), result.row_count);
    try testing.expectEqual(@as(i64, 1), result.rows[0].values[0].bigint);
    try testing.expectEqualSlices(
        u8,
        "Alice",
        result.rows[0].values[1].string,
    );
}

test "insert updates catalog stats" {
    var env: TestEnv = undefined;
    try env.init();
    defer env.deinit();

    const tx = try env.tm.begin();

    const src = "User |> insert(id = 1, name = \"Test\")";
    const tok = tokenizer_mod.tokenize(src);
    const parsed = parser_mod.parse(&tok, src);
    const root = parsed.ast.getNode(parsed.ast.root);
    const pipeline = parsed.ast.getNode(root.data.unary);
    const insert_op = parsed.ast.getNode(pipeline.data.binary.rhs);

    _ = try executeInsert(
        &env.catalog,
        &env.pool,
        &env.wal,
        tx,
        env.model_id,
        &parsed.ast,
        &tok,
        src,
        insert_op.data.unary,
    );

    try testing.expectEqual(
        @as(u64, 1),
        env.catalog.models[env.model_id].row_count,
    );
    try testing.expect(env.catalog.models[env.model_id].total_pages >= 1);
}

test "delete removes row" {
    var env: TestEnv = undefined;
    try env.init();
    defer env.deinit();

    const tx = try env.tm.begin();
    var snap = try env.tm.snapshot(tx);
    defer snap.deinit();

    const src1 = "User |> insert(id = 1, name = \"Alice\")";
    const tok1 = tokenizer_mod.tokenize(src1);
    const p1 = parser_mod.parse(&tok1, src1);
    const r1 = p1.ast.getNode(p1.ast.root);
    const pipe1 = p1.ast.getNode(r1.data.unary);
    const ins = p1.ast.getNode(pipe1.data.binary.rhs);
    _ = try executeInsert(
        &env.catalog,
        &env.pool,
        &env.wal,
        tx,
        env.model_id,
        &p1.ast,
        &tok1,
        src1,
        ins.data.unary,
    );

    const src2 = "User |> where(id = 1) |> delete";
    const tok2 = tokenizer_mod.tokenize(src2);
    const p2 = parser_mod.parse(&tok2, src2);
    const r2 = p2.ast.getNode(p2.ast.root);
    const pipe2 = p2.ast.getNode(r2.data.unary);
    const where_op = p2.ast.getNode(pipe2.data.binary.rhs);
    const predicate = where_op.data.unary;

    const deleted = try executeDelete(
        &env.catalog,
        &env.pool,
        &env.wal,
        &env.undo_log,
        tx,
        &snap,
        &env.tm,
        env.model_id,
        &p2.ast,
        &tok2,
        src2,
        predicate,
        testing.allocator,
    );

    try testing.expectEqual(@as(u32, 1), deleted);
    try testing.expectEqual(
        @as(u64, 0),
        env.catalog.models[env.model_id].row_count,
    );
}

test "delete updates catalog stats" {
    var env: TestEnv = undefined;
    try env.init();
    defer env.deinit();

    const tx = try env.tm.begin();
    var snap = try env.tm.snapshot(tx);
    defer snap.deinit();

    const src1 = "User |> insert(id = 1, name = \"A\")";
    const tok1 = tokenizer_mod.tokenize(src1);
    const p1 = parser_mod.parse(&tok1, src1);
    const r1 = p1.ast.getNode(p1.ast.root);
    const pipe1 = p1.ast.getNode(r1.data.unary);
    const ins1 = p1.ast.getNode(pipe1.data.binary.rhs);
    _ = try executeInsert(
        &env.catalog,
        &env.pool,
        &env.wal,
        tx,
        env.model_id,
        &p1.ast,
        &tok1,
        src1,
        ins1.data.unary,
    );

    const src2 = "User |> insert(id = 2, name = \"B\")";
    const tok2 = tokenizer_mod.tokenize(src2);
    const p2 = parser_mod.parse(&tok2, src2);
    const r2 = p2.ast.getNode(p2.ast.root);
    const pipe2 = p2.ast.getNode(r2.data.unary);
    const ins2 = p2.ast.getNode(pipe2.data.binary.rhs);
    _ = try executeInsert(
        &env.catalog,
        &env.pool,
        &env.wal,
        tx,
        env.model_id,
        &p2.ast,
        &tok2,
        src2,
        ins2.data.unary,
    );

    try testing.expectEqual(
        @as(u64, 2),
        env.catalog.models[env.model_id].row_count,
    );

    const src3 = "User |> where(id = 1) |> delete";
    const tok3 = tokenizer_mod.tokenize(src3);
    const p3 = parser_mod.parse(&tok3, src3);
    const r3 = p3.ast.getNode(p3.ast.root);
    const pipe3 = p3.ast.getNode(r3.data.unary);
    const where3 = p3.ast.getNode(pipe3.data.binary.rhs);
    const pred3 = where3.data.unary;

    _ = try executeDelete(
        &env.catalog,
        &env.pool,
        &env.wal,
        &env.undo_log,
        tx,
        &snap,
        &env.tm,
        env.model_id,
        &p3.ast,
        &tok3,
        src3,
        pred3,
        testing.allocator,
    );

    try testing.expectEqual(
        @as(u64, 1),
        env.catalog.models[env.model_id].row_count,
    );
}

test "update modifies row" {
    var env: TestEnv = undefined;
    try env.init();
    defer env.deinit();

    const tx = try env.tm.begin();
    var snap = try env.tm.snapshot(tx);
    defer snap.deinit();

    const src1 = "User |> insert(id = 1, name = \"Alice\")";
    const tok1 = tokenizer_mod.tokenize(src1);
    const p1 = parser_mod.parse(&tok1, src1);
    const r1 = p1.ast.getNode(p1.ast.root);
    const pipe1 = p1.ast.getNode(r1.data.unary);
    const ins1 = p1.ast.getNode(pipe1.data.binary.rhs);
    _ = try executeInsert(
        &env.catalog,
        &env.pool,
        &env.wal,
        tx,
        env.model_id,
        &p1.ast,
        &tok1,
        src1,
        ins1.data.unary,
    );

    const src2 = "User |> where(id = 1) |> update(name = \"Bob\")";
    const tok2 = tokenizer_mod.tokenize(src2);
    const p2 = parser_mod.parse(&tok2, src2);
    const r2 = p2.ast.getNode(p2.ast.root);
    const pipe2 = p2.ast.getNode(r2.data.unary);
    const where_op = p2.ast.getNode(pipe2.data.binary.rhs);
    const update_op = p2.ast.getNode(where_op.next);

    const updated = try executeUpdate(
        &env.catalog,
        &env.pool,
        &env.wal,
        &env.undo_log,
        tx,
        &snap,
        &env.tm,
        env.model_id,
        &p2.ast,
        &tok2,
        src2,
        where_op.data.unary,
        update_op.data.unary,
        testing.allocator,
    );

    try testing.expectEqual(@as(u32, 1), updated);

    var result = try scan_mod.tableScan(
        &env.catalog,
        &env.pool,
        &env.undo_log,
        &snap,
        &env.tm,
        env.model_id,
        testing.allocator,
    );
    defer result.deinit();

    try testing.expectEqual(@as(u16, 1), result.row_count);
    try testing.expectEqualSlices(
        u8,
        "Bob",
        result.rows[0].values[1].string,
    );
}

test "update pushes undo entry" {
    var env: TestEnv = undefined;
    try env.init();
    defer env.deinit();

    const tx = try env.tm.begin();
    var snap = try env.tm.snapshot(tx);
    defer snap.deinit();

    const src1 = "User |> insert(id = 1, name = \"Alice\")";
    const tok1 = tokenizer_mod.tokenize(src1);
    const p1 = parser_mod.parse(&tok1, src1);
    const r1 = p1.ast.getNode(p1.ast.root);
    const pipe1 = p1.ast.getNode(r1.data.unary);
    const ins1 = p1.ast.getNode(pipe1.data.binary.rhs);
    _ = try executeInsert(
        &env.catalog,
        &env.pool,
        &env.wal,
        tx,
        env.model_id,
        &p1.ast,
        &tok1,
        src1,
        ins1.data.unary,
    );

    try testing.expectEqual(@as(usize, 0), env.undo_log.len());

    const src2 = "User |> where(id = 1) |> update(name = \"Bob\")";
    const tok2 = tokenizer_mod.tokenize(src2);
    const p2 = parser_mod.parse(&tok2, src2);
    const r2 = p2.ast.getNode(p2.ast.root);
    const pipe2 = p2.ast.getNode(r2.data.unary);
    const where_op = p2.ast.getNode(pipe2.data.binary.rhs);
    const update_op = p2.ast.getNode(where_op.next);

    _ = try executeUpdate(
        &env.catalog,
        &env.pool,
        &env.wal,
        &env.undo_log,
        tx,
        &snap,
        &env.tm,
        env.model_id,
        &p2.ast,
        &tok2,
        src2,
        where_op.data.unary,
        update_op.data.unary,
        testing.allocator,
    );

    try testing.expectEqual(@as(usize, 1), env.undo_log.len());
}

const ReferentialTestEnv = struct {
    disk: disk_mod.SimulatedDisk,
    pool: BufferPool,
    wal: Wal,
    tm: TxManager,
    undo_log: UndoLog,
    catalog: Catalog,
    user_model_id: ModelId,
    post_model_id: ModelId,

    fn init(self: *ReferentialTestEnv) !void {
        self.disk = disk_mod.SimulatedDisk.init(testing.allocator);
        self.pool = try BufferPool.init(
            testing.allocator,
            self.disk.storage(),
            16,
        );
        self.wal = Wal.init(testing.allocator, self.disk.storage());
        self.tm = TxManager.init(testing.allocator);
        self.undo_log = try UndoLog.init(testing.allocator, 1024, 64 * 1024);

        self.catalog = Catalog{};
        self.user_model_id = try self.catalog.addModel("User");
        _ = try self.catalog.addColumn(self.user_model_id, "id", .bigint, false);
        self.catalog.setColumnPrimaryKey(self.user_model_id, 0);
        self.catalog.models[self.user_model_id].heap_first_page_id = 100;
        self.catalog.models[self.user_model_id].total_pages = 1;
        const user_page = try self.pool.pin(100);
        HeapPage.init(user_page);
        self.pool.unpin(100, true);

        self.post_model_id = try self.catalog.addModel("Post");
        _ = try self.catalog.addColumn(self.post_model_id, "id", .bigint, false);
        _ = try self.catalog.addColumn(self.post_model_id, "user_id", .bigint, true);
        self.catalog.setColumnPrimaryKey(self.post_model_id, 0);
        self.catalog.models[self.post_model_id].heap_first_page_id = 200;
        self.catalog.models[self.post_model_id].total_pages = 1;
        const post_page = try self.pool.pin(200);
        HeapPage.init(post_page);
        self.pool.unpin(200, true);
    }

    fn configureReference(
        self: *ReferentialTestEnv,
        on_delete: catalog_mod.ReferentialAction,
        on_update: catalog_mod.ReferentialAction,
    ) !void {
        const assoc_id = try self.catalog.addAssociation(
            self.post_model_id,
            "author",
            .belongs_to,
            "User",
        );
        try self.catalog.setAssociationKeys(
            self.post_model_id,
            assoc_id,
            "user_id",
            "id",
        );
        try self.catalog.setAssociationReferentialIntegrity(
            self.post_model_id,
            assoc_id,
            .with_referential_integrity,
            on_delete,
            on_update,
        );
        try self.catalog.resolveAssociations();
    }

    fn deinit(self: *ReferentialTestEnv) void {
        self.undo_log.deinit();
        self.tm.deinit();
        self.wal.deinit();
        self.pool.deinit();
        self.disk.deinit();
    }
};

test "insert fails when referential integrity parent row is missing" {
    var env: ReferentialTestEnv = undefined;
    try env.init();
    defer env.deinit();
    try env.configureReference(.restrict, .restrict);

    const tx = try env.tm.begin();

    const src = "Post |> insert(id = 1, user_id = 42)";
    const tok = tokenizer_mod.tokenize(src);
    const parsed = parser_mod.parse(&tok, src);
    const root = parsed.ast.getNode(parsed.ast.root);
    const pipeline = parsed.ast.getNode(root.data.unary);
    const insert_op = parsed.ast.getNode(pipeline.data.binary.rhs);

    try testing.expectError(
        error.ReferentialIntegrityViolation,
        executeInsert(
            &env.catalog,
            &env.pool,
            &env.wal,
            tx,
            env.post_model_id,
            &parsed.ast,
            &tok,
            src,
            insert_op.data.unary,
        ),
    );
}

test "delete restrict blocks parent delete when child references exist" {
    var env: ReferentialTestEnv = undefined;
    try env.init();
    defer env.deinit();
    try env.configureReference(.restrict, .restrict);

    const tx = try env.tm.begin();
    var snap = try env.tm.snapshot(tx);
    defer snap.deinit();

    const user_insert_src = "User |> insert(id = 1)";
    const user_insert_tok = tokenizer_mod.tokenize(user_insert_src);
    const user_insert_parsed = parser_mod.parse(&user_insert_tok, user_insert_src);
    const user_insert_root = user_insert_parsed.ast.getNode(user_insert_parsed.ast.root);
    const user_insert_pipeline = user_insert_parsed.ast.getNode(user_insert_root.data.unary);
    const user_insert_op = user_insert_parsed.ast.getNode(user_insert_pipeline.data.binary.rhs);
    _ = try executeInsert(
        &env.catalog,
        &env.pool,
        &env.wal,
        tx,
        env.user_model_id,
        &user_insert_parsed.ast,
        &user_insert_tok,
        user_insert_src,
        user_insert_op.data.unary,
    );

    const post_insert_src = "Post |> insert(id = 10, user_id = 1)";
    const post_insert_tok = tokenizer_mod.tokenize(post_insert_src);
    const post_insert_parsed = parser_mod.parse(&post_insert_tok, post_insert_src);
    const post_insert_root = post_insert_parsed.ast.getNode(post_insert_parsed.ast.root);
    const post_insert_pipeline = post_insert_parsed.ast.getNode(post_insert_root.data.unary);
    const post_insert_op = post_insert_parsed.ast.getNode(post_insert_pipeline.data.binary.rhs);
    _ = try executeInsert(
        &env.catalog,
        &env.pool,
        &env.wal,
        tx,
        env.post_model_id,
        &post_insert_parsed.ast,
        &post_insert_tok,
        post_insert_src,
        post_insert_op.data.unary,
    );

    const delete_src = "User |> where(id = 1) |> delete";
    const delete_tok = tokenizer_mod.tokenize(delete_src);
    const delete_parsed = parser_mod.parse(&delete_tok, delete_src);
    const delete_root = delete_parsed.ast.getNode(delete_parsed.ast.root);
    const delete_pipeline = delete_parsed.ast.getNode(delete_root.data.unary);
    const where_op = delete_parsed.ast.getNode(delete_pipeline.data.binary.rhs);

    try testing.expectError(
        error.ReferentialIntegrityViolation,
        executeDelete(
            &env.catalog,
            &env.pool,
            &env.wal,
            &env.undo_log,
            tx,
            &snap,
            &env.tm,
            env.user_model_id,
            &delete_parsed.ast,
            &delete_tok,
            delete_src,
            where_op.data.unary,
            testing.allocator,
        ),
    );
}

test "delete cascade removes referencing child rows" {
    var env: ReferentialTestEnv = undefined;
    try env.init();
    defer env.deinit();
    try env.configureReference(.cascade, .cascade);

    const tx = try env.tm.begin();
    var snap = try env.tm.snapshot(tx);
    defer snap.deinit();

    const user_insert_src = "User |> insert(id = 1)";
    const user_insert_tok = tokenizer_mod.tokenize(user_insert_src);
    const user_insert_parsed = parser_mod.parse(&user_insert_tok, user_insert_src);
    const user_insert_root = user_insert_parsed.ast.getNode(user_insert_parsed.ast.root);
    const user_insert_pipeline = user_insert_parsed.ast.getNode(user_insert_root.data.unary);
    const user_insert_op = user_insert_parsed.ast.getNode(user_insert_pipeline.data.binary.rhs);
    _ = try executeInsert(
        &env.catalog,
        &env.pool,
        &env.wal,
        tx,
        env.user_model_id,
        &user_insert_parsed.ast,
        &user_insert_tok,
        user_insert_src,
        user_insert_op.data.unary,
    );

    const post_insert_src = "Post |> insert(id = 10, user_id = 1)";
    const post_insert_tok = tokenizer_mod.tokenize(post_insert_src);
    const post_insert_parsed = parser_mod.parse(&post_insert_tok, post_insert_src);
    const post_insert_root = post_insert_parsed.ast.getNode(post_insert_parsed.ast.root);
    const post_insert_pipeline = post_insert_parsed.ast.getNode(post_insert_root.data.unary);
    const post_insert_op = post_insert_parsed.ast.getNode(post_insert_pipeline.data.binary.rhs);
    _ = try executeInsert(
        &env.catalog,
        &env.pool,
        &env.wal,
        tx,
        env.post_model_id,
        &post_insert_parsed.ast,
        &post_insert_tok,
        post_insert_src,
        post_insert_op.data.unary,
    );

    const delete_src = "User |> where(id = 1) |> delete";
    const delete_tok = tokenizer_mod.tokenize(delete_src);
    const delete_parsed = parser_mod.parse(&delete_tok, delete_src);
    const delete_root = delete_parsed.ast.getNode(delete_parsed.ast.root);
    const delete_pipeline = delete_parsed.ast.getNode(delete_root.data.unary);
    const where_op = delete_parsed.ast.getNode(delete_pipeline.data.binary.rhs);
    const deleted = try executeDelete(
        &env.catalog,
        &env.pool,
        &env.wal,
        &env.undo_log,
        tx,
        &snap,
        &env.tm,
        env.user_model_id,
        &delete_parsed.ast,
        &delete_tok,
        delete_src,
        where_op.data.unary,
        testing.allocator,
    );
    try testing.expectEqual(@as(u32, 1), deleted);

    var post_rows = try scan_mod.tableScan(
        &env.catalog,
        &env.pool,
        &env.undo_log,
        &snap,
        &env.tm,
        env.post_model_id,
        testing.allocator,
    );
    defer post_rows.deinit();
    try testing.expectEqual(@as(u16, 0), post_rows.row_count);
}

test "update restrict blocks parent key update when child references exist" {
    var env: ReferentialTestEnv = undefined;
    try env.init();
    defer env.deinit();
    try env.configureReference(.restrict, .restrict);

    const tx = try env.tm.begin();
    var snap = try env.tm.snapshot(tx);
    defer snap.deinit();

    const user_insert_src = "User |> insert(id = 1)";
    const user_insert_tok = tokenizer_mod.tokenize(user_insert_src);
    const user_insert_parsed = parser_mod.parse(&user_insert_tok, user_insert_src);
    const user_insert_root = user_insert_parsed.ast.getNode(user_insert_parsed.ast.root);
    const user_insert_pipeline = user_insert_parsed.ast.getNode(user_insert_root.data.unary);
    const user_insert_op = user_insert_parsed.ast.getNode(user_insert_pipeline.data.binary.rhs);
    _ = try executeInsert(
        &env.catalog,
        &env.pool,
        &env.wal,
        tx,
        env.user_model_id,
        &user_insert_parsed.ast,
        &user_insert_tok,
        user_insert_src,
        user_insert_op.data.unary,
    );

    const post_insert_src = "Post |> insert(id = 10, user_id = 1)";
    const post_insert_tok = tokenizer_mod.tokenize(post_insert_src);
    const post_insert_parsed = parser_mod.parse(&post_insert_tok, post_insert_src);
    const post_insert_root = post_insert_parsed.ast.getNode(post_insert_parsed.ast.root);
    const post_insert_pipeline = post_insert_parsed.ast.getNode(post_insert_root.data.unary);
    const post_insert_op = post_insert_parsed.ast.getNode(post_insert_pipeline.data.binary.rhs);
    _ = try executeInsert(
        &env.catalog,
        &env.pool,
        &env.wal,
        tx,
        env.post_model_id,
        &post_insert_parsed.ast,
        &post_insert_tok,
        post_insert_src,
        post_insert_op.data.unary,
    );

    const update_src = "User |> where(id = 1) |> update(id = 2)";
    const update_tok = tokenizer_mod.tokenize(update_src);
    const update_parsed = parser_mod.parse(&update_tok, update_src);
    const update_root = update_parsed.ast.getNode(update_parsed.ast.root);
    const update_pipeline = update_parsed.ast.getNode(update_root.data.unary);
    const where_op = update_parsed.ast.getNode(update_pipeline.data.binary.rhs);
    const update_op = update_parsed.ast.getNode(where_op.next);

    try testing.expectError(
        error.ReferentialIntegrityViolation,
        executeUpdate(
            &env.catalog,
            &env.pool,
            &env.wal,
            &env.undo_log,
            tx,
            &snap,
            &env.tm,
            env.user_model_id,
            &update_parsed.ast,
            &update_tok,
            update_src,
            where_op.data.unary,
            update_op.data.unary,
            testing.allocator,
        ),
    );
}

test "update cascade rewrites child foreign keys" {
    var env: ReferentialTestEnv = undefined;
    try env.init();
    defer env.deinit();
    try env.configureReference(.restrict, .cascade);

    const tx = try env.tm.begin();
    var snap = try env.tm.snapshot(tx);
    defer snap.deinit();

    const user_insert_src = "User |> insert(id = 1)";
    const user_insert_tok = tokenizer_mod.tokenize(user_insert_src);
    const user_insert_parsed = parser_mod.parse(&user_insert_tok, user_insert_src);
    const user_insert_root = user_insert_parsed.ast.getNode(user_insert_parsed.ast.root);
    const user_insert_pipeline = user_insert_parsed.ast.getNode(user_insert_root.data.unary);
    const user_insert_op = user_insert_parsed.ast.getNode(user_insert_pipeline.data.binary.rhs);
    _ = try executeInsert(
        &env.catalog,
        &env.pool,
        &env.wal,
        tx,
        env.user_model_id,
        &user_insert_parsed.ast,
        &user_insert_tok,
        user_insert_src,
        user_insert_op.data.unary,
    );

    const post_insert_src = "Post |> insert(id = 10, user_id = 1)";
    const post_insert_tok = tokenizer_mod.tokenize(post_insert_src);
    const post_insert_parsed = parser_mod.parse(&post_insert_tok, post_insert_src);
    const post_insert_root = post_insert_parsed.ast.getNode(post_insert_parsed.ast.root);
    const post_insert_pipeline = post_insert_parsed.ast.getNode(post_insert_root.data.unary);
    const post_insert_op = post_insert_parsed.ast.getNode(post_insert_pipeline.data.binary.rhs);
    _ = try executeInsert(
        &env.catalog,
        &env.pool,
        &env.wal,
        tx,
        env.post_model_id,
        &post_insert_parsed.ast,
        &post_insert_tok,
        post_insert_src,
        post_insert_op.data.unary,
    );

    const update_src = "User |> where(id = 1) |> update(id = 2)";
    const update_tok = tokenizer_mod.tokenize(update_src);
    const update_parsed = parser_mod.parse(&update_tok, update_src);
    const update_root = update_parsed.ast.getNode(update_parsed.ast.root);
    const update_pipeline = update_parsed.ast.getNode(update_root.data.unary);
    const where_op = update_parsed.ast.getNode(update_pipeline.data.binary.rhs);
    const update_op = update_parsed.ast.getNode(where_op.next);
    const updated = try executeUpdate(
        &env.catalog,
        &env.pool,
        &env.wal,
        &env.undo_log,
        tx,
        &snap,
        &env.tm,
        env.user_model_id,
        &update_parsed.ast,
        &update_tok,
        update_src,
        where_op.data.unary,
        update_op.data.unary,
        testing.allocator,
    );
    try testing.expectEqual(@as(u32, 1), updated);

    var post_rows = try scan_mod.tableScan(
        &env.catalog,
        &env.pool,
        &env.undo_log,
        &snap,
        &env.tm,
        env.post_model_id,
        testing.allocator,
    );
    defer post_rows.deinit();
    try testing.expectEqual(@as(u16, 1), post_rows.row_count);
    try testing.expectEqual(@as(i64, 2), post_rows.rows[0].values[1].bigint);
}

test "update set null clears child foreign keys" {
    var env: ReferentialTestEnv = undefined;
    try env.init();
    defer env.deinit();
    try env.configureReference(.restrict, .set_null);

    const tx = try env.tm.begin();
    var snap = try env.tm.snapshot(tx);
    defer snap.deinit();

    const user_insert_src = "User |> insert(id = 1)";
    const user_insert_tok = tokenizer_mod.tokenize(user_insert_src);
    const user_insert_parsed = parser_mod.parse(&user_insert_tok, user_insert_src);
    const user_insert_root = user_insert_parsed.ast.getNode(user_insert_parsed.ast.root);
    const user_insert_pipeline = user_insert_parsed.ast.getNode(user_insert_root.data.unary);
    const user_insert_op = user_insert_parsed.ast.getNode(user_insert_pipeline.data.binary.rhs);
    _ = try executeInsert(
        &env.catalog,
        &env.pool,
        &env.wal,
        tx,
        env.user_model_id,
        &user_insert_parsed.ast,
        &user_insert_tok,
        user_insert_src,
        user_insert_op.data.unary,
    );

    const post_insert_src = "Post |> insert(id = 10, user_id = 1)";
    const post_insert_tok = tokenizer_mod.tokenize(post_insert_src);
    const post_insert_parsed = parser_mod.parse(&post_insert_tok, post_insert_src);
    const post_insert_root = post_insert_parsed.ast.getNode(post_insert_parsed.ast.root);
    const post_insert_pipeline = post_insert_parsed.ast.getNode(post_insert_root.data.unary);
    const post_insert_op = post_insert_parsed.ast.getNode(post_insert_pipeline.data.binary.rhs);
    _ = try executeInsert(
        &env.catalog,
        &env.pool,
        &env.wal,
        tx,
        env.post_model_id,
        &post_insert_parsed.ast,
        &post_insert_tok,
        post_insert_src,
        post_insert_op.data.unary,
    );

    const update_src = "User |> where(id = 1) |> update(id = 2)";
    const update_tok = tokenizer_mod.tokenize(update_src);
    const update_parsed = parser_mod.parse(&update_tok, update_src);
    const update_root = update_parsed.ast.getNode(update_parsed.ast.root);
    const update_pipeline = update_parsed.ast.getNode(update_root.data.unary);
    const where_op = update_parsed.ast.getNode(update_pipeline.data.binary.rhs);
    const update_op = update_parsed.ast.getNode(where_op.next);
    _ = try executeUpdate(
        &env.catalog,
        &env.pool,
        &env.wal,
        &env.undo_log,
        tx,
        &snap,
        &env.tm,
        env.user_model_id,
        &update_parsed.ast,
        &update_tok,
        update_src,
        where_op.data.unary,
        update_op.data.unary,
        testing.allocator,
    );

    var post_rows = try scan_mod.tableScan(
        &env.catalog,
        &env.pool,
        &env.undo_log,
        &snap,
        &env.tm,
        env.post_model_id,
        testing.allocator,
    );
    defer post_rows.deinit();
    try testing.expectEqual(@as(u16, 1), post_rows.row_count);
    try testing.expect(post_rows.rows[0].values[1] == .null_value);
}

test "set default referential action is rejected at configuration time" {
    var env: ReferentialTestEnv = undefined;
    try env.init();
    defer env.deinit();
    try testing.expectError(
        error.InvalidAssociationConfig,
        env.configureReference(.restrict, .set_default),
    );
}
