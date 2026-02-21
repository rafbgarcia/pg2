//! Data-mutation executor path (insert/update/delete).
//!
//! Responsibilities in this file:
//! - Builds/encodes row values from mutation assignments.
//! - Applies heap/index writes through buffer pool and WAL.
//! - Integrates overflow-chain lifecycle for oversized strings.
//! - Enforces outgoing/incoming referential-integrity actions.
//! - Records undo entries required for MVCC visibility and recovery.
const std = @import("std");
const ast_mod = @import("../parser/ast.zig");
const tokenizer_mod = @import("../parser/tokenizer.zig");
const page_mod = @import("../storage/page.zig");
const heap_mod = @import("../storage/heap.zig");
const buffer_pool_mod = @import("../storage/buffer_pool.zig");
const wal_mod = @import("../storage/wal.zig");
const row_mod = @import("../storage/row.zig");
const overflow_mod = @import("../storage/overflow.zig");
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
const ColumnType = row_mod.ColumnType;
const OverflowPage = overflow_mod.OverflowPage;
const OverflowPageIdAllocator = overflow_mod.PageIdAllocator;
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
    DuplicateKey,
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
    OverflowRegionExhausted,
    OverflowReclaimQueueFull,
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

const OverflowChainRecordMeta = struct {
    first_page_id: u64,
    page_count: u32,
    payload_bytes: u32,
};

const OverflowRelinkRecordMeta = struct {
    old_first_page_id: u64,
    new_first_page_id: u64,
};

const OverflowChainStats = struct {
    first_page_id: u64,
    page_count: u32,
    payload_bytes: u32,
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
    var assigned_columns: [max_assignments]bool = [_]bool{false} ** max_assignments;
    try buildRowFromAssignments(
        tree,
        tokens,
        source,
        schema,
        first_assignment_node,
        &values,
        &assigned_columns,
    );
    applyColumnDefaultsForInsert(
        catalog,
        model_id,
        values[0..schema.column_count],
        assigned_columns[0..schema.column_count],
    );
    try enforceInsertUniqueness(
        catalog,
        pool,
        model_id,
        values[0..schema.column_count],
    );
    try enforceOutgoingReferentialIntegrity(
        catalog,
        pool,
        model_id,
        values[0..schema.column_count],
    );

    var overflow_page_ids: [max_assignments]u64 = [_]u64{0} ** max_assignments;
    var overflow_chain_stats: [max_assignments]OverflowChainStats =
        [_]OverflowChainStats{.{ .first_page_id = 0, .page_count = 0, .payload_bytes = 0 }} ** max_assignments;
    markOversizedStringSlots(
        schema,
        values[0..schema.column_count],
        overflow_page_ids[0..schema.column_count],
    );

    // Dry-run encode to size/select target heap page before allocating overflow pages.
    var row_buf: [max_row_buf_size]u8 = undefined;
    const dry_row_len = row_mod.encodeRowWithOverflow(
        schema,
        values[0..schema.column_count],
        overflow_page_ids[0..schema.column_count],
        &row_buf,
    ) catch |e| return mapEncodeError(e);
    std.debug.assert(dry_row_len > 0);

    // Find a page with space.
    const page_id = try findPageWithSpace(
        pool,
        model.heap_first_page_id,
        model.total_pages,
        dry_row_len,
    );

    try spillOversizedStrings(
        pool,
        wal,
        tx_id,
        &catalog.overflow_page_allocator,
        schema,
        values[0..schema.column_count],
        overflow_page_ids[0..schema.column_count],
        overflow_chain_stats[0..schema.column_count],
    );

    const row_len = row_mod.encodeRowWithOverflow(
        schema,
        values[0..schema.column_count],
        overflow_page_ids[0..schema.column_count],
        &row_buf,
    ) catch |e| return mapEncodeError(e);

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

    try appendOverflowRelinkWalForRow(
        wal,
        tx_id,
        page_id,
        overflow_page_ids[0..schema.column_count],
        0,
    );
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
    const model = &catalog.models[model_id];
    const schema = &model.row_schema;
    _ = allocator;
    var string_decode_bytes: [max_row_buf_size]u8 = undefined;
    var string_arena = scan_mod.StringArena.init(string_decode_bytes[0..]);

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
            string_arena.reset();
            decodeRowWithOverflow(
                schema,
                data_to_decode,
                pool,
                &catalog.overflow_page_allocator,
                &string_arena,
                &row.values,
            ) catch |e| return e;

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
                catalog,
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
    var string_decode_bytes: [max_row_buf_size]u8 = undefined;
    var string_arena = scan_mod.StringArena.init(string_decode_bytes[0..]);

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
            string_arena.reset();
            decodeRowWithOverflow(
                schema,
                data_to_decode,
                pool,
                &catalog.overflow_page_allocator,
                &string_arena,
                &row.values,
            ) catch |e| return e;

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
            try deleteSingleRow(
                catalog,
                pool,
                wal,
                undo_log,
                tx_id,
                schema,
                row.row_id,
            );
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
    catalog: *Catalog,
    pool: *BufferPool,
    wal: *Wal,
    undo_log: *UndoLog,
    tx_id: TxId,
    schema: *const RowSchema,
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

    var old_overflow_roots: [max_assignments]u64 = [_]u64{0} ** max_assignments;
    const old_overflow_count = try collectOverflowRootsFromRow(
        schema,
        old_data,
        old_overflow_roots[0..schema.column_count],
    );

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

    for (0..old_overflow_count) |i| {
        try enqueueOverflowChainForReclaim(catalog, wal, tx_id, old_overflow_roots[i]);
    }
}

fn updateRowWithValues(
    catalog: *Catalog,
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
    var old_overflow_roots: [max_assignments]u64 = [_]u64{0} ** max_assignments;
    const old_overflow_count = try collectOverflowRootsFromRow(
        schema,
        old_data,
        old_overflow_roots[0..schema.column_count],
    );

    var overflow_page_ids: [max_assignments]u64 = [_]u64{0} ** max_assignments;
    var overflow_chain_stats: [max_assignments]OverflowChainStats =
        [_]OverflowChainStats{.{ .first_page_id = 0, .page_count = 0, .payload_bytes = 0 }} ** max_assignments;
    markOversizedStringSlots(
        schema,
        new_values,
        overflow_page_ids[0..schema.column_count],
    );

    var row_buf: [max_row_buf_size]u8 = undefined;
    const dry_row_len = row_mod.encodeRowWithOverflow(
        schema,
        new_values,
        overflow_page_ids[0..schema.column_count],
        &row_buf,
    ) catch |e| return mapEncodeError(e);

    if (!canUpdateFitInPage(
        pinned.page,
        row_id.slot,
        dry_row_len,
    )) return error.PageFull;

    try spillOversizedStrings(
        pool,
        wal,
        tx_id,
        &catalog.overflow_page_allocator,
        schema,
        new_values,
        overflow_page_ids[0..schema.column_count],
        overflow_chain_stats[0..schema.column_count],
    );

    const row_len = row_mod.encodeRowWithOverflow(
        schema,
        new_values,
        overflow_page_ids[0..schema.column_count],
        &row_buf,
    ) catch |e| return mapEncodeError(e);

    HeapPage.update(pinned.page, row_id.slot, row_buf[0..row_len]) catch |e|
        return mapHeapError(e);
    pinned.markDirty();

    const lsn = wal.append(tx_id, .update, row_id.page_id, row_buf[0..row_len]) catch |e|
        return mapWalAppendError(e);
    pinned.page.header.lsn = lsn;

    try appendOverflowRelinkWalForRow(
        wal,
        tx_id,
        row_id.page_id,
        overflow_page_ids[0..schema.column_count],
        0,
    );
    for (0..old_overflow_count) |i| {
        try enqueueOverflowChainForReclaim(catalog, wal, tx_id, old_overflow_roots[i]);
    }
}

fn canUpdateFitInPage(page: *const Page, slot_idx: u16, new_len: u16) bool {
    const old_row = HeapPage.read(page, slot_idx) catch return false;
    if (new_len <= old_row.len) return true;

    const free = HeapPage.free_space(page);
    if (free >= new_len) return true;
    const fragmented = HeapPage.fragmented_bytes(page);
    return @as(u32, free) + @as(u32, fragmented) >= new_len;
}

fn markOversizedStringSlots(
    schema: *const RowSchema,
    values: []const Value,
    overflow_page_ids: []u64,
) void {
    std.debug.assert(values.len >= schema.column_count);
    std.debug.assert(overflow_page_ids.len >= schema.column_count);
    for (0..schema.column_count) |i| {
        if (values[i] == .null_value) continue;
        const col = schema.columns[i];
        if (col.column_type != .string) continue;
        if (values[i].string.len > overflow_mod.string_inline_threshold_bytes) {
            // Non-zero sentinel to force pointer-sized dry-run encoding.
            overflow_page_ids[i] = 1;
        }
    }
}

fn spillOversizedStrings(
    pool: *BufferPool,
    wal: *Wal,
    tx_id: TxId,
    overflow_allocator: *OverflowPageIdAllocator,
    schema: *const RowSchema,
    values: []const Value,
    overflow_page_ids: []u64,
    overflow_chain_stats: []OverflowChainStats,
) MutationError!void {
    std.debug.assert(values.len >= schema.column_count);
    std.debug.assert(overflow_page_ids.len >= schema.column_count);
    std.debug.assert(overflow_chain_stats.len >= schema.column_count);
    for (0..schema.column_count) |i| {
        if (overflow_page_ids[i] == 0) continue;
        if (values[i] == .null_value) continue;
        const col = schema.columns[i];
        if (col.column_type != .string) continue;
        overflow_chain_stats[i] = try writeOverflowChain(
            pool,
            wal,
            tx_id,
            overflow_allocator,
            values[i].string,
        );
        overflow_page_ids[i] = overflow_chain_stats[i].first_page_id;
    }
}

fn writeOverflowChain(
    pool: *BufferPool,
    wal: *Wal,
    tx_id: TxId,
    overflow_allocator: *OverflowPageIdAllocator,
    payload: []const u8,
) MutationError!OverflowChainStats {
    std.debug.assert(payload.len > overflow_mod.string_inline_threshold_bytes);
    const chunk_capacity = OverflowPage.max_payload_len();
    std.debug.assert(chunk_capacity > 0);

    const first_page_id = overflow_allocator.allocate() catch |e|
        return mapOverflowAllocatorError(e);
    var page_count: u32 = 0;

    var payload_offset: usize = 0;
    var current_page_id = first_page_id;
    while (payload_offset < payload.len) {
        const remaining = payload.len - payload_offset;
        const chunk_len = @min(remaining, chunk_capacity);
        const next_page_id = if (chunk_len == remaining)
            OverflowPage.null_page_id
        else
            overflow_allocator.allocate() catch |e| return mapOverflowAllocatorError(e);

        var pinned = try PinnedMutationPage.pin(pool, current_page_id);
        defer pinned.release();
        if (pinned.page.header.page_type == .free) {
            OverflowPage.init(pinned.page);
        } else if (pinned.page.header.page_type != .overflow) {
            return error.Corruption;
        }
        OverflowPage.writeChunk(
            pinned.page,
            payload[payload_offset..][0..chunk_len],
            next_page_id,
        ) catch |e| return mapOverflowPageError(e);
        pinned.markDirty();

        payload_offset += chunk_len;
        page_count += 1;
        current_page_id = next_page_id;
    }

    var payload_buf: [16]u8 = undefined;
    const payload_meta: OverflowChainRecordMeta = .{
        .first_page_id = first_page_id,
        .page_count = page_count,
        .payload_bytes = @intCast(payload.len),
    };
    const payload_len = encodeOverflowChainRecordMeta(payload_buf[0..], payload_meta);
    _ = wal.append(
        tx_id,
        .overflow_chain_create,
        first_page_id,
        payload_buf[0..payload_len],
    ) catch |e| return mapWalAppendError(e);

    return .{
        .first_page_id = first_page_id,
        .page_count = page_count,
        .payload_bytes = @intCast(payload.len),
    };
}

fn decodeRowWithOverflow(
    schema: *const RowSchema,
    row_data: []const u8,
    pool: *BufferPool,
    overflow_allocator: *const OverflowPageIdAllocator,
    string_arena: *scan_mod.StringArena,
    out: []Value,
) MutationError!void {
    std.debug.assert(out.len >= schema.column_count);
    var col_idx: u16 = 0;
    while (col_idx < schema.column_count) : (col_idx += 1) {
        const decoded = row_mod.decodeColumnStorageChecked(
            schema,
            row_data,
            col_idx,
        ) catch return error.Corruption;
        out[col_idx] = switch (decoded) {
            .value => |v| v,
            .string_overflow_page_id => |first_page_id| .{
                .string = try resolveOverflowStringIntoArena(
                    pool,
                    overflow_allocator,
                    first_page_id,
                    string_arena,
                ),
            },
        };
    }
}

fn resolveOverflowStringIntoArena(
    pool: *BufferPool,
    overflow_allocator: *const OverflowPageIdAllocator,
    first_page_id: u64,
    string_arena: *scan_mod.StringArena,
) MutationError![]const u8 {
    if (!overflow_allocator.ownsPageId(first_page_id)) return error.Corruption;
    const start = string_arena.startString();
    var current = first_page_id;
    var hops: u64 = 0;
    const max_hops = overflow_allocator.capacity();
    while (true) {
        if (hops >= max_hops) return error.Corruption;
        hops += 1;

        var pinned = try PinnedMutationPage.pin(pool, current);
        defer pinned.release();
        if (pinned.page.header.page_type != .overflow) return error.Corruption;

        const chunk = OverflowPage.readChunk(pinned.page) catch return error.Corruption;
        string_arena.appendChunk(chunk.payload) catch return error.OutOfMemory;
        if (chunk.next_page_id == OverflowPage.null_page_id) break;
        if (!overflow_allocator.ownsPageId(chunk.next_page_id)) return error.Corruption;
        current = chunk.next_page_id;
    }
    return string_arena.finishString(start);
}

fn collectOverflowRootsFromRow(
    schema: *const RowSchema,
    row_data: []const u8,
    out_overflow_roots: []u64,
) MutationError!usize {
    std.debug.assert(out_overflow_roots.len >= schema.column_count);
    var count: usize = 0;
    for (0..schema.column_count) |i| {
        const decoded = row_mod.decodeColumnStorageChecked(
            schema,
            row_data,
            @intCast(i),
        ) catch return error.Corruption;
        switch (decoded) {
            .value => {},
            .string_overflow_page_id => |first_page_id| {
                if (first_page_id == 0) return error.Corruption;
                var seen = false;
                for (0..count) |existing_idx| {
                    if (out_overflow_roots[existing_idx] == first_page_id) {
                        seen = true;
                        break;
                    }
                }
                if (seen) continue;
                if (count >= out_overflow_roots.len) return error.Corruption;
                out_overflow_roots[count] = first_page_id;
                count += 1;
            },
        }
    }
    return count;
}

fn appendOverflowRelinkWalForRow(
    wal: *Wal,
    tx_id: TxId,
    row_page_id: u64,
    new_overflow_page_ids: []const u64,
    old_first_page_id: u64,
) MutationError!void {
    for (new_overflow_page_ids) |new_first_page_id| {
        if (new_first_page_id == 0) continue;
        var payload_buf: [16]u8 = undefined;
        const payload_len = encodeOverflowRelinkRecordMeta(
            payload_buf[0..],
            .{
                .old_first_page_id = old_first_page_id,
                .new_first_page_id = new_first_page_id,
            },
        );
        _ = wal.append(
            tx_id,
            .overflow_chain_relink,
            row_page_id,
            payload_buf[0..payload_len],
        ) catch |e| return mapWalAppendError(e);
    }
}

fn enqueueOverflowChainForReclaim(
    catalog: *Catalog,
    wal: *Wal,
    tx_id: TxId,
    first_page_id: u64,
) MutationError!void {
    catalog.overflow_reclaim_queue.enqueue(tx_id, first_page_id) catch |e| {
        return switch (e) {
            error.InvalidChainRoot => error.Corruption,
            error.QueueFull => error.OverflowReclaimQueueFull,
            error.QueueEmpty => error.Corruption,
            error.DuplicateChainRoot => error.Corruption,
        };
    };
    catalog.recordOverflowReclaimEnqueue();
    var payload_buf: [16]u8 = undefined;
    const payload_len = encodeOverflowChainRecordMeta(
        payload_buf[0..],
        .{
            .first_page_id = first_page_id,
            .page_count = 0,
            .payload_bytes = 0,
        },
    );
    _ = wal.append(
        tx_id,
        .overflow_chain_unlink,
        first_page_id,
        payload_buf[0..payload_len],
    ) catch |e| return mapWalAppendError(e);
}

fn drainOverflowReclaimQueue(
    catalog: *Catalog,
    pool: *BufferPool,
    wal: *Wal,
    tx_id: TxId,
    max_items: usize,
) MutationError!void {
    var processed: usize = 0;
    while (processed < max_items and !catalog.overflow_reclaim_queue.isEmpty()) : (processed += 1) {
        const first_page_id = (catalog.overflow_reclaim_queue.dequeueCommitted() catch
            return error.Corruption) orelse break;
        catalog.recordOverflowReclaimDequeue();
        const page_count = reclaimOverflowChain(
            pool,
            &catalog.overflow_page_allocator,
            first_page_id,
        ) catch |err| {
            catalog.recordOverflowReclaimFailure();
            return err;
        };
        catalog.recordOverflowReclaimSuccess(page_count);
        var payload_buf: [16]u8 = undefined;
        const payload_len = encodeOverflowChainRecordMeta(
            payload_buf[0..],
            .{
                .first_page_id = first_page_id,
                .page_count = page_count,
                .payload_bytes = 0,
            },
        );
        _ = wal.append(
            tx_id,
            .overflow_chain_reclaim,
            first_page_id,
            payload_buf[0..payload_len],
        ) catch |e| return mapWalAppendError(e);
    }
}

pub fn commitOverflowReclaimEntriesForTx(
    catalog: *Catalog,
    pool: *BufferPool,
    wal: *Wal,
    tx_id: TxId,
    max_items: usize,
) MutationError!void {
    catalog.overflow_reclaim_queue.commitTx(tx_id);
    try drainOverflowReclaimQueue(catalog, pool, wal, tx_id, max_items);
}

pub fn rollbackOverflowReclaimEntriesForTx(
    catalog: *Catalog,
    tx_id: TxId,
) void {
    catalog.overflow_reclaim_queue.abortTx(tx_id);
}

fn reclaimOverflowChain(
    pool: *BufferPool,
    overflow_allocator: *const OverflowPageIdAllocator,
    first_page_id: u64,
) MutationError!u32 {
    if (!overflow_allocator.ownsPageId(first_page_id)) return error.Corruption;

    var page_count: u32 = 0;
    var hops: u64 = 0;
    const max_hops = overflow_allocator.capacity();
    var current = first_page_id;
    while (true) {
        if (hops >= max_hops) return error.Corruption;
        hops += 1;

        var pinned = try PinnedMutationPage.pin(pool, current);
        defer pinned.release();
        if (pinned.page.header.page_type != .overflow) return error.Corruption;

        const chunk = OverflowPage.readChunk(pinned.page) catch return error.Corruption;
        const next_page_id = chunk.next_page_id;
        if (next_page_id != OverflowPage.null_page_id and
            !overflow_allocator.ownsPageId(next_page_id))
        {
            return error.Corruption;
        }

        pinned.page.header.page_type = .free;
        @memset(&pinned.page.content, 0);
        pinned.markDirty();
        page_count += 1;

        if (next_page_id == OverflowPage.null_page_id) break;
        current = next_page_id;
    }
    return page_count;
}

fn encodeOverflowChainRecordMeta(
    out: []u8,
    meta: OverflowChainRecordMeta,
) usize {
    std.debug.assert(out.len >= 16);
    @memcpy(out[0..8], std.mem.asBytes(&std.mem.nativeToLittle(u64, meta.first_page_id)));
    @memcpy(out[8..12], std.mem.asBytes(&std.mem.nativeToLittle(u32, meta.page_count)));
    @memcpy(out[12..16], std.mem.asBytes(&std.mem.nativeToLittle(u32, meta.payload_bytes)));
    return 16;
}

fn decodeOverflowChainRecordMeta(payload: []const u8) MutationError!OverflowChainRecordMeta {
    if (payload.len != 16) return error.Corruption;
    return .{
        .first_page_id = std.mem.littleToNative(u64, std.mem.bytesAsValue(u64, payload[0..8]).*),
        .page_count = std.mem.littleToNative(u32, std.mem.bytesAsValue(u32, payload[8..12]).*),
        .payload_bytes = std.mem.littleToNative(u32, std.mem.bytesAsValue(u32, payload[12..16]).*),
    };
}

fn encodeOverflowRelinkRecordMeta(
    out: []u8,
    meta: OverflowRelinkRecordMeta,
) usize {
    std.debug.assert(out.len >= 16);
    @memcpy(out[0..8], std.mem.asBytes(&std.mem.nativeToLittle(u64, meta.old_first_page_id)));
    @memcpy(out[8..16], std.mem.asBytes(&std.mem.nativeToLittle(u64, meta.new_first_page_id)));
    return 16;
}

fn decodeOverflowRelinkRecordMeta(payload: []const u8) MutationError!OverflowRelinkRecordMeta {
    if (payload.len != 16) return error.Corruption;
    return .{
        .old_first_page_id = std.mem.littleToNative(u64, std.mem.bytesAsValue(u64, payload[0..8]).*),
        .new_first_page_id = std.mem.littleToNative(u64, std.mem.bytesAsValue(u64, payload[8..16]).*),
    };
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
    const source_model = &catalog.models[source_model_id];
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
        try deleteSingleRow(
            @constCast(catalog),
            pool,
            wal,
            undo_log,
            tx_id,
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

fn enforceInsertUniqueness(
    catalog: *const Catalog,
    pool: *BufferPool,
    model_id: ModelId,
    values: []const Value,
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

    var idx_id: u16 = 0;
    while (idx_id < model.index_count) : (idx_id += 1) {
        const idx = model.indexes[idx_id];
        if (!idx.is_unique or idx.column_count == 0) continue;
        if (uniqueKeyHasNull(values, idx.column_ids[0..idx.column_count])) continue;
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
    out_assigned: []bool,
) MutationError!void {
    std.debug.assert(out_values.len >= schema.column_count);
    std.debug.assert(out_assigned.len >= schema.column_count);

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

        const expected_type = schema.columns[col_idx].column_type;
        out_values[col_idx] = try coerceValueForColumn(val, expected_type);
        out_assigned[col_idx] = true;
        current = node.next;
    }
}

fn applyColumnDefaultsForInsert(
    catalog: *const Catalog,
    model_id: ModelId,
    values: []Value,
    assigned_columns: []const bool,
) void {
    std.debug.assert(model_id < catalog.model_count);
    const model = &catalog.models[model_id];
    std.debug.assert(values.len >= model.column_count);
    std.debug.assert(assigned_columns.len >= model.column_count);

    var col_id: u16 = 0;
    while (col_id < model.column_count) : (col_id += 1) {
        if (assigned_columns[col_id]) continue;
        const default_value = catalog.getColumnDefault(model_id, col_id) orelse continue;
        values[col_id] = default_value;
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

        const expected_type = schema.columns[col_idx].column_type;
        values[col_idx] = try coerceValueForColumn(val, expected_type);
        current = node.next;
    }
}

fn coerceValueForColumn(value: Value, target: ColumnType) MutationError!Value {
    if (value == .null_value) return value;
    if (value.columnType()) |actual| {
        if (actual == target) return value;
    }

    return switch (target) {
        .i8 => Value{ .i8 = try toI8(value) },
        .i16 => Value{ .i16 = try toI16(value) },
        .i32 => Value{ .i32 = try toI32(value) },
        .i64 => Value{ .i64 = try toI64(value) },
        .u8 => Value{ .u8 = try toU8(value) },
        .u16 => Value{ .u16 = try toU16(value) },
        .u32 => Value{ .u32 = try toU32(value) },
        .u64 => Value{ .u64 = try toU64(value) },
        .f64 => Value{ .f64 = try toF64(value) },
        .bool => if (value == .bool) value else error.TypeMismatch,
        .string => if (value == .string) value else error.TypeMismatch,
        .timestamp => Value{ .timestamp = try toI64(value) },
    };
}

fn toI8(value: Value) MutationError!i8 {
    const v = try toI64(value);
    return std.math.cast(i8, v) orelse error.TypeMismatch;
}

fn toI16(value: Value) MutationError!i16 {
    const v = try toI64(value);
    return std.math.cast(i16, v) orelse error.TypeMismatch;
}

fn toI32(value: Value) MutationError!i32 {
    const v = try toI64(value);
    return std.math.cast(i32, v) orelse error.TypeMismatch;
}

fn toI64(value: Value) MutationError!i64 {
    return switch (value) {
        .i8 => |v| v,
        .i16 => |v| v,
        .i32 => |v| v,
        .i64 => |v| v,
        .u8 => |v| v,
        .u16 => |v| v,
        .u32 => |v| v,
        .u64 => |v| std.math.cast(i64, v) orelse return error.TypeMismatch,
        else => error.TypeMismatch,
    };
}

fn toU8(value: Value) MutationError!u8 {
    const v = try toU64(value);
    return std.math.cast(u8, v) orelse error.TypeMismatch;
}

fn toU16(value: Value) MutationError!u16 {
    const v = try toU64(value);
    return std.math.cast(u16, v) orelse error.TypeMismatch;
}

fn toU32(value: Value) MutationError!u32 {
    const v = try toU64(value);
    return std.math.cast(u32, v) orelse error.TypeMismatch;
}

fn toU64(value: Value) MutationError!u64 {
    return switch (value) {
        .i8 => |v| std.math.cast(u64, v) orelse return error.TypeMismatch,
        .i16 => |v| std.math.cast(u64, v) orelse return error.TypeMismatch,
        .i32 => |v| std.math.cast(u64, v) orelse return error.TypeMismatch,
        .i64 => |v| std.math.cast(u64, v) orelse return error.TypeMismatch,
        .u8 => |v| v,
        .u16 => |v| v,
        .u32 => |v| v,
        .u64 => |v| v,
        else => error.TypeMismatch,
    };
}

fn toF64(value: Value) MutationError!f64 {
    return switch (value) {
        .i8 => |v| @floatFromInt(v),
        .i16 => |v| @floatFromInt(v),
        .i32 => |v| @floatFromInt(v),
        .i64 => |v| @floatFromInt(v),
        .u8 => |v| @floatFromInt(v),
        .u16 => |v| @floatFromInt(v),
        .u32 => |v| @floatFromInt(v),
        .u64 => |v| @floatFromInt(v),
        .f64 => |v| v,
        else => error.TypeMismatch,
    };
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

fn mapOverflowAllocatorError(err: overflow_mod.OverflowAllocatorError) MutationError {
    return switch (err) {
        error.InvalidRegion => error.Corruption,
        error.RegionExhausted => error.OverflowRegionExhausted,
    };
}

fn mapOverflowPageError(err: overflow_mod.OverflowError) MutationError {
    return switch (err) {
        error.PageFull => error.RowTooLarge,
        error.InvalidPageFormat => error.Corruption,
        error.UnsupportedPageVersion => error.Corruption,
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
        _ = try self.catalog.addColumn(self.model_id, "id", .i64, false);
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
    try testing.expectEqual(@as(i64, 1), result.rows[0].values[0].i64);
    try testing.expectEqualSlices(
        u8,
        "Alice",
        result.rows[0].values[1].string,
    );
}

test "insert spills oversized string and scan resolves overflow payload" {
    var env: TestEnv = undefined;
    try env.init();
    defer env.deinit();
    env.catalog.overflow_page_allocator = try overflow_mod.PageIdAllocator.initWithBounds(2_000, 4);

    const tx = try env.tm.begin();
    var snap = try env.tm.snapshot(tx);
    defer snap.deinit();

    var name_buf: [1200]u8 = undefined;
    @memset(name_buf[0..], 'x');
    var src_buf: [1400]u8 = undefined;
    const src = try std.fmt.bufPrint(
        src_buf[0..],
        "User |> insert(id = 1, name = \"{s}\")",
        .{name_buf[0..]},
    );
    const tok = tokenizer_mod.tokenize(src);
    const parsed = parser_mod.parse(&tok, src);
    std.debug.assert(!parsed.has_error);
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
    try testing.expectEqualSlices(u8, name_buf[0..], result.rows[0].values[1].string);
}

test "insert fails deterministically when overflow region is exhausted" {
    var env: TestEnv = undefined;
    try env.init();
    defer env.deinit();
    env.catalog.overflow_page_allocator = try overflow_mod.PageIdAllocator.initWithBounds(3_000, 1);

    const tx = try env.tm.begin();
    var name_buf: [1200]u8 = undefined;
    @memset(name_buf[0..], 'y');

    var src1_buf: [1400]u8 = undefined;
    const src1 = try std.fmt.bufPrint(
        src1_buf[0..],
        "User |> insert(id = 1, name = \"{s}\")",
        .{name_buf[0..]},
    );
    const tok1 = tokenizer_mod.tokenize(src1);
    const p1 = parser_mod.parse(&tok1, src1);
    std.debug.assert(!p1.has_error);
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

    var src2_buf: [1400]u8 = undefined;
    const src2 = try std.fmt.bufPrint(
        src2_buf[0..],
        "User |> insert(id = 2, name = \"{s}\")",
        .{name_buf[0..]},
    );
    const tok2 = tokenizer_mod.tokenize(src2);
    const p2 = parser_mod.parse(&tok2, src2);
    std.debug.assert(!p2.has_error);
    const r2 = p2.ast.getNode(p2.ast.root);
    const pipe2 = p2.ast.getNode(r2.data.unary);
    const ins2 = p2.ast.getNode(pipe2.data.binary.rhs);
    try testing.expectError(
        error.OverflowRegionExhausted,
        executeInsert(
            &env.catalog,
            &env.pool,
            &env.wal,
            tx,
            env.model_id,
            &p2.ast,
            &tok2,
            src2,
            ins2.data.unary,
        ),
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

test "update spills oversized string and read resolves overflow payload" {
    var env: TestEnv = undefined;
    try env.init();
    defer env.deinit();
    env.catalog.overflow_page_allocator = try overflow_mod.PageIdAllocator.initWithBounds(4_000, 4);

    const tx = try env.tm.begin();
    var snap = try env.tm.snapshot(tx);
    defer snap.deinit();

    const src1 = "User |> insert(id = 1, name = \"short\")";
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

    var long_name: [1200]u8 = undefined;
    @memset(long_name[0..], 'z');
    var src2_buf: [1500]u8 = undefined;
    const src2 = try std.fmt.bufPrint(
        src2_buf[0..],
        "User |> where(id = 1) |> update(name = \"{s}\")",
        .{long_name[0..]},
    );
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
    try testing.expectEqualSlices(u8, long_name[0..], result.rows[0].values[1].string);
}

test "overflow WAL lifecycle is deterministic for replace path" {
    var env: TestEnv = undefined;
    try env.init();
    defer env.deinit();
    env.catalog.overflow_page_allocator = try overflow_mod.PageIdAllocator.initWithBounds(6_000, 16);

    const tx = try env.tm.begin();
    var snap = try env.tm.snapshot(tx);
    defer snap.deinit();

    var first_name: [1200]u8 = undefined;
    @memset(first_name[0..], 'a');
    var insert_src_buf: [1500]u8 = undefined;
    const insert_src = try std.fmt.bufPrint(
        insert_src_buf[0..],
        "User |> insert(id = 1, name = \"{s}\")",
        .{first_name[0..]},
    );
    const insert_tok = tokenizer_mod.tokenize(insert_src);
    const insert_parsed = parser_mod.parse(&insert_tok, insert_src);
    const insert_root = insert_parsed.ast.getNode(insert_parsed.ast.root);
    const insert_pipeline = insert_parsed.ast.getNode(insert_root.data.unary);
    const insert_op = insert_parsed.ast.getNode(insert_pipeline.data.binary.rhs);
    _ = try executeInsert(
        &env.catalog,
        &env.pool,
        &env.wal,
        tx,
        env.model_id,
        &insert_parsed.ast,
        &insert_tok,
        insert_src,
        insert_op.data.unary,
    );

    var second_name: [1200]u8 = undefined;
    @memset(second_name[0..], 'b');
    var update_src_buf: [1600]u8 = undefined;
    const update_src = try std.fmt.bufPrint(
        update_src_buf[0..],
        "User |> where(id = 1) |> update(name = \"{s}\")",
        .{second_name[0..]},
    );
    const update_tok = tokenizer_mod.tokenize(update_src);
    const update_parsed = parser_mod.parse(&update_tok, update_src);
    const update_root = update_parsed.ast.getNode(update_parsed.ast.root);
    const update_pipeline = update_parsed.ast.getNode(update_root.data.unary);
    const update_where = update_parsed.ast.getNode(update_pipeline.data.binary.rhs);
    const update_op = update_parsed.ast.getNode(update_where.next);
    _ = try executeUpdate(
        &env.catalog,
        &env.pool,
        &env.wal,
        &env.undo_log,
        tx,
        &snap,
        &env.tm,
        env.model_id,
        &update_parsed.ast,
        &update_tok,
        update_src,
        update_where.data.unary,
        update_op.data.unary,
        testing.allocator,
    );
    try commitOverflowReclaimEntriesForTx(
        &env.catalog,
        &env.pool,
        &env.wal,
        tx,
        1,
    );

    try env.wal.flush();

    var records_buf: [64]wal_mod.Record = undefined;
    var payload_buf: [16 * 1024]u8 = undefined;
    const decoded = try env.wal.readFromInto(1, &records_buf, &payload_buf);
    const records = records_buf[0..decoded.records_len];

    var overflow_types: [8]wal_mod.RecordType = undefined;
    var overflow_type_count: usize = 0;
    var created_chain: u64 = 0;
    var unlinked_chain: u64 = 0;
    var reclaimed_chain: u64 = 0;
    for (records) |rec| {
        switch (rec.record_type) {
            .overflow_chain_create, .overflow_chain_relink, .overflow_chain_unlink, .overflow_chain_reclaim => {
                overflow_types[overflow_type_count] = rec.record_type;
                overflow_type_count += 1;
            },
            else => continue,
        }
        if (rec.record_type == .overflow_chain_create and created_chain == 0) {
            const meta = try decodeOverflowChainRecordMeta(rec.payload);
            created_chain = meta.first_page_id;
        }
        if (rec.record_type == .overflow_chain_unlink) {
            const meta = try decodeOverflowChainRecordMeta(rec.payload);
            unlinked_chain = meta.first_page_id;
        }
        if (rec.record_type == .overflow_chain_reclaim) {
            const meta = try decodeOverflowChainRecordMeta(rec.payload);
            reclaimed_chain = meta.first_page_id;
            try testing.expect(meta.page_count > 0);
        }
    }
    try testing.expectEqual(@as(usize, 6), overflow_type_count);
    try testing.expectEqual(wal_mod.RecordType.overflow_chain_create, overflow_types[0]);
    try testing.expectEqual(wal_mod.RecordType.overflow_chain_relink, overflow_types[1]);
    try testing.expectEqual(wal_mod.RecordType.overflow_chain_create, overflow_types[2]);
    try testing.expectEqual(wal_mod.RecordType.overflow_chain_relink, overflow_types[3]);
    try testing.expectEqual(wal_mod.RecordType.overflow_chain_unlink, overflow_types[4]);
    try testing.expectEqual(wal_mod.RecordType.overflow_chain_reclaim, overflow_types[5]);
    try testing.expectEqual(created_chain, unlinked_chain);
    try testing.expectEqual(created_chain, reclaimed_chain);
    try testing.expect(env.catalog.overflow_reclaim_queue.isEmpty());
}

test "overflow WAL lifecycle includes unlink and reclaim on delete" {
    var env: TestEnv = undefined;
    try env.init();
    defer env.deinit();
    env.catalog.overflow_page_allocator = try overflow_mod.PageIdAllocator.initWithBounds(7_000, 8);

    const tx = try env.tm.begin();
    var snap = try env.tm.snapshot(tx);
    defer snap.deinit();

    var long_name: [1200]u8 = undefined;
    @memset(long_name[0..], 'd');
    var insert_src_buf: [1500]u8 = undefined;
    const insert_src = try std.fmt.bufPrint(
        insert_src_buf[0..],
        "User |> insert(id = 1, name = \"{s}\")",
        .{long_name[0..]},
    );
    const insert_tok = tokenizer_mod.tokenize(insert_src);
    const insert_parsed = parser_mod.parse(&insert_tok, insert_src);
    const insert_root = insert_parsed.ast.getNode(insert_parsed.ast.root);
    const insert_pipeline = insert_parsed.ast.getNode(insert_root.data.unary);
    const insert_op = insert_parsed.ast.getNode(insert_pipeline.data.binary.rhs);
    _ = try executeInsert(
        &env.catalog,
        &env.pool,
        &env.wal,
        tx,
        env.model_id,
        &insert_parsed.ast,
        &insert_tok,
        insert_src,
        insert_op.data.unary,
    );

    const delete_src = "User |> where(id = 1) |> delete";
    const delete_tok = tokenizer_mod.tokenize(delete_src);
    const delete_parsed = parser_mod.parse(&delete_tok, delete_src);
    const delete_root = delete_parsed.ast.getNode(delete_parsed.ast.root);
    const delete_pipeline = delete_parsed.ast.getNode(delete_root.data.unary);
    const delete_where = delete_parsed.ast.getNode(delete_pipeline.data.binary.rhs);
    _ = try executeDelete(
        &env.catalog,
        &env.pool,
        &env.wal,
        &env.undo_log,
        tx,
        &snap,
        &env.tm,
        env.model_id,
        &delete_parsed.ast,
        &delete_tok,
        delete_src,
        delete_where.data.unary,
        testing.allocator,
    );
    try commitOverflowReclaimEntriesForTx(
        &env.catalog,
        &env.pool,
        &env.wal,
        tx,
        1,
    );

    try env.wal.flush();

    var records_buf: [32]wal_mod.Record = undefined;
    var payload_buf: [8 * 1024]u8 = undefined;
    const decoded = try env.wal.readFromInto(1, &records_buf, &payload_buf);
    const records = records_buf[0..decoded.records_len];
    var overflow_types: [6]wal_mod.RecordType = undefined;
    var overflow_type_count: usize = 0;
    for (records) |rec| {
        switch (rec.record_type) {
            .overflow_chain_create, .overflow_chain_relink, .overflow_chain_unlink, .overflow_chain_reclaim => {
                overflow_types[overflow_type_count] = rec.record_type;
                overflow_type_count += 1;
            },
            else => {},
        }
    }
    try testing.expectEqual(@as(usize, 4), overflow_type_count);
    try testing.expectEqual(wal_mod.RecordType.overflow_chain_create, overflow_types[0]);
    try testing.expectEqual(wal_mod.RecordType.overflow_chain_relink, overflow_types[1]);
    try testing.expectEqual(wal_mod.RecordType.overflow_chain_unlink, overflow_types[2]);
    try testing.expectEqual(wal_mod.RecordType.overflow_chain_reclaim, overflow_types[3]);
    try testing.expect(env.catalog.overflow_reclaim_queue.isEmpty());
}

test "overflow lifecycle WAL records survive crash and restart recovery" {
    var env: TestEnv = undefined;
    try env.init();
    defer env.deinit();
    env.catalog.overflow_page_allocator = try overflow_mod.PageIdAllocator.initWithBounds(8_000, 24);

    const tx = try env.tm.begin();
    var snap = try env.tm.snapshot(tx);
    defer snap.deinit();

    var name_a: [1200]u8 = undefined;
    @memset(name_a[0..], 'k');
    var insert_src_buf: [1500]u8 = undefined;
    const insert_src = try std.fmt.bufPrint(
        insert_src_buf[0..],
        "User |> insert(id = 1, name = \"{s}\")",
        .{name_a[0..]},
    );
    const insert_tok = tokenizer_mod.tokenize(insert_src);
    const insert_parsed = parser_mod.parse(&insert_tok, insert_src);
    const insert_root = insert_parsed.ast.getNode(insert_parsed.ast.root);
    const insert_pipeline = insert_parsed.ast.getNode(insert_root.data.unary);
    const insert_op = insert_parsed.ast.getNode(insert_pipeline.data.binary.rhs);
    _ = try executeInsert(
        &env.catalog,
        &env.pool,
        &env.wal,
        tx,
        env.model_id,
        &insert_parsed.ast,
        &insert_tok,
        insert_src,
        insert_op.data.unary,
    );

    var name_b: [1200]u8 = undefined;
    @memset(name_b[0..], 'm');
    var update_src_buf: [1600]u8 = undefined;
    const update_src = try std.fmt.bufPrint(
        update_src_buf[0..],
        "User |> where(id = 1) |> update(name = \"{s}\")",
        .{name_b[0..]},
    );
    const update_tok = tokenizer_mod.tokenize(update_src);
    const update_parsed = parser_mod.parse(&update_tok, update_src);
    const update_root = update_parsed.ast.getNode(update_parsed.ast.root);
    const update_pipeline = update_parsed.ast.getNode(update_root.data.unary);
    const update_where = update_parsed.ast.getNode(update_pipeline.data.binary.rhs);
    const update_op = update_parsed.ast.getNode(update_where.next);
    _ = try executeUpdate(
        &env.catalog,
        &env.pool,
        &env.wal,
        &env.undo_log,
        tx,
        &snap,
        &env.tm,
        env.model_id,
        &update_parsed.ast,
        &update_tok,
        update_src,
        update_where.data.unary,
        update_op.data.unary,
        testing.allocator,
    );

    const delete_src = "User |> where(id = 1) |> delete";
    const delete_tok = tokenizer_mod.tokenize(delete_src);
    const delete_parsed = parser_mod.parse(&delete_tok, delete_src);
    const delete_root = delete_parsed.ast.getNode(delete_parsed.ast.root);
    const delete_pipeline = delete_parsed.ast.getNode(delete_root.data.unary);
    const delete_where = delete_parsed.ast.getNode(delete_pipeline.data.binary.rhs);
    _ = try executeDelete(
        &env.catalog,
        &env.pool,
        &env.wal,
        &env.undo_log,
        tx,
        &snap,
        &env.tm,
        env.model_id,
        &delete_parsed.ast,
        &delete_tok,
        delete_src,
        delete_where.data.unary,
        testing.allocator,
    );
    try commitOverflowReclaimEntriesForTx(
        &env.catalog,
        &env.pool,
        &env.wal,
        tx,
        2,
    );

    try env.wal.flush();
    env.disk.crash();

    var recovered = Wal.init(testing.allocator, env.disk.storage());
    defer recovered.deinit();
    try recovered.recover();

    var records_buf: [64]wal_mod.Record = undefined;
    var payload_buf: [16 * 1024]u8 = undefined;
    const decoded = try recovered.readFromInto(1, &records_buf, &payload_buf);
    const records = records_buf[0..decoded.records_len];

    var creates: usize = 0;
    var relinks: usize = 0;
    var unlinks: usize = 0;
    var reclaims: usize = 0;
    for (records) |rec| {
        switch (rec.record_type) {
            .overflow_chain_create => {
                _ = try decodeOverflowChainRecordMeta(rec.payload);
                creates += 1;
            },
            .overflow_chain_relink => {
                const meta = try decodeOverflowRelinkRecordMeta(rec.payload);
                try testing.expect(meta.new_first_page_id != 0);
                relinks += 1;
            },
            .overflow_chain_unlink => {
                _ = try decodeOverflowChainRecordMeta(rec.payload);
                unlinks += 1;
            },
            .overflow_chain_reclaim => {
                const meta = try decodeOverflowChainRecordMeta(rec.payload);
                try testing.expect(meta.page_count > 0);
                reclaims += 1;
            },
            else => {},
        }
    }
    try testing.expectEqual(@as(usize, 2), creates);
    try testing.expectEqual(@as(usize, 2), relinks);
    try testing.expectEqual(@as(usize, 2), unlinks);
    try testing.expectEqual(@as(usize, 2), reclaims);
}

test "reclaim drain fails closed on cyclic overflow chain corruption" {
    var env: TestEnv = undefined;
    try env.init();
    defer env.deinit();
    env.catalog.overflow_page_allocator = try overflow_mod.PageIdAllocator.initWithBounds(9_000, 2);

    {
        var first = try PinnedMutationPage.pin(&env.pool, 9_000);
        defer first.release();
        OverflowPage.init(first.page);
        try OverflowPage.writeChunk(first.page, "x", 9_001);
        first.markDirty();
    }
    {
        var second = try PinnedMutationPage.pin(&env.pool, 9_001);
        defer second.release();
        OverflowPage.init(second.page);
        try OverflowPage.writeChunk(second.page, "y", 9_000);
        second.markDirty();
    }

    const tx = try env.tm.begin();
    try env.catalog.overflow_reclaim_queue.enqueue(tx, 9_000);
    env.catalog.overflow_reclaim_queue.commitTx(tx);
    try testing.expectError(
        error.Corruption,
        drainOverflowReclaimQueue(&env.catalog, &env.pool, &env.wal, tx, 1),
    );
}

test "reclaim queue preserves ordering across tx rollback and commit" {
    var env: TestEnv = undefined;
    try env.init();
    defer env.deinit();
    env.catalog.overflow_page_allocator = try overflow_mod.PageIdAllocator.initWithBounds(15_000, 8);

    {
        var first = try PinnedMutationPage.pin(&env.pool, 15_000);
        defer first.release();
        OverflowPage.init(first.page);
        try OverflowPage.writeChunk(first.page, "a", OverflowPage.null_page_id);
        first.markDirty();
    }
    {
        var second = try PinnedMutationPage.pin(&env.pool, 15_001);
        defer second.release();
        OverflowPage.init(second.page);
        try OverflowPage.writeChunk(second.page, "b", OverflowPage.null_page_id);
        second.markDirty();
    }

    const tx_a: u64 = 41;
    const tx_b: u64 = 42;
    try env.catalog.overflow_reclaim_queue.enqueue(tx_a, 15_000);
    try env.catalog.overflow_reclaim_queue.enqueue(tx_b, 15_001);

    try commitOverflowReclaimEntriesForTx(
        &env.catalog,
        &env.pool,
        &env.wal,
        tx_b,
        1,
    );
    {
        const page = try env.pool.pin(15_001);
        defer env.pool.unpin(15_001, false);
        try testing.expectEqual(page_mod.PageType.overflow, page.header.page_type);
    }

    rollbackOverflowReclaimEntriesForTx(&env.catalog, tx_a);
    try commitOverflowReclaimEntriesForTx(
        &env.catalog,
        &env.pool,
        &env.wal,
        tx_b,
        1,
    );

    {
        const page = try env.pool.pin(15_000);
        defer env.pool.unpin(15_000, false);
        try testing.expectEqual(page_mod.PageType.overflow, page.header.page_type);
    }
    {
        const page = try env.pool.pin(15_001);
        defer env.pool.unpin(15_001, false);
        try testing.expectEqual(page_mod.PageType.free, page.header.page_type);
    }
    try testing.expect(env.catalog.overflow_reclaim_queue.isEmpty());
}

test "abort rollback prevents reclaim of live overflow chain" {
    var env: TestEnv = undefined;
    try env.init();
    defer env.deinit();
    env.catalog.overflow_page_allocator = try overflow_mod.PageIdAllocator.initWithBounds(16_000, 8);

    {
        var page = try PinnedMutationPage.pin(&env.pool, 16_000);
        defer page.release();
        OverflowPage.init(page.page);
        try OverflowPage.writeChunk(page.page, "live", OverflowPage.null_page_id);
        page.markDirty();
    }

    const aborted_tx: u64 = 51;
    try env.catalog.overflow_reclaim_queue.enqueue(aborted_tx, 16_000);
    rollbackOverflowReclaimEntriesForTx(&env.catalog, aborted_tx);

    try commitOverflowReclaimEntriesForTx(
        &env.catalog,
        &env.pool,
        &env.wal,
        52,
        1,
    );

    const page = try env.pool.pin(16_000);
    defer env.pool.unpin(16_000, false);
    try testing.expectEqual(page_mod.PageType.overflow, page.header.page_type);
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
        _ = try self.catalog.addColumn(self.user_model_id, "id", .i64, false);
        self.catalog.setColumnPrimaryKey(self.user_model_id, 0);
        self.catalog.models[self.user_model_id].heap_first_page_id = 100;
        self.catalog.models[self.user_model_id].total_pages = 1;
        const user_page = try self.pool.pin(100);
        HeapPage.init(user_page);
        self.pool.unpin(100, true);

        self.post_model_id = try self.catalog.addModel("Post");
        _ = try self.catalog.addColumn(self.post_model_id, "id", .i64, false);
        _ = try self.catalog.addColumn(self.post_model_id, "user_id", .i64, true);
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
    try testing.expectEqual(@as(i64, 2), post_rows.rows[0].values[1].i64);
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
