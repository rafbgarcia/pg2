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
const expression_mod = @import("../parser/expression.zig");
const tokenizer_mod = @import("../parser/tokenizer.zig");
const page_mod = @import("../storage/page.zig");
const heap_mod = @import("../storage/heap.zig");
const buffer_pool_mod = @import("../storage/buffer_pool.zig");
const wal_mod = @import("../storage/wal.zig");
const row_mod = @import("../storage/row.zig");
const overflow_mod = @import("../storage/overflow.zig");
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
const OverflowPage = overflow_mod.OverflowPage;
const OverflowPageIdAllocator = overflow_mod.PageIdAllocator;
const Catalog = catalog_mod.Catalog;
const ModelId = catalog_mod.ModelId;
const TxId = tx_mod.TxId;
const Snapshot = tx_mod.Snapshot;
const TxManager = tx_mod.TxManager;
const UndoLog = undo_mod.UndoLog;
const ResultRow = scan_mod.ResultRow;
const ParameterBinding = filter_mod.ParameterBinding;
const ParameterResolver = filter_mod.ParameterResolver;

// --- Extracted submodules ---
const overflow_chains_mod = @import("overflow_chains.zig");
const referential_integrity_mod = @import("referential_integrity.zig");
const value_builder_mod = @import("value_builder.zig");
const constraints_mod = @import("constraints.zig");

// Overflow chains delegation
const OverflowChainStats = overflow_chains_mod.OverflowChainStats;
const markOversizedStringSlots = overflow_chains_mod.markOversizedStringSlots;
const spillOversizedStrings = overflow_chains_mod.spillOversizedStrings;
const decodeRowWithOverflow = overflow_chains_mod.decodeRowWithOverflow;
const collectOverflowRootsFromRow = overflow_chains_mod.collectOverflowRootsFromRow;
const appendOverflowRelinkWalForRow = overflow_chains_mod.appendOverflowRelinkWalForRow;
const enqueueOverflowChainForReclaim = overflow_chains_mod.enqueueOverflowChainForReclaim;
const decodeOverflowChainRecordMeta = overflow_chains_mod.decodeOverflowChainRecordMeta;
const decodeOverflowRelinkRecordMeta = overflow_chains_mod.decodeOverflowRelinkRecordMeta;
const drainOverflowReclaimQueue = overflow_chains_mod.drainOverflowReclaimQueue;
pub const commitOverflowReclaimEntriesForTx = overflow_chains_mod.commitOverflowReclaimEntriesForTx;
pub const rollbackOverflowReclaimEntriesForTx = overflow_chains_mod.rollbackOverflowReclaimEntriesForTx;

pub fn commitSlotReclaimEntriesForTx(
    catalog: *Catalog,
    pool: *BufferPool,
    wal: *Wal,
    tx_id: TxId,
    oldest_active: TxId,
    max_items: usize,
) MutationError!void {
    catalog.slot_reclaim_queue.commitTx(tx_id);
    catalog.index_reclaim_queue.commitTx(tx_id);
    try drainSlotReclaimQueue(catalog, pool, wal, tx_id, oldest_active, max_items);
}

pub fn rollbackSlotReclaimEntriesForTx(
    catalog: *Catalog,
    tx_id: TxId,
) void {
    catalog.slot_reclaim_queue.abortTx(tx_id);
    catalog.index_reclaim_queue.abortTx(tx_id);
}

// Referential integrity delegation
const enforceOutgoingReferentialIntegrity = referential_integrity_mod.enforceOutgoingReferentialIntegrity;
const enforceIncomingDeleteReferentialIntegrity = referential_integrity_mod.enforceIncomingDeleteReferentialIntegrity;
const enforceIncomingUpdateReferentialIntegrity = referential_integrity_mod.enforceIncomingUpdateReferentialIntegrity;

// Constraints delegation
const enforceInsertUniqueness = constraints_mod.enforceInsertUniqueness;
const enforceNonPkUniqueness = constraints_mod.enforceNonPkUniqueness;

// Index maintenance delegation
const index_maintenance_mod = @import("index_maintenance.zig");
const openPrimaryKeyIndex = index_maintenance_mod.openPrimaryKeyIndex;
const insertPrimaryKey = index_maintenance_mod.insertPrimaryKey;
const insertPrimaryKeyWithHintNoSync = index_maintenance_mod.insertPrimaryKeyWithHintNoSync;
const primaryKeyExists = index_maintenance_mod.primaryKeyExists;
const primaryKeyVisibleInIndex = index_maintenance_mod.primaryKeyVisibleInIndex;
const openIndex = index_maintenance_mod.openIndex;
const insertIndexKey = index_maintenance_mod.insertIndexKey;
const insertIndexKeyWithHintNoSync = index_maintenance_mod.insertIndexKeyWithHintNoSync;
const syncIndexBTreeState = index_maintenance_mod.syncIndexBTreeState;
const LeafHint = index_maintenance_mod.LeafHint;
const index_key_mod = @import("../storage/index_key.zig");

// Value builder delegation
const ParameterBindingContext = value_builder_mod.ParameterBindingContext;
const resolveParameterBinding = value_builder_mod.resolveParameterBinding;
pub const buildRowFromAssignments = value_builder_mod.buildRowFromAssignments;
const applyColumnDefaultsForInsert = value_builder_mod.applyColumnDefaultsForInsert;
const applyAssignments = value_builder_mod.applyAssignments;
const coerceValueForColumn = value_builder_mod.coerceValueForColumn;

/// Maximum encoded row size.
pub const max_row_buf_size = 8000;
/// Maximum number of field assignments in a single mutation.
pub const max_assignments = 128;

inline fn shouldSkipUniqueIndexKey(value: Value) bool {
    return value == .null_value;
}

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
    NumericDomain,
    NullArithmeticOperand,
    UnknownFunction,
    NullInPredicate,
    UndefinedParameter,
    ClockUnavailable,
    ResultOverflow,
    ReturningBufferExhausted,
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
    PredicateMustBeBoolean,
    PredicateUndefinedParameter,
    PredicateClockUnavailable,
};

pub const MutationDiagnosticCode = enum {
    IntegerOutOfRange,
    TypeMismatch,
    NullArithmeticOperand,
    ColumnNotFound,
    NumericOverflow,
};

pub const MutationDiagnostic = struct {
    has_value: bool = false,
    code: MutationDiagnosticCode = .TypeMismatch,
    field_token: ?u16 = null,
    location_token: ?u16 = null,
    message: [220]u8 = std.mem.zeroes([220]u8),

    pub fn messageSlice(self: *const MutationDiagnostic) []const u8 {
        const len = std.mem.indexOfScalar(u8, &self.message, 0) orelse self.message.len;
        return self.message[0..len];
    }
};

pub const PinnedMutationPage = struct {
    pool: *BufferPool,
    page_id: u64,
    page: *Page,
    dirty: bool = false,
    active: bool = true,

    pub fn pin(pool: *BufferPool, page_id: u64) MutationError!PinnedMutationPage {
        const page = pool.pin(page_id) catch |e| return mapPoolError(e);
        return .{
            .pool = pool,
            .page_id = page_id,
            .page = page,
        };
    }

    pub fn markDirty(self: *PinnedMutationPage) void {
        self.dirty = true;
    }

    pub fn release(self: *PinnedMutationPage) void {
        if (!self.active) return;
        self.pool.unpin(self.page_id, self.dirty);
        self.active = false;
    }
};

/// Optional bounded row capture sink for mutation RETURNING semantics.
/// Rows are appended in mutation scan order into caller-owned storage.
pub const ReturningCapture = struct {
    rows: []ResultRow,
    row_count: *u16,
    string_arena: *scan_mod.StringArena,
};

const UniqueHashEntry = struct {
    row_group: NodeIndex,
    hash: u64,
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
    return executeInsertWithDiagnostic(
        catalog,
        pool,
        wal,
        tx_id,
        model_id,
        tree,
        tokens,
        source,
        first_assignment_node,
        null,
    );
}

pub fn executeInsertWithDiagnostic(
    catalog: *Catalog,
    pool: *BufferPool,
    wal: *Wal,
    tx_id: TxId,
    model_id: ModelId,
    tree: *const Ast,
    tokens: *const TokenizeResult,
    source: []const u8,
    first_assignment_node: NodeIndex,
    diagnostic: ?*MutationDiagnostic,
) MutationError!RowId {
    return executeInsertWithDiagnosticAndParameters(
        catalog,
        pool,
        wal,
        tx_id,
        model_id,
        tree,
        tokens,
        source,
        first_assignment_node,
        &.{},
        diagnostic,
        null,
        null,
        null,
        null,
    );
}

pub fn executeInsertWithDiagnosticAndParameters(
    catalog: *Catalog,
    pool: *BufferPool,
    wal: *Wal,
    tx_id: TxId,
    model_id: ModelId,
    tree: *const Ast,
    tokens: *const TokenizeResult,
    source: []const u8,
    first_assignment_node: NodeIndex,
    parameter_bindings: []const ParameterBinding,
    diagnostic: ?*MutationDiagnostic,
    eval_ctx: *const filter_mod.EvalContext,
    undo_log: ?*const UndoLog,
    snapshot: ?*const Snapshot,
    tx_manager: ?*const TxManager,
) MutationError!RowId {
    return executeInsertWithDiagnosticAndParametersWithOptions(
        catalog,
        pool,
        wal,
        tx_id,
        model_id,
        tree,
        tokens,
        source,
        first_assignment_node,
        parameter_bindings,
        diagnostic,
        eval_ctx,
        undo_log,
        snapshot,
        tx_manager,
        false,
        null,
    );
}

fn executeInsertWithDiagnosticAndParametersWithOptions(
    catalog: *Catalog,
    pool: *BufferPool,
    wal: *Wal,
    tx_id: TxId,
    model_id: ModelId,
    tree: *const Ast,
    tokens: *const TokenizeResult,
    source: []const u8,
    first_assignment_node: NodeIndex,
    parameter_bindings: []const ParameterBinding,
    diagnostic: ?*MutationDiagnostic,
    eval_ctx: *const filter_mod.EvalContext,
    undo_log: ?*const UndoLog,
    snapshot: ?*const Snapshot,
    tx_manager: ?*const TxManager,
    skip_index_writes: bool,
    page_hint: ?*u64,
) MutationError!RowId {
    std.debug.assert(model_id < catalog.model_count);
    const model = &catalog.models[model_id];
    const schema = &model.row_schema;
    std.debug.assert(schema.column_count > 0);
    var string_decode_bytes: [max_row_buf_size]u8 = undefined;
    var string_arena = scan_mod.StringArena.init(string_decode_bytes[0..]);

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
        parameter_bindings,
        &values,
        &assigned_columns,
        diagnostic,
        &string_arena,
        eval_ctx,
    );
    applyColumnDefaultsForInsert(
        catalog,
        model_id,
        values[0..schema.column_count],
        assigned_columns[0..schema.column_count],
    );
    // PK uniqueness: use B+ tree O(log n) lookup when available, else heap scan.
    var pk_btree = openPrimaryKeyIndex(catalog, pool, wal, model_id);
    if (pk_btree != null) {
        const pk_col = catalog_mod.findPrimaryKeyColumnId(catalog, model_id).?;
        if (try primaryKeyVisibleInIndex(catalog, &pk_btree.?, model_id, values[pk_col], pool, undo_log, snapshot, tx_manager)) {
            return error.DuplicateKey;
        }
        // Non-PK unique indexes still need heap scan.
        try enforceNonPkUniqueness(catalog, pool, wal, model_id, values[0..schema.column_count], undo_log, snapshot, tx_manager);
    } else {
        try enforceInsertUniqueness(catalog, pool, wal, model_id, values[0..schema.column_count], undo_log, snapshot, tx_manager);
    }
    try enforceOutgoingReferentialIntegrity(
        catalog,
        pool,
        wal,
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
        page_hint,
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

    if (!skip_index_writes) {
        // Insert into PK B+ tree index after successful heap write.
        if (pk_btree) |*btree| {
            const pk_col = catalog_mod.findPrimaryKeyColumnId(catalog, model_id).?;
            try insertPrimaryKey(catalog, btree, model_id, values[pk_col], result);
        }

        // Insert into non-PK unique indexes after successful heap write.
        {
            const pk_col = catalog_mod.findPrimaryKeyColumnId(catalog, model_id);
            var idx_id: u16 = 0;
            while (idx_id < model.index_count) : (idx_id += 1) {
                const idx = &model.indexes[idx_id];
                if (!idx.is_unique) continue;
                if (idx.column_count != 1) continue;
                if (idx.btree_root_page_id == 0) continue;
                // Skip PK index — already handled above.
                if (pk_col != null and idx.column_ids[0] == pk_col.?) continue;
                // Nullable unique keys with NULL do not participate in uniqueness.
                if (shouldSkipUniqueIndexKey(values[idx.column_ids[0]])) continue;
                var idx_btree = openIndex(catalog, pool, wal, model_id, idx_id) orelse continue;
                try insertIndexKey(catalog, &idx_btree, model_id, idx_id, values[idx.column_ids[0]], result);
            }
        }
    }

    try appendOverflowRelinkWalForRow(
        wal,
        tx_id,
        page_id,
        overflow_page_ids[0..schema.column_count],
        0,
    );
    if (page_hint) |hint| hint.* = page_id;
    return result;
}

pub fn executeBulkInsertWithDiagnosticAndParameters(
    catalog: *Catalog,
    pool: *BufferPool,
    wal: *Wal,
    tx_id: TxId,
    model_id: ModelId,
    tree: *const Ast,
    tokens: *const TokenizeResult,
    source: []const u8,
    first_row_group_node: NodeIndex,
    parameter_bindings: []const ParameterBinding,
    diagnostic: ?*MutationDiagnostic,
    eval_ctx: *const filter_mod.EvalContext,
    undo_log: ?*UndoLog,
    snapshot: ?*const Snapshot,
    tx_manager: ?*const TxManager,
    out_row_ids: []RowId,
    out_row_count: *u16,
) MutationError!u32 {
    std.debug.assert(model_id < catalog.model_count);
    const model = &catalog.models[model_id];
    const schema = &catalog.models[model_id].row_schema;
    var inserted_count: u32 = 0;
    out_row_count.* = 0;

    var group_count: u16 = 0;
    var group_cursor = first_row_group_node;
    while (group_cursor != null_node) {
        const group_node = tree.getNode(group_cursor);
        if (group_node.tag != .insert_row_group) return error.Corruption;
        if (group_count >= out_row_ids.len) return error.ResultOverflow;
        group_count += 1;
        group_cursor = group_node.next;
    }

    // Fail-fast on in-batch duplicates for all single-column unique indexes
    // backed by B+ trees. This preserves correctness while index writes are
    // deferred to Phase B.
    {
        var idx_id: u16 = 0;
        while (idx_id < model.index_count) : (idx_id += 1) {
            const idx = &model.indexes[idx_id];
            if (!idx.is_unique) continue;
            if (idx.column_count != 1) continue;
            if (idx.btree_root_page_id == 0) continue;
            try precheckInBatchUniqueDuplicates(
                catalog,
                model_id,
                schema,
                tree,
                tokens,
                source,
                first_row_group_node,
                parameter_bindings,
                diagnostic,
                eval_ctx,
                idx.column_ids[0],
            );
        }
    }

    // Phase A: build+validate rows, then heap insert + WAL append only.
    var row_group = first_row_group_node;
    var bulk_page_hint: u64 = 0;
    var has_bulk_page_hint = false;
    while (row_group != null_node) {
        const group_node = tree.getNode(row_group);
        if (group_node.tag != .insert_row_group) return error.Corruption;
        if (out_row_count.* >= out_row_ids.len) return error.ResultOverflow;

        const page_hint_ptr: ?*u64 = if (has_bulk_page_hint) &bulk_page_hint else null;
        const row_id = executeInsertWithDiagnosticAndParametersWithOptions(
            catalog,
            pool,
            wal,
            tx_id,
            model_id,
            tree,
            tokens,
            source,
            group_node.data.unary,
            parameter_bindings,
            diagnostic,
            eval_ctx,
            undo_log,
            snapshot,
            tx_manager,
            true,
            page_hint_ptr,
        ) catch |insert_err| {
            try rollbackBulkInsertedRows(
                catalog,
                pool,
                wal,
                undo_log,
                tx_id,
                schema,
                model_id,
                out_row_ids[0..out_row_count.*],
            );
            out_row_count.* = 0;
            return insert_err;
        };
        out_row_ids[out_row_count.*] = row_id;
        out_row_count.* += 1;
        inserted_count += 1;
        bulk_page_hint = row_id.page_id;
        has_bulk_page_hint = true;

        row_group = group_node.next;
    }

    // Phase B: sorted index insertion over collected row metadata.
    performBulkIndexPhaseB(
        catalog,
        pool,
        wal,
        model_id,
        schema,
        out_row_ids[0..out_row_count.*],
    ) catch |phase_b_err| {
        try rollbackBulkInsertedRows(
            catalog,
            pool,
            wal,
            undo_log,
            tx_id,
            schema,
            model_id,
            out_row_ids[0..out_row_count.*],
        );
        out_row_count.* = 0;
        return phase_b_err;
    };

    return inserted_count;
}

fn rollbackBulkInsertedRows(
    catalog: *Catalog,
    pool: *BufferPool,
    wal: *Wal,
    undo_log: ?*UndoLog,
    tx_id: TxId,
    schema: *const RowSchema,
    model_id: ModelId,
    row_ids: []const RowId,
) MutationError!void {
    const undo = undo_log orelse return;
    _ = undo;
    _ = wal;
    var i: usize = 0;
    while (i < row_ids.len) : (i += 1) {
        try deleteInsertedRowForRollback(catalog, pool, schema, tx_id, row_ids[i]);
    }
    if (row_ids.len > 0) {
        catalog.decrementRowCount(model_id, @intCast(row_ids.len));
    }
}

/// Compensation delete used only for same-transaction bulk-insert rollback.
///
/// It intentionally skips undo/WAL writes: rollback is already represented by
/// tx abort, and pushing undo here can resurrect rows during abort processing.
fn deleteInsertedRowForRollback(
    catalog: *Catalog,
    pool: *BufferPool,
    schema: *const RowSchema,
    tx_id: TxId,
    row_id: RowId,
) MutationError!void {
    _ = catalog;
    _ = schema;
    _ = tx_id;
    var pinned = try PinnedMutationPage.pin(pool, row_id.page_id);
    defer pinned.release();

    _ = HeapPage.read(pinned.page, row_id.slot) catch return error.StorageRead;
    HeapPage.delete(pinned.page, row_id.slot) catch return error.StorageRead;
    pinned.markDirty();
}

fn precheckInBatchUniqueDuplicates(
    catalog: *Catalog,
    model_id: ModelId,
    schema: *const RowSchema,
    tree: *const Ast,
    tokens: *const TokenizeResult,
    source: []const u8,
    first_row_group_node: NodeIndex,
    parameter_bindings: []const ParameterBinding,
    diagnostic: ?*MutationDiagnostic,
    eval_ctx: *const filter_mod.EvalContext,
    unique_col: u16,
) MutationError!void {
    var unique_hash_entries: [scan_mod.scan_batch_size]UniqueHashEntry = undefined;
    var string_decode_bytes: [max_row_buf_size]u8 = undefined;
    var string_arena = scan_mod.StringArena.init(string_decode_bytes[0..]);

    var entry_idx: u16 = 0;
    var check_group = first_row_group_node;
    while (check_group != null_node) {
        var key_buf: [max_row_buf_size]u8 = undefined;
        const key = try buildUniqueKeyForRowGroup(
            catalog,
            model_id,
            schema,
            tree,
            tokens,
            source,
            check_group,
            parameter_bindings,
            diagnostic,
            eval_ctx,
            &string_arena,
            unique_col,
            &key_buf,
        );
        if (key == null) {
            check_group = tree.getNode(check_group).next;
            continue;
        }
        unique_hash_entries[entry_idx] = .{
            .row_group = check_group,
            .hash = std.hash.Wyhash.hash(0, key.?),
        };
        entry_idx += 1;
        check_group = tree.getNode(check_group).next;
    }

    if (entry_idx < 2) return;

    std.sort.heap(
        UniqueHashEntry,
        unique_hash_entries[0..entry_idx],
        {},
        lessThanUniqueHashEntry,
    );

    var sorted_idx: u16 = 1;
    while (sorted_idx < entry_idx) : (sorted_idx += 1) {
        const prev = unique_hash_entries[sorted_idx - 1];
        const curr = unique_hash_entries[sorted_idx];
        if (prev.hash != curr.hash) continue;

        var prev_key_buf: [max_row_buf_size]u8 = undefined;
        const prev_key = try buildUniqueKeyForRowGroup(
            catalog,
            model_id,
            schema,
            tree,
            tokens,
            source,
            prev.row_group,
            parameter_bindings,
            diagnostic,
            eval_ctx,
            &string_arena,
            unique_col,
            &prev_key_buf,
        );
        if (prev_key == null) continue;
        var curr_key_buf: [max_row_buf_size]u8 = undefined;
        const curr_key = try buildUniqueKeyForRowGroup(
            catalog,
            model_id,
            schema,
            tree,
            tokens,
            source,
            curr.row_group,
            parameter_bindings,
            diagnostic,
            eval_ctx,
            &string_arena,
            unique_col,
            &curr_key_buf,
        );
        if (curr_key == null) continue;
        if (std.mem.eql(u8, prev_key.?, curr_key.?)) return error.DuplicateKey;
    }
}

fn lessThanUniqueHashEntry(_: void, lhs: UniqueHashEntry, rhs: UniqueHashEntry) bool {
    if (lhs.hash < rhs.hash) return true;
    if (lhs.hash > rhs.hash) return false;
    return lhs.row_group < rhs.row_group;
}

fn buildUniqueKeyForRowGroup(
    catalog: *Catalog,
    model_id: ModelId,
    schema: *const RowSchema,
    tree: *const Ast,
    tokens: *const TokenizeResult,
    source: []const u8,
    row_group: NodeIndex,
    parameter_bindings: []const ParameterBinding,
    diagnostic: ?*MutationDiagnostic,
    eval_ctx: *const filter_mod.EvalContext,
    string_arena: *scan_mod.StringArena,
    unique_col: u16,
    key_buf: *[max_row_buf_size]u8,
) MutationError!?[]const u8 {
    const row_group_node = tree.getNode(row_group);
    if (row_group_node.tag != .insert_row_group) return error.Corruption;

    var values: [max_assignments]Value =
        [_]Value{.{ .null_value = {} }} ** max_assignments;
    var assigned_columns: [max_assignments]bool = [_]bool{false} ** max_assignments;
    string_arena.reset();
    try buildRowFromAssignments(
        tree,
        tokens,
        source,
        schema,
        row_group_node.data.unary,
        parameter_bindings,
        &values,
        &assigned_columns,
        diagnostic,
        string_arena,
        eval_ctx,
    );
    applyColumnDefaultsForInsert(
        catalog,
        model_id,
        values[0..schema.column_count],
        assigned_columns[0..schema.column_count],
    );
    if (shouldSkipUniqueIndexKey(values[unique_col])) return null;
    return index_key_mod.encodeValue(values[unique_col], key_buf);
}

fn performBulkIndexPhaseB(
    catalog: *Catalog,
    pool: *BufferPool,
    wal: *Wal,
    model_id: ModelId,
    schema: *const RowSchema,
    row_ids: []const RowId,
) MutationError!void {
    if (row_ids.len == 0) return;
    const model = &catalog.models[model_id];
    var sorted_row_ids: [scan_mod.scan_batch_size]RowId = undefined;
    var idx_id: u16 = 0;
    const pk_col = catalog_mod.findPrimaryKeyColumnId(catalog, model_id);

    while (idx_id < model.index_count) : (idx_id += 1) {
        const idx = &model.indexes[idx_id];
        if (!idx.is_unique) continue;
        if (idx.column_count != 1) continue;
        if (idx.btree_root_page_id == 0) continue;

        std.mem.copyForwards(RowId, sorted_row_ids[0..row_ids.len], row_ids);
        try sortRowIdsByIndexKey(
            catalog,
            pool,
            schema,
            idx.column_ids[0],
            sorted_row_ids[0..row_ids.len],
        );

        var btree = openIndex(catalog, pool, wal, model_id, idx_id) orelse return error.Corruption;
        var decode_buf: [max_row_buf_size]u8 = undefined;
        var string_arena = scan_mod.StringArena.init(decode_buf[0..]);
        var leaf_hint = LeafHint{};

        for (sorted_row_ids[0..row_ids.len]) |row_id| {
            const key_value = try readRowColumnValue(
                catalog,
                pool,
                schema,
                row_id,
                idx.column_ids[0],
                &string_arena,
            );
            if (pk_col != null and idx.column_ids[0] == pk_col.?) {
                try insertPrimaryKeyWithHintNoSync(catalog, model_id, &btree, key_value, row_id, &leaf_hint);
            } else {
                if (shouldSkipUniqueIndexKey(key_value)) continue;
                try insertIndexKeyWithHintNoSync(&btree, key_value, row_id, &leaf_hint);
            }
        }
        syncIndexBTreeState(catalog, model_id, idx_id, &btree);
    }
}

fn sortRowIdsByIndexKey(
    catalog: *Catalog,
    pool: *BufferPool,
    schema: *const RowSchema,
    key_column_id: u16,
    row_ids: []RowId,
) MutationError!void {
    if (row_ids.len < 2) return;
    var scratch: [scan_mod.scan_batch_size]RowId = undefined;
    var run_width: usize = 1;
    while (run_width < row_ids.len) : (run_width *= 2) {
        var start: usize = 0;
        while (start < row_ids.len) : (start += run_width * 2) {
            const mid = @min(start + run_width, row_ids.len);
            const end = @min(start + (run_width * 2), row_ids.len);
            try mergeRowIdsByIndexKey(
                catalog,
                pool,
                schema,
                key_column_id,
                row_ids,
                scratch[0..],
                start,
                mid,
                end,
            );
        }
        std.mem.copyForwards(RowId, row_ids, scratch[0..row_ids.len]);
    }
}

fn mergeRowIdsByIndexKey(
    catalog: *Catalog,
    pool: *BufferPool,
    schema: *const RowSchema,
    key_column_id: u16,
    row_ids: []const RowId,
    scratch: []RowId,
    start: usize,
    mid: usize,
    end: usize,
) MutationError!void {
    var left = start;
    var right = mid;
    var out = start;
    while (left < mid and right < end) {
        if (try lessThanRowIdByIndexKey(catalog, pool, schema, key_column_id, row_ids[left], row_ids[right])) {
            scratch[out] = row_ids[left];
            left += 1;
        } else {
            scratch[out] = row_ids[right];
            right += 1;
        }
        out += 1;
    }
    while (left < mid) : (left += 1) {
        scratch[out] = row_ids[left];
        out += 1;
    }
    while (right < end) : (right += 1) {
        scratch[out] = row_ids[right];
        out += 1;
    }
}

fn lessThanRowIdByIndexKey(
    catalog: *Catalog,
    pool: *BufferPool,
    schema: *const RowSchema,
    key_column_id: u16,
    lhs: RowId,
    rhs: RowId,
) MutationError!bool {
    const order = try compareRowIdsByIndexKey(catalog, pool, schema, key_column_id, lhs, rhs);
    return order == .lt;
}

fn compareRowIdsByIndexKey(
    catalog: *Catalog,
    pool: *BufferPool,
    schema: *const RowSchema,
    key_column_id: u16,
    lhs: RowId,
    rhs: RowId,
) MutationError!std.math.Order {
    var lhs_buf: [max_row_buf_size]u8 = undefined;
    var rhs_buf: [max_row_buf_size]u8 = undefined;
    var lhs_arena = scan_mod.StringArena.init(lhs_buf[0..]);
    var rhs_arena = scan_mod.StringArena.init(rhs_buf[0..]);

    const lhs_key = try readRowColumnValue(catalog, pool, schema, lhs, key_column_id, &lhs_arena);
    const rhs_key = try readRowColumnValue(catalog, pool, schema, rhs, key_column_id, &rhs_arena);
    const key_order = row_mod.compareValues(lhs_key, rhs_key);
    if (key_order != .eq) return key_order;
    if (lhs.page_id < rhs.page_id) return .lt;
    if (lhs.page_id > rhs.page_id) return .gt;
    return std.math.order(lhs.slot, rhs.slot);
}

fn readRowColumnValue(
    catalog: *Catalog,
    pool: *BufferPool,
    schema: *const RowSchema,
    row_id: RowId,
    column_id: u16,
    string_arena: *scan_mod.StringArena,
) MutationError!Value {
    const page = pool.pin(row_id.page_id) catch |e| return mapPoolError(e);
    defer pool.unpin(row_id.page_id, false);

    const row_data = HeapPage.read(page, row_id.slot) catch return error.StorageRead;
    var row = scan_mod.ResultRow.init();
    row.column_count = schema.column_count;
    string_arena.reset();
    try decodeRowWithOverflow(
        schema,
        row_data,
        pool,
        &catalog.overflow_page_allocator,
        string_arena,
        &row.values,
    );
    return row.values[column_id];
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
    const empty_ctx = filter_mod.EvalContext{};
    return executeUpdateWithDiagnosticAndReturningAndParameters(
        catalog,
        pool,
        wal,
        undo_log,
        tx_id,
        snapshot,
        tx_manager,
        model_id,
        tree,
        tokens,
        source,
        predicate_node,
        first_assignment_node,
        allocator,
        &.{},
        null,
        null,
        &empty_ctx,
    );
}

pub fn executeUpdateWithDiagnostic(
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
    diagnostic: ?*MutationDiagnostic,
) MutationError!u32 {
    const empty_ctx = filter_mod.EvalContext{};
    return executeUpdateWithDiagnosticAndReturningAndParameters(
        catalog,
        pool,
        wal,
        undo_log,
        tx_id,
        snapshot,
        tx_manager,
        model_id,
        tree,
        tokens,
        source,
        predicate_node,
        first_assignment_node,
        allocator,
        &.{},
        null,
        diagnostic,
        &empty_ctx,
    );
}

pub fn executeUpdateWithDiagnosticAndReturning(
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
    parameter_bindings: []const ParameterBinding,
    returning_capture: ?*ReturningCapture,
    diagnostic: ?*MutationDiagnostic,
) MutationError!u32 {
    const empty_ctx = filter_mod.EvalContext{};
    return executeUpdateWithDiagnosticAndReturningAndParameters(
        catalog,
        pool,
        wal,
        undo_log,
        tx_id,
        snapshot,
        tx_manager,
        model_id,
        tree,
        tokens,
        source,
        predicate_node,
        first_assignment_node,
        allocator,
        parameter_bindings,
        returning_capture,
        diagnostic,
        &empty_ctx,
    );
}

pub fn executeUpdateWithDiagnosticAndReturningAndParameters(
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
    parameter_bindings: []const ParameterBinding,
    returning_capture: ?*ReturningCapture,
    diagnostic: ?*MutationDiagnostic,
    eval_ctx: *const filter_mod.EvalContext,
) MutationError!u32 {
    std.debug.assert(model_id < catalog.model_count);
    const model = &catalog.models[model_id];
    const schema = &model.row_schema;
    _ = allocator;
    var string_decode_bytes: [max_row_buf_size]u8 = undefined;
    var string_arena = scan_mod.StringArena.init(string_decode_bytes[0..]);

    // Build a local EvalContext with parameter resolver from bindings.
    const parameter_ctx = ParameterBindingContext{
        .bindings = parameter_bindings,
    };
    const parameter_resolver = ParameterResolver{
        .ctx = &parameter_ctx,
        .resolve = resolveParameterBinding,
    };
    var local_eval_ctx = filter_mod.EvalContext{
        .statement_timestamp_micros = eval_ctx.statement_timestamp_micros,
        .parameter_resolver = &parameter_resolver,
        .string_arena = &string_arena,
    };

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
                const matches = filter_mod.evaluatePredicateFull(
                    tree,
                    tokens,
                    source,
                    predicate_node,
                    row.values[0..row.column_count],
                    schema,
                    null,
                    &local_eval_ctx,
                ) catch |e| switch (e) {
                    error.TypeMismatch => return error.PredicateMustBeBoolean,
                    error.UndefinedParameter => return error.PredicateUndefinedParameter,
                    error.ClockUnavailable => return error.PredicateClockUnavailable,
                    else => continue,
                };
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
                parameter_bindings,
                new_values[0..row.column_count],
                diagnostic,
                &string_arena,
                eval_ctx,
            ) catch |e| return e;

            // PK uniqueness enforcement on UPDATE (also maintains B+ tree).
            var pk_btree_update = openPrimaryKeyIndex(catalog, pool, wal, model_id);
            var pk_changed = false;
            if (pk_btree_update != null) {
                const pk_col_update = catalog_mod.findPrimaryKeyColumnId(catalog, model_id).?;
                const old_pk = row.values[pk_col_update];
                const new_pk = new_values[pk_col_update];
                if (row_mod.compareValues(old_pk, new_pk) != .eq) {
                    pk_changed = true;
                    // Check that the new PK value doesn't already exist.
                    if (try primaryKeyVisibleInIndex(catalog, &pk_btree_update.?, model_id, new_pk, pool, undo_log, snapshot, tx_manager)) {
                        return error.DuplicateKey;
                    }
                }
            }

            try enforceOutgoingReferentialIntegrity(
                catalog,
                pool,
                wal,
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

            // Insert new PK key after successful heap update.
            if (pk_changed) {
                if (pk_btree_update) |*btree| {
                    const pk_col_update = catalog_mod.findPrimaryKeyColumnId(catalog, model_id).?;
                    try insertPrimaryKey(catalog, btree, model_id, new_values[pk_col_update], row.row_id);
                }
            }
            if (returning_capture) |capture| {
                try appendReturningRow(
                    capture,
                    row.row_id,
                    new_values[0..row.column_count],
                    row.column_count,
                );
            }
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
    const empty_ctx = filter_mod.EvalContext{};
    return executeDeleteWithReturningAndParameters(
        catalog,
        pool,
        wal,
        undo_log,
        tx_id,
        snapshot,
        tx_manager,
        model_id,
        tree,
        tokens,
        source,
        predicate_node,
        allocator,
        &.{},
        null,
        &empty_ctx,
    );
}

pub fn executeDeleteWithReturning(
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
    parameter_bindings: []const ParameterBinding,
    returning_capture: ?*ReturningCapture,
) MutationError!u32 {
    const empty_ctx = filter_mod.EvalContext{};
    return executeDeleteWithReturningAndParameters(
        catalog,
        pool,
        wal,
        undo_log,
        tx_id,
        snapshot,
        tx_manager,
        model_id,
        tree,
        tokens,
        source,
        predicate_node,
        allocator,
        parameter_bindings,
        returning_capture,
        &empty_ctx,
    );
}

pub fn executeDeleteWithReturningAndParameters(
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
    parameter_bindings: []const ParameterBinding,
    returning_capture: ?*ReturningCapture,
    eval_ctx: *const filter_mod.EvalContext,
) MutationError!u32 {
    std.debug.assert(model_id < catalog.model_count);
    _ = allocator;
    var string_decode_bytes: [max_row_buf_size]u8 = undefined;
    var string_arena = scan_mod.StringArena.init(string_decode_bytes[0..]);

    // Build a local EvalContext with parameter resolver from bindings.
    const parameter_ctx = ParameterBindingContext{
        .bindings = parameter_bindings,
    };
    const parameter_resolver = ParameterResolver{
        .ctx = &parameter_ctx,
        .resolve = resolveParameterBinding,
    };
    var local_eval_ctx = filter_mod.EvalContext{
        .statement_timestamp_micros = eval_ctx.statement_timestamp_micros,
        .parameter_resolver = &parameter_resolver,
        .string_arena = &string_arena,
    };

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
                const matches = filter_mod.evaluatePredicateFull(
                    tree,
                    tokens,
                    source,
                    predicate_node,
                    row.values[0..row.column_count],
                    schema,
                    null,
                    &local_eval_ctx,
                ) catch |e| switch (e) {
                    error.TypeMismatch => return error.PredicateMustBeBoolean,
                    error.UndefinedParameter => return error.PredicateUndefinedParameter,
                    error.ClockUnavailable => return error.PredicateClockUnavailable,
                    else => continue,
                };
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
            if (returning_capture) |capture| {
                try appendReturningRow(
                    capture,
                    row.row_id,
                    row.values[0..row.column_count],
                    row.column_count,
                );
            }
            try deleteSingleRow(
                catalog,
                pool,
                wal,
                undo_log,
                tx_id,
                model_id,
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

fn appendReturningRow(
    capture: *ReturningCapture,
    row_id: RowId,
    values: []const Value,
    column_count: u16,
) MutationError!void {
    std.debug.assert(column_count <= scan_mod.max_columns);
    if (capture.row_count.* >= capture.rows.len) return error.ReturningBufferExhausted;

    var out = ResultRow.init();
    out.row_id = row_id;
    out.column_count = column_count;

    for (0..column_count) |i| {
        out.values[i] = try cloneValueForReturning(values[i], capture.string_arena);
    }
    capture.rows[capture.row_count.*] = out;
    capture.row_count.* += 1;
}

fn cloneValueForReturning(
    value: Value,
    arena: *scan_mod.StringArena,
) MutationError!Value {
    return switch (value) {
        .string => |s| .{ .string = arena.copyString(s) catch return error.ReturningBufferExhausted },
        else => value,
    };
}

/// Delete a single matched row: undo push, tombstone, WAL append.
pub fn deleteSingleRow(
    catalog: *Catalog,
    pool: *BufferPool,
    wal: *Wal,
    undo_log: *UndoLog,
    tx_id: TxId,
    model_id: ModelId,
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
    try enqueueIndexEntriesForReclaim(
        catalog,
        wal,
        tx_id,
        model_id,
        row_id,
        schema,
        old_data,
        pool,
    );
    try enqueueSlotForReclaim(catalog, tx_id, row_id);
}

pub fn updateRowWithValues(
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

fn enqueueSlotForReclaim(
    catalog: *Catalog,
    tx_id: TxId,
    row_id: RowId,
) MutationError!void {
    catalog.slot_reclaim_queue.enqueue(tx_id, row_id.page_id, row_id.slot) catch |e| {
        return switch (e) {
            error.InvalidEntry => error.Corruption,
            error.QueueFull => error.OverflowReclaimQueueFull,
            error.QueueEmpty => error.Corruption,
            error.DuplicateEntry => error.Corruption,
        };
    };
    catalog.recordSlotReclaimEnqueue();
}

fn enqueueIndexEntriesForReclaim(
    catalog: *Catalog,
    wal: *Wal,
    tx_id: TxId,
    model_id: ModelId,
    row_id: RowId,
    schema: *const RowSchema,
    old_row_data: []const u8,
    pool: *BufferPool,
) MutationError!void {
    std.debug.assert(model_id < catalog.model_count);
    const model = &catalog.models[model_id];

    var decode_bytes: [max_row_buf_size]u8 = undefined;
    var string_arena = scan_mod.StringArena.init(decode_bytes[0..]);
    var old_values: [max_assignments]Value =
        [_]Value{.{ .null_value = {} }} ** max_assignments;
    decodeRowWithOverflow(
        schema,
        old_row_data,
        pool,
        &catalog.overflow_page_allocator,
        &string_arena,
        old_values[0..schema.column_count],
    ) catch |e| return e;

    var key_buf: [max_row_buf_size]u8 = undefined;
    var payload_buf: [max_row_buf_size + 16]u8 = undefined;
    for (0..model.index_count) |idx| {
        const index_id: u16 = @intCast(idx);
        const index = &model.indexes[index_id];
        if (!index.is_unique) continue;
        if (index.column_count != 1) continue;
        if (index.btree_root_page_id == 0) continue;
        const key_value = old_values[index.column_ids[0]];
        if (shouldSkipUniqueIndexKey(key_value)) continue;

        const encoded_key = index_key_mod.encodeValue(key_value, &key_buf);
        catalog.index_reclaim_queue.enqueue(
            tx_id,
            model_id,
            index_id,
            row_id.page_id,
            row_id.slot,
            encoded_key,
        ) catch |e| {
            return switch (e) {
                error.InvalidEntry => error.Corruption,
                error.QueueFull => error.OverflowReclaimQueueFull,
                error.QueueEmpty => error.Corruption,
                error.DuplicateEntry => error.Corruption,
                error.KeyTooLarge => error.Corruption,
            };
        };
        catalog.recordIndexReclaimEnqueue();

        const payload_len = encodeIndexReclaimWalPayload(
            payload_buf[0..],
            model_id,
            index_id,
            row_id,
            encoded_key,
        ) catch return error.Corruption;
        _ = wal.append(
            tx_id,
            .index_reclaim_enqueue,
            row_id.page_id,
            payload_buf[0..payload_len],
        ) catch |e| return mapWalAppendError(e);
    }
}

fn drainSlotReclaimQueue(
    catalog: *Catalog,
    pool: *BufferPool,
    wal: *Wal,
    tx_id: TxId,
    oldest_active: TxId,
    max_items: usize,
) MutationError!void {
    var processed: usize = 0;
    while (processed < max_items and !catalog.slot_reclaim_queue.isEmpty()) : (processed += 1) {
        const entry = (catalog.slot_reclaim_queue.dequeueReclaimable(oldest_active) catch
            return error.Corruption) orelse break;
        catalog.recordSlotReclaimDequeue();

        var pinned = try PinnedMutationPage.pin(pool, entry.page_id);
        defer pinned.release();
        if (pinned.page.header.page_type != .heap) {
            catalog.recordSlotReclaimFailure();
            return error.Corruption;
        }
        const reclaimed = HeapPage.reclaim(pinned.page, entry.slot) catch |reclaim_err| {
            catalog.recordSlotReclaimFailure();
            return switch (reclaim_err) {
                error.InvalidSlot => error.Corruption,
                error.PageFull => error.Corruption,
                error.RowTooLarge => error.Corruption,
            };
        };
        if (!reclaimed) continue;

        var payload: [2]u8 = undefined;
        @memcpy(payload[0..2], std.mem.asBytes(&std.mem.nativeToLittle(u16, entry.slot)));
        const lsn = wal.append(
            tx_id,
            .reclaim_slot,
            entry.page_id,
            payload[0..],
        ) catch |e| {
            catalog.recordSlotReclaimFailure();
            return mapWalAppendError(e);
        };
        pinned.page.header.lsn = lsn;
        pinned.markDirty();
        catalog.recordSlotReclaimSuccess();

        try drainIndexReclaimQueueForRow(
            catalog,
            pool,
            wal,
            tx_id,
            oldest_active,
            entry.page_id,
            entry.slot,
        );
    }
}

fn drainIndexReclaimQueueForRow(
    catalog: *Catalog,
    pool: *BufferPool,
    wal: *Wal,
    tx_id: TxId,
    oldest_active: TxId,
    page_id: u64,
    slot: u16,
) MutationError!void {
    var payload_buf: [max_row_buf_size + 16]u8 = undefined;
    while (true) {
        const entry = (catalog.index_reclaim_queue.dequeueReclaimableForRow(
            oldest_active,
            page_id,
            slot,
        ) catch |e| switch (e) {
            error.QueueEmpty => break,
            else => return error.Corruption,
        }) orelse break;
        catalog.recordIndexReclaimDequeue();

        var btree = openIndex(
            catalog,
            pool,
            wal,
            entry.model_id,
            entry.index_id,
        ) orelse {
            catalog.recordIndexReclaimFailure();
            return error.Corruption;
        };

        const key = catalog.index_reclaim_queue.keySlice(&entry);
        const found_row = btree.find(key) catch |err| {
            catalog.recordIndexReclaimFailure();
            return index_maintenance_mod.mapBTreeError(err);
        };
        if (found_row) |current_row| {
            if (current_row.page_id == entry.page_id and current_row.slot == entry.slot) {
                btree.delete(key) catch |err| switch (err) {
                    error.KeyNotFound => {},
                    else => {
                        catalog.recordIndexReclaimFailure();
                        return index_maintenance_mod.mapBTreeError(err);
                    },
                };
                syncIndexBTreeState(catalog, entry.model_id, entry.index_id, &btree);
                catalog.recordIndexReclaimSuccess();

                const payload_len = encodeIndexReclaimWalPayload(
                    payload_buf[0..],
                    entry.model_id,
                    entry.index_id,
                    .{ .page_id = entry.page_id, .slot = entry.slot },
                    key,
                ) catch return error.Corruption;
                _ = wal.append(
                    tx_id,
                    .index_reclaim_delete,
                    entry.page_id,
                    payload_buf[0..payload_len],
                ) catch |e| return mapWalAppendError(e);
                continue;
            }
        }
        catalog.recordIndexReclaimSuccess();

        // Stale metadata can race with newer live rows reusing the same key.
        // In that case, dequeue without deleting to preserve current visibility.
    }
}

const IndexReclaimWalMeta = struct {
    model_id: u16,
    index_id: u16,
    row_id: RowId,
    key: []const u8,
};

fn encodeIndexReclaimWalPayload(
    out: []u8,
    model_id: u16,
    index_id: u16,
    row_id: RowId,
    key: []const u8,
) error{BufferTooSmall}!usize {
    const required = 16 + key.len;
    if (out.len < required) return error.BufferTooSmall;
    @memcpy(out[0..2], std.mem.asBytes(&std.mem.nativeToLittle(u16, model_id)));
    @memcpy(out[2..4], std.mem.asBytes(&std.mem.nativeToLittle(u16, index_id)));
    @memcpy(out[4..6], std.mem.asBytes(&std.mem.nativeToLittle(u16, row_id.slot)));
    @memcpy(out[6..8], std.mem.asBytes(&std.mem.nativeToLittle(u16, @as(u16, @intCast(key.len)))));
    @memcpy(out[8..16], std.mem.asBytes(&std.mem.nativeToLittle(u64, row_id.page_id)));
    @memcpy(out[16..required], key);
    return required;
}

fn decodeIndexReclaimWalPayload(payload: []const u8) MutationError!IndexReclaimWalMeta {
    if (payload.len < 16) return error.Corruption;
    const key_len = std.mem.littleToNative(u16, std.mem.bytesAsValue(u16, payload[6..8]).*);
    if (payload.len != 16 + key_len) return error.Corruption;
    return .{
        .model_id = std.mem.littleToNative(u16, std.mem.bytesAsValue(u16, payload[0..2]).*),
        .index_id = std.mem.littleToNative(u16, std.mem.bytesAsValue(u16, payload[2..4]).*),
        .row_id = .{
            .slot = std.mem.littleToNative(u16, std.mem.bytesAsValue(u16, payload[4..6]).*),
            .page_id = std.mem.littleToNative(u64, std.mem.bytesAsValue(u64, payload[8..16]).*),
        },
        .key = payload[16..],
    };
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

/// Find a heap page with enough free space, or allocate the next page.
fn findPageWithSpace(
    pool: *BufferPool,
    first_page_id: u32,
    total_pages: u32,
    row_len: u16,
    page_hint: ?*u64,
) MutationError!u64 {
    std.debug.assert(row_len > 0);

    if (page_hint) |hint| {
        const hint_page = hint.*;
        const first_id: u64 = first_page_id;
        const last_existing: u64 = if (total_pages == 0) first_id else first_id + total_pages - 1;
        if (total_pages > 0 and hint_page >= first_id and hint_page <= last_existing) {
            const page = pool.pin(hint_page) catch |e| return mapPoolError(e);
            const can_insert = HeapPage.can_insert(page, row_len);
            pool.unpin(hint_page, false);
            if (can_insert) return hint_page;
        }
    }

    // Try existing pages (scan from last to first for locality).
    if (total_pages > 0) {
        var p: u32 = total_pages;
        while (p > 0) {
            p -= 1;
            const page_id: u64 = @as(u64, first_page_id) + p;
            const page = pool.pin(page_id) catch |e|
                return mapPoolError(e);
            const can_insert = HeapPage.can_insert(page, row_len);
            pool.unpin(page_id, false);
            if (can_insert) return page_id;
        }
    }

    // No existing page has space — use next page.
    return @as(u64, first_page_id) + total_pages;
}

pub fn mapPoolError(err: buffer_pool_mod.BufferPoolError) MutationError {
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

pub fn mapOverflowAllocatorError(err: overflow_mod.OverflowAllocatorError) MutationError {
    return switch (err) {
        error.InvalidRegion => error.Corruption,
        error.RegionExhausted => error.OverflowRegionExhausted,
        error.InvalidPageId => error.Corruption,
    };
}

pub fn mapOverflowPageError(err: overflow_mod.OverflowError) MutationError {
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

pub fn mapFilterError(err: filter_mod.EvalError) MutationError {
    return switch (err) {
        error.StackOverflow => error.StackOverflow,
        error.StackUnderflow => error.StackUnderflow,
        error.TypeMismatch => error.TypeMismatch,
        error.DivisionByZero => error.DivisionByZero,
        error.NumericOverflow => error.NumericOverflow,
        error.NumericDomain => error.NumericDomain,
        error.NullArithmeticOperand => error.NullArithmeticOperand,
        error.ColumnNotFound => error.ColumnNotFound,
        error.InvalidLiteral => error.InvalidLiteral,
        error.UnknownFunction => error.UnknownFunction,
        error.NullInPredicate => error.NullInPredicate,
        error.UndefinedParameter => error.UndefinedParameter,
        error.ClockUnavailable => error.ClockUnavailable,
    };
}

pub fn mapWalAppendError(err: wal_mod.WalError) MutationError {
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

test "mutation coercion accepts signed i64 minimum literal" {
    var expr_ast = Ast{};
    const source = "-9223372036854775808";
    const tokens = tokenizer_mod.tokenize(source);
    const expr = try expression_mod.parseExpression(&expr_ast, &tokens, "", 0);
    const schema = RowSchema{};
    const value = try filter_mod.evaluateExpression(
        &expr_ast,
        &tokens,
        source,
        expr.node,
        &.{},
        &schema,
    );

    const coerced = try coerceValueForColumn(
        value,
        .i64,
        null,
        0,
        null,
    );
    try testing.expect(coerced == .i64);
    try testing.expectEqual(std.math.minInt(i64), coerced.i64);
}

test "mutation coercion fails closed for i8 underflow literal" {
    var expr_ast = Ast{};
    const source = "-129";
    const tokens = tokenizer_mod.tokenize(source);
    const expr = try expression_mod.parseExpression(&expr_ast, &tokens, "", 0);
    const schema = RowSchema{};
    const value = try filter_mod.evaluateExpression(
        &expr_ast,
        &tokens,
        source,
        expr.node,
        &.{},
        &schema,
    );

    const result = coerceValueForColumn(
        value,
        .i8,
        null,
        0,
        null,
    );
    try testing.expectError(error.TypeMismatch, result);
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

    const src2 = "User |> where(id == 1) |> delete";
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

    const src3 = "User |> where(id == 1) |> delete";
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

    const src2 = "User |> where(id == 1) |> update(name = \"Bob\")";
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
        "User |> where(id == 1) |> update(name = \"{s}\")",
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
        "User |> where(id == 1) |> update(name = \"{s}\")",
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

    const delete_src = "User |> where(id == 1) |> delete";
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
        "User |> where(id == 1) |> update(name = \"{s}\")",
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

    const delete_src = "User |> where(id == 1) |> delete";
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

    const src2 = "User |> where(id == 1) |> update(name = \"Bob\")";
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

    const delete_src = "User |> where(id == 1) |> delete";
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

    const delete_src = "User |> where(id == 1) |> delete";
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

    const update_src = "User |> where(id == 1) |> update(id = 2)";
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

    const update_src = "User |> where(id == 1) |> update(id = 2)";
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

    const update_src = "User |> where(id == 1) |> update(id = 2)";
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
