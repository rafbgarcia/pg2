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
    UnknownFunction,
    NullInPredicate,
    ResultOverflow,
    // WAL errors
    WalWriteError,
    WalFsyncError,
    OutOfMemory,
    // Undo errors
    UndoLogFull,
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
        tree, tokens, source, schema, first_assignment_node, &values,
    );

    // Encode row.
    var row_buf: [max_row_buf_size]u8 = undefined;
    const row_len = row_mod.encodeRow(
        schema, values[0..schema.column_count], &row_buf,
    ) catch |e| return mapEncodeError(e);
    std.debug.assert(row_len > 0);

    // Find a page with space.
    const page_id = try findPageWithSpace(
        pool, model.heap_first_page_id, model.total_pages, row_len,
    );

    // Insert into heap page.
    const page = pool.pin(page_id) catch |e| return mapPoolError(e);
    if (page.header.page_type == .free) {
        HeapPage.init(page);
    }
    const slot = HeapPage.insert(page, row_buf[0..row_len]) catch |e| {
        pool.unpin(page_id, false);
        return mapHeapError(e);
    };

    // WAL append.
    const lsn = wal.append(
        tx_id, .insert, page_id, row_buf[0..row_len],
    ) catch {
        pool.unpin(page_id, true);
        return error.WalWriteError;
    };
    page.header.lsn = lsn;
    pool.unpin(page_id, true);

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
    const model = &catalog.models[model_id];
    const schema = &model.row_schema;

    // Scan for matching rows.
    var scan_result = scan_mod.tableScan(
        catalog, pool, undo_log, snapshot, tx_manager, model_id, allocator,
    ) catch |e| return mapScanError(e);
    defer scan_result.deinit();

    var updated_count: u32 = 0;

    var i: u16 = 0;
    while (i < scan_result.row_count) : (i += 1) {
        const row = &scan_result.rows[i];

        // Apply predicate filter.
        if (predicate_node != null_node) {
            const matches = filter_mod.evaluatePredicate(
                tree, tokens, source, predicate_node,
                row.values[0..row.column_count], schema,
            ) catch continue;
            if (!matches) continue;
        }

        // Read old data for undo log.
        try updateSingleRow(
            pool, wal, undo_log, tx_id, schema, tree, tokens, source,
            row, first_assignment_node,
        );
        updated_count += 1;
    }

    return updated_count;
}

/// Update a single matched row: undo push, apply assignments, re-encode,
/// write back.
fn updateSingleRow(
    pool: *BufferPool,
    wal: *Wal,
    undo_log: *UndoLog,
    tx_id: TxId,
    schema: *const RowSchema,
    tree: *const Ast,
    tokens: *const TokenizeResult,
    source: []const u8,
    row: *const scan_mod.ResultRow,
    first_assignment_node: NodeIndex,
) MutationError!void {
    const page = pool.pin(row.row_id.page_id) catch |e|
        return mapPoolError(e);

    // Read old data for undo.
    const old_data = HeapPage.read(page, row.row_id.slot) catch {
        pool.unpin(row.row_id.page_id, false);
        return error.StorageRead;
    };

    _ = undo_log.push(
        tx_id, row.row_id.page_id, row.row_id.slot, old_data,
    ) catch {
        pool.unpin(row.row_id.page_id, false);
        return error.UndoLogFull;
    };

    // Apply assignments to existing values.
    var new_values: [max_assignments]Value = undefined;
    for (0..row.column_count) |c| {
        new_values[c] = row.values[c];
    }
    applyAssignments(
        tree, tokens, source, schema, first_assignment_node, &new_values,
    ) catch {
        pool.unpin(row.row_id.page_id, false);
        return error.ColumnNotFound;
    };

    // Re-encode.
    var row_buf: [max_row_buf_size]u8 = undefined;
    const row_len = row_mod.encodeRow(
        schema, new_values[0..schema.column_count], &row_buf,
    ) catch |e| {
        pool.unpin(row.row_id.page_id, false);
        return mapEncodeError(e);
    };

    // Update in-place.
    HeapPage.update(
        page, row.row_id.slot, row_buf[0..row_len],
    ) catch |e| {
        pool.unpin(row.row_id.page_id, false);
        return mapHeapError(e);
    };

    // WAL append.
    const lsn = wal.append(
        tx_id, .update, row.row_id.page_id, row_buf[0..row_len],
    ) catch {
        pool.unpin(row.row_id.page_id, true);
        return error.WalWriteError;
    };
    page.header.lsn = lsn;
    pool.unpin(row.row_id.page_id, true);
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

    const model = &catalog.models[model_id];
    const schema = &model.row_schema;

    // Scan for matching rows.
    var scan_result = scan_mod.tableScan(
        catalog, pool, undo_log, snapshot, tx_manager, model_id, allocator,
    ) catch |e| return mapScanError(e);
    defer scan_result.deinit();

    var deleted_count: u32 = 0;

    var i: u16 = 0;
    while (i < scan_result.row_count) : (i += 1) {
        const row = &scan_result.rows[i];

        // Apply predicate filter.
        if (predicate_node != null_node) {
            const matches = filter_mod.evaluatePredicate(
                tree, tokens, source, predicate_node,
                row.values[0..row.column_count], schema,
            ) catch continue;
            if (!matches) continue;
        }

        try deleteSingleRow(pool, wal, undo_log, tx_id, row);
        deleted_count += 1;
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
    row: *const scan_mod.ResultRow,
) MutationError!void {
    const page = pool.pin(row.row_id.page_id) catch |e|
        return mapPoolError(e);

    // Read old data for undo.
    const old_data = HeapPage.read(page, row.row_id.slot) catch {
        pool.unpin(row.row_id.page_id, false);
        return error.StorageRead;
    };

    _ = undo_log.push(
        tx_id, row.row_id.page_id, row.row_id.slot, old_data,
    ) catch {
        pool.unpin(row.row_id.page_id, false);
        return error.UndoLogFull;
    };

    // Tombstone the slot.
    HeapPage.delete(page, row.row_id.slot) catch {
        pool.unpin(row.row_id.page_id, false);
        return error.StorageRead;
    };

    // WAL append.
    const lsn = wal.append(
        tx_id, .delete, row.row_id.page_id, &.{},
    ) catch {
        pool.unpin(row.row_id.page_id, true);
        return error.WalWriteError;
    };
    page.header.lsn = lsn;
    pool.unpin(row.row_id.page_id, true);
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
            tree, tokens, source, expr_node, &.{}, schema,
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
            tree, tokens, source, expr_node, values, schema,
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
        error.ColumnNotFound => error.ColumnNotFound,
        error.InvalidLiteral => error.InvalidLiteral,
        error.UnknownFunction => error.UnknownFunction,
        error.NullInPredicate => error.NullInPredicate,
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
            testing.allocator, self.disk.storage(), 16,
        );
        self.wal = Wal.init(testing.allocator, self.disk.storage());
        self.tm = TxManager.init(testing.allocator);
        self.undo_log = try UndoLog.init(testing.allocator, 1024, 64 * 1024);

        self.catalog = Catalog{};
        self.model_id = try self.catalog.addModel("User");
        _ = try self.catalog.addColumn(self.model_id, "id", .bigint, false);
        _ = try self.catalog.addColumn(
            self.model_id, "name", .string, true,
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
        &env.catalog, &env.pool, &env.wal, tx, env.model_id,
        &parsed.ast, &tok, src, insert_op.data.unary,
    );

    try testing.expectEqual(@as(u64, 100), row_id.page_id);
    try testing.expectEqual(@as(u16, 0), row_id.slot);
    try testing.expectEqual(
        @as(u64, 1), env.catalog.models[env.model_id].row_count,
    );

    var result = try scan_mod.tableScan(
        &env.catalog, &env.pool, &env.undo_log,
        &snap, &env.tm, env.model_id, testing.allocator,
    );
    defer result.deinit();

    try testing.expectEqual(@as(u16, 1), result.row_count);
    try testing.expectEqual(@as(i64, 1), result.rows[0].values[0].bigint);
    try testing.expectEqualSlices(
        u8, "Alice", result.rows[0].values[1].string,
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
        &env.catalog, &env.pool, &env.wal, tx, env.model_id,
        &parsed.ast, &tok, src, insert_op.data.unary,
    );

    try testing.expectEqual(
        @as(u64, 1), env.catalog.models[env.model_id].row_count,
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
        &env.catalog, &env.pool, &env.wal, tx, env.model_id,
        &p1.ast, &tok1, src1, ins.data.unary,
    );

    const src2 = "User |> where(id = 1) |> delete";
    const tok2 = tokenizer_mod.tokenize(src2);
    const p2 = parser_mod.parse(&tok2, src2);
    const r2 = p2.ast.getNode(p2.ast.root);
    const pipe2 = p2.ast.getNode(r2.data.unary);
    const where_op = p2.ast.getNode(pipe2.data.binary.rhs);
    const predicate = where_op.data.unary;

    const deleted = try executeDelete(
        &env.catalog, &env.pool, &env.wal, &env.undo_log, tx,
        &snap, &env.tm, env.model_id,
        &p2.ast, &tok2, src2, predicate, testing.allocator,
    );

    try testing.expectEqual(@as(u32, 1), deleted);
    try testing.expectEqual(
        @as(u64, 0), env.catalog.models[env.model_id].row_count,
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
        &env.catalog, &env.pool, &env.wal, tx, env.model_id,
        &p1.ast, &tok1, src1, ins1.data.unary,
    );

    const src2 = "User |> insert(id = 2, name = \"B\")";
    const tok2 = tokenizer_mod.tokenize(src2);
    const p2 = parser_mod.parse(&tok2, src2);
    const r2 = p2.ast.getNode(p2.ast.root);
    const pipe2 = p2.ast.getNode(r2.data.unary);
    const ins2 = p2.ast.getNode(pipe2.data.binary.rhs);
    _ = try executeInsert(
        &env.catalog, &env.pool, &env.wal, tx, env.model_id,
        &p2.ast, &tok2, src2, ins2.data.unary,
    );

    try testing.expectEqual(
        @as(u64, 2), env.catalog.models[env.model_id].row_count,
    );

    const src3 = "User |> where(id = 1) |> delete";
    const tok3 = tokenizer_mod.tokenize(src3);
    const p3 = parser_mod.parse(&tok3, src3);
    const r3 = p3.ast.getNode(p3.ast.root);
    const pipe3 = p3.ast.getNode(r3.data.unary);
    const where3 = p3.ast.getNode(pipe3.data.binary.rhs);
    const pred3 = where3.data.unary;

    _ = try executeDelete(
        &env.catalog, &env.pool, &env.wal, &env.undo_log, tx,
        &snap, &env.tm, env.model_id,
        &p3.ast, &tok3, src3, pred3, testing.allocator,
    );

    try testing.expectEqual(
        @as(u64, 1), env.catalog.models[env.model_id].row_count,
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
        &env.catalog, &env.pool, &env.wal, tx, env.model_id,
        &p1.ast, &tok1, src1, ins1.data.unary,
    );

    const src2 = "User |> where(id = 1) |> update(name = \"Bob\")";
    const tok2 = tokenizer_mod.tokenize(src2);
    const p2 = parser_mod.parse(&tok2, src2);
    const r2 = p2.ast.getNode(p2.ast.root);
    const pipe2 = p2.ast.getNode(r2.data.unary);
    const where_op = p2.ast.getNode(pipe2.data.binary.rhs);
    const update_op = p2.ast.getNode(where_op.next);

    const updated = try executeUpdate(
        &env.catalog, &env.pool, &env.wal, &env.undo_log, tx,
        &snap, &env.tm, env.model_id,
        &p2.ast, &tok2, src2,
        where_op.data.unary, update_op.data.unary, testing.allocator,
    );

    try testing.expectEqual(@as(u32, 1), updated);

    var result = try scan_mod.tableScan(
        &env.catalog, &env.pool, &env.undo_log,
        &snap, &env.tm, env.model_id, testing.allocator,
    );
    defer result.deinit();

    try testing.expectEqual(@as(u16, 1), result.row_count);
    try testing.expectEqualSlices(
        u8, "Bob", result.rows[0].values[1].string,
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
        &env.catalog, &env.pool, &env.wal, tx, env.model_id,
        &p1.ast, &tok1, src1, ins1.data.unary,
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
        &env.catalog, &env.pool, &env.wal, &env.undo_log, tx,
        &snap, &env.tm, env.model_id,
        &p2.ast, &tok2, src2,
        where_op.data.unary, update_op.data.unary, testing.allocator,
    );

    try testing.expectEqual(@as(usize, 1), env.undo_log.len());
}
