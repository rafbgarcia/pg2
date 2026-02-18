const std = @import("std");
const page_mod = @import("../storage/page.zig");
const heap_mod = @import("../storage/heap.zig");
const buffer_pool_mod = @import("../storage/buffer_pool.zig");
const row_mod = @import("../storage/row.zig");
const btree_mod = @import("../storage/btree.zig");
const catalog_mod = @import("../catalog/catalog.zig");
const tx_mod = @import("../mvcc/transaction.zig");
const undo_mod = @import("../mvcc/undo.zig");

const Allocator = std.mem.Allocator;
const Page = page_mod.Page;
const HeapPage = heap_mod.HeapPage;
const RowId = heap_mod.RowId;
const BufferPool = buffer_pool_mod.BufferPool;
const Value = row_mod.Value;
const RowSchema = row_mod.RowSchema;
const BTree = btree_mod.BTree;
const Catalog = catalog_mod.Catalog;
const ModelId = catalog_mod.ModelId;
const Snapshot = tx_mod.Snapshot;
const TxManager = tx_mod.TxManager;
const UndoLog = undo_mod.UndoLog;

/// Maximum rows in a single scan result.
pub const max_result_rows = 4096;
/// Maximum byte size for a single row.
pub const max_row_size_bytes = 8000;
/// Maximum columns per row.
pub const max_columns = 128;

pub const ScanError = error{
    AllFramesPinned,
    ChecksumMismatch,
    StorageRead,
    StorageWrite,
    StorageFsync,
    WalNotFlushed,
    ResultOverflow,
    OutOfMemory,
};

/// A single row in a scan result.
pub const ResultRow = struct {
    values: [max_columns]Value,
    column_count: u16,
    row_id: RowId,

    pub fn init() ResultRow {
        return .{
            .values = [_]Value{.{ .null_value = {} }} ** max_columns,
            .column_count = 0,
            .row_id = .{ .page_id = 0, .slot = 0 },
        };
    }
};

/// Result of a scan operation. Row buffer is heap-allocated from the
/// provided allocator (per-query arena in production, testing.allocator
/// in tests). Caller must call deinit() when done.
pub const ScanResult = struct {
    rows: []ResultRow,
    row_count: u16,
    pages_read: u32,
    allocator: Allocator,

    pub fn init(allocator: Allocator) ScanError!ScanResult {
        const rows = allocator.alloc(ResultRow, max_result_rows) catch
            return error.OutOfMemory;
        const result = ScanResult{
            .rows = rows,
            .row_count = 0,
            .pages_read = 0,
            .allocator = allocator,
        };
        std.debug.assert(result.row_count == 0);
        std.debug.assert(result.rows.len == max_result_rows);
        return result;
    }

    pub fn deinit(self: *ScanResult) void {
        self.allocator.free(self.rows);
        self.* = undefined;
    }

    /// Append a row to the result set.
    /// Asserts capacity is not exceeded — exceeding the bound is an
    /// invariant violation (callers must check before calling).
    pub fn appendRow(self: *ScanResult, row: ResultRow) void {
        std.debug.assert(self.row_count < max_result_rows);
        self.rows[self.row_count] = row;
        self.row_count += 1;
    }
};

/// Full table scan with MVCC visibility.
///
/// Iterates pages from the model's heap_first_page_id for total_pages.
/// For each live slot: checks undo log for visibility, decodes row, appends
/// to result. Skipped: deleted slots, invisible rows.
///
/// M4 simplification: rows without undo history are visible to all snapshots.
pub fn tableScan(
    catalog: *const Catalog,
    pool: *BufferPool,
    undo_log: *const UndoLog,
    snapshot: *const Snapshot,
    tx_manager: *const TxManager,
    model_id: ModelId,
    allocator: Allocator,
) ScanError!ScanResult {
    std.debug.assert(model_id < catalog.model_count);

    var result = try ScanResult.init(allocator);
    errdefer result.deinit();

    const model = &catalog.models[model_id];
    const schema = &model.row_schema;
    const first_page = model.heap_first_page_id;
    const total_pages = model.total_pages;

    if (total_pages == 0) return result;

    var page_idx: u32 = 0;
    while (page_idx < total_pages) : (page_idx += 1) {
        const page_id: u64 = @as(u64, first_page) + page_idx;
        const page = pool.pin(page_id) catch |e| return mapPoolError(e);
        defer pool.unpin(page_id, false);
        result.pages_read += 1;

        const slot_count = HeapPage.slot_count(page);
        var slot_idx: u16 = 0;
        while (slot_idx < slot_count) : (slot_idx += 1) {
            if (result.row_count >= max_result_rows) break;
            scanSlot(
                page, page_id, slot_idx, schema,
                undo_log, snapshot, tx_manager, &result,
            );
        }
    }

    std.debug.assert(result.row_count <= max_result_rows);
    return result;
}

/// Process a single slot during table scan.
fn scanSlot(
    page: *const Page,
    page_id: u64,
    slot_idx: u16,
    schema: *const RowSchema,
    undo_log: *const UndoLog,
    snapshot: *const Snapshot,
    tx_manager: *const TxManager,
    result: *ScanResult,
) void {
    std.debug.assert(result.row_count < max_result_rows);

    // Read raw row data. Skip deleted slots.
    const row_data = HeapPage.read(page, slot_idx) catch return;

    // For M4: rows without undo history are always visible.
    // If undo_log has an entry, use findVisible; otherwise use heap data.
    const data_to_decode = blk: {
        const head = undo_log.getHead(page_id, slot_idx);
        if (head == null) {
            // No undo history — row is visible to all snapshots.
            break :blk row_data;
        }
        // Has undo history — check visibility.
        const vis = undo_log.findVisible(
            page_id, slot_idx, snapshot, tx_manager,
        );
        if (vis) |old_data| {
            break :blk old_data;
        }
        // null means current heap version is visible.
        break :blk row_data;
    };

    // Decode and append to result.
    var row = ResultRow.init();
    row.row_id = .{ .page_id = page_id, .slot = slot_idx };
    row.column_count = schema.column_count;
    row_mod.decodeRow(schema, data_to_decode, &row.values);
    result.appendRow(row);
}

/// Index point lookup: find a single row by exact key match.
pub fn indexFind(
    catalog: *const Catalog,
    pool: *BufferPool,
    undo_log: *const UndoLog,
    snapshot: *const Snapshot,
    tx_manager: *const TxManager,
    btree: *BTree,
    model_id: ModelId,
    key: []const u8,
) ScanError!?ResultRow {
    std.debug.assert(model_id < catalog.model_count);
    std.debug.assert(key.len > 0);

    const model = &catalog.models[model_id];
    const schema = &model.row_schema;

    const row_id_opt = btree.find(key) catch return error.StorageRead;
    const row_id = row_id_opt orelse return null;

    const page = pool.pin(row_id.page_id) catch |e| return mapPoolError(e);
    defer pool.unpin(row_id.page_id, false);

    const row_data = HeapPage.read(page, row_id.slot) catch return null;

    // MVCC check.
    const data_to_decode = resolveVisibleVersion(
        undo_log, row_id.page_id, row_id.slot,
        snapshot, tx_manager, row_data,
    );

    var row = ResultRow.init();
    row.row_id = row_id;
    row.column_count = schema.column_count;
    row_mod.decodeRow(schema, data_to_decode, &row.values);
    return row;
}

/// Index range scan: find all rows with keys in [lo, hi].
pub fn indexRange(
    catalog: *const Catalog,
    pool: *BufferPool,
    undo_log: *const UndoLog,
    snapshot: *const Snapshot,
    tx_manager: *const TxManager,
    btree: *BTree,
    model_id: ModelId,
    lo: ?[]const u8,
    hi: ?[]const u8,
    allocator: Allocator,
) ScanError!ScanResult {
    std.debug.assert(model_id < catalog.model_count);

    var result = try ScanResult.init(allocator);
    errdefer result.deinit();

    const model = &catalog.models[model_id];
    const schema = &model.row_schema;

    var iter = btree.rangeScan(lo, hi) catch return error.StorageRead;
    defer iter.close();

    while (true) {
        const entry_opt = iter.next() catch return error.StorageRead;
        const entry = entry_opt orelse break;
        if (result.row_count >= max_result_rows) break;

        const row_id = entry.row_id;
        const page = pool.pin(row_id.page_id) catch |e| {
            return mapPoolError(e);
        };
        defer pool.unpin(row_id.page_id, false);

        const row_data = HeapPage.read(page, row_id.slot) catch continue;

        // MVCC check.
        const data_to_decode = resolveVisibleVersion(
            undo_log, row_id.page_id, row_id.slot,
            snapshot, tx_manager, row_data,
        );

        var row = ResultRow.init();
        row.row_id = row_id;
        row.column_count = schema.column_count;
        row_mod.decodeRow(schema, data_to_decode, &row.values);
        result.appendRow(row);
    }

    std.debug.assert(result.row_count <= max_result_rows);
    return result;
}

/// Resolve the visible version of a row for the given snapshot.
/// M4: rows without undo history are visible to all snapshots.
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
        page_id, slot_idx, snapshot, tx_manager,
    );
    return vis orelse heap_data;
}

fn mapPoolError(err: buffer_pool_mod.BufferPoolError) ScanError {
    return switch (err) {
        error.AllFramesPinned => error.AllFramesPinned,
        error.ChecksumMismatch => error.ChecksumMismatch,
        error.StorageRead => error.StorageRead,
        error.StorageWrite => error.StorageWrite,
        error.StorageFsync => error.StorageFsync,
        error.WalNotFlushed => error.WalNotFlushed,
    };
}

// --- Tests ---

const testing = std.testing;
const disk_mod = @import("../simulator/disk.zig");

test "empty scan returns zero rows" {
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();
    var pool = try BufferPool.init(testing.allocator, disk.storage(), 8);
    defer pool.deinit();
    var tm = TxManager.init(testing.allocator);
    defer tm.deinit();
    var undo_log = try UndoLog.init(testing.allocator, 1024, 64 * 1024);
    defer undo_log.deinit();

    var catalog = Catalog{};
    const model_id = try catalog.addModel("User");
    _ = try catalog.addColumn(model_id, "id", .bigint, false);
    // total_pages = 0, so scan should return nothing.

    const tx = try tm.begin();
    var snap = try tm.snapshot(tx);
    defer snap.deinit();

    var result = try tableScan(
        &catalog, &pool, &undo_log, &snap, &tm, model_id, testing.allocator,
    );
    defer result.deinit();

    try testing.expectEqual(@as(u16, 0), result.row_count);
}

test "scan with rows inserted via HeapPage" {
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();
    var pool = try BufferPool.init(testing.allocator, disk.storage(), 8);
    defer pool.deinit();
    var tm = TxManager.init(testing.allocator);
    defer tm.deinit();
    var undo_log = try UndoLog.init(testing.allocator, 1024, 64 * 1024);
    defer undo_log.deinit();

    var catalog = Catalog{};
    const model_id = try catalog.addModel("User");
    _ = try catalog.addColumn(model_id, "id", .bigint, false);
    _ = try catalog.addColumn(model_id, "name", .string, false);

    // Set up heap page at page_id 10.
    const page_id: u64 = 10;
    catalog.models[model_id].heap_first_page_id = 10;
    catalog.models[model_id].total_pages = 1;

    const page = try pool.pin(page_id);
    HeapPage.init(page);

    // Encode and insert two rows.
    const schema = &catalog.models[model_id].row_schema;
    var buf1: [256]u8 = undefined;
    const vals1 = [_]Value{ .{ .bigint = 1 }, .{ .string = "Alice" } };
    const len1 = try row_mod.encodeRow(schema, &vals1, &buf1);
    _ = try HeapPage.insert(page, buf1[0..len1]);

    var buf2: [256]u8 = undefined;
    const vals2 = [_]Value{ .{ .bigint = 2 }, .{ .string = "Bob" } };
    const len2 = try row_mod.encodeRow(schema, &vals2, &buf2);
    _ = try HeapPage.insert(page, buf2[0..len2]);

    pool.unpin(page_id, true);

    const tx = try tm.begin();
    var snap = try tm.snapshot(tx);
    defer snap.deinit();

    var result = try tableScan(
        &catalog, &pool, &undo_log, &snap, &tm, model_id, testing.allocator,
    );
    defer result.deinit();

    try testing.expectEqual(@as(u16, 2), result.row_count);
    try testing.expectEqual(@as(i64, 1), result.rows[0].values[0].bigint);
    try testing.expectEqualSlices(u8, "Alice", result.rows[0].values[1].string);
    try testing.expectEqual(@as(i64, 2), result.rows[1].values[0].bigint);
    try testing.expectEqualSlices(u8, "Bob", result.rows[1].values[1].string);
}

test "scan skips deleted slots" {
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();
    var pool = try BufferPool.init(testing.allocator, disk.storage(), 8);
    defer pool.deinit();
    var tm = TxManager.init(testing.allocator);
    defer tm.deinit();
    var undo_log = try UndoLog.init(testing.allocator, 1024, 64 * 1024);
    defer undo_log.deinit();

    var catalog = Catalog{};
    const model_id = try catalog.addModel("Item");
    _ = try catalog.addColumn(model_id, "id", .bigint, false);
    catalog.models[model_id].heap_first_page_id = 20;
    catalog.models[model_id].total_pages = 1;

    const page = try pool.pin(20);
    HeapPage.init(page);

    const schema = &catalog.models[model_id].row_schema;
    var buf: [256]u8 = undefined;
    const vals1 = [_]Value{.{ .bigint = 1 }};
    const len1 = try row_mod.encodeRow(schema, &vals1, &buf);
    const slot0 = try HeapPage.insert(page, buf[0..len1]);

    const vals2 = [_]Value{.{ .bigint = 2 }};
    const len2 = try row_mod.encodeRow(schema, &vals2, &buf);
    _ = try HeapPage.insert(page, buf[0..len2]);

    // Delete first row.
    try HeapPage.delete(page, slot0);
    pool.unpin(20, true);

    const tx = try tm.begin();
    var snap = try tm.snapshot(tx);
    defer snap.deinit();

    var result = try tableScan(
        &catalog, &pool, &undo_log, &snap, &tm, model_id, testing.allocator,
    );
    defer result.deinit();

    try testing.expectEqual(@as(u16, 1), result.row_count);
    try testing.expectEqual(@as(i64, 2), result.rows[0].values[0].bigint);
}

test "scan pages_read tracks correctly" {
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();
    var pool = try BufferPool.init(testing.allocator, disk.storage(), 8);
    defer pool.deinit();
    var tm = TxManager.init(testing.allocator);
    defer tm.deinit();
    var undo_log = try UndoLog.init(testing.allocator, 1024, 64 * 1024);
    defer undo_log.deinit();

    var catalog = Catalog{};
    const model_id = try catalog.addModel("Page");
    _ = try catalog.addColumn(model_id, "id", .bigint, false);
    catalog.models[model_id].heap_first_page_id = 30;
    catalog.models[model_id].total_pages = 3;

    // Initialize 3 heap pages.
    var p: u32 = 0;
    while (p < 3) : (p += 1) {
        const pid: u64 = 30 + p;
        const page = try pool.pin(pid);
        HeapPage.init(page);
        pool.unpin(pid, true);
    }

    const tx = try tm.begin();
    var snap = try tm.snapshot(tx);
    defer snap.deinit();

    var result = try tableScan(
        &catalog, &pool, &undo_log, &snap, &tm, model_id, testing.allocator,
    );
    defer result.deinit();

    try testing.expectEqual(@as(u32, 3), result.pages_read);
    try testing.expectEqual(@as(u16, 0), result.row_count);
}
