//! Scan and row materialization path for query reads.
//!
//! Responsibilities in this file:
//! - Reads heap/index-backed rows into bounded result buffers.
//! - Applies snapshot/undo visibility reconstruction for reads.
//! - Materializes overflow-backed strings into caller-managed arenas.
//! - Exposes deterministic scan stats used by executor introspection.
const std = @import("std");
const page_mod = @import("../storage/page.zig");
const heap_mod = @import("../storage/heap.zig");
const buffer_pool_mod = @import("../storage/buffer_pool.zig");
const row_mod = @import("../storage/row.zig");
const overflow_mod = @import("../storage/overflow.zig");
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
const OverflowPage = overflow_mod.OverflowPage;
const BTree = btree_mod.BTree;
const Catalog = catalog_mod.Catalog;
const ModelId = catalog_mod.ModelId;
const Snapshot = tx_mod.Snapshot;
const TxManager = tx_mod.TxManager;
const UndoLog = undo_mod.UndoLog;

/// Physical scan batch size: number of rows in a single working buffer.
/// This is a batch/chunk size, not a query result cap. The executor loops
/// over batches, feeding post-filter survivors into the SpillingResultCollector
/// which handles overflow to temp pages.
pub const scan_batch_size = 4096;
/// Maximum byte size for a single row.
pub const max_row_size_bytes = 8000;
/// Maximum columns per row.
pub const max_columns = 128;
/// Default temporary bytes for string materialization in allocator-based scans.
pub const default_string_arena_bytes: usize = 4 * 1024 * 1024;

/// Resumable cursor for chunked table scans.
///
/// When passed to `tableScanInto()`, the scan resumes from the cursor's
/// position and writes back the position when the output buffer fills.
/// `done` is set to `true` when all pages have been exhausted.
pub const ScanCursor = struct {
    page_idx: u32,
    slot_idx: u16,
    done: bool,

    pub fn init() ScanCursor {
        return .{ .page_idx = 0, .slot_idx = 0, .done = false };
    }
};

pub const ScanError = error{
    AllFramesPinned,
    ChecksumMismatch,
    Corruption,
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
    string_storage: []u8,

    pub fn init(allocator: Allocator) ScanError!ScanResult {
        const rows = allocator.alloc(ResultRow, scan_batch_size) catch
            return error.OutOfMemory;
        const result = ScanResult{
            .rows = rows,
            .row_count = 0,
            .pages_read = 0,
            .allocator = allocator,
            .string_storage = &.{},
        };
        std.debug.assert(result.row_count == 0);
        std.debug.assert(result.rows.len == scan_batch_size);
        return result;
    }

    pub fn deinit(self: *ScanResult) void {
        if (self.string_storage.len > 0) {
            self.allocator.free(self.string_storage);
        }
        self.allocator.free(self.rows);
        self.* = undefined;
    }

    /// Append a row to the result set.
    /// Asserts capacity is not exceeded — exceeding the bound is an
    /// invariant violation (callers must check before calling).
    pub fn appendRow(self: *ScanResult, row: ResultRow) void {
        std.debug.assert(self.row_count < scan_batch_size);
        self.rows[self.row_count] = row;
        self.row_count += 1;
    }
};

pub const ScanIntoResult = struct {
    row_count: u16,
    pages_read: u32,
};

pub const StringArena = struct {
    bytes: []u8,
    used: usize = 0,

    pub fn init(bytes: []u8) StringArena {
        return .{ .bytes = bytes, .used = 0 };
    }

    pub fn reset(self: *StringArena) void {
        self.used = 0;
    }

    pub fn startString(self: *const StringArena) usize {
        return self.used;
    }

    pub fn appendChunk(self: *StringArena, source: []const u8) error{OutOfMemory}!void {
        if (source.len == 0) return;
        if (self.used + source.len > self.bytes.len) return error.OutOfMemory;
        const start = self.used;
        const end = start + source.len;
        @memcpy(self.bytes[start..end], source);
        self.used = end;
    }

    pub fn finishString(self: *const StringArena, start: usize) []const u8 {
        std.debug.assert(start <= self.used);
        return self.bytes[start..self.used];
    }

    pub fn copyString(self: *StringArena, source: []const u8) error{OutOfMemory}![]const u8 {
        if (source.len == 0) return "";
        const start = self.startString();
        try self.appendChunk(source);
        return self.finishString(start);
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
    result.string_storage = allocator.alloc(u8, default_string_arena_bytes) catch
        return error.OutOfMemory;
    var string_arena = StringArena.init(result.string_storage);

    const scan_into = try tableScanInto(
        catalog,
        pool,
        undo_log,
        snapshot,
        tx_manager,
        model_id,
        result.rows,
        &string_arena,
        null,
    );
    result.row_count = scan_into.row_count;
    result.pages_read = scan_into.pages_read;
    return result;
}

/// Table scan into caller-owned row storage with optional cursor.
///
/// This bounded path avoids scan-time heap allocation and enforces an
/// explicit row-capacity contract through `out_rows.len`.
///
/// When `cursor` is non-null the scan resumes from the cursor's saved
/// position.  On return the cursor is updated:
///   - If the buffer filled before all pages were exhausted, the cursor
///     records the resume position and `done` remains false.
///   - If all pages were exhausted, `done` is set to true.
///
/// When `cursor` is null the scan starts from the beginning and stops
/// when the buffer is full (existing behavior).
pub fn tableScanInto(
    catalog: *const Catalog,
    pool: *BufferPool,
    undo_log: *const UndoLog,
    snapshot: *const Snapshot,
    tx_manager: *const TxManager,
    model_id: ModelId,
    out_rows: []ResultRow,
    string_arena: *StringArena,
    cursor: ?*ScanCursor,
) ScanError!ScanIntoResult {
    std.debug.assert(model_id < catalog.model_count);
    std.debug.assert(out_rows.len <= std.math.maxInt(u16));

    const model = &catalog.models[model_id];
    const schema = &model.row_schema;
    const first_page = model.heap_first_page_id;
    const total_pages = model.total_pages;

    if (total_pages == 0) {
        if (cursor) |c| c.done = true;
        return .{ .row_count = 0, .pages_read = 0 };
    }

    var row_count: u16 = 0;
    var pages_read: u32 = 0;
    // Resume position from cursor, or start at the beginning.
    var page_idx: u32 = if (cursor) |c| c.page_idx else 0;
    const start_slot: u16 = if (cursor) |c| c.slot_idx else 0;
    var is_first_page = true;

    while (page_idx < total_pages) : (page_idx += 1) {
        const page_id: u64 = @as(u64, first_page) + page_idx;
        const page = pool.pin(page_id) catch |e| return mapPoolError(e);
        defer pool.unpin(page_id, false);
        pages_read += 1;

        const slot_count = HeapPage.slot_count(page);
        // On the first page of a resumed scan, skip to the saved slot.
        var slot_idx: u16 = if (is_first_page) start_slot else 0;
        is_first_page = false;

        while (slot_idx < slot_count) : (slot_idx += 1) {
            if (@as(usize, row_count) >= out_rows.len) {
                // Buffer full. Save resume position if cursor is present.
                if (cursor) |c| {
                    c.page_idx = page_idx;
                    c.slot_idx = slot_idx;
                    c.done = false;
                }
                return .{ .row_count = row_count, .pages_read = pages_read };
            }
            try scanSlotInto(
                catalog,
                pool,
                page,
                page_id,
                slot_idx,
                schema,
                undo_log,
                snapshot,
                tx_manager,
                out_rows,
                &row_count,
                string_arena,
            );
        }
    }

    // All pages exhausted.
    if (cursor) |c| c.done = true;
    std.debug.assert(@as(usize, row_count) <= out_rows.len);
    return .{ .row_count = row_count, .pages_read = pages_read };
}

/// Process a single slot during table scan.
fn scanSlotInto(
    catalog: *const Catalog,
    pool: *BufferPool,
    page: *const Page,
    page_id: u64,
    slot_idx: u16,
    schema: *const RowSchema,
    undo_log: *const UndoLog,
    snapshot: *const Snapshot,
    tx_manager: *const TxManager,
    out_rows: []ResultRow,
    row_count: *u16,
    string_arena: *StringArena,
) ScanError!void {
    std.debug.assert(@as(usize, row_count.*) < out_rows.len);

    // Read raw row data if the slot currently stores a visible heap tuple.
    // Tombstoned slots can still be visible through undo (e.g. aborted delete),
    // so visibility resolution must run even when HeapPage.read fails.
    const row_data_opt = HeapPage.read(page, slot_idx) catch null;

    const data_to_decode = resolveVisibleVersion(
        undo_log,
        page_id,
        slot_idx,
        snapshot,
        tx_manager,
        row_data_opt,
    ) orelse return;

    // Decode and append to result.
    var row = ResultRow.init();
    row.row_id = .{ .page_id = page_id, .slot = slot_idx };
    try decodeRowIntoResult(
        catalog,
        pool,
        schema,
        data_to_decode,
        &row,
        string_arena,
    );
    out_rows[@as(usize, row_count.*)] = row;
    row_count.* += 1;
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
    string_arena: *StringArena,
) ScanError!?ResultRow {
    std.debug.assert(model_id < catalog.model_count);
    std.debug.assert(key.len > 0);

    const model = &catalog.models[model_id];
    const schema = &model.row_schema;

    const row_id_opt = btree.find(key) catch |e| return mapBTreeError(e);
    const row_id = row_id_opt orelse return null;

    const page = pool.pin(row_id.page_id) catch |e| return mapPoolError(e);
    defer pool.unpin(row_id.page_id, false);

    const row_data_opt = HeapPage.read(page, row_id.slot) catch null;

    const data_to_decode = resolveVisibleVersion(
        undo_log,
        row_id.page_id,
        row_id.slot,
        snapshot,
        tx_manager,
        row_data_opt,
    ) orelse return null;

    var row = ResultRow.init();
    row.row_id = row_id;
    try decodeRowIntoResult(
        catalog,
        pool,
        schema,
        data_to_decode,
        &row,
        string_arena,
    );
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
    result.string_storage = allocator.alloc(u8, default_string_arena_bytes) catch
        return error.OutOfMemory;
    var string_arena = StringArena.init(result.string_storage);

    const model = &catalog.models[model_id];
    const schema = &model.row_schema;

    var iter = btree.rangeScan(lo, hi) catch |e| return mapBTreeError(e);
    defer iter.close();

    while (true) {
        const entry_opt = iter.next() catch |e| return mapBTreeError(e);
        const entry = entry_opt orelse break;
        if (result.row_count >= scan_batch_size) break;

        const row_id = entry.row_id;
        const page = pool.pin(row_id.page_id) catch |e| {
            return mapPoolError(e);
        };
        defer pool.unpin(row_id.page_id, false);

        const row_data_opt = HeapPage.read(page, row_id.slot) catch null;

        const data_to_decode = resolveVisibleVersion(
            undo_log,
            row_id.page_id,
            row_id.slot,
            snapshot,
            tx_manager,
            row_data_opt,
        ) orelse continue;

        var row = ResultRow.init();
        row.row_id = row_id;
        try decodeRowIntoResult(
            catalog,
            pool,
            schema,
            data_to_decode,
            &row,
            &string_arena,
        );
        result.appendRow(row);
    }

    std.debug.assert(result.row_count <= scan_batch_size);
    return result;
}

fn decodeRowIntoResult(
    catalog: *const Catalog,
    pool: *BufferPool,
    schema: *const RowSchema,
    row_data: []const u8,
    out_row: *ResultRow,
    string_arena: *StringArena,
) ScanError!void {
    out_row.column_count = schema.column_count;
    var col_idx: u16 = 0;
    while (col_idx < schema.column_count) : (col_idx += 1) {
        const decoded = row_mod.decodeColumnStorageChecked(
            schema,
            row_data,
            col_idx,
        ) catch return error.Corruption;
        out_row.values[col_idx] = switch (decoded) {
            .value => |v| switch (v) {
                .string => |s| .{ .string = string_arena.copyString(s) catch return error.OutOfMemory },
                else => v,
            },
            .string_overflow_page_id => |first_page_id| .{
                .string = resolveOverflowStringIntoArena(
                    catalog,
                    pool,
                    first_page_id,
                    string_arena,
                ) catch |e| return e,
            },
        };
    }
}

fn resolveOverflowStringIntoArena(
    catalog: *const Catalog,
    pool: *BufferPool,
    first_page_id: u64,
    string_arena: *StringArena,
) ScanError![]const u8 {
    const overflow_allocator = &catalog.overflow_page_allocator;
    if (!overflow_allocator.ownsPageId(first_page_id)) return error.Corruption;

    const start = string_arena.startString();
    var current = first_page_id;
    var hops: u64 = 0;
    const max_hops = overflow_allocator.capacity();
    while (true) {
        if (hops >= max_hops) return error.Corruption;
        hops += 1;

        const page = pool.pin(current) catch |e| return mapPoolError(e);
        defer pool.unpin(current, false);
        if (page.header.page_type != .overflow) return error.Corruption;

        const chunk = OverflowPage.readChunk(page) catch return error.Corruption;
        string_arena.appendChunk(chunk.payload) catch return error.OutOfMemory;
        if (chunk.next_page_id == OverflowPage.null_page_id) break;
        if (!overflow_allocator.ownsPageId(chunk.next_page_id)) return error.Corruption;
        current = chunk.next_page_id;
    }

    return string_arena.finishString(start);
}

/// Resolve the visible version of a row for the given snapshot.
/// M4: rows without undo history are visible to all snapshots.
fn resolveVisibleVersion(
    undo_log: *const UndoLog,
    page_id: u64,
    slot_idx: u16,
    snapshot: *const Snapshot,
    tx_manager: *const TxManager,
    heap_data_opt: ?[]const u8,
) ?[]const u8 {
    const head = undo_log.getHead(page_id, slot_idx);
    if (head == null) return heap_data_opt;
    const vis = undo_log.findVisible(
        page_id,
        slot_idx,
        snapshot,
        tx_manager,
    );
    return vis orelse heap_data_opt;
}

fn mapPoolError(err: buffer_pool_mod.BufferPoolError) ScanError {
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

fn mapBTreeError(err: btree_mod.BTreeError) ScanError {
    return switch (err) {
        error.Corruption => error.Corruption,
        error.ChecksumMismatch => error.ChecksumMismatch,
        else => error.StorageRead,
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
    _ = try catalog.addColumn(model_id, "id", .i64, false);
    // total_pages = 0, so scan should return nothing.

    const tx = try tm.begin();
    var snap = try tm.snapshot(tx);
    defer snap.deinit();

    var result = try tableScan(
        &catalog,
        &pool,
        &undo_log,
        &snap,
        &tm,
        model_id,
        testing.allocator,
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
    _ = try catalog.addColumn(model_id, "id", .i64, false);
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
    const vals1 = [_]Value{ .{ .i64 = 1 }, .{ .string = "Alice" } };
    const len1 = try row_mod.encodeRow(schema, &vals1, &buf1);
    _ = try HeapPage.insert(page, buf1[0..len1]);

    var buf2: [256]u8 = undefined;
    const vals2 = [_]Value{ .{ .i64 = 2 }, .{ .string = "Bob" } };
    const len2 = try row_mod.encodeRow(schema, &vals2, &buf2);
    _ = try HeapPage.insert(page, buf2[0..len2]);

    pool.unpin(page_id, true);

    const tx = try tm.begin();
    var snap = try tm.snapshot(tx);
    defer snap.deinit();

    var result = try tableScan(
        &catalog,
        &pool,
        &undo_log,
        &snap,
        &tm,
        model_id,
        testing.allocator,
    );
    defer result.deinit();

    try testing.expectEqual(@as(u16, 2), result.row_count);
    try testing.expectEqual(@as(i64, 1), result.rows[0].values[0].i64);
    try testing.expectEqualSlices(u8, "Alice", result.rows[0].values[1].string);
    try testing.expectEqual(@as(i64, 2), result.rows[1].values[0].i64);
    try testing.expectEqualSlices(u8, "Bob", result.rows[1].values[1].string);
}

test "tableScanInto respects caller row capacity" {
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();
    var pool = try BufferPool.init(testing.allocator, disk.storage(), 8);
    defer pool.deinit();
    var tm = TxManager.init(testing.allocator);
    defer tm.deinit();
    var undo_log = try UndoLog.init(testing.allocator, 1024, 64 * 1024);
    defer undo_log.deinit();

    var catalog = Catalog{};
    const model_id = try catalog.addModel("Cap");
    _ = try catalog.addColumn(model_id, "id", .i64, false);
    catalog.models[model_id].heap_first_page_id = 40;
    catalog.models[model_id].total_pages = 1;

    const page = try pool.pin(40);
    HeapPage.init(page);
    const schema = &catalog.models[model_id].row_schema;

    var buf: [256]u8 = undefined;
    const vals1 = [_]Value{.{ .i64 = 11 }};
    const len1 = try row_mod.encodeRow(schema, &vals1, &buf);
    _ = try HeapPage.insert(page, buf[0..len1]);

    const vals2 = [_]Value{.{ .i64 = 22 }};
    const len2 = try row_mod.encodeRow(schema, &vals2, &buf);
    _ = try HeapPage.insert(page, buf[0..len2]);
    pool.unpin(40, true);

    const tx = try tm.begin();
    var snap = try tm.snapshot(tx);
    defer snap.deinit();

    var out_rows: [1]ResultRow = .{ResultRow.init()};
    var arena_buf: [1024]u8 = undefined;
    var string_arena = StringArena.init(arena_buf[0..]);
    const out = try tableScanInto(
        &catalog,
        &pool,
        &undo_log,
        &snap,
        &tm,
        model_id,
        &out_rows,
        &string_arena,
        null,
    );

    try testing.expectEqual(@as(u16, 1), out.row_count);
    try testing.expectEqual(@as(u32, 1), out.pages_read);
    try testing.expectEqual(@as(i64, 11), out_rows[0].values[0].i64);
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
    _ = try catalog.addColumn(model_id, "id", .i64, false);
    catalog.models[model_id].heap_first_page_id = 20;
    catalog.models[model_id].total_pages = 1;

    const page = try pool.pin(20);
    HeapPage.init(page);

    const schema = &catalog.models[model_id].row_schema;
    var buf: [256]u8 = undefined;
    const vals1 = [_]Value{.{ .i64 = 1 }};
    const len1 = try row_mod.encodeRow(schema, &vals1, &buf);
    const slot0 = try HeapPage.insert(page, buf[0..len1]);

    const vals2 = [_]Value{.{ .i64 = 2 }};
    const len2 = try row_mod.encodeRow(schema, &vals2, &buf);
    _ = try HeapPage.insert(page, buf[0..len2]);

    // Delete first row.
    try HeapPage.delete(page, slot0);
    pool.unpin(20, true);

    const tx = try tm.begin();
    var snap = try tm.snapshot(tx);
    defer snap.deinit();

    var result = try tableScan(
        &catalog,
        &pool,
        &undo_log,
        &snap,
        &tm,
        model_id,
        testing.allocator,
    );
    defer result.deinit();

    try testing.expectEqual(@as(u16, 1), result.row_count);
    try testing.expectEqual(@as(i64, 2), result.rows[0].values[0].i64);
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
    _ = try catalog.addColumn(model_id, "id", .i64, false);
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
        &catalog,
        &pool,
        &undo_log,
        &snap,
        &tm,
        model_id,
        testing.allocator,
    );
    defer result.deinit();

    try testing.expectEqual(@as(u32, 3), result.pages_read);
    try testing.expectEqual(@as(u16, 0), result.row_count);
}

test "cursor scan yields all rows across multiple chunks" {
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();
    var pool = try BufferPool.init(testing.allocator, disk.storage(), 8);
    defer pool.deinit();
    var tm = TxManager.init(testing.allocator);
    defer tm.deinit();
    var undo_log = try UndoLog.init(testing.allocator, 1024, 64 * 1024);
    defer undo_log.deinit();

    var catalog = Catalog{};
    const model_id = try catalog.addModel("Cur");
    _ = try catalog.addColumn(model_id, "id", .i64, false);
    catalog.models[model_id].heap_first_page_id = 50;
    catalog.models[model_id].total_pages = 1;

    const page = try pool.pin(50);
    HeapPage.init(page);
    const schema = &catalog.models[model_id].row_schema;
    var buf: [256]u8 = undefined;
    // Insert 5 rows.
    var i: i64 = 1;
    while (i <= 5) : (i += 1) {
        const vals = [_]Value{.{ .i64 = i }};
        const len = try row_mod.encodeRow(schema, &vals, &buf);
        _ = try HeapPage.insert(page, buf[0..len]);
    }
    pool.unpin(50, true);

    const tx = try tm.begin();
    var snap = try tm.snapshot(tx);
    defer snap.deinit();

    // Use a 2-row buffer to force multiple chunks.
    var out_rows: [2]ResultRow = .{ ResultRow.init(), ResultRow.init() };
    var arena_buf: [4096]u8 = undefined;
    var string_arena = StringArena.init(arena_buf[0..]);
    var cursor = ScanCursor.init();

    var all_ids: [5]i64 = undefined;
    var total: usize = 0;

    while (!cursor.done) {
        const res = try tableScanInto(
            &catalog,
            &pool,
            &undo_log,
            &snap,
            &tm,
            model_id,
            &out_rows,
            &string_arena,
            &cursor,
        );
        var j: u16 = 0;
        while (j < res.row_count) : (j += 1) {
            all_ids[total] = out_rows[j].values[0].i64;
            total += 1;
        }
    }

    try testing.expectEqual(@as(usize, 5), total);
    try testing.expectEqual(@as(i64, 1), all_ids[0]);
    try testing.expectEqual(@as(i64, 2), all_ids[1]);
    try testing.expectEqual(@as(i64, 3), all_ids[2]);
    try testing.expectEqual(@as(i64, 4), all_ids[3]);
    try testing.expectEqual(@as(i64, 5), all_ids[4]);
}

test "cursor with null preserves stop-at-full behavior" {
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();
    var pool = try BufferPool.init(testing.allocator, disk.storage(), 8);
    defer pool.deinit();
    var tm = TxManager.init(testing.allocator);
    defer tm.deinit();
    var undo_log = try UndoLog.init(testing.allocator, 1024, 64 * 1024);
    defer undo_log.deinit();

    var catalog = Catalog{};
    const model_id = try catalog.addModel("Null");
    _ = try catalog.addColumn(model_id, "id", .i64, false);
    catalog.models[model_id].heap_first_page_id = 60;
    catalog.models[model_id].total_pages = 1;

    const page = try pool.pin(60);
    HeapPage.init(page);
    const schema = &catalog.models[model_id].row_schema;
    var buf: [256]u8 = undefined;
    var i: i64 = 1;
    while (i <= 4) : (i += 1) {
        const vals = [_]Value{.{ .i64 = i }};
        const len = try row_mod.encodeRow(schema, &vals, &buf);
        _ = try HeapPage.insert(page, buf[0..len]);
    }
    pool.unpin(60, true);

    const tx = try tm.begin();
    var snap = try tm.snapshot(tx);
    defer snap.deinit();

    // Buffer of 2 rows, null cursor — should stop at 2 rows.
    var out_rows: [2]ResultRow = .{ ResultRow.init(), ResultRow.init() };
    var arena_buf: [4096]u8 = undefined;
    var string_arena = StringArena.init(arena_buf[0..]);

    const res = try tableScanInto(
        &catalog,
        &pool,
        &undo_log,
        &snap,
        &tm,
        model_id,
        &out_rows,
        &string_arena,
        null,
    );

    try testing.expectEqual(@as(u16, 2), res.row_count);
    try testing.expectEqual(@as(i64, 1), out_rows[0].values[0].i64);
    try testing.expectEqual(@as(i64, 2), out_rows[1].values[0].i64);
}

test "cursor scan across page boundaries" {
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();
    var pool = try BufferPool.init(testing.allocator, disk.storage(), 8);
    defer pool.deinit();
    var tm = TxManager.init(testing.allocator);
    defer tm.deinit();
    var undo_log = try UndoLog.init(testing.allocator, 1024, 64 * 1024);
    defer undo_log.deinit();

    var catalog = Catalog{};
    const model_id = try catalog.addModel("Multi");
    _ = try catalog.addColumn(model_id, "id", .i64, false);
    catalog.models[model_id].heap_first_page_id = 70;
    catalog.models[model_id].total_pages = 3;

    const schema = &catalog.models[model_id].row_schema;
    var buf: [256]u8 = undefined;
    // Put 2 rows on each of 3 pages = 6 rows total.
    var pg: u32 = 0;
    while (pg < 3) : (pg += 1) {
        const pid: u64 = 70 + pg;
        const page = try pool.pin(pid);
        HeapPage.init(page);
        var r: i64 = 0;
        while (r < 2) : (r += 1) {
            const id_val: i64 = @as(i64, pg) * 10 + r + 1;
            const vals = [_]Value{.{ .i64 = id_val }};
            const len = try row_mod.encodeRow(schema, &vals, &buf);
            _ = try HeapPage.insert(page, buf[0..len]);
        }
        pool.unpin(pid, true);
    }

    const tx = try tm.begin();
    var snap = try tm.snapshot(tx);
    defer snap.deinit();

    // Buffer of 2 rows — exactly one page worth.
    var out_rows: [2]ResultRow = .{ ResultRow.init(), ResultRow.init() };
    var arena_buf: [4096]u8 = undefined;
    var string_arena = StringArena.init(arena_buf[0..]);
    var cursor = ScanCursor.init();

    var all_ids: [6]i64 = undefined;
    var total: usize = 0;
    var chunks: u32 = 0;

    while (!cursor.done) {
        const res = try tableScanInto(
            &catalog,
            &pool,
            &undo_log,
            &snap,
            &tm,
            model_id,
            &out_rows,
            &string_arena,
            &cursor,
        );
        chunks += 1;
        var j: u16 = 0;
        while (j < res.row_count) : (j += 1) {
            all_ids[total] = out_rows[j].values[0].i64;
            total += 1;
        }
    }

    try testing.expectEqual(@as(usize, 6), total);
    try testing.expect(chunks >= 3); // At least 3 chunks (one per page).
    try testing.expectEqual(@as(i64, 1), all_ids[0]);
    try testing.expectEqual(@as(i64, 2), all_ids[1]);
    try testing.expectEqual(@as(i64, 11), all_ids[2]);
    try testing.expectEqual(@as(i64, 12), all_ids[3]);
    try testing.expectEqual(@as(i64, 21), all_ids[4]);
    try testing.expectEqual(@as(i64, 22), all_ids[5]);
}

test "cursor on empty table sets done immediately" {
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();
    var pool = try BufferPool.init(testing.allocator, disk.storage(), 8);
    defer pool.deinit();
    var tm = TxManager.init(testing.allocator);
    defer tm.deinit();
    var undo_log = try UndoLog.init(testing.allocator, 1024, 64 * 1024);
    defer undo_log.deinit();

    var catalog = Catalog{};
    const model_id = try catalog.addModel("Empty");
    _ = try catalog.addColumn(model_id, "id", .i64, false);
    // total_pages = 0 by default.

    const tx = try tm.begin();
    var snap = try tm.snapshot(tx);
    defer snap.deinit();

    var out_rows: [2]ResultRow = .{ ResultRow.init(), ResultRow.init() };
    var arena_buf: [1024]u8 = undefined;
    var string_arena = StringArena.init(arena_buf[0..]);
    var cursor = ScanCursor.init();

    const res = try tableScanInto(
        &catalog,
        &pool,
        &undo_log,
        &snap,
        &tm,
        model_id,
        &out_rows,
        &string_arena,
        &cursor,
    );

    try testing.expect(cursor.done);
    try testing.expectEqual(@as(u16, 0), res.row_count);
}
