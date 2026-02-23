//! Spilling result collector for degrade-first query execution.
//!
//! Responsibilities in this file:
//! - Wraps a pre-allocated row buffer ("hot batch") with temp page overflow.
//! - Tracks accumulated serialized bytes against a per-slot memory budget.
//! - Flushes the hot batch to temp pages when the buffer is full or the byte
//!   budget is exceeded, then continues accepting rows.
//! - Provides a streaming iterator that reads spilled pages first (in spill
//!   order), then the in-memory remainder.
//!
//! Why this exists:
//! - The 4096-slot `result_rows` buffer caps total query results at a fixed
//!   row count. For queries that return more rows or wide rows that exceed the
//!   memory budget, the collector transparently spills intermediate data to
//!   temp pages and reads it back on demand.
//! - This is the "degrade performance before failing" contract: queries slow
//!   down (disk I/O) instead of returning truncated results.
//!
//! How it works:
//! - `appendRow()` copies each row into the hot batch and tracks its serialized
//!   byte size. When the batch is full or the byte budget would be exceeded,
//!   the batch is serialized to temp pages via `SpillPageWriter` and the buffer
//!   is reset for the next batch.
//! - `iterator()` returns a streaming reader: spilled pages are read from
//!   storage via `TempStorageManager.readPage()`, deserialized row by row via
//!   `SpillPageReader`, then the in-memory remainder is returned directly.
//! - `reset()` reclaims all temp pages in O(1) via `TempPageAllocator.reset()`.
//!
//! Boundaries and non-responsibilities:
//! - This file does not decide which rows to collect; the executor pipeline
//!   feeds post-filter survivors.
//! - This file does not manage the string arena used during scan; it serializes
//!   strings inline during spill and deserializes into a caller-provided arena
//!   during iteration.
//! - Sort/group/join spill is a separate concern (Phase 3).
//!
//! Contributor notes:
//! - The spill page ID tracking array has a compile-time cap of `max_spill_pages`.
//!   This matches the default temp page budget per query slot (1024 pages).
//!   If the budget is increased beyond this, the tracking cap must also increase.
//! - `appendRow()` must not be called after `iterator()` — enforced by debug assert.
const std = @import("std");
const spill_row = @import("../storage/spill_row.zig");
const temp_mod = @import("../storage/temp.zig");
const page_mod = @import("../storage/page.zig");
const scan_mod = @import("scan.zig");

const ResultRow = scan_mod.ResultRow;
const StringArena = scan_mod.StringArena;
const TempStorageManager = temp_mod.TempStorageManager;
const TempPage = temp_mod.TempPage;
const SpillPageWriter = spill_row.SpillPageWriter;
const SpillPageReader = spill_row.SpillPageReader;
const Page = page_mod.Page;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum temp pages the collector can track. Matches the default temp page
/// budget per query slot. If `BootstrapConfig.temp_pages_per_query_slot` is
/// increased beyond this value, this constant must be raised accordingly.
pub const max_spill_pages: usize = 1024;

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

pub const CollectorError = error{
    /// The spill page tracking array is full. This means the collector has
    /// written `max_spill_pages` temp pages and cannot spill further.
    SpillTrackingOverflow,
};

// ---------------------------------------------------------------------------
// SpillingResultCollector
// ---------------------------------------------------------------------------

pub const SpillingResultCollector = struct {
    // -- Hot batch (in-memory working buffer) --
    hot_batch: []ResultRow,
    hot_count: u16,
    hot_bytes: u64,

    // -- Lifetime counters (never reset by flushHotBatch) --
    total_rows: u64,
    result_bytes_accumulated: u64,

    // -- Budget --
    work_memory_budget: u64,

    // -- Spill infrastructure --
    temp_mgr: TempStorageManager,
    spill_page_ids: [max_spill_pages]u64,
    spill_page_count: u32,

    // -- State --
    spill_triggered: bool,
    iteration_started: bool,

    /// Create a collector wrapping the given hot batch buffer.
    ///
    /// `hot_batch` is the pre-allocated row buffer (typically `QueryBuffers.result_rows`).
    /// `temp_mgr` is a freshly initialized `TempStorageManager` for this query slot.
    /// `work_memory_budget` is the byte threshold that triggers spill (e.g. 4 MB).
    pub fn init(
        hot_batch: []ResultRow,
        temp_mgr: TempStorageManager,
        work_memory_budget: u64,
    ) SpillingResultCollector {
        std.debug.assert(hot_batch.len > 0);
        std.debug.assert(hot_batch.len <= std.math.maxInt(u16));
        std.debug.assert(work_memory_budget > 0);
        return .{
            .hot_batch = hot_batch,
            .hot_count = 0,
            .hot_bytes = 0,
            .total_rows = 0,
            .result_bytes_accumulated = 0,
            .work_memory_budget = work_memory_budget,
            .temp_mgr = temp_mgr,
            .spill_page_ids = undefined,
            .spill_page_count = 0,
            .spill_triggered = false,
            .iteration_started = false,
        };
    }

    /// Append a row to the collector.
    ///
    /// If the hot batch is full or the byte budget would be exceeded, the
    /// current batch is flushed to temp pages before the new row is added.
    pub fn appendRow(
        self: *SpillingResultCollector,
        row: *const ResultRow,
    ) AppendError!void {
        std.debug.assert(!self.iteration_started);

        const row_size: u64 = try spill_row.spillRowSize(row);

        // Check if we need to flush before adding this row.
        if (self.hot_count > 0) {
            const batch_full = self.hot_count >= @as(u16, @intCast(self.hot_batch.len));
            const budget_exceeded = self.hot_bytes + row_size > self.work_memory_budget;
            if (batch_full or budget_exceeded) {
                try self.flushHotBatch();
            }
        }

        self.hot_batch[self.hot_count] = row.*;
        self.hot_count += 1;
        self.hot_bytes += row_size;
        self.total_rows += 1;
        self.result_bytes_accumulated += row_size;
    }

    /// Flush the current hot batch to temp pages.
    ///
    /// Public so the executor can force a flush as an arena safety valve
    /// (e.g. when the string arena is nearly full mid-scan).
    pub fn flushHotBatch(self: *SpillingResultCollector) FlushError!void {
        std.debug.assert(self.hot_count > 0);

        var writer = SpillPageWriter.init();
        var i: u16 = 0;
        while (i < self.hot_count) : (i += 1) {
            const row = &self.hot_batch[i];
            const ok = try writer.appendRow(row);
            if (!ok) {
                // Page full — write it out and start a new one.
                try self.writeSpillPage(&writer);
                writer.reset();
                // Retry on the fresh page. A single row that fit in
                // spillRowSize must fit in an empty page.
                const retry = try writer.appendRow(row);
                std.debug.assert(retry);
            }
        }

        // Write any remaining rows.
        if (writer.row_count > 0) {
            try self.writeSpillPage(&writer);
        }

        self.spill_triggered = true;
        self.hot_count = 0;
        self.hot_bytes = 0;
    }

    /// Write one finalized page from the writer to temp storage.
    fn writeSpillPage(self: *SpillingResultCollector, writer: *SpillPageWriter) (CollectorError || FlushStorageError)!void {
        if (self.spill_page_count >= max_spill_pages) {
            return error.SpillTrackingOverflow;
        }
        const payload = writer.finalize();
        const page_id = try self.temp_mgr.allocateAndWrite(payload, TempPage.null_page_id);
        self.spill_page_ids[self.spill_page_count] = page_id;
        self.spill_page_count += 1;
    }

    /// Return a streaming iterator over all collected rows.
    ///
    /// Spilled rows are returned first (in spill order), followed by any
    /// rows remaining in the hot batch.
    ///
    /// After calling this, `appendRow()` must not be called.
    pub fn iterator(self: *SpillingResultCollector) Iterator {
        self.iteration_started = true;
        return Iterator{
            .collector = self,
            .phase = if (self.spill_page_count > 0) .spilled else .in_memory,
            .spill_page_index = 0,
            .page_reader = undefined,
            .current_page = undefined,
            .has_active_reader = false,
            .in_memory_index = 0,
        };
    }

    /// Reclaim all temp pages and reset the collector to a clean state.
    ///
    /// The `TempStorageManager` stats (temp_pages_allocated, etc.) are NOT
    /// reset — they are cumulative for the query slot's lifetime.
    pub fn reset(self: *SpillingResultCollector) void {
        self.temp_mgr.reset();
        self.hot_count = 0;
        self.hot_bytes = 0;
        self.total_rows = 0;
        self.result_bytes_accumulated = 0;
        self.spill_page_count = 0;
        self.spill_triggered = false;
        self.iteration_started = false;
    }

    /// Total number of rows collected (spilled + in-memory).
    pub fn totalRowCount(self: *const SpillingResultCollector) u64 {
        return self.total_rows;
    }

    /// Whether any spill has occurred.
    pub fn spillTriggered(self: *const SpillingResultCollector) bool {
        return self.spill_triggered;
    }

    /// Snapshot the underlying temp storage stats.
    pub fn tempStats(self: *const SpillingResultCollector) temp_mod.TempSpillStats {
        return self.temp_mgr.snapshotStats();
    }

    // -- Error sets --

    /// Errors from temp storage writes during flush.
    const FlushStorageError = temp_mod.TempAllocatorError || temp_mod.TempPageError || temp_mod.TempStorageError;

    /// Errors possible during `flushHotBatch`.
    const FlushError = spill_row.SpillError || FlushStorageError || CollectorError;

    /// Errors possible during `appendRow`.
    pub const AppendError = FlushError;

    /// Errors possible during iteration (reading spilled pages).
    pub const IteratorError = spill_row.SpillError ||
        temp_mod.TempPageError || temp_mod.TempStorageError ||
        page_mod.PageDeserializeError;

    // -- Iterator --

    pub const Iterator = struct {
        collector: *SpillingResultCollector,
        phase: Phase,
        spill_page_index: u32,
        page_reader: SpillPageReader,
        current_page: Page,
        has_active_reader: bool,
        in_memory_index: u16,

        const Phase = enum { spilled, in_memory, done };

        /// Read the next row. Returns `true` if a row was produced, `false`
        /// when all rows have been consumed.
        ///
        /// For spilled rows, string values are decoded into `arena`.
        /// For in-memory rows, the row is copied directly (string pointers
        /// remain valid in their original arena).
        pub fn next(self: *Iterator, out: *ResultRow, arena: *StringArena) IteratorError!bool {
            switch (self.phase) {
                .spilled => return self.nextSpilled(out, arena),
                .in_memory => return self.nextInMemory(out),
                .done => return false,
            }
        }

        fn nextSpilled(self: *Iterator, out: *ResultRow, arena: *StringArena) IteratorError!bool {
            // Try current page reader first.
            if (self.has_active_reader) {
                if (try self.page_reader.next(out, arena)) return true;
                // Current page exhausted, fall through to load next.
                self.has_active_reader = false;
            }

            // Load next spill page.
            if (self.spill_page_index >= self.collector.spill_page_count) {
                // All spill pages consumed — move to in-memory phase.
                self.phase = .in_memory;
                return self.nextInMemory(out);
            }

            const page_id = self.collector.spill_page_ids[self.spill_page_index];
            self.spill_page_index += 1;

            const read_result = try self.collector.temp_mgr.readPage(page_id);
            // Store the page locally so the payload slice remains valid.
            self.current_page = read_result.page;
            // Re-derive payload from our local copy (the returned payload
            // pointed into the stack-local page inside readPage).
            const chunk = try TempPage.readChunk(&self.current_page);
            self.page_reader = try SpillPageReader.init(chunk.payload);
            self.has_active_reader = true;

            return self.page_reader.next(out, arena);
        }

        fn nextInMemory(self: *Iterator, out: *ResultRow) IteratorError!bool {
            if (self.in_memory_index >= self.collector.hot_count) {
                self.phase = .done;
                return false;
            }
            out.* = self.collector.hot_batch[self.in_memory_index];
            self.in_memory_index += 1;
            return true;
        }
    };
};

// ===========================================================================
// Tests
// ===========================================================================

const testing = std.testing;
const disk_mod = @import("../simulator/disk.zig");

fn makeRow(values: []const row_mod.Value) ResultRow {
    var row = ResultRow.init();
    row.column_count = @intCast(values.len);
    for (values, 0..) |v, i| {
        row.values[i] = v;
    }
    return row;
}

const row_mod = @import("../storage/row.zig");
const Value = row_mod.Value;

fn testArenaBuffer() [8192]u8 {
    return [_]u8{0} ** 8192;
}

fn setupTestCollector(
    disk: *disk_mod.SimulatedDisk,
    hot_batch: []ResultRow,
    budget: u64,
) SpillingResultCollector {
    const mgr = TempStorageManager.initDefault(0, disk.storage()) catch unreachable;
    return SpillingResultCollector.init(hot_batch, mgr, budget);
}

test "empty collector" {
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();
    var hot: [8]ResultRow = undefined;
    var collector = setupTestCollector(&disk, &hot, 4 * 1024 * 1024);

    try testing.expectEqual(@as(u64, 0), collector.totalRowCount());
    try testing.expect(!collector.spillTriggered());

    var arena_buf = testArenaBuffer();
    var arena = StringArena.init(&arena_buf);
    var iter = collector.iterator();
    var out = ResultRow.init();
    try testing.expect(!try iter.next(&out, &arena));
}

test "append and iterate without spill" {
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();
    var hot: [8]ResultRow = undefined;
    var collector = setupTestCollector(&disk, &hot, 4 * 1024 * 1024);

    const row1 = makeRow(&.{ .{ .i64 = 1 }, .{ .string = "alpha" } });
    const row2 = makeRow(&.{ .{ .i64 = 2 }, .{ .string = "beta" } });
    const row3 = makeRow(&.{ .{ .i64 = 3 }, .{ .string = "gamma" } });

    try collector.appendRow(&row1);
    try collector.appendRow(&row2);
    try collector.appendRow(&row3);

    try testing.expectEqual(@as(u64, 3), collector.totalRowCount());
    try testing.expect(!collector.spillTriggered());
    try testing.expectEqual(@as(u32, 0), collector.spill_page_count);

    var arena_buf = testArenaBuffer();
    var arena = StringArena.init(&arena_buf);
    var iter = collector.iterator();
    var out = ResultRow.init();

    try testing.expect(try iter.next(&out, &arena));
    try testing.expectEqual(@as(i64, 1), out.values[0].i64);
    try testing.expectEqualStrings("alpha", out.values[1].string);

    try testing.expect(try iter.next(&out, &arena));
    try testing.expectEqual(@as(i64, 2), out.values[0].i64);
    try testing.expectEqualStrings("beta", out.values[1].string);

    try testing.expect(try iter.next(&out, &arena));
    try testing.expectEqual(@as(i64, 3), out.values[0].i64);
    try testing.expectEqualStrings("gamma", out.values[1].string);

    try testing.expect(!try iter.next(&out, &arena));
}

test "hot batch full triggers spill" {
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    // Tiny hot batch of 4 rows.
    var hot: [4]ResultRow = undefined;
    var collector = setupTestCollector(&disk, &hot, 4 * 1024 * 1024);

    // Append 6 rows — first 4 fill the batch, 5th triggers spill.
    var i: i64 = 0;
    while (i < 6) : (i += 1) {
        const row = makeRow(&.{.{ .i64 = i + 1 }});
        try collector.appendRow(&row);
    }

    try testing.expectEqual(@as(u64, 6), collector.totalRowCount());
    try testing.expect(collector.spillTriggered());
    try testing.expect(collector.spill_page_count > 0);

    // Iterate and verify all rows in order.
    var arena_buf = testArenaBuffer();
    var arena = StringArena.init(&arena_buf);
    var iter = collector.iterator();
    var out = ResultRow.init();
    var expected: i64 = 1;
    while (try iter.next(&out, &arena)) {
        try testing.expectEqual(expected, out.values[0].i64);
        expected += 1;
    }
    try testing.expectEqual(@as(i64, 7), expected); // 6 rows + 1
}

test "byte budget triggers spill before batch capacity" {
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    // Large hot batch (won't fill by count) but tiny byte budget.
    var hot: [64]ResultRow = undefined;
    // Budget of 50 bytes — a row with a 30-char string serializes to ~38 bytes,
    // so second row should trigger spill.
    var collector = setupTestCollector(&disk, &hot, 50);

    const str = [_]u8{'x'} ** 30;
    const row1 = makeRow(&.{.{ .string = &str }});
    const row2 = makeRow(&.{.{ .string = &str }});
    const row3 = makeRow(&.{.{ .i64 = 99 }});

    try collector.appendRow(&row1);
    try testing.expect(!collector.spillTriggered());

    try collector.appendRow(&row2);
    try testing.expect(collector.spillTriggered());

    try collector.appendRow(&row3);

    // Verify all three rows come back.
    var arena_buf = testArenaBuffer();
    var arena = StringArena.init(&arena_buf);
    var iter = collector.iterator();
    var out = ResultRow.init();
    var count: u64 = 0;

    while (try iter.next(&out, &arena)) {
        count += 1;
    }
    try testing.expectEqual(@as(u64, 3), count);
    try testing.expectEqual(@as(u64, 3), collector.totalRowCount());
}

test "multi-batch spill preserves order" {
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    // Hot batch of 2 rows — forces spill every 2 rows.
    var hot: [2]ResultRow = undefined;
    var collector = setupTestCollector(&disk, &hot, 4 * 1024 * 1024);

    // Append 7 rows: flushes at rows 3, 5, 7 (partial batch in memory: depends on count).
    var i: i64 = 0;
    while (i < 7) : (i += 1) {
        const row = makeRow(&.{.{ .i64 = (i + 1) * 10 }});
        try collector.appendRow(&row);
    }

    try testing.expectEqual(@as(u64, 7), collector.totalRowCount());
    try testing.expect(collector.spillTriggered());

    // Iterate and verify order.
    var arena_buf = testArenaBuffer();
    var arena = StringArena.init(&arena_buf);
    var iter = collector.iterator();
    var out = ResultRow.init();
    var expected: i64 = 10;
    while (try iter.next(&out, &arena)) {
        try testing.expectEqual(expected, out.values[0].i64);
        expected += 10;
    }
    try testing.expectEqual(@as(i64, 80), expected); // 7 * 10 + 10
}

test "mixed spill and in-memory" {
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    // Hot batch of 3 rows.
    var hot: [3]ResultRow = undefined;
    var collector = setupTestCollector(&disk, &hot, 4 * 1024 * 1024);

    // Append 5 rows. Batch fills at 3, row 4 triggers spill. Rows 4,5 in hot batch.
    const row_a = makeRow(&.{ .{ .i64 = 1 }, .{ .string = "first" } });
    const row_b = makeRow(&.{ .{ .i64 = 2 }, .{ .string = "second" } });
    const row_c = makeRow(&.{ .{ .i64 = 3 }, .{ .string = "third" } });
    const row_d = makeRow(&.{ .{ .i64 = 4 }, .{ .string = "fourth" } });
    const row_e = makeRow(&.{ .{ .i64 = 5 }, .{ .string = "fifth" } });

    try collector.appendRow(&row_a);
    try collector.appendRow(&row_b);
    try collector.appendRow(&row_c);
    try testing.expect(!collector.spillTriggered());

    try collector.appendRow(&row_d);
    try testing.expect(collector.spillTriggered());

    try collector.appendRow(&row_e);

    // 3 spilled rows + 2 in-memory.
    try testing.expectEqual(@as(u64, 5), collector.totalRowCount());
    try testing.expectEqual(@as(u16, 2), collector.hot_count);

    var arena_buf = testArenaBuffer();
    var arena = StringArena.init(&arena_buf);
    var iter = collector.iterator();
    var out = ResultRow.init();

    // Spilled: rows 1, 2, 3
    try testing.expect(try iter.next(&out, &arena));
    try testing.expectEqual(@as(i64, 1), out.values[0].i64);
    try testing.expectEqualStrings("first", out.values[1].string);

    try testing.expect(try iter.next(&out, &arena));
    try testing.expectEqual(@as(i64, 2), out.values[0].i64);

    try testing.expect(try iter.next(&out, &arena));
    try testing.expectEqual(@as(i64, 3), out.values[0].i64);

    // In-memory: rows 4, 5
    try testing.expect(try iter.next(&out, &arena));
    try testing.expectEqual(@as(i64, 4), out.values[0].i64);
    try testing.expectEqualStrings("fourth", out.values[1].string);

    try testing.expect(try iter.next(&out, &arena));
    try testing.expectEqual(@as(i64, 5), out.values[0].i64);
    try testing.expectEqualStrings("fifth", out.values[1].string);

    try testing.expect(!try iter.next(&out, &arena));
}

test "reset reclaims pages and zeros counters" {
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    var hot: [2]ResultRow = undefined;
    var collector = setupTestCollector(&disk, &hot, 4 * 1024 * 1024);

    // Force a spill.
    var i: i64 = 0;
    while (i < 4) : (i += 1) {
        const row = makeRow(&.{.{ .i64 = i }});
        try collector.appendRow(&row);
    }
    try testing.expect(collector.spillTriggered());
    try testing.expect(collector.spill_page_count > 0);

    // Capture stats before reset — they should persist.
    const stats_before = collector.tempStats();
    try testing.expect(stats_before.temp_pages_allocated > 0);

    collector.reset();

    try testing.expectEqual(@as(u64, 0), collector.totalRowCount());
    try testing.expectEqual(@as(u16, 0), collector.hot_count);
    try testing.expectEqual(@as(u64, 0), collector.hot_bytes);
    try testing.expectEqual(@as(u64, 0), collector.result_bytes_accumulated);
    try testing.expectEqual(@as(u32, 0), collector.spill_page_count);
    try testing.expect(!collector.spillTriggered());
    try testing.expect(!collector.iteration_started);

    // Temp storage stats are cumulative — reset doesn't clear them.
    const stats_after = collector.tempStats();
    try testing.expect(stats_after.temp_pages_allocated > 0);
    try testing.expect(stats_after.temp_pages_reclaimed > 0);

    // Pages are reclaimed.
    try testing.expectEqual(@as(u64, 0), collector.temp_mgr.pagesInUse());
}

test "single row exceeding budget" {
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    var hot: [8]ResultRow = undefined;
    // Budget so small that even one row exceeds it.
    var collector = setupTestCollector(&disk, &hot, 5);

    const big_str = [_]u8{'A'} ** 100;
    const big_row = makeRow(&.{.{ .string = &big_str }});

    // First row: budget exceeded but hot_count==0 so no flush yet.
    try collector.appendRow(&big_row);
    try testing.expect(!collector.spillTriggered());
    try testing.expectEqual(@as(u16, 1), collector.hot_count);

    // Second row: hot_count > 0 and budget exceeded, triggers flush of first row.
    const small_row = makeRow(&.{.{ .i64 = 42 }});
    try collector.appendRow(&small_row);
    try testing.expect(collector.spillTriggered());

    // Verify both rows come back.
    var arena_buf = testArenaBuffer();
    var arena = StringArena.init(&arena_buf);
    var iter = collector.iterator();
    var out = ResultRow.init();

    try testing.expect(try iter.next(&out, &arena));
    try testing.expectEqualStrings(&big_str, out.values[0].string);

    try testing.expect(try iter.next(&out, &arena));
    try testing.expectEqual(@as(i64, 42), out.values[0].i64);

    try testing.expect(!try iter.next(&out, &arena));
}

test "result_bytes_accumulated tracks lifetime across flushes" {
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    var hot: [2]ResultRow = undefined;
    var collector = setupTestCollector(&disk, &hot, 4 * 1024 * 1024);

    const row = makeRow(&.{.{ .i64 = 1 }});
    const row_size: u64 = spill_row.spillRowSize(&row) catch unreachable;

    // Append 5 rows. With batch size 2, flushes happen at rows 3 and 5.
    var i: u64 = 0;
    while (i < 5) : (i += 1) {
        try collector.appendRow(&row);
    }

    // result_bytes_accumulated should be 5 * row_size (never resets).
    try testing.expectEqual(5 * row_size, collector.result_bytes_accumulated);
    // hot_bytes tracks only current batch content.
    try testing.expect(collector.hot_bytes < collector.result_bytes_accumulated);
}

test "deterministic roundtrip" {
    var disk1 = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk1.deinit();
    var disk2 = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk2.deinit();

    const rows_to_add = [_]ResultRow{
        makeRow(&.{ .{ .i64 = 100 }, .{ .string = "hello" } }),
        makeRow(&.{ .{ .bool = true }, .{ .f64 = 3.14 } }),
        makeRow(&.{ .{ .null_value = {} }, .{ .i32 = -7 }, .{ .string = "world" } }),
        makeRow(&.{.{ .u64 = 999 }}),
        makeRow(&.{ .{ .string = "determinism" }, .{ .timestamp = 1_700_000_000 } }),
    };

    // Run the same sequence through two independent collectors.
    var results1: [5]ResultRow = undefined;
    var results2: [5]ResultRow = undefined;

    inline for (0..2) |pass| {
        const disk = if (pass == 0) &disk1 else &disk2;
        const results = if (pass == 0) &results1 else &results2;

        var hot: [2]ResultRow = undefined;
        var collector = setupTestCollector(disk, &hot, 4 * 1024 * 1024);

        for (&rows_to_add) |*r| {
            try collector.appendRow(r);
        }

        var arena_buf = testArenaBuffer();
        var arena = StringArena.init(&arena_buf);
        var iter = collector.iterator();
        var out = ResultRow.init();
        var idx: usize = 0;
        while (try iter.next(&out, &arena)) {
            results[idx] = out;
            idx += 1;
        }
        try testing.expectEqual(@as(usize, 5), idx);
    }

    // Compare row by row.
    for (0..5) |idx| {
        const r1 = &results1[idx];
        const r2 = &results2[idx];
        try testing.expectEqual(r1.column_count, r2.column_count);
        for (0..r1.column_count) |c| {
            const v1 = r1.values[c];
            const v2 = r2.values[c];
            switch (v1) {
                .i64 => try testing.expectEqual(v1.i64, v2.i64),
                .i32 => try testing.expectEqual(v1.i32, v2.i32),
                .u64 => try testing.expectEqual(v1.u64, v2.u64),
                .f64 => try testing.expectEqual(v1.f64, v2.f64),
                .bool => try testing.expectEqual(v1.bool, v2.bool),
                .string => try testing.expectEqualStrings(v1.string, v2.string),
                .null_value => try testing.expect(v2 == .null_value),
                .timestamp => try testing.expectEqual(v1.timestamp, v2.timestamp),
                else => {},
            }
        }
    }
}

test "spill with many rows across multiple pages" {
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    // Hot batch of 4, append 20 rows — forces multiple spills.
    var hot: [4]ResultRow = undefined;
    var collector = setupTestCollector(&disk, &hot, 4 * 1024 * 1024);

    var i: i64 = 0;
    while (i < 20) : (i += 1) {
        const row = makeRow(&.{ .{ .i64 = i }, .{ .string = "row-data" } });
        try collector.appendRow(&row);
    }

    try testing.expectEqual(@as(u64, 20), collector.totalRowCount());
    try testing.expect(collector.spillTriggered());

    // Iterate all 20 rows and verify order.
    var arena_buf = testArenaBuffer();
    var arena = StringArena.init(&arena_buf);
    var iter = collector.iterator();
    var out = ResultRow.init();
    var expected: i64 = 0;
    while (try iter.next(&out, &arena)) {
        try testing.expectEqual(expected, out.values[0].i64);
        try testing.expectEqualStrings("row-data", out.values[1].string);
        expected += 1;
    }
    try testing.expectEqual(@as(i64, 20), expected);
}

test "collector reuse after reset" {
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    var hot: [2]ResultRow = undefined;
    var collector = setupTestCollector(&disk, &hot, 4 * 1024 * 1024);

    // First collection cycle with spill.
    var i: i64 = 0;
    while (i < 5) : (i += 1) {
        const row = makeRow(&.{.{ .i64 = i }});
        try collector.appendRow(&row);
    }
    try testing.expect(collector.spillTriggered());

    collector.reset();

    // Second cycle — should work cleanly.
    i = 100;
    while (i < 103) : (i += 1) {
        const row = makeRow(&.{.{ .i64 = i }});
        try collector.appendRow(&row);
    }

    try testing.expectEqual(@as(u64, 3), collector.totalRowCount());

    var arena_buf = testArenaBuffer();
    var arena = StringArena.init(&arena_buf);
    var iter = collector.iterator();
    var out = ResultRow.init();
    var expected: i64 = 100;
    while (try iter.next(&out, &arena)) {
        try testing.expectEqual(expected, out.values[0].i64);
        expected += 1;
    }
    try testing.expectEqual(@as(i64, 103), expected);
}
