//! External merge sort for ORDER BY with spilled input data.
//!
//! Responsibilities in this file:
//! - Reads arbitrarily large input from `SpillingResultCollector` in batches.
//! - Sorts each batch in-memory via Phase 3a's merge sort, producing "runs".
//! - Serializes each sorted run to temp pages via `SpillPageWriter`.
//! - Merges K sorted runs via a min-heap k-way merge.
//! - Outputs sorted data to `result.rows` (if fits) or to fresh temp pages.
//!
//! Why this exists:
//! - Phase 2's chunked scan + spill can produce more rows than `scan_batch_size`
//!   (4096). The in-memory merge sort (Phase 3a) is limited to `scan_batch_size`
//!   rows. External merge sort removes this limitation: total input is bounded
//!   only by `work_memory_bytes_per_slot` + temp page budget.
//!
//! How it works:
//! - **Run generation**: Flush the collector's hot batch so all data is in spill
//!   pages. Iterate the collector in batches of `scan_batch_size` rows. Sort each
//!   batch in-memory and write the sorted batch to temp pages as a "run".
//! - **K-way merge**: One `RunMergeState` per run holds a `SpillPageReader` and
//!   the loaded `Page`. A fixed-size min-heap (max 16 entries) drives the merge.
//!   Ties are broken by run index for stability (earlier runs win).
//! - **Output**: If total rows ≤ `scan_batch_size`, the merged output is written
//!   directly into `result.rows`. Otherwise, merged output is serialized to fresh
//!   temp pages and the collector is reconfigured to iterate those pages.
//!
//! Boundaries and non-responsibilities:
//! - This file does not decide when to invoke external sort; the executor
//!   pipeline routes to it when scan spill has occurred and a sort is needed.
//! - Multi-pass merge (merging runs into larger runs) is deferred; the single-
//!   pass approach handles K ≤ `max_merge_runs` (typically K ≤ 4 with defaults).
//! - GROUP/HAVING/LIMIT/OFFSET are applied by the executor after sort completes.
//!
//! Contributor notes:
//! - `max_merge_runs` is a compile-time bound. With defaults (4 MB work_mem,
//!   8 MB temp budget), K ≤ 2 for most workloads.
//! - The string arena is reset between run-generation batches (safe because
//!   strings are serialized inline into temp pages). During k-way merge, the
//!   arena grows monotonically; a rescue-and-reset mechanism handles overflow.
//! - All comparisons reuse `sorting.compareRowsBySortKeys()` for consistency.
const std = @import("std");
const ast_mod = @import("../parser/ast.zig");
const row_mod = @import("../storage/row.zig");
const scan_mod = @import("scan.zig");
const temp_mod = @import("../storage/temp.zig");
const page_mod = @import("../storage/page.zig");
const spill_row = @import("../storage/spill_row.zig");
const sorting_mod = @import("sorting.zig");
const capacity_mod = @import("capacity.zig");
const spill_collector_mod = @import("spill_collector.zig");
const aggregation_mod = @import("aggregation.zig");

const NodeIndex = ast_mod.NodeIndex;
const null_node = ast_mod.null_node;
const Value = row_mod.Value;
const RowSchema = row_mod.RowSchema;
const ResultRow = scan_mod.ResultRow;
const StringArena = scan_mod.StringArena;
const TempStorageManager = temp_mod.TempStorageManager;
const TempPage = temp_mod.TempPage;
const Page = page_mod.Page;
const SpillPageWriter = spill_row.SpillPageWriter;
const SpillPageReader = spill_row.SpillPageReader;
const SpillingResultCollector = spill_collector_mod.SpillingResultCollector;
const SortKeyDescriptor = sorting_mod.SortKeyDescriptor;
const GroupRuntime = aggregation_mod.GroupRuntime;
const ExecContext = @import("executor.zig").ExecContext;
const QueryResult = @import("executor.zig").QueryResult;
const setError = @import("executor.zig").setError;

const max_sort_keys = capacity_mod.max_sort_keys;
const max_spill_pages = spill_collector_mod.max_spill_pages;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum number of sorted runs that can be merged in a single pass.
/// With defaults (4 MB work_mem, 8 MB temp budget), K ≤ 2 for typical loads.
/// 16 supports up to 16 × 4 MB = 64 MB of sort input in one pass.
pub const max_merge_runs: usize = 16;

/// Size of the rescue buffer for heap entry strings during arena reset.
/// Sized for max_merge_runs rows with moderate string content.
const rescue_arena_size: usize = 64 * 1024;

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

pub const ExternalSortError = error{
    /// Too many sorted runs for single-pass merge.
    TooManyRuns,
    /// Temp page budget exhausted during run generation or merge output.
    TempPagesExhausted,
    /// Sort key evaluation failed during comparison.
    SortKeyEvalFailed,
    /// Spill serialization or deserialization failed.
    SpillError,
    /// String arena exhausted during merge with no rescue possible.
    ArenaExhausted,
    /// Storage read/write failure.
    StorageError,
    /// Run generation produced zero runs (internal invariant violation).
    InternalError,
};

// ---------------------------------------------------------------------------
// Run metadata
// ---------------------------------------------------------------------------

const RunInfo = struct {
    /// Index into the shared `all_page_ids` array where this run's pages start.
    page_id_start: u32,
    /// Number of temp pages in this run.
    page_count: u32,
    /// Number of rows in this run (bounded by scan_batch_size per run).
    row_count: u32,
};

// ---------------------------------------------------------------------------
// Min-heap for k-way merge
// ---------------------------------------------------------------------------

/// Fixed-size binary min-heap for k-way merge.
///
/// Each entry is a run index. The comparison consults `run_heads[run_index]`
/// using sort key descriptors. Ties are broken by run index (lower wins)
/// to preserve stability across runs.
const MinHeap = struct {
    entries: [max_merge_runs]u16,
    size: u16,

    fn init() MinHeap {
        return .{
            .entries = undefined,
            .size = 0,
        };
    }

    fn push(
        self: *MinHeap,
        run_index: u16,
        run_heads: []const ResultRow,
        ctx: *const ExecContext,
        schema: *const RowSchema,
        sort_keys: []const SortKeyDescriptor,
        string_arena: *StringArena,
    ) ExternalSortError!void {
        std.debug.assert(self.size < max_merge_runs);
        self.entries[self.size] = run_index;
        self.size += 1;
        try self.siftUp(self.size - 1, run_heads, ctx, schema, sort_keys, string_arena);
    }

    fn pop(
        self: *MinHeap,
        run_heads: []const ResultRow,
        ctx: *const ExecContext,
        schema: *const RowSchema,
        sort_keys: []const SortKeyDescriptor,
        string_arena: *StringArena,
    ) ExternalSortError!u16 {
        std.debug.assert(self.size > 0);
        const result = self.entries[0];
        self.size -= 1;
        if (self.size > 0) {
            self.entries[0] = self.entries[self.size];
            try self.siftDown(0, run_heads, ctx, schema, sort_keys, string_arena);
        }
        return result;
    }

    fn siftUp(
        self: *MinHeap,
        start_idx: u16,
        run_heads: []const ResultRow,
        ctx: *const ExecContext,
        schema: *const RowSchema,
        sort_keys: []const SortKeyDescriptor,
        string_arena: *StringArena,
    ) ExternalSortError!void {
        var idx = start_idx;
        while (idx > 0) {
            const parent = (idx - 1) / 2;
            if (try self.isLess(idx, parent, run_heads, ctx, schema, sort_keys, string_arena)) {
                const tmp = self.entries[idx];
                self.entries[idx] = self.entries[parent];
                self.entries[parent] = tmp;
                idx = parent;
            } else {
                break;
            }
        }
    }

    fn siftDown(
        self: *MinHeap,
        start_idx: u16,
        run_heads: []const ResultRow,
        ctx: *const ExecContext,
        schema: *const RowSchema,
        sort_keys: []const SortKeyDescriptor,
        string_arena: *StringArena,
    ) ExternalSortError!void {
        var idx = start_idx;
        while (true) {
            var smallest = idx;
            const left = 2 * idx + 1;
            const right = 2 * idx + 2;

            if (left < self.size) {
                if (try self.isLess(left, smallest, run_heads, ctx, schema, sort_keys, string_arena)) {
                    smallest = left;
                }
            }
            if (right < self.size) {
                if (try self.isLess(right, smallest, run_heads, ctx, schema, sort_keys, string_arena)) {
                    smallest = right;
                }
            }

            if (smallest == idx) break;

            const tmp = self.entries[idx];
            self.entries[idx] = self.entries[smallest];
            self.entries[smallest] = tmp;
            idx = smallest;
        }
    }

    /// Returns true if entry at `a` is strictly less than entry at `b`.
    /// Comparison by sort keys, then by run index for stability.
    fn isLess(
        self: *const MinHeap,
        a: u16,
        b: u16,
        run_heads: []const ResultRow,
        ctx: *const ExecContext,
        schema: *const RowSchema,
        sort_keys: []const SortKeyDescriptor,
        string_arena: *StringArena,
    ) ExternalSortError!bool {
        const run_a = self.entries[a];
        const run_b = self.entries[b];
        var group_runtime = GroupRuntime{};

        const order = sorting_mod.compareRowsBySortKeys(
            ctx,
            schema,
            &group_runtime,
            0,
            0,
            &run_heads[run_a],
            &run_heads[run_b],
            sort_keys,
            string_arena,
        ) catch return error.SortKeyEvalFailed;

        return switch (order) {
            .lt => true,
            .gt => false,
            // Equal keys: lower run index wins (stability).
            .eq => run_a < run_b,
        };
    }
};

// ---------------------------------------------------------------------------
// Run merge state — per-run state during k-way merge
// ---------------------------------------------------------------------------

const RunMergeState = struct {
    page: Page,
    reader: SpillPageReader,
    has_reader: bool,
    page_id_start: u32,
    page_count: u32,
    next_page_offset: u32,
    exhausted: bool,

    fn initFromRun(run: *const RunInfo) RunMergeState {
        return .{
            .page = undefined,
            .reader = undefined,
            .has_reader = false,
            .page_id_start = run.page_id_start,
            .page_count = run.page_count,
            .next_page_offset = 0,
            .exhausted = false,
        };
    }

    /// Advance to the next row. Returns true if a row was produced.
    fn advance(
        self: *RunMergeState,
        out: *ResultRow,
        arena: *StringArena,
        temp_mgr: *TempStorageManager,
        all_page_ids: []const u64,
    ) ExternalSortError!bool {
        // Try current page reader first.
        if (self.has_reader) {
            const has_row = self.reader.next(out, arena) catch
                return error.SpillError;
            if (has_row) return true;
            self.has_reader = false;
        }

        // Load next page.
        if (self.next_page_offset >= self.page_count) {
            self.exhausted = true;
            return false;
        }

        const page_id = all_page_ids[self.page_id_start + self.next_page_offset];
        self.next_page_offset += 1;

        const read_result = temp_mgr.readPage(page_id) catch
            return error.StorageError;
        self.page = read_result.page;
        const chunk = TempPage.readChunk(&self.page) catch
            return error.SpillError;
        self.reader = SpillPageReader.init(chunk.payload) catch
            return error.SpillError;
        self.has_reader = true;

        const has_row = self.reader.next(out, arena) catch
            return error.SpillError;
        if (!has_row) {
            self.exhausted = true;
        }
        return has_row;
    }
};

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Perform external merge sort on data in the collector.
///
/// Preconditions:
/// - `collector.spillTriggered()` is true (there is spilled data).
/// - `collector.totalRowCount()` > 0.
///
/// On success, one of two outcomes:
/// - **In-memory output**: sorted rows are in `result.rows[0..row_count]` and
///   `result.collector` is null. Downstream operators work on the flat buffer.
/// - **Spilled output**: sorted rows are in temp pages; `result.collector` is
///   set so serialization iterates the sorted pages.
///
/// Returns `true` on success, `false` on error (error message set in result).
pub fn applyExternalSort(
    ctx: *const ExecContext,
    result: *QueryResult,
    collector: *SpillingResultCollector,
    sort_node: NodeIndex,
    schema: *const RowSchema,
    string_arena: *StringArena,
) bool {
    const node = ctx.ast.getNode(sort_node);
    const key_count = ctx.ast.listLen(node.data.unary);
    if (key_count == 0) {
        setError(result, "sort requires at least one key");
        return false;
    }
    if (@as(usize, key_count) > max_sort_keys) {
        setError(result, "sort capacity exceeded");
        return false;
    }

    var sort_keys: [max_sort_keys]SortKeyDescriptor = undefined;
    if (!sorting_mod.buildSortKeyDescriptors(
        ctx,
        result,
        node.data.unary,
        schema,
        sort_keys[0..],
        key_count,
    )) {
        return false;
    }

    applyExternalSortImpl(
        ctx,
        result,
        collector,
        sort_keys[0..key_count],
        schema,
        string_arena,
    ) catch |err| {
        const msg = switch (err) {
            error.TooManyRuns => "external sort: too many runs for single-pass merge",
            error.TempPagesExhausted => "external sort: temp page budget exhausted",
            error.SortKeyEvalFailed => "external sort: sort key evaluation failed",
            error.SpillError => "external sort: spill serialization failed",
            error.ArenaExhausted => "external sort: string arena exhausted",
            error.StorageError => "external sort: storage I/O error",
            error.InternalError => "external sort: internal error",
        };
        setError(result, msg);
        return false;
    };
    return true;
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

fn applyExternalSortImpl(
    ctx: *const ExecContext,
    result: *QueryResult,
    collector: *SpillingResultCollector,
    sort_keys: []const SortKeyDescriptor,
    schema: *const RowSchema,
    string_arena: *StringArena,
) ExternalSortError!void {
    // Step 1: Flush hot batch so all data is in spill pages and result.rows
    // is free for use as a working buffer.
    if (collector.hot_count > 0) {
        collector.flushHotBatch() catch return error.SpillError;
    }

    // Step 2: Generate sorted runs.
    var all_page_ids: [max_spill_pages]u64 = undefined;
    var runs: [max_merge_runs]RunInfo = undefined;
    var run_count: u16 = 0;
    var total_page_id_count: u32 = 0;

    try generateSortedRuns(
        ctx,
        result,
        sort_keys,
        schema,
        string_arena,
        collector,
        &all_page_ids,
        &runs,
        &run_count,
        &total_page_id_count,
    );

    if (run_count == 0) return error.InternalError;

    // Step 3: Merge and output.
    const total_rows = collector.totalRowCount();

    if (run_count == 1 and total_rows <= scan_mod.scan_batch_size) {
        // Single run that fits in memory — read directly into result.rows.
        try readRunIntoBuffer(
            result.rows,
            &runs[0],
            all_page_ids[0..total_page_id_count],
            &collector.temp_mgr,
            string_arena,
        );
        result.row_count = @intCast(total_rows);
        result.collector = null;
    } else if (run_count == 1) {
        // Single run but too large for memory — point collector at run pages.
        reconfigureCollectorForSortedOutput(
            collector,
            all_page_ids[runs[0].page_id_start..][0..runs[0].page_count],
            total_rows,
        );
        result.row_count = @intCast(@min(total_rows, scan_mod.scan_batch_size));
        result.collector = collector;
    } else {
        // K-way merge required.
        try kWayMerge(
            ctx,
            result,
            sort_keys,
            schema,
            string_arena,
            collector,
            all_page_ids[0..total_page_id_count],
            runs[0..run_count],
            total_rows,
        );
    }
}

/// Generate sorted runs from the collector's spilled data.
///
/// Reads batches from the collector iterator, sorts each in-memory,
/// and writes sorted batches to temp pages.
fn generateSortedRuns(
    ctx: *const ExecContext,
    result: *QueryResult,
    sort_keys: []const SortKeyDescriptor,
    schema: *const RowSchema,
    string_arena: *StringArena,
    collector: *SpillingResultCollector,
    all_page_ids: *[max_spill_pages]u64,
    runs: *[max_merge_runs]RunInfo,
    run_count: *u16,
    total_page_id_count: *u32,
) ExternalSortError!void {
    var iter = collector.iterator();
    var row_buf = result.rows;
    var group_runtime = GroupRuntime{};

    while (true) {
        // Read a batch of rows into result.rows.
        string_arena.reset();
        var batch_count: u16 = 0;
        while (batch_count < scan_mod.scan_batch_size) {
            const has_row = iter.next(&row_buf[batch_count], string_arena) catch
                return error.SpillError;
            if (!has_row) break;
            batch_count += 1;
        }

        if (batch_count == 0) break;

        // Sort the batch in-memory.
        result.row_count = batch_count;
        sorting_mod.sortRowsMerge(
            ctx,
            result,
            schema,
            sort_keys,
            &group_runtime,
            string_arena,
        ) catch return error.SortKeyEvalFailed;

        // Write sorted batch to temp pages.
        if (run_count.* >= max_merge_runs) return error.TooManyRuns;

        const run_page_start = total_page_id_count.*;
        var writer = SpillPageWriter.init();
        var i: u16 = 0;
        while (i < batch_count) : (i += 1) {
            const ok = writer.appendRow(&row_buf[i]) catch
                return error.SpillError;
            if (!ok) {
                // Page full — flush it.
                if (total_page_id_count.* >= max_spill_pages)
                    return error.TempPagesExhausted;
                const payload = writer.finalize();
                const page_id = collector.temp_mgr.allocateAndWrite(
                    payload,
                    TempPage.null_page_id,
                ) catch return error.TempPagesExhausted;
                all_page_ids[total_page_id_count.*] = page_id;
                total_page_id_count.* += 1;
                writer.reset();
                // Retry on fresh page.
                const retry = writer.appendRow(&row_buf[i]) catch
                    return error.SpillError;
                if (!retry) return error.SpillError;
            }
        }
        // Flush remaining rows.
        if (writer.row_count > 0) {
            if (total_page_id_count.* >= max_spill_pages)
                return error.TempPagesExhausted;
            const payload = writer.finalize();
            const page_id = collector.temp_mgr.allocateAndWrite(
                payload,
                TempPage.null_page_id,
            ) catch return error.TempPagesExhausted;
            all_page_ids[total_page_id_count.*] = page_id;
            total_page_id_count.* += 1;
        }

        runs[run_count.*] = .{
            .page_id_start = run_page_start,
            .page_count = total_page_id_count.* - run_page_start,
            .row_count = batch_count,
        };
        run_count.* += 1;
    }
}

/// Read a single run's sorted rows back into a buffer.
fn readRunIntoBuffer(
    buf: []ResultRow,
    run: *const RunInfo,
    all_page_ids: []const u64,
    temp_mgr: *TempStorageManager,
    string_arena: *StringArena,
) ExternalSortError!void {
    string_arena.reset();
    var row_idx: u64 = 0;
    var page_offset: u32 = 0;
    while (page_offset < run.page_count) : (page_offset += 1) {
        const page_id = all_page_ids[run.page_id_start + page_offset];
        const read_result = temp_mgr.readPage(page_id) catch
            return error.StorageError;
        // Must copy page locally so payload stays valid.
        var local_page = read_result.page;
        const chunk = TempPage.readChunk(&local_page) catch
            return error.SpillError;
        var reader = SpillPageReader.init(chunk.payload) catch
            return error.SpillError;
        var out = ResultRow.init();
        while (reader.next(&out, string_arena) catch return error.SpillError) {
            if (row_idx >= buf.len) return error.InternalError;
            buf[@intCast(row_idx)] = out;
            row_idx += 1;
        }
    }
}

/// K-way merge of sorted runs.
fn kWayMerge(
    ctx: *const ExecContext,
    result: *QueryResult,
    sort_keys: []const SortKeyDescriptor,
    schema: *const RowSchema,
    string_arena: *StringArena,
    collector: *SpillingResultCollector,
    all_page_ids: []const u64,
    runs: []const RunInfo,
    total_rows: u64,
) ExternalSortError!void {
    const k = runs.len;
    std.debug.assert(k >= 2);
    std.debug.assert(k <= max_merge_runs);

    // Initialize per-run state.
    var run_states: [max_merge_runs]RunMergeState = undefined;
    for (runs, 0..) |*run, i| {
        run_states[i] = RunMergeState.initFromRun(run);
    }

    // Run head rows — the current "smallest" row from each run.
    var run_heads: [max_merge_runs]ResultRow = undefined;

    // Build the initial heap.
    string_arena.reset();
    var heap = MinHeap.init();
    for (0..k) |i| {
        const has_row = try run_states[i].advance(
            &run_heads[i],
            string_arena,
            &collector.temp_mgr,
            all_page_ids,
        );
        if (has_row) {
            try heap.push(
                @intCast(i),
                &run_heads,
                ctx,
                schema,
                sort_keys,
                string_arena,
            );
        }
    }

    // Output setup.
    const output_in_memory = total_rows <= scan_mod.scan_batch_size;
    var output_row_idx: u64 = 0;

    // For spilled output: track page IDs.
    var output_page_ids: [max_spill_pages]u64 = undefined;
    var output_page_count: u32 = 0;
    var writer = SpillPageWriter.init();

    // Rescue arena for heap strings.
    var rescue_buf: [rescue_arena_size]u8 = undefined;

    // Merge loop.
    while (heap.size > 0) {
        const run_idx = try heap.pop(&run_heads, ctx, schema, sort_keys, string_arena);

        // Emit the current head row from this run.
        if (output_in_memory) {
            result.rows[@intCast(output_row_idx)] = run_heads[run_idx];
            output_row_idx += 1;
        } else {
            // Write to temp pages.
            const ok = writer.appendRow(&run_heads[run_idx]) catch
                return error.SpillError;
            if (!ok) {
                // Page full — flush.
                if (output_page_count >= max_spill_pages)
                    return error.TempPagesExhausted;
                const payload = writer.finalize();
                const page_id = collector.temp_mgr.allocateAndWrite(
                    payload,
                    TempPage.null_page_id,
                ) catch return error.TempPagesExhausted;
                output_page_ids[output_page_count] = page_id;
                output_page_count += 1;
                writer.reset();
                const retry = writer.appendRow(&run_heads[run_idx]) catch
                    return error.SpillError;
                std.debug.assert(retry);
            }
            output_row_idx += 1;
        }

        // Advance this run.
        const has_next = try run_states[run_idx].advance(
            &run_heads[run_idx],
            string_arena,
            &collector.temp_mgr,
            all_page_ids,
        );
        if (has_next) {
            try heap.push(
                @intCast(run_idx),
                &run_heads,
                ctx,
                schema,
                sort_keys,
                string_arena,
            );
        }

        // Arena safety valve: if arena is >90% full, rescue heap strings.
        if (string_arena.bytes.len > 0) {
            const remaining = string_arena.bytes.len - string_arena.used;
            const threshold = string_arena.bytes.len / 10;
            if (remaining < threshold and heap.size > 0) {
                try rescueHeapStrings(
                    &run_heads,
                    &heap,
                    &rescue_buf,
                    string_arena,
                );
            }
        }
    }

    // Flush remaining output.
    if (!output_in_memory and writer.row_count > 0) {
        if (output_page_count >= max_spill_pages)
            return error.TempPagesExhausted;
        const payload = writer.finalize();
        const page_id = collector.temp_mgr.allocateAndWrite(
            payload,
            TempPage.null_page_id,
        ) catch return error.TempPagesExhausted;
        output_page_ids[output_page_count] = page_id;
        output_page_count += 1;
    }

    // Set result.
    if (output_in_memory) {
        result.row_count = @intCast(output_row_idx);
        result.collector = null;
    } else {
        reconfigureCollectorForSortedOutput(
            collector,
            output_page_ids[0..output_page_count],
            output_row_idx,
        );
        result.row_count = @intCast(@min(output_row_idx, scan_mod.scan_batch_size));
        result.collector = collector;
    }
}

/// Rescue string values from heap entries into a temporary buffer,
/// reset the main arena, and re-intern the strings.
fn rescueHeapStrings(
    run_heads: *[max_merge_runs]ResultRow,
    heap: *const MinHeap,
    rescue_buf: *[rescue_arena_size]u8,
    string_arena: *StringArena,
) ExternalSortError!void {
    // First pass: copy all heap entry strings into the rescue buffer.
    var rescue_arena = StringArena.init(rescue_buf);

    for (0..heap.size) |i| {
        const run_idx = heap.entries[i];
        const row = &run_heads[run_idx];
        var col: u16 = 0;
        while (col < row.column_count) : (col += 1) {
            switch (row.values[col]) {
                .string => |s| {
                    const copy = rescue_arena.copyString(s) catch
                        return error.ArenaExhausted;
                    row.values[col] = .{ .string = copy };
                },
                else => {},
            }
        }
    }

    // Reset main arena and re-intern from rescue buffer.
    string_arena.reset();

    for (0..heap.size) |i| {
        const run_idx = heap.entries[i];
        const row = &run_heads[run_idx];
        var col: u16 = 0;
        while (col < row.column_count) : (col += 1) {
            switch (row.values[col]) {
                .string => |s| {
                    const copy = string_arena.copyString(s) catch
                        return error.ArenaExhausted;
                    row.values[col] = .{ .string = copy };
                },
                else => {},
            }
        }
    }
}

/// Reconfigure the collector to iterate sorted output pages.
///
/// After external sort writes merged output to temp pages, this
/// overwrites the collector's spill tracking so its iterator will
/// yield the sorted data instead of the original scan data.
fn reconfigureCollectorForSortedOutput(
    collector: *SpillingResultCollector,
    sorted_page_ids: []const u64,
    total_rows: u64,
) void {
    @memcpy(
        collector.spill_page_ids[0..sorted_page_ids.len],
        sorted_page_ids,
    );
    collector.spill_page_count = @intCast(sorted_page_ids.len);
    collector.hot_count = 0;
    collector.total_rows = total_rows;
    collector.iteration_started = false;
}

// ===========================================================================
// Tests
// ===========================================================================

const testing = std.testing;
const disk_mod = @import("../simulator/disk.zig");

const TestSortBuffers = struct {
    result_rows: []ResultRow,
    scratch_a: []ResultRow,
    scratch_b: []ResultRow,

    fn init() !TestSortBuffers {
        const result_rows = try testing.allocator.alloc(ResultRow, scan_mod.scan_batch_size);
        errdefer testing.allocator.free(result_rows);
        const scratch_a = try testing.allocator.alloc(ResultRow, scan_mod.scan_batch_size);
        errdefer testing.allocator.free(scratch_a);
        const scratch_b = try testing.allocator.alloc(ResultRow, scan_mod.scan_batch_size);
        return .{
            .result_rows = result_rows,
            .scratch_a = scratch_a,
            .scratch_b = scratch_b,
        };
    }

    fn deinit(self: *TestSortBuffers) void {
        testing.allocator.free(self.scratch_b);
        testing.allocator.free(self.scratch_a);
        testing.allocator.free(self.result_rows);
    }
};

fn makeRow(values: []const Value) ResultRow {
    var row = ResultRow.init();
    row.column_count = @intCast(values.len);
    for (values, 0..) |v, i| {
        row.values[i] = v;
    }
    return row;
}

fn testArenaBuffer() [64 * 1024]u8 {
    return [_]u8{0} ** (64 * 1024);
}

/// Helper: create a collector with N rows of (i64) values, using a tiny
/// hot batch to force spilling.
fn setupCollectorWithRows(
    disk: *disk_mod.SimulatedDisk,
    hot_batch: []ResultRow,
    values: []const i64,
    budget: u64,
) SpillingResultCollector {
    const mgr = TempStorageManager.initDefault(0, disk.storage()) catch unreachable;
    var collector = SpillingResultCollector.init(hot_batch, mgr, budget);
    for (values) |v| {
        const row = makeRow(&.{.{ .i64 = v }});
        collector.appendRow(&row) catch unreachable;
    }
    return collector;
}

/// Helper: create a collector with N rows of (i64, string) values.
fn setupCollectorWithStringRows(
    disk: *disk_mod.SimulatedDisk,
    hot_batch: []ResultRow,
    int_values: []const i64,
    str_values: []const []const u8,
    budget: u64,
) SpillingResultCollector {
    const mgr = TempStorageManager.initDefault(0, disk.storage()) catch unreachable;
    var collector = SpillingResultCollector.init(hot_batch, mgr, budget);
    for (int_values, str_values) |iv, sv| {
        const row = makeRow(&.{ .{ .i64 = iv }, .{ .string = sv } });
        collector.appendRow(&row) catch unreachable;
    }
    return collector;
}

test "empty collector produces no sorted output" {
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();
    var hot: [4]ResultRow = undefined;
    const mgr = TempStorageManager.initDefault(0, disk.storage()) catch unreachable;
    var collector = SpillingResultCollector.init(&hot, mgr, 4 * 1024 * 1024);

    // No rows appended — totalRowCount is 0.
    // generateSortedRuns would produce 0 runs which is InternalError.
    // But applyExternalSort is only called when collector.spillTriggered().
    // This test validates the invariant: empty collector is not routed here.
    try testing.expectEqual(@as(u64, 0), collector.totalRowCount());
}

test "single batch sort — all rows fit in one run" {
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    // 8 rows with tiny hot batch (2) to force spilling.
    var hot: [2]ResultRow = undefined;
    const values = [_]i64{ 5, 3, 8, 1, 7, 2, 6, 4 };
    var collector = setupCollectorWithRows(&disk, &hot, &values, 4 * 1024 * 1024);

    try testing.expect(collector.spillTriggered());
    try testing.expectEqual(@as(u64, 8), collector.totalRowCount());

    // Generate sorted runs.
    var all_page_ids: [max_spill_pages]u64 = undefined;
    var runs: [max_merge_runs]RunInfo = undefined;
    var run_count: u16 = 0;
    var total_page_id_count: u32 = 0;

    // We need working buffers for sorting.
    var bufs = try TestSortBuffers.init();
    defer bufs.deinit();
    var result = QueryResult.init(bufs.result_rows);

    // Build a minimal ExecContext with just what generateSortedRuns needs.
    // compareRowsBySortKeys with column sort keys only needs schema.
    var arena_buf = testArenaBuffer();
    var string_arena = StringArena.init(&arena_buf);

    // Build sort key: sort by column 0 ascending.
    const sort_keys = [_]SortKeyDescriptor{
        .{
            .kind = .column,
            .descending = false,
            .column_index = 0,
        },
    };

    // Create minimal schema with one i64 column.
    var schema: RowSchema = .{
        .columns = undefined,
        .name_buffer = undefined,
    };
    _ = try schema.addColumn("val", .i64, false);

    // Minimal ExecContext — only fields used by compareRowsBySortKeys.
    const ctx = ExecContext{
        .catalog = undefined,
        .pool = undefined,
        .wal = undefined,
        .tx_manager = undefined,
        .undo_log = undefined,
        .tx_id = 0,
        .snapshot = undefined,
        .ast = undefined,
        .tokens = undefined,
        .source = "",
        .statement_timestamp_micros = null,
        .parameter_bindings = &.{},
        .allocator = testing.allocator,
        .result_rows = bufs.result_rows,
        .scratch_rows_a = bufs.scratch_a,
        .scratch_rows_b = bufs.scratch_b,
        .string_arena_bytes = &arena_buf,
        .nested_rows = undefined,
        .nested_decode_arena_bytes = undefined,
        .nested_match_arena_bytes = undefined,
        .storage = disk.storage(),
        .query_slot_index = 0,
        .collector = &collector,
        .work_memory_bytes_per_slot = 4 * 1024 * 1024,
    };

    try generateSortedRuns(
        &ctx,
        &result,
        &sort_keys,
        &schema,
        &string_arena,
        &collector,
        &all_page_ids,
        &runs,
        &run_count,
        &total_page_id_count,
    );

    // 8 rows fit in one batch (< scan_batch_size), so 1 run.
    try testing.expectEqual(@as(u16, 1), run_count);
    try testing.expectEqual(@as(u64, 8), runs[0].row_count);

    // Read run back into buffer and verify sorted order.
    string_arena.reset();
    try readRunIntoBuffer(
        bufs.result_rows,
        &runs[0],
        all_page_ids[0..total_page_id_count],
        &collector.temp_mgr,
        &string_arena,
    );

    const expected = [_]i64{ 1, 2, 3, 4, 5, 6, 7, 8 };
    for (expected, 0..) |exp, i| {
        try testing.expectEqual(exp, bufs.result_rows[i].values[0].i64);
    }
}

test "stability — equal keys preserve insertion order" {
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    // Rows with duplicate sort keys but different second columns.
    var hot: [2]ResultRow = undefined;
    const mgr = TempStorageManager.initDefault(0, disk.storage()) catch unreachable;
    var collector = SpillingResultCollector.init(&hot, mgr, 4 * 1024 * 1024);

    // All have key=1 but different secondary values.
    const rows = [_][2]i64{ .{ 1, 10 }, .{ 1, 20 }, .{ 1, 30 }, .{ 1, 40 } };
    for (rows) |r| {
        const row = makeRow(&.{ .{ .i64 = r[0] }, .{ .i64 = r[1] } });
        collector.appendRow(&row) catch unreachable;
    }
    try testing.expect(collector.spillTriggered());
    var bufs = try TestSortBuffers.init();
    defer bufs.deinit();
    var result = QueryResult.init(bufs.result_rows);
    var arena_buf = testArenaBuffer();
    var string_arena = StringArena.init(&arena_buf);

    const sort_keys = [_]SortKeyDescriptor{
        .{ .kind = .column, .descending = false, .column_index = 0 },
    };

    var schema: RowSchema = .{
        .columns = undefined,
        .name_buffer = undefined,
    };
    _ = try schema.addColumn("a", .i64, false);
    _ = try schema.addColumn("b", .i64, false);

    const ctx = ExecContext{
        .catalog = undefined,
        .pool = undefined,
        .wal = undefined,
        .tx_manager = undefined,
        .undo_log = undefined,
        .tx_id = 0,
        .snapshot = undefined,
        .ast = undefined,
        .tokens = undefined,
        .source = "",
        .statement_timestamp_micros = null,
        .parameter_bindings = &.{},
        .allocator = testing.allocator,
        .result_rows = bufs.result_rows,
        .scratch_rows_a = bufs.scratch_a,
        .scratch_rows_b = bufs.scratch_b,
        .string_arena_bytes = &arena_buf,
        .nested_rows = undefined,
        .nested_decode_arena_bytes = undefined,
        .nested_match_arena_bytes = undefined,
        .storage = disk.storage(),
        .query_slot_index = 0,
        .collector = &collector,
        .work_memory_bytes_per_slot = 4 * 1024 * 1024,
    };

    var all_page_ids: [max_spill_pages]u64 = undefined;
    var runs_arr: [max_merge_runs]RunInfo = undefined;
    var run_count: u16 = 0;
    var total_page_id_count: u32 = 0;

    try generateSortedRuns(
        &ctx,
        &result,
        &sort_keys,
        &schema,
        &string_arena,
        &collector,
        &all_page_ids,
        &runs_arr,
        &run_count,
        &total_page_id_count,
    );

    string_arena.reset();
    try readRunIntoBuffer(
        bufs.result_rows,
        &runs_arr[0],
        all_page_ids[0..total_page_id_count],
        &collector.temp_mgr,
        &string_arena,
    );

    // Insertion order preserved: secondary values should be 10, 20, 30, 40.
    const expected_secondary = [_]i64{ 10, 20, 30, 40 };
    for (expected_secondary, 0..) |exp, i| {
        try testing.expectEqual(exp, bufs.result_rows[i].values[1].i64);
    }
}

test "descending sort order" {
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    var hot: [2]ResultRow = undefined;
    const values = [_]i64{ 3, 1, 4, 1, 5, 9, 2, 6 };
    var collector = setupCollectorWithRows(&disk, &hot, &values, 4 * 1024 * 1024);
    var bufs = try TestSortBuffers.init();
    defer bufs.deinit();
    var result = QueryResult.init(bufs.result_rows);
    var arena_buf = testArenaBuffer();
    var string_arena = StringArena.init(&arena_buf);

    const sort_keys = [_]SortKeyDescriptor{
        .{ .kind = .column, .descending = true, .column_index = 0 },
    };

    var schema: RowSchema = .{
        .columns = undefined,
        .name_buffer = undefined,
    };
    _ = try schema.addColumn("val", .i64, false);

    const ctx = ExecContext{
        .catalog = undefined,
        .pool = undefined,
        .wal = undefined,
        .tx_manager = undefined,
        .undo_log = undefined,
        .tx_id = 0,
        .snapshot = undefined,
        .ast = undefined,
        .tokens = undefined,
        .source = "",
        .statement_timestamp_micros = null,
        .parameter_bindings = &.{},
        .allocator = testing.allocator,
        .result_rows = bufs.result_rows,
        .scratch_rows_a = bufs.scratch_a,
        .scratch_rows_b = bufs.scratch_b,
        .string_arena_bytes = &arena_buf,
        .nested_rows = undefined,
        .nested_decode_arena_bytes = undefined,
        .nested_match_arena_bytes = undefined,
        .storage = disk.storage(),
        .query_slot_index = 0,
        .collector = &collector,
        .work_memory_bytes_per_slot = 4 * 1024 * 1024,
    };

    var all_page_ids: [max_spill_pages]u64 = undefined;
    var runs_arr: [max_merge_runs]RunInfo = undefined;
    var run_count: u16 = 0;
    var total_page_id_count: u32 = 0;

    try generateSortedRuns(
        &ctx,
        &result,
        &sort_keys,
        &schema,
        &string_arena,
        &collector,
        &all_page_ids,
        &runs_arr,
        &run_count,
        &total_page_id_count,
    );

    string_arena.reset();
    try readRunIntoBuffer(
        bufs.result_rows,
        &runs_arr[0],
        all_page_ids[0..total_page_id_count],
        &collector.temp_mgr,
        &string_arena,
    );

    const expected = [_]i64{ 9, 6, 5, 4, 3, 2, 1, 1 };
    for (expected, 0..) |exp, i| {
        try testing.expectEqual(exp, bufs.result_rows[i].values[0].i64);
    }
}

test "two-run merge" {
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    // Test k-way merge directly with two manually created sorted runs.
    var hot: [2]ResultRow = undefined;
    const mgr = TempStorageManager.initDefault(0, disk.storage()) catch unreachable;
    var collector = SpillingResultCollector.init(&hot, mgr, 4 * 1024 * 1024);
    var bufs = try TestSortBuffers.init();
    defer bufs.deinit();
    var arena_buf = testArenaBuffer();
    var string_arena = StringArena.init(&arena_buf);

    const sort_keys = [_]SortKeyDescriptor{
        .{ .kind = .column, .descending = false, .column_index = 0 },
    };

    var schema: RowSchema = .{
        .columns = undefined,
        .name_buffer = undefined,
    };
    _ = try schema.addColumn("val", .i64, false);

    const ctx = ExecContext{
        .catalog = undefined,
        .pool = undefined,
        .wal = undefined,
        .tx_manager = undefined,
        .undo_log = undefined,
        .tx_id = 0,
        .snapshot = undefined,
        .ast = undefined,
        .tokens = undefined,
        .source = "",
        .statement_timestamp_micros = null,
        .parameter_bindings = &.{},
        .allocator = testing.allocator,
        .result_rows = bufs.result_rows,
        .scratch_rows_a = bufs.scratch_a,
        .scratch_rows_b = bufs.scratch_b,
        .string_arena_bytes = &arena_buf,
        .nested_rows = undefined,
        .nested_decode_arena_bytes = undefined,
        .nested_match_arena_bytes = undefined,
        .storage = disk.storage(),
        .query_slot_index = 0,
        .collector = &collector,
        .work_memory_bytes_per_slot = 4 * 1024 * 1024,
    };

    // Manually create two sorted runs:
    // Run 0: [2, 4, 6, 8, 10] (even numbers, sorted)
    // Run 1: [1, 3, 5, 7, 9] (odd numbers, sorted)
    var all_page_ids: [max_spill_pages]u64 = undefined;
    var total_pid: u32 = 0;

    // Write run 0.
    var writer = SpillPageWriter.init();
    const run0_vals = [_]i64{ 2, 4, 6, 8, 10 };
    for (run0_vals) |v| {
        const row = makeRow(&.{.{ .i64 = v }});
        const ok = writer.appendRow(&row) catch unreachable;
        std.debug.assert(ok);
    }
    const payload0 = writer.finalize();
    const pid0 = collector.temp_mgr.allocateAndWrite(
        payload0,
        TempPage.null_page_id,
    ) catch unreachable;
    all_page_ids[total_pid] = pid0;
    total_pid += 1;

    // Write run 1.
    writer.reset();
    const run1_vals = [_]i64{ 1, 3, 5, 7, 9 };
    for (run1_vals) |v| {
        const row = makeRow(&.{.{ .i64 = v }});
        const ok = writer.appendRow(&row) catch unreachable;
        std.debug.assert(ok);
    }
    const payload1 = writer.finalize();
    const pid1 = collector.temp_mgr.allocateAndWrite(
        payload1,
        TempPage.null_page_id,
    ) catch unreachable;
    all_page_ids[total_pid] = pid1;
    total_pid += 1;

    const runs = [_]RunInfo{
        .{ .page_id_start = 0, .page_count = 1, .row_count = 5 },
        .{ .page_id_start = 1, .page_count = 1, .row_count = 5 },
    };

    var result = QueryResult.init(bufs.result_rows);
    string_arena.reset();

    try kWayMerge(
        &ctx,
        &result,
        &sort_keys,
        &schema,
        &string_arena,
        &collector,
        all_page_ids[0..total_pid],
        &runs,
        10,
    );

    // Output should be in-memory (10 rows < 4096).
    try testing.expectEqual(@as(?*SpillingResultCollector, null), result.collector);
    try testing.expectEqual(@as(u16, 10), result.row_count);

    // Verify sorted order: 1..10.
    var expected: i64 = 1;
    for (0..10) |idx| {
        try testing.expectEqual(expected, bufs.result_rows[idx].values[0].i64);
        expected += 1;
    }
}

test "two-run merge with stability across runs" {
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    var hot: [2]ResultRow = undefined;
    const mgr = TempStorageManager.initDefault(0, disk.storage()) catch unreachable;
    var collector = SpillingResultCollector.init(&hot, mgr, 4 * 1024 * 1024);
    // Just need the collector for its temp_mgr.
    var bufs = try TestSortBuffers.init();
    defer bufs.deinit();
    var arena_buf = testArenaBuffer();
    var string_arena = StringArena.init(&arena_buf);

    // Sort by column 0 only. Column 1 distinguishes insertion order.
    const sort_keys = [_]SortKeyDescriptor{
        .{ .kind = .column, .descending = false, .column_index = 0 },
    };

    var schema: RowSchema = .{
        .columns = undefined,
        .name_buffer = undefined,
    };
    _ = try schema.addColumn("k", .i64, false);
    _ = try schema.addColumn("v", .i64, false);

    const ctx = ExecContext{
        .catalog = undefined,
        .pool = undefined,
        .wal = undefined,
        .tx_manager = undefined,
        .undo_log = undefined,
        .tx_id = 0,
        .snapshot = undefined,
        .ast = undefined,
        .tokens = undefined,
        .source = "",
        .statement_timestamp_micros = null,
        .parameter_bindings = &.{},
        .allocator = testing.allocator,
        .result_rows = bufs.result_rows,
        .scratch_rows_a = bufs.scratch_a,
        .scratch_rows_b = bufs.scratch_b,
        .string_arena_bytes = &arena_buf,
        .nested_rows = undefined,
        .nested_decode_arena_bytes = undefined,
        .nested_match_arena_bytes = undefined,
        .storage = disk.storage(),
        .query_slot_index = 0,
        .collector = &collector,
        .work_memory_bytes_per_slot = 4 * 1024 * 1024,
    };

    // Run 0: (1, 100), (2, 200)  — from earlier in input
    // Run 1: (1, 300), (2, 400)  — from later in input
    // After merge with stability: (1,100), (1,300), (2,200), (2,400)
    var all_page_ids: [max_spill_pages]u64 = undefined;
    var total_pid: u32 = 0;

    var writer = SpillPageWriter.init();
    const r0_rows = [_][2]i64{ .{ 1, 100 }, .{ 2, 200 } };
    for (r0_rows) |r| {
        const row = makeRow(&.{ .{ .i64 = r[0] }, .{ .i64 = r[1] } });
        _ = writer.appendRow(&row) catch unreachable;
    }
    var payload = writer.finalize();
    all_page_ids[total_pid] = collector.temp_mgr.allocateAndWrite(payload, TempPage.null_page_id) catch unreachable;
    total_pid += 1;

    writer.reset();
    const r1_rows = [_][2]i64{ .{ 1, 300 }, .{ 2, 400 } };
    for (r1_rows) |r| {
        const row = makeRow(&.{ .{ .i64 = r[0] }, .{ .i64 = r[1] } });
        _ = writer.appendRow(&row) catch unreachable;
    }
    payload = writer.finalize();
    all_page_ids[total_pid] = collector.temp_mgr.allocateAndWrite(payload, TempPage.null_page_id) catch unreachable;
    total_pid += 1;

    const runs = [_]RunInfo{
        .{ .page_id_start = 0, .page_count = 1, .row_count = 2 },
        .{ .page_id_start = 1, .page_count = 1, .row_count = 2 },
    };

    var result = QueryResult.init(bufs.result_rows);
    string_arena.reset();
    try kWayMerge(
        &ctx,
        &result,
        &sort_keys,
        &schema,
        &string_arena,
        &collector,
        all_page_ids[0..total_pid],
        &runs,
        4,
    );

    try testing.expectEqual(@as(u16, 4), result.row_count);
    // Stability: run 0's (1,100) before run 1's (1,300).
    try testing.expectEqual(@as(i64, 100), bufs.result_rows[0].values[1].i64);
    try testing.expectEqual(@as(i64, 300), bufs.result_rows[1].values[1].i64);
    try testing.expectEqual(@as(i64, 200), bufs.result_rows[2].values[1].i64);
    try testing.expectEqual(@as(i64, 400), bufs.result_rows[3].values[1].i64);
}

test "null values sort correctly" {
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    var hot: [2]ResultRow = undefined;
    const mgr = TempStorageManager.initDefault(0, disk.storage()) catch unreachable;
    var collector = SpillingResultCollector.init(&hot, mgr, 4 * 1024 * 1024);

    // Mix of null and non-null values.
    const row1 = makeRow(&.{.{ .null_value = {} }});
    const row2 = makeRow(&.{.{ .i64 = 3 }});
    const row3 = makeRow(&.{.{ .i64 = 1 }});
    const row4 = makeRow(&.{.{ .null_value = {} }});
    const row5 = makeRow(&.{.{ .i64 = 2 }});

    collector.appendRow(&row1) catch unreachable;
    collector.appendRow(&row2) catch unreachable;
    collector.appendRow(&row3) catch unreachable;
    collector.appendRow(&row4) catch unreachable;
    collector.appendRow(&row5) catch unreachable;

    try testing.expect(collector.spillTriggered());
    var bufs = try TestSortBuffers.init();
    defer bufs.deinit();
    var result = QueryResult.init(bufs.result_rows);
    var arena_buf = testArenaBuffer();
    var string_arena = StringArena.init(&arena_buf);

    const sort_keys = [_]SortKeyDescriptor{
        .{ .kind = .column, .descending = false, .column_index = 0 },
    };

    var schema: RowSchema = .{
        .columns = undefined,
        .name_buffer = undefined,
    };
    _ = try schema.addColumn("val", .i64, true);

    const ctx = ExecContext{
        .catalog = undefined,
        .pool = undefined,
        .wal = undefined,
        .tx_manager = undefined,
        .undo_log = undefined,
        .tx_id = 0,
        .snapshot = undefined,
        .ast = undefined,
        .tokens = undefined,
        .source = "",
        .statement_timestamp_micros = null,
        .parameter_bindings = &.{},
        .allocator = testing.allocator,
        .result_rows = bufs.result_rows,
        .scratch_rows_a = bufs.scratch_a,
        .scratch_rows_b = bufs.scratch_b,
        .string_arena_bytes = &arena_buf,
        .nested_rows = undefined,
        .nested_decode_arena_bytes = undefined,
        .nested_match_arena_bytes = undefined,
        .storage = disk.storage(),
        .query_slot_index = 0,
        .collector = &collector,
        .work_memory_bytes_per_slot = 4 * 1024 * 1024,
    };

    var all_page_ids: [max_spill_pages]u64 = undefined;
    var runs_arr: [max_merge_runs]RunInfo = undefined;
    var run_count: u16 = 0;
    var total_page_id_count: u32 = 0;

    try generateSortedRuns(
        &ctx,
        &result,
        &sort_keys,
        &schema,
        &string_arena,
        &collector,
        &all_page_ids,
        &runs_arr,
        &run_count,
        &total_page_id_count,
    );

    string_arena.reset();
    try readRunIntoBuffer(
        bufs.result_rows,
        &runs_arr[0],
        all_page_ids[0..total_page_id_count],
        &collector.temp_mgr,
        &string_arena,
    );

    // All 5 rows present.
    try testing.expectEqual(@as(u16, 1), run_count);

    // Nulls should sort consistently (pg2's compareValues puts nulls at end).
    // Verify all non-null values come first in ascending order.
    try testing.expectEqual(@as(i64, 1), bufs.result_rows[0].values[0].i64);
    try testing.expectEqual(@as(i64, 2), bufs.result_rows[1].values[0].i64);
    try testing.expectEqual(@as(i64, 3), bufs.result_rows[2].values[0].i64);
    try testing.expect(bufs.result_rows[3].values[0] == .null_value);
    try testing.expect(bufs.result_rows[4].values[0] == .null_value);
}

test "deterministic output — two passes produce identical results" {
    // Run the same sort twice with independent disks and verify byte-identical output.
    var results1: [16]ResultRow = undefined;
    var results2: [16]ResultRow = undefined;

    for (0..2) |pass| {
        var disk = disk_mod.SimulatedDisk.init(testing.allocator);
        defer disk.deinit();

        var hot: [2]ResultRow = undefined;
        const values = [_]i64{ 7, 2, 9, 4, 1, 8, 3, 6, 5, 10 };
        var collector = setupCollectorWithRows(&disk, &hot, &values, 4 * 1024 * 1024);
    var bufs = try TestSortBuffers.init();
    defer bufs.deinit();
        var result = QueryResult.init(bufs.result_rows);
        var arena_buf = testArenaBuffer();
        var string_arena = StringArena.init(&arena_buf);

        const sort_keys = [_]SortKeyDescriptor{
            .{ .kind = .column, .descending = false, .column_index = 0 },
        };

        var schema: RowSchema = .{
            .columns = undefined,
            .name_buffer = undefined,
        };
        _ = try schema.addColumn("val", .i64, false);

        const ctx = ExecContext{
            .catalog = undefined,
            .pool = undefined,
            .wal = undefined,
            .tx_manager = undefined,
            .undo_log = undefined,
            .tx_id = 0,
            .snapshot = undefined,
            .ast = undefined,
            .tokens = undefined,
            .source = "",
            .statement_timestamp_micros = null,
            .parameter_bindings = &.{},
            .allocator = testing.allocator,
            .result_rows = bufs.result_rows,
            .scratch_rows_a = bufs.scratch_a,
            .scratch_rows_b = bufs.scratch_b,
            .string_arena_bytes = &arena_buf,
        .nested_rows = undefined,
        .nested_decode_arena_bytes = undefined,
        .nested_match_arena_bytes = undefined,
            .storage = disk.storage(),
            .query_slot_index = 0,
            .collector = &collector,
            .work_memory_bytes_per_slot = 4 * 1024 * 1024,
        };

        var all_page_ids: [max_spill_pages]u64 = undefined;
        var runs_arr: [max_merge_runs]RunInfo = undefined;
        var run_count: u16 = 0;
        var total_page_id_count: u32 = 0;

        generateSortedRuns(
            &ctx,
            &result,
            &sort_keys,
            &schema,
            &string_arena,
            &collector,
            &all_page_ids,
            &runs_arr,
            &run_count,
            &total_page_id_count,
        ) catch unreachable;

        string_arena.reset();
        readRunIntoBuffer(
            bufs.result_rows,
            &runs_arr[0],
            all_page_ids[0..total_page_id_count],
            &collector.temp_mgr,
            &string_arena,
        ) catch unreachable;

        const target = if (pass == 0) &results1 else &results2;
        @memcpy(target[0..10], bufs.result_rows[0..10]);
    }

    // Compare row by row.
    for (0..10) |idx| {
        try testing.expectEqual(results1[idx].values[0].i64, results2[idx].values[0].i64);
    }
}

test "string column sort" {
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    var hot: [2]ResultRow = undefined;
    const int_vals = [_]i64{ 3, 1, 2 };
    const str_vals = [_][]const u8{ "charlie", "alpha", "bravo" };
    var collector = setupCollectorWithStringRows(&disk, &hot, &int_vals, &str_vals, 4 * 1024 * 1024);
    var bufs = try TestSortBuffers.init();
    defer bufs.deinit();
    var result = QueryResult.init(bufs.result_rows);
    var arena_buf = testArenaBuffer();
    var string_arena = StringArena.init(&arena_buf);

    // Sort by string column (column 1).
    const sort_keys = [_]SortKeyDescriptor{
        .{ .kind = .column, .descending = false, .column_index = 1 },
    };

    var schema: RowSchema = .{
        .columns = undefined,
        .name_buffer = undefined,
    };
    _ = try schema.addColumn("id", .i64, false);
    _ = try schema.addColumn("name", .string, false);

    const ctx = ExecContext{
        .catalog = undefined,
        .pool = undefined,
        .wal = undefined,
        .tx_manager = undefined,
        .undo_log = undefined,
        .tx_id = 0,
        .snapshot = undefined,
        .ast = undefined,
        .tokens = undefined,
        .source = "",
        .statement_timestamp_micros = null,
        .parameter_bindings = &.{},
        .allocator = testing.allocator,
        .result_rows = bufs.result_rows,
        .scratch_rows_a = bufs.scratch_a,
        .scratch_rows_b = bufs.scratch_b,
        .string_arena_bytes = &arena_buf,
        .nested_rows = undefined,
        .nested_decode_arena_bytes = undefined,
        .nested_match_arena_bytes = undefined,
        .storage = disk.storage(),
        .query_slot_index = 0,
        .collector = &collector,
        .work_memory_bytes_per_slot = 4 * 1024 * 1024,
    };

    var all_page_ids: [max_spill_pages]u64 = undefined;
    var runs_arr: [max_merge_runs]RunInfo = undefined;
    var run_count: u16 = 0;
    var total_page_id_count: u32 = 0;

    try generateSortedRuns(
        &ctx,
        &result,
        &sort_keys,
        &schema,
        &string_arena,
        &collector,
        &all_page_ids,
        &runs_arr,
        &run_count,
        &total_page_id_count,
    );

    string_arena.reset();
    try readRunIntoBuffer(
        bufs.result_rows,
        &runs_arr[0],
        all_page_ids[0..total_page_id_count],
        &collector.temp_mgr,
        &string_arena,
    );

    // Sorted by string: alpha, bravo, charlie.
    try testing.expectEqualStrings("alpha", bufs.result_rows[0].values[1].string);
    try testing.expectEqualStrings("bravo", bufs.result_rows[1].values[1].string);
    try testing.expectEqualStrings("charlie", bufs.result_rows[2].values[1].string);
}

test "reconfigure collector for sorted output" {
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    var hot: [4]ResultRow = undefined;
    const mgr = TempStorageManager.initDefault(0, disk.storage()) catch unreachable;
    var collector = SpillingResultCollector.init(&hot, mgr, 4 * 1024 * 1024);

    // Write some rows and spill them.
    const row = makeRow(&.{.{ .i64 = 42 }});
    collector.appendRow(&row) catch unreachable;

    // Write a sorted page directly.
    var writer = SpillPageWriter.init();
    const sorted_row1 = makeRow(&.{.{ .i64 = 1 }});
    const sorted_row2 = makeRow(&.{.{ .i64 = 2 }});
    _ = writer.appendRow(&sorted_row1) catch unreachable;
    _ = writer.appendRow(&sorted_row2) catch unreachable;
    const payload = writer.finalize();
    const page_id = collector.temp_mgr.allocateAndWrite(
        payload,
        TempPage.null_page_id,
    ) catch unreachable;

    // Reconfigure to point at sorted output.
    const sorted_ids = [_]u64{page_id};
    reconfigureCollectorForSortedOutput(&collector, &sorted_ids, 2);

    // Verify collector state.
    try testing.expectEqual(@as(u32, 1), collector.spill_page_count);
    try testing.expectEqual(@as(u16, 0), collector.hot_count);
    try testing.expectEqual(@as(u64, 2), collector.total_rows);

    // Iterate and verify sorted output.
    var arena_buf = testArenaBuffer();
    var arena = StringArena.init(&arena_buf);
    var iter = collector.iterator();
    var out = ResultRow.init();

    try testing.expect(try iter.next(&out, &arena));
    try testing.expectEqual(@as(i64, 1), out.values[0].i64);
    try testing.expect(try iter.next(&out, &arena));
    try testing.expectEqual(@as(i64, 2), out.values[0].i64);
    try testing.expect(!try iter.next(&out, &arena));
}
