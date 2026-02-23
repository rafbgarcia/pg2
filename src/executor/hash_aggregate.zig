//! Hash-based GROUP BY with Grace hash partition spill.
//!
//! Responsibilities in this file:
//! - Reads all input rows from a `SpillingResultCollector` iterator.
//! - Groups rows using a hash table (open addressing, linear probing).
//! - Accumulates aggregate state (SUM, AVG, MIN, MAX, COUNT) per group.
//! - Outputs grouped rows to `result.rows` with aggregate state in `GroupRuntime`.
//!
//! Why this exists:
//! - The existing `aggregation.applyGroup` uses O(n^2) linear scan for group
//!   matching, which only works on in-memory data bounded by `scan_batch_size`.
//! - When scan spill has occurred, the collector may hold more rows than fit in
//!   memory. Hash aggregation provides O(1) amortized group lookup and supports
//!   partition-based spill for group cardinalities exceeding in-memory capacity.
//!
//! How it works:
//! - **In-memory fast path**: All groups fit in the hash table (up to 4096
//!   groups at load factor 0.5 in an 8192-slot table). Process all input from
//!   the collector iterator, output groups.
//! - **Grace hash spill path**: When groups exceed in-memory capacity, re-iterate
//!   input from the collector, partition rows by `hash % P`. Partition 0 aggregates
//!   inline. Other partitions serialize raw rows to temp pages. After all input is
//!   consumed, emit partition 0 groups, then process spilled partitions one at a
//!   time, reusing the hash table for each.
//!
//! Boundaries and non-responsibilities:
//! - This file does not decide when to invoke hash aggregation; the executor
//!   pipeline routes to it when scan spill has occurred and GROUP BY is present.
//! - Spilled partitions store raw input rows (not partial aggregates) so AVG
//!   correctness is maintained across partition boundaries.
//! - HAVING/LIMIT/OFFSET are applied by the executor after aggregation completes.
//!
//! Contributor notes:
//! - Hash function: `std.hash.Wyhash` with seed 0 (deterministic for simulation).
//! - Max partitions: 16. Max groups: 4096 (across all partitions combined).
//! - String arena management: a small read arena (stack-allocated) is used per
//!   row from the collector. Group representative row strings and aggregate
//!   min/max strings are copied to the main string arena. A rescue mechanism
//!   handles main arena overflow.
const std = @import("std");
const ast_mod = @import("../parser/ast.zig");
const tokenizer_mod = @import("../parser/tokenizer.zig");
const row_mod = @import("../storage/row.zig");
const scan_mod = @import("scan.zig");
const temp_mod = @import("../storage/temp.zig");
const page_mod = @import("../storage/page.zig");
const spill_row = @import("../storage/spill_row.zig");
const capacity_mod = @import("capacity.zig");
const aggregation_mod = @import("aggregation.zig");
const spill_collector_mod = @import("spill_collector.zig");
const filter_mod = @import("filter.zig");

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
const GroupRuntime = aggregation_mod.GroupRuntime;
const AggregateState = aggregation_mod.AggregateState;
const ExecContext = @import("executor.zig").ExecContext;
const QueryResult = @import("executor.zig").QueryResult;
const max_operators = @import("executor.zig").max_operators;
const OpDescriptor = @import("executor.zig").OpDescriptor;
const setError = @import("executor.zig").setError;
const evalContextForExec = @import("executor.zig").evalContextForExec;
const max_group_keys = capacity_mod.max_group_keys;
const max_spill_pages = spill_collector_mod.max_spill_pages;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Hash table size: power of 2, providing load factor 0.5 with max 4096 groups.
const hash_table_size: usize = 8192;

/// Maximum groups that can be stored in the hash table (load factor 0.5).
const max_in_memory_groups: usize = capacity_mod.max_aggregate_groups;

/// Sentinel value for empty hash table slots.
const empty_group: u16 = std.math.maxInt(u16);

/// Maximum number of Grace hash partitions.
const max_partitions: u64 = 16;

/// Rescue arena size for string arena overflow recovery.
const rescue_arena_size: usize = 64 * 1024;

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

pub const HashAggregateError = error{
    /// Total distinct groups across all partitions exceeds capacity.
    TooManyGroups,
    /// Temp page budget exhausted during partition spill.
    TempPagesExhausted,
    /// Spill serialization or deserialization failed.
    SpillError,
    /// String arena exhausted and rescue failed.
    ArenaExhausted,
    /// Storage I/O error reading spilled partition pages.
    StorageError,
    /// Internal invariant violation.
    InternalError,
    /// Aggregate expression evaluation failed.
    AggregateEvalFailed,
};

// ---------------------------------------------------------------------------
// HashTable — open addressing with linear probing
// ---------------------------------------------------------------------------

const HashTable = struct {
    hashes: [hash_table_size]u64,
    indices: [hash_table_size]u16,

    fn init() HashTable {
        return .{
            .hashes = [_]u64{0} ** hash_table_size,
            .indices = [_]u16{empty_group} ** hash_table_size,
        };
    }

    fn clear(self: *HashTable) void {
        @memset(&self.indices, empty_group);
    }

    /// Find existing group or insert new one.
    ///
    /// Returns the group index if found or newly inserted. Returns null if
    /// the table is at capacity (group_count >= max_groups) and the key is
    /// not already present.
    fn findOrInsert(
        self: *HashTable,
        hash: u64,
        row: *const ResultRow,
        key_indices: []const u16,
        groups: []const ResultRow,
        group_count: *u16,
        max_groups: u16,
    ) ?u16 {
        const mask = hash_table_size - 1;
        var idx = @as(usize, @truncate(hash)) & mask;
        var probes: usize = 0;
        while (probes < hash_table_size) : (probes += 1) {
            if (self.indices[idx] == empty_group) {
                // Empty slot: insert new group if capacity allows.
                if (group_count.* >= max_groups) return null;
                self.hashes[idx] = hash;
                const new_idx = group_count.*;
                self.indices[idx] = new_idx;
                group_count.* += 1;
                return new_idx;
            }
            if (self.hashes[idx] == hash) {
                const gi = self.indices[idx];
                if (rowsEqualOnKeys(&groups[gi], row, key_indices)) {
                    return gi;
                }
            }
            idx = (idx + 1) & mask;
        }
        // Full probe cycle without finding or inserting — should not happen
        // at load factor 0.5, but return null defensively.
        return null;
    }
};

// ---------------------------------------------------------------------------
// Hash functions
// ---------------------------------------------------------------------------

fn hashGroupKeys(row: *const ResultRow, key_indices: []const u16) u64 {
    var hasher = std.hash.Wyhash.init(0);
    for (key_indices) |col_idx| {
        hashValue(&hasher, row.values[col_idx]);
    }
    return hasher.final();
}

fn hashValue(hasher: *std.hash.Wyhash, value: Value) void {
    // Type tag prefix prevents cross-type hash collisions.
    switch (value) {
        .null_value => hasher.update(&[_]u8{0}),
        .bool => |v| {
            hasher.update(&[_]u8{1});
            hasher.update(&[_]u8{@intFromBool(v)});
        },
        .i8 => |v| {
            hasher.update(&[_]u8{2});
            hasher.update(std.mem.asBytes(&v));
        },
        .i16 => |v| {
            hasher.update(&[_]u8{3});
            hasher.update(std.mem.asBytes(&v));
        },
        .i32 => |v| {
            hasher.update(&[_]u8{4});
            hasher.update(std.mem.asBytes(&v));
        },
        .i64 => |v| {
            hasher.update(&[_]u8{5});
            hasher.update(std.mem.asBytes(&v));
        },
        .u8 => |v| {
            hasher.update(&[_]u8{6});
            hasher.update(std.mem.asBytes(&v));
        },
        .u16 => |v| {
            hasher.update(&[_]u8{7});
            hasher.update(std.mem.asBytes(&v));
        },
        .u32 => |v| {
            hasher.update(&[_]u8{8});
            hasher.update(std.mem.asBytes(&v));
        },
        .u64 => |v| {
            hasher.update(&[_]u8{9});
            hasher.update(std.mem.asBytes(&v));
        },
        .f64 => |v| {
            hasher.update(&[_]u8{10});
            hasher.update(std.mem.asBytes(&v));
        },
        .string => |s| {
            hasher.update(&[_]u8{11});
            hasher.update(s);
        },
        .timestamp => |v| {
            hasher.update(&[_]u8{12});
            hasher.update(std.mem.asBytes(&v));
        },
    }
}

// ---------------------------------------------------------------------------
// Key equality
// ---------------------------------------------------------------------------

fn rowsEqualOnKeys(lhs: *const ResultRow, rhs: *const ResultRow, key_indices: []const u16) bool {
    for (key_indices) |col_index| {
        if (row_mod.compareValues(lhs.values[col_index], rhs.values[col_index]) != .eq) {
            return false;
        }
    }
    return true;
}

// ---------------------------------------------------------------------------
// String management helpers
// ---------------------------------------------------------------------------

/// Copy all string column values in a row to the main arena.
fn copyRowStringsToArena(row: *ResultRow, arena: *StringArena) HashAggregateError!void {
    var col: u16 = 0;
    while (col < row.column_count) : (col += 1) {
        switch (row.values[col]) {
            .string => |s| {
                row.values[col] = .{ .string = arena.copyString(s) catch return error.ArenaExhausted };
            },
            else => {},
        }
    }
}

/// After accumulateGroupAggregates, copy any aggregate min/max string values
/// to the main arena. These values may point into the read arena (if the
/// raw column value was a string) or already into the main arena (if an
/// expression produced a new string). Copying unconditionally is safe.
fn rescueAggregateStrings(
    group_runtime: *GroupRuntime,
    group_index: u16,
    main_arena: *StringArena,
) HashAggregateError!void {
    var slot: u16 = 0;
    while (slot < group_runtime.aggregate_count) : (slot += 1) {
        const state = &group_runtime.aggregate_states[slot][group_index];
        switch (state.min_value) {
            .string => |s| {
                state.min_value = .{ .string = main_arena.copyString(s) catch return error.ArenaExhausted };
            },
            else => {},
        }
        switch (state.max_value) {
            .string => |s| {
                state.max_value = .{ .string = main_arena.copyString(s) catch return error.ArenaExhausted };
            },
            else => {},
        }
    }
}

/// Rescue all group representative row strings and aggregate state strings
/// from the main arena into a temporary rescue buffer, reset the main arena,
/// and re-intern everything back into it. This recovers fragmentation.
fn rescueGroupStrings(
    result_rows: []ResultRow,
    group_count: u16,
    group_runtime: *GroupRuntime,
    rescue_buf: []u8,
    string_arena: *StringArena,
) HashAggregateError!void {
    var rescue_arena = StringArena.init(rescue_buf);

    // Save group representative row strings.
    for (0..group_count) |gi| {
        var col: u16 = 0;
        while (col < result_rows[gi].column_count) : (col += 1) {
            switch (result_rows[gi].values[col]) {
                .string => |s| {
                    result_rows[gi].values[col] = .{ .string = rescue_arena.copyString(s) catch return error.ArenaExhausted };
                },
                else => {},
            }
        }
    }

    // Save aggregate min/max strings.
    var slot: u16 = 0;
    while (slot < group_runtime.aggregate_count) : (slot += 1) {
        for (0..group_count) |gi| {
            const state = &group_runtime.aggregate_states[slot][gi];
            switch (state.min_value) {
                .string => |s| {
                    state.min_value = .{ .string = rescue_arena.copyString(s) catch return error.ArenaExhausted };
                },
                else => {},
            }
            switch (state.max_value) {
                .string => |s| {
                    state.max_value = .{ .string = rescue_arena.copyString(s) catch return error.ArenaExhausted };
                },
                else => {},
            }
        }
    }

    // Reset main arena and re-intern from rescue buffer.
    string_arena.reset();

    for (0..group_count) |gi| {
        var col: u16 = 0;
        while (col < result_rows[gi].column_count) : (col += 1) {
            switch (result_rows[gi].values[col]) {
                .string => |s| {
                    result_rows[gi].values[col] = .{ .string = string_arena.copyString(s) catch return error.ArenaExhausted };
                },
                else => {},
            }
        }
    }

    slot = 0;
    while (slot < group_runtime.aggregate_count) : (slot += 1) {
        for (0..group_count) |gi| {
            const state = &group_runtime.aggregate_states[slot][gi];
            switch (state.min_value) {
                .string => |s| {
                    state.min_value = .{ .string = string_arena.copyString(s) catch return error.ArenaExhausted };
                },
                else => {},
            }
            switch (state.max_value) {
                .string => |s| {
                    state.max_value = .{ .string = string_arena.copyString(s) catch return error.ArenaExhausted };
                },
                else => {},
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Partition count selection
// ---------------------------------------------------------------------------

/// Choose the number of Grace hash partitions based on the observed group
/// count from the in-memory attempt.
///
/// Target: each partition holds about half of max_in_memory_groups groups,
/// rounded up to the next power of 2 and clamped to [2, max_partitions].
fn choosePartitionCount(current_group_count: u16) u64 {
    const target = max_in_memory_groups / 2;
    if (target == 0) return 2;
    var p: u64 = 2;
    const need = if (current_group_count > 0) (@as(u64, current_group_count) + target - 1) / target else 2;
    while (p < need and p < max_partitions) : (p *= 2) {}
    if (p > max_partitions) p = max_partitions;
    return p;
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Apply hash-based GROUP BY aggregation over spilled input data.
///
/// Reads all input rows from `ctx.collector`, groups them using a hash table,
/// and populates `result.rows[0..result.row_count]` with group representative
/// rows and `group_runtime` with aggregate state.
///
/// Returns true on success, false on error (error message set in result).
pub fn applyHashAggregate(
    ctx: *const ExecContext,
    result: *QueryResult,
    group_node: NodeIndex,
    group_op_index: u16,
    schema: *const RowSchema,
    ops: *const [max_operators]OpDescriptor,
    op_count: u16,
    caps: *const capacity_mod.OperatorCapacities,
    group_runtime: *GroupRuntime,
    string_arena: *StringArena,
) bool {
    result.stats.plan.group_strategy = .hash_spill;

    // Parse group keys from AST.
    const node = ctx.ast.getNode(group_node);
    const group_key_count = ctx.ast.listLen(node.data.unary);
    if (group_key_count == 0) {
        setError(result, "group requires at least one key");
        return false;
    }
    if (@as(usize, group_key_count) > caps.group_keys) {
        setError(result, "group key capacity exceeded");
        return false;
    }
    var group_key_indices: [max_group_keys]u16 = undefined;
    if (!aggregation_mod.buildGroupKeyIndices(
        ctx,
        result,
        schema,
        node.data.unary,
        group_key_count,
        group_key_indices[0..],
    )) {
        return false;
    }

    // Setup group runtime.
    group_runtime.active = true;
    group_runtime.group_key_count = group_key_count;
    @memcpy(
        group_runtime.group_key_indices[0..group_key_count],
        group_key_indices[0..group_key_count],
    );
    @memset(group_runtime.group_counts[0..], 0);

    if (!aggregation_mod.collectPostGroupAggregates(
        ctx,
        result,
        ops,
        op_count,
        group_op_index,
        caps,
        group_runtime,
    )) {
        return false;
    }

    // Dispatch to implementation.
    applyHashAggregateImpl(
        ctx,
        result,
        group_key_indices[0..group_key_count],
        schema,
        group_runtime,
        string_arena,
    ) catch |err| {
        const msg = switch (err) {
            error.TooManyGroups => "hash aggregate: too many groups for available partitions",
            error.TempPagesExhausted => "hash aggregate: temp page budget exhausted",
            error.SpillError => "hash aggregate: spill serialization failed",
            error.ArenaExhausted => "hash aggregate: string arena exhausted",
            error.StorageError => "hash aggregate: storage I/O error",
            error.InternalError => "hash aggregate: internal error",
            error.AggregateEvalFailed => "hash aggregate: aggregate evaluation failed",
        };
        setError(result, msg);
        return false;
    };
    return true;
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

fn applyHashAggregateImpl(
    ctx: *const ExecContext,
    result: *QueryResult,
    group_key_indices: []const u16,
    schema: *const RowSchema,
    group_runtime: *GroupRuntime,
    string_arena: *StringArena,
) HashAggregateError!void {
    const collector = ctx.collector;

    // Flush hot batch so all data is in spill pages.
    if (collector.hot_count > 0) {
        collector.flushHotBatch() catch return error.SpillError;
    }

    // --- In-memory attempt ---
    var table = HashTable.init();
    var group_count: u16 = 0;
    var needs_spill = false;

    var read_buf: [scan_mod.max_row_size_bytes + 256]u8 = undefined;
    var read_arena = StringArena.init(&read_buf);
    var rescue_buf: [rescue_arena_size]u8 = undefined;

    {
        collector.iteration_started = false;
        var iter = collector.iterator();
        var input_row = ResultRow.init();

        while (true) {
            read_arena.reset();
            const has_row = iter.next(&input_row, &read_arena) catch return error.SpillError;
            if (!has_row) break;

            const hash = hashGroupKeys(&input_row, group_key_indices);
            const old_count = group_count;
            const maybe_gi = table.findOrInsert(
                hash,
                &input_row,
                group_key_indices,
                result.rows,
                &group_count,
                @intCast(max_in_memory_groups),
            );

            if (maybe_gi) |gi| {
                if (group_count > old_count) {
                    // New group: copy representative row and initialize state.
                    result.rows[gi] = input_row;
                    try copyRowStringsToArena(&result.rows[gi], string_arena);
                    aggregation_mod.resetAggregateStatesForGroup(group_runtime, gi);
                    group_runtime.group_counts[gi] = 1;
                } else {
                    group_runtime.group_counts[gi] += 1;
                }
                if (!aggregation_mod.accumulateGroupAggregates(
                    ctx,
                    result,
                    schema,
                    group_runtime,
                    gi,
                    input_row.values[0..input_row.column_count],
                    string_arena,
                )) return error.AggregateEvalFailed;
                try rescueAggregateStrings(group_runtime, gi, string_arena);

                // Arena safety valve.
                if (string_arena.bytes.len > 0) {
                    const remaining = string_arena.bytes.len - string_arena.used;
                    const threshold = string_arena.bytes.len / 10;
                    if (remaining < threshold and group_count > 0) {
                        try rescueGroupStrings(result.rows, group_count, group_runtime, &rescue_buf, string_arena);
                    }
                }
            } else {
                needs_spill = true;
                break;
            }
        }
    }

    if (!needs_spill) {
        result.row_count = group_count;
        result.collector = null;
        return;
    }

    // --- Grace hash spill path ---
    const p_count = choosePartitionCount(group_count);

    // Reset everything for full re-processing.
    table.clear();
    group_count = 0;
    @memset(group_runtime.group_counts[0..scan_mod.scan_batch_size], 0);
    string_arena.reset();

    // Partition writers (for partitions 1..P-1; partition 0 aggregates inline).
    var writers: [max_partitions]SpillPageWriter = undefined;
    for (&writers) |*w| w.* = SpillPageWriter.init();

    // Page tracking for spilled partitions.
    var all_page_ids: [max_spill_pages]u64 = undefined;
    var page_partition: [max_spill_pages]u8 = undefined;
    var total_page_count: u32 = 0;
    var partition_row_counts: [max_partitions]u64 = [_]u64{0} ** max_partitions;

    // Re-iterate all input.
    {
        collector.iteration_started = false;
        var iter = collector.iterator();
        var input_row = ResultRow.init();

        while (true) {
            read_arena.reset();
            const has_row = iter.next(&input_row, &read_arena) catch return error.SpillError;
            if (!has_row) break;

            const hash = hashGroupKeys(&input_row, group_key_indices);
            const partition = @as(u8, @intCast(hash % p_count));

            if (partition == 0) {
                // Aggregate inline for partition 0.
                const old_count = group_count;
                const maybe_gi = table.findOrInsert(
                    hash,
                    &input_row,
                    group_key_indices,
                    result.rows,
                    &group_count,
                    @intCast(max_in_memory_groups),
                );
                if (maybe_gi) |gi| {
                    if (group_count > old_count) {
                        result.rows[gi] = input_row;
                        try copyRowStringsToArena(&result.rows[gi], string_arena);
                        aggregation_mod.resetAggregateStatesForGroup(group_runtime, gi);
                        group_runtime.group_counts[gi] = 1;
                    } else {
                        group_runtime.group_counts[gi] += 1;
                    }
                    if (!aggregation_mod.accumulateGroupAggregates(
                        ctx,
                        result,
                        schema,
                        group_runtime,
                        gi,
                        input_row.values[0..input_row.column_count],
                        string_arena,
                    )) return error.AggregateEvalFailed;
                    try rescueAggregateStrings(group_runtime, gi, string_arena);

                    // Arena safety valve.
                    if (string_arena.bytes.len > 0) {
                        const remaining = string_arena.bytes.len - string_arena.used;
                        const threshold = string_arena.bytes.len / 10;
                        if (remaining < threshold and group_count > 0) {
                            try rescueGroupStrings(result.rows, group_count, group_runtime, &rescue_buf, string_arena);
                        }
                    }
                } else {
                    return error.TooManyGroups;
                }
            } else {
                // Serialize to partition's temp pages.
                partition_row_counts[partition] += 1;
                const ok = writers[partition].appendRow(&input_row) catch return error.SpillError;
                if (!ok) {
                    // Flush full page.
                    if (total_page_count >= max_spill_pages) return error.TempPagesExhausted;
                    const payload = writers[partition].finalize();
                    const page_id = collector.temp_mgr.allocateAndWrite(payload, TempPage.null_page_id) catch return error.TempPagesExhausted;
                    all_page_ids[total_page_count] = page_id;
                    page_partition[total_page_count] = partition;
                    total_page_count += 1;
                    writers[partition].reset();
                    const retry = writers[partition].appendRow(&input_row) catch return error.SpillError;
                    std.debug.assert(retry);
                }
            }
        }
    }

    // Flush remaining partition writers.
    for (1..p_count) |p| {
        if (writers[p].row_count > 0) {
            if (total_page_count >= max_spill_pages) return error.TempPagesExhausted;
            const payload = writers[p].finalize();
            const page_id = collector.temp_mgr.allocateAndWrite(payload, TempPage.null_page_id) catch return error.TempPagesExhausted;
            all_page_ids[total_page_count] = page_id;
            page_partition[total_page_count] = @intCast(p);
            total_page_count += 1;
        }
    }

    // Partition 0 groups are already in result.rows[0..group_count] with
    // aggregate state. Now process spilled partitions.
    for (1..p_count) |p| {
        if (partition_row_counts[p] == 0) continue;

        // Clear hash table for this partition (but NOT result.rows or
        // group_runtime — accumulate across partitions).
        table.clear();

        // Read partition p's pages and aggregate.
        for (0..total_page_count) |page_idx| {
            if (page_partition[page_idx] != @as(u8, @intCast(p))) continue;

            const read_result = collector.temp_mgr.readPage(all_page_ids[page_idx]) catch return error.StorageError;
            var local_page = read_result.page;
            const chunk = TempPage.readChunk(&local_page) catch return error.SpillError;
            var reader = SpillPageReader.init(chunk.payload) catch return error.SpillError;

            var input_row = ResultRow.init();
            while (true) {
                read_arena.reset();
                const has_row = reader.next(&input_row, &read_arena) catch return error.SpillError;
                if (!has_row) break;

                const hash = hashGroupKeys(&input_row, group_key_indices);
                const old_count = group_count;
                const maybe_gi = table.findOrInsert(
                    hash,
                    &input_row,
                    group_key_indices,
                    result.rows,
                    &group_count,
                    @intCast(max_in_memory_groups),
                );

                if (maybe_gi) |gi| {
                    if (group_count > old_count) {
                        result.rows[gi] = input_row;
                        try copyRowStringsToArena(&result.rows[gi], string_arena);
                        aggregation_mod.resetAggregateStatesForGroup(group_runtime, gi);
                        group_runtime.group_counts[gi] = 1;
                    } else {
                        group_runtime.group_counts[gi] += 1;
                    }
                    if (!aggregation_mod.accumulateGroupAggregates(
                        ctx,
                        result,
                        schema,
                        group_runtime,
                        gi,
                        input_row.values[0..input_row.column_count],
                        string_arena,
                    )) return error.AggregateEvalFailed;
                    try rescueAggregateStrings(group_runtime, gi, string_arena);

                    if (string_arena.bytes.len > 0) {
                        const remaining = string_arena.bytes.len - string_arena.used;
                        const threshold = string_arena.bytes.len / 10;
                        if (remaining < threshold and group_count > 0) {
                            try rescueGroupStrings(result.rows, group_count, group_runtime, &rescue_buf, string_arena);
                        }
                    }
                } else {
                    return error.TooManyGroups;
                }
            }
        }
    }

    result.row_count = group_count;
    result.collector = null;
}

// ===========================================================================
// Tests
// ===========================================================================

const testing = std.testing;
const disk_mod = @import("../simulator/disk.zig");

fn makeRow(values: []const Value) ResultRow {
    var row = ResultRow.init();
    row.column_count = @intCast(values.len);
    for (values, 0..) |v, i| {
        row.values[i] = v;
    }
    return row;
}

test "hash function determinism" {
    // Same keys must produce the same hash across calls.
    const row1 = makeRow(&.{ .{ .i64 = 42 }, .{ .string = "hello" } });
    const row2 = makeRow(&.{ .{ .i64 = 42 }, .{ .string = "hello" } });
    const row3 = makeRow(&.{ .{ .i64 = 43 }, .{ .string = "hello" } });
    const row4 = makeRow(&.{ .{ .i64 = 42 }, .{ .string = "world" } });

    const keys = [_]u16{ 0, 1 };
    const h1 = hashGroupKeys(&row1, &keys);
    const h2 = hashGroupKeys(&row2, &keys);
    const h3 = hashGroupKeys(&row3, &keys);
    const h4 = hashGroupKeys(&row4, &keys);

    // Identical rows must hash identically.
    try testing.expectEqual(h1, h2);
    // Different values should (with high probability) produce different hashes.
    try testing.expect(h1 != h3);
    try testing.expect(h1 != h4);
}

test "hash function type-tag differentiation" {
    // Ensure different types with the same bit pattern produce different hashes.
    const row_i32 = makeRow(&.{.{ .i32 = 1 }});
    const row_i64 = makeRow(&.{.{ .i64 = 1 }});
    const row_bool = makeRow(&.{.{ .bool = true }});

    const keys = [_]u16{0};
    const h_i32 = hashGroupKeys(&row_i32, &keys);
    const h_i64 = hashGroupKeys(&row_i64, &keys);
    const h_bool = hashGroupKeys(&row_bool, &keys);

    // Type tags differ so hashes should differ.
    try testing.expect(h_i32 != h_i64);
    try testing.expect(h_i32 != h_bool);
    try testing.expect(h_i64 != h_bool);
}

test "hash function null handling" {
    const row_null = makeRow(&.{.{ .null_value = {} }});
    const row_zero = makeRow(&.{.{ .i64 = 0 }});

    const keys = [_]u16{0};
    const h_null = hashGroupKeys(&row_null, &keys);
    const h_zero = hashGroupKeys(&row_zero, &keys);

    // Null should not collide with zero.
    try testing.expect(h_null != h_zero);
}

test "hash table insert and find" {
    var table = HashTable.init();
    var groups: [16]ResultRow = undefined;
    var group_count: u16 = 0;

    const keys = [_]u16{0};

    // Insert group with key=10.
    const row_a = makeRow(&.{.{ .i64 = 10 }});
    const hash_a = hashGroupKeys(&row_a, &keys);
    const gi_a = table.findOrInsert(hash_a, &row_a, &keys, &groups, &group_count, 16);
    try testing.expect(gi_a != null);
    try testing.expectEqual(@as(u16, 0), gi_a.?);
    try testing.expectEqual(@as(u16, 1), group_count);
    groups[0] = row_a;

    // Insert group with key=20.
    const row_b = makeRow(&.{.{ .i64 = 20 }});
    const hash_b = hashGroupKeys(&row_b, &keys);
    const gi_b = table.findOrInsert(hash_b, &row_b, &keys, &groups, &group_count, 16);
    try testing.expect(gi_b != null);
    try testing.expectEqual(@as(u16, 1), gi_b.?);
    try testing.expectEqual(@as(u16, 2), group_count);
    groups[1] = row_b;

    // Find existing group with key=10.
    const gi_a2 = table.findOrInsert(hash_a, &row_a, &keys, &groups, &group_count, 16);
    try testing.expect(gi_a2 != null);
    try testing.expectEqual(@as(u16, 0), gi_a2.?);
    try testing.expectEqual(@as(u16, 2), group_count); // No new group.

    // Find existing group with key=20.
    const gi_b2 = table.findOrInsert(hash_b, &row_b, &keys, &groups, &group_count, 16);
    try testing.expect(gi_b2 != null);
    try testing.expectEqual(@as(u16, 1), gi_b2.?);
    try testing.expectEqual(@as(u16, 2), group_count); // No new group.
}

test "hash table capacity limit" {
    var table = HashTable.init();
    var groups: [4]ResultRow = undefined;
    var group_count: u16 = 0;
    const keys = [_]u16{0};

    // Fill to capacity (max_groups=4).
    var i: i64 = 0;
    while (i < 4) : (i += 1) {
        const row = makeRow(&.{.{ .i64 = i }});
        const hash = hashGroupKeys(&row, &keys);
        const gi = table.findOrInsert(hash, &row, &keys, &groups, &group_count, 4);
        try testing.expect(gi != null);
        groups[gi.?] = row;
    }
    try testing.expectEqual(@as(u16, 4), group_count);

    // 5th distinct group should fail.
    const overflow_row = makeRow(&.{.{ .i64 = 999 }});
    const overflow_hash = hashGroupKeys(&overflow_row, &keys);
    const gi_overflow = table.findOrInsert(overflow_hash, &overflow_row, &keys, &groups, &group_count, 4);
    try testing.expect(gi_overflow == null);

    // But existing group should still be found.
    const existing_row = makeRow(&.{.{ .i64 = 2 }});
    const existing_hash = hashGroupKeys(&existing_row, &keys);
    const gi_existing = table.findOrInsert(existing_hash, &existing_row, &keys, &groups, &group_count, 4);
    try testing.expect(gi_existing != null);
    try testing.expectEqual(@as(u16, 2), gi_existing.?);
}

test "hash table clear resets indices" {
    var table = HashTable.init();
    var groups: [4]ResultRow = undefined;
    var group_count: u16 = 0;
    const keys = [_]u16{0};

    const row = makeRow(&.{.{ .i64 = 42 }});
    const hash = hashGroupKeys(&row, &keys);
    _ = table.findOrInsert(hash, &row, &keys, &groups, &group_count, 4);
    groups[0] = row;
    try testing.expectEqual(@as(u16, 1), group_count);

    // Clear and reset group count.
    table.clear();
    group_count = 0;

    // Same key should produce a new group (index 0 again).
    const gi2 = table.findOrInsert(hash, &row, &keys, &groups, &group_count, 4);
    try testing.expect(gi2 != null);
    try testing.expectEqual(@as(u16, 0), gi2.?);
    try testing.expectEqual(@as(u16, 1), group_count);
}

test "key equality with multiple columns" {
    const row1 = makeRow(&.{ .{ .i64 = 1 }, .{ .string = "a" }, .{ .i32 = 10 } });
    const row2 = makeRow(&.{ .{ .i64 = 1 }, .{ .string = "a" }, .{ .i32 = 99 } });
    const row3 = makeRow(&.{ .{ .i64 = 1 }, .{ .string = "b" }, .{ .i32 = 10 } });

    // Keys on columns 0 and 1 only.
    const keys_01 = [_]u16{ 0, 1 };
    try testing.expect(rowsEqualOnKeys(&row1, &row2, &keys_01)); // col2 differs but not a key
    try testing.expect(!rowsEqualOnKeys(&row1, &row3, &keys_01)); // col1 differs

    // Key on column 0 only.
    const keys_0 = [_]u16{0};
    try testing.expect(rowsEqualOnKeys(&row1, &row2, &keys_0));
    try testing.expect(rowsEqualOnKeys(&row1, &row3, &keys_0));
}

test "key equality with null values" {
    const row_null1 = makeRow(&.{.{ .null_value = {} }});
    const row_null2 = makeRow(&.{.{ .null_value = {} }});
    const row_zero = makeRow(&.{.{ .i64 = 0 }});

    const keys = [_]u16{0};
    // Two nulls are equal (for grouping purposes, compareValues(null, null) == .eq).
    try testing.expect(rowsEqualOnKeys(&row_null1, &row_null2, &keys));
    // Null vs non-null are not equal.
    try testing.expect(!rowsEqualOnKeys(&row_null1, &row_zero, &keys));
}

test "choosePartitionCount basic cases" {
    // With max_in_memory_groups=4096, target=2048:
    // 4096 groups -> need=2 partitions
    try testing.expectEqual(@as(u64, 2), choosePartitionCount(4096));
    // 1 group -> need=1, clamped to 2
    try testing.expectEqual(@as(u64, 2), choosePartitionCount(1));
    // 0 groups -> default 2
    try testing.expectEqual(@as(u64, 2), choosePartitionCount(0));
}

test "choosePartitionCount returns power of 2" {
    // All return values should be powers of 2.
    const test_counts = [_]u16{ 0, 1, 100, 2048, 4096 };
    for (test_counts) |count| {
        const p = choosePartitionCount(count);
        try testing.expect(p >= 2);
        try testing.expect(p <= max_partitions);
        // Check power of 2: p & (p-1) == 0
        try testing.expectEqual(@as(u64, 0), p & (p - 1));
    }
}

test "copyRowStringsToArena copies string values" {
    var arena_buf: [1024]u8 = undefined;
    var arena = StringArena.init(&arena_buf);

    // Create a row with a string that points to some external buffer.
    const external_str = "external";
    var row = makeRow(&.{ .{ .i64 = 1 }, .{ .string = external_str } });

    try copyRowStringsToArena(&row, &arena);

    // String should now point into the arena, not the external buffer.
    try testing.expectEqualStrings("external", row.values[1].string);
    try testing.expect(row.values[1].string.ptr != external_str.ptr);
    // Should be within arena bounds.
    const arena_start = @intFromPtr(arena_buf[0..].ptr);
    const arena_end = arena_start + arena_buf.len;
    const str_addr = @intFromPtr(row.values[1].string.ptr);
    try testing.expect(str_addr >= arena_start and str_addr < arena_end);
}
