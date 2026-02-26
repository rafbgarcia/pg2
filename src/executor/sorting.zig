//! ORDER BY execution via stable bottom-up merge sort.
//!
//! Supports multi-key sorting with column references and expression keys,
//! ascending/descending order, and group-aware expression evaluation.
//!
//! Uses `scratch_rows_b` from `ExecContext` as the merge auxiliary buffer,
//! giving O(n log n) worst-case with stability (equal-key rows preserve
//! their original order).
const std = @import("std");
const ast_mod = @import("../parser/ast.zig");
const row_mod = @import("../storage/row.zig");
const scan_mod = @import("scan.zig");
const filter_mod = @import("filter.zig");
const capacity_mod = @import("capacity.zig");
const aggregation_mod = @import("aggregation.zig");

const NodeIndex = ast_mod.NodeIndex;
const null_node = ast_mod.null_node;
const Value = row_mod.Value;
const RowSchema = row_mod.RowSchema;
const compareValues = row_mod.compareValues;
const ResultRow = scan_mod.ResultRow;
const ExecContext = @import("executor.zig").ExecContext;
const QueryResult = @import("executor.zig").QueryResult;
const evalContextForExec = @import("executor.zig").evalContextForExec;
const setError = @import("executor.zig").setError;
const GroupRuntime = aggregation_mod.GroupRuntime;
const AggregateState = aggregation_mod.AggregateState;

const max_sort_keys = capacity_mod.max_sort_keys;
const sort_key_desc_mask: u16 = 0x0001;
const sort_key_expr_mask: u16 = 0x8000;
const max_parallel_sort_workers: usize = 8;
const max_parallel_worker_cap: usize = 8;
const parallel_sort_worker_arena_bytes: usize = filter_mod.max_string_result_bytes * 2;

pub const SortKeyKind = enum {
    column,
    expression,
};

pub const SortKeyDescriptor = struct {
    kind: SortKeyKind,
    descending: bool,
    column_index: u16 = 0,
    expr_node: NodeIndex = null_node,
};

pub fn applySort(
    ctx: *const ExecContext,
    result: *QueryResult,
    sort_node: NodeIndex,
    schema: *const RowSchema,
    caps: *const capacity_mod.OperatorCapacities,
    group_runtime: *GroupRuntime,
    string_arena: *scan_mod.StringArena,
    parallel_enabled: bool,
    parallel_min_rows_per_worker: u16,
) bool {
    return applySortWithAux(
        ctx,
        result,
        sort_node,
        schema,
        caps,
        group_runtime,
        string_arena,
        ctx.scratch_rows_b,
        parallel_enabled,
        parallel_min_rows_per_worker,
    );
}

pub fn applySortWithAux(
    ctx: *const ExecContext,
    result: *QueryResult,
    sort_node: NodeIndex,
    schema: *const RowSchema,
    caps: *const capacity_mod.OperatorCapacities,
    group_runtime: *GroupRuntime,
    string_arena: *scan_mod.StringArena,
    aux_rows: []ResultRow,
    parallel_enabled: bool,
    parallel_min_rows_per_worker: u16,
) bool {
    const node = ctx.ast.getNode(sort_node);
    const key_count = ctx.ast.listLen(node.data.unary);
    if (key_count == 0) {
        setError(result, "sort requires at least one key");
        return false;
    }
    if (@as(usize, key_count) > caps.sort_keys) {
        setError(result, "sort capacity exceeded");
        return false;
    }
    // max_sort_rows is no longer enforced as a hard error (Phase 3b).
    // In-memory sort handles up to scan_batch_size rows; larger inputs
    // are routed to external merge sort by the executor pipeline.

    var sort_keys: [max_sort_keys]SortKeyDescriptor = undefined;
    if (!buildSortKeyDescriptors(
        ctx,
        result,
        node.data.unary,
        schema,
        sort_keys[0..],
        key_count,
    )) {
        return false;
    }

    if (parallel_enabled and
        tryApplySortParallel(
            ctx,
            result,
            schema,
            sort_keys[0..key_count],
            group_runtime,
            aux_rows,
            parallel_min_rows_per_worker,
            string_arena,
        ))
    {
        result.stats.plan.parallel_schedule_applied_tasks =
            result.stats.plan.parallel_schedule_task_count;
        return true;
    }

    sortRowsMergeWithAux(
        ctx,
        result,
        schema,
        sort_keys[0..key_count],
        group_runtime,
        aux_rows,
        string_arena,
    ) catch {
        setError(result, "sort key evaluation failed");
        return false;
    };
    return true;
}

const ParallelSortWorker = struct {
    ctx: *const ExecContext,
    group_runtime: *GroupRuntime,
    rows: []ResultRow,
    state_indices: *[scan_mod.scan_batch_size]u16,
    schema: *const RowSchema,
    sort_keys: []const SortKeyDescriptor,
    start_idx: u16,
    end_idx: u16,
    had_eval_error: bool = false,

    fn run(self: *ParallelSortWorker) void {
        var arena_buf: [parallel_sort_worker_arena_bytes]u8 = undefined;
        var arena = scan_mod.StringArena.init(&arena_buf);
        insertionSortWindow(
            self.ctx,
            self.group_runtime,
            self.rows,
            self.state_indices,
            self.schema,
            self.sort_keys,
            self.start_idx,
            self.end_idx,
            &arena,
        ) catch {
            self.had_eval_error = true;
        };
    }
};

fn tryApplySortParallel(
    ctx: *const ExecContext,
    result: *QueryResult,
    schema: *const RowSchema,
    sort_keys: []const SortKeyDescriptor,
    group_runtime: *GroupRuntime,
    aux_rows: []ResultRow,
    parallel_min_rows_per_worker: u16,
    string_arena: *scan_mod.StringArena,
) bool {
    const row_count: usize = result.row_count;
    const min_rows_per_worker = @as(usize, @max(@as(u16, 1), parallel_min_rows_per_worker));
    if (row_count < min_rows_per_worker * 2) return false;

    const configured_cap = @max(
        @as(usize, 1),
        @min(
            @as(usize, result.stats.plan.parallel_worker_budget),
            max_parallel_worker_cap,
        ),
    );
    const stage_cap = @min(max_parallel_sort_workers, configured_cap);
    const max_workers = @min(stage_cap, row_count);
    var worker_count = @min(max_workers, row_count / min_rows_per_worker);
    if (worker_count < 2) return false;
    if (worker_count > max_parallel_sort_workers) worker_count = max_parallel_sort_workers;

    var boundaries: [max_parallel_sort_workers + 1]u16 = [_]u16{0} ** (max_parallel_sort_workers + 1);
    const base = row_count / worker_count;
    const remainder = row_count % worker_count;
    var cursor: usize = 0;
    var worker_idx: usize = 0;
    while (worker_idx < worker_count) : (worker_idx += 1) {
        boundaries[worker_idx] = @intCast(cursor);
        const span = base + if (worker_idx < remainder) @as(usize, 1) else @as(usize, 0);
        cursor += span;
    }
    boundaries[worker_count] = @intCast(cursor);
    std.debug.assert(boundaries[worker_count] == result.row_count);

    var state_indices: [scan_mod.scan_batch_size]u16 = undefined;
    var row_idx: u16 = 0;
    while (row_idx < result.row_count) : (row_idx += 1) {
        state_indices[row_idx] = row_idx;
    }

    var workers: [max_parallel_sort_workers]ParallelSortWorker = undefined;
    worker_idx = 0;
    while (worker_idx < worker_count) : (worker_idx += 1) {
        const start = boundaries[worker_idx];
        const end = boundaries[worker_idx + 1];
        workers[worker_idx] = .{
            .ctx = ctx,
            .group_runtime = group_runtime,
            .rows = result.rows[0..result.row_count],
            .state_indices = &state_indices,
            .schema = schema,
            .sort_keys = sort_keys,
            .start_idx = start,
            .end_idx = end,
        };
    }

    var threads: [max_parallel_sort_workers - 1]?std.Thread =
        [_]?std.Thread{null} ** (max_parallel_sort_workers - 1);
    var spawned: usize = 0;
    worker_idx = 1;
    while (worker_idx < worker_count) : (worker_idx += 1) {
        threads[spawned] = std.Thread.spawn(.{}, ParallelSortWorker.run, .{&workers[worker_idx]}) catch {
            var join_idx: usize = 0;
            while (join_idx < spawned) : (join_idx += 1) {
                threads[join_idx].?.join();
            }
            if (group_runtime.active and group_runtime.aggregate_count > 0) {
                reorderAggregateStatesToCurrentRowOrder(
                    group_runtime,
                    &state_indices,
                    result.row_count,
                );
            }
            return false;
        };
        spawned += 1;
    }
    workers[0].run();
    var join_idx: usize = 0;
    while (join_idx < spawned) : (join_idx += 1) {
        threads[join_idx].?.join();
    }
    worker_idx = 0;
    while (worker_idx < worker_count) : (worker_idx += 1) {
        if (!workers[worker_idx].had_eval_error) continue;
        setError(result, "sort key evaluation failed");
        return true;
    }

    mergeSortedChunks(
        ctx,
        result.rows[0..result.row_count],
        aux_rows[0..result.row_count],
        group_runtime,
        &state_indices,
        schema,
        sort_keys,
        boundaries[0 .. worker_count + 1],
        string_arena,
    ) catch {
        setError(result, "sort key evaluation failed");
        return true;
    };
    return true;
}

fn insertionSortWindow(
    ctx: *const ExecContext,
    group_runtime: *GroupRuntime,
    rows: []ResultRow,
    state_indices: *[scan_mod.scan_batch_size]u16,
    schema: *const RowSchema,
    sort_keys: []const SortKeyDescriptor,
    start_idx: u16,
    end_idx: u16,
    string_arena: *scan_mod.StringArena,
) SortEvalError!void {
    if (end_idx <= start_idx + 1) return;
    var i: u16 = start_idx + 1;
    while (i < end_idx) : (i += 1) {
        var j: u16 = i;
        while (j > start_idx) {
            const order = compareRowsBySortKeys(
                ctx,
                schema,
                group_runtime,
                state_indices[j - 1],
                state_indices[j],
                &rows[j - 1],
                &rows[j],
                sort_keys,
                string_arena,
            ) catch return error.EvalFailed;
            if (order != .gt) break;

            const tmp_row = rows[j - 1];
            rows[j - 1] = rows[j];
            rows[j] = tmp_row;

            if (group_runtime.active) {
                const tmp_count = group_runtime.group_counts[j - 1];
                group_runtime.group_counts[j - 1] = group_runtime.group_counts[j];
                group_runtime.group_counts[j] = tmp_count;
            }

            const tmp_idx = state_indices[j - 1];
            state_indices[j - 1] = state_indices[j];
            state_indices[j] = tmp_idx;
            j -= 1;
        }
    }
}

fn reorderAggregateStatesToCurrentRowOrder(
    group_runtime: *GroupRuntime,
    state_indices: *[scan_mod.scan_batch_size]u16,
    row_count: u16,
) void {
    if (!group_runtime.active or group_runtime.aggregate_count == 0) return;
    var reordered_states: [scan_mod.scan_batch_size]AggregateState = undefined;
    var slot: u16 = 0;
    while (slot < group_runtime.aggregate_count) : (slot += 1) {
        var idx: u16 = 0;
        while (idx < row_count) : (idx += 1) {
            const state_idx = state_indices[idx];
            reordered_states[idx] = group_runtime.aggregate_states[slot][state_idx];
        }
        @memcpy(
            group_runtime.aggregate_states[slot][0..row_count],
            reordered_states[0..row_count],
        );
    }
    var idx: u16 = 0;
    while (idx < row_count) : (idx += 1) {
        state_indices[idx] = idx;
    }
}

fn mergeSortedChunks(
    ctx: *const ExecContext,
    rows: []ResultRow,
    aux: []ResultRow,
    group_runtime: *GroupRuntime,
    state_indices: *[scan_mod.scan_batch_size]u16,
    schema: *const RowSchema,
    sort_keys: []const SortKeyDescriptor,
    boundaries: []const u16,
    string_arena: *scan_mod.StringArena,
) SortEvalError!void {
    const chunk_count = boundaries.len - 1;
    std.debug.assert(chunk_count >= 2);

    var positions: [max_parallel_sort_workers]u16 = undefined;
    for (0..chunk_count) |i| positions[i] = boundaries[i];
    var aux_counts: [scan_mod.scan_batch_size]u32 = undefined;
    var aux_state_indices: [scan_mod.scan_batch_size]u16 = undefined;

    var out: usize = 0;
    while (out < rows.len) : (out += 1) {
        var best_chunk: ?usize = null;
        var chunk_idx: usize = 0;
        while (chunk_idx < chunk_count) : (chunk_idx += 1) {
            if (positions[chunk_idx] >= boundaries[chunk_idx + 1]) continue;
            if (best_chunk == null) {
                best_chunk = chunk_idx;
                continue;
            }
            const current_idx = positions[chunk_idx];
            const best_idx = positions[best_chunk.?];
            const order = compareRowsBySortKeys(
                ctx,
                schema,
                group_runtime,
                state_indices[current_idx],
                state_indices[best_idx],
                &rows[current_idx],
                &rows[best_idx],
                sort_keys,
                string_arena,
            ) catch return error.EvalFailed;
            if (order == .lt or (order == .eq and chunk_idx < best_chunk.?)) {
                best_chunk = chunk_idx;
            }
        }
        std.debug.assert(best_chunk != null);
        const selected_chunk = best_chunk.?;
        const selected_idx = positions[selected_chunk];
        aux[out] = rows[selected_idx];
        if (group_runtime.active) aux_counts[out] = group_runtime.group_counts[selected_idx];
        aux_state_indices[out] = state_indices[selected_idx];
        positions[selected_chunk] += 1;
    }
    @memcpy(rows, aux[0..rows.len]);
    if (group_runtime.active) {
        @memcpy(group_runtime.group_counts[0..rows.len], aux_counts[0..rows.len]);
    }
    @memcpy(state_indices[0..rows.len], aux_state_indices[0..rows.len]);
    if (group_runtime.active and group_runtime.aggregate_count > 0) {
        reorderAggregateStatesToCurrentRowOrder(group_runtime, state_indices, @intCast(rows.len));
    }
}

pub fn buildSortKeyDescriptors(
    ctx: *const ExecContext,
    result: *QueryResult,
    first_key: NodeIndex,
    schema: *const RowSchema,
    out_keys: []SortKeyDescriptor,
    key_count: u16,
) bool {
    std.debug.assert(@as(usize, key_count) <= out_keys.len);

    var current = first_key;
    var index: u16 = 0;
    while (current != null_node and index < key_count) : (index += 1) {
        const key_node = ctx.ast.getNode(current);
        if (key_node.tag != .sort_key) {
            setError(result, "invalid sort key node");
            return false;
        }

        const descending = (key_node.extra & sort_key_desc_mask) != 0;
        const is_expr = (key_node.extra & sort_key_expr_mask) != 0;
        if (is_expr) {
            const expr_node = key_node.data.unary;
            if (expr_node == null_node) {
                setError(result, "invalid sort expression key");
                return false;
            }
            out_keys[index] = .{
                .kind = .expression,
                .descending = descending,
                .expr_node = expr_node,
            };
        } else {
            const col_name = ctx.tokens.getText(key_node.data.token, ctx.source);
            const col_index = schema.findColumn(col_name) orelse {
                setError(result, "sort column not found");
                return false;
            };
            out_keys[index] = .{
                .kind = .column,
                .descending = descending,
                .column_index = col_index,
            };
        }

        current = key_node.next;
    }

    if (index != key_count) {
        setError(result, "sort key list malformed");
        return false;
    }
    return true;
}

pub const SortEvalError = error{
    EvalFailed,
};

/// Bottom-up merge sort using `ctx.scratch_rows_b` as auxiliary buffer.
///
/// Stable: equal-key rows preserve their original order.
/// O(n log n) worst-case time, O(n) auxiliary space (the pre-allocated
/// scratch buffer — no heap allocation).
///
/// Group counts are kept in sync with rows so that `COUNT(*)` resolution
/// via `group_runtime.group_counts[row_index]` remains correct after
/// sorting.
pub fn sortRowsMerge(
    ctx: *const ExecContext,
    result: *QueryResult,
    schema: *const RowSchema,
    sort_keys: []const SortKeyDescriptor,
    group_runtime: *GroupRuntime,
    string_arena: *scan_mod.StringArena,
) SortEvalError!void {
    try sortRowsMergeWithAux(
        ctx,
        result,
        schema,
        sort_keys,
        group_runtime,
        ctx.scratch_rows_b,
        string_arena,
    );
}

pub fn sortRowsMergeWithAux(
    ctx: *const ExecContext,
    result: *QueryResult,
    schema: *const RowSchema,
    sort_keys: []const SortKeyDescriptor,
    group_runtime: *GroupRuntime,
    aux_rows: []ResultRow,
    string_arena: *scan_mod.StringArena,
) SortEvalError!void {
    const n = result.row_count;
    if (n <= 1) return;

    std.debug.assert(aux_rows.len >= n);

    // Auxiliary group counts buffer (16 KB on stack for scan_batch_size=4096).
    var aux_counts: [scan_mod.scan_batch_size]u32 = undefined;
    var src_group_state_indices: [scan_mod.scan_batch_size]u16 = undefined;
    var dst_group_state_indices: [scan_mod.scan_batch_size]u16 = undefined;
    var i: u16 = 0;
    while (i < n) : (i += 1) {
        src_group_state_indices[i] = i;
    }

    var src_rows: []ResultRow = result.rows[0..n];
    var dst_rows: []ResultRow = aux_rows[0..n];
    var in_result = true;

    var width: u16 = 1;
    while (width < n) {
        // Merge pass: merge adjacent runs of `width` from src into dst.
        var run_start: u16 = 0;
        while (run_start < n) {
            const mid = @min(run_start +| width, n);
            const end = @min(mid +| width, n);

            try mergeAdjacentRuns(
                ctx,
                schema,
                group_runtime,
                sort_keys,
                string_arena,
                src_rows,
                dst_rows,
                &group_runtime.group_counts,
                &aux_counts,
                &src_group_state_indices,
                &dst_group_state_indices,
                run_start,
                mid,
                end,
            );

            run_start = end;
        }

        // After the merge pass, merged counts are in aux_counts.
        // Copy back to group_runtime.group_counts so the next pass's
        // comparisons (which resolve aggregates via row_index into
        // group_runtime) read the correct values.
        if (group_runtime.active) {
            @memcpy(group_runtime.group_counts[0..n], aux_counts[0..n]);
        }

        // Swap row buffers for the next pass.
        const tmp = src_rows;
        src_rows = dst_rows;
        dst_rows = tmp;
        in_result = !in_result;
        const tmp_indices = src_group_state_indices;
        src_group_state_indices = dst_group_state_indices;
        dst_group_state_indices = tmp_indices;

        // Prevent u16 overflow on the final doubling.
        if (width > n / 2) break;
        width *= 2;
    }

    // Ensure sorted rows end up in result.rows.
    if (!in_result) {
        @memcpy(result.rows[0..n], src_rows);
    }
    if (group_runtime.active and group_runtime.aggregate_count > 0) {
        var reordered_states: [scan_mod.scan_batch_size]AggregateState = undefined;
        var slot: u16 = 0;
        while (slot < group_runtime.aggregate_count) : (slot += 1) {
            var idx: u16 = 0;
            while (idx < n) : (idx += 1) {
                const state_idx = src_group_state_indices[idx];
                reordered_states[idx] = group_runtime.aggregate_states[slot][state_idx];
            }
            @memcpy(
                group_runtime.aggregate_states[slot][0..n],
                reordered_states[0..n],
            );
        }
    }
    // group_runtime.group_counts is already correct (copied after each pass).
}

/// Merge two adjacent sorted runs [left..mid) and [mid..right) from `src`
/// into `dst`, maintaining stability (left run wins ties).
///
/// Group counts are merged in parallel from `src_counts` to `dst_counts`.
fn mergeAdjacentRuns(
    ctx: *const ExecContext,
    schema: *const RowSchema,
    group_runtime: *const GroupRuntime,
    sort_keys: []const SortKeyDescriptor,
    string_arena: *scan_mod.StringArena,
    src: []const ResultRow,
    dst: []ResultRow,
    src_counts: *const [scan_mod.scan_batch_size]u32,
    dst_counts: *[scan_mod.scan_batch_size]u32,
    src_group_state_indices: *const [scan_mod.scan_batch_size]u16,
    dst_group_state_indices: *[scan_mod.scan_batch_size]u16,
    left: u16,
    mid: u16,
    right: u16,
) SortEvalError!void {
    var l = left;
    var r = mid;
    var out = left;

    while (l < mid and r < right) {
        const order = compareRowsBySortKeys(
            ctx,
            schema,
            group_runtime,
            src_group_state_indices[l],
            src_group_state_indices[r],
            &src[l],
            &src[r],
            sort_keys,
            string_arena,
        ) catch return error.EvalFailed;

        if (order != .gt) {
            // Left <= right: take left (preserves stability).
            dst[out] = src[l];
            if (group_runtime.active) dst_counts[out] = src_counts[l];
            if (group_runtime.active) dst_group_state_indices[out] = src_group_state_indices[l];
            l += 1;
        } else {
            dst[out] = src[r];
            if (group_runtime.active) dst_counts[out] = src_counts[r];
            if (group_runtime.active) dst_group_state_indices[out] = src_group_state_indices[r];
            r += 1;
        }
        out += 1;
    }

    // Copy remaining left run.
    while (l < mid) {
        dst[out] = src[l];
        if (group_runtime.active) dst_counts[out] = src_counts[l];
        if (group_runtime.active) dst_group_state_indices[out] = src_group_state_indices[l];
        l += 1;
        out += 1;
    }

    // Copy remaining right run.
    while (r < right) {
        dst[out] = src[r];
        if (group_runtime.active) dst_counts[out] = src_counts[r];
        if (group_runtime.active) dst_group_state_indices[out] = src_group_state_indices[r];
        r += 1;
        out += 1;
    }
}

pub fn compareRowsBySortKeys(
    ctx: *const ExecContext,
    schema: *const RowSchema,
    group_runtime: *const GroupRuntime,
    lhs_row_index: u16,
    rhs_row_index: u16,
    lhs_row: *const ResultRow,
    rhs_row: *const ResultRow,
    sort_keys: []const SortKeyDescriptor,
    string_arena: *scan_mod.StringArena,
) SortEvalError!std.math.Order {
    for (sort_keys) |key| {
        const lhs_value = evaluateSortKeyValue(
            ctx,
            schema,
            group_runtime,
            lhs_row_index,
            lhs_row,
            key,
            string_arena,
        ) catch return error.EvalFailed;
        const rhs_value = evaluateSortKeyValue(
            ctx,
            schema,
            group_runtime,
            rhs_row_index,
            rhs_row,
            key,
            string_arena,
        ) catch return error.EvalFailed;
        var order = compareValues(lhs_value, rhs_value);
        if (key.descending) {
            order = switch (order) {
                .lt => .gt,
                .gt => .lt,
                .eq => .eq,
            };
        }
        if (order != .eq) return order;
    }
    return .eq;
}

fn evaluateSortKeyValue(
    ctx: *const ExecContext,
    schema: *const RowSchema,
    group_runtime: *const GroupRuntime,
    row_index: u16,
    row: *const ResultRow,
    key: SortKeyDescriptor,
    string_arena: *scan_mod.StringArena,
) filter_mod.EvalError!Value {
    var exec_eval = evalContextForExec(ctx, string_arena);
    exec_eval.bind();
    return switch (key.kind) {
        .column => row.values[key.column_index],
        .expression => if (group_runtime.active)
            aggregation_mod.evaluateGroupedExpression(
                ctx,
                group_runtime,
                key.expr_node,
                row.values[0..row.column_count],
                schema,
                row_index,
                &exec_eval.eval_ctx,
            )
        else
            filter_mod.evaluateExpressionFull(
                ctx.ast,
                ctx.tokens,
                ctx.source,
                key.expr_node,
                row.values[0..row.column_count],
                schema,
                null,
                &exec_eval.eval_ctx,
            ),
    };
}
