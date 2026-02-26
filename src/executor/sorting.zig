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

const max_sort_keys = capacity_mod.max_sort_keys;
const sort_key_desc_mask: u16 = 0x0001;
const sort_key_expr_mask: u16 = 0x8000;

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

    sortRowsMerge(
        ctx,
        result,
        schema,
        sort_keys[0..key_count],
        group_runtime,
        string_arena,
    ) catch {
        setError(result, "sort key evaluation failed");
        return false;
    };
    return true;
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
    const n = result.row_count;
    if (n <= 1) return;

    const aux_rows = ctx.scratch_rows_b;
    std.debug.assert(aux_rows.len >= n);

    // Auxiliary group counts buffer (16 KB on stack for scan_batch_size=4096).
    var aux_counts: [scan_mod.scan_batch_size]u32 = undefined;

    var src_rows: []ResultRow = result.rows[0..n];
    var dst_rows: []ResultRow = aux_rows[0..n];
    var in_result = true;

    var width: u16 = 1;
    while (width < n) {
        // Merge pass: merge adjacent runs of `width` from src into dst.
        var i: u16 = 0;
        while (i < n) {
            const mid = @min(i +| width, n);
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
                i,
                mid,
                end,
            );

            i = end;
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

        // Prevent u16 overflow on the final doubling.
        if (width > n / 2) break;
        width *= 2;
    }

    // Ensure sorted rows end up in result.rows.
    if (!in_result) {
        @memcpy(result.rows[0..n], src_rows);
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
            l,
            r,
            &src[l],
            &src[r],
            sort_keys,
            string_arena,
        ) catch return error.EvalFailed;

        if (order != .gt) {
            // Left <= right: take left (preserves stability).
            dst[out] = src[l];
            if (group_runtime.active) dst_counts[out] = src_counts[l];
            l += 1;
        } else {
            dst[out] = src[r];
            if (group_runtime.active) dst_counts[out] = src_counts[r];
            r += 1;
        }
        out += 1;
    }

    // Copy remaining left run.
    while (l < mid) {
        dst[out] = src[l];
        if (group_runtime.active) dst_counts[out] = src_counts[l];
        l += 1;
        out += 1;
    }

    // Copy remaining right run.
    while (r < right) {
        dst[out] = src[r];
        if (group_runtime.active) dst_counts[out] = src_counts[r];
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
