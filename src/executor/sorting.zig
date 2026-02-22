//! ORDER BY execution via bounded in-place insertion sort.
//!
//! Supports multi-key sorting with column references and expression keys,
//! ascending/descending order, and group-aware expression evaluation.
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
    result.stats.plan.sort_strategy = .in_place_insertion;
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
    if (@as(usize, result.row_count) > caps.sort_rows) {
        setError(result, "sort row capacity exceeded");
        return false;
    }

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

    sortRowsInPlace(
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

fn buildSortKeyDescriptors(
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

const SortEvalError = error{
    EvalFailed,
};

fn sortRowsInPlace(
    ctx: *const ExecContext,
    result: *QueryResult,
    schema: *const RowSchema,
    sort_keys: []const SortKeyDescriptor,
    group_runtime: *GroupRuntime,
    string_arena: *scan_mod.StringArena,
) SortEvalError!void {
    if (result.row_count <= 1) return;

    var i: u16 = 1;
    while (i < result.row_count) : (i += 1) {
        var j: u16 = i;
        while (j > 0) {
            const prev_idx = j - 1;
            const order = compareRowsBySortKeys(
                ctx,
                schema,
                group_runtime,
                prev_idx,
                j,
                &result.rows[prev_idx],
                &result.rows[j],
                sort_keys,
                string_arena,
            ) catch return error.EvalFailed;
            if (order != .gt) break;
            const temp = result.rows[prev_idx];
            result.rows[prev_idx] = result.rows[j];
            result.rows[j] = temp;
            if (group_runtime.active) {
                const count_tmp = group_runtime.group_counts[prev_idx];
                group_runtime.group_counts[prev_idx] =
                    group_runtime.group_counts[j];
                group_runtime.group_counts[j] = count_tmp;
            }
            j -= 1;
        }
    }
}

fn compareRowsBySortKeys(
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
