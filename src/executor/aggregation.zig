//! GROUP BY execution and aggregate function state machines.
//!
//! Handles group key matching, aggregate accumulation (SUM, AVG, MIN,
//! MAX, COUNT), and grouped expression/predicate evaluation via
//! aggregate resolvers.
const std = @import("std");
const ast_mod = @import("../parser/ast.zig");
const tokenizer_mod = @import("../parser/tokenizer.zig");
const row_mod = @import("../storage/row.zig");
const scan_mod = @import("scan.zig");
const filter_mod = @import("filter.zig");
const capacity_mod = @import("capacity.zig");

const NodeIndex = ast_mod.NodeIndex;
const null_node = ast_mod.null_node;
const TokenizeResult = tokenizer_mod.TokenizeResult;
const Value = row_mod.Value;
const RowSchema = row_mod.RowSchema;
const compareValues = row_mod.compareValues;
const ResultRow = scan_mod.ResultRow;
const ExecContext = @import("executor.zig").ExecContext;
const QueryResult = @import("executor.zig").QueryResult;
const max_operators = @import("executor.zig").max_operators;
const PlanOp = @import("executor.zig").PlanOp;
const OpDescriptor = @import("executor.zig").OpDescriptor;
const evalContextForExec = @import("executor.zig").evalContextForExec;
const setError = @import("executor.zig").setError;

const max_group_aggregate_exprs = capacity_mod.max_group_aggregate_exprs;
const invalid_aggregate_slot: u8 = std.math.maxInt(u8);
const sort_key_expr_mask: u16 = 0x8000;
const max_parallel_group_workers: usize = 8;
const max_parallel_worker_cap: usize = 8;
const parallel_group_min_rows_per_worker: usize = 32;

pub const AggregateKind = enum {
    count_star,
    sum,
    avg,
    min,
    max,
};

pub const AggregateDescriptor = struct {
    node_index: NodeIndex,
    kind: AggregateKind,
    arg_node: NodeIndex,
};

pub const AggregateState = struct {
    value_count: u32 = 0,
    sum_bigint: i64 = 0,
    sum_float: f64 = 0.0,
    has_min: bool = false,
    has_max: bool = false,
    min_value: Value = .{ .null_value = {} },
    max_value: Value = .{ .null_value = {} },
    value_type: ?row_mod.ColumnType = null,
};

pub const GroupRuntime = struct {
    active: bool = false,
    group_key_count: u16 = 0,
    group_key_indices: [capacity_mod.max_group_keys]u16 = undefined,
    group_counts: [scan_mod.scan_batch_size]u32 =
        [_]u32{0} ** scan_mod.scan_batch_size,
    aggregate_count: u16 = 0,
    aggregate_descriptors: [max_group_aggregate_exprs]AggregateDescriptor =
        undefined,
    aggregate_slot_by_node: [ast_mod.max_ast_nodes]u8 =
        [_]u8{invalid_aggregate_slot} ** ast_mod.max_ast_nodes,
    aggregate_states: [max_group_aggregate_exprs][scan_mod.scan_batch_size]AggregateState =
        undefined,
};

pub const GroupAggregateContext = struct {
    ctx: *const ExecContext,
    group_runtime: *const GroupRuntime,
    row_index: u16,
};

pub fn applyGroup(
    ctx: *const ExecContext,
    result: *QueryResult,
    group_node: NodeIndex,
    schema: *const RowSchema,
    ops: *const [max_operators]OpDescriptor,
    op_count: u16,
    group_op_index: u16,
    caps: *const capacity_mod.OperatorCapacities,
    group_runtime: *GroupRuntime,
    parallel_enabled: bool,
    string_arena: *scan_mod.StringArena,
) bool {
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
    var group_key_indices: [capacity_mod.max_group_keys]u16 = undefined;
    if (!buildGroupKeyIndices(
        ctx,
        result,
        schema,
        node.data.unary,
        group_key_count,
        group_key_indices[0..],
    )) {
        return false;
    }

    group_runtime.active = true;
    group_runtime.group_key_count = group_key_count;
    @memcpy(
        group_runtime.group_key_indices[0..group_key_count],
        group_key_indices[0..group_key_count],
    );
    @memset(group_runtime.group_counts[0..], 0);
    if (!collectPostGroupAggregates(
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

    if (parallel_enabled and
        tryApplyGroupParallel(
            ctx,
            result,
            schema,
            group_key_indices[0..group_key_count],
            caps,
            group_runtime,
            string_arena,
        ))
    {
        result.stats.plan.parallel_schedule_applied_tasks =
            result.stats.plan.parallel_schedule_task_count;
        return true;
    }

    var write_idx: u16 = 0;
    var read_idx: u16 = 0;
    while (read_idx < result.row_count) : (read_idx += 1) {
        const candidate = &result.rows[read_idx];
        if (findGroupIndex(
            result.rows[0..write_idx],
            candidate,
            group_key_indices[0..group_key_count],
        )) |group_index| {
            group_runtime.group_counts[group_index] += 1;
            if (!accumulateGroupAggregates(
                ctx,
                result,
                schema,
                group_runtime,
                group_index,
                candidate.values[0..candidate.column_count],
                string_arena,
            )) {
                return false;
            }
        } else {
            if (@as(usize, write_idx) >= caps.aggregate_groups) {
                setError(result, "aggregate group capacity exceeded");
                return false;
            }
            result.rows[write_idx] = candidate.*;
            resetAggregateStatesForGroup(group_runtime, write_idx);
            group_runtime.group_counts[write_idx] = 1;
            if (!accumulateGroupAggregates(
                ctx,
                result,
                schema,
                group_runtime,
                write_idx,
                candidate.values[0..candidate.column_count],
                string_arena,
            )) {
                return false;
            }
            write_idx += 1;
        }
    }
    result.row_count = write_idx;
    return true;
}

const ParallelGroupWorker = struct {
    rows: []const ResultRow,
    key_indices: []const u16,
    start_idx: u16,
    end_idx: u16,
    local_group_count: u16 = 0,
    local_first_indices: [scan_mod.scan_batch_size]u16 =
        [_]u16{0} ** scan_mod.scan_batch_size,
    local_group_counts: [scan_mod.scan_batch_size]u32 =
        [_]u32{0} ** scan_mod.scan_batch_size,

    fn run(self: *ParallelGroupWorker) void {
        var read_idx = self.start_idx;
        while (read_idx < self.end_idx) : (read_idx += 1) {
            const candidate = &self.rows[read_idx];
            if (findGroupIndexByFirstIndices(
                self.rows,
                &self.local_first_indices,
                self.local_group_count,
                candidate,
                self.key_indices,
            )) |local_group_index| {
                self.local_group_counts[local_group_index] += 1;
            } else {
                const slot = self.local_group_count;
                self.local_first_indices[slot] = read_idx;
                self.local_group_counts[slot] = 1;
                self.local_group_count += 1;
            }
        }
    }
};

fn findGroupIndexByFirstIndices(
    rows: []const ResultRow,
    local_first_indices: *const [scan_mod.scan_batch_size]u16,
    local_group_count: u16,
    candidate: *const ResultRow,
    key_indices: []const u16,
) ?u16 {
    var idx: u16 = 0;
    while (idx < local_group_count) : (idx += 1) {
        const first_idx = local_first_indices[idx];
        if (rowsEqualOnGroupKeys(&rows[first_idx], candidate, key_indices)) {
            return idx;
        }
    }
    return null;
}

fn tryApplyGroupParallel(
    ctx: *const ExecContext,
    result: *QueryResult,
    schema: *const RowSchema,
    key_indices: []const u16,
    caps: *const capacity_mod.OperatorCapacities,
    group_runtime: *GroupRuntime,
    string_arena: *scan_mod.StringArena,
) bool {
    const row_count_usize: usize = result.row_count;
    if (row_count_usize < parallel_group_min_rows_per_worker * 2) return false;

    const configured_cap = @max(
        @as(usize, 1),
        @min(
            @as(usize, result.stats.plan.parallel_worker_budget),
            max_parallel_worker_cap,
        ),
    );
    const stage_cap = @min(max_parallel_group_workers, configured_cap);
    const max_workers = @min(stage_cap, row_count_usize);
    var worker_count = @min(max_workers, row_count_usize / parallel_group_min_rows_per_worker);
    if (worker_count < 2) return false;
    if (worker_count > max_parallel_group_workers) worker_count = max_parallel_group_workers;

    const source_rows = ctx.scratch_rows_b[0..result.row_count];
    @memcpy(source_rows, result.rows[0..result.row_count]);

    var boundaries: [max_parallel_group_workers + 1]u16 =
        [_]u16{0} ** (max_parallel_group_workers + 1);
    const base = row_count_usize / worker_count;
    const remainder = row_count_usize % worker_count;
    var cursor: usize = 0;
    var worker_idx: usize = 0;
    while (worker_idx < worker_count) : (worker_idx += 1) {
        boundaries[worker_idx] = @intCast(cursor);
        const span = base + if (worker_idx < remainder) @as(usize, 1) else @as(usize, 0);
        cursor += span;
    }
    boundaries[worker_count] = @intCast(cursor);
    std.debug.assert(boundaries[worker_count] == result.row_count);

    var workers: [max_parallel_group_workers]ParallelGroupWorker = undefined;
    worker_idx = 0;
    while (worker_idx < worker_count) : (worker_idx += 1) {
        workers[worker_idx] = .{
            .rows = source_rows,
            .key_indices = key_indices,
            .start_idx = boundaries[worker_idx],
            .end_idx = boundaries[worker_idx + 1],
        };
    }

    var threads: [max_parallel_group_workers - 1]?std.Thread =
        [_]?std.Thread{null} ** (max_parallel_group_workers - 1);
    var spawned: usize = 0;
    worker_idx = 1;
    while (worker_idx < worker_count) : (worker_idx += 1) {
        threads[spawned] = std.Thread.spawn(
            .{},
            ParallelGroupWorker.run,
            .{&workers[worker_idx]},
        ) catch {
            var join_idx: usize = 0;
            while (join_idx < spawned) : (join_idx += 1) {
                threads[join_idx].?.join();
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

    @memset(group_runtime.group_counts[0..], 0);
    var write_idx: u16 = 0;
    worker_idx = 0;
    while (worker_idx < worker_count) : (worker_idx += 1) {
        var local_idx: u16 = 0;
        while (local_idx < workers[worker_idx].local_group_count) : (local_idx += 1) {
            const first_idx = workers[worker_idx].local_first_indices[local_idx];
            const candidate = &source_rows[first_idx];
            const local_count = workers[worker_idx].local_group_counts[local_idx];
            if (findGroupIndex(
                result.rows[0..write_idx],
                candidate,
                key_indices,
            )) |group_index| {
                group_runtime.group_counts[group_index] += local_count;
            } else {
                if (@as(usize, write_idx) >= caps.aggregate_groups) {
                    setError(result, "aggregate group capacity exceeded");
                    return true;
                }
                result.rows[write_idx] = candidate.*;
                group_runtime.group_counts[write_idx] = local_count;
                write_idx += 1;
            }
        }
    }
    result.row_count = write_idx;
    std.debug.assert(@as(usize, result.row_count) <= row_count_usize);

    if (group_runtime.aggregate_count == 0) return true;

    var group_idx: u16 = 0;
    while (group_idx < result.row_count) : (group_idx += 1) {
        resetAggregateStatesForGroup(group_runtime, group_idx);
    }

    var read_idx: u16 = 0;
    while (read_idx < @as(u16, @intCast(row_count_usize))) : (read_idx += 1) {
        const candidate = &source_rows[read_idx];
        const resolved_group = findGroupIndex(
            result.rows[0..result.row_count],
            candidate,
            key_indices,
        ) orelse {
            setError(result, "group merge state mismatch");
            return true;
        };
        if (!accumulateGroupAggregates(
            ctx,
            result,
            schema,
            group_runtime,
            resolved_group,
            candidate.values[0..candidate.column_count],
            string_arena,
        )) {
            return true;
        }
    }

    return true;
}

pub fn collectPostGroupAggregates(
    ctx: *const ExecContext,
    result: *QueryResult,
    ops: *const [max_operators]OpDescriptor,
    op_count: u16,
    group_op_index: u16,
    caps: *const capacity_mod.OperatorCapacities,
    group_runtime: *GroupRuntime,
) bool {
    group_runtime.aggregate_count = 0;
    @memset(
        group_runtime.aggregate_slot_by_node[0..],
        invalid_aggregate_slot,
    );

    var op_idx = group_op_index + 1;
    while (op_idx < op_count) : (op_idx += 1) {
        const op = ops[op_idx];
        switch (op.kind) {
            .where_filter, .having_filter => {
                const where_node = ctx.ast.getNode(op.node);
                if (!registerAggregateExprTree(
                    ctx,
                    result,
                    where_node.data.unary,
                    caps,
                    group_runtime,
                )) return false;
            },
            .sort_op => {
                const sort_node = ctx.ast.getNode(op.node);
                var key = sort_node.data.unary;
                while (key != null_node) {
                    const key_node = ctx.ast.getNode(key);
                    const is_expr = (key_node.extra & sort_key_expr_mask) != 0;
                    if (is_expr and !registerAggregateExprTree(
                        ctx,
                        result,
                        key_node.data.unary,
                        caps,
                        group_runtime,
                    )) {
                        return false;
                    }
                    key = key_node.next;
                }
            },
            .limit_op, .offset_op => {
                const n = ctx.ast.getNode(op.node);
                if (!registerAggregateExprTree(
                    ctx,
                    result,
                    n.data.unary,
                    caps,
                    group_runtime,
                )) return false;
            },
            .group_op, .insert_op, .update_op, .delete_op, .inspect_op => {},
        }
    }

    return true;
}

pub fn resetAggregateStatesForGroup(
    group_runtime: *GroupRuntime,
    group_index: u16,
) void {
    var slot: u16 = 0;
    while (slot < group_runtime.aggregate_count) : (slot += 1) {
        group_runtime.aggregate_states[slot][group_index] = .{};
    }
}

fn registerAggregateExprTree(
    ctx: *const ExecContext,
    result: *QueryResult,
    expr_root: NodeIndex,
    caps: *const capacity_mod.OperatorCapacities,
    group_runtime: *GroupRuntime,
) bool {
    if (expr_root == null_node) return true;

    var stack: [ast_mod.max_ast_nodes]NodeIndex = undefined;
    var stack_len: usize = 0;
    stack[0] = expr_root;
    stack_len = 1;

    while (stack_len > 0) {
        stack_len -= 1;
        const node_idx = stack[stack_len];
        if (node_idx == null_node) continue;
        const node = ctx.ast.getNode(node_idx);

        if (node.tag == .expr_aggregate) {
            if (!registerAggregateDescriptor(
                ctx,
                result,
                node_idx,
                caps,
                group_runtime,
            )) {
                return false;
            }
        }

        switch (node.tag) {
            .expr_binary, .expr_in, .expr_not_in => {
                if (stack_len + 2 > stack.len) {
                    setError(result, "aggregate expression too complex");
                    return false;
                }
                stack[stack_len] = node.data.binary.lhs;
                stack[stack_len + 1] = node.data.binary.rhs;
                stack_len += 2;
            },
            .expr_unary, .expr_function_call, .expr_list, .expr_aggregate => {
                var child = node.data.unary;
                while (child != null_node) {
                    if (stack_len >= stack.len) {
                        setError(result, "aggregate expression too complex");
                        return false;
                    }
                    stack[stack_len] = child;
                    stack_len += 1;
                    child = ctx.ast.getNode(child).next;
                }
            },
            else => {},
        }
    }

    return true;
}

fn registerAggregateDescriptor(
    ctx: *const ExecContext,
    result: *QueryResult,
    aggregate_node: NodeIndex,
    caps: *const capacity_mod.OperatorCapacities,
    group_runtime: *GroupRuntime,
) bool {
    std.debug.assert(aggregate_node < ast_mod.max_ast_nodes);
    if (group_runtime.aggregate_slot_by_node[aggregate_node] !=
        invalid_aggregate_slot)
    {
        return true;
    }

    const node = ctx.ast.getNode(aggregate_node);
    const token_type = ctx.tokens.tokens[node.extra].token_type;
    const descriptor = switch (token_type) {
        .agg_count => blk: {
            if (node.data.unary != null_node) {
                setError(result, "count aggregate shape unsupported");
                return false;
            }
            break :blk null;
        },
        .agg_sum => blk: {
            if (node.data.unary == null_node) {
                setError(result, "sum aggregate requires an argument");
                return false;
            }
            break :blk AggregateDescriptor{
                .node_index = aggregate_node,
                .kind = .sum,
                .arg_node = node.data.unary,
            };
        },
        .agg_avg => blk: {
            if (node.data.unary == null_node) {
                setError(result, "avg aggregate requires an argument");
                return false;
            }
            break :blk AggregateDescriptor{
                .node_index = aggregate_node,
                .kind = .avg,
                .arg_node = node.data.unary,
            };
        },
        .agg_min => blk: {
            if (node.data.unary == null_node) {
                setError(result, "min aggregate requires an argument");
                return false;
            }
            break :blk AggregateDescriptor{
                .node_index = aggregate_node,
                .kind = .min,
                .arg_node = node.data.unary,
            };
        },
        .agg_max => blk: {
            if (node.data.unary == null_node) {
                setError(result, "max aggregate requires an argument");
                return false;
            }
            break :blk AggregateDescriptor{
                .node_index = aggregate_node,
                .kind = .max,
                .arg_node = node.data.unary,
            };
        },
        else => {
            setError(result, "unsupported aggregate function");
            return false;
        },
    };

    if (descriptor) |desc| {
        if (group_runtime.aggregate_count >= max_group_aggregate_exprs) {
            setError(result, "aggregate expression capacity exceeded");
            return false;
        }
        const groups_upper_bound = @min(
            @as(usize, result.row_count),
            caps.aggregate_groups,
        );
        const used_slots = @as(usize, group_runtime.aggregate_count) + 1;
        const total_state_bytes = used_slots *
            groups_upper_bound *
            @sizeOf(AggregateState);
        if (total_state_bytes > caps.aggregate_state_bytes) {
            setError(result, "aggregate state capacity exceeded");
            return false;
        }

        const slot = group_runtime.aggregate_count;
        group_runtime.aggregate_descriptors[slot] = desc;
        group_runtime.aggregate_slot_by_node[aggregate_node] = @intCast(slot);
        group_runtime.aggregate_count += 1;
    }
    return true;
}

pub fn accumulateGroupAggregates(
    ctx: *const ExecContext,
    result: *QueryResult,
    schema: *const RowSchema,
    group_runtime: *GroupRuntime,
    group_index: u16,
    row_values: []const Value,
    string_arena: *scan_mod.StringArena,
) bool {
    var slot: u16 = 0;
    while (slot < group_runtime.aggregate_count) : (slot += 1) {
        const descriptor = group_runtime.aggregate_descriptors[slot];
        if (descriptor.kind == .count_star) continue;

        var exec_eval = evalContextForExec(ctx, string_arena);
        exec_eval.bind();
        const arg_value = filter_mod.evaluateExpressionFull(
            ctx.ast,
            ctx.tokens,
            ctx.source,
            descriptor.arg_node,
            row_values,
            schema,
            null,
            &exec_eval.eval_ctx,
        ) catch {
            setError(result, "aggregate evaluation failed");
            return false;
        };

        const state = &group_runtime.aggregate_states[slot][group_index];
        updateAggregateState(state, descriptor.kind, arg_value) catch {
            setError(result, "aggregate evaluation failed");
            return false;
        };
    }
    return true;
}

pub fn updateAggregateState(
    state: *AggregateState,
    kind: AggregateKind,
    value: Value,
) filter_mod.EvalError!void {
    if (value == .null_value) return;
    const value_type = value.columnType() orelse return error.TypeMismatch;

    switch (kind) {
        .sum => {
            switch (value) {
                .i8 => |v| {
                    if (state.value_type == null) state.value_type = .i8;
                    if (state.value_type.? != .i8) return error.TypeMismatch;
                    state.sum_bigint = std.math.add(i64, state.sum_bigint, v) catch
                        return error.NumericOverflow;
                },
                .i16 => |v| {
                    if (state.value_type == null) state.value_type = .i16;
                    if (state.value_type.? != .i16) return error.TypeMismatch;
                    state.sum_bigint = std.math.add(i64, state.sum_bigint, v) catch
                        return error.NumericOverflow;
                },
                .i64 => |v| {
                    if (state.value_type == null) state.value_type = .i64;
                    if (state.value_type.? != .i64) return error.TypeMismatch;
                    state.sum_bigint = std.math.add(i64, state.sum_bigint, v) catch
                        return error.NumericOverflow;
                },
                .i32 => |v| {
                    if (state.value_type == null) state.value_type = .i32;
                    if (state.value_type.? != .i32) return error.TypeMismatch;
                    state.sum_bigint = std.math.add(i64, state.sum_bigint, v) catch
                        return error.NumericOverflow;
                },
                .u8 => |v| {
                    if (state.value_type == null) state.value_type = .u8;
                    if (state.value_type.? != .u8) return error.TypeMismatch;
                    state.sum_bigint = std.math.add(i64, state.sum_bigint, v) catch
                        return error.NumericOverflow;
                },
                .u16 => |v| {
                    if (state.value_type == null) state.value_type = .u16;
                    if (state.value_type.? != .u16) return error.TypeMismatch;
                    state.sum_bigint = std.math.add(i64, state.sum_bigint, v) catch
                        return error.NumericOverflow;
                },
                .u32 => |v| {
                    if (state.value_type == null) state.value_type = .u32;
                    if (state.value_type.? != .u32) return error.TypeMismatch;
                    state.sum_bigint = std.math.add(i64, state.sum_bigint, v) catch
                        return error.NumericOverflow;
                },
                .u64 => |v| {
                    if (state.value_type == null) state.value_type = .u64;
                    if (state.value_type.? != .u64) return error.TypeMismatch;
                    const narrowed = std.math.cast(i64, v) orelse return error.NumericOverflow;
                    state.sum_bigint = std.math.add(i64, state.sum_bigint, narrowed) catch
                        return error.NumericOverflow;
                },
                .f64 => |v| {
                    if (state.value_type == null) state.value_type = .f64;
                    if (state.value_type.? != .f64) return error.TypeMismatch;
                    state.sum_float += v;
                },
                else => return error.TypeMismatch,
            }
            state.value_count += 1;
        },
        .avg => {
            const numeric = switch (value) {
                .i8 => |v| @as(f64, @floatFromInt(v)),
                .i16 => |v| @as(f64, @floatFromInt(v)),
                .i64 => |v| @as(f64, @floatFromInt(v)),
                .i32 => |v| @as(f64, @floatFromInt(v)),
                .u8 => |v| @as(f64, @floatFromInt(v)),
                .u16 => |v| @as(f64, @floatFromInt(v)),
                .u32 => |v| @as(f64, @floatFromInt(v)),
                .u64 => |v| @as(f64, @floatFromInt(v)),
                .f64 => |v| v,
                else => return error.TypeMismatch,
            };
            if (state.value_type == null) {
                state.value_type = value_type;
            } else if (state.value_type.? != value_type) {
                return error.TypeMismatch;
            }
            state.sum_float += numeric;
            state.value_count += 1;
        },
        .min => {
            if (state.value_type == null) {
                state.value_type = value_type;
            } else if (state.value_type.? != value_type) {
                return error.TypeMismatch;
            }
            if (!state.has_min or compareValues(value, state.min_value) == .lt) {
                state.min_value = value;
                state.has_min = true;
            }
        },
        .max => {
            if (state.value_type == null) {
                state.value_type = value_type;
            } else if (state.value_type.? != value_type) {
                return error.TypeMismatch;
            }
            if (!state.has_max or compareValues(value, state.max_value) == .gt) {
                state.max_value = value;
                state.has_max = true;
            }
        },
        .count_star => {},
    }
}

pub fn buildGroupKeyIndices(
    ctx: *const ExecContext,
    result: *QueryResult,
    schema: *const RowSchema,
    first_group_key: NodeIndex,
    group_key_count: u16,
    out_indices: []u16,
) bool {
    std.debug.assert(@as(usize, group_key_count) <= out_indices.len);

    var key_idx: u16 = 0;
    var current = first_group_key;
    while (current != null_node and key_idx < group_key_count) : (key_idx += 1) {
        const key_node = ctx.ast.getNode(current);
        if (key_node.tag != .expr_column_ref) {
            setError(result, "group key must be a column");
            return false;
        }
        const col_name = ctx.tokens.getText(key_node.data.token, ctx.source);
        const col_index = schema.findColumn(col_name) orelse {
            setError(result, "group column not found");
            return false;
        };
        out_indices[key_idx] = col_index;
        current = key_node.next;
    }
    return key_idx == group_key_count;
}

fn findGroupIndex(
    grouped_rows: []const ResultRow,
    candidate: *const ResultRow,
    key_indices: []const u16,
) ?u16 {
    var idx: u16 = 0;
    for (grouped_rows) |grouped| {
        if (rowsEqualOnGroupKeys(&grouped, candidate, key_indices)) {
            return idx;
        }
        idx += 1;
    }
    return null;
}

fn rowsEqualOnGroupKeys(
    lhs: *const ResultRow,
    rhs: *const ResultRow,
    key_indices: []const u16,
) bool {
    for (key_indices) |col_index| {
        if (compareValues(lhs.values[col_index], rhs.values[col_index]) != .eq) {
            return false;
        }
    }
    return true;
}

pub fn evaluateGroupedPredicate(
    ctx: *const ExecContext,
    group_runtime: *const GroupRuntime,
    predicate: NodeIndex,
    row_values: []const Value,
    schema: *const RowSchema,
    row_index: u16,
    eval_ctx: *const filter_mod.EvalContext,
) filter_mod.EvalError!bool {
    const agg_ctx = GroupAggregateContext{
        .ctx = ctx,
        .group_runtime = group_runtime,
        .row_index = row_index,
    };
    const resolver = filter_mod.AggregateResolver{
        .ctx = &agg_ctx,
        .resolve = resolveGroupedAggregate,
    };
    return filter_mod.evaluatePredicateFull(
        ctx.ast,
        ctx.tokens,
        ctx.source,
        predicate,
        row_values,
        schema,
        &resolver,
        eval_ctx,
    );
}

pub fn evaluateGroupedExpression(
    ctx: *const ExecContext,
    group_runtime: *const GroupRuntime,
    expr: NodeIndex,
    row_values: []const Value,
    schema: *const RowSchema,
    row_index: u16,
    eval_ctx: *const filter_mod.EvalContext,
) filter_mod.EvalError!Value {
    const agg_ctx = GroupAggregateContext{
        .ctx = ctx,
        .group_runtime = group_runtime,
        .row_index = row_index,
    };
    const resolver = filter_mod.AggregateResolver{
        .ctx = &agg_ctx,
        .resolve = resolveGroupedAggregate,
    };
    return filter_mod.evaluateExpressionFull(
        ctx.ast,
        ctx.tokens,
        ctx.source,
        expr,
        row_values,
        schema,
        &resolver,
        eval_ctx,
    );
}

fn resolveGroupedAggregate(
    raw_ctx: *const anyopaque,
    node_index: NodeIndex,
    row_values: []const Value,
    schema: *const RowSchema,
) filter_mod.EvalError!Value {
    _ = row_values;
    _ = schema;

    const agg_ctx: *const GroupAggregateContext = @ptrCast(@alignCast(raw_ctx));
    if (!agg_ctx.group_runtime.active) return error.UnknownFunction;

    const node = agg_ctx.ctx.ast.getNode(node_index);
    if (node.tag != .expr_aggregate) return error.UnknownFunction;
    const token_type = agg_ctx.ctx.tokens.tokens[node.extra].token_type;
    if (token_type == .agg_count) {
        if (node.data.unary != null_node) return error.UnknownFunction;
        const count = agg_ctx.group_runtime.group_counts[agg_ctx.row_index];
        return .{ .i64 = @intCast(count) };
    }

    if (node_index >= agg_ctx.group_runtime.aggregate_slot_by_node.len) {
        return error.UnknownFunction;
    }
    const slot = agg_ctx.group_runtime.aggregate_slot_by_node[node_index];
    if (slot == invalid_aggregate_slot) return error.UnknownFunction;

    const descriptor = agg_ctx.group_runtime.aggregate_descriptors[slot];
    const state = agg_ctx.group_runtime.aggregate_states[slot][agg_ctx.row_index];
    return switch (descriptor.kind) {
        .count_star => return error.UnknownFunction,
        .sum => if (state.value_type) |ty| switch (ty) {
            .i8, .i16, .i32, .i64, .u8, .u16, .u32, .u64 => .{ .i64 = state.sum_bigint },
            .f64 => .{ .f64 = state.sum_float },
            else => return error.TypeMismatch,
        } else .{ .null_value = {} },
        .avg => blk: {
            if (state.value_count == 0) break :blk .{ .null_value = {} };
            const divisor = @as(f64, @floatFromInt(state.value_count));
            break :blk .{ .f64 = state.sum_float / divisor };
        },
        .min => if (state.has_min) state.min_value else .{ .null_value = {} },
        .max => if (state.has_max) state.max_value else .{ .null_value = {} },
    };
}
