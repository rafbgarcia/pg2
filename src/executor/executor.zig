const std = @import("std");
const ast_mod = @import("../parser/ast.zig");
const tokenizer_mod = @import("../parser/tokenizer.zig");
const row_mod = @import("../storage/row.zig");
const buffer_pool_mod = @import("../storage/buffer_pool.zig");
const wal_mod = @import("../storage/wal.zig");
const catalog_mod = @import("../catalog/catalog.zig");
const tx_mod = @import("../mvcc/transaction.zig");
const undo_mod = @import("../mvcc/undo.zig");
const filter_mod = @import("filter.zig");
const scan_mod = @import("scan.zig");
const mutation_mod = @import("mutation.zig");
const capacity_mod = @import("capacity.zig");

const Allocator = std.mem.Allocator;
const Ast = ast_mod.Ast;
const NodeIndex = ast_mod.NodeIndex;
const NodeTag = ast_mod.NodeTag;
const null_node = ast_mod.null_node;
const TokenizeResult = tokenizer_mod.TokenizeResult;
const Value = row_mod.Value;
const RowSchema = row_mod.RowSchema;
const BufferPool = buffer_pool_mod.BufferPool;
const Wal = wal_mod.Wal;
const Catalog = catalog_mod.Catalog;
const ModelId = catalog_mod.ModelId;
const AssociationInfo = catalog_mod.AssociationInfo;
const AssociationKind = catalog_mod.AssociationKind;
const null_model = catalog_mod.null_model;
const TxId = tx_mod.TxId;
const Snapshot = tx_mod.Snapshot;
const TxManager = tx_mod.TxManager;
const UndoLog = undo_mod.UndoLog;
const ResultRow = scan_mod.ResultRow;
const compareValues = row_mod.compareValues;
const max_sort_keys = capacity_mod.max_sort_keys;
const max_group_aggregate_exprs = capacity_mod.max_group_aggregate_exprs;
const sort_key_desc_mask: u16 = 0x0001;
const sort_key_expr_mask: u16 = 0x8000;
const invalid_aggregate_slot: u8 = std.math.maxInt(u8);

const AggregateKind = enum {
    count_star,
    sum,
    avg,
    min,
    max,
};

const AggregateDescriptor = struct {
    node_index: NodeIndex,
    kind: AggregateKind,
    arg_node: NodeIndex,
};

const AggregateState = struct {
    value_count: u32 = 0,
    sum_bigint: i64 = 0,
    sum_float: f64 = 0.0,
    has_min: bool = false,
    has_max: bool = false,
    min_value: Value = .{ .null_value = {} },
    max_value: Value = .{ .null_value = {} },
    value_type: ?row_mod.ColumnType = null,
};

const GroupRuntime = struct {
    active: bool = false,
    group_key_count: u16 = 0,
    group_key_indices: [capacity_mod.max_group_keys]u16 = undefined,
    group_counts: [scan_mod.max_result_rows]u32 =
        [_]u32{0} ** scan_mod.max_result_rows,
    aggregate_count: u16 = 0,
    aggregate_descriptors: [max_group_aggregate_exprs]AggregateDescriptor =
        undefined,
    aggregate_slot_by_node: [ast_mod.max_ast_nodes]u8 =
        [_]u8{invalid_aggregate_slot} ** ast_mod.max_ast_nodes,
    aggregate_states: [max_group_aggregate_exprs][scan_mod.max_result_rows]AggregateState =
        undefined,
};

/// Maximum pipeline operators in a single query.
pub const max_operators = capacity_mod.max_pipeline_operators;

/// Execution statistics for a query.
pub const ExecStats = struct {
    rows_scanned: u32 = 0,
    rows_matched: u32 = 0,
    rows_returned: u32 = 0,
    rows_inserted: u32 = 0,
    rows_updated: u32 = 0,
    rows_deleted: u32 = 0,
    pages_read: u32 = 0,
    pages_written: u32 = 0,
};

/// Result of executing a query. Row buffer is heap-allocated from the
/// allocator in ExecContext (per-query arena in production,
/// testing.allocator in tests). Caller must call deinit() when done.
pub const QueryResult = struct {
    rows: []ResultRow,
    row_count: u16 = 0,
    stats: ExecStats = .{},
    has_error: bool = false,
    error_message: [128]u8 = std.mem.zeroes([128]u8),
    allocator: Allocator,

    pub fn init(allocator: Allocator) error{OutOfMemory}!QueryResult {
        const rows = try allocator.alloc(
            ResultRow,
            scan_mod.max_result_rows,
        );
        const result = QueryResult{
            .rows = rows,
            .allocator = allocator,
        };
        std.debug.assert(result.row_count == 0);
        std.debug.assert(result.rows.len == scan_mod.max_result_rows);
        return result;
    }

    pub fn deinit(self: *QueryResult) void {
        self.allocator.free(self.rows);
        self.* = undefined;
    }

    pub fn getError(self: *const QueryResult) ?[]const u8 {
        if (!self.has_error) return null;
        const len = std.mem.indexOfScalar(
            u8,
            &self.error_message,
            0,
        ) orelse self.error_message.len;
        return self.error_message[0..len];
    }
};

/// Execution context passed through the pipeline.
pub const ExecContext = struct {
    catalog: *Catalog,
    pool: *BufferPool,
    wal: *Wal,
    tx_manager: *TxManager,
    undo_log: *UndoLog,
    tx_id: TxId,
    snapshot: *const Snapshot,
    ast: *const Ast,
    tokens: *const TokenizeResult,
    source: []const u8,
    allocator: Allocator,
};

/// Operator kind extracted from the AST.
const OpKind = enum {
    where_filter,
    group_op,
    limit_op,
    offset_op,
    insert_op,
    update_op,
    delete_op,
    sort_op,
    inspect_op,
};

/// A pipeline operator descriptor.
const OpDescriptor = struct {
    kind: OpKind,
    node: NodeIndex,
};

/// Execute a query from a parsed AST.
///
/// Finds the pipeline node from the AST root, resolves the source model,
/// walks the operator chain, and dispatches to the appropriate handlers.
/// Query-level errors are stored in result.error_message. Only
/// OutOfMemory escapes as a Zig error (system-level, not query-level).
pub fn execute(ctx: *const ExecContext) error{OutOfMemory}!QueryResult {
    var result = try QueryResult.init(ctx.allocator);
    errdefer result.deinit();

    // Find pipeline from AST root.
    const pipeline_idx = findPipeline(ctx.ast) orelse {
        setError(&result, "no pipeline found in query");
        return result;
    };
    const pipeline = ctx.ast.getNode(pipeline_idx);
    if (pipeline.tag != .pipeline) {
        setError(&result, "expected pipeline node");
        return result;
    }

    // Resolve source model.
    const source_node = ctx.ast.getNode(pipeline.data.binary.lhs);
    if (source_node.tag != .pipe_source) {
        setError(&result, "expected pipe_source node");
        return result;
    }
    const model_name = ctx.tokens.getText(
        source_node.data.token,
        ctx.source,
    );
    const model_id = ctx.catalog.findModel(model_name) orelse {
        setError(&result, "model not found");
        return result;
    };

    // Build operator list from linked list.
    var ops: [max_operators]OpDescriptor = undefined;
    var op_count: u16 = 0;
    buildOperatorList(
        ctx.ast,
        pipeline.data.binary.rhs,
        &ops,
        &op_count,
    );

    // Check for mutations.
    if (findMutationOp(&ops, op_count)) |mut_idx| {
        executeMutation(ctx, &result, model_id, &ops, op_count, mut_idx);
        return result;
    }

    // Read path: scan → apply operators → project.
    executeReadPipeline(
        ctx,
        &result,
        pipeline_idx,
        model_id,
        &ops,
        op_count,
    );

    std.debug.assert(result.row_count <= scan_mod.max_result_rows);
    return result;
}

/// Execute the read path: table scan, then apply operators in sequence.
fn executeReadPipeline(
    ctx: *const ExecContext,
    result: *QueryResult,
    pipeline_node: NodeIndex,
    model_id: ModelId,
    ops: *const [max_operators]OpDescriptor,
    op_count: u16,
) void {
    const caps = capacity_mod.OperatorCapacities.defaults();

    const scan_result = scan_mod.tableScanInto(
        ctx.catalog,
        ctx.pool,
        ctx.undo_log,
        ctx.snapshot,
        ctx.tx_manager,
        model_id,
        result.rows,
    ) catch {
        setError(result, "table scan failed");
        return;
    };
    result.stats.pages_read = scan_result.pages_read;
    result.stats.rows_scanned = scan_result.row_count;
    result.row_count = scan_result.row_count;

    if (!applyReadOperators(
        ctx,
        result,
        model_id,
        ops,
        op_count,
        &caps,
    )) {
        return;
    }
    if (!applyNestedSelectionJoin(
        ctx,
        result,
        pipeline_node,
        model_id,
        &caps,
    )) {
        return;
    }

    result.stats.rows_matched = result.row_count;
    result.stats.rows_returned = result.row_count;
}

fn applyReadOperators(
    ctx: *const ExecContext,
    result: *QueryResult,
    model_id: ModelId,
    ops: *const [max_operators]OpDescriptor,
    op_count: u16,
    caps: *const capacity_mod.OperatorCapacities,
) bool {
    var group_runtime = GroupRuntime{};
    const schema = &ctx.catalog.models[model_id].row_schema;
    var i: u16 = 0;
    while (i < op_count) : (i += 1) {
        const op = ops[i];
        switch (op.kind) {
            .where_filter => applyWhereFilter(
                ctx,
                result,
                op.node,
                schema,
                &group_runtime,
            ),
            .group_op => {
                if (!applyGroup(
                    ctx,
                    result,
                    op.node,
                    schema,
                    ops,
                    op_count,
                    i,
                    caps,
                    &group_runtime,
                )) return false;
            },
            .limit_op => applyLimit(ctx, result, op.node),
            .offset_op => applyOffset(ctx, result, op.node, &group_runtime),
            .sort_op => {
                if (!applySort(
                    ctx,
                    result,
                    op.node,
                    schema,
                    caps,
                    &group_runtime,
                )) return false;
            },
            .inspect_op => {},
            .insert_op, .update_op, .delete_op => {},
        }
    }
    return true;
}

fn applyNestedSelectionJoin(
    ctx: *const ExecContext,
    result: *QueryResult,
    pipeline_node: NodeIndex,
    source_model_id: ModelId,
    caps: *const capacity_mod.OperatorCapacities,
) bool {
    const selection = getPipelineSelection(ctx.ast, pipeline_node) orelse
        return true;
    const nested = findSingleNestedSelection(ctx.ast, selection, result) orelse
        return !result.has_error;

    const relation_name = ctx.tokens.getText(
        ctx.ast.getNode(nested).extra,
        ctx.source,
    );
    const assoc_id = ctx.catalog.findAssociation(
        source_model_id,
        relation_name,
    ) orelse {
        setError(result, "nested relation association not found");
        return false;
    };
    const assoc = &ctx.catalog.models[source_model_id].associations[assoc_id];
    if (assoc.target_model_id == null_model) {
        setError(result, "nested relation target unresolved");
        return false;
    }
    const target_model_id = assoc.target_model_id;

    var right_result = QueryResult.init(ctx.allocator) catch {
        setError(result, "nested relation setup failed");
        return false;
    };
    defer right_result.deinit();

    const right_scan = scan_mod.tableScanInto(
        ctx.catalog,
        ctx.pool,
        ctx.undo_log,
        ctx.snapshot,
        ctx.tx_manager,
        target_model_id,
        right_result.rows,
    ) catch {
        setError(result, "nested relation scan failed");
        return false;
    };
    right_result.row_count = right_scan.row_count;
    right_result.stats.pages_read = right_scan.pages_read;

    var nested_ops: [max_operators]OpDescriptor = undefined;
    var nested_op_count: u16 = 0;
    if (ctx.ast.getNode(nested).data.unary != null_node) {
        const nested_pipeline = ctx.ast.getNode(ctx.ast.getNode(nested).data.unary);
        if (nested_pipeline.tag != .pipeline) {
            setError(result, "invalid nested relation pipeline");
            return false;
        }
        buildOperatorList(
            ctx.ast,
            nested_pipeline.data.binary.rhs,
            &nested_ops,
            &nested_op_count,
        );
    }
    if (!applyReadOperators(
        ctx,
        &right_result,
        target_model_id,
        &nested_ops,
        nested_op_count,
        caps,
    )) {
        if (right_result.getError()) |msg| setError(result, msg);
        return false;
    }

    const left_copy = ctx.allocator.alloc(ResultRow, result.row_count) catch {
        setError(result, "nested relation setup failed");
        return false;
    };
    defer ctx.allocator.free(left_copy);
    @memcpy(left_copy, result.rows[0..result.row_count]);

    const join = inferAssociationJoinDescriptor(
        ctx.catalog,
        source_model_id,
        assoc,
        result,
    ) orelse return false;
    if (!executeInnerJoinBounded(
        result,
        left_copy,
        right_result.rows[0..right_result.row_count],
        join,
        caps,
    )) {
        return false;
    }
    result.stats.pages_read += right_result.stats.pages_read;
    return true;
}

fn getPipelineSelection(
    tree: *const Ast,
    pipeline_node: NodeIndex,
) ?NodeIndex {
    const pipeline = tree.getNode(pipeline_node);
    if (pipeline.tag != .pipeline) return null;
    const idx: NodeIndex = pipeline.extra;
    if (idx >= tree.node_count) return null;
    const node = tree.getNode(idx);
    if (node.tag != .selection_set) return null;
    return idx;
}

fn findSingleNestedSelection(
    tree: *const Ast,
    selection_node: NodeIndex,
    result: *QueryResult,
) ?NodeIndex {
    const selection = tree.getNode(selection_node);
    if (selection.tag != .selection_set) return null;

    var nested: NodeIndex = null_node;
    var field = selection.data.unary;
    while (field != null_node) {
        const node = tree.getNode(field);
        if (node.tag == .select_nested) {
            if (nested != null_node) {
                setError(result, "multiple nested relations not yet supported");
                return null;
            }
            nested = field;
        }
        field = node.next;
    }
    return if (nested == null_node) null else nested;
}

fn inferAssociationJoinDescriptor(
    catalog: *const Catalog,
    source_model_id: ModelId,
    assoc: *const AssociationInfo,
    result: *QueryResult,
) ?JoinDescriptor {
    switch (assoc.kind) {
        .has_many, .has_one => {
            const left_key = findPrimaryKeyOrId(catalog, source_model_id) orelse {
                setError(result, "association local key not found");
                return null;
            };
            const right_key = findModelForeignKey(
                catalog,
                assoc.target_model_id,
                catalog.getModelName(source_model_id),
            ) orelse {
                setError(result, "association foreign key not found");
                return null;
            };
            return .{
                .left_key_index = left_key,
                .right_key_index = right_key,
            };
        },
        .belongs_to => {
            const left_key = findModelForeignKey(
                catalog,
                source_model_id,
                catalog.getModelName(assoc.target_model_id),
            ) orelse {
                setError(result, "association foreign key not found");
                return null;
            };
            const right_key = findPrimaryKeyOrId(catalog, assoc.target_model_id) orelse {
                setError(result, "association target key not found");
                return null;
            };
            return .{
                .left_key_index = left_key,
                .right_key_index = right_key,
            };
        },
    }
}

fn findPrimaryKeyOrId(
    catalog: *const Catalog,
    model_id: ModelId,
) ?u16 {
    const model = &catalog.models[model_id];
    var i: u16 = 0;
    while (i < model.column_count) : (i += 1) {
        if (model.columns[i].is_primary_key) return i;
    }
    return catalog.findColumn(model_id, "id");
}

fn findModelForeignKey(
    catalog: *const Catalog,
    model_id: ModelId,
    base_model_name: []const u8,
) ?u16 {
    var buf: [96]u8 = undefined;
    const key_name = modelForeignKeyName(base_model_name, &buf) orelse
        return null;
    return catalog.findColumn(model_id, key_name);
}

fn modelForeignKeyName(
    model_name: []const u8,
    out: []u8,
) ?[]const u8 {
    var write_idx: usize = 0;
    var prev_was_lower = false;
    for (model_name, 0..) |ch, i| {
        const is_upper = std.ascii.isUpper(ch);
        if (is_upper and i > 0 and prev_was_lower) {
            if (write_idx >= out.len) return null;
            out[write_idx] = '_';
            write_idx += 1;
        }
        if (write_idx >= out.len) return null;
        out[write_idx] = std.ascii.toLower(ch);
        write_idx += 1;
        prev_was_lower = std.ascii.isLower(ch) or std.ascii.isDigit(ch);
    }
    if (write_idx + 3 > out.len) return null;
    out[write_idx] = '_';
    out[write_idx + 1] = 'i';
    out[write_idx + 2] = 'd';
    write_idx += 3;
    return out[0..write_idx];
}

/// Filter rows in-place using a where predicate.
fn applyWhereFilter(
    ctx: *const ExecContext,
    result: *QueryResult,
    where_node: NodeIndex,
    schema: *const RowSchema,
    group_runtime: *GroupRuntime,
) void {
    const node = ctx.ast.getNode(where_node);
    const predicate = node.data.unary;
    if (predicate == null_node) return;

    const original_count = result.row_count;
    var write_idx: u16 = 0;
    var read_idx: u16 = 0;
    while (read_idx < result.row_count) : (read_idx += 1) {
        const row = &result.rows[read_idx];
        const matches = if (group_runtime.active)
            evaluateGroupedPredicate(
                ctx,
                group_runtime,
                predicate,
                row.values[0..row.column_count],
                schema,
                read_idx,
            ) catch false
        else
            filter_mod.evaluatePredicate(
                ctx.ast,
                ctx.tokens,
                ctx.source,
                predicate,
                row.values[0..row.column_count],
                schema,
            ) catch false;

        if (matches) {
            if (write_idx != read_idx) {
                result.rows[write_idx] = result.rows[read_idx];
                if (group_runtime.active) {
                    group_runtime.group_counts[write_idx] =
                        group_runtime.group_counts[read_idx];
                }
            }
            write_idx += 1;
        }
    }
    result.row_count = write_idx;
    std.debug.assert(result.row_count <= original_count);
}

/// Truncate result to limit rows.
fn applyLimit(
    ctx: *const ExecContext,
    result: *QueryResult,
    limit_node: NodeIndex,
) void {
    const node = ctx.ast.getNode(limit_node);
    const expr = node.data.unary;
    if (expr == null_node) return;

    const val = filter_mod.evaluateExpression(
        ctx.ast,
        ctx.tokens,
        ctx.source,
        expr,
        &.{},
        &RowSchema{},
    ) catch return;

    const limit: u16 = switch (val) {
        .bigint => |v| if (v >= 0)
            @intCast(@min(v, scan_mod.max_result_rows))
        else
            0,
        .int => |v| if (v >= 0)
            @intCast(@min(v, scan_mod.max_result_rows))
        else
            0,
        else => return,
    };

    if (result.row_count > limit) {
        result.row_count = limit;
    }
    std.debug.assert(result.row_count <= limit);
}

/// Skip the first N rows.
fn applyOffset(
    ctx: *const ExecContext,
    result: *QueryResult,
    offset_node: NodeIndex,
    group_runtime: *GroupRuntime,
) void {
    const node = ctx.ast.getNode(offset_node);
    const expr = node.data.unary;
    if (expr == null_node) return;

    const val = filter_mod.evaluateExpression(
        ctx.ast,
        ctx.tokens,
        ctx.source,
        expr,
        &.{},
        &RowSchema{},
    ) catch return;

    const offset: u16 = switch (val) {
        .bigint => |v| if (v >= 0)
            @intCast(@min(v, scan_mod.max_result_rows))
        else
            0,
        .int => |v| if (v >= 0)
            @intCast(@min(v, scan_mod.max_result_rows))
        else
            0,
        else => return,
    };

    if (offset >= result.row_count) {
        result.row_count = 0;
        return;
    }

    const remaining = result.row_count - offset;
    var i: u16 = 0;
    while (i < remaining) : (i += 1) {
        result.rows[i] = result.rows[i + offset];
        if (group_runtime.active) {
            group_runtime.group_counts[i] =
                group_runtime.group_counts[i + offset];
        }
    }
    result.row_count = remaining;
    std.debug.assert(result.row_count == remaining);
}

const SortKeyKind = enum {
    column,
    expression,
};

const SortKeyDescriptor = struct {
    kind: SortKeyKind,
    descending: bool,
    column_index: u16 = 0,
    expr_node: NodeIndex = null_node,
};

fn applySort(
    ctx: *const ExecContext,
    result: *QueryResult,
    sort_node: NodeIndex,
    schema: *const RowSchema,
    caps: *const capacity_mod.OperatorCapacities,
    group_runtime: *GroupRuntime,
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
    ) catch {
        setError(result, "sort key evaluation failed");
        return false;
    };
    return true;
}

fn applyGroup(
    ctx: *const ExecContext,
    result: *QueryResult,
    group_node: NodeIndex,
    schema: *const RowSchema,
    ops: *const [max_operators]OpDescriptor,
    op_count: u16,
    group_op_index: u16,
    caps: *const capacity_mod.OperatorCapacities,
    group_runtime: *GroupRuntime,
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
            )) {
                return false;
            }
            write_idx += 1;
        }
    }
    result.row_count = write_idx;
    return true;
}

fn collectPostGroupAggregates(
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
            .where_filter => {
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

fn resetAggregateStatesForGroup(
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

fn accumulateGroupAggregates(
    ctx: *const ExecContext,
    result: *QueryResult,
    schema: *const RowSchema,
    group_runtime: *GroupRuntime,
    group_index: u16,
    row_values: []const Value,
) bool {
    var slot: u16 = 0;
    while (slot < group_runtime.aggregate_count) : (slot += 1) {
        const descriptor = group_runtime.aggregate_descriptors[slot];
        if (descriptor.kind == .count_star) continue;

        const arg_value = filter_mod.evaluateExpression(
            ctx.ast,
            ctx.tokens,
            ctx.source,
            descriptor.arg_node,
            row_values,
            schema,
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

fn updateAggregateState(
    state: *AggregateState,
    kind: AggregateKind,
    value: Value,
) filter_mod.EvalError!void {
    if (value == .null_value) return;
    const value_type = value.columnType() orelse return error.TypeMismatch;

    switch (kind) {
        .sum => {
            switch (value) {
                .bigint => |v| {
                    if (state.value_type == null) state.value_type = .bigint;
                    if (state.value_type.? != .bigint) return error.TypeMismatch;
                    state.sum_bigint = std.math.add(i64, state.sum_bigint, v) catch
                        return error.NumericOverflow;
                },
                .int => |v| {
                    if (state.value_type == null) state.value_type = .int;
                    if (state.value_type.? != .int) return error.TypeMismatch;
                    state.sum_bigint = std.math.add(i64, state.sum_bigint, v) catch
                        return error.NumericOverflow;
                },
                .float => |v| {
                    if (state.value_type == null) state.value_type = .float;
                    if (state.value_type.? != .float) return error.TypeMismatch;
                    state.sum_float += v;
                },
                else => return error.TypeMismatch,
            }
            state.value_count += 1;
        },
        .avg => {
            const numeric = switch (value) {
                .bigint => |v| @as(f64, @floatFromInt(v)),
                .int => |v| @as(f64, @floatFromInt(v)),
                .float => |v| v,
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

fn buildGroupKeyIndices(
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
) SortEvalError!std.math.Order {
    for (sort_keys) |key| {
        const lhs_value = evaluateSortKeyValue(
            ctx,
            schema,
            group_runtime,
            lhs_row_index,
            lhs_row,
            key,
        ) catch return error.EvalFailed;
        const rhs_value = evaluateSortKeyValue(
            ctx,
            schema,
            group_runtime,
            rhs_row_index,
            rhs_row,
            key,
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
) filter_mod.EvalError!Value {
    return switch (key.kind) {
        .column => row.values[key.column_index],
        .expression => if (group_runtime.active)
            evaluateGroupedExpression(
                ctx,
                group_runtime,
                key.expr_node,
                row.values[0..row.column_count],
                schema,
                row_index,
            )
        else
            filter_mod.evaluateExpression(
                ctx.ast,
                ctx.tokens,
                ctx.source,
                key.expr_node,
                row.values[0..row.column_count],
                schema,
            ),
    };
}

const GroupAggregateContext = struct {
    ctx: *const ExecContext,
    group_runtime: *const GroupRuntime,
    row_index: u16,
};

fn evaluateGroupedPredicate(
    ctx: *const ExecContext,
    group_runtime: *const GroupRuntime,
    predicate: NodeIndex,
    row_values: []const Value,
    schema: *const RowSchema,
    row_index: u16,
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
    return filter_mod.evaluatePredicateWithResolver(
        ctx.ast,
        ctx.tokens,
        ctx.source,
        predicate,
        row_values,
        schema,
        &resolver,
    );
}

fn evaluateGroupedExpression(
    ctx: *const ExecContext,
    group_runtime: *const GroupRuntime,
    expr: NodeIndex,
    row_values: []const Value,
    schema: *const RowSchema,
    row_index: u16,
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
    return filter_mod.evaluateExpressionWithResolver(
        ctx.ast,
        ctx.tokens,
        ctx.source,
        expr,
        row_values,
        schema,
        &resolver,
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
        return .{ .bigint = @intCast(count) };
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
            .bigint, .int => .{ .bigint = state.sum_bigint },
            .float => .{ .float = state.sum_float },
            else => return error.TypeMismatch,
        } else .{ .null_value = {} },
        .avg => blk: {
            if (state.value_count == 0) break :blk .{ .null_value = {} };
            const divisor = @as(f64, @floatFromInt(state.value_count));
            break :blk .{ .float = state.sum_float / divisor };
        },
        .min => if (state.has_min) state.min_value else .{ .null_value = {} },
        .max => if (state.has_max) state.max_value else .{ .null_value = {} },
    };
}

const JoinDescriptor = struct {
    left_key_index: u16,
    right_key_index: u16,
};

fn executeInnerJoinBounded(
    result: *QueryResult,
    left_rows: []const ResultRow,
    right_rows: []const ResultRow,
    join: JoinDescriptor,
    caps: *const capacity_mod.OperatorCapacities,
) bool {
    if (left_rows.len > caps.join_build_rows) {
        setError(result, "join build row capacity exceeded");
        return false;
    }
    const state_bytes = left_rows.len * (@sizeOf(Value) + @sizeOf(u16));
    if (state_bytes > caps.join_state_bytes) {
        setError(result, "join state capacity exceeded");
        return false;
    }
    if (left_rows.len > 0 and
        join.left_key_index >= left_rows[0].column_count)
    {
        setError(result, "join key out of bounds");
        return false;
    }
    if (right_rows.len > 0 and
        join.right_key_index >= right_rows[0].column_count)
    {
        setError(result, "join key out of bounds");
        return false;
    }

    result.row_count = 0;
    for (left_rows) |left_row| {
        const left_key = left_row.values[join.left_key_index];
        for (right_rows) |right_row| {
            if (compareValues(left_key, right_row.values[join.right_key_index]) !=
                .eq) continue;
            const total_columns = @as(usize, left_row.column_count) +
                @as(usize, right_row.column_count);
            if (total_columns > scan_mod.max_columns) {
                setError(result, "join column capacity exceeded");
                return false;
            }
            if (@as(usize, result.row_count) >= caps.join_output_rows) {
                setError(result, "join output row capacity exceeded");
                return false;
            }

            var out = ResultRow.init();
            out.column_count = @intCast(total_columns);
            out.row_id = left_row.row_id;
            @memcpy(
                out.values[0..left_row.column_count],
                left_row.values[0..left_row.column_count],
            );
            @memcpy(
                out.values[left_row.column_count..out.column_count],
                right_row.values[0..right_row.column_count],
            );
            result.rows[result.row_count] = out;
            result.row_count += 1;
        }
    }
    return true;
}

/// Execute a mutation pipeline (insert, update, or delete).
fn executeMutation(
    ctx: *const ExecContext,
    result: *QueryResult,
    model_id: ModelId,
    ops: *const [max_operators]OpDescriptor,
    op_count: u16,
    mut_idx: u16,
) void {
    const mut_op = ops[mut_idx];

    switch (mut_op.kind) {
        .insert_op => {
            const node = ctx.ast.getNode(mut_op.node);
            const row_id = mutation_mod.executeInsert(
                ctx.catalog,
                ctx.pool,
                ctx.wal,
                ctx.tx_id,
                model_id,
                ctx.ast,
                ctx.tokens,
                ctx.source,
                node.data.unary,
            ) catch {
                setError(result, "insert failed");
                return;
            };
            result.stats.rows_inserted = 1;
            _ = row_id;
        },
        .update_op => {
            const predicate = findPredicate(ctx.ast, ops, op_count);
            const node = ctx.ast.getNode(mut_op.node);
            const count = mutation_mod.executeUpdate(
                ctx.catalog,
                ctx.pool,
                ctx.wal,
                ctx.undo_log,
                ctx.tx_id,
                ctx.snapshot,
                ctx.tx_manager,
                model_id,
                ctx.ast,
                ctx.tokens,
                ctx.source,
                predicate,
                node.data.unary,
                ctx.allocator,
            ) catch {
                setError(result, "update failed");
                return;
            };
            result.stats.rows_updated = count;
        },
        .delete_op => {
            const predicate = findPredicate(ctx.ast, ops, op_count);
            const count = mutation_mod.executeDelete(
                ctx.catalog,
                ctx.pool,
                ctx.wal,
                ctx.undo_log,
                ctx.tx_id,
                ctx.snapshot,
                ctx.tx_manager,
                model_id,
                ctx.ast,
                ctx.tokens,
                ctx.source,
                predicate,
                ctx.allocator,
            ) catch {
                setError(result, "delete failed");
                return;
            };
            result.stats.rows_deleted = count;
        },
        else => {
            setError(result, "unexpected mutation type");
        },
    }
}

/// Find the pipeline node from the AST root.
fn findPipeline(tree: *const Ast) ?NodeIndex {
    if (tree.root == null_node) return null;
    const root = tree.getNode(tree.root);
    if (root.tag != .root) return null;
    const first_stmt = root.data.unary;
    if (first_stmt == null_node) return null;
    const stmt = tree.getNode(first_stmt);
    if (stmt.tag == .pipeline) return first_stmt;
    if (stmt.tag == .let_binding) return null;
    return null;
}

/// Build a flat operator array from the linked list.
fn buildOperatorList(
    tree: *const Ast,
    first_op: NodeIndex,
    ops: *[max_operators]OpDescriptor,
    count: *u16,
) void {
    var current = first_op;
    while (current != null_node and count.* < max_operators) {
        const node = tree.getNode(current);
        const kind: ?OpKind = switch (node.tag) {
            .op_where => .where_filter,
            .op_group => .group_op,
            .op_limit => .limit_op,
            .op_offset => .offset_op,
            .op_insert => .insert_op,
            .op_update => .update_op,
            .op_delete => .delete_op,
            .op_sort => .sort_op,
            .op_inspect => .inspect_op,
            else => null,
        };
        if (kind) |k| {
            ops[count.*] = .{ .kind = k, .node = current };
            count.* += 1;
        }
        current = node.next;
    }
    std.debug.assert(count.* <= max_operators);
}

/// Find the first mutation operator in the list.
fn findMutationOp(
    ops: *const [max_operators]OpDescriptor,
    count: u16,
) ?u16 {
    for (0..count) |i| {
        switch (ops[i].kind) {
            .insert_op, .update_op, .delete_op => return @intCast(i),
            else => {},
        }
    }
    return null;
}

/// Find the where predicate expression from the operator list.
fn findPredicate(
    tree: *const Ast,
    ops: *const [max_operators]OpDescriptor,
    count: u16,
) NodeIndex {
    for (0..count) |i| {
        if (ops[i].kind == .where_filter) {
            const node = tree.getNode(ops[i].node);
            return node.data.unary;
        }
    }
    return null_node;
}

fn setError(result: *QueryResult, msg: []const u8) void {
    result.has_error = true;
    const copy_len = @min(msg.len, result.error_message.len);
    @memcpy(result.error_message[0..copy_len], msg[0..copy_len]);
}

// --- Tests ---

const testing = std.testing;
const disk_mod = @import("../simulator/disk.zig");
const parser_mod = @import("../parser/parser.zig");
const heap_mod = @import("../storage/heap.zig");

const ExecTestEnv = struct {
    disk: disk_mod.SimulatedDisk,
    pool: BufferPool,
    wal: Wal,
    tm: TxManager,
    undo_log: UndoLog,
    catalog: Catalog,
    model_id: ModelId,

    /// Initialize in-place so that disk.storage() captures a stable pointer.
    fn init(self: *ExecTestEnv) !void {
        self.disk = disk_mod.SimulatedDisk.init(testing.allocator);
        self.pool = try BufferPool.init(
            testing.allocator,
            self.disk.storage(),
            16,
        );
        self.wal = Wal.init(testing.allocator, self.disk.storage());
        self.tm = TxManager.init(testing.allocator);
        self.undo_log = try UndoLog.init(testing.allocator, 1024, 64 * 1024);

        self.catalog = Catalog{};
        self.model_id = try self.catalog.addModel("User");
        _ = try self.catalog.addColumn(
            self.model_id,
            "id",
            .bigint,
            false,
        );
        _ = try self.catalog.addColumn(
            self.model_id,
            "name",
            .string,
            true,
        );
        _ = try self.catalog.addColumn(
            self.model_id,
            "active",
            .boolean,
            true,
        );
        self.catalog.models[self.model_id].heap_first_page_id = 100;

        const page = try self.pool.pin(100);
        heap_mod.HeapPage.init(page);
        self.pool.unpin(100, true);
        self.catalog.models[self.model_id].total_pages = 1;
    }

    fn deinit(self: *ExecTestEnv) void {
        self.undo_log.deinit();
        self.tm.deinit();
        self.wal.deinit();
        self.pool.deinit();
        self.disk.deinit();
    }

    fn makeCtx(
        self: *ExecTestEnv,
        tx: TxId,
        snap: *const Snapshot,
        ast: *const Ast,
        tokens: *const TokenizeResult,
        source: []const u8,
    ) ExecContext {
        return .{
            .catalog = &self.catalog,
            .pool = &self.pool,
            .wal = &self.wal,
            .tx_manager = &self.tm,
            .undo_log = &self.undo_log,
            .tx_id = tx,
            .snapshot = snap,
            .ast = ast,
            .tokens = tokens,
            .source = source,
            .allocator = testing.allocator,
        };
    }
};

fn makeJoinLeftRow(id: i64, name: []const u8) ResultRow {
    var row = ResultRow.init();
    row.column_count = 2;
    row.values[0] = .{ .bigint = id };
    row.values[1] = .{ .string = name };
    return row;
}

fn makeJoinRightRow(id: i64, active: bool) ResultRow {
    var row = ResultRow.init();
    row.column_count = 2;
    row.values[0] = .{ .bigint = id };
    row.values[1] = .{ .boolean = active };
    return row;
}

test "execute insert query" {
    var env: ExecTestEnv = undefined;
    try env.init();
    defer env.deinit();

    const tx = try env.tm.begin();
    var snap = try env.tm.snapshot(tx);
    defer snap.deinit();

    const source =
        "User |> insert(id = 1, name = \"Alice\", active = true)";
    const tokens = tokenizer_mod.tokenize(source);
    const parsed = parser_mod.parse(&tokens, source);
    std.debug.assert(!parsed.has_error);

    const ctx = env.makeCtx(tx, &snap, &parsed.ast, &tokens, source);
    var result = try execute(&ctx);
    defer result.deinit();

    try testing.expect(!result.has_error);
    try testing.expectEqual(@as(u32, 1), result.stats.rows_inserted);
}

test "execute scan query returns rows" {
    var env: ExecTestEnv = undefined;
    try env.init();
    defer env.deinit();

    const tx = try env.tm.begin();
    var snap = try env.tm.snapshot(tx);
    defer snap.deinit();

    const src1 =
        "User |> insert(id = 1, name = \"Alice\", active = true)";
    const tok1 = tokenizer_mod.tokenize(src1);
    const p1 = parser_mod.parse(&tok1, src1);
    var r1 = try execute(
        &env.makeCtx(tx, &snap, &p1.ast, &tok1, src1),
    );
    defer r1.deinit();

    const src2 = "User";
    const tok2 = tokenizer_mod.tokenize(src2);
    const p2 = parser_mod.parse(&tok2, src2);
    var result = try execute(
        &env.makeCtx(tx, &snap, &p2.ast, &tok2, src2),
    );
    defer result.deinit();

    try testing.expect(!result.has_error);
    try testing.expectEqual(@as(u16, 1), result.row_count);
    try testing.expectEqual(
        @as(i64, 1),
        result.rows[0].values[0].bigint,
    );
}

test "execute where filter" {
    var env: ExecTestEnv = undefined;
    try env.init();
    defer env.deinit();

    const tx = try env.tm.begin();
    var snap = try env.tm.snapshot(tx);
    defer snap.deinit();

    const src1 =
        "User |> insert(id = 1, name = \"Alice\", active = true)";
    const tok1 = tokenizer_mod.tokenize(src1);
    const p1 = parser_mod.parse(&tok1, src1);
    var r1 = try execute(
        &env.makeCtx(tx, &snap, &p1.ast, &tok1, src1),
    );
    defer r1.deinit();

    const src2 =
        "User |> insert(id = 2, name = \"Bob\", active = false)";
    const tok2 = tokenizer_mod.tokenize(src2);
    const p2 = parser_mod.parse(&tok2, src2);
    var r2 = try execute(
        &env.makeCtx(tx, &snap, &p2.ast, &tok2, src2),
    );
    defer r2.deinit();

    const src3 = "User |> where(active = true)";
    const tok3 = tokenizer_mod.tokenize(src3);
    const p3 = parser_mod.parse(&tok3, src3);
    var result = try execute(
        &env.makeCtx(tx, &snap, &p3.ast, &tok3, src3),
    );
    defer result.deinit();

    try testing.expect(!result.has_error);
    try testing.expectEqual(@as(u16, 1), result.row_count);
    try testing.expectEqual(
        @as(i64, 1),
        result.rows[0].values[0].bigint,
    );
}

test "execute limit" {
    var env: ExecTestEnv = undefined;
    try env.init();
    defer env.deinit();

    const tx = try env.tm.begin();
    var snap = try env.tm.snapshot(tx);
    defer snap.deinit();

    const inserts = [_][]const u8{
        "User |> insert(id = 1, name = \"A\", active = true)",
        "User |> insert(id = 2, name = \"B\", active = true)",
        "User |> insert(id = 3, name = \"C\", active = true)",
    };
    for (inserts) |src| {
        const tok = tokenizer_mod.tokenize(src);
        const p = parser_mod.parse(&tok, src);
        var r = try execute(
            &env.makeCtx(tx, &snap, &p.ast, &tok, src),
        );
        defer r.deinit();
    }

    const src = "User |> limit(2)";
    const tok = tokenizer_mod.tokenize(src);
    const p = parser_mod.parse(&tok, src);
    var result = try execute(
        &env.makeCtx(tx, &snap, &p.ast, &tok, src),
    );
    defer result.deinit();

    try testing.expect(!result.has_error);
    try testing.expectEqual(@as(u16, 2), result.row_count);
}

test "execute offset" {
    var env: ExecTestEnv = undefined;
    try env.init();
    defer env.deinit();

    const tx = try env.tm.begin();
    var snap = try env.tm.snapshot(tx);
    defer snap.deinit();

    const inserts = [_][]const u8{
        "User |> insert(id = 1, name = \"A\", active = true)",
        "User |> insert(id = 2, name = \"B\", active = true)",
        "User |> insert(id = 3, name = \"C\", active = true)",
    };
    for (inserts) |src| {
        const tok = tokenizer_mod.tokenize(src);
        const p = parser_mod.parse(&tok, src);
        var r = try execute(
            &env.makeCtx(tx, &snap, &p.ast, &tok, src),
        );
        defer r.deinit();
    }

    const src = "User |> offset(1) |> limit(1)";
    const tok = tokenizer_mod.tokenize(src);
    const p = parser_mod.parse(&tok, src);
    var result = try execute(
        &env.makeCtx(tx, &snap, &p.ast, &tok, src),
    );
    defer result.deinit();

    try testing.expect(!result.has_error);
    try testing.expectEqual(@as(u16, 1), result.row_count);
    try testing.expectEqual(
        @as(i64, 2),
        result.rows[0].values[0].bigint,
    );
}

test "execute with unknown model returns error" {
    var env: ExecTestEnv = undefined;
    try env.init();
    defer env.deinit();

    const tx = try env.tm.begin();
    var snap = try env.tm.snapshot(tx);
    defer snap.deinit();

    const src = "Unknown |> limit(10)";
    const tok = tokenizer_mod.tokenize(src);
    const p = parser_mod.parse(&tok, src);
    var result = try execute(
        &env.makeCtx(tx, &snap, &p.ast, &tok, src),
    );
    defer result.deinit();

    try testing.expect(result.has_error);
    try testing.expect(result.getError() != null);
}

test "execute nested relation join through selection set" {
    var env: ExecTestEnv = undefined;
    try env.init();
    defer env.deinit();

    const post_model = try env.catalog.addModel("Post");
    _ = try env.catalog.addColumn(post_model, "id", .bigint, false);
    _ = try env.catalog.addColumn(post_model, "user_id", .bigint, false);
    env.catalog.models[post_model].heap_first_page_id = 120;
    env.catalog.models[post_model].total_pages = 1;
    _ = try env.catalog.addAssociation(
        env.model_id,
        "posts",
        AssociationKind.has_many,
        "Post",
    );
    try env.catalog.resolveAssociations();

    const post_page = try env.pool.pin(120);
    heap_mod.HeapPage.init(post_page);
    env.pool.unpin(120, true);

    const tx = try env.tm.begin();
    var snap = try env.tm.snapshot(tx);
    defer snap.deinit();

    const inserts = [_][]const u8{
        "User |> insert(id = 1, name = \"Alice\", active = true)",
        "User |> insert(id = 2, name = \"Bob\", active = true)",
        "Post |> insert(id = 20, user_id = 1)",
        "Post |> insert(id = 10, user_id = 1)",
        "Post |> insert(id = 15, user_id = 2)",
    };
    for (inserts) |src| {
        const tok = tokenizer_mod.tokenize(src);
        const p = parser_mod.parse(&tok, src);
        try testing.expect(!p.has_error);
        var r = try execute(&env.makeCtx(tx, &snap, &p.ast, &tok, src));
        defer r.deinit();
        try testing.expect(!r.has_error);
    }

    const src =
        "User |> sort(id asc) { id posts |> sort(id asc) { id } }";
    const tok = tokenizer_mod.tokenize(src);
    const p = parser_mod.parse(&tok, src);
    try testing.expect(!p.has_error);
    var result = try execute(&env.makeCtx(tx, &snap, &p.ast, &tok, src));
    defer result.deinit();

    try testing.expect(!result.has_error);
    try testing.expectEqual(@as(u16, 3), result.row_count);
    try testing.expectEqual(@as(i64, 1), result.rows[0].values[0].bigint);
    try testing.expectEqual(@as(i64, 10), result.rows[0].values[3].bigint);
    try testing.expectEqual(@as(i64, 1), result.rows[1].values[0].bigint);
    try testing.expectEqual(@as(i64, 20), result.rows[1].values[3].bigint);
    try testing.expectEqual(@as(i64, 2), result.rows[2].values[0].bigint);
    try testing.expectEqual(@as(i64, 15), result.rows[2].values[3].bigint);
}

test "execute nested relation fails closed when association is missing" {
    var env: ExecTestEnv = undefined;
    try env.init();
    defer env.deinit();

    const tx = try env.tm.begin();
    var snap = try env.tm.snapshot(tx);
    defer snap.deinit();

    const src = "User { missing { id } }";
    const tok = tokenizer_mod.tokenize(src);
    const p = parser_mod.parse(&tok, src);
    var result = try execute(&env.makeCtx(tx, &snap, &p.ast, &tok, src));
    defer result.deinit();

    try testing.expect(result.has_error);
    const msg = result.getError().?;
    try testing.expect(std.mem.indexOf(
        u8,
        msg,
        "nested relation association not found",
    ) != null);
}

test "execute delete via pipeline" {
    var env: ExecTestEnv = undefined;
    try env.init();
    defer env.deinit();

    const tx = try env.tm.begin();
    var snap = try env.tm.snapshot(tx);
    defer snap.deinit();

    const src1 =
        "User |> insert(id = 1, name = \"Alice\", active = true)";
    const tok1 = tokenizer_mod.tokenize(src1);
    const p1 = parser_mod.parse(&tok1, src1);
    var r1 = try execute(
        &env.makeCtx(tx, &snap, &p1.ast, &tok1, src1),
    );
    defer r1.deinit();

    const src2 = "User |> where(id = 1) |> delete";
    const tok2 = tokenizer_mod.tokenize(src2);
    const p2 = parser_mod.parse(&tok2, src2);
    var result = try execute(
        &env.makeCtx(tx, &snap, &p2.ast, &tok2, src2),
    );
    defer result.deinit();

    try testing.expect(!result.has_error);
    try testing.expectEqual(@as(u32, 1), result.stats.rows_deleted);
}

test "execute sort orders rows by key and direction" {
    var env: ExecTestEnv = undefined;
    try env.init();
    defer env.deinit();

    const tx = try env.tm.begin();
    var snap = try env.tm.snapshot(tx);
    defer snap.deinit();

    const inserts = [_][]const u8{
        "User |> insert(id = 1, name = \"Bob\", active = true)",
        "User |> insert(id = 2, name = \"Alice\", active = true)",
        "User |> insert(id = 3, name = \"Carol\", active = false)",
    };
    for (inserts) |src| {
        const tok = tokenizer_mod.tokenize(src);
        const p = parser_mod.parse(&tok, src);
        var r = try execute(&env.makeCtx(tx, &snap, &p.ast, &tok, src));
        defer r.deinit();
    }

    const asc_src = "User |> sort(name asc)";
    const asc_tok = tokenizer_mod.tokenize(asc_src);
    const asc_parsed = parser_mod.parse(&asc_tok, asc_src);
    var asc_result = try execute(
        &env.makeCtx(tx, &snap, &asc_parsed.ast, &asc_tok, asc_src),
    );
    defer asc_result.deinit();
    try testing.expect(!asc_result.has_error);
    try testing.expectEqual(@as(i64, 2), asc_result.rows[0].values[0].bigint);
    try testing.expectEqual(@as(i64, 1), asc_result.rows[1].values[0].bigint);
    try testing.expectEqual(@as(i64, 3), asc_result.rows[2].values[0].bigint);

    const desc_src = "User |> sort(name desc)";
    const desc_tok = tokenizer_mod.tokenize(desc_src);
    const desc_parsed = parser_mod.parse(&desc_tok, desc_src);
    var desc_result = try execute(
        &env.makeCtx(tx, &snap, &desc_parsed.ast, &desc_tok, desc_src),
    );
    defer desc_result.deinit();
    try testing.expect(!desc_result.has_error);
    try testing.expectEqual(@as(i64, 3), desc_result.rows[0].values[0].bigint);
    try testing.expectEqual(@as(i64, 1), desc_result.rows[1].values[0].bigint);
    try testing.expectEqual(@as(i64, 2), desc_result.rows[2].values[0].bigint);
}

test "execute sort supports expression keys" {
    var env: ExecTestEnv = undefined;
    try env.init();
    defer env.deinit();

    const tx = try env.tm.begin();
    var snap = try env.tm.snapshot(tx);
    defer snap.deinit();

    const inserts = [_][]const u8{
        "User |> insert(id = 1, name = \"xx\", active = true)",
        "User |> insert(id = 2, name = \"a\", active = true)",
        "User |> insert(id = 3, name = \"bbbb\", active = true)",
    };
    for (inserts) |src| {
        const tok = tokenizer_mod.tokenize(src);
        const p = parser_mod.parse(&tok, src);
        var r = try execute(&env.makeCtx(tx, &snap, &p.ast, &tok, src));
        defer r.deinit();
    }

    const src = "User |> sort(length(name) desc)";
    const tok = tokenizer_mod.tokenize(src);
    const p = parser_mod.parse(&tok, src);
    var result = try execute(&env.makeCtx(tx, &snap, &p.ast, &tok, src));
    defer result.deinit();

    try testing.expect(!result.has_error);
    try testing.expectEqual(@as(i64, 3), result.rows[0].values[0].bigint);
    try testing.expectEqual(@as(i64, 1), result.rows[1].values[0].bigint);
    try testing.expectEqual(@as(i64, 2), result.rows[2].values[0].bigint);
}

test "execute sort enforces key capacity contract" {
    var env: ExecTestEnv = undefined;
    try env.init();
    defer env.deinit();

    const tx = try env.tm.begin();
    var snap = try env.tm.snapshot(tx);
    defer snap.deinit();

    const src = "User |> sort(id asc, name asc, active asc, id asc, name asc, active asc, id asc, name asc, active asc)";
    const tok = tokenizer_mod.tokenize(src);
    const p = parser_mod.parse(&tok, src);
    var result = try execute(&env.makeCtx(tx, &snap, &p.ast, &tok, src));
    defer result.deinit();

    try testing.expect(result.has_error);
    const msg = result.getError().?;
    try testing.expect(std.mem.indexOf(u8, msg, "sort capacity exceeded") != null);
}

test "execute group collapses rows by key" {
    var env: ExecTestEnv = undefined;
    try env.init();
    defer env.deinit();

    const tx = try env.tm.begin();
    var snap = try env.tm.snapshot(tx);
    defer snap.deinit();

    const inserts = [_][]const u8{
        "User |> insert(id = 1, name = \"A\", active = true)",
        "User |> insert(id = 2, name = \"B\", active = false)",
        "User |> insert(id = 3, name = \"C\", active = true)",
    };
    for (inserts) |src| {
        const tok = tokenizer_mod.tokenize(src);
        const p = parser_mod.parse(&tok, src);
        var r = try execute(&env.makeCtx(tx, &snap, &p.ast, &tok, src));
        defer r.deinit();
    }

    const src = "User |> group(active) |> sort(active asc)";
    const tok = tokenizer_mod.tokenize(src);
    const p = parser_mod.parse(&tok, src);
    var result = try execute(&env.makeCtx(tx, &snap, &p.ast, &tok, src));
    defer result.deinit();

    try testing.expect(!result.has_error);
    try testing.expectEqual(@as(u16, 2), result.row_count);
    try testing.expectEqual(false, result.rows[0].values[2].boolean);
    try testing.expectEqual(true, result.rows[1].values[2].boolean);
}

test "execute group enforces key capacity contract" {
    var env: ExecTestEnv = undefined;
    try env.init();
    defer env.deinit();

    const tx = try env.tm.begin();
    var snap = try env.tm.snapshot(tx);
    defer snap.deinit();

    const src = "User |> group(id, name, active, id, name, active, id, name, active)";
    const tok = tokenizer_mod.tokenize(src);
    const p = parser_mod.parse(&tok, src);
    var result = try execute(&env.makeCtx(tx, &snap, &p.ast, &tok, src));
    defer result.deinit();

    try testing.expect(result.has_error);
    const msg = result.getError().?;
    try testing.expect(std.mem.indexOf(u8, msg, "group key capacity exceeded") != null);
}

test "execute group supports sort by count star" {
    var env: ExecTestEnv = undefined;
    try env.init();
    defer env.deinit();

    const tx = try env.tm.begin();
    var snap = try env.tm.snapshot(tx);
    defer snap.deinit();

    const inserts = [_][]const u8{
        "User |> insert(id = 1, name = \"A\", active = true)",
        "User |> insert(id = 2, name = \"B\", active = true)",
        "User |> insert(id = 3, name = \"C\", active = false)",
    };
    for (inserts) |src| {
        const tok = tokenizer_mod.tokenize(src);
        const p = parser_mod.parse(&tok, src);
        var r = try execute(&env.makeCtx(tx, &snap, &p.ast, &tok, src));
        defer r.deinit();
    }

    const src = "User |> group(active) |> sort(count(*) desc)";
    const tok = tokenizer_mod.tokenize(src);
    const p = parser_mod.parse(&tok, src);
    var result = try execute(&env.makeCtx(tx, &snap, &p.ast, &tok, src));
    defer result.deinit();

    try testing.expect(!result.has_error);
    try testing.expectEqual(@as(u16, 2), result.row_count);
    try testing.expectEqual(true, result.rows[0].values[2].boolean);
    try testing.expectEqual(false, result.rows[1].values[2].boolean);
}

test "execute group supports where on count star" {
    var env: ExecTestEnv = undefined;
    try env.init();
    defer env.deinit();

    const tx = try env.tm.begin();
    var snap = try env.tm.snapshot(tx);
    defer snap.deinit();

    const inserts = [_][]const u8{
        "User |> insert(id = 1, name = \"A\", active = true)",
        "User |> insert(id = 2, name = \"B\", active = true)",
        "User |> insert(id = 3, name = \"C\", active = false)",
    };
    for (inserts) |src| {
        const tok = tokenizer_mod.tokenize(src);
        const p = parser_mod.parse(&tok, src);
        var r = try execute(&env.makeCtx(tx, &snap, &p.ast, &tok, src));
        defer r.deinit();
    }

    const src = "User |> group(active) |> where(count(*) > 1)";
    const tok = tokenizer_mod.tokenize(src);
    const p = parser_mod.parse(&tok, src);
    var result = try execute(&env.makeCtx(tx, &snap, &p.ast, &tok, src));
    defer result.deinit();

    try testing.expect(!result.has_error);
    try testing.expectEqual(@as(u16, 1), result.row_count);
    try testing.expectEqual(true, result.rows[0].values[2].boolean);
}

test "execute group supports sum avg min max aggregates" {
    var env: ExecTestEnv = undefined;
    try env.init();
    defer env.deinit();

    const tx = try env.tm.begin();
    var snap = try env.tm.snapshot(tx);
    defer snap.deinit();

    const inserts = [_][]const u8{
        "User |> insert(id = 1, name = \"A\", active = true)",
        "User |> insert(id = 2, name = \"B\", active = true)",
        "User |> insert(id = 10, name = \"C\", active = false)",
    };
    for (inserts) |src| {
        const tok = tokenizer_mod.tokenize(src);
        const p = parser_mod.parse(&tok, src);
        var r = try execute(&env.makeCtx(tx, &snap, &p.ast, &tok, src));
        defer r.deinit();
    }

    const src =
        "User |> group(active) |> where(max(id) > 1 and min(id) >= 1) |> sort(sum(id) asc, avg(id) asc)";
    const tok = tokenizer_mod.tokenize(src);
    const p = parser_mod.parse(&tok, src);
    var result = try execute(&env.makeCtx(tx, &snap, &p.ast, &tok, src));
    defer result.deinit();

    try testing.expect(!result.has_error);
    try testing.expectEqual(@as(u16, 2), result.row_count);
    try testing.expectEqual(true, result.rows[0].values[2].boolean);
    try testing.expectEqual(false, result.rows[1].values[2].boolean);
}

test "execute group sum enforces numeric input types" {
    var env: ExecTestEnv = undefined;
    try env.init();
    defer env.deinit();

    const tx = try env.tm.begin();
    var snap = try env.tm.snapshot(tx);
    defer snap.deinit();

    const inserts = [_][]const u8{
        "User |> insert(id = 1, name = \"A\", active = true)",
        "User |> insert(id = 2, name = \"B\", active = false)",
    };
    for (inserts) |src| {
        const tok = tokenizer_mod.tokenize(src);
        const p = parser_mod.parse(&tok, src);
        var r = try execute(&env.makeCtx(tx, &snap, &p.ast, &tok, src));
        defer r.deinit();
    }

    const src = "User |> group(active) |> where(sum(name) > 0)";
    const tok = tokenizer_mod.tokenize(src);
    const p = parser_mod.parse(&tok, src);
    var result = try execute(&env.makeCtx(tx, &snap, &p.ast, &tok, src));
    defer result.deinit();

    try testing.expect(result.has_error);
    const msg = result.getError().?;
    try testing.expect(std.mem.indexOf(u8, msg, "aggregate evaluation failed") != null);
}

test "execute group enforces aggregate expression capacity contract" {
    var env: ExecTestEnv = undefined;
    try env.init();
    defer env.deinit();

    const tx = try env.tm.begin();
    var snap = try env.tm.snapshot(tx);
    defer snap.deinit();

    const src =
        "User |> group(active) |> where(sum(id) > 0 and avg(id) > 0 and min(id) > 0 and max(id) > 0 and sum(id + 1) > 0)";
    const tok = tokenizer_mod.tokenize(src);
    const p = parser_mod.parse(&tok, src);
    var result = try execute(&env.makeCtx(tx, &snap, &p.ast, &tok, src));
    defer result.deinit();

    try testing.expect(result.has_error);
    const msg = result.getError().?;
    try testing.expect(std.mem.indexOf(u8, msg, "aggregate expression capacity exceeded") != null);
}

test "bounded inner join preserves deterministic left-major ordering" {
    const left = [_]ResultRow{
        makeJoinLeftRow(1, "A"),
        makeJoinLeftRow(2, "B"),
        makeJoinLeftRow(1, "C"),
    };
    const right = [_]ResultRow{
        makeJoinRightRow(1, true),
        makeJoinRightRow(1, false),
        makeJoinRightRow(2, true),
    };

    var result = try QueryResult.init(testing.allocator);
    defer result.deinit();
    const caps = capacity_mod.OperatorCapacities.defaults();
    const ok = executeInnerJoinBounded(
        &result,
        left[0..],
        right[0..],
        .{ .left_key_index = 0, .right_key_index = 0 },
        &caps,
    );

    try testing.expect(ok);
    try testing.expect(!result.has_error);
    try testing.expectEqual(@as(u16, 5), result.row_count);
    try testing.expectEqual(@as(i64, 1), result.rows[0].values[0].bigint);
    try testing.expectEqualSlices(u8, "A", result.rows[0].values[1].string);
    try testing.expectEqual(true, result.rows[0].values[3].boolean);
    try testing.expectEqual(@as(i64, 1), result.rows[1].values[0].bigint);
    try testing.expectEqualSlices(u8, "A", result.rows[1].values[1].string);
    try testing.expectEqual(false, result.rows[1].values[3].boolean);
    try testing.expectEqual(@as(i64, 2), result.rows[2].values[0].bigint);
    try testing.expectEqualSlices(u8, "B", result.rows[2].values[1].string);
    try testing.expectEqual(true, result.rows[2].values[3].boolean);
    try testing.expectEqual(@as(i64, 1), result.rows[3].values[0].bigint);
    try testing.expectEqualSlices(u8, "C", result.rows[3].values[1].string);
    try testing.expectEqual(true, result.rows[3].values[3].boolean);
    try testing.expectEqual(@as(i64, 1), result.rows[4].values[0].bigint);
    try testing.expectEqualSlices(u8, "C", result.rows[4].values[1].string);
    try testing.expectEqual(false, result.rows[4].values[3].boolean);
}

test "bounded inner join enforces build row capacity contract" {
    const left = [_]ResultRow{
        makeJoinLeftRow(1, "A"),
        makeJoinLeftRow(2, "B"),
    };
    const right = [_]ResultRow{
        makeJoinRightRow(1, true),
    };

    var result = try QueryResult.init(testing.allocator);
    defer result.deinit();
    var caps = capacity_mod.OperatorCapacities.defaults();
    caps.join_build_rows = 1;
    const ok = executeInnerJoinBounded(
        &result,
        left[0..],
        right[0..],
        .{ .left_key_index = 0, .right_key_index = 0 },
        &caps,
    );

    try testing.expect(!ok);
    try testing.expect(result.has_error);
    const msg = result.getError().?;
    try testing.expect(std.mem.indexOf(u8, msg, "join build row capacity exceeded") != null);
}

test "bounded inner join enforces output row capacity contract" {
    const left = [_]ResultRow{
        makeJoinLeftRow(1, "A"),
        makeJoinLeftRow(1, "B"),
    };
    const right = [_]ResultRow{
        makeJoinRightRow(1, true),
        makeJoinRightRow(1, false),
    };

    var result = try QueryResult.init(testing.allocator);
    defer result.deinit();
    var caps = capacity_mod.OperatorCapacities.defaults();
    caps.join_output_rows = 3;
    const ok = executeInnerJoinBounded(
        &result,
        left[0..],
        right[0..],
        .{ .left_key_index = 0, .right_key_index = 0 },
        &caps,
    );

    try testing.expect(!ok);
    try testing.expect(result.has_error);
    const msg = result.getError().?;
    try testing.expect(std.mem.indexOf(u8, msg, "join output row capacity exceeded") != null);
}

test "bounded inner join enforces state byte capacity contract" {
    const left = [_]ResultRow{
        makeJoinLeftRow(1, "A"),
        makeJoinLeftRow(2, "B"),
    };
    const right = [_]ResultRow{
        makeJoinRightRow(1, true),
    };

    var result = try QueryResult.init(testing.allocator);
    defer result.deinit();
    var caps = capacity_mod.OperatorCapacities.defaults();
    caps.join_state_bytes = @sizeOf(Value);
    const ok = executeInnerJoinBounded(
        &result,
        left[0..],
        right[0..],
        .{ .left_key_index = 0, .right_key_index = 0 },
        &caps,
    );

    try testing.expect(!ok);
    try testing.expect(result.has_error);
    const msg = result.getError().?;
    try testing.expect(std.mem.indexOf(u8, msg, "join state capacity exceeded") != null);
}
