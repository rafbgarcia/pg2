//! SELECT column projection and nested relation field reshaping.
//!
//! Applies column selection and computed expression projection to
//! result rows after filtering, grouping, and sorting are complete.
const std = @import("std");
const ast_mod = @import("../parser/ast.zig");
const row_mod = @import("../storage/row.zig");
const scan_mod = @import("scan.zig");
const filter_mod = @import("filter.zig");
const catalog_mod = @import("../catalog/catalog.zig");

const NodeIndex = ast_mod.NodeIndex;
const null_node = ast_mod.null_node;
const Value = row_mod.Value;
const RowSchema = row_mod.RowSchema;
const Catalog = catalog_mod.Catalog;
const ModelId = catalog_mod.ModelId;
const null_model = catalog_mod.null_model;
const ExecContext = @import("executor.zig").ExecContext;
const QueryResult = @import("executor.zig").QueryResult;
const evalContextForExec = @import("executor.zig").evalContextForExec;
const setError = @import("executor.zig").setError;
const max_parallel_projection_workers: usize = 8;
const max_parallel_worker_cap: usize = 8;
const parallel_projection_worker_arena_bytes: usize = filter_mod.max_string_result_bytes * 2;

pub const ProjectionKind = enum {
    column,
    expression,
};

pub const ProjectionDescriptor = struct {
    kind: ProjectionKind,
    column_index: u16 = 0,
    expr_node: NodeIndex = null_node,
};

pub fn applyFlatColumnProjection(
    ctx: *const ExecContext,
    result: *QueryResult,
    pipeline_node: NodeIndex,
    model_id: ModelId,
    string_arena: *scan_mod.StringArena,
    parallel_enabled: bool,
    parallel_min_rows_per_worker: u16,
) bool {
    const selection = getPipelineSelection(ctx.ast, pipeline_node) orelse
        return true;
    const source_model = &ctx.catalog.models[model_id];
    const source_schema = &source_model.row_schema;

    var projection_descriptors: [scan_mod.max_columns]ProjectionDescriptor = undefined;
    var projection_count: u16 = 0;
    var joined_column_offset: u16 = source_schema.column_count;

    var field = ctx.ast.getNode(selection).data.unary;
    while (field != null_node) {
        const node = ctx.ast.getNode(field);
        switch (node.tag) {
            .select_field => {
                const col_name = ctx.tokens.getText(node.data.token, ctx.source);
                const col_idx = source_schema.findColumn(col_name) orelse {
                    setError(result, "select column not found");
                    return false;
                };
                if (!appendProjectionDescriptor(
                    result,
                    &projection_descriptors,
                    &projection_count,
                    .{
                        .kind = .column,
                        .column_index = col_idx,
                    },
                )) return false;
            },
            .select_computed => {
                const expr_node = node.data.unary;
                if (expr_node == null_node) {
                    setError(result, "computed select expression missing");
                    return false;
                }
                if (!appendProjectionDescriptor(
                    result,
                    &projection_descriptors,
                    &projection_count,
                    .{
                        .kind = .expression,
                        .expr_node = expr_node,
                    },
                )) return false;
            },
            .select_nested => {
                const relation_name = ctx.tokens.getText(node.extra, ctx.source);
                const assoc_id = ctx.catalog.findAssociation(
                    model_id,
                    relation_name,
                ) orelse {
                    setError(result, "nested relation association not found");
                    return false;
                };
                const assoc = &source_model.associations[assoc_id];
                if (assoc.target_model_id == null_model) {
                    setError(result, "nested relation target unresolved");
                    return false;
                }
                const target_model = &ctx.catalog.models[assoc.target_model_id];
                const nested_selection = getNestedSelection(ctx.ast, field) orelse {
                    joined_column_offset += target_model.row_schema.column_count;
                    field = node.next;
                    continue;
                };

                var nested_field = ctx.ast.getNode(nested_selection).data.unary;
                while (nested_field != null_node) {
                    const nested_node = ctx.ast.getNode(nested_field);
                    switch (nested_node.tag) {
                        .select_field => {
                            const nested_col_name = ctx.tokens.getText(
                                nested_node.data.token,
                                ctx.source,
                            );
                            const nested_col_idx = target_model.row_schema.findColumn(
                                nested_col_name,
                            ) orelse {
                                setError(result, "select column not found");
                                return false;
                            };
                            if (!appendProjectionDescriptor(
                                result,
                                &projection_descriptors,
                                &projection_count,
                                .{
                                    .kind = .column,
                                    .column_index = joined_column_offset + nested_col_idx,
                                },
                            )) return false;
                        },
                        // Nested relation and computed-field projection shaping
                        // are not implemented in this pass.
                        .select_nested, .select_computed => return true,
                        else => return true,
                    }
                    nested_field = nested_node.next;
                }
                joined_column_offset += target_model.row_schema.column_count;
            },
            else => return true,
        }
        field = node.next;
    }

    if (projection_count == 0) return true;
    if (parallel_enabled and
        tryApplyFlatProjectionParallel(
            ctx,
            result,
            projection_descriptors[0..projection_count],
            projection_count,
            source_schema,
            parallel_min_rows_per_worker,
        ))
    {
        result.stats.plan.parallel_schedule_applied_tasks =
            result.stats.plan.parallel_schedule_task_count;
        return true;
    }

    var row_index: u16 = 0;
    while (row_index < result.row_count) : (row_index += 1) {
        var projected: [scan_mod.max_columns]Value = undefined;
        for (projection_descriptors[0..projection_count], 0..) |descriptor, out_idx| {
            switch (descriptor.kind) {
                .column => {
                    if (descriptor.column_index >= result.rows[row_index].column_count) {
                        setError(result, "projection column out of bounds");
                        return false;
                    }
                    projected[out_idx] = result.rows[row_index].values[descriptor.column_index];
                },
                .expression => {
                    var exec_eval = evalContextForExec(ctx, string_arena);
                    exec_eval.bind();
                    const value = filter_mod.evaluateExpressionFull(
                        ctx.ast,
                        ctx.tokens,
                        ctx.source,
                        descriptor.expr_node,
                        result.rows[row_index].values[0..result.rows[row_index].column_count],
                        source_schema,
                        null,
                        &exec_eval.eval_ctx,
                    ) catch {
                        setError(result, "select computed expression evaluation failed");
                        return false;
                    };
                    projected[out_idx] = value;
                },
            }
        }
        @memcpy(
            result.rows[row_index].values[0..projection_count],
            projected[0..projection_count],
        );
        result.rows[row_index].column_count = projection_count;
    }

    return true;
}

const ParallelProjectionError = enum(u8) {
    none = 0,
    out_of_bounds,
    eval_failed,
};

const ParallelProjectionWorker = struct {
    ctx: *const ExecContext,
    result: *QueryResult,
    descriptors: []const ProjectionDescriptor,
    projection_count: u16,
    source_schema: *const RowSchema,
    start_idx: u16,
    end_idx: u16,
    first_error_index: u16 = std.math.maxInt(u16),
    first_error_code: ParallelProjectionError = .none,

    fn run(self: *ParallelProjectionWorker) void {
        var arena_buf: [parallel_projection_worker_arena_bytes]u8 = undefined;
        var arena = scan_mod.StringArena.init(&arena_buf);
        var exec_eval = evalContextForExec(self.ctx, &arena);
        exec_eval.bind();

        var row_index = self.start_idx;
        while (row_index < self.end_idx) : (row_index += 1) {
            arena.reset();
            var projected: [scan_mod.max_columns]Value = undefined;
            for (self.descriptors, 0..) |descriptor, out_idx| {
                switch (descriptor.kind) {
                    .column => {
                        if (descriptor.column_index >= self.result.rows[row_index].column_count) {
                            self.first_error_index = row_index;
                            self.first_error_code = .out_of_bounds;
                            return;
                        }
                        projected[out_idx] = self.result.rows[row_index].values[descriptor.column_index];
                    },
                    .expression => {
                        const value = filter_mod.evaluateExpressionFull(
                            self.ctx.ast,
                            self.ctx.tokens,
                            self.ctx.source,
                            descriptor.expr_node,
                            self.result.rows[row_index].values[0..self.result.rows[row_index].column_count],
                            self.source_schema,
                            null,
                            &exec_eval.eval_ctx,
                        ) catch {
                            self.first_error_index = row_index;
                            self.first_error_code = .eval_failed;
                            return;
                        };
                        projected[out_idx] = value;
                    },
                }
            }
            @memcpy(
                self.result.rows[row_index].values[0..self.projection_count],
                projected[0..self.projection_count],
            );
            self.result.rows[row_index].column_count = self.projection_count;
        }
    }
};

fn tryApplyFlatProjectionParallel(
    ctx: *const ExecContext,
    result: *QueryResult,
    descriptors: []const ProjectionDescriptor,
    projection_count: u16,
    source_schema: *const RowSchema,
    parallel_min_rows_per_worker: u16,
) bool {
    const row_count_usize: usize = result.row_count;
    const min_rows_per_worker = @as(usize, @max(@as(u16, 1), parallel_min_rows_per_worker));
    if (row_count_usize < min_rows_per_worker * 2) return false;

    const configured_cap = @max(
        @as(usize, 1),
        @min(
            @as(usize, result.stats.plan.parallel_worker_budget),
            max_parallel_worker_cap,
        ),
    );
    const stage_cap = @min(max_parallel_projection_workers, configured_cap);
    const max_workers = @min(stage_cap, row_count_usize);
    var worker_count = @min(
        max_workers,
        row_count_usize / min_rows_per_worker,
    );
    if (worker_count < 2) return false;
    if (worker_count > max_parallel_projection_workers) worker_count = max_parallel_projection_workers;

    var workers: [max_parallel_projection_workers]ParallelProjectionWorker = undefined;
    var threads: [max_parallel_projection_workers - 1]?std.Thread =
        [_]?std.Thread{null} ** (max_parallel_projection_workers - 1);
    var spawned: usize = 0;

    const base = row_count_usize / worker_count;
    const remainder = row_count_usize % worker_count;
    var start_idx: usize = 0;
    var worker_idx: usize = 0;
    while (worker_idx < worker_count) : (worker_idx += 1) {
        const span = base + if (worker_idx < remainder) @as(usize, 1) else @as(usize, 0);
        const end_idx = start_idx + span;
        workers[worker_idx] = .{
            .ctx = ctx,
            .result = result,
            .descriptors = descriptors,
            .projection_count = projection_count,
            .source_schema = source_schema,
            .start_idx = @intCast(start_idx),
            .end_idx = @intCast(end_idx),
        };
        start_idx = end_idx;
    }

    worker_idx = 1;
    while (worker_idx < worker_count) : (worker_idx += 1) {
        threads[spawned] = std.Thread.spawn(.{}, ParallelProjectionWorker.run, .{&workers[worker_idx]}) catch {
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

    var first_error_index: u16 = std.math.maxInt(u16);
    var first_error_code: ParallelProjectionError = .none;
    worker_idx = 0;
    while (worker_idx < worker_count) : (worker_idx += 1) {
        if (workers[worker_idx].first_error_code == .none) continue;
        if (workers[worker_idx].first_error_index < first_error_index) {
            first_error_index = workers[worker_idx].first_error_index;
            first_error_code = workers[worker_idx].first_error_code;
        }
    }

    switch (first_error_code) {
        .none => {},
        .out_of_bounds => {
            setError(result, "projection column out of bounds");
            return true;
        },
        .eval_failed => {
            setError(result, "select computed expression evaluation failed");
            return true;
        },
    }
    return true;
}

fn appendProjectionDescriptor(
    result: *QueryResult,
    projection_descriptors: *[scan_mod.max_columns]ProjectionDescriptor,
    projection_count: *u16,
    descriptor: ProjectionDescriptor,
) bool {
    if (projection_count.* >= scan_mod.max_columns) {
        setError(result, "projection column capacity exceeded");
        return false;
    }
    projection_descriptors.*[projection_count.*] = descriptor;
    projection_count.* += 1;
    return true;
}

pub fn getNestedSelection(tree: *const ast_mod.Ast, nested_node: NodeIndex) ?NodeIndex {
    const nested = tree.getNode(nested_node);
    if (nested.tag != .select_nested) return null;
    if (nested.data.unary == null_node) return null;
    const nested_pipeline = tree.getNode(nested.data.unary);
    if (nested_pipeline.tag != .pipeline) return null;
    const selection: NodeIndex = nested_pipeline.extra;
    if (selection >= tree.node_count) return null;
    if (tree.getNode(selection).tag != .selection_set) return null;
    return selection;
}

pub fn getPipelineSelection(
    tree: *const ast_mod.Ast,
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
