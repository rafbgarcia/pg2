//! Query result serialization: flat row output, inspect stats, and plan explanation.
const std = @import("std");
const session = @import("session.zig");
const tree_protocol = @import("tree_protocol.zig");
const exec_mod = @import("../executor/executor.zig");
const catalog_mod = @import("../catalog/catalog.zig");
const pool_mod = @import("pool.zig");
const tokenizer_mod = @import("../parser/tokenizer.zig");
const ast_mod = @import("../parser/ast.zig");

const Catalog = catalog_mod.Catalog;
const OverflowReclaimStatsSnapshot = catalog_mod.OverflowReclaimStatsSnapshot;
const PoolStats = pool_mod.PoolStats;
const Ast = ast_mod.Ast;
const serializeValue = session.serializeValue;
const buildTreeProjection = tree_protocol.buildTreeProjection;
const countProtocolRootRows = tree_protocol.countProtocolRootRows;
const serializeTreeProtocol = tree_protocol.serializeTreeProtocol;

pub fn serializeQueryResult(
    writer: anytype,
    result: *const exec_mod.QueryResult,
    catalog: *const Catalog,
    pool_stats: ?PoolStats,
    ast: *const Ast,
    tokens: *const tokenizer_mod.TokenizeResult,
    source: []const u8,
) error{ResponseTooLarge}!void {
    std.debug.assert(result.row_count <= result.rows.len);
    if (result.getError()) |message| {
        writer.print("ERR query: {s}\n", .{message}) catch
            return error.ResponseTooLarge;
        return;
    }

    const tree_projection = buildTreeProjection(ast, tokens, source, catalog);
    const returned_rows: u16 = if (tree_projection) |projection|
        countProtocolRootRows(result, &projection)
    else
        result.row_count;

    writer.print(
        "OK returned_rows={d} inserted_rows={d} updated_rows={d} deleted_rows={d}\n",
        .{
            returned_rows,
            result.stats.rows_inserted,
            result.stats.rows_updated,
            result.stats.rows_deleted,
        },
    ) catch return error.ResponseTooLarge;

    if (tree_projection) |projection| {
        try serializeTreeProtocol(writer, result, &projection, catalog, tokens, source);
        if (pool_stats) |stats| {
            try serializeInspectStats(
                writer,
                &result.stats,
                stats,
                catalog.snapshotOverflowReclaimStats(),
            );
        }
        return;
    }

    var row_index: usize = 0;
    while (row_index < result.row_count) : (row_index += 1) {
        const row = result.rows[row_index];
        std.debug.assert(row.column_count <= row.values.len);
        var column_index: usize = 0;
        while (column_index < row.column_count) : (column_index += 1) {
            if (column_index > 0) {
                writer.writeAll(",") catch return error.ResponseTooLarge;
            }
            try serializeValue(writer, row.values[column_index]);
        }
        writer.writeAll("\n") catch return error.ResponseTooLarge;
    }

    if (pool_stats) |stats| {
        try serializeInspectStats(
            writer,
            &result.stats,
            stats,
            catalog.snapshotOverflowReclaimStats(),
        );
    }
}

fn serializeInspectStats(
    writer: anytype,
    exec_stats: *const exec_mod.ExecStats,
    pool_stats: PoolStats,
    overflow_stats: OverflowReclaimStatsSnapshot,
) error{ResponseTooLarge}!void {
    writer.print(
        "INSPECT exec rows_scanned={d} rows_matched={d} rows_returned={d} rows_inserted={d} rows_updated={d} rows_deleted={d} pages_read={d} pages_written={d}\n",
        .{
            exec_stats.rows_scanned,
            exec_stats.rows_matched,
            exec_stats.rows_returned,
            exec_stats.rows_inserted,
            exec_stats.rows_updated,
            exec_stats.rows_deleted,
            exec_stats.pages_read,
            exec_stats.pages_written,
        },
    ) catch return error.ResponseTooLarge;
    writer.print(
        "INSPECT pool policy={s} size={d} checked_out={d} pinned={d} exhausted_total={d}\n",
        .{
            @tagName(pool_stats.overload_policy),
            pool_stats.pool_size,
            pool_stats.checked_out,
            pool_stats.pinned,
            pool_stats.pool_exhausted_total,
        },
    ) catch return error.ResponseTooLarge;
    writer.print(
        "INSPECT overflow reclaim_queue_depth={d} reclaim_enqueued_total={d} reclaim_dequeued_total={d} reclaim_chains_total={d} reclaim_pages_total={d} reclaim_failures_total={d}\n",
        .{
            overflow_stats.queue_depth,
            overflow_stats.enqueued_total,
            overflow_stats.dequeued_total,
            overflow_stats.reclaimed_chains_total,
            overflow_stats.reclaimed_pages_total,
            overflow_stats.reclaim_failures_total,
        },
    ) catch return error.ResponseTooLarge;
    writer.print(
        "INSPECT spill temp_pages_allocated={d} temp_pages_reclaimed={d} temp_bytes_written={d} temp_bytes_read={d}\n",
        .{
            exec_stats.temp_pages_allocated,
            exec_stats.temp_pages_reclaimed,
            exec_stats.temp_bytes_written,
            exec_stats.temp_bytes_read,
        },
    ) catch return error.ResponseTooLarge;
    writer.writeAll("INSPECT plan source_model=") catch
        return error.ResponseTooLarge;
    writer.writeAll(
        exec_stats.plan.source_model[0..exec_stats.plan.source_model_len],
    ) catch return error.ResponseTooLarge;
    writer.writeAll(" pipeline=") catch return error.ResponseTooLarge;
    if (exec_stats.plan.pipeline_op_count == 0) {
        writer.writeAll("scan_only") catch return error.ResponseTooLarge;
    } else {
        var op_index: u8 = 0;
        while (op_index < exec_stats.plan.pipeline_op_count) : (op_index += 1) {
            if (op_index > 0) {
                writer.writeAll(">") catch return error.ResponseTooLarge;
            }
            writer.writeAll(
                planOpLabel(exec_stats.plan.pipeline_ops[op_index]),
            ) catch return error.ResponseTooLarge;
        }
    }
    writer.print(
        " join_strategy={s} join_order={s} materialization={s} sort_strategy={s} group_strategy={s} nested_relations={d}\n",
        .{
            @tagName(exec_stats.plan.join_strategy),
            @tagName(exec_stats.plan.join_order),
            @tagName(exec_stats.plan.materialization_mode),
            @tagName(exec_stats.plan.sort_strategy),
            @tagName(exec_stats.plan.group_strategy),
            exec_stats.plan.nested_relation_count,
        },
    ) catch return error.ResponseTooLarge;
    writer.print(
        "INSPECT explain sort={s} group={s}\n",
        .{
            sortStrategyExplain(exec_stats.plan.sort_strategy),
            groupStrategyExplain(exec_stats.plan.group_strategy),
        },
    ) catch return error.ResponseTooLarge;
}

fn planOpLabel(op: exec_mod.PlanOp) []const u8 {
    return switch (op) {
        .where_filter => "where",
        .having_filter => "having",
        .group_op => "group",
        .limit_op => "limit",
        .offset_op => "offset",
        .insert_op => "insert",
        .update_op => "update",
        .delete_op => "delete",
        .sort_op => "sort",
        .inspect_op => "inspect",
    };
}

fn sortStrategyExplain(strategy: exec_mod.SortStrategy) []const u8 {
    return switch (strategy) {
        .none => "not_applied",
        .in_place_insertion => "rows sorted in place with insertion order swaps",
    };
}

fn groupStrategyExplain(strategy: exec_mod.GroupStrategy) []const u8 {
    return switch (strategy) {
        .none => "not_applied",
        .in_memory_linear => "groups merged with linear key scan in memory",
    };
}
