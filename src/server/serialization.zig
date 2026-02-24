//! Query result serialization: flat row output, inspect stats, and plan explanation.
const std = @import("std");
const session = @import("session.zig");
const tree_protocol = @import("tree_protocol.zig");
const exec_mod = @import("../executor/executor.zig");
const catalog_mod = @import("../catalog/catalog.zig");
const pool_mod = @import("pool.zig");
const tokenizer_mod = @import("../parser/tokenizer.zig");
const ast_mod = @import("../parser/ast.zig");
const scan_mod = @import("../executor/scan.zig");
const spill_collector_mod = @import("../executor/spill_collector.zig");

const Catalog = catalog_mod.Catalog;
const OverflowReclaimStatsSnapshot = catalog_mod.OverflowReclaimStatsSnapshot;
const PoolStats = pool_mod.PoolStats;
const Ast = ast_mod.Ast;
const ResultRow = scan_mod.ResultRow;
const StringArena = scan_mod.StringArena;
const SpillingResultCollector = spill_collector_mod.SpillingResultCollector;
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

    // Determine returned row count: when a spill collector is active,
    // use its total count; otherwise fall back to the flat row_count.
    const returned_rows: u32 = if (tree_projection) |projection|
        countProtocolRootRows(result, &projection)
    else if (result.collector) |collector|
        @intCast(@min(
            collectorWindowCount(
                collector.totalRowCount(),
                result.collector_output_offset,
                result.collector_output_count,
            ),
            std.math.maxInt(u32),
        ))
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
        // Tree protocol always uses the flat array path.
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

    // When a collector is active, iterate from spilled + in-memory rows.
    if (result.collector) |collector| {
        var iter = collector.iterator();
        var arena_buf: [scan_mod.max_row_size_bytes + 192]u8 = undefined;
        var arena = StringArena.init(&arena_buf);
        var out = ResultRow.init();
        var skip = result.collector_output_offset;
        var remaining = result.collector_output_count;
        while (true) {
            if (remaining == 0) break;
            arena.reset();
            const has_row = iter.next(&out, &arena) catch break;
            if (!has_row) break;
            if (skip > 0) {
                skip -= 1;
                continue;
            }
            try serializeRowColumns(writer, &out);
            remaining -= 1;
        }
    } else {
        // Standard flat-array path.
        var row_index: usize = 0;
        while (row_index < result.row_count) : (row_index += 1) {
            try serializeRowColumns(writer, &result.rows[row_index]);
        }
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

fn collectorWindowCount(total_rows: u64, offset: u64, count: u64) u64 {
    if (offset >= total_rows) return 0;
    const available = total_rows - offset;
    return @min(available, count);
}

fn serializeInspectStats(
    writer: anytype,
    exec_stats: *const exec_mod.ExecStats,
    pool_stats: PoolStats,
    overflow_stats: OverflowReclaimStatsSnapshot,
) error{ResponseTooLarge}!void {
    const pin_invariant_ok =
        pool_stats.pinned <= pool_stats.checked_out and
        pool_stats.checked_out <= pool_stats.pool_size;
    std.debug.assert(pin_invariant_ok);
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
        "INSPECT pool policy={s} size={d} checked_out={d} pinned={d} exhausted_total={d} pin_invariant_ok={}\n",
        .{
            @tagName(pool_stats.overload_policy),
            pool_stats.pool_size,
            pool_stats.checked_out,
            pool_stats.pinned,
            pool_stats.pool_exhausted_total,
            pin_invariant_ok,
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
        "INSPECT spill spill_triggered={} result_bytes_accumulated={d} temp_pages_allocated={d} temp_pages_reclaimed={d} temp_bytes_written={d} temp_bytes_read={d}\n",
        .{
            exec_stats.spill_triggered,
            exec_stats.result_bytes_accumulated,
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
        " join_strategy={s} join_order={s} materialization={s} sort_strategy={s} group_strategy={s} nested_relations={d} nested_join_nested_loop={d} nested_join_hash_in_memory={d} nested_join_hash_spill={d}\n",
        .{
            @tagName(exec_stats.plan.join_strategy),
            @tagName(exec_stats.plan.join_order),
            @tagName(exec_stats.plan.materialization_mode),
            @tagName(exec_stats.plan.sort_strategy),
            @tagName(exec_stats.plan.group_strategy),
            exec_stats.plan.nested_relation_count,
            exec_stats.plan.nested_join_nested_loop_count,
            exec_stats.plan.nested_join_hash_in_memory_count,
            exec_stats.plan.nested_join_hash_spill_count,
        },
    ) catch return error.ResponseTooLarge;
    writer.print(
        "INSPECT explain sort={s} group={s} nested_join_breakdown=nested_loop:{d},hash_in_memory:{d},hash_spill:{d}\n",
        .{
            sortStrategyExplain(exec_stats.plan.sort_strategy),
            groupStrategyExplain(exec_stats.plan.group_strategy),
            exec_stats.plan.nested_join_nested_loop_count,
            exec_stats.plan.nested_join_hash_in_memory_count,
            exec_stats.plan.nested_join_hash_spill_count,
        },
    ) catch return error.ResponseTooLarge;
}

/// Serialize one row's column values as comma-separated text followed by newline.
fn serializeRowColumns(writer: anytype, row: *const ResultRow) error{ResponseTooLarge}!void {
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
        .in_memory_merge => "rows sorted with stable bottom-up merge sort",
        .external_merge => "rows sorted with external merge sort via temp pages",
    };
}

fn groupStrategyExplain(strategy: exec_mod.GroupStrategy) []const u8 {
    return switch (strategy) {
        .none => "not_applied",
        .in_memory_linear => "groups merged with linear key scan in memory",
        .hash_spill => "groups aggregated with hash table and partition spill",
    };
}
