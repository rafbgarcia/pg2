//! Query result serialization: flat row output, inspect stats, and plan explanation.
const std = @import("std");
const diagnostics_mod = @import("diagnostics.zig");
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
const SlotReclaimStatsSnapshot = catalog_mod.SlotReclaimStatsSnapshot;
const IndexReclaimStatsSnapshot = catalog_mod.IndexReclaimStatsSnapshot;
const PoolStats = pool_mod.PoolStats;
const Ast = ast_mod.Ast;
const ResultRow = scan_mod.ResultRow;
const StringArena = scan_mod.StringArena;
const SpillingResultCollector = spill_collector_mod.SpillingResultCollector;
const RuntimeInspectStats = diagnostics_mod.RuntimeInspectStats;
const TxInspectStats = diagnostics_mod.TxInspectStats;
const serializeValue = session.serializeValue;
const buildTreeProjection = tree_protocol.buildTreeProjection;
const countProtocolRootRows = tree_protocol.countProtocolRootRows;
const serializeTreeProtocol = tree_protocol.serializeTreeProtocol;

pub fn serializeQueryResult(
    writer: anytype,
    result: *exec_mod.QueryResult,
    catalog: *const Catalog,
    pool_stats: ?PoolStats,
    runtime_stats: ?RuntimeInspectStats,
    tx_stats: ?TxInspectStats,
    ast: *const Ast,
    tokens: *const tokenizer_mod.TokenizeResult,
    source: []const u8,
) error{ResponseTooLarge}!void {
    std.debug.assert(result.row_count <= result.rows.len);
    if (result.getError()) |message| {
        try serializeCanonicalQueryError(writer, message);
        return;
    }

    const tree_projection = buildTreeProjection(ast, tokens, source, catalog);
    if (!result.has_final_payload and tree_projection == null and result.collector != null) {
        if (!collectorWindowReadable(
            result.collector.?,
            result.collector_output_offset,
            result.collector_output_count,
        )) {
            exec_mod.setError(
                result,
                "spill collector read failed during serialization",
            );
            try serializeCanonicalQueryError(writer, result.getError().?);
            return;
        }
    }

    // Determine returned row count: when a spill collector is active,
    // use its total count; otherwise fall back to the flat row_count.
    const returned_rows: u32 = if (result.has_final_payload)
        1
    else if (tree_projection) |projection|
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

    if (result.has_final_payload) {
        writer.writeAll(result.final_payload) catch return error.ResponseTooLarge;
        writer.writeAll("\n") catch return error.ResponseTooLarge;
        if (pool_stats) |stats| {
            const oldest_active = if (tx_stats) |ts| ts.oldest_active_tx_id else std.math.maxInt(u64);
            try serializeInspectStats(
                writer,
                &result.stats,
                stats,
                runtime_stats,
                tx_stats,
                catalog.snapshotOverflowReclaimStats(),
                catalog.snapshotSlotReclaimStatsAtOldest(oldest_active),
                catalog.snapshotIndexReclaimStatsAtOldest(oldest_active),
            );
        }
        return;
    }

    if (tree_projection) |projection| {
        // Tree protocol always uses the flat array path.
        try serializeTreeProtocol(writer, result, &projection, catalog, tokens, source);
        if (pool_stats) |stats| {
            const oldest_active = if (tx_stats) |ts| ts.oldest_active_tx_id else std.math.maxInt(u64);
            try serializeInspectStats(
                writer,
                &result.stats,
                stats,
                runtime_stats,
                tx_stats,
                catalog.snapshotOverflowReclaimStats(),
                catalog.snapshotSlotReclaimStatsAtOldest(oldest_active),
                catalog.snapshotIndexReclaimStatsAtOldest(oldest_active),
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
            const has_row = iter.next(&out, &arena) catch return error.ResponseTooLarge;
            if (!has_row) return error.ResponseTooLarge;
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
        const oldest_active = if (tx_stats) |ts| ts.oldest_active_tx_id else std.math.maxInt(u64);
        try serializeInspectStats(
            writer,
            &result.stats,
            stats,
            runtime_stats,
            tx_stats,
            catalog.snapshotOverflowReclaimStats(),
            catalog.snapshotSlotReclaimStatsAtOldest(oldest_active),
            catalog.snapshotIndexReclaimStatsAtOldest(oldest_active),
        );
    }
}

fn collectorWindowCount(total_rows: u64, offset: u64, count: u64) u64 {
    if (offset >= total_rows) return 0;
    const available = total_rows - offset;
    return @min(available, count);
}

const CanonicalQueryError = struct {
    statement_index: ?u16 = null,
    message: []const u8,
    phase: []const u8 = "execution",
    code: []const u8 = "QueryExecutionError",
    path: []const u8 = "query",
    line: u16 = 1,
    col: u16 = 1,
};

fn serializeCanonicalQueryError(writer: anytype, raw_message: []const u8) error{ResponseTooLarge}!void {
    const canonical = parseCanonicalQueryError(raw_message);
    if (canonical.statement_index) |statement_index| {
        writer.print("ERR query: statement_index={d} ", .{statement_index}) catch
            return error.ResponseTooLarge;
    } else {
        writer.writeAll("ERR query: ") catch return error.ResponseTooLarge;
    }
    writer.writeAll("message=\"") catch return error.ResponseTooLarge;
    try writeEscapedQueryErrorMessage(writer, canonical.message);
    writer.print(
        "\" phase={s} code={s} path={s} line={d} col={d}\n",
        .{
            canonical.phase,
            canonical.code,
            canonical.path,
            canonical.line,
            canonical.col,
        },
    ) catch return error.ResponseTooLarge;
}

fn writeEscapedQueryErrorMessage(writer: anytype, message: []const u8) error{ResponseTooLarge}!void {
    for (message) |ch| {
        if (ch == '"' or ch == '\\') {
            writer.writeByte('\\') catch return error.ResponseTooLarge;
        }
        writer.writeByte(ch) catch return error.ResponseTooLarge;
    }
}

fn parseCanonicalQueryError(raw_message: []const u8) CanonicalQueryError {
    var parsed = CanonicalQueryError{
        .message = raw_message,
    };
    const with_stmt = parseStatementIndexPrefix(raw_message);
    parsed.statement_index = with_stmt.statement_index;
    const trimmed = std.mem.trim(u8, with_stmt.rest, " \t\r\n");
    if (trimmed.len == 0) return parsed;

    if (parseKeyValueError(trimmed)) |kv| {
        parsed.message = kv.message;
        parsed.phase = kv.phase;
        parsed.code = kv.code;
        parsed.path = kv.path;
        parsed.line = kv.line;
        parsed.col = kv.col;
        return parsed;
    }

    if (parseLegacyBoundaryError(trimmed)) |legacy| {
        parsed.message = legacy.message;
        parsed.code = legacy.code;
        return parsed;
    }

    parsed.message = trimmed;
    return parsed;
}

fn parseStatementIndexPrefix(raw: []const u8) struct { statement_index: ?u16, rest: []const u8 } {
    const prefix = "statement_index=";
    if (!std.mem.startsWith(u8, raw, prefix)) {
        return .{ .statement_index = null, .rest = raw };
    }
    var cursor: usize = prefix.len;
    const start = cursor;
    while (cursor < raw.len and std.ascii.isDigit(raw[cursor])) : (cursor += 1) {}
    if (cursor == start) return .{ .statement_index = null, .rest = raw };
    const parsed = std.fmt.parseInt(u16, raw[start..cursor], 10) catch
        return .{ .statement_index = null, .rest = raw };
    while (cursor < raw.len and raw[cursor] == ' ') : (cursor += 1) {}
    return .{ .statement_index = parsed, .rest = raw[cursor..] };
}

fn parseLegacyBoundaryError(raw: []const u8) ?struct { message: []const u8, code: []const u8 } {
    const class_key = "; class=";
    const code_key = "; code=";
    const class_pos = std.mem.indexOf(u8, raw, class_key) orelse return null;
    const code_pos = std.mem.indexOf(u8, raw, code_key) orelse return null;
    if (code_pos <= class_pos) return null;
    const message = std.mem.trim(u8, raw[0..class_pos], " \t\r\n");
    const code_start = code_pos + code_key.len;
    if (code_start >= raw.len) return null;
    const code = std.mem.trim(u8, raw[code_start..], " \t\r\n");
    if (message.len == 0 or code.len == 0) return null;
    return .{ .message = message, .code = code };
}

fn parseKeyValueError(raw: []const u8) ?struct {
    message: []const u8,
    phase: []const u8,
    code: []const u8,
    path: []const u8,
    line: u16,
    col: u16,
} {
    const message = extractQuotedField(raw, "message=\"") orelse return null;
    const phase = extractTokenField(raw, "phase=") orelse return null;
    const code = extractTokenField(raw, "code=") orelse return null;
    const path = extractTokenField(raw, "path=") orelse return null;
    const line = parseFieldU16(raw, "line=") orelse 1;
    const col = parseFieldU16(raw, "col=") orelse 1;
    return .{
        .message = message,
        .phase = phase,
        .code = code,
        .path = path,
        .line = line,
        .col = col,
    };
}

fn extractQuotedField(raw: []const u8, key: []const u8) ?[]const u8 {
    const start = std.mem.indexOf(u8, raw, key) orelse return null;
    var cursor = start + key.len;
    const message_start = cursor;
    while (cursor < raw.len) : (cursor += 1) {
        if (raw[cursor] == '"' and (cursor == message_start or raw[cursor - 1] != '\\')) {
            return raw[message_start..cursor];
        }
    }
    return null;
}

fn extractTokenField(raw: []const u8, key: []const u8) ?[]const u8 {
    const start = std.mem.indexOf(u8, raw, key) orelse return null;
    var cursor = start + key.len;
    const field_start = cursor;
    while (cursor < raw.len and raw[cursor] != ' ') : (cursor += 1) {}
    if (cursor == field_start) return null;
    return raw[field_start..cursor];
}

fn parseFieldU16(raw: []const u8, key: []const u8) ?u16 {
    const token = extractTokenField(raw, key) orelse return null;
    return std.fmt.parseInt(u16, token, 10) catch null;
}

fn collectorWindowReadable(
    collector: *SpillingResultCollector,
    offset: u64,
    count: u64,
) bool {
    var iter = collector.iterator();
    var arena_buf: [scan_mod.max_row_size_bytes + 192]u8 = undefined;
    var arena = StringArena.init(&arena_buf);
    var out = ResultRow.init();
    var skip = offset;
    var remaining = count;
    while (remaining > 0) {
        arena.reset();
        const has_row = iter.next(&out, &arena) catch return false;
        if (!has_row) return false;
        if (skip > 0) {
            skip -= 1;
            continue;
        }
        remaining -= 1;
    }
    return true;
}

fn serializeInspectStats(
    writer: anytype,
    exec_stats: *const exec_mod.ExecStats,
    pool_stats: PoolStats,
    runtime_stats: ?RuntimeInspectStats,
    tx_stats: ?TxInspectStats,
    overflow_stats: OverflowReclaimStatsSnapshot,
    slot_stats: SlotReclaimStatsSnapshot,
    index_stats: IndexReclaimStatsSnapshot,
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
    if (runtime_stats) |stats| {
        const request_invariant_ok =
            stats.requests_enqueued_total >= stats.requests_dispatched_total and
            stats.requests_dispatched_total >= stats.requests_completed_total;
        std.debug.assert(request_invariant_ok);
        writer.print(
            "INSPECT runtime queue_depth={d} workers_busy={d} pool_pinned={d} requests_enqueued_total={d} requests_dispatched_total={d} requests_completed_total={d} queue_full_total={d} queue_timeout_total={d} max_queue_wait_ticks={d} max_pin_wait_ticks={d} max_pin_duration_ticks={d} request_invariant_ok={}\n",
            .{
                stats.queue_depth,
                stats.workers_busy,
                stats.pool_pinned,
                stats.requests_enqueued_total,
                stats.requests_dispatched_total,
                stats.requests_completed_total,
                stats.queue_full_total,
                stats.queue_timeout_total,
                stats.max_queue_wait_ticks,
                stats.max_pin_wait_ticks,
                stats.max_pin_duration_ticks,
                request_invariant_ok,
            },
        ) catch return error.ResponseTooLarge;
    }
    if (tx_stats) |stats| {
        writer.print(
            "INSPECT tx active_count={d} oldest_active_tx_id={d} next_tx_id={d} base_tx_id={d}\n",
            .{
                stats.active_count,
                stats.oldest_active_tx_id,
                stats.next_tx_id,
                stats.base_tx_id,
            },
        ) catch return error.ResponseTooLarge;
    }
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
        "INSPECT heap_reclaim queue_depth={d} pinned_by_snapshot={d} reclaim_enqueued_total={d} reclaim_dequeued_total={d} reclaimed_slots_total={d} reclaim_failures_total={d}\n",
        .{
            slot_stats.queue_depth,
            slot_stats.pinned_by_snapshot,
            slot_stats.enqueued_total,
            slot_stats.dequeued_total,
            slot_stats.reclaimed_total,
            slot_stats.reclaim_failures_total,
        },
    ) catch return error.ResponseTooLarge;
    writer.print(
        "INSPECT index_reclaim queue_depth={d} pinned_by_snapshot={d} reclaim_enqueued_total={d} reclaim_dequeued_total={d} reclaimed_entries_total={d} reclaim_failures_total={d}\n",
        .{
            index_stats.queue_depth,
            index_stats.pinned_by_snapshot,
            index_stats.enqueued_total,
            index_stats.dequeued_total,
            index_stats.reclaimed_total,
            index_stats.reclaim_failures_total,
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
        " join_strategy={s} join_order={s} materialization={s} sort_strategy={s} group_strategy={s} streaming_mode={s} parallel_mode={s} parallel_scheduler_path={s} parallel_schedule_task_count={d} parallel_schedule_applied_tasks={d} parallel_schedule_fingerprint={x} nested_relations={d} nested_join_nested_loop={d} nested_join_hash_in_memory={d} nested_join_hash_spill={d} planner_policy_version={d} planner_snapshot_fingerprint={x} planner_decision_fingerprint={x} join_reason={s} materialization_reason={s} sort_reason={s} group_reason={s} streaming_reason={s}\n",
        .{
            @tagName(exec_stats.plan.join_strategy),
            @tagName(exec_stats.plan.join_order),
            @tagName(exec_stats.plan.materialization_mode),
            @tagName(exec_stats.plan.sort_strategy),
            @tagName(exec_stats.plan.group_strategy),
            @tagName(exec_stats.plan.streaming_mode),
            @tagName(exec_stats.plan.parallel_mode),
            @tagName(exec_stats.plan.parallel_scheduler_path),
            exec_stats.plan.parallel_schedule_task_count,
            exec_stats.plan.parallel_schedule_applied_tasks,
            exec_stats.plan.parallel_schedule_fingerprint,
            exec_stats.plan.nested_relation_count,
            exec_stats.plan.nested_join_nested_loop_count,
            exec_stats.plan.nested_join_hash_in_memory_count,
            exec_stats.plan.nested_join_hash_spill_count,
            exec_stats.plan.planner_policy_version,
            exec_stats.plan.planner_snapshot_fingerprint,
            exec_stats.plan.planner_decision_fingerprint,
            @tagName(exec_stats.plan.join_reason),
            @tagName(exec_stats.plan.materialization_reason),
            @tagName(exec_stats.plan.sort_reason),
            @tagName(exec_stats.plan.group_reason),
            @tagName(exec_stats.plan.streaming_reason),
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
    writer.print(
        "INSPECT explain_detail join={s} materialization={s} streaming={s} parallel={s} scheduler={s}\n",
        .{
            joinStrategyExplain(exec_stats.plan.join_strategy),
            materializationExplain(exec_stats.plan.materialization_mode),
            streamingExplain(exec_stats.plan.streaming_mode),
            parallelModeExplain(exec_stats.plan.parallel_mode),
            parallelSchedulerExplain(exec_stats.plan.parallel_scheduler_path),
        },
    ) catch return error.ResponseTooLarge;
    var checkpoint_index: u8 = 0;
    while (checkpoint_index < exec_stats.plan.checkpoint_count) : (checkpoint_index += 1) {
        const checkpoint = exec_stats.plan.checkpoints[checkpoint_index];
        writer.print(
            "INSPECT checkpoint name={s} prior_decision={x} new_decision={x} reason={s} degraded={}\n",
            .{
                @tagName(checkpoint.checkpoint),
                checkpoint.prior_decision_fingerprint,
                checkpoint.new_decision_fingerprint,
                @tagName(checkpoint.reason),
                checkpoint.degraded,
            },
        ) catch return error.ResponseTooLarge;
    }
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

fn joinStrategyExplain(strategy: exec_mod.JoinStrategy) []const u8 {
    return switch (strategy) {
        .none => "not_applied",
        .nested_loop => "nested loop join",
        .hash_in_memory => "in-memory hash join",
        .hash_spill => "spill-backed hash join",
    };
}

fn materializationExplain(mode: exec_mod.MaterializationMode) []const u8 {
    return switch (mode) {
        .none => "no explicit bounded materialization",
        .bounded_row_buffers => "bounded row buffers with spill-safe path",
    };
}

fn streamingExplain(mode: exec_mod.StreamingMode) []const u8 {
    return switch (mode) {
        .disabled => "streaming disabled for bounded safety",
        .enabled => "streaming enabled",
    };
}

fn parallelModeExplain(mode: exec_mod.ParallelMode) []const u8 {
    return switch (mode) {
        .sequential => "sequential mode",
        .enabled => "parallel policy enabled",
    };
}

fn parallelSchedulerExplain(path: exec_mod.ParallelSchedulerPath) []const u8 {
    return switch (path) {
        .direct => "direct execution path",
        .scheduled_serial => "deterministic serial scheduler path",
    };
}
