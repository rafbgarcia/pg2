//! Query executor orchestration and operator pipeline runtime.
//!
//! Responsibilities in this file:
//! - Interprets parsed pipeline AST into executable operator stages.
//! - Coordinates scan/filter/mutation helpers under one execution context.
//! - Produces query results plus always-on execution/planning statistics.
//! - Enforces bounded in-memory operator behavior via capacity contracts.
const std = @import("std");
const ast_mod = @import("../parser/ast.zig");
const tokenizer_mod = @import("../parser/tokenizer.zig");
const heap_storage_mod = @import("../storage/heap.zig");
const row_mod = @import("../storage/row.zig");
const io_mod = @import("../storage/io.zig");
const buffer_pool_mod = @import("../storage/buffer_pool.zig");
const wal_mod = @import("../storage/wal.zig");
const catalog_mod = @import("../catalog/catalog.zig");
const tx_mod = @import("../mvcc/transaction.zig");
const undo_mod = @import("../mvcc/undo.zig");
const filter_mod = @import("filter.zig");
const scan_mod = @import("scan.zig");
const mutation_mod = @import("mutation.zig");
const capacity_mod = @import("capacity.zig");
const runtime_errors = @import("../runtime/error_taxonomy.zig");
const aggregation_mod = @import("aggregation.zig");
const sorting_mod = @import("sorting.zig");
const joins_mod = @import("joins.zig");
const projections_mod = @import("projections.zig");
const spill_collector_mod = @import("spill_collector.zig");
const temp_mod = @import("../storage/temp.zig");

const SpillingResultCollector = spill_collector_mod.SpillingResultCollector;
const TempStorageManager = temp_mod.TempStorageManager;

const Allocator = std.mem.Allocator;
const Ast = ast_mod.Ast;
const NodeIndex = ast_mod.NodeIndex;
const NodeTag = ast_mod.NodeTag;
const null_node = ast_mod.null_node;
const TokenizeResult = tokenizer_mod.TokenizeResult;
const max_tokens = tokenizer_mod.max_tokens;
const RowId = heap_storage_mod.RowId;
const Value = row_mod.Value;
const RowSchema = row_mod.RowSchema;
const Storage = io_mod.Storage;
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
pub const ParameterBinding = filter_mod.ParameterBinding;
const GroupRuntime = aggregation_mod.GroupRuntime;
const JoinDescriptor = joins_mod.JoinDescriptor;

/// Maximum pipeline operators in a single query.
pub const max_operators = capacity_mod.max_pipeline_operators;

pub const PlanOp = enum {
    where_filter,
    having_filter,
    group_op,
    limit_op,
    offset_op,
    insert_op,
    update_op,
    delete_op,
    sort_op,
    inspect_op,
};

pub const JoinStrategy = enum {
    none,
    nested_loop,
};

pub const JoinOrder = enum {
    none,
    source_then_nested,
};

pub const MaterializationMode = enum {
    none,
    bounded_row_buffers,
};

pub const SortStrategy = enum {
    none,
    in_place_insertion,
};

pub const GroupStrategy = enum {
    none,
    in_memory_linear,
};

pub const PlanStats = struct {
    source_model: [32]u8 = [_]u8{0} ** 32,
    source_model_len: u8 = 0,
    pipeline_ops: [max_operators]PlanOp =
        [_]PlanOp{.inspect_op} ** max_operators,
    pipeline_op_count: u8 = 0,
    join_strategy: JoinStrategy = .none,
    join_order: JoinOrder = .none,
    materialization_mode: MaterializationMode = .none,
    sort_strategy: SortStrategy = .none,
    group_strategy: GroupStrategy = .none,
    nested_relation_count: u8 = 0,
};

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
    temp_pages_allocated: u32 = 0,
    temp_pages_reclaimed: u32 = 0,
    temp_bytes_written: u64 = 0,
    temp_bytes_read: u64 = 0,
    spill_triggered: bool = false,
    result_bytes_accumulated: u64 = 0,
    plan: PlanStats = .{},
};

/// Result of executing a query. Row storage is fixed-capacity and embedded to
/// avoid runtime allocation in the hot execution path.
pub const QueryResult = struct {
    rows: []ResultRow,
    row_count: u16 = 0,
    stats: ExecStats = .{},
    has_error: bool = false,
    error_message: [512]u8 = std.mem.zeroes([512]u8),
    /// When non-null, serialization iterates from this collector instead of
    /// the flat `rows` array. Points to the per-slot collector in
    /// `BootstrappedRuntime` — survives through serialization.
    collector: ?*SpillingResultCollector = null,

    pub fn init(rows: []ResultRow) QueryResult {
        std.debug.assert(rows.len >= scan_mod.scan_batch_size);
        return .{
            .rows = rows,
        };
    }

    pub fn deinit(self: *QueryResult) void {
        _ = self;
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
    statement_timestamp_micros: ?i64,
    parameter_bindings: []const ParameterBinding,
    allocator: Allocator,
    result_rows: []ResultRow,
    scratch_rows_a: []ResultRow,
    scratch_rows_b: []ResultRow,
    string_arena_bytes: []u8,
    storage: Storage,
    query_slot_index: u16,
    collector: *SpillingResultCollector,
    work_memory_bytes_per_slot: u64,
};

/// Operator kind extracted from the AST.
const OpKind = PlanOp;

/// A pipeline operator descriptor.
pub const OpDescriptor = struct {
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
    var result = QueryResult.init(ctx.result_rows);
    errdefer result.deinit();
    var string_arena = scan_mod.StringArena.init(ctx.string_arena_bytes);
    string_arena.reset();

    const first_stmt = findFirstStatement(ctx.ast) orelse {
        setError(&result, "no statements found in query");
        return result;
    };

    var total_stats = ExecStats{};
    var statement_index: u16 = 0;
    var stmt = first_stmt;
    while (stmt != null_node) : (statement_index += 1) {
        resetQueryResultForStatement(&result);
        executeSingleStatement(
            ctx,
            &result,
            stmt,
            statement_index,
            &string_arena,
        );
        accumulateStatementStats(&total_stats, &result.stats);

        if (result.has_error) {
            result.stats = total_stats;
            return result;
        }

        const node = ctx.ast.getNode(stmt);
        stmt = node.next;
    }
    result.stats = total_stats;

    std.debug.assert(result.row_count <= scan_mod.scan_batch_size);
    return result;
}

fn resetQueryResultForStatement(result: *QueryResult) void {
    result.row_count = 0;
    result.stats = .{};
    result.has_error = false;
    @memset(result.error_message[0..], 0);
}

fn accumulateStatementStats(total: *ExecStats, current: *const ExecStats) void {
    total.rows_scanned +|= current.rows_scanned;
    total.rows_matched +|= current.rows_matched;
    total.rows_returned +|= current.rows_returned;
    total.rows_inserted +|= current.rows_inserted;
    total.rows_updated +|= current.rows_updated;
    total.rows_deleted +|= current.rows_deleted;
    total.pages_read +|= current.pages_read;
    total.pages_written +|= current.pages_written;
    total.temp_pages_allocated +|= current.temp_pages_allocated;
    total.temp_pages_reclaimed +|= current.temp_pages_reclaimed;
    total.temp_bytes_written +|= current.temp_bytes_written;
    total.temp_bytes_read +|= current.temp_bytes_read;
    total.spill_triggered = total.spill_triggered or current.spill_triggered;
    total.result_bytes_accumulated +|= current.result_bytes_accumulated;
    total.plan = current.plan;
}

fn executeSingleStatement(
    ctx: *const ExecContext,
    result: *QueryResult,
    statement_node: NodeIndex,
    statement_index: u16,
    string_arena: *scan_mod.StringArena,
) void {
    const statement = ctx.ast.getNode(statement_node);
    switch (statement.tag) {
        .pipeline => executePipelineStatement(
            ctx,
            result,
            statement_node,
            string_arena,
        ),
        .let_binding => setStatementError(
            result,
            statement_index,
            "let bindings are not executable yet",
        ),
        else => setStatementError(
            result,
            statement_index,
            "unsupported statement type",
        ),
    }
}

fn executePipelineStatement(
    ctx: *const ExecContext,
    result: *QueryResult,
    pipeline_idx: NodeIndex,
    string_arena: *scan_mod.StringArena,
) void {
    const pipeline = ctx.ast.getNode(pipeline_idx);
    if (pipeline.tag != .pipeline) {
        setError(result, "expected pipeline node");
        return;
    }

    const source_node = ctx.ast.getNode(pipeline.data.binary.lhs);
    if (source_node.tag != .pipe_source) {
        setError(result, "expected pipe_source node");
        return;
    }
    const model_name = ctx.tokens.getText(
        source_node.data.token,
        ctx.source,
    );
    const model_id = ctx.catalog.findModel(model_name) orelse {
        setError(result, "model not found");
        return;
    };

    var ops: [max_operators]OpDescriptor = undefined;
    var op_count: u16 = 0;
    buildOperatorList(
        ctx.ast,
        pipeline.data.binary.rhs,
        &ops,
        &op_count,
    );
    capturePlanStats(&result.stats.plan, model_name, &ops, op_count);

    if (findMutationOp(&ops, op_count)) |mut_idx| {
        executeMutation(
            ctx,
            result,
            pipeline_idx,
            model_id,
            &ops,
            op_count,
            mut_idx,
            string_arena,
        );
        return;
    }

    executeReadPipeline(
        ctx,
        result,
        pipeline_idx,
        model_id,
        &ops,
        op_count,
        string_arena,
    );
}

fn setStatementError(result: *QueryResult, statement_index: u16, message: []const u8) void {
    var buf: [512]u8 = undefined;
    const formatted = std.fmt.bufPrint(
        buf[0..],
        "statement_index={d} {s}",
        .{ statement_index, message },
    ) catch message;
    setError(result, formatted);
}

/// Execute the read path: chunked table scan with per-chunk WHERE filter,
/// spilling result collector, and post-scan operators.
///
/// The scan loop reads one batch at a time via ScanCursor, applies the WHERE
/// filter in-place, and feeds survivors into the SpillingResultCollector.
/// When all pages are exhausted the post-scan path runs GROUP/SORT/LIMIT/
/// OFFSET, nested joins, and column projection.
fn executeReadPipeline(
    ctx: *const ExecContext,
    result: *QueryResult,
    pipeline_node: NodeIndex,
    model_id: ModelId,
    ops: *const [max_operators]OpDescriptor,
    op_count: u16,
    string_arena: *scan_mod.StringArena,
) void {
    const caps = capacity_mod.OperatorCapacities.defaults();

    // --- 1. Init temp storage and collector for this query slot ---
    const temp_mgr = TempStorageManager.initDefault(
        ctx.query_slot_index,
        ctx.storage,
    ) catch |err| {
        setBoundaryError(
            result,
            "temp storage init failed",
            runtime_errors.classifyScan(mapTempInitError(err)),
            mapTempInitError(err),
        );
        return;
    };
    ctx.collector.* = SpillingResultCollector.init(
        result.rows,
        temp_mgr,
        ctx.work_memory_bytes_per_slot,
    );

    // --- 2. Chunked scan loop ---
    // Scan into scratch_rows_a so the collector's hot batch (result.rows)
    // is never overwritten by scan output.
    const scan_buf = ctx.scratch_rows_a[0..scan_mod.scan_batch_size];
    var chunk_result = QueryResult.init(ctx.scratch_rows_a);

    var cursor = scan_mod.ScanCursor.init();
    var total_pages_read: u32 = 0;
    var total_rows_scanned: u32 = 0;

    while (!cursor.done) {
        // Arena safety valve: if the hot batch is empty we can safely
        // reset the string arena (all prior strings have been serialized
        // into spill pages or are still in the hot batch).
        if (ctx.collector.hot_count == 0) {
            string_arena.reset();
        } else if (string_arena.bytes.len > 0) {
            // If the arena is nearly exhausted (< 10% remaining) and we
            // have hot rows, force-flush them so we can reclaim the arena.
            const remaining = string_arena.bytes.len - string_arena.used;
            const threshold = string_arena.bytes.len / 10;
            if (remaining < threshold) {
                ctx.collector.flushHotBatch() catch {
                    setError(result, "spill flush failed during arena reclaim");
                    return;
                };
                string_arena.reset();
            }
        }

        // Scan one chunk into the scratch buffer.
        const scan_result = scan_mod.tableScanInto(
            ctx.catalog,
            ctx.pool,
            ctx.undo_log,
            ctx.snapshot,
            ctx.tx_manager,
            model_id,
            scan_buf,
            string_arena,
            &cursor,
        ) catch |err| {
            setBoundaryError(
                result,
                "table scan failed",
                runtime_errors.classifyScan(err),
                err,
            );
            captureTempStats(result, ctx.collector);
            return;
        };
        total_pages_read += scan_result.pages_read;
        total_rows_scanned += scan_result.row_count;
        chunk_result.row_count = scan_result.row_count;

        // Apply per-chunk operators (WHERE filter) on the chunk result.
        applyPerChunkOperators(ctx, &chunk_result, model_id, ops, op_count, string_arena);
        if (chunk_result.has_error) {
            // Propagate error to the real result.
            if (chunk_result.getError()) |msg| setError(result, msg);
            captureTempStats(result, ctx.collector);
            return;
        }

        // Feed surviving rows into the collector.
        var row_idx: u16 = 0;
        while (row_idx < chunk_result.row_count) : (row_idx += 1) {
            ctx.collector.appendRow(&chunk_result.rows[row_idx]) catch {
                setError(result, "spill collector append failed");
                captureTempStats(result, ctx.collector);
                return;
            };
        }
    }

    result.stats.pages_read = total_pages_read;
    result.stats.rows_scanned = total_rows_scanned;

    // --- 3. Post-scan: materialize final result ---
    const spilled = ctx.collector.spillTriggered();
    const needs_full_input = hasFullInputOperators(ops, op_count);

    if (!spilled) {
        // No spill — hot batch rows are already in result.rows[0..hot_count].
        result.row_count = ctx.collector.hot_count;
        result.collector = null;

        if (!applyPostScanOperators(ctx, result, model_id, ops, op_count, &caps, string_arena)) {
            captureTempStats(result, ctx.collector);
            return;
        }
        if (!applyNestedSelectionJoin(ctx, result, pipeline_node, model_id, &caps, string_arena)) {
            captureTempStats(result, ctx.collector);
            return;
        }
    } else if (!needs_full_input) {
        // Spill occurred but no GROUP/SORT needed — let serialization
        // iterate directly from the collector.
        result.row_count = @intCast(@min(
            ctx.collector.totalRowCount(),
            scan_mod.scan_batch_size,
        ));
        result.collector = ctx.collector;
    } else {
        // Spill + full-input operators: reload from collector into result.rows.
        string_arena.reset();
        var iter = ctx.collector.iterator();
        var reload_count: u16 = 0;
        var arena_buf: [scan_mod.max_row_size_bytes + 192]u8 = undefined;
        var reload_arena = scan_mod.StringArena.init(&arena_buf);
        while (reload_count < scan_mod.scan_batch_size) {
            reload_arena.reset();
            const has_row = iter.next(&result.rows[reload_count], &reload_arena) catch {
                setError(result, "spill collector reload failed");
                captureTempStats(result, ctx.collector);
                return;
            };
            if (!has_row) break;
            // Copy strings from the temporary reload arena into the main
            // string arena so they survive post-scan operator processing.
            copyRowStringsToArena(&result.rows[reload_count], string_arena) catch {
                setError(result, "string arena exhausted during reload");
                captureTempStats(result, ctx.collector);
                return;
            };
            reload_count += 1;
        }
        result.row_count = reload_count;
        result.collector = null;

        if (!applyPostScanOperators(ctx, result, model_id, ops, op_count, &caps, string_arena)) {
            captureTempStats(result, ctx.collector);
            return;
        }
        if (!applyNestedSelectionJoin(ctx, result, pipeline_node, model_id, &caps, string_arena)) {
            captureTempStats(result, ctx.collector);
            return;
        }
    }

    result.stats.rows_matched = result.row_count;
    if (!applyFlatColumnProjection(ctx, result, pipeline_node, model_id, string_arena)) {
        captureTempStats(result, ctx.collector);
        return;
    }
    result.stats.rows_returned = result.row_count;
    captureTempStats(result, ctx.collector);
}

/// Copy string values in a row from their current backing into `arena`.
/// This is needed when reloading spilled rows: the reload arena is
/// temporary, so strings must be relocated into the main string arena.
fn copyRowStringsToArena(
    row: *ResultRow,
    arena: *scan_mod.StringArena,
) error{OutOfMemory}!void {
    var col: u16 = 0;
    while (col < row.column_count) : (col += 1) {
        switch (row.values[col]) {
            .string => |s| {
                row.values[col] = .{ .string = try arena.copyString(s) };
            },
            else => {},
        }
    }
}

/// Snapshot temp storage stats into the query result.
fn captureTempStats(result: *QueryResult, collector: *const SpillingResultCollector) void {
    const stats = collector.tempStats();
    result.stats.temp_pages_allocated = stats.temp_pages_allocated;
    result.stats.temp_pages_reclaimed = stats.temp_pages_reclaimed;
    result.stats.temp_bytes_written = stats.temp_bytes_written;
    result.stats.temp_bytes_read = stats.temp_bytes_read;
    result.stats.spill_triggered = collector.spillTriggered();
    result.stats.result_bytes_accumulated = collector.resultBytesAccumulated();
}

/// Map TempAllocatorError to ScanError for boundary error reporting.
fn mapTempInitError(err: temp_mod.TempAllocatorError) scan_mod.ScanError {
    return switch (err) {
        error.InvalidRegion => error.Corruption,
        error.RegionExhausted => error.StorageRead,
    };
}

const applyFlatColumnProjection = projections_mod.applyFlatColumnProjection;
const getPipelineSelection = projections_mod.getPipelineSelection;

fn applyReadOperators(
    ctx: *const ExecContext,
    result: *QueryResult,
    model_id: ModelId,
    ops: *const [max_operators]OpDescriptor,
    op_count: u16,
    caps: *const capacity_mod.OperatorCapacities,
    string_arena: *scan_mod.StringArena,
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
                string_arena,
            ),
            .having_filter => applyWhereFilter(
                ctx,
                result,
                op.node,
                schema,
                &group_runtime,
                string_arena,
            ),
            .group_op => {
                if (!aggregation_mod.applyGroup(
                    ctx,
                    result,
                    op.node,
                    schema,
                    ops,
                    op_count,
                    i,
                    caps,
                    &group_runtime,
                    string_arena,
                )) return false;
            },
            .limit_op => applyLimit(ctx, result, op.node, string_arena),
            .offset_op => applyOffset(ctx, result, op.node, &group_runtime, string_arena),
            .sort_op => {
                if (!sorting_mod.applySort(
                    ctx,
                    result,
                    op.node,
                    schema,
                    caps,
                    &group_runtime,
                    string_arena,
                )) return false;
            },
            .inspect_op => {},
            .insert_op, .update_op, .delete_op => {},
        }
    }
    return true;
}

/// Apply only per-chunk operators (WHERE filter) to the current result batch.
/// Called once per scan chunk inside the chunked scan loop.
fn applyPerChunkOperators(
    ctx: *const ExecContext,
    result: *QueryResult,
    model_id: ModelId,
    ops: *const [max_operators]OpDescriptor,
    op_count: u16,
    string_arena: *scan_mod.StringArena,
) void {
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
                string_arena,
            ),
            else => {},
        }
    }
}

/// Apply post-scan operators (GROUP/SORT/LIMIT/OFFSET/HAVING) to the
/// fully-collected result set. WHERE is skipped because it was already
/// applied per-chunk during the scan loop.
fn applyPostScanOperators(
    ctx: *const ExecContext,
    result: *QueryResult,
    model_id: ModelId,
    ops: *const [max_operators]OpDescriptor,
    op_count: u16,
    caps: *const capacity_mod.OperatorCapacities,
    string_arena: *scan_mod.StringArena,
) bool {
    var group_runtime = GroupRuntime{};
    const schema = &ctx.catalog.models[model_id].row_schema;
    var i: u16 = 0;
    while (i < op_count) : (i += 1) {
        const op = ops[i];
        switch (op.kind) {
            .where_filter => {}, // Already applied per-chunk.
            .having_filter => applyWhereFilter(
                ctx,
                result,
                op.node,
                schema,
                &group_runtime,
                string_arena,
            ),
            .group_op => {
                if (!aggregation_mod.applyGroup(
                    ctx,
                    result,
                    op.node,
                    schema,
                    ops,
                    op_count,
                    i,
                    caps,
                    &group_runtime,
                    string_arena,
                )) return false;
            },
            .limit_op => applyLimit(ctx, result, op.node, string_arena),
            .offset_op => applyOffset(ctx, result, op.node, &group_runtime, string_arena),
            .sort_op => {
                if (!sorting_mod.applySort(
                    ctx,
                    result,
                    op.node,
                    schema,
                    caps,
                    &group_runtime,
                    string_arena,
                )) return false;
            },
            .inspect_op => {},
            .insert_op, .update_op, .delete_op => {},
        }
    }
    return true;
}

/// Returns true if the operator list contains GROUP or SORT — operators
/// that require the full input set before producing output.
fn hasFullInputOperators(
    ops: *const [max_operators]OpDescriptor,
    op_count: u16,
) bool {
    var i: u16 = 0;
    while (i < op_count) : (i += 1) {
        switch (ops[i].kind) {
            .group_op, .sort_op => return true,
            else => {},
        }
    }
    return false;
}

fn applyNestedSelectionJoin(
    ctx: *const ExecContext,
    result: *QueryResult,
    pipeline_node: NodeIndex,
    source_model_id: ModelId,
    caps: *const capacity_mod.OperatorCapacities,
    string_arena: *scan_mod.StringArena,
) bool {
    const selection = getPipelineSelection(ctx.ast, pipeline_node) orelse
        return true;
    var field = ctx.ast.getNode(selection).data.unary;
    while (field != null_node) {
        const node = ctx.ast.getNode(field);
        if (node.tag == .select_nested) {
            if (!applySingleNestedSelectionJoin(
                ctx,
                result,
                source_model_id,
                field,
                caps,
                string_arena,
            )) return false;
        }
        field = node.next;
    }
    return true;
}

fn applySingleNestedSelectionJoin(
    ctx: *const ExecContext,
    result: *QueryResult,
    source_model_id: ModelId,
    nested: NodeIndex,
    caps: *const capacity_mod.OperatorCapacities,
    string_arena: *scan_mod.StringArena,
) bool {
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

    var right_result = QueryResult.init(ctx.scratch_rows_a);
    defer right_result.deinit();

    const right_scan = scan_mod.tableScanInto(
        ctx.catalog,
        ctx.pool,
        ctx.undo_log,
        ctx.snapshot,
        ctx.tx_manager,
        target_model_id,
        right_result.rows[0..scan_mod.scan_batch_size],
        string_arena,
        null,
    ) catch |err| {
        setBoundaryError(
            result,
            "nested relation scan failed",
            runtime_errors.classifyScan(err),
            err,
        );
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
        string_arena,
    )) {
        if (right_result.getError()) |msg| setError(result, msg);
        return false;
    }

    std.debug.assert(ctx.scratch_rows_b.len >= scan_mod.scan_batch_size);
    const left_copy = ctx.scratch_rows_b;
    @memcpy(left_copy[0..result.row_count], result.rows[0..result.row_count]);

    const join = inferAssociationJoinDescriptor(
        ctx.catalog,
        source_model_id,
        assoc,
        result,
    ) orelse return false;
    recordNestedJoinPlan(&result.stats.plan);
    if (!joins_mod.executeLeftJoinBounded(
        result,
        left_copy[0..result.row_count],
        right_result.rows[0..right_result.row_count],
        join,
        ctx.catalog.models[target_model_id].row_schema.column_count,
        caps,
    )) {
        return false;
    }
    result.stats.pages_read += right_result.stats.pages_read;
    return true;
}

fn inferAssociationJoinDescriptor(
    catalog: *const Catalog,
    source_model_id: ModelId,
    assoc: *const AssociationInfo,
    result: *QueryResult,
) ?joins_mod.JoinDescriptor {
    _ = catalog;
    _ = source_model_id;
    if (assoc.local_column_id == catalog_mod.null_column) {
        setError(result, "association local key not configured");
        return null;
    }
    if (assoc.foreign_key_column_id == catalog_mod.null_column) {
        setError(result, "association foreign key not configured");
        return null;
    }
    return .{
        .left_key_index = assoc.local_column_id,
        .right_key_index = assoc.foreign_key_column_id,
    };
}

/// Filter rows in-place using a where predicate.
fn applyWhereFilter(
    ctx: *const ExecContext,
    result: *QueryResult,
    where_node: NodeIndex,
    schema: *const RowSchema,
    group_runtime: *GroupRuntime,
    string_arena: *scan_mod.StringArena,
) void {
    const node = ctx.ast.getNode(where_node);
    const predicate = node.data.unary;
    if (predicate == null_node) return;

    const original_count = result.row_count;
    var exec_eval = evalContextForExec(ctx, string_arena);
    var write_idx: u16 = 0;
    var read_idx: u16 = 0;
    while (read_idx < result.row_count) : (read_idx += 1) {
        const row = &result.rows[read_idx];
        const matches = if (group_runtime.active)
            aggregation_mod.evaluateGroupedPredicate(
                ctx,
                group_runtime,
                predicate,
                row.values[0..row.column_count],
                schema,
                read_idx,
                &exec_eval.eval_ctx,
            ) catch |err| switch (err) {
                error.UndefinedParameter => {
                    setError(result, "undefined parameter in where predicate");
                    return;
                },
                error.ClockUnavailable => {
                    setError(result, "clock unavailable in where predicate");
                    return;
                },
                else => false,
            }
        else
            filter_mod.evaluatePredicateFull(
                ctx.ast,
                ctx.tokens,
                ctx.source,
                predicate,
                row.values[0..row.column_count],
                schema,
                null,
                &exec_eval.eval_ctx,
            ) catch |err| switch (err) {
                error.UndefinedParameter => {
                    setError(result, "undefined parameter in where predicate");
                    return;
                },
                error.ClockUnavailable => {
                    setError(result, "clock unavailable in where predicate");
                    return;
                },
                else => false,
            };

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

/// Bundles a ParameterResolver and EvalContext derived from an ExecContext.
///
/// Both values live on the caller's stack frame. The EvalContext's
/// parameter_resolver pointer targets the co-located resolver, so the
/// returned struct must not be moved after construction.
pub const ExecEvalContext = struct {
    parameter_resolver: filter_mod.ParameterResolver,
    eval_ctx: filter_mod.EvalContext,
};

pub fn evalContextForExec(
    ctx: *const ExecContext,
    string_arena: ?*scan_mod.StringArena,
) ExecEvalContext {
    var result: ExecEvalContext = .{
        .parameter_resolver = .{
            .ctx = ctx,
            .resolve = resolveParameterBinding,
        },
        .eval_ctx = .{
            .statement_timestamp_micros = ctx.statement_timestamp_micros,
            .string_arena = string_arena,
        },
    };
    result.eval_ctx.parameter_resolver = &result.parameter_resolver;
    return result;
}

fn parameterResolverForContext(
    ctx: *const ExecContext,
) filter_mod.ParameterResolver {
    return .{
        .ctx = ctx,
        .resolve = resolveParameterBinding,
    };
}

fn resolveParameterBinding(
    raw_ctx: *const anyopaque,
    tokens: *const TokenizeResult,
    source: []const u8,
    token_index: u16,
) filter_mod.EvalError!Value {
    const ctx: *const ExecContext = @ptrCast(@alignCast(raw_ctx));
    const parameter_name = tokens.getText(token_index, source);
    for (ctx.parameter_bindings) |binding| {
        if (std.mem.eql(u8, binding.name, parameter_name)) {
            return binding.value;
        }
    }
    return error.UndefinedParameter;
}

/// Truncate result to limit rows.
fn applyLimit(
    ctx: *const ExecContext,
    result: *QueryResult,
    limit_node: NodeIndex,
    string_arena: *scan_mod.StringArena,
) void {
    const node = ctx.ast.getNode(limit_node);
    const expr = node.data.unary;
    if (expr == null_node) return;

    var exec_eval = evalContextForExec(ctx, string_arena);
    const val = filter_mod.evaluateExpressionFull(
        ctx.ast,
        ctx.tokens,
        ctx.source,
        expr,
        &.{},
        &RowSchema{},
        null,
        &exec_eval.eval_ctx,
    ) catch return;

    const limit = numericToRowCount(val) orelse return;

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
    string_arena: *scan_mod.StringArena,
) void {
    const node = ctx.ast.getNode(offset_node);
    const expr = node.data.unary;
    if (expr == null_node) return;

    var exec_eval = evalContextForExec(ctx, string_arena);
    const val = filter_mod.evaluateExpressionFull(
        ctx.ast,
        ctx.tokens,
        ctx.source,
        expr,
        &.{},
        &RowSchema{},
        null,
        &exec_eval.eval_ctx,
    ) catch return;

    const offset = numericToRowCount(val) orelse return;

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



fn numericToRowCount(value: Value) ?u16 {
    return switch (value) {
        .i8 => |v| if (v >= 0) @intCast(@min(v, scan_mod.scan_batch_size)) else 0,
        .i16 => |v| if (v >= 0) @intCast(@min(v, scan_mod.scan_batch_size)) else 0,
        .i32 => |v| if (v >= 0) @intCast(@min(v, scan_mod.scan_batch_size)) else 0,
        .i64 => |v| if (v >= 0) @intCast(@min(v, scan_mod.scan_batch_size)) else 0,
        .u8 => |v| @intCast(@min(v, scan_mod.scan_batch_size)),
        .u16 => |v| @intCast(@min(v, scan_mod.scan_batch_size)),
        .u32 => |v| @intCast(@min(v, scan_mod.scan_batch_size)),
        .u64 => |v| @intCast(@min(v, scan_mod.scan_batch_size)),
        else => null,
    };
}

/// Execute a mutation pipeline (insert, update, or delete).
fn executeMutation(
    ctx: *const ExecContext,
    result: *QueryResult,
    pipeline_node: NodeIndex,
    model_id: ModelId,
    ops: *const [max_operators]OpDescriptor,
    op_count: u16,
    mut_idx: u16,
    string_arena: *scan_mod.StringArena,
) void {
    const mut_op = ops[mut_idx];
    const has_projection = hasNonEmptySelectionSet(ctx.ast, pipeline_node);
    var diagnostic = mutation_mod.MutationDiagnostic{};
    var exec_eval = evalContextForExec(ctx, string_arena);

    switch (mut_op.kind) {
        .insert_op => {
            const node = ctx.ast.getNode(mut_op.node);
            const row_id = mutation_mod.executeInsertWithDiagnosticAndParameters(
                ctx.catalog,
                ctx.pool,
                ctx.wal,
                ctx.tx_id,
                model_id,
                ctx.ast,
                ctx.tokens,
                ctx.source,
                node.data.unary,
                ctx.parameter_bindings,
                &diagnostic,
                &exec_eval.eval_ctx,
            ) catch |err| {
                setMutationBoundaryError(result, ctx, .insert_op, err, &diagnostic);
                return;
            };
            result.stats.rows_inserted = 1;
            if (has_projection) {
                string_arena.reset();
                materializeRowsById(
                    ctx,
                    result,
                    model_id,
                    &[_]RowId{row_id},
                    string_arena,
                );
            }
        },
        .update_op => {
            const predicate = findPredicate(ctx.ast, ops, op_count);
            var returning_capture: mutation_mod.ReturningCapture = undefined;
            const capture: ?*mutation_mod.ReturningCapture = if (has_projection) blk: {
                string_arena.reset();
                result.row_count = 0;
                returning_capture = .{
                    .rows = result.rows[0..scan_mod.scan_batch_size],
                    .row_count = &result.row_count,
                    .string_arena = string_arena,
                };
                break :blk &returning_capture;
            } else null;

            const node = ctx.ast.getNode(mut_op.node);
            const count = mutation_mod.executeUpdateWithDiagnosticAndReturningAndParameters(
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
                ctx.parameter_bindings,
                capture,
                &diagnostic,
                &exec_eval.eval_ctx,
            ) catch |err| {
                setMutationBoundaryError(result, ctx, .update_op, err, &diagnostic);
                return;
            };
            result.stats.rows_updated = count;
        },
        .delete_op => {
            const predicate = findPredicate(ctx.ast, ops, op_count);
            var returning_capture: mutation_mod.ReturningCapture = undefined;
            const capture: ?*mutation_mod.ReturningCapture = if (has_projection) blk: {
                string_arena.reset();
                result.row_count = 0;
                returning_capture = .{
                    .rows = result.rows[0..scan_mod.scan_batch_size],
                    .row_count = &result.row_count,
                    .string_arena = string_arena,
                };
                break :blk &returning_capture;
            } else null;
            const count = mutation_mod.executeDeleteWithReturningAndParameters(
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
                ctx.parameter_bindings,
                capture,
                &exec_eval.eval_ctx,
            ) catch |err| {
                setBoundaryError(
                    result,
                    "delete failed",
                    runtime_errors.classifyMutation(err),
                    err,
                );
                return;
            };
            result.stats.rows_deleted = count;
        },
        else => {
            setError(result, "unexpected mutation type");
        },
    }

    if (has_projection and !result.has_error) {
        _ = applyFlatColumnProjection(ctx, result, pipeline_node, model_id, string_arena);
    }
}

fn hasNonEmptySelectionSet(tree: *const Ast, pipeline_node: NodeIndex) bool {
    const selection = getPipelineSelection(tree, pipeline_node) orelse return false;
    return tree.getNode(selection).data.unary != null_node;
}

fn materializeRowsMatchingPredicate(
    ctx: *const ExecContext,
    out: *QueryResult,
    model_id: ModelId,
    predicate_node: NodeIndex,
    string_arena: *scan_mod.StringArena,
) bool {
    const model = &ctx.catalog.models[model_id];
    const scan_result = scan_mod.tableScanInto(
        ctx.catalog,
        ctx.pool,
        ctx.undo_log,
        ctx.snapshot,
        ctx.tx_manager,
        model_id,
        out.rows[0..scan_mod.scan_batch_size],
        string_arena,
        null,
    ) catch |err| {
        setBoundaryError(
            out,
            "table scan failed",
            runtime_errors.classifyScan(err),
            err,
        );
        return false;
    };
    out.row_count = scan_result.row_count;

    if (predicate_node != null_node) {
        const schema = &model.row_schema;
        var exec_eval = evalContextForExec(ctx, string_arena);
        const original_count = out.row_count;
        var write_idx: u16 = 0;
        var read_idx: u16 = 0;
        while (read_idx < out.row_count) : (read_idx += 1) {
            const row = &out.rows[read_idx];
            const matches = filter_mod.evaluatePredicateFull(
                ctx.ast,
                ctx.tokens,
                ctx.source,
                predicate_node,
                row.values[0..row.column_count],
                schema,
                null,
                &exec_eval.eval_ctx,
            ) catch |err| switch (err) {
                error.UndefinedParameter => {
                    setError(out, "undefined parameter in predicate");
                    return false;
                },
                error.ClockUnavailable => {
                    setError(out, "clock unavailable in predicate");
                    return false;
                },
                else => false,
            };

            if (matches) {
                if (write_idx != read_idx) out.rows[write_idx] = out.rows[read_idx];
                write_idx += 1;
            }
        }
        out.row_count = write_idx;
        std.debug.assert(out.row_count <= original_count);
    }
    return true;
}

fn materializeRowsById(
    ctx: *const ExecContext,
    out: *QueryResult,
    model_id: ModelId,
    row_ids: []const RowId,
    string_arena: *scan_mod.StringArena,
) void {
    var scanned_rows = QueryResult.init(ctx.scratch_rows_b);
    if (!materializeRowsMatchingPredicate(
        ctx,
        &scanned_rows,
        model_id,
        null_node,
        string_arena,
    )) {
        out.has_error = scanned_rows.has_error;
        out.error_message = scanned_rows.error_message;
        out.row_count = 0;
        return;
    }

    var write_idx: u16 = 0;
    for (row_ids) |row_id| {
        var read_idx: u16 = 0;
        while (read_idx < scanned_rows.row_count) : (read_idx += 1) {
            const candidate = scanned_rows.rows[read_idx];
            if (candidate.row_id.page_id != row_id.page_id) continue;
            if (candidate.row_id.slot != row_id.slot) continue;
            out.rows[write_idx] = candidate;
            write_idx += 1;
            break;
        }
    }
    out.row_count = write_idx;
}

fn setMutationBoundaryError(
    result: *QueryResult,
    ctx: *const ExecContext,
    op_kind: PlanOp,
    err: mutation_mod.MutationError,
    diagnostic: *const mutation_mod.MutationDiagnostic,
) void {
    if (!diagnostic.has_value) {
        const summary = switch (op_kind) {
            .insert_op => "insert failed",
            .update_op => "update failed",
            .delete_op => "delete failed",
            else => "mutation failed",
        };
        setBoundaryError(
            result,
            summary,
            runtime_errors.classifyMutation(err),
            err,
        );
        return;
    }

    const field_name: []const u8 = if (diagnostic.field_token) |field_tok|
        ctx.tokens.getText(field_tok, ctx.source)
    else
        "field";
    const path_prefix: []const u8 = switch (op_kind) {
        .insert_op => "insert",
        .update_op => "update",
        .delete_op => "delete",
        else => "mutation",
    };

    const token_idx = diagnostic.location_token orelse diagnostic.field_token orelse 0;
    const line: u16 = if (token_idx < ctx.tokens.count) ctx.tokens.tokens[token_idx].line else 1;
    const col: u16 = tokenColumn(ctx.tokens, ctx.source, token_idx);
    const code_name = @tagName(diagnostic.code);
    result.has_error = true;
    @memset(&result.error_message, 0);
    _ = std.fmt.bufPrint(
        result.error_message[0..],
        "phase=mutation code={s} path={s}.{s} line={d} col={d} message=\"{s}\"",
        .{
            code_name,
            path_prefix,
            field_name,
            line,
            col,
            diagnostic.messageSlice(),
        },
    ) catch setBoundaryError(
        result,
        "mutation failed",
        runtime_errors.classifyMutation(err),
        err,
    );
}

fn tokenColumn(tokens: *const TokenizeResult, source: []const u8, token_idx: u16) u16 {
    if (token_idx >= tokens.count) return 1;
    const tok = tokens.tokens[token_idx];
    const start: usize = @intCast(tok.start);
    var line_start = start;
    while (line_start > 0 and source[line_start - 1] != '\n') {
        line_start -= 1;
    }
    const col = start - line_start + 1;
    return @intCast(@min(col, std.math.maxInt(u16)));
}

fn findFirstStatement(tree: *const Ast) ?NodeIndex {
    if (tree.root == null_node) return null;
    const root = tree.getNode(tree.root);
    if (root.tag != .root) return null;
    const first_stmt = root.data.unary;
    if (first_stmt == null_node) return null;
    return first_stmt;
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
            .op_having => .having_filter,
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

fn capturePlanStats(
    plan: *PlanStats,
    model_name: []const u8,
    ops: *const [max_operators]OpDescriptor,
    op_count: u16,
) void {
    @memset(plan.source_model[0..], 0);
    const source_len = @min(model_name.len, plan.source_model.len);
    @memcpy(plan.source_model[0..source_len], model_name[0..source_len]);
    plan.source_model_len = @intCast(source_len);

    plan.pipeline_op_count = @intCast(@min(
        @as(u16, @intCast(max_operators)),
        op_count,
    ));
    var i: u16 = 0;
    while (i < plan.pipeline_op_count) : (i += 1) {
        plan.pipeline_ops[i] = ops[i].kind;
    }
}

fn recordNestedJoinPlan(plan: *PlanStats) void {
    if (plan.nested_relation_count < std.math.maxInt(u8)) {
        plan.nested_relation_count += 1;
    }
    plan.join_strategy = .nested_loop;
    plan.join_order = .source_then_nested;
    plan.materialization_mode = .bounded_row_buffers;
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

pub fn setError(result: *QueryResult, msg: []const u8) void {
    result.has_error = true;
    @memset(&result.error_message, 0);
    const copy_len = @min(msg.len, result.error_message.len);
    @memcpy(result.error_message[0..copy_len], msg[0..copy_len]);
}

fn setBoundaryError(
    result: *QueryResult,
    summary: []const u8,
    class: runtime_errors.ErrorClass,
    err: anyerror,
) void {
    result.has_error = true;
    @memset(&result.error_message, 0);
    _ = std.fmt.bufPrint(
        result.error_message[0..],
        "{s}; class={s}; code={s}",
        .{ summary, @tagName(class), @errorName(err) },
    ) catch {
        setError(result, summary);
    };
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
    result_rows: []ResultRow,
    scratch_rows_a: []ResultRow,
    scratch_rows_b: []ResultRow,
    string_arena_bytes: []u8,
    collector: SpillingResultCollector,

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
        self.result_rows = try testing.allocator.alloc(
            ResultRow,
            scan_mod.scan_batch_size,
        );
        errdefer testing.allocator.free(self.result_rows);
        self.scratch_rows_a = try testing.allocator.alloc(
            ResultRow,
            scan_mod.scan_batch_size,
        );
        errdefer testing.allocator.free(self.scratch_rows_a);
        self.scratch_rows_b = try testing.allocator.alloc(
            ResultRow,
            scan_mod.scan_batch_size,
        );
        errdefer testing.allocator.free(self.scratch_rows_b);
        self.string_arena_bytes = try testing.allocator.alloc(
            u8,
            scan_mod.default_string_arena_bytes,
        );
        errdefer testing.allocator.free(self.string_arena_bytes);
        // Collector is initialized lazily per-query by executeReadPipeline;
        // just zero-init here so the pointer is stable.
        self.collector = undefined;

        self.catalog = Catalog{};
        self.model_id = try self.catalog.addModel("User");
        _ = try self.catalog.addColumn(
            self.model_id,
            "id",
            .i64,
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
            .bool,
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
        testing.allocator.free(self.scratch_rows_b);
        testing.allocator.free(self.scratch_rows_a);
        testing.allocator.free(self.result_rows);
        testing.allocator.free(self.string_arena_bytes);
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
            .statement_timestamp_micros = 0,
            .now_source = null,
            .parameter_bindings = &.{},
            .allocator = testing.allocator,
            .result_rows = self.result_rows,
            .scratch_rows_a = self.scratch_rows_a,
            .scratch_rows_b = self.scratch_rows_b,
            .string_arena_bytes = self.string_arena_bytes,
            .storage = self.disk.storage(),
            .query_slot_index = 0,
            .collector = &self.collector,
            .work_memory_bytes_per_slot = 4 * 1024 * 1024,
        };
    }
};

fn makeJoinLeftRow(id: i64, name: []const u8) ResultRow {
    var row = ResultRow.init();
    row.column_count = 2;
    row.values[0] = .{ .i64 = id };
    row.values[1] = .{ .string = name };
    return row;
}

fn makeJoinRightRow(id: i64, active: bool) ResultRow {
    var row = ResultRow.init();
    row.column_count = 2;
    row.values[0] = .{ .i64 = id };
    row.values[1] = .{ .bool = active };
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
        result.rows[0].values[0].i64,
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

    const src3 = "User |> where(active == true)";
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
        result.rows[0].values[0].i64,
    );
}

test "execute where filter resolves bound parameter expressions" {
    var env: ExecTestEnv = undefined;
    try env.init();
    defer env.deinit();

    const tx = try env.tm.begin();
    var snap = try env.tm.snapshot(tx);
    defer snap.deinit();

    const src1 = "User |> insert(id = 1, name = \"Alice\", active = true)";
    const tok1 = tokenizer_mod.tokenize(src1);
    const p1 = parser_mod.parse(&tok1, src1);
    var r1 = try execute(&env.makeCtx(tx, &snap, &p1.ast, &tok1, src1));
    defer r1.deinit();

    const src2 = "User |> insert(id = 2, name = \"Bob\", active = false)";
    const tok2 = tokenizer_mod.tokenize(src2);
    const p2 = parser_mod.parse(&tok2, src2);
    var r2 = try execute(&env.makeCtx(tx, &snap, &p2.ast, &tok2, src2));
    defer r2.deinit();

    const src3 = "User |> where(id == $target_id)";
    const tok3 = tokenizer_mod.tokenize(src3);
    const p3 = parser_mod.parse(&tok3, src3);
    const bindings = [_]ParameterBinding{
        .{ .name = "$target_id", .value = .{ .i64 = 2 } },
    };
    var ctx = env.makeCtx(tx, &snap, &p3.ast, &tok3, src3);
    ctx.parameter_bindings = bindings[0..];
    var result = try execute(&ctx);
    defer result.deinit();

    try testing.expect(!result.has_error);
    try testing.expectEqual(@as(u16, 1), result.row_count);
    try testing.expectEqual(@as(i64, 2), result.rows[0].values[0].i64);
}

test "execute where filter fails closed on undefined parameter" {
    var env: ExecTestEnv = undefined;
    try env.init();
    defer env.deinit();

    const tx = try env.tm.begin();
    var snap = try env.tm.snapshot(tx);
    defer snap.deinit();

    const src1 = "User |> insert(id = 1, name = \"Alice\", active = true)";
    const tok1 = tokenizer_mod.tokenize(src1);
    const p1 = parser_mod.parse(&tok1, src1);
    var r1 = try execute(&env.makeCtx(tx, &snap, &p1.ast, &tok1, src1));
    defer r1.deinit();

    const src2 = "User |> where(id == $missing)";
    const tok2 = tokenizer_mod.tokenize(src2);
    const p2 = parser_mod.parse(&tok2, src2);
    var result = try execute(&env.makeCtx(tx, &snap, &p2.ast, &tok2, src2));
    defer result.deinit();

    try testing.expect(result.has_error);
    const msg = result.getError().?;
    try testing.expect(std.mem.indexOf(u8, msg, "undefined parameter in where predicate") != null);
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
        result.rows[0].values[0].i64,
    );
}

test "execute captures deterministic inspect plan metadata" {
    var env: ExecTestEnv = undefined;
    try env.init();
    defer env.deinit();

    const tx = try env.tm.begin();
    var snap = try env.tm.snapshot(tx);
    defer snap.deinit();

    const src = "User |> where(active == true) |> sort(id desc) |> inspect";
    const tok = tokenizer_mod.tokenize(src);
    const p = parser_mod.parse(&tok, src);
    var result = try execute(
        &env.makeCtx(tx, &snap, &p.ast, &tok, src),
    );
    defer result.deinit();

    try testing.expect(!result.has_error);
    const source_name =
        result.stats.plan.source_model[0..result.stats.plan.source_model_len];
    try testing.expectEqualStrings("User", source_name);
    try testing.expectEqual(@as(u8, 3), result.stats.plan.pipeline_op_count);
    try testing.expectEqual(
        PlanOp.where_filter,
        result.stats.plan.pipeline_ops[0],
    );
    try testing.expectEqual(
        PlanOp.sort_op,
        result.stats.plan.pipeline_ops[1],
    );
    try testing.expectEqual(
        PlanOp.inspect_op,
        result.stats.plan.pipeline_ops[2],
    );
    try testing.expectEqual(JoinStrategy.none, result.stats.plan.join_strategy);
    try testing.expectEqual(JoinOrder.none, result.stats.plan.join_order);
    try testing.expectEqual(
        MaterializationMode.none,
        result.stats.plan.materialization_mode,
    );
    try testing.expectEqual(
        SortStrategy.in_place_insertion,
        result.stats.plan.sort_strategy,
    );
    try testing.expectEqual(
        GroupStrategy.none,
        result.stats.plan.group_strategy,
    );
    try testing.expectEqual(@as(u8, 0), result.stats.plan.nested_relation_count);
}

test "execute captures group strategy in inspect plan metadata" {
    var env: ExecTestEnv = undefined;
    try env.init();
    defer env.deinit();

    const tx = try env.tm.begin();
    var snap = try env.tm.snapshot(tx);
    defer snap.deinit();

    const src = "User |> group(active) |> inspect";
    const tok = tokenizer_mod.tokenize(src);
    const p = parser_mod.parse(&tok, src);
    var result = try execute(
        &env.makeCtx(tx, &snap, &p.ast, &tok, src),
    );
    defer result.deinit();

    try testing.expect(!result.has_error);
    try testing.expectEqual(
        SortStrategy.none,
        result.stats.plan.sort_strategy,
    );
    try testing.expectEqual(
        GroupStrategy.in_memory_linear,
        result.stats.plan.group_strategy,
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
    _ = try env.catalog.addColumn(post_model, "id", .i64, false);
    _ = try env.catalog.addColumn(post_model, "user_id", .i64, false);
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
    try testing.expectEqual(@as(u16, 2), result.rows[0].column_count);
    try testing.expectEqual(@as(i64, 1), result.rows[0].values[0].i64);
    try testing.expectEqual(@as(i64, 10), result.rows[0].values[1].i64);
    try testing.expectEqual(@as(i64, 1), result.rows[1].values[0].i64);
    try testing.expectEqual(@as(i64, 20), result.rows[1].values[1].i64);
    try testing.expectEqual(@as(i64, 2), result.rows[2].values[0].i64);
    try testing.expectEqual(@as(i64, 15), result.rows[2].values[1].i64);
    try testing.expectEqual(
        JoinStrategy.nested_loop,
        result.stats.plan.join_strategy,
    );
    try testing.expectEqual(
        JoinOrder.source_then_nested,
        result.stats.plan.join_order,
    );
    try testing.expectEqual(
        MaterializationMode.bounded_row_buffers,
        result.stats.plan.materialization_mode,
    );
    try testing.expectEqual(@as(u8, 1), result.stats.plan.nested_relation_count);
}

test "execute multiple nested relations through selection set" {
    var env: ExecTestEnv = undefined;
    try env.init();
    defer env.deinit();

    const post_model = try env.catalog.addModel("Post");
    _ = try env.catalog.addColumn(post_model, "id", .i64, false);
    _ = try env.catalog.addColumn(post_model, "user_id", .i64, false);
    env.catalog.models[post_model].heap_first_page_id = 120;
    env.catalog.models[post_model].total_pages = 1;

    const comment_model = try env.catalog.addModel("Comment");
    _ = try env.catalog.addColumn(comment_model, "id", .i64, false);
    _ = try env.catalog.addColumn(comment_model, "user_id", .i64, false);
    env.catalog.models[comment_model].heap_first_page_id = 121;
    env.catalog.models[comment_model].total_pages = 1;

    _ = try env.catalog.addAssociation(
        env.model_id,
        "posts",
        AssociationKind.has_many,
        "Post",
    );
    _ = try env.catalog.addAssociation(
        env.model_id,
        "comments",
        AssociationKind.has_many,
        "Comment",
    );
    try env.catalog.resolveAssociations();

    const post_page = try env.pool.pin(120);
    heap_mod.HeapPage.init(post_page);
    env.pool.unpin(120, true);

    const comment_page = try env.pool.pin(121);
    heap_mod.HeapPage.init(comment_page);
    env.pool.unpin(121, true);

    const tx = try env.tm.begin();
    var snap = try env.tm.snapshot(tx);
    defer snap.deinit();

    const inserts = [_][]const u8{
        "User |> insert(id = 1, name = \"Alice\", active = true)",
        "User |> insert(id = 2, name = \"Bob\", active = true)",
        "Post |> insert(id = 20, user_id = 1)",
        "Post |> insert(id = 10, user_id = 1)",
        "Post |> insert(id = 15, user_id = 2)",
        "Comment |> insert(id = 200, user_id = 1)",
        "Comment |> insert(id = 100, user_id = 1)",
        "Comment |> insert(id = 150, user_id = 2)",
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
        "User |> sort(id asc) { id posts |> sort(id asc) { id } comments |> sort(id asc) { id } }";
    const tok = tokenizer_mod.tokenize(src);
    const p = parser_mod.parse(&tok, src);
    try testing.expect(!p.has_error);
    var result = try execute(&env.makeCtx(tx, &snap, &p.ast, &tok, src));
    defer result.deinit();

    try testing.expect(!result.has_error);
    try testing.expectEqual(@as(u16, 5), result.row_count);
    try testing.expectEqual(@as(u16, 3), result.rows[0].column_count);
    try testing.expectEqual(@as(i64, 1), result.rows[0].values[0].i64);
    try testing.expectEqual(@as(i64, 10), result.rows[0].values[1].i64);
    try testing.expectEqual(@as(i64, 100), result.rows[0].values[2].i64);
    try testing.expectEqual(@as(i64, 1), result.rows[3].values[0].i64);
    try testing.expectEqual(@as(i64, 20), result.rows[3].values[1].i64);
    try testing.expectEqual(@as(i64, 200), result.rows[3].values[2].i64);
    try testing.expectEqual(@as(i64, 2), result.rows[4].values[0].i64);
    try testing.expectEqual(@as(i64, 15), result.rows[4].values[1].i64);
    try testing.expectEqual(@as(i64, 150), result.rows[4].values[2].i64);
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

test "execute nested relation uses explicit association keys" {
    var env: ExecTestEnv = undefined;
    try env.init();
    defer env.deinit();

    const post_model = try env.catalog.addModel("Post");
    _ = try env.catalog.addColumn(post_model, "id", .i64, false);
    _ = try env.catalog.addColumn(post_model, "owner_user", .i64, false);
    env.catalog.models[post_model].heap_first_page_id = 122;
    env.catalog.models[post_model].total_pages = 1;

    const assoc_id = try env.catalog.addAssociation(
        env.model_id,
        "posts",
        AssociationKind.has_many,
        "Post",
    );
    try env.catalog.setAssociationKeys(
        env.model_id,
        assoc_id,
        "id",
        "owner_user",
    );
    try env.catalog.resolveAssociations();

    const post_page = try env.pool.pin(122);
    heap_mod.HeapPage.init(post_page);
    env.pool.unpin(122, true);

    const tx = try env.tm.begin();
    var snap = try env.tm.snapshot(tx);
    defer snap.deinit();

    const inserts = [_][]const u8{
        "User |> insert(id = 1, name = \"Alice\", active = true)",
        "User |> insert(id = 2, name = \"Bob\", active = true)",
        "Post |> insert(id = 20, owner_user = 1)",
        "Post |> insert(id = 15, owner_user = 2)",
    };
    for (inserts) |src| {
        const tok = tokenizer_mod.tokenize(src);
        const p = parser_mod.parse(&tok, src);
        try testing.expect(!p.has_error);
        var r = try execute(&env.makeCtx(tx, &snap, &p.ast, &tok, src));
        defer r.deinit();
        try testing.expect(!r.has_error);
    }

    const src = "User |> sort(id asc) { id posts |> sort(id asc) { id } }";
    const tok = tokenizer_mod.tokenize(src);
    const p = parser_mod.parse(&tok, src);
    try testing.expect(!p.has_error);
    var result = try execute(&env.makeCtx(tx, &snap, &p.ast, &tok, src));
    defer result.deinit();

    try testing.expect(!result.has_error);
    try testing.expectEqual(@as(u16, 2), result.row_count);
    try testing.expectEqual(@as(u16, 2), result.rows[0].column_count);
    try testing.expectEqual(@as(i64, 1), result.rows[0].values[0].i64);
    try testing.expectEqual(@as(i64, 20), result.rows[0].values[1].i64);
    try testing.expectEqual(@as(i64, 2), result.rows[1].values[0].i64);
    try testing.expectEqual(@as(i64, 15), result.rows[1].values[1].i64);
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

    const src2 = "User |> where(id == 1) |> delete";
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
    try testing.expectEqual(@as(i64, 2), asc_result.rows[0].values[0].i64);
    try testing.expectEqual(@as(i64, 1), asc_result.rows[1].values[0].i64);
    try testing.expectEqual(@as(i64, 3), asc_result.rows[2].values[0].i64);

    const desc_src = "User |> sort(name desc)";
    const desc_tok = tokenizer_mod.tokenize(desc_src);
    const desc_parsed = parser_mod.parse(&desc_tok, desc_src);
    var desc_result = try execute(
        &env.makeCtx(tx, &snap, &desc_parsed.ast, &desc_tok, desc_src),
    );
    defer desc_result.deinit();
    try testing.expect(!desc_result.has_error);
    try testing.expectEqual(@as(i64, 3), desc_result.rows[0].values[0].i64);
    try testing.expectEqual(@as(i64, 1), desc_result.rows[1].values[0].i64);
    try testing.expectEqual(@as(i64, 2), desc_result.rows[2].values[0].i64);
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
    try testing.expectEqual(@as(i64, 3), result.rows[0].values[0].i64);
    try testing.expectEqual(@as(i64, 1), result.rows[1].values[0].i64);
    try testing.expectEqual(@as(i64, 2), result.rows[2].values[0].i64);
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
    try testing.expectEqual(false, result.rows[0].values[2].bool);
    try testing.expectEqual(true, result.rows[1].values[2].bool);
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
    try testing.expectEqual(true, result.rows[0].values[2].bool);
    try testing.expectEqual(false, result.rows[1].values[2].bool);
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
    try testing.expectEqual(true, result.rows[0].values[2].bool);
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
        "User |> group(active) |> where(max(id) > 1 && min(id) >= 1) |> sort(sum(id) asc, avg(id) asc)";
    const tok = tokenizer_mod.tokenize(src);
    const p = parser_mod.parse(&tok, src);
    var result = try execute(&env.makeCtx(tx, &snap, &p.ast, &tok, src));
    defer result.deinit();

    try testing.expect(!result.has_error);
    try testing.expectEqual(@as(u16, 2), result.row_count);
    try testing.expectEqual(true, result.rows[0].values[2].bool);
    try testing.expectEqual(false, result.rows[1].values[2].bool);
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
        "User |> group(active) |> where(sum(id) > 0 && avg(id) > 0 && min(id) > 0 && max(id) > 0 && sum(id + 1) > 0)";
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

    var result_rows: [scan_mod.scan_batch_size]ResultRow = undefined;
    var result = QueryResult.init(result_rows[0..]);
    defer result.deinit();
    const caps = capacity_mod.OperatorCapacities.defaults();
    const ok = joins_mod.executeInnerJoinBounded(
        &result,
        left[0..],
        right[0..],
        .{ .left_key_index = 0, .right_key_index = 0 },
        &caps,
    );

    try testing.expect(ok);
    try testing.expect(!result.has_error);
    try testing.expectEqual(@as(u16, 5), result.row_count);
    try testing.expectEqual(@as(i64, 1), result.rows[0].values[0].i64);
    try testing.expectEqualSlices(u8, "A", result.rows[0].values[1].string);
    try testing.expectEqual(true, result.rows[0].values[3].bool);
    try testing.expectEqual(@as(i64, 1), result.rows[1].values[0].i64);
    try testing.expectEqualSlices(u8, "A", result.rows[1].values[1].string);
    try testing.expectEqual(false, result.rows[1].values[3].bool);
    try testing.expectEqual(@as(i64, 2), result.rows[2].values[0].i64);
    try testing.expectEqualSlices(u8, "B", result.rows[2].values[1].string);
    try testing.expectEqual(true, result.rows[2].values[3].bool);
    try testing.expectEqual(@as(i64, 1), result.rows[3].values[0].i64);
    try testing.expectEqualSlices(u8, "C", result.rows[3].values[1].string);
    try testing.expectEqual(true, result.rows[3].values[3].bool);
    try testing.expectEqual(@as(i64, 1), result.rows[4].values[0].i64);
    try testing.expectEqualSlices(u8, "C", result.rows[4].values[1].string);
    try testing.expectEqual(false, result.rows[4].values[3].bool);
}

test "bounded inner join enforces build row capacity contract" {
    const left = [_]ResultRow{
        makeJoinLeftRow(1, "A"),
        makeJoinLeftRow(2, "B"),
    };
    const right = [_]ResultRow{
        makeJoinRightRow(1, true),
    };

    var result_rows: [scan_mod.scan_batch_size]ResultRow = undefined;
    var result = QueryResult.init(result_rows[0..]);
    defer result.deinit();
    var caps = capacity_mod.OperatorCapacities.defaults();
    caps.join_build_rows = 1;
    const ok = joins_mod.executeInnerJoinBounded(
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

    var result_rows: [scan_mod.scan_batch_size]ResultRow = undefined;
    var result = QueryResult.init(result_rows[0..]);
    defer result.deinit();
    var caps = capacity_mod.OperatorCapacities.defaults();
    caps.join_output_rows = 3;
    const ok = joins_mod.executeInnerJoinBounded(
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

    var result_rows: [scan_mod.scan_batch_size]ResultRow = undefined;
    var result = QueryResult.init(result_rows[0..]);
    defer result.deinit();
    var caps = capacity_mod.OperatorCapacities.defaults();
    caps.join_state_bytes = @sizeOf(Value);
    const ok = joins_mod.executeInnerJoinBounded(
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
