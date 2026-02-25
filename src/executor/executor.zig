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
const hash_join_mod = @import("hash_join.zig");
const projections_mod = @import("projections.zig");
const spill_collector_mod = @import("spill_collector.zig");
const external_sort_mod = @import("external_sort.zig");
const hash_aggregate_mod = @import("hash_aggregate.zig");
const spill_row_mod = @import("../storage/spill_row.zig");
const index_scan_planner = @import("index_scan_planner.zig");
const index_maintenance_mod = @import("index_maintenance.zig");
const index_key_mod = @import("../storage/index_key.zig");
const btree_mod = @import("../storage/btree.zig");
const temp_mod = @import("../storage/temp.zig");

const SpillingResultCollector = spill_collector_mod.SpillingResultCollector;
const SpillPageWriter = spill_row_mod.SpillPageWriter;
const TempStorageManager = temp_mod.TempStorageManager;
const max_spill_pages = spill_collector_mod.max_spill_pages;

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
const LeftHashIndex = hash_join_mod.LeftHashIndex;

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
    hash_in_memory,
    hash_spill,
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
    in_memory_merge,
    external_merge,
};

pub const GroupStrategy = enum {
    none,
    in_memory_linear,
    hash_spill,
};

pub const ScanStrategy = index_scan_planner.ScanStrategy;

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
    scan_strategy: ScanStrategy = .table_scan,
    nested_relation_count: u8 = 0,
    nested_join_nested_loop_count: u8 = 0,
    nested_join_hash_in_memory_count: u8 = 0,
    nested_join_hash_spill_count: u8 = 0,
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
    /// Collector-backed output window. Used only when `collector != null`.
    collector_output_offset: u64 = 0,
    collector_output_count: u64 = 0,

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

pub const SpillRowSet = struct {
    collector: *SpillingResultCollector,
    offset: u64,
    count: u64,
};

/// Output row-set contract for post-scan operator chaining.
/// `flat` carries in-memory row count in `result.rows`.
/// `spill` carries a collector stream view plus visible window.
pub const RowSet = union(enum) {
    flat: u16,
    spill: SpillRowSet,
};

pub const NestedSpillRowSet = struct {
    collector: *SpillingResultCollector,
    offset: u64,
    count: u64,
};

pub const NestedFlatRowSet = struct {
    rows: []ResultRow,
    count: u16,
};

/// Parent-local nested operator row source/output descriptor.
///
/// This is intentionally separate from root `RowSet` so nested spill
/// contracts remain parent-scoped and cannot accidentally inherit root-level
/// semantics.
pub const NestedRowSet = union(enum) {
    flat: NestedFlatRowSet,
    spill: NestedSpillRowSet,
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
    nested_rows: []ResultRow,
    string_arena_bytes: []u8,
    nested_decode_arena_bytes: []u8,
    nested_match_arena_bytes: []u8,
    storage: Storage,
    query_slot_index: u16,
    collector: *SpillingResultCollector,
    temp_pages_per_query_slot: u64 = temp_mod.default_pages_per_query_slot,
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
    result.collector = null;
    result.collector_output_offset = 0;
    result.collector_output_count = 0;
    @memset(result.error_message[0..], 0);
}

fn applyRowSetToResult(result: *QueryResult, row_set: RowSet) void {
    switch (row_set) {
        .flat => |count| {
            result.row_count = count;
            result.collector = null;
            result.collector_output_offset = 0;
            result.collector_output_count = 0;
        },
        .spill => |spill| {
            result.row_count = @intCast(@min(
                collectorWindowCount(
                    spill.collector.totalRowCount(),
                    spill.offset,
                    spill.count,
                ),
                scan_mod.scan_batch_size,
            ));
            result.collector = spill.collector;
            result.collector_output_offset = spill.offset;
            result.collector_output_count = spill.count;
        },
    }
}

fn rowSetVisibleCount(row_set: RowSet) u64 {
    return switch (row_set) {
        .flat => |count| count,
        .spill => |spill| collectorWindowCount(
            spill.collector.totalRowCount(),
            spill.offset,
            spill.count,
        ),
    };
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
    const temp_mgr = TempStorageManager.init(
        ctx.query_slot_index,
        ctx.storage,
        ctx.temp_pages_per_query_slot,
        temp_mod.default_region_start_page_id,
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

    // --- 2. Try PK index scan before falling back to full table scan ---
    // If the index scan succeeds, it populates the collector and stats
    // directly, then we skip the chunked scan loop and proceed to post-scan.
    var total_pages_read: u32 = 0;
    var total_rows_scanned: u32 = 0;
    const index_scan_used = blk: {
        const where_node = findWhereOpNode(ops, op_count);
        if (where_node == null) break :blk false;
        break :blk tryIndexScan(
            ctx,
            result,
            model_id,
            where_node.?,
            ops,
            op_count,
            string_arena,
            &total_pages_read,
            &total_rows_scanned,
        );
    };

    if (!index_scan_used) {
        // --- 3. Chunked scan loop ---
        // Scan into scratch_rows_a so the collector's hot batch (result.rows)
        // is never overwritten by scan output.
        const scan_buf = ctx.scratch_rows_a[0..scan_mod.scan_batch_size];
        var chunk_result = QueryResult.init(ctx.scratch_rows_a);

        var cursor = scan_mod.ScanCursor.init();

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
    } // end if (!index_scan_used)

    result.stats.pages_read = total_pages_read;
    result.stats.rows_scanned = total_rows_scanned;

    // --- 4. Post-scan: materialize final result ---
    const spilled = ctx.collector.spillTriggered();
    const needs_full_input = hasFullInputOperators(ops, op_count);
    var output_rows: RowSet = .{ .flat = 0 };

    if (!spilled) {
        // No spill — hot batch rows are already in result.rows[0..hot_count].
        output_rows = .{ .flat = ctx.collector.hot_count };
        applyRowSetToResult(result, output_rows);

        if (!applyPostScanOperators(ctx, result, model_id, ops, op_count, &caps, string_arena)) {
            captureTempStats(result, ctx.collector);
            return;
        }
        if (!applyNestedSelectionJoin(ctx, result, pipeline_node, model_id, &caps, string_arena)) {
            captureTempStats(result, ctx.collector);
            return;
        }
        output_rows = .{ .flat = result.row_count };
    } else if (!needs_full_input) {
        // Spill occurred but no GROUP/SORT needed — let serialization
        // iterate directly from the collector.
        if (hasUnsupportedCollectorBackedPostOps(ops, op_count)) {
            setError(
                result,
                "spill path: collector-backed operator not implemented",
            );
            captureTempStats(result, ctx.collector);
            return;
        }
        output_rows = .{
            .spill = .{
                .collector = ctx.collector,
                .offset = 0,
                .count = ctx.collector.totalRowCount(),
            },
        };
        applyRowSetToResult(result, output_rows);
        if (hasCollectorHavingOp(ops, op_count)) {
            if (!rewriteCollectorForPostOps(
                ctx,
                result,
                model_id,
                ops,
                op_count,
                string_arena,
            )) {
                captureTempStats(result, ctx.collector);
                return;
            }
            output_rows = .{
                .spill = .{
                    .collector = result.collector.?,
                    .offset = 0,
                    .count = result.collector.?.totalRowCount(),
                },
            };
        } else {
            const window = computeCollectorOutputWindow(
                ctx,
                ops,
                op_count,
                string_arena,
                ctx.collector.totalRowCount(),
            );
            output_rows = .{
                .spill = .{
                    .collector = ctx.collector,
                    .offset = window.offset,
                    .count = window.count,
                },
            };
        }
        applyRowSetToResult(result, output_rows);
        if (hasNestedSelection(ctx.ast, pipeline_node)) {
            if (!applyNestedSelectionJoin(ctx, result, pipeline_node, model_id, &caps, string_arena)) {
                captureTempStats(result, ctx.collector);
                return;
            }
            output_rows = if (result.collector) |collector|
                .{
                    .spill = .{
                        .collector = collector,
                        .offset = result.collector_output_offset,
                        .count = result.collector_output_count,
                    },
                }
            else
                .{ .flat = result.row_count };
        }
    } else if (findSortOpNode(ops, op_count) != null and !hasGroupOp(ops, op_count)) {
        // Spill + sort (no GROUP): external merge sort reads from collector
        // directly, avoiding the scan_batch_size reload truncation.
        const sort_node = findSortOpNode(ops, op_count).?;
        const schema = &ctx.catalog.models[model_id].row_schema;
        if (!external_sort_mod.applyExternalSort(ctx, result, ctx.collector, sort_node, schema, string_arena)) {
            captureTempStats(result, ctx.collector);
            return;
        }
        if (result.collector != null) {
            // External sort spilled output back to collector-backed pages.
            // Downstream post-sort operators currently operate on flat rows only.
            if (hasUnsupportedCollectorBackedPostOps(ops, op_count)) {
                setError(
                    result,
                    "spill path: collector-backed operator not implemented",
                );
                captureTempStats(result, ctx.collector);
                return;
            }
            if (hasCollectorHavingOp(ops, op_count)) {
                if (!rewriteCollectorForPostOps(
                    ctx,
                    result,
                    model_id,
                    ops,
                    op_count,
                    string_arena,
                )) {
                    captureTempStats(result, ctx.collector);
                    return;
                }
                output_rows = .{
                    .spill = .{
                        .collector = result.collector.?,
                        .offset = 0,
                        .count = result.collector.?.totalRowCount(),
                    },
                };
            } else {
                const window = computeCollectorOutputWindow(
                    ctx,
                    ops,
                    op_count,
                    string_arena,
                    ctx.collector.totalRowCount(),
                );
                output_rows = .{
                    .spill = .{
                        .collector = result.collector.?,
                        .offset = window.offset,
                        .count = window.count,
                    },
                };
            }
            applyRowSetToResult(result, output_rows);
            if (hasNestedSelection(ctx.ast, pipeline_node)) {
                if (!applyNestedSelectionJoin(ctx, result, pipeline_node, model_id, &caps, string_arena)) {
                    captureTempStats(result, ctx.collector);
                    return;
                }
                output_rows = if (result.collector) |collector|
                    .{
                        .spill = .{
                            .collector = collector,
                            .offset = result.collector_output_offset,
                            .count = result.collector_output_count,
                        },
                    }
                else
                    .{ .flat = result.row_count };
            }
        } else {
            // Apply remaining post-sort operators (HAVING, LIMIT, OFFSET).
            // Sort is done; skip WHERE (already per-chunk) and SORT.
            if (!applyPostExternalSortOperators(ctx, result, model_id, ops, op_count, &caps, string_arena)) {
                captureTempStats(result, ctx.collector);
                return;
            }
            if (!applyNestedSelectionJoin(ctx, result, pipeline_node, model_id, &caps, string_arena)) {
                captureTempStats(result, ctx.collector);
                return;
            }
            output_rows = .{ .flat = result.row_count };
        }
    } else {
        // Spill + GROUP (with or without sort): hash aggregation reads from
        // collector directly, processing all input without truncation.
        const group_info = findGroupOpInfo(ops, op_count).?;
        const schema = &ctx.catalog.models[model_id].row_schema;
        var group_runtime = GroupRuntime{};

        if (!hash_aggregate_mod.applyHashAggregate(
            ctx,
            result,
            ctx.collector,
            group_info.node,
            group_info.index,
            schema,
            ops,
            op_count,
            &caps,
            &group_runtime,
            string_arena,
        )) {
            captureTempStats(result, ctx.collector);
            return;
        }
        // Apply post-hash-aggregate operators (HAVING, SORT, LIMIT, OFFSET).
        if (!applyPostHashAggregateOperators(ctx, result, model_id, ops, op_count, &caps, &group_runtime, string_arena)) {
            captureTempStats(result, ctx.collector);
            return;
        }
        if (!applyNestedSelectionJoin(ctx, result, pipeline_node, model_id, &caps, string_arena)) {
            captureTempStats(result, ctx.collector);
            return;
        }
        output_rows = .{ .flat = result.row_count };
    }

    applyRowSetToResult(result, output_rows);
    const matched_count = rowSetVisibleCount(output_rows);
    result.stats.rows_matched = @intCast(@min(matched_count, std.math.maxInt(u32)));
    if (result.collector) |_| {
        if (!applyCollectorProjection(
            ctx,
            result,
            pipeline_node,
            model_id,
            string_arena,
        )) {
            captureTempStats(result, ctx.collector);
            return;
        }
        output_rows = .{
            .spill = .{
                .collector = result.collector.?,
                .offset = result.collector_output_offset,
                .count = result.collector_output_count,
            },
        };
    } else {
        if (!applyFlatColumnProjection(ctx, result, pipeline_node, model_id, string_arena)) {
            captureTempStats(result, ctx.collector);
            return;
        }
        output_rows = .{ .flat = result.row_count };
    }
    applyRowSetToResult(result, output_rows);
    const returned_count = rowSetVisibleCount(output_rows);
    result.stats.rows_returned = @intCast(@min(returned_count, std.math.maxInt(u32)));
    captureTempStats(result, ctx.collector);
}

/// Apply selection projection to collector-backed rows.
///
/// This rewrites the collector stream into projected rows so serialization
/// emits the same column shape as flat-buffer execution.
fn applyCollectorProjection(
    ctx: *const ExecContext,
    result: *QueryResult,
    pipeline_node: NodeIndex,
    model_id: ModelId,
    string_arena: *scan_mod.StringArena,
) bool {
    const collector = result.collector orelse return true;
    const selection = getPipelineSelection(ctx.ast, pipeline_node) orelse return true;
    const source_schema = &ctx.catalog.models[model_id].row_schema;

    var descriptors: [scan_mod.max_columns]projections_mod.ProjectionDescriptor = undefined;
    var descriptor_count: u16 = 0;
    var has_nested = false;

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
                if (descriptor_count >= scan_mod.max_columns) {
                    setError(result, "projection column capacity exceeded");
                    return false;
                }
                descriptors[descriptor_count] = .{
                    .kind = .column,
                    .column_index = col_idx,
                };
                descriptor_count += 1;
            },
            .select_computed => {
                const expr_node = node.data.unary;
                if (expr_node == null_node) {
                    setError(result, "computed select expression missing");
                    return false;
                }
                if (descriptor_count >= scan_mod.max_columns) {
                    setError(result, "projection column capacity exceeded");
                    return false;
                }
                descriptors[descriptor_count] = .{
                    .kind = .expression,
                    .expr_node = expr_node,
                };
                descriptor_count += 1;
            },
            .select_nested => has_nested = true,
            else => {},
        }
        field = node.next;
    }

    // Nested selection is guarded earlier for collector-backed paths.
    if (has_nested) return true;
    if (descriptor_count == 0) return true;

    var iter = collector.iterator();
    var read_arena_buf: [scan_mod.max_row_size_bytes + 192]u8 = undefined;
    var read_arena = scan_mod.StringArena.init(&read_arena_buf);
    var input_row = scan_mod.ResultRow.init();

    var output_page_ids: [max_spill_pages]u64 = undefined;
    var output_page_count: u32 = 0;
    var output_rows: u64 = 0;
    var writer = SpillPageWriter.init();

    while (true) {
        read_arena.reset();
        const has_row = iter.next(&input_row, &read_arena) catch {
            setError(result, "spill projection read failed");
            return false;
        };
        if (!has_row) break;

        var projected_row = scan_mod.ResultRow.init();
        projected_row.row_id = input_row.row_id;
        projected_row.column_count = descriptor_count;

        for (descriptors[0..descriptor_count], 0..) |descriptor, out_idx| {
            switch (descriptor.kind) {
                .column => {
                    if (descriptor.column_index >= input_row.column_count) {
                        setError(result, "projection column out of bounds");
                        return false;
                    }
                    projected_row.values[out_idx] = input_row.values[descriptor.column_index];
                },
                .expression => {
                    var exec_eval = evalContextForExec(ctx, string_arena);
                    const value = filter_mod.evaluateExpressionFull(
                        ctx.ast,
                        ctx.tokens,
                        ctx.source,
                        descriptor.expr_node,
                        input_row.values[0..input_row.column_count],
                        source_schema,
                        null,
                        &exec_eval.eval_ctx,
                    ) catch {
                        setError(result, "select computed expression evaluation failed");
                        return false;
                    };
                    projected_row.values[out_idx] = value;
                },
            }
        }

        const appended = writer.appendRow(&projected_row) catch {
            setError(result, "spill projection write failed");
            return false;
        };
        if (!appended) {
            if (output_page_count >= max_spill_pages) {
                setError(result, "spill projection temp page budget exhausted");
                return false;
            }
            const payload = writer.finalize();
            const page_id = collector.temp_mgr.allocateAndWrite(payload, temp_mod.TempPage.null_page_id) catch {
                setError(result, "spill projection temp page budget exhausted");
                return false;
            };
            output_page_ids[output_page_count] = page_id;
            output_page_count += 1;
            writer.reset();
            const retried = writer.appendRow(&projected_row) catch {
                setError(result, "spill projection write failed");
                return false;
            };
            std.debug.assert(retried);
        }
        output_rows += 1;
    }

    if (writer.row_count > 0) {
        if (output_page_count >= max_spill_pages) {
            setError(result, "spill projection temp page budget exhausted");
            return false;
        }
        const payload = writer.finalize();
        const page_id = collector.temp_mgr.allocateAndWrite(payload, temp_mod.TempPage.null_page_id) catch {
            setError(result, "spill projection temp page budget exhausted");
            return false;
        };
        output_page_ids[output_page_count] = page_id;
        output_page_count += 1;
    }

    @memcpy(
        collector.spill_page_ids[0..output_page_count],
        output_page_ids[0..output_page_count],
    );
    collector.spill_page_count = output_page_count;
    collector.hot_count = 0;
    collector.hot_bytes = 0;
    collector.total_rows = output_rows;
    collector.iteration_started = false;
    result.collector = collector;

    return true;
}

/// Attempt a PK index scan for the given WHERE clause. Returns true if
/// the index scan was used (rows are in the collector and stats are set).
/// Returns false if the query should fall back to a full table scan.
/// On true, the caller skips the chunked scan loop but still runs
/// the normal post-scan pipeline (projections, joins, etc.).
fn tryIndexScan(
    ctx: *const ExecContext,
    result: *QueryResult,
    model_id: ModelId,
    where_node: NodeIndex,
    ops: *const [max_operators]OpDescriptor,
    op_count: u16,
    string_arena: *scan_mod.StringArena,
    total_pages_read: *u32,
    total_rows_scanned: *u32,
) bool {
    // Check if this model has a PK column with a B+ tree index.
    const pk_col_id = catalog_mod.findPrimaryKeyColumnId(ctx.catalog, model_id) orelse return false;
    var pk_btree = index_maintenance_mod.openPrimaryKeyIndex(
        ctx.catalog,
        ctx.pool,
        ctx.wal,
        model_id,
    ) orelse return false;

    // Get the PK column name for AST matching.
    const schema = &ctx.catalog.models[model_id].row_schema;
    const pk_col_name = schema.getColumnName(pk_col_id);

    // Analyze the WHERE predicate for PK-indexable patterns.
    const ast_node = ctx.ast.getNode(where_node);
    const predicate = ast_node.data.unary;
    if (predicate == null_node) return false;

    const plan = index_scan_planner.analyze(
        ctx.ast,
        ctx.tokens,
        ctx.source,
        predicate,
        pk_col_name,
    );

    switch (plan.strategy) {
        .table_scan => return false,
        .pk_point_lookup => {
            return executePointLookup(
                ctx,
                result,
                model_id,
                &pk_btree,
                plan.eq_value,
                ops,
                op_count,
                string_arena,
                total_pages_read,
                total_rows_scanned,
            );
        },
        .pk_range_scan => {
            return executeRangeScan(
                ctx,
                result,
                model_id,
                &pk_btree,
                plan.loKey(),
                plan.hiKey(),
                ops,
                op_count,
                string_arena,
                total_pages_read,
                total_rows_scanned,
            );
        },
    }
}

/// Execute a PK point lookup: find one row by exact key, apply WHERE, feed to collector.
fn executePointLookup(
    ctx: *const ExecContext,
    result: *QueryResult,
    model_id: ModelId,
    btree: *btree_mod.BTree,
    pk_value: Value,
    ops: *const [max_operators]OpDescriptor,
    op_count: u16,
    string_arena: *scan_mod.StringArena,
    total_pages_read: *u32,
    total_rows_scanned: *u32,
) bool {
    var key_buf: [index_scan_planner.max_key_buf]u8 = undefined;
    const key = index_key_mod.encodeValue(pk_value, &key_buf);

    const row_opt = scan_mod.indexFind(
        ctx.catalog,
        ctx.pool,
        ctx.undo_log,
        ctx.snapshot,
        ctx.tx_manager,
        btree,
        model_id,
        key,
        string_arena,
    ) catch {
        setError(result, "index point lookup failed");
        return true;
    };

    result.stats.plan.scan_strategy = .pk_point_lookup;
    total_pages_read.* = 1;

    if (row_opt) |row| {
        // Put the found row in the scratch buffer for WHERE filtering.
        var chunk_result = QueryResult.init(ctx.scratch_rows_a);
        ctx.scratch_rows_a[0] = row;
        chunk_result.row_count = 1;
        total_rows_scanned.* = 1;

        // Apply WHERE filter (and any other per-chunk operators).
        applyPerChunkOperators(ctx, &chunk_result, model_id, ops, op_count, string_arena);
        if (chunk_result.has_error) {
            if (chunk_result.getError()) |msg| setError(result, msg);
            return true;
        }

        // Feed survivors into the collector.
        var row_idx: u16 = 0;
        while (row_idx < chunk_result.row_count) : (row_idx += 1) {
            ctx.collector.appendRow(&chunk_result.rows[row_idx]) catch {
                setError(result, "spill collector append failed");
                return true;
            };
        }
    }
    return true;
}

/// Execute a PK range scan: iterate B+ tree entries in [lo, hi), apply WHERE, feed to collector.
fn executeRangeScan(
    ctx: *const ExecContext,
    result: *QueryResult,
    model_id: ModelId,
    btree: *btree_mod.BTree,
    lo: ?[]const u8,
    hi: ?[]const u8,
    ops: *const [max_operators]OpDescriptor,
    op_count: u16,
    string_arena: *scan_mod.StringArena,
    total_pages_read: *u32,
    total_rows_scanned: *u32,
) bool {
    const scan_buf = ctx.scratch_rows_a[0..scan_mod.scan_batch_size];

    const scan_result = scan_mod.indexRangeScanInto(
        ctx.catalog,
        ctx.pool,
        ctx.undo_log,
        ctx.snapshot,
        ctx.tx_manager,
        btree,
        model_id,
        lo,
        hi,
        scan_buf,
        string_arena,
    ) catch {
        setError(result, "index range scan failed");
        return true;
    };

    result.stats.plan.scan_strategy = .pk_range_scan;
    total_pages_read.* = scan_result.pages_read;
    total_rows_scanned.* = scan_result.row_count;

    // Apply WHERE filter on the scanned batch.
    var chunk_result = QueryResult.init(ctx.scratch_rows_a);
    chunk_result.row_count = scan_result.row_count;

    applyPerChunkOperators(ctx, &chunk_result, model_id, ops, op_count, string_arena);
    if (chunk_result.has_error) {
        if (chunk_result.getError()) |msg| setError(result, msg);
        return true;
    }

    // Feed survivors into the collector.
    var row_idx: u16 = 0;
    while (row_idx < chunk_result.row_count) : (row_idx += 1) {
        ctx.collector.appendRow(&chunk_result.rows[row_idx]) catch {
            setError(result, "spill collector append failed");
            return true;
        };
    }
    return true;
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

/// Returns true when the pipeline contains post-scan operators that currently
/// require flat in-memory rows and are not safe with collector-backed output.
fn hasUnsupportedCollectorBackedPostOps(
    ops: *const [max_operators]OpDescriptor,
    op_count: u16,
) bool {
    _ = ops;
    _ = op_count;
    return false;
}

fn hasCollectorHavingOp(
    ops: *const [max_operators]OpDescriptor,
    op_count: u16,
) bool {
    var i: u16 = 0;
    while (i < op_count) : (i += 1) {
        if (ops[i].kind == .having_filter) return true;
    }
    return false;
}

const CollectorOutputWindow = struct {
    offset: u64,
    count: u64,
};

/// Compute LIMIT/OFFSET window over a collector-backed row stream.
/// Applies operators in pipeline order against `total_rows`.
fn computeCollectorOutputWindow(
    ctx: *const ExecContext,
    ops: *const [max_operators]OpDescriptor,
    op_count: u16,
    string_arena: *scan_mod.StringArena,
    total_rows: u64,
) CollectorOutputWindow {
    var offset: u64 = 0;
    var count: u64 = total_rows;

    var i: u16 = 0;
    while (i < op_count) : (i += 1) {
        const op = ops[i];
        switch (op.kind) {
            .limit_op => {
                const limit = evaluateCollectorCountExpr(ctx, op.node, string_arena) orelse continue;
                if (count > limit) count = limit;
            },
            .offset_op => {
                const skip = evaluateCollectorCountExpr(ctx, op.node, string_arena) orelse continue;
                if (skip >= count) {
                    offset +|= count;
                    count = 0;
                } else {
                    offset +|= skip;
                    count -= skip;
                }
            },
            else => {},
        }
    }

    return .{
        .offset = offset,
        .count = count,
    };
}

fn evaluateCollectorCountExpr(
    ctx: *const ExecContext,
    op_node: NodeIndex,
    string_arena: *scan_mod.StringArena,
) ?u64 {
    const node = ctx.ast.getNode(op_node);
    const expr = node.data.unary;
    if (expr == null_node) return null;

    var exec_eval = evalContextForExec(ctx, string_arena);
    const value = filter_mod.evaluateExpressionFull(
        ctx.ast,
        ctx.tokens,
        ctx.source,
        expr,
        &.{},
        &RowSchema{},
        null,
        &exec_eval.eval_ctx,
    ) catch return null;

    return numericToCollectorCount(value);
}

fn numericToCollectorCount(value: Value) ?u64 {
    return switch (value) {
        .i8 => |v| if (v >= 0) @intCast(v) else 0,
        .i16 => |v| if (v >= 0) @intCast(v) else 0,
        .i32 => |v| if (v >= 0) @intCast(v) else 0,
        .i64 => |v| if (v >= 0) @intCast(v) else 0,
        .u8 => |v| v,
        .u16 => |v| v,
        .u32 => |v| v,
        .u64 => |v| v,
        else => null,
    };
}

fn collectorWindowCount(total_rows: u64, offset: u64, count: u64) u64 {
    if (offset >= total_rows) return 0;
    const available = total_rows - offset;
    return @min(available, count);
}

const CollectorPostOpKind = enum {
    where_filter,
    having_filter,
    limit_op,
    offset_op,
};

const CollectorPostOp = struct {
    kind: CollectorPostOpKind,
    node: NodeIndex = null_node,
    count: ?u64 = null,
    remaining: u64 = 0,
};

/// Rewrite collector output by applying HAVING/LIMIT/OFFSET in pipeline order.
///
/// Used when collector-backed output contains HAVING, since simple offset/count
/// windowing cannot preserve all ordering semantics for mixed operators.
fn rewriteCollectorForPostOps(
    ctx: *const ExecContext,
    result: *QueryResult,
    model_id: ModelId,
    ops: *const [max_operators]OpDescriptor,
    op_count: u16,
    string_arena: *scan_mod.StringArena,
) bool {
    const collector = result.collector orelse return true;
    const source_schema = &ctx.catalog.models[model_id].row_schema;

    var stages: [max_operators]CollectorPostOp = undefined;
    var stage_count: u16 = 0;

    var i: u16 = 0;
    while (i < op_count) : (i += 1) {
        const op = ops[i];
        switch (op.kind) {
            .where_filter => {
                stages[stage_count] = .{
                    .kind = .where_filter,
                    .node = op.node,
                };
                stage_count += 1;
            },
            .having_filter => {
                stages[stage_count] = .{
                    .kind = .having_filter,
                    .node = op.node,
                };
                stage_count += 1;
            },
            .limit_op => {
                const n = evaluateCollectorCountExpr(ctx, op.node, string_arena);
                stages[stage_count] = .{
                    .kind = .limit_op,
                    .count = n,
                    .remaining = n orelse 0,
                };
                stage_count += 1;
            },
            .offset_op => {
                const n = evaluateCollectorCountExpr(ctx, op.node, string_arena);
                stages[stage_count] = .{
                    .kind = .offset_op,
                    .count = n,
                    .remaining = n orelse 0,
                };
                stage_count += 1;
            },
            else => {},
        }
    }

    if (stage_count == 0) return true;

    var iter = collector.iterator();
    var read_arena_buf: [scan_mod.max_row_size_bytes + 192]u8 = undefined;
    var read_arena = scan_mod.StringArena.init(&read_arena_buf);
    var row = scan_mod.ResultRow.init();

    var output_page_ids: [max_spill_pages]u64 = undefined;
    var output_page_count: u32 = 0;
    var output_rows: u64 = 0;
    var writer = SpillPageWriter.init();

    while (true) {
        read_arena.reset();
        const has_row = iter.next(&row, &read_arena) catch {
            setError(result, "spill post-operator read failed");
            return false;
        };
        if (!has_row) break;

        var keep = true;
        var stage_idx: u16 = 0;
        while (stage_idx < stage_count and keep) : (stage_idx += 1) {
            var stage = &stages[stage_idx];
            switch (stage.kind) {
                .where_filter, .having_filter => {
                    const predicate = ctx.ast.getNode(stage.node).data.unary;
                    if (predicate == null_node) continue;

                    var exec_eval = evalContextForExec(ctx, string_arena);
                    const matches = filter_mod.evaluatePredicateFull(
                        ctx.ast,
                        ctx.tokens,
                        ctx.source,
                        predicate,
                        row.values[0..row.column_count],
                        source_schema,
                        null,
                        &exec_eval.eval_ctx,
                    ) catch |err| switch (err) {
                        error.UndefinedParameter => {
                            setPredicateUndefinedParameterError(result, switch (stage.kind) {
                                .having_filter => "having",
                                else => "where",
                            });
                            return false;
                        },
                        error.ClockUnavailable => {
                            setPredicateClockUnavailableError(result, switch (stage.kind) {
                                .having_filter => "having",
                                else => "where",
                            });
                            return false;
                        },
                        error.TypeMismatch => {
                            setPredicateMustBeBooleanError(result, switch (stage.kind) {
                                .having_filter => "having",
                                else => "where",
                            });
                            return false;
                        },
                        error.UndefinedVariable => {
                            setPredicateUndefinedVariableError(result, switch (stage.kind) {
                                .having_filter => "having",
                                else => "where",
                            });
                            return false;
                        },
                        error.AmbiguousIdentifier => {
                            setPredicateAmbiguousIdentifierError(result, switch (stage.kind) {
                                .having_filter => "having",
                                else => "where",
                            });
                            return false;
                        },
                        error.VariableTypeMismatch => {
                            setPredicateVariableTypeMismatchError(result, switch (stage.kind) {
                                .having_filter => "having",
                                else => "where",
                            });
                            return false;
                        },
                        else => false,
                    };
                    if (!matches) keep = false;
                },
                .limit_op => {
                    if (stage.count == null) continue;
                    if (stage.remaining == 0) {
                        keep = false;
                    } else {
                        stage.remaining -= 1;
                    }
                },
                .offset_op => {
                    if (stage.count == null) continue;
                    if (stage.remaining > 0) {
                        stage.remaining -= 1;
                        keep = false;
                    }
                },
            }
        }

        if (!keep) continue;

        const appended = writer.appendRow(&row) catch {
            setError(result, "spill post-operator write failed");
            return false;
        };
        if (!appended) {
            if (output_page_count >= max_spill_pages) {
                setError(result, "spill post-operator temp page budget exhausted");
                return false;
            }
            const payload = writer.finalize();
            const page_id = collector.temp_mgr.allocateAndWrite(payload, temp_mod.TempPage.null_page_id) catch {
                setError(result, "spill post-operator temp page budget exhausted");
                return false;
            };
            output_page_ids[output_page_count] = page_id;
            output_page_count += 1;
            writer.reset();
            const retried = writer.appendRow(&row) catch {
                setError(result, "spill post-operator write failed");
                return false;
            };
            std.debug.assert(retried);
        }

        output_rows += 1;
    }

    if (writer.row_count > 0) {
        if (output_page_count >= max_spill_pages) {
            setError(result, "spill post-operator temp page budget exhausted");
            return false;
        }
        const payload = writer.finalize();
        const page_id = collector.temp_mgr.allocateAndWrite(payload, temp_mod.TempPage.null_page_id) catch {
            setError(result, "spill post-operator temp page budget exhausted");
            return false;
        };
        output_page_ids[output_page_count] = page_id;
        output_page_count += 1;
    }

    @memcpy(
        collector.spill_page_ids[0..output_page_count],
        output_page_ids[0..output_page_count],
    );
    collector.spill_page_count = output_page_count;
    collector.hot_count = 0;
    collector.hot_bytes = 0;
    collector.total_rows = output_rows;
    collector.iteration_started = false;
    result.collector = collector;

    return true;
}

/// Returns true if the selection set contains at least one nested relation.
fn hasNestedSelection(tree: *const Ast, pipeline_node: NodeIndex) bool {
    const selection = getPipelineSelection(tree, pipeline_node) orelse return false;
    var field = tree.getNode(selection).data.unary;
    while (field != null_node) {
        const node = tree.getNode(field);
        if (node.tag == .select_nested) return true;
        field = node.next;
    }
    return false;
}

/// Find the AST node for the WHERE operator, if present.
fn findWhereOpNode(
    ops: *const [max_operators]OpDescriptor,
    op_count: u16,
) ?NodeIndex {
    var i: u16 = 0;
    while (i < op_count) : (i += 1) {
        if (ops[i].kind == .where_filter) return ops[i].node;
    }
    return null;
}

/// Find the AST node for the sort operator, if present.
fn findSortOpNode(
    ops: *const [max_operators]OpDescriptor,
    op_count: u16,
) ?NodeIndex {
    var i: u16 = 0;
    while (i < op_count) : (i += 1) {
        if (ops[i].kind == .sort_op) return ops[i].node;
    }
    return null;
}

/// Returns true if the operator list contains a GROUP operator.
fn hasGroupOp(
    ops: *const [max_operators]OpDescriptor,
    op_count: u16,
) bool {
    var i: u16 = 0;
    while (i < op_count) : (i += 1) {
        if (ops[i].kind == .group_op) return true;
    }
    return false;
}

/// Find the group operator's node and index, if present.
const GroupOpInfo = struct { node: NodeIndex, index: u16 };
fn findGroupOpInfo(
    ops: *const [max_operators]OpDescriptor,
    op_count: u16,
) ?GroupOpInfo {
    var i: u16 = 0;
    while (i < op_count) : (i += 1) {
        if (ops[i].kind == .group_op) return .{ .node = ops[i].node, .index = i };
    }
    return null;
}

/// Apply post-hash-aggregate operators: HAVING, SORT, LIMIT, OFFSET.
/// Skips WHERE (already applied per-chunk) and GROUP (already done by
/// hash aggregate). Uses the populated group_runtime for aggregate resolution.
fn applyPostHashAggregateOperators(
    ctx: *const ExecContext,
    result: *QueryResult,
    model_id: ModelId,
    ops: *const [max_operators]OpDescriptor,
    op_count: u16,
    caps: *const capacity_mod.OperatorCapacities,
    group_runtime: *GroupRuntime,
    string_arena: *scan_mod.StringArena,
) bool {
    const schema = &ctx.catalog.models[model_id].row_schema;
    var i: u16 = 0;
    while (i < op_count) : (i += 1) {
        const op = ops[i];
        switch (op.kind) {
            .where_filter, .group_op => {}, // Already handled.
            .having_filter => applyWhereFilter(
                ctx,
                result,
                op.node,
                schema,
                group_runtime,
                string_arena,
            ),
            .sort_op => {
                if (!sorting_mod.applySort(
                    ctx,
                    result,
                    op.node,
                    schema,
                    caps,
                    group_runtime,
                    string_arena,
                )) return false;
            },
            .limit_op => applyLimit(ctx, result, op.node, string_arena),
            .offset_op => applyOffset(ctx, result, op.node, group_runtime, string_arena),
            .inspect_op => {},
            .insert_op, .update_op, .delete_op => {},
        }
    }
    return true;
}

/// Apply post-external-sort operators: HAVING, LIMIT, OFFSET.
/// Skips WHERE (already applied per-chunk) and SORT (already done by
/// external sort).
fn applyPostExternalSortOperators(
    ctx: *const ExecContext,
    result: *QueryResult,
    model_id: ModelId,
    ops: *const [max_operators]OpDescriptor,
    op_count: u16,
    caps: *const capacity_mod.OperatorCapacities,
    string_arena: *scan_mod.StringArena,
) bool {
    _ = caps;
    var group_runtime = GroupRuntime{};
    const schema = &ctx.catalog.models[model_id].row_schema;
    var i: u16 = 0;
    while (i < op_count) : (i += 1) {
        const op = ops[i];
        switch (op.kind) {
            .where_filter, .sort_op, .group_op => {}, // Already handled.
            .having_filter => applyWhereFilter(
                ctx,
                result,
                op.node,
                schema,
                &group_runtime,
                string_arena,
            ),
            .limit_op => applyLimit(ctx, result, op.node, string_arena),
            .offset_op => applyOffset(ctx, result, op.node, &group_runtime, string_arena),
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
    string_arena: *scan_mod.StringArena,
) bool {
    const selection = getPipelineSelection(ctx.ast, pipeline_node) orelse
        return true;
    var field = ctx.ast.getNode(selection).data.unary;
    const collector_backed = result.collector != null;
    while (field != null_node) {
        const node = ctx.ast.getNode(field);
        if (node.tag == .select_nested) {
            if (collector_backed) {
                if (!applySingleNestedSelectionJoinCollector(
                    ctx,
                    result,
                    source_model_id,
                    field,
                    caps,
                    string_arena,
                )) return false;
            } else {
                if (!applySingleNestedSelectionJoin(
                    ctx,
                    result,
                    source_model_id,
                    field,
                    caps,
                    string_arena,
                )) return false;
            }
        }
        field = node.next;
    }
    return true;
}

fn nestedRowSetCount(row_set: NestedRowSet) u64 {
    return switch (row_set) {
        .flat => |flat| flat.count,
        .spill => |spill| spill.count,
    };
}

fn initParentLocalNestedCollector(
    ctx: *const ExecContext,
    result: *QueryResult,
) ?SpillingResultCollector {
    const temp_mgr = TempStorageManager.init(
        ctx.query_slot_index,
        ctx.storage,
        ctx.temp_pages_per_query_slot,
        temp_mod.nested_region_start_page_id,
    ) catch {
        setError(result, "nested relation parent-local spill temp init failed");
        return null;
    };
    return SpillingResultCollector.init(
        ctx.nested_rows,
        temp_mgr,
        ctx.work_memory_bytes_per_slot,
    );
}

fn applySingleNestedSelectionJoinCollector(
    ctx: *const ExecContext,
    result: *QueryResult,
    source_model_id: ModelId,
    nested: NodeIndex,
    caps: *const capacity_mod.OperatorCapacities,
    string_arena: *scan_mod.StringArena,
) bool {
    const collector = result.collector orelse {
        setError(result, "nested collector path requires collector");
        return false;
    };
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
    const target_schema = &ctx.catalog.models[target_model_id].row_schema;
    _ = string_arena;
    var nested_match_arena = scan_mod.StringArena.init(
        ctx.nested_match_arena_bytes,
    );

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

    const join = inferAssociationJoinDescriptor(
        ctx.catalog,
        source_model_id,
        assoc,
        result,
    ) orelse return false;
    if (nested_op_count == 0) {
        return switch (tryApplyNestedSelectionHashJoinCollectorNoOps(
            ctx,
            result,
            collector,
            target_model_id,
            join,
            target_schema,
            caps,
        )) {
            .applied => true,
            .failed => false,
        };
    }
    return switch (tryApplyNestedSelectionHashJoinCollectorWithOps(
        ctx,
        result,
        collector,
        target_model_id,
        join,
        target_schema,
        &nested_ops,
        nested_op_count,
        caps,
        &nested_match_arena,
    )) {
        .applied => true,
        .failed => false,
    };
}

fn appendNestedCollectorOutputRow(
    result: *QueryResult,
    collector: *SpillingResultCollector,
    writer: *SpillPageWriter,
    output_page_ids: *[max_spill_pages]u64,
    output_page_count: *u32,
    output_rows: *u64,
    left_row: *const ResultRow,
    nested_row_opt: ?*const ResultRow,
    target_column_count: u16,
) bool {
    var out = ResultRow.init();
    if (nested_row_opt) |nested_row| {
        const total_columns = @as(usize, left_row.column_count) +
            @as(usize, nested_row.column_count);
        if (total_columns > scan_mod.max_columns) {
            setError(result, "join column capacity exceeded");
            return false;
        }
        out.column_count = @intCast(total_columns);
        out.row_id = left_row.row_id;
        @memcpy(
            out.values[0..left_row.column_count],
            left_row.values[0..left_row.column_count],
        );
        @memcpy(
            out.values[left_row.column_count..out.column_count],
            nested_row.values[0..nested_row.column_count],
        );
    } else {
        const total_columns = @as(usize, left_row.column_count) +
            @as(usize, target_column_count);
        if (total_columns > scan_mod.max_columns) {
            setError(result, "join column capacity exceeded");
            return false;
        }
        out.column_count = @intCast(total_columns);
        out.row_id = left_row.row_id;
        @memcpy(
            out.values[0..left_row.column_count],
            left_row.values[0..left_row.column_count],
        );
        var null_col: u16 = 0;
        while (null_col < target_column_count) : (null_col += 1) {
            out.values[left_row.column_count + null_col] = .{ .null_value = {} };
        }
    }

    const appended = writer.appendRow(&out) catch {
        setError(result, "nested relation spill row encode failed");
        return false;
    };
    if (!appended) {
        if (output_page_count.* >= max_spill_pages) {
            setError(result, "nested relation spill page tracking overflow");
            return false;
        }
        const payload = writer.finalize();
        const page_id = collector.temp_mgr.allocateAndWrite(payload, temp_mod.TempPage.null_page_id) catch {
            setError(result, "nested relation spill write failed");
            return false;
        };
        output_page_ids.*[output_page_count.*] = page_id;
        output_page_count.* += 1;
        writer.reset();
        const retry = writer.appendRow(&out) catch {
            setError(result, "nested relation spill row encode failed");
            return false;
        };
        std.debug.assert(retry);
    }
    output_rows.* += 1;
    return true;
}

fn copyNestedMatchRowIntoArena(
    src: *const ResultRow,
    dst: *ResultRow,
    match_arena: *scan_mod.StringArena,
) error{OutOfMemory}!void {
    dst.* = ResultRow.init();
    dst.column_count = src.column_count;
    dst.row_id = src.row_id;
    var col_idx: u16 = 0;
    while (col_idx < src.column_count) : (col_idx += 1) {
        dst.values[col_idx] = switch (src.values[col_idx]) {
            .string => |text| .{ .string = try match_arena.copyString(text) },
            else => src.values[col_idx],
        };
    }
}

fn copyNestedValueIntoArena(
    value: Value,
    arena: *scan_mod.StringArena,
) error{OutOfMemory}!Value {
    return switch (value) {
        .string => |text| .{ .string = try arena.copyString(text) },
        else => value,
    };
}

const NestedHashFastPathOutcome = enum {
    applied,
    failed,
};

fn setNestedHashPartitionError(
    result: *QueryResult,
    err: anyerror,
) void {
    setError(result, switch (err) {
        error.InvalidPartitionCount => "nested relation hash spill invalid partition count",
        error.RightKeyOutOfBounds => "join key out of bounds",
        error.SpillPageBudgetExceeded => "nested relation hash spill temp page budget exhausted",
        error.SpillError => "nested relation hash spill row serialization failed",
        error.RegionExhausted => "nested relation hash spill temp page budget exhausted",
        error.PageFull => "nested relation hash spill row too large",
        error.InvalidPageFormat, error.UnsupportedPageVersion, error.ChecksumMismatch, error.InvalidPageType => "nested relation hash spill page corruption",
        error.ReadError => "nested relation hash spill read failed",
        error.WriteError => "nested relation hash spill write failed",
        else => "nested relation hash spill failed",
    });
}

const RightPartitionHashCacheState = struct {
    partition: ?u8 = null,
    valid: bool = false,
    index: LeftHashIndex = undefined,
};

fn prepareRightPartitionHashCache(
    result: *QueryResult,
    temp_mgr: *TempStorageManager,
    descriptor: *const hash_join_mod.PartitionSpillDescriptor,
    join: JoinDescriptor,
    caps: *const capacity_mod.OperatorCapacities,
    partition: u8,
    cache_rows: []ResultRow,
    cache_arena: *scan_mod.StringArena,
    state: *RightPartitionHashCacheState,
) bool {
    if (state.partition != null and state.partition.? == partition) {
        return true;
    }
    state.partition = partition;
    state.valid = false;

    if (descriptor.partition_row_counts[partition] == 0) return true;
    if (descriptor.partition_row_counts[partition] > cache_rows.len) return true;

    var partition_iter = hash_join_mod.PartitionRowIterator.init(
        temp_mgr,
        descriptor,
        partition,
    ) catch |err| {
        setNestedHashPartitionError(result, err);
        return false;
    };

    cache_arena.reset();
    var decode_arena_buf: [scan_mod.max_row_size_bytes + 192]u8 = undefined;
    var decode_arena = scan_mod.StringArena.init(&decode_arena_buf);
    var decoded = ResultRow.init();
    var cached_count: u16 = 0;
    while (true) {
        decode_arena.reset();
        const has_row = partition_iter.next(&decoded, &decode_arena) catch |err| {
            setNestedHashPartitionError(result, err);
            return false;
        };
        if (!has_row) break;
        if (cached_count >= cache_rows.len) {
            state.valid = false;
            return true;
        }
        copyNestedMatchRowIntoArena(
            &decoded,
            &cache_rows[cached_count],
            cache_arena,
        ) catch {
            state.valid = false;
            return true;
        };
        cached_count += 1;
    }

    if (cached_count == 0) return true;
    const cache_index = LeftHashIndex.init(
        cache_rows[0..cached_count],
        .{
            .left_key_index = join.left_key_index,
            .right_key_index = join.right_key_index,
        },
        caps,
    ) catch {
        state.valid = false;
        return true;
    };
    state.index = cache_index;
    state.valid = true;
    return true;
}

fn collectNestedMatchesForParentFromHashIndex(
    result: *QueryResult,
    hash_index: *const LeftHashIndex,
    left_key: Value,
    parent_collector: *SpillingResultCollector,
    match_arena: *scan_mod.StringArena,
    out_row_set: *NestedRowSet,
) bool {
    parent_collector.reset();
    out_row_set.* = .{
        .flat = .{
            .rows = parent_collector.hot_batch,
            .count = 0,
        },
    };

    var matches = hash_index.matchIterator(left_key);
    while (matches.next()) |right_row| {
        var copied = ResultRow.init();
        copyNestedMatchRowIntoArena(
            right_row,
            &copied,
            match_arena,
        ) catch {
            setError(result, "nested relation parent string arena exhausted");
            return false;
        };
        parent_collector.appendRow(&copied) catch {
            setError(result, "nested relation parent-local spill append failed");
            return false;
        };
    }

    if (parent_collector.spillTriggered()) {
        out_row_set.* = .{
            .spill = .{
                .collector = parent_collector,
                .offset = 0,
                .count = parent_collector.totalRowCount(),
            },
        };
    } else {
        out_row_set.* = .{
            .flat = .{
                .rows = parent_collector.hot_batch,
                .count = parent_collector.hot_count,
            },
        };
    }
    return true;
}

fn collectNestedMatchesForParentFromPartitionSpill(
    result: *QueryResult,
    temp_mgr: *TempStorageManager,
    descriptor: *const hash_join_mod.PartitionSpillDescriptor,
    join: JoinDescriptor,
    caps: *const capacity_mod.OperatorCapacities,
    left_key: Value,
    cache_rows: []ResultRow,
    cache_arena: *scan_mod.StringArena,
    cache_state: *RightPartitionHashCacheState,
    parent_collector: *SpillingResultCollector,
    match_arena: *scan_mod.StringArena,
    out_row_set: *NestedRowSet,
) bool {
    parent_collector.reset();
    out_row_set.* = .{
        .flat = .{
            .rows = parent_collector.hot_batch,
            .count = 0,
        },
    };

    const partition = hash_join_mod.partitionForKey(
        left_key,
        descriptor.partition_count,
    ) catch |err| {
        setNestedHashPartitionError(result, err);
        return false;
    };

    if (descriptor.partition_row_counts[partition] > 0) {
        if (!prepareRightPartitionHashCache(
            result,
            temp_mgr,
            descriptor,
            join,
            caps,
            partition,
            cache_rows,
            cache_arena,
            cache_state,
        )) return false;

        if (cache_state.valid) {
            var matches = cache_state.index.matchIterator(left_key);
            while (matches.next()) |cached_right| {
                var copied = ResultRow.init();
                copyNestedMatchRowIntoArena(
                    cached_right,
                    &copied,
                    match_arena,
                ) catch {
                    setError(result, "nested relation parent string arena exhausted");
                    return false;
                };
                parent_collector.appendRow(&copied) catch {
                    setError(result, "nested relation parent-local spill append failed");
                    return false;
                };
            }
        } else {
            var partition_iter = hash_join_mod.PartitionRowIterator.init(
                temp_mgr,
                descriptor,
                partition,
            ) catch |err| {
                setNestedHashPartitionError(result, err);
                return false;
            };
            var decode_arena_buf: [scan_mod.max_row_size_bytes + 192]u8 = undefined;
            var decode_arena = scan_mod.StringArena.init(&decode_arena_buf);
            var right_row = ResultRow.init();
            while (true) {
                decode_arena.reset();
                const has_right = partition_iter.next(&right_row, &decode_arena) catch |err| {
                    setNestedHashPartitionError(result, err);
                    return false;
                };
                if (!has_right) break;
                if (row_mod.compareValues(
                    left_key,
                    right_row.values[join.right_key_index],
                ) != .eq) continue;

                var copied = ResultRow.init();
                copyNestedMatchRowIntoArena(
                    &right_row,
                    &copied,
                    match_arena,
                ) catch {
                    setError(result, "nested relation parent string arena exhausted");
                    return false;
                };
                parent_collector.appendRow(&copied) catch {
                    setError(result, "nested relation parent-local spill append failed");
                    return false;
                };
            }
        }
    }

    if (parent_collector.spillTriggered()) {
        out_row_set.* = .{
            .spill = .{
                .collector = parent_collector,
                .offset = 0,
                .count = parent_collector.totalRowCount(),
            },
        };
    } else {
        out_row_set.* = .{
            .flat = .{
                .rows = parent_collector.hot_batch,
                .count = parent_collector.hot_count,
            },
        };
    }
    return true;
}

fn applyNestedOperatorsToParentRowSet(
    ctx: *const ExecContext,
    result: *QueryResult,
    parent_row_set: *NestedRowSet,
    target_model_id: ModelId,
    nested_ops: *const [max_operators]OpDescriptor,
    nested_op_count: u16,
    caps: *const capacity_mod.OperatorCapacities,
    string_arena: *scan_mod.StringArena,
) bool {
    if (nestedRowSetCount(parent_row_set.*) == 0) return true;

    var parent_subset = QueryResult{
        .rows = ctx.nested_rows,
        .row_count = @intCast(@min(
            nestedRowSetCount(parent_row_set.*),
            scan_mod.scan_batch_size,
        )),
    };
    if (!applyNestedReadOperatorsPerParent(
        ctx,
        &parent_subset,
        parent_row_set,
        target_model_id,
        nested_ops,
        nested_op_count,
        caps,
        string_arena,
    )) {
        if (parent_subset.getError()) |msg| setError(result, msg);
        return false;
    }
    return true;
}

fn emitNestedFlatOutputRowsForParent(
    result: *QueryResult,
    parent_row_set: NestedRowSet,
    left_row: *const ResultRow,
    target_schema: *const RowSchema,
    caps: *const capacity_mod.OperatorCapacities,
    output_count: *u16,
    string_arena: *scan_mod.StringArena,
) bool {
    if (nestedRowSetCount(parent_row_set) == 0) {
        if (@as(usize, output_count.*) >= caps.join_output_rows) {
            setError(result, "join output row capacity exceeded");
            return false;
        }
        const total_columns_with_right = @as(usize, left_row.column_count) +
            @as(usize, target_schema.column_count);
        if (total_columns_with_right > scan_mod.max_columns) {
            setError(result, "join column capacity exceeded");
            return false;
        }
        var out = ResultRow.init();
        out.column_count = @intCast(total_columns_with_right);
        out.row_id = left_row.row_id;
        @memcpy(
            out.values[0..left_row.column_count],
            left_row.values[0..left_row.column_count],
        );
        var null_col: u16 = 0;
        while (null_col < target_schema.column_count) : (null_col += 1) {
            out.values[left_row.column_count + null_col] = .{ .null_value = {} };
        }
        result.rows[output_count.*] = out;
        output_count.* += 1;
        return true;
    }

    switch (parent_row_set) {
        .flat => |flat| {
            var nested_idx: u16 = 0;
            while (nested_idx < flat.count) : (nested_idx += 1) {
                if (@as(usize, output_count.*) >= caps.join_output_rows) {
                    setError(result, "join output row capacity exceeded");
                    return false;
                }
                const nested_row = flat.rows[nested_idx];
                const total_columns = @as(usize, left_row.column_count) +
                    @as(usize, nested_row.column_count);
                if (total_columns > scan_mod.max_columns) {
                    setError(result, "join column capacity exceeded");
                    return false;
                }

                var out = ResultRow.init();
                out.column_count = @intCast(total_columns);
                out.row_id = left_row.row_id;
                @memcpy(
                    out.values[0..left_row.column_count],
                    left_row.values[0..left_row.column_count],
                );
                var nested_col: u16 = 0;
                while (nested_col < nested_row.column_count) : (nested_col += 1) {
                    out.values[left_row.column_count + nested_col] = copyNestedValueIntoArena(
                        nested_row.values[nested_col],
                        string_arena,
                    ) catch {
                        setError(result, "nested relation output string arena exhausted");
                        return false;
                    };
                }
                result.rows[output_count.*] = out;
                output_count.* += 1;
            }
        },
        .spill => |spill| {
            spill.collector.iteration_started = false;
            var nested_iter = spill.collector.iterator();
            var nested_read_arena_buf: [scan_mod.max_row_size_bytes + 192]u8 = undefined;
            var nested_read_arena = scan_mod.StringArena.init(&nested_read_arena_buf);
            var nested_row = ResultRow.init();
            while (true) {
                nested_read_arena.reset();
                const has_nested = nested_iter.next(&nested_row, &nested_read_arena) catch {
                    setError(result, "nested relation right-side spill read failed");
                    return false;
                };
                if (!has_nested) break;
                if (@as(usize, output_count.*) >= caps.join_output_rows) {
                    setError(result, "join output row capacity exceeded");
                    return false;
                }
                const total_columns = @as(usize, left_row.column_count) +
                    @as(usize, nested_row.column_count);
                if (total_columns > scan_mod.max_columns) {
                    setError(result, "join column capacity exceeded");
                    return false;
                }
                var out = ResultRow.init();
                out.column_count = @intCast(total_columns);
                out.row_id = left_row.row_id;
                @memcpy(
                    out.values[0..left_row.column_count],
                    left_row.values[0..left_row.column_count],
                );
                var nested_col: u16 = 0;
                while (nested_col < nested_row.column_count) : (nested_col += 1) {
                    out.values[left_row.column_count + nested_col] = copyNestedValueIntoArena(
                        nested_row.values[nested_col],
                        string_arena,
                    ) catch {
                        setError(result, "nested relation output string arena exhausted");
                        return false;
                    };
                }
                result.rows[output_count.*] = out;
                output_count.* += 1;
            }
        },
    }
    return true;
}

fn emitNestedCollectorOutputRowsForParent(
    result: *QueryResult,
    collector: *SpillingResultCollector,
    writer: *SpillPageWriter,
    output_page_ids: *[max_spill_pages]u64,
    output_page_count: *u32,
    output_rows: *u64,
    parent_row_set: NestedRowSet,
    left_row: *const ResultRow,
    target_schema: *const RowSchema,
) bool {
    if (nestedRowSetCount(parent_row_set) == 0) {
        return appendNestedCollectorOutputRow(
            result,
            collector,
            writer,
            output_page_ids,
            output_page_count,
            output_rows,
            left_row,
            null,
            target_schema.column_count,
        );
    }

    switch (parent_row_set) {
        .flat => |flat| {
            var nested_idx: u16 = 0;
            while (nested_idx < flat.count) : (nested_idx += 1) {
                if (!appendNestedCollectorOutputRow(
                    result,
                    collector,
                    writer,
                    output_page_ids,
                    output_page_count,
                    output_rows,
                    left_row,
                    &flat.rows[nested_idx],
                    target_schema.column_count,
                )) return false;
            }
        },
        .spill => |spill| {
            spill.collector.iteration_started = false;
            var nested_iter = spill.collector.iterator();
            var nested_read_arena_buf: [scan_mod.max_row_size_bytes + 192]u8 = undefined;
            var nested_read_arena = scan_mod.StringArena.init(&nested_read_arena_buf);
            var nested_row = ResultRow.init();
            while (true) {
                nested_read_arena.reset();
                const has_nested = nested_iter.next(&nested_row, &nested_read_arena) catch {
                    setError(result, "nested relation right-side spill read failed");
                    return false;
                };
                if (!has_nested) break;
                if (!appendNestedCollectorOutputRow(
                    result,
                    collector,
                    writer,
                    output_page_ids,
                    output_page_count,
                    output_rows,
                    left_row,
                    &nested_row,
                    target_schema.column_count,
                )) return false;
            }
        },
    }
    return true;
}

fn tryApplyNestedSelectionHashJoinFlatWithOps(
    ctx: *const ExecContext,
    result: *QueryResult,
    target_model_id: ModelId,
    join: JoinDescriptor,
    target_schema: *const RowSchema,
    nested_ops: *const [max_operators]OpDescriptor,
    nested_op_count: u16,
    caps: *const capacity_mod.OperatorCapacities,
    nested_match_arena: *scan_mod.StringArena,
    string_arena: *scan_mod.StringArena,
) NestedHashFastPathOutcome {
    if (result.row_count == 0) {
        recordNestedHashJoinPlan(&result.stats.plan);
        return .applied;
    }

    std.debug.assert(ctx.scratch_rows_b.len >= scan_mod.scan_batch_size);
    const left_count = result.row_count;
    const left_copy = ctx.scratch_rows_b;
    @memcpy(left_copy[0..left_count], result.rows[0..left_count]);

    var parent_collector = initParentLocalNestedCollector(ctx, result) orelse
        return .failed;
    var parent_row_set: NestedRowSet = .{
        .flat = .{
            .rows = ctx.nested_rows,
            .count = 0,
        },
    };

    var decode_arena = scan_mod.StringArena.init(ctx.nested_decode_arena_bytes);
    var right_cursor = scan_mod.ScanCursor.init();
    const right_chunk = scan_mod.tableScanInto(
        ctx.catalog,
        ctx.pool,
        ctx.undo_log,
        ctx.snapshot,
        ctx.tx_manager,
        target_model_id,
        ctx.scratch_rows_a[0..scan_mod.scan_batch_size],
        &decode_arena,
        &right_cursor,
    ) catch |err| {
        setBoundaryError(
            result,
            "nested relation scan failed",
            runtime_errors.classifyScan(err),
            err,
        );
        return .failed;
    };
    result.stats.pages_read +|= right_chunk.pages_read;

    var output_count: u16 = 0;
    if (!right_cursor.done) {
        var temp_mgr = TempStorageManager.init(
            ctx.query_slot_index,
            ctx.storage,
            ctx.temp_pages_per_query_slot,
            temp_mod.nested_region_start_page_id,
        ) catch {
            setError(result, "nested relation hash spill temp init failed");
            return .failed;
        };
        var descriptor = hash_join_mod.PartitionSpillDescriptor.init(
            hash_join_mod.max_partitions,
        ) catch |err| {
            setNestedHashPartitionError(result, err);
            return .failed;
        };
        var builder = hash_join_mod.PartitionSpillBuilder.init(
            &temp_mgr,
            join.right_key_index,
            &descriptor,
        );
        builder.appendRows(ctx.scratch_rows_a[0..right_chunk.row_count]) catch |err| {
            setNestedHashPartitionError(result, err);
            return .failed;
        };
        while (!right_cursor.done) {
            decode_arena.reset();
            const chunk = scan_mod.tableScanInto(
                ctx.catalog,
                ctx.pool,
                ctx.undo_log,
                ctx.snapshot,
                ctx.tx_manager,
                target_model_id,
                ctx.scratch_rows_a[0..scan_mod.scan_batch_size],
                &decode_arena,
                &right_cursor,
            ) catch |err| {
                setBoundaryError(
                    result,
                    "nested relation scan failed",
                    runtime_errors.classifyScan(err),
                    err,
                );
                return .failed;
            };
            result.stats.pages_read +|= chunk.pages_read;
            builder.appendRows(ctx.scratch_rows_a[0..chunk.row_count]) catch |err| {
                setNestedHashPartitionError(result, err);
                return .failed;
            };
        }
        builder.finish() catch |err| {
            setNestedHashPartitionError(result, err);
            return .failed;
        };

        var cache_arena = scan_mod.StringArena.init(ctx.nested_decode_arena_bytes);
        var right_cache = RightPartitionHashCacheState{};

        var left_idx: u16 = 0;
        while (left_idx < left_count) : (left_idx += 1) {
            const left_row = left_copy[left_idx];
            if (join.left_key_index >= left_row.column_count) {
                setError(result, "join key out of bounds");
                return .failed;
            }
            nested_match_arena.reset();
            if (!collectNestedMatchesForParentFromPartitionSpill(
                result,
                &temp_mgr,
                &descriptor,
                join,
                caps,
                left_row.values[join.left_key_index],
                ctx.scratch_rows_a,
                &cache_arena,
                &right_cache,
                &parent_collector,
                nested_match_arena,
                &parent_row_set,
            )) return .failed;

            if (!applyNestedOperatorsToParentRowSet(
                ctx,
                result,
                &parent_row_set,
                target_model_id,
                nested_ops,
                nested_op_count,
                caps,
                nested_match_arena,
            )) return .failed;

            if (!emitNestedFlatOutputRowsForParent(
                result,
                parent_row_set,
                &left_row,
                target_schema,
                caps,
                &output_count,
                string_arena,
            )) return .failed;
        }

        result.row_count = output_count;
        recordNestedHashSpillJoinPlan(&result.stats.plan);
        return .applied;
    }

    const right_rows = ctx.scratch_rows_a[0..right_chunk.row_count];
    const hash_index = LeftHashIndex.init(
        right_rows,
        .{
            .left_key_index = join.left_key_index,
            .right_key_index = join.right_key_index,
        },
        caps,
    ) catch |err| {
        setError(result, switch (err) {
            error.BuildRowCapacityExceeded => "join build row capacity exceeded",
            error.StateCapacityExceeded => "join state capacity exceeded",
            error.LeftKeyOutOfBounds, error.RightKeyOutOfBounds => "join key out of bounds",
            error.JoinColumnCapacityExceeded => "join column capacity exceeded",
            error.OutputRowCapacityExceeded => "join output row capacity exceeded",
        });
        return .failed;
    };

    var left_idx: u16 = 0;
    while (left_idx < left_count) : (left_idx += 1) {
        const left_row = left_copy[left_idx];
        if (join.left_key_index >= left_row.column_count) {
            setError(result, "join key out of bounds");
            return .failed;
        }
        nested_match_arena.reset();
        if (!collectNestedMatchesForParentFromHashIndex(
            result,
            &hash_index,
            left_row.values[join.left_key_index],
            &parent_collector,
            nested_match_arena,
            &parent_row_set,
        )) return .failed;

        if (!applyNestedOperatorsToParentRowSet(
            ctx,
            result,
            &parent_row_set,
            target_model_id,
            nested_ops,
            nested_op_count,
            caps,
            nested_match_arena,
        )) return .failed;

        if (!emitNestedFlatOutputRowsForParent(
            result,
            parent_row_set,
            &left_row,
            target_schema,
            caps,
            &output_count,
            string_arena,
        )) return .failed;
    }

    result.row_count = output_count;
    recordNestedHashJoinPlan(&result.stats.plan);
    return .applied;
}

fn tryApplyNestedSelectionHashJoinCollectorWithOps(
    ctx: *const ExecContext,
    result: *QueryResult,
    collector: *SpillingResultCollector,
    target_model_id: ModelId,
    join: JoinDescriptor,
    target_schema: *const RowSchema,
    nested_ops: *const [max_operators]OpDescriptor,
    nested_op_count: u16,
    caps: *const capacity_mod.OperatorCapacities,
    nested_match_arena: *scan_mod.StringArena,
) NestedHashFastPathOutcome {
    var parent_collector = initParentLocalNestedCollector(ctx, result) orelse
        return .failed;
    var parent_row_set: NestedRowSet = .{
        .flat = .{
            .rows = ctx.nested_rows,
            .count = 0,
        },
    };

    var decode_arena = scan_mod.StringArena.init(ctx.nested_decode_arena_bytes);
    var right_cursor = scan_mod.ScanCursor.init();
    const right_chunk = scan_mod.tableScanInto(
        ctx.catalog,
        ctx.pool,
        ctx.undo_log,
        ctx.snapshot,
        ctx.tx_manager,
        target_model_id,
        ctx.scratch_rows_a[0..scan_mod.scan_batch_size],
        &decode_arena,
        &right_cursor,
    ) catch |err| {
        setBoundaryError(
            result,
            "nested relation scan failed",
            runtime_errors.classifyScan(err),
            err,
        );
        return .failed;
    };
    result.stats.pages_read +|= right_chunk.pages_read;

    var output_page_ids: [max_spill_pages]u64 = undefined;
    var output_page_count: u32 = 0;
    var output_rows: u64 = 0;
    var writer = SpillPageWriter.init();

    if (!right_cursor.done) {
        var temp_mgr = TempStorageManager.init(
            ctx.query_slot_index,
            ctx.storage,
            ctx.temp_pages_per_query_slot,
            temp_mod.nested_region_start_page_id,
        ) catch {
            setError(result, "nested relation hash spill temp init failed");
            return .failed;
        };
        var descriptor = hash_join_mod.PartitionSpillDescriptor.init(
            hash_join_mod.max_partitions,
        ) catch |err| {
            setNestedHashPartitionError(result, err);
            return .failed;
        };
        var builder = hash_join_mod.PartitionSpillBuilder.init(
            &temp_mgr,
            join.right_key_index,
            &descriptor,
        );
        builder.appendRows(ctx.scratch_rows_a[0..right_chunk.row_count]) catch |err| {
            setNestedHashPartitionError(result, err);
            return .failed;
        };
        while (!right_cursor.done) {
            decode_arena.reset();
            const chunk = scan_mod.tableScanInto(
                ctx.catalog,
                ctx.pool,
                ctx.undo_log,
                ctx.snapshot,
                ctx.tx_manager,
                target_model_id,
                ctx.scratch_rows_a[0..scan_mod.scan_batch_size],
                &decode_arena,
                &right_cursor,
            ) catch |err| {
                setBoundaryError(
                    result,
                    "nested relation scan failed",
                    runtime_errors.classifyScan(err),
                    err,
                );
                return .failed;
            };
            result.stats.pages_read +|= chunk.pages_read;
            builder.appendRows(ctx.scratch_rows_a[0..chunk.row_count]) catch |err| {
                setNestedHashPartitionError(result, err);
                return .failed;
            };
        }
        builder.finish() catch |err| {
            setNestedHashPartitionError(result, err);
            return .failed;
        };

        var left_iter = collector.iterator();
        var left_arena_buf: [scan_mod.max_row_size_bytes + 192]u8 = undefined;
        var left_arena = scan_mod.StringArena.init(&left_arena_buf);
        var left_row = ResultRow.init();
        var skip = result.collector_output_offset;
        var remaining = result.collector_output_count;
        var cache_arena = scan_mod.StringArena.init(ctx.nested_decode_arena_bytes);
        var right_cache = RightPartitionHashCacheState{};

        while (remaining > 0) {
            left_arena.reset();
            const has_left = left_iter.next(&left_row, &left_arena) catch {
                setError(result, "nested relation left-side spill read failed");
                return .failed;
            };
            if (!has_left) break;
            if (skip > 0) {
                skip -= 1;
                continue;
            }
            remaining -= 1;
            if (join.left_key_index >= left_row.column_count) {
                setError(result, "join key out of bounds");
                return .failed;
            }

            nested_match_arena.reset();
            if (!collectNestedMatchesForParentFromPartitionSpill(
                result,
                &temp_mgr,
                &descriptor,
                join,
                caps,
                left_row.values[join.left_key_index],
                ctx.scratch_rows_a,
                &cache_arena,
                &right_cache,
                &parent_collector,
                nested_match_arena,
                &parent_row_set,
            )) return .failed;

            if (!applyNestedOperatorsToParentRowSet(
                ctx,
                result,
                &parent_row_set,
                target_model_id,
                nested_ops,
                nested_op_count,
                caps,
                nested_match_arena,
            )) return .failed;

            if (!emitNestedCollectorOutputRowsForParent(
                result,
                collector,
                &writer,
                &output_page_ids,
                &output_page_count,
                &output_rows,
                parent_row_set,
                &left_row,
                target_schema,
            )) return .failed;
        }

        if (writer.row_count > 0) {
            if (output_page_count >= max_spill_pages) {
                setError(result, "nested relation spill page tracking overflow");
                return .failed;
            }
            const payload = writer.finalize();
            const page_id = collector.temp_mgr.allocateAndWrite(payload, temp_mod.TempPage.null_page_id) catch {
                setError(result, "nested relation spill write failed");
                return .failed;
            };
            output_page_ids[output_page_count] = page_id;
            output_page_count += 1;
        }

        @memcpy(
            collector.spill_page_ids[0..output_page_count],
            output_page_ids[0..output_page_count],
        );
        collector.spill_page_count = output_page_count;
        collector.hot_count = 0;
        collector.hot_bytes = 0;
        collector.total_rows = output_rows;
        collector.iteration_started = false;

        result.collector = collector;
        result.collector_output_offset = 0;
        result.collector_output_count = output_rows;
        result.row_count = @intCast(@min(output_rows, scan_mod.scan_batch_size));
        recordNestedHashSpillJoinPlan(&result.stats.plan);
        return .applied;
    }

    const right_rows = ctx.scratch_rows_a[0..right_chunk.row_count];
    const hash_index = LeftHashIndex.init(
        right_rows,
        .{
            .left_key_index = join.left_key_index,
            .right_key_index = join.right_key_index,
        },
        caps,
    ) catch |err| {
        setError(result, switch (err) {
            error.BuildRowCapacityExceeded => "join build row capacity exceeded",
            error.StateCapacityExceeded => "join state capacity exceeded",
            error.LeftKeyOutOfBounds, error.RightKeyOutOfBounds => "join key out of bounds",
            error.JoinColumnCapacityExceeded => "join column capacity exceeded",
            error.OutputRowCapacityExceeded => "join output row capacity exceeded",
        });
        return .failed;
    };

    var left_iter = collector.iterator();
    var left_arena_buf: [scan_mod.max_row_size_bytes + 192]u8 = undefined;
    var left_arena = scan_mod.StringArena.init(&left_arena_buf);
    var left_row = ResultRow.init();
    var skip = result.collector_output_offset;
    var remaining = result.collector_output_count;
    while (remaining > 0) {
        left_arena.reset();
        const has_left = left_iter.next(&left_row, &left_arena) catch {
            setError(result, "nested relation left-side spill read failed");
            return .failed;
        };
        if (!has_left) break;
        if (skip > 0) {
            skip -= 1;
            continue;
        }
        remaining -= 1;
        if (join.left_key_index >= left_row.column_count) {
            setError(result, "join key out of bounds");
            return .failed;
        }

        nested_match_arena.reset();
        if (!collectNestedMatchesForParentFromHashIndex(
            result,
            &hash_index,
            left_row.values[join.left_key_index],
            &parent_collector,
            nested_match_arena,
            &parent_row_set,
        )) return .failed;

        if (!applyNestedOperatorsToParentRowSet(
            ctx,
            result,
            &parent_row_set,
            target_model_id,
            nested_ops,
            nested_op_count,
            caps,
            nested_match_arena,
        )) return .failed;

        if (!emitNestedCollectorOutputRowsForParent(
            result,
            collector,
            &writer,
            &output_page_ids,
            &output_page_count,
            &output_rows,
            parent_row_set,
            &left_row,
            target_schema,
        )) return .failed;
    }

    if (writer.row_count > 0) {
        if (output_page_count >= max_spill_pages) {
            setError(result, "nested relation spill page tracking overflow");
            return .failed;
        }
        const payload = writer.finalize();
        const page_id = collector.temp_mgr.allocateAndWrite(payload, temp_mod.TempPage.null_page_id) catch {
            setError(result, "nested relation spill write failed");
            return .failed;
        };
        output_page_ids[output_page_count] = page_id;
        output_page_count += 1;
    }

    @memcpy(
        collector.spill_page_ids[0..output_page_count],
        output_page_ids[0..output_page_count],
    );
    collector.spill_page_count = output_page_count;
    collector.hot_count = 0;
    collector.hot_bytes = 0;
    collector.total_rows = output_rows;
    collector.iteration_started = false;

    result.collector = collector;
    result.collector_output_offset = 0;
    result.collector_output_count = output_rows;
    result.row_count = @intCast(@min(output_rows, scan_mod.scan_batch_size));
    recordNestedHashJoinPlan(&result.stats.plan);
    return .applied;
}

fn tryApplyNestedSelectionHashJoinFlatNoOps(
    ctx: *const ExecContext,
    result: *QueryResult,
    target_model_id: ModelId,
    join: JoinDescriptor,
    target_schema: *const RowSchema,
    caps: *const capacity_mod.OperatorCapacities,
    string_arena: *scan_mod.StringArena,
) NestedHashFastPathOutcome {
    if (result.row_count == 0) {
        recordNestedHashJoinPlan(&result.stats.plan);
        return .applied;
    }

    std.debug.assert(ctx.scratch_rows_b.len >= scan_mod.scan_batch_size);
    const left_count = result.row_count;
    const left_copy = ctx.scratch_rows_b;
    @memcpy(left_copy[0..left_count], result.rows[0..left_count]);

    var decode_arena = scan_mod.StringArena.init(ctx.nested_decode_arena_bytes);
    var right_cursor = scan_mod.ScanCursor.init();
    const right_chunk = scan_mod.tableScanInto(
        ctx.catalog,
        ctx.pool,
        ctx.undo_log,
        ctx.snapshot,
        ctx.tx_manager,
        target_model_id,
        ctx.scratch_rows_a[0..scan_mod.scan_batch_size],
        &decode_arena,
        &right_cursor,
    ) catch |err| {
        setBoundaryError(
            result,
            "nested relation scan failed",
            runtime_errors.classifyScan(err),
            err,
        );
        return .failed;
    };
    result.stats.pages_read +|= right_chunk.pages_read;
    if (!right_cursor.done) {
        var temp_mgr = TempStorageManager.init(
            ctx.query_slot_index,
            ctx.storage,
            ctx.temp_pages_per_query_slot,
            temp_mod.nested_region_start_page_id,
        ) catch {
            setError(result, "nested relation hash spill temp init failed");
            return .failed;
        };
        var descriptor = hash_join_mod.PartitionSpillDescriptor.init(
            hash_join_mod.max_partitions,
        ) catch |err| {
            setNestedHashPartitionError(result, err);
            return .failed;
        };
        // Include first chunk already loaded in `scratch_rows_a`.
        var builder = hash_join_mod.PartitionSpillBuilder.init(
            &temp_mgr,
            join.right_key_index,
            &descriptor,
        );
        builder.appendRows(ctx.scratch_rows_a[0..right_chunk.row_count]) catch |err| {
            setNestedHashPartitionError(result, err);
            return .failed;
        };
        while (!right_cursor.done) {
            decode_arena.reset();
            const chunk = scan_mod.tableScanInto(
                ctx.catalog,
                ctx.pool,
                ctx.undo_log,
                ctx.snapshot,
                ctx.tx_manager,
                target_model_id,
                ctx.scratch_rows_a[0..scan_mod.scan_batch_size],
                &decode_arena,
                &right_cursor,
            ) catch |err| {
                setBoundaryError(
                    result,
                    "nested relation scan failed",
                    runtime_errors.classifyScan(err),
                    err,
                );
                return .failed;
            };
            result.stats.pages_read +|= chunk.pages_read;
            builder.appendRows(ctx.scratch_rows_a[0..chunk.row_count]) catch |err| {
                setNestedHashPartitionError(result, err);
                return .failed;
            };
        }
        builder.finish() catch |err| {
            setNestedHashPartitionError(result, err);
            return .failed;
        };

        var output_count: u16 = 0;
        var iter_arena_buf: [scan_mod.max_row_size_bytes + 192]u8 = undefined;
        var iter_arena = scan_mod.StringArena.init(&iter_arena_buf);
        var cache_arena = scan_mod.StringArena.init(ctx.nested_match_arena_bytes);
        var right_cache = RightPartitionHashCacheState{};
        var right_row = ResultRow.init();
        for (left_copy[0..left_count]) |left_row| {
            if (join.left_key_index >= left_row.column_count) {
                setError(result, "join key out of bounds");
                return .failed;
            }
            const total_columns_with_right = @as(usize, left_row.column_count) +
                @as(usize, target_schema.column_count);
            if (total_columns_with_right > scan_mod.max_columns) {
                setError(result, "join column capacity exceeded");
                return .failed;
            }
            const partition = hash_join_mod.partitionForKey(
                left_row.values[join.left_key_index],
                descriptor.partition_count,
            ) catch |err| {
                setNestedHashPartitionError(result, err);
                return .failed;
            };

            var matched = false;
            if (descriptor.partition_row_counts[partition] > 0) {
                if (!prepareRightPartitionHashCache(
                    result,
                    &temp_mgr,
                    &descriptor,
                    join,
                    caps,
                    partition,
                    ctx.nested_rows,
                    &cache_arena,
                    &right_cache,
                )) return .failed;
                if (right_cache.valid) {
                    var matches = right_cache.index.matchIterator(
                        left_row.values[join.left_key_index],
                    );
                    while (matches.next()) |cached_right| {
                        matched = true;
                        if (@as(usize, output_count) >= caps.join_output_rows) {
                            setError(result, "join output row capacity exceeded");
                            return .failed;
                        }
                        const total_columns = @as(usize, left_row.column_count) +
                            @as(usize, cached_right.column_count);
                        if (total_columns > scan_mod.max_columns) {
                            setError(result, "join column capacity exceeded");
                            return .failed;
                        }
                        var out = ResultRow.init();
                        out.column_count = @intCast(total_columns);
                        out.row_id = left_row.row_id;
                        @memcpy(
                            out.values[0..left_row.column_count],
                            left_row.values[0..left_row.column_count],
                        );
                        var nested_col: u16 = 0;
                        while (nested_col < cached_right.column_count) : (nested_col += 1) {
                            out.values[left_row.column_count + nested_col] = copyNestedValueIntoArena(
                                cached_right.values[nested_col],
                                string_arena,
                            ) catch {
                                setError(result, "nested relation output string arena exhausted");
                                return .failed;
                            };
                        }
                        result.rows[output_count] = out;
                        output_count += 1;
                    }
                } else {
                    var partition_iter = hash_join_mod.PartitionRowIterator.init(
                        &temp_mgr,
                        &descriptor,
                        partition,
                    ) catch |err| {
                        setNestedHashPartitionError(result, err);
                        return .failed;
                    };
                    while (true) {
                        iter_arena.reset();
                        const has_right = partition_iter.next(&right_row, &iter_arena) catch |err| {
                            setNestedHashPartitionError(result, err);
                            return .failed;
                        };
                        if (!has_right) break;
                        if (row_mod.compareValues(
                            left_row.values[join.left_key_index],
                            right_row.values[join.right_key_index],
                        ) != .eq) continue;
                        matched = true;
                        if (@as(usize, output_count) >= caps.join_output_rows) {
                            setError(result, "join output row capacity exceeded");
                            return .failed;
                        }
                        const total_columns = @as(usize, left_row.column_count) +
                            @as(usize, right_row.column_count);
                        if (total_columns > scan_mod.max_columns) {
                            setError(result, "join column capacity exceeded");
                            return .failed;
                        }
                        var out = ResultRow.init();
                        out.column_count = @intCast(total_columns);
                        out.row_id = left_row.row_id;
                        @memcpy(
                            out.values[0..left_row.column_count],
                            left_row.values[0..left_row.column_count],
                        );
                        var nested_col: u16 = 0;
                        while (nested_col < right_row.column_count) : (nested_col += 1) {
                            out.values[left_row.column_count + nested_col] = copyNestedValueIntoArena(
                                right_row.values[nested_col],
                                string_arena,
                            ) catch {
                                setError(result, "nested relation output string arena exhausted");
                                return .failed;
                            };
                        }
                        result.rows[output_count] = out;
                        output_count += 1;
                    }
                }
            }
            if (!matched) {
                if (@as(usize, output_count) >= caps.join_output_rows) {
                    setError(result, "join output row capacity exceeded");
                    return .failed;
                }
                var out = ResultRow.init();
                out.column_count = @intCast(total_columns_with_right);
                out.row_id = left_row.row_id;
                @memcpy(
                    out.values[0..left_row.column_count],
                    left_row.values[0..left_row.column_count],
                );
                var null_col: u16 = 0;
                while (null_col < target_schema.column_count) : (null_col += 1) {
                    out.values[left_row.column_count + null_col] = .{ .null_value = {} };
                }
                result.rows[output_count] = out;
                output_count += 1;
            }
        }
        result.row_count = output_count;
        recordNestedHashSpillJoinPlan(&result.stats.plan);
        return .applied;
    }

    const right_rows = ctx.scratch_rows_a[0..right_chunk.row_count];
    if (!joins_mod.executeLeftJoinHashFlat(
        result,
        left_copy[0..left_count],
        right_rows,
        join,
        target_schema.column_count,
        caps,
    )) return .failed;

    const left_column_count = left_copy[0].column_count;
    var row_idx: u16 = 0;
    while (row_idx < result.row_count) : (row_idx += 1) {
        var col_idx = left_column_count;
        while (col_idx < result.rows[row_idx].column_count) : (col_idx += 1) {
            result.rows[row_idx].values[col_idx] = switch (result.rows[row_idx].values[col_idx]) {
                .string => |text| .{ .string = string_arena.copyString(text) catch {
                    setError(result, "nested relation output string arena exhausted");
                    return .failed;
                } },
                else => result.rows[row_idx].values[col_idx],
            };
        }
    }

    recordNestedHashJoinPlan(&result.stats.plan);
    return .applied;
}

fn tryApplyNestedSelectionHashJoinCollectorNoOps(
    ctx: *const ExecContext,
    result: *QueryResult,
    collector: *SpillingResultCollector,
    target_model_id: ModelId,
    join: JoinDescriptor,
    target_schema: *const RowSchema,
    caps: *const capacity_mod.OperatorCapacities,
) NestedHashFastPathOutcome {
    var decode_arena = scan_mod.StringArena.init(ctx.nested_decode_arena_bytes);
    var right_cursor = scan_mod.ScanCursor.init();
    const right_chunk = scan_mod.tableScanInto(
        ctx.catalog,
        ctx.pool,
        ctx.undo_log,
        ctx.snapshot,
        ctx.tx_manager,
        target_model_id,
        ctx.scratch_rows_a[0..scan_mod.scan_batch_size],
        &decode_arena,
        &right_cursor,
    ) catch |err| {
        setBoundaryError(
            result,
            "nested relation scan failed",
            runtime_errors.classifyScan(err),
            err,
        );
        return .failed;
    };
    result.stats.pages_read +|= right_chunk.pages_read;
    if (!right_cursor.done) {
        var temp_mgr = TempStorageManager.init(
            ctx.query_slot_index,
            ctx.storage,
            ctx.temp_pages_per_query_slot,
            temp_mod.nested_region_start_page_id,
        ) catch {
            setError(result, "nested relation hash spill temp init failed");
            return .failed;
        };
        var descriptor = hash_join_mod.PartitionSpillDescriptor.init(
            hash_join_mod.max_partitions,
        ) catch |err| {
            setNestedHashPartitionError(result, err);
            return .failed;
        };
        var builder = hash_join_mod.PartitionSpillBuilder.init(
            &temp_mgr,
            join.right_key_index,
            &descriptor,
        );
        builder.appendRows(ctx.scratch_rows_a[0..right_chunk.row_count]) catch |err| {
            setNestedHashPartitionError(result, err);
            return .failed;
        };
        while (!right_cursor.done) {
            decode_arena.reset();
            const chunk = scan_mod.tableScanInto(
                ctx.catalog,
                ctx.pool,
                ctx.undo_log,
                ctx.snapshot,
                ctx.tx_manager,
                target_model_id,
                ctx.scratch_rows_a[0..scan_mod.scan_batch_size],
                &decode_arena,
                &right_cursor,
            ) catch |err| {
                setBoundaryError(
                    result,
                    "nested relation scan failed",
                    runtime_errors.classifyScan(err),
                    err,
                );
                return .failed;
            };
            result.stats.pages_read +|= chunk.pages_read;
            builder.appendRows(ctx.scratch_rows_a[0..chunk.row_count]) catch |err| {
                setNestedHashPartitionError(result, err);
                return .failed;
            };
        }
        builder.finish() catch |err| {
            setNestedHashPartitionError(result, err);
            return .failed;
        };

        var output_page_ids: [max_spill_pages]u64 = undefined;
        var output_page_count: u32 = 0;
        var output_rows: u64 = 0;
        var writer = SpillPageWriter.init();

        var left_iter = collector.iterator();
        var left_arena_buf: [scan_mod.max_row_size_bytes + 192]u8 = undefined;
        var left_arena = scan_mod.StringArena.init(&left_arena_buf);
        var left_row = ResultRow.init();
        var iter_arena_buf: [scan_mod.max_row_size_bytes + 192]u8 = undefined;
        var iter_arena = scan_mod.StringArena.init(&iter_arena_buf);
        var cache_arena = scan_mod.StringArena.init(ctx.nested_match_arena_bytes);
        var right_cache = RightPartitionHashCacheState{};
        var right_row = ResultRow.init();
        var skip = result.collector_output_offset;
        var remaining = result.collector_output_count;

        while (remaining > 0) {
            left_arena.reset();
            const has_left = left_iter.next(&left_row, &left_arena) catch {
                setError(result, "nested relation left-side spill read failed");
                return .failed;
            };
            if (!has_left) break;
            if (skip > 0) {
                skip -= 1;
                continue;
            }
            remaining -= 1;
            if (join.left_key_index >= left_row.column_count) {
                setError(result, "join key out of bounds");
                return .failed;
            }

            const partition = hash_join_mod.partitionForKey(
                left_row.values[join.left_key_index],
                descriptor.partition_count,
            ) catch |err| {
                setNestedHashPartitionError(result, err);
                return .failed;
            };

            var matched = false;
            if (descriptor.partition_row_counts[partition] > 0) {
                if (!prepareRightPartitionHashCache(
                    result,
                    &temp_mgr,
                    &descriptor,
                    join,
                    caps,
                    partition,
                    ctx.nested_rows,
                    &cache_arena,
                    &right_cache,
                )) return .failed;
                if (right_cache.valid) {
                    var matches = right_cache.index.matchIterator(
                        left_row.values[join.left_key_index],
                    );
                    while (matches.next()) |cached_right| {
                        matched = true;
                        if (!appendNestedCollectorOutputRow(
                            result,
                            collector,
                            &writer,
                            &output_page_ids,
                            &output_page_count,
                            &output_rows,
                            &left_row,
                            cached_right,
                            target_schema.column_count,
                        )) return .failed;
                    }
                } else {
                    var partition_iter = hash_join_mod.PartitionRowIterator.init(
                        &temp_mgr,
                        &descriptor,
                        partition,
                    ) catch |err| {
                        setNestedHashPartitionError(result, err);
                        return .failed;
                    };
                    while (true) {
                        iter_arena.reset();
                        const has_right = partition_iter.next(&right_row, &iter_arena) catch |err| {
                            setNestedHashPartitionError(result, err);
                            return .failed;
                        };
                        if (!has_right) break;
                        if (row_mod.compareValues(
                            left_row.values[join.left_key_index],
                            right_row.values[join.right_key_index],
                        ) != .eq) continue;
                        matched = true;
                        if (!appendNestedCollectorOutputRow(
                            result,
                            collector,
                            &writer,
                            &output_page_ids,
                            &output_page_count,
                            &output_rows,
                            &left_row,
                            &right_row,
                            target_schema.column_count,
                        )) return .failed;
                    }
                }
            }
            if (!matched) {
                if (!appendNestedCollectorOutputRow(
                    result,
                    collector,
                    &writer,
                    &output_page_ids,
                    &output_page_count,
                    &output_rows,
                    &left_row,
                    null,
                    target_schema.column_count,
                )) return .failed;
            }
        }

        if (writer.row_count > 0) {
            if (output_page_count >= max_spill_pages) {
                setError(result, "nested relation spill page tracking overflow");
                return .failed;
            }
            const payload = writer.finalize();
            const page_id = collector.temp_mgr.allocateAndWrite(payload, temp_mod.TempPage.null_page_id) catch {
                setError(result, "nested relation spill write failed");
                return .failed;
            };
            output_page_ids[output_page_count] = page_id;
            output_page_count += 1;
        }

        @memcpy(
            collector.spill_page_ids[0..output_page_count],
            output_page_ids[0..output_page_count],
        );
        collector.spill_page_count = output_page_count;
        collector.hot_count = 0;
        collector.hot_bytes = 0;
        collector.total_rows = output_rows;
        collector.iteration_started = false;

        result.collector = collector;
        result.collector_output_offset = 0;
        result.collector_output_count = output_rows;
        result.row_count = @intCast(@min(output_rows, scan_mod.scan_batch_size));
        recordNestedHashSpillJoinPlan(&result.stats.plan);
        return .applied;
    }

    const right_rows = ctx.scratch_rows_a[0..right_chunk.row_count];
    const hash_index = LeftHashIndex.init(
        right_rows,
        .{
            .left_key_index = join.left_key_index,
            .right_key_index = join.right_key_index,
        },
        caps,
    ) catch |err| {
        setError(result, switch (err) {
            error.BuildRowCapacityExceeded => "join build row capacity exceeded",
            error.StateCapacityExceeded => "join state capacity exceeded",
            error.LeftKeyOutOfBounds, error.RightKeyOutOfBounds => "join key out of bounds",
            error.JoinColumnCapacityExceeded => "join column capacity exceeded",
            error.OutputRowCapacityExceeded => "join output row capacity exceeded",
        });
        return .failed;
    };

    var output_page_ids: [max_spill_pages]u64 = undefined;
    var output_page_count: u32 = 0;
    var output_rows: u64 = 0;
    var writer = SpillPageWriter.init();

    var left_iter = collector.iterator();
    var left_arena_buf: [scan_mod.max_row_size_bytes + 192]u8 = undefined;
    var left_arena = scan_mod.StringArena.init(&left_arena_buf);
    var left_row = ResultRow.init();
    var skip = result.collector_output_offset;
    var remaining = result.collector_output_count;

    while (remaining > 0) {
        left_arena.reset();
        const has_left = left_iter.next(&left_row, &left_arena) catch {
            setError(result, "nested relation left-side spill read failed");
            return .failed;
        };
        if (!has_left) break;
        if (skip > 0) {
            skip -= 1;
            continue;
        }
        remaining -= 1;
        if (join.left_key_index >= left_row.column_count) {
            setError(result, "join key out of bounds");
            return .failed;
        }

        var matched = false;
        var matches = hash_index.matchIterator(left_row.values[join.left_key_index]);
        while (matches.next()) |right_row| {
            matched = true;
            if (!appendNestedCollectorOutputRow(
                result,
                collector,
                &writer,
                &output_page_ids,
                &output_page_count,
                &output_rows,
                &left_row,
                right_row,
                target_schema.column_count,
            )) return .failed;
        }
        if (!matched) {
            if (!appendNestedCollectorOutputRow(
                result,
                collector,
                &writer,
                &output_page_ids,
                &output_page_count,
                &output_rows,
                &left_row,
                null,
                target_schema.column_count,
            )) return .failed;
        }
    }

    if (writer.row_count > 0) {
        if (output_page_count >= max_spill_pages) {
            setError(result, "nested relation spill page tracking overflow");
            return .failed;
        }
        const payload = writer.finalize();
        const page_id = collector.temp_mgr.allocateAndWrite(payload, temp_mod.TempPage.null_page_id) catch {
            setError(result, "nested relation spill write failed");
            return .failed;
        };
        output_page_ids[output_page_count] = page_id;
        output_page_count += 1;
    }

    @memcpy(
        collector.spill_page_ids[0..output_page_count],
        output_page_ids[0..output_page_count],
    );
    collector.spill_page_count = output_page_count;
    collector.hot_count = 0;
    collector.hot_bytes = 0;
    collector.total_rows = output_rows;
    collector.iteration_started = false;

    result.collector = collector;
    result.collector_output_offset = 0;
    result.collector_output_count = output_rows;
    result.row_count = @intCast(@min(output_rows, scan_mod.scan_batch_size));
    recordNestedHashJoinPlan(&result.stats.plan);
    return .applied;
}

fn collectNestedMatchesForParent(
    ctx: *const ExecContext,
    result: *QueryResult,
    target_model_id: ModelId,
    right_key_index: u16,
    left_key: Value,
    decode_arena: *scan_mod.StringArena,
    match_arena: *scan_mod.StringArena,
    parent_collector: *SpillingResultCollector,
    out_row_set: *NestedRowSet,
    out_pages_read: *u32,
) bool {
    const nested_scan_chunk_rows: usize = 64;
    parent_collector.reset();
    out_row_set.* = .{
        .flat = .{
            .rows = parent_collector.hot_batch,
            .count = 0,
        },
    };
    out_pages_read.* = 0;
    var right_cursor = scan_mod.ScanCursor.init();

    while (!right_cursor.done) {
        decode_arena.reset();
        const chunk = scan_mod.tableScanInto(
            ctx.catalog,
            ctx.pool,
            ctx.undo_log,
            ctx.snapshot,
            ctx.tx_manager,
            target_model_id,
            ctx.scratch_rows_a[0..nested_scan_chunk_rows],
            decode_arena,
            &right_cursor,
        ) catch |err| {
            setBoundaryError(
                result,
                "nested relation scan failed",
                runtime_errors.classifyScan(err),
                err,
            );
            return false;
        };
        out_pages_read.* +|= chunk.pages_read;

        var right_idx: u16 = 0;
        while (right_idx < chunk.row_count) : (right_idx += 1) {
            const right_row = ctx.scratch_rows_a[right_idx];
            if (row_mod.compareValues(left_key, right_row.values[right_key_index]) != .eq) {
                continue;
            }
            var copied = ResultRow.init();
            copyNestedMatchRowIntoArena(
                &right_row,
                &copied,
                match_arena,
            ) catch {
                setError(result, "nested relation parent string arena exhausted");
                return false;
            };
            parent_collector.appendRow(&copied) catch {
                setError(result, "nested relation parent-local spill append failed");
                return false;
            };
        }
    }

    if (parent_collector.spillTriggered()) {
        out_row_set.* = .{
            .spill = .{
                .collector = parent_collector,
                .offset = 0,
                .count = parent_collector.totalRowCount(),
            },
        };
    } else {
        out_row_set.* = .{
            .flat = .{
                .rows = parent_collector.hot_batch,
                .count = parent_collector.hot_count,
            },
        };
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
    const target_schema = &ctx.catalog.models[target_model_id].row_schema;
    var nested_match_arena = scan_mod.StringArena.init(
        ctx.nested_match_arena_bytes,
    );

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
    const join = inferAssociationJoinDescriptor(
        ctx.catalog,
        source_model_id,
        assoc,
        result,
    ) orelse return false;
    if (nested_op_count == 0) {
        return switch (tryApplyNestedSelectionHashJoinFlatNoOps(
            ctx,
            result,
            target_model_id,
            join,
            target_schema,
            caps,
            string_arena,
        )) {
            .applied => true,
            .failed => false,
        };
    }
    return switch (tryApplyNestedSelectionHashJoinFlatWithOps(
        ctx,
        result,
        target_model_id,
        join,
        target_schema,
        &nested_ops,
        nested_op_count,
        caps,
        &nested_match_arena,
        string_arena,
    )) {
        .applied => true,
        .failed => false,
    };
}

/// Apply nested child pipeline operators to a single parent's child subset.
///
/// Uses insertion sort for `sort_op` to avoid mutating `ctx.scratch_rows_b`,
/// which is reserved for left-row preservation in nested joins.
fn applyNestedReadOperatorsPerParent(
    ctx: *const ExecContext,
    result: *QueryResult,
    row_set: *NestedRowSet,
    model_id: ModelId,
    ops: *const [max_operators]OpDescriptor,
    op_count: u16,
    caps: *const capacity_mod.OperatorCapacities,
    string_arena: *scan_mod.StringArena,
) bool {
    var active_row_set = row_set.*;
    switch (active_row_set) {
        .flat => |flat| {
            result.rows = flat.rows;
            result.row_count = flat.count;
            result.collector = null;
            result.collector_output_offset = 0;
            result.collector_output_count = 0;
        },
        .spill => |spill| {
            result.collector = spill.collector;
            result.collector_output_offset = spill.offset;
            result.collector_output_count = spill.count;
            // Collector-backed row sets do not have materialized in-memory rows.
            // Keep row_count at 0 so aggregate-state budgeting does not treat
            // spill cardinality as resident in `result.rows`.
            result.row_count = 0;
        },
    }

    var group_runtime = GroupRuntime{};
    const schema = &ctx.catalog.models[model_id].row_schema;
    var i: u16 = 0;
    while (i < op_count) : (i += 1) {
        const op = ops[i];
        switch (active_row_set) {
            .flat => {
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
                        if (!applySortNoScratch(
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
                active_row_set = .{
                    .flat = .{
                        .rows = result.rows,
                        .count = result.row_count,
                    },
                };
            },
            .spill => |spill| {
                result.collector = spill.collector;
                result.collector_output_offset = spill.offset;
                result.collector_output_count = spill.count;
                switch (op.kind) {
                    .where_filter, .having_filter, .limit_op, .offset_op => {
                        var single_op_list: [max_operators]OpDescriptor = undefined;
                        single_op_list[0] = op;
                        if (!rewriteCollectorForPostOps(
                            ctx,
                            result,
                            model_id,
                            &single_op_list,
                            1,
                            string_arena,
                        )) return false;
                    },
                    .group_op => {
                        const spill_collector = result.collector orelse {
                            setError(result, "nested spill group requires collector");
                            return false;
                        };
                        if (!hash_aggregate_mod.applyHashAggregate(
                            ctx,
                            result,
                            spill_collector,
                            op.node,
                            i,
                            schema,
                            ops,
                            op_count,
                            caps,
                            &group_runtime,
                            string_arena,
                        )) return false;
                    },
                    .sort_op => {
                        const spill_collector = result.collector orelse {
                            setError(result, "nested spill sort requires collector");
                            return false;
                        };
                        if (!external_sort_mod.applyExternalSort(
                            ctx,
                            result,
                            spill_collector,
                            op.node,
                            schema,
                            string_arena,
                        )) return false;
                    },
                    .inspect_op => {},
                    .insert_op, .update_op, .delete_op => {},
                }

                if (result.collector) |current_collector| {
                    active_row_set = .{
                        .spill = .{
                            .collector = current_collector,
                            .offset = 0,
                            .count = current_collector.totalRowCount(),
                        },
                    };
                } else {
                    active_row_set = .{
                        .flat = .{
                            .rows = result.rows,
                            .count = result.row_count,
                        },
                    };
                }
            },
        }
        if (result.has_error) return false;
    }

    row_set.* = active_row_set;
    return true;
}

/// Stable insertion sort variant for nested child subsets.
///
/// This path avoids `ctx.scratch_rows_b` usage so nested joins can preserve
/// left rows while still honoring per-parent `sort(...)` semantics.
fn applySortNoScratch(
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

    var sort_keys: [capacity_mod.max_sort_keys]sorting_mod.SortKeyDescriptor = undefined;
    if (!sorting_mod.buildSortKeyDescriptors(
        ctx,
        result,
        node.data.unary,
        schema,
        sort_keys[0..],
        key_count,
    )) return false;

    var i: u16 = 1;
    while (i < result.row_count) : (i += 1) {
        var j = i;
        while (j > 0) {
            const order = sorting_mod.compareRowsBySortKeys(
                ctx,
                schema,
                group_runtime,
                j - 1,
                j,
                &result.rows[j - 1],
                &result.rows[j],
                sort_keys[0..key_count],
                string_arena,
            ) catch {
                setError(result, "sort key evaluation failed");
                return false;
            };
            if (order != .gt) break;

            const tmp = result.rows[j - 1];
            result.rows[j - 1] = result.rows[j];
            result.rows[j] = tmp;

            if (group_runtime.active) {
                const ctmp = group_runtime.group_counts[j - 1];
                group_runtime.group_counts[j - 1] = group_runtime.group_counts[j];
                group_runtime.group_counts[j] = ctmp;
            }
            j -= 1;
        }
    }
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
    const predicate_scope: []const u8 = switch (node.tag) {
        .op_having => "having",
        else => "where",
    };
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
                    setPredicateUndefinedParameterError(result, predicate_scope);
                    return;
                },
                error.ClockUnavailable => {
                    setPredicateClockUnavailableError(result, predicate_scope);
                    return;
                },
                error.TypeMismatch => {
                    setPredicateMustBeBooleanError(result, predicate_scope);
                    return;
                },
                error.UndefinedVariable => {
                    setPredicateUndefinedVariableError(result, predicate_scope);
                    return;
                },
                error.AmbiguousIdentifier => {
                    setPredicateAmbiguousIdentifierError(result, predicate_scope);
                    return;
                },
                error.VariableTypeMismatch => {
                    setPredicateVariableTypeMismatchError(result, predicate_scope);
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
                    setPredicateUndefinedParameterError(result, predicate_scope);
                    return;
                },
                error.ClockUnavailable => {
                    setPredicateClockUnavailableError(result, predicate_scope);
                    return;
                },
                error.TypeMismatch => {
                    setPredicateMustBeBooleanError(result, predicate_scope);
                    return;
                },
                error.UndefinedVariable => {
                    setPredicateUndefinedVariableError(result, predicate_scope);
                    return;
                },
                error.AmbiguousIdentifier => {
                    setPredicateAmbiguousIdentifierError(result, predicate_scope);
                    return;
                },
                error.VariableTypeMismatch => {
                    setPredicateVariableTypeMismatchError(result, predicate_scope);
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

fn setPredicateMustBeBooleanError(result: *QueryResult, scope: []const u8) void {
    var msg_buf: [96]u8 = undefined;
    const msg = std.fmt.bufPrint(
        msg_buf[0..],
        "{s} expression must evaluate to boolean",
        .{scope},
    ) catch {
        setError(result, "predicate expression must evaluate to boolean");
        return;
    };
    setError(result, msg);
}

fn setPredicateUndefinedParameterError(result: *QueryResult, scope: []const u8) void {
    var msg_buf: [96]u8 = undefined;
    const msg = std.fmt.bufPrint(
        msg_buf[0..],
        "undefined parameter in {s} expression",
        .{scope},
    ) catch {
        setError(result, "undefined parameter in predicate expression");
        return;
    };
    setError(result, msg);
}

fn setPredicateUndefinedVariableError(result: *QueryResult, scope: []const u8) void {
    var msg_buf: [96]u8 = undefined;
    const msg = std.fmt.bufPrint(
        msg_buf[0..],
        "undefined variable in {s} expression",
        .{scope},
    ) catch {
        setError(result, "undefined variable in predicate expression");
        return;
    };
    setError(result, msg);
}

fn setPredicateAmbiguousIdentifierError(result: *QueryResult, scope: []const u8) void {
    var msg_buf: [112]u8 = undefined;
    const msg = std.fmt.bufPrint(
        msg_buf[0..],
        "ambiguous identifier in {s} expression",
        .{scope},
    ) catch {
        setError(result, "ambiguous identifier in predicate expression");
        return;
    };
    setError(result, msg);
}

fn setPredicateVariableTypeMismatchError(result: *QueryResult, scope: []const u8) void {
    var msg_buf: [128]u8 = undefined;
    const msg = std.fmt.bufPrint(
        msg_buf[0..],
        "variable type mismatch in {s} expression",
        .{scope},
    ) catch {
        setError(result, "variable type mismatch in predicate expression");
        return;
    };
    setError(result, msg);
}

fn setPredicateClockUnavailableError(result: *QueryResult, scope: []const u8) void {
    var msg_buf: [96]u8 = undefined;
    const msg = std.fmt.bufPrint(
        msg_buf[0..],
        "clock unavailable in {s} expression",
        .{scope},
    ) catch {
        setError(result, "clock unavailable in predicate expression");
        return;
    };
    setError(result, msg);
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
            if (node.data.unary != null_node and ctx.ast.getNode(node.data.unary).tag == .insert_row_group) {
                var row_ids: [scan_mod.scan_batch_size]RowId = undefined;
                var row_id_count: u16 = 0;
                const inserted_count = mutation_mod.executeBulkInsertWithDiagnosticAndParameters(
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
                    ctx.undo_log,
                    ctx.snapshot,
                    ctx.tx_manager,
                    row_ids[0..],
                    &row_id_count,
                ) catch |err| {
                    setMutationBoundaryError(result, ctx, .insert_op, err, &diagnostic);
                    return;
                };
                result.stats.rows_inserted = inserted_count;
                if (has_projection) {
                    string_arena.reset();
                    materializeRowsById(
                        ctx,
                        result,
                        model_id,
                        row_ids[0..row_id_count],
                        string_arena,
                    );
                }
            } else {
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
                    ctx.undo_log,
                    ctx.snapshot,
                    ctx.tx_manager,
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
                switch (err) {
                    error.PredicateMustBeBoolean => {
                        setPredicateMustBeBooleanError(result, "where");
                        return;
                    },
                    error.PredicateUndefinedParameter => {
                        setPredicateUndefinedParameterError(result, "where");
                        return;
                    },
                    error.PredicateClockUnavailable => {
                        setPredicateClockUnavailableError(result, "where");
                        return;
                    },
                    else => {},
                }
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
                switch (err) {
                    error.PredicateMustBeBoolean => {
                        setPredicateMustBeBooleanError(result, "where");
                        return;
                    },
                    error.PredicateUndefinedParameter => {
                        setPredicateUndefinedParameterError(result, "where");
                        return;
                    },
                    error.PredicateClockUnavailable => {
                        setPredicateClockUnavailableError(result, "where");
                        return;
                    },
                    else => {},
                }
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
                    setPredicateUndefinedParameterError(out, "where");
                    return false;
                },
                error.ClockUnavailable => {
                    setPredicateClockUnavailableError(out, "where");
                    return false;
                },
                error.TypeMismatch => {
                    setPredicateMustBeBooleanError(out, "where");
                    return false;
                },
                error.UndefinedVariable => {
                    setPredicateUndefinedVariableError(out, "where");
                    return false;
                },
                error.AmbiguousIdentifier => {
                    setPredicateAmbiguousIdentifierError(out, "where");
                    return false;
                },
                error.VariableTypeMismatch => {
                    setPredicateVariableTypeMismatchError(out, "where");
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

fn incrementPlanCounter(counter: *u8) void {
    if (counter.* < std.math.maxInt(u8)) {
        counter.* += 1;
    }
}

fn recordNestedHashJoinPlan(plan: *PlanStats) void {
    incrementPlanCounter(&plan.nested_relation_count);
    incrementPlanCounter(&plan.nested_join_hash_in_memory_count);
    plan.join_strategy = .hash_in_memory;
    plan.join_order = .source_then_nested;
    plan.materialization_mode = .bounded_row_buffers;
}

fn recordNestedHashSpillJoinPlan(plan: *PlanStats) void {
    incrementPlanCounter(&plan.nested_relation_count);
    incrementPlanCounter(&plan.nested_join_hash_spill_count);
    plan.join_strategy = .hash_spill;
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
    nested_rows: []ResultRow,
    string_arena_bytes: []u8,
    nested_decode_arena_bytes: []u8,
    nested_match_arena_bytes: []u8,
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
        self.nested_rows = try testing.allocator.alloc(
            ResultRow,
            scan_mod.scan_batch_size,
        );
        errdefer testing.allocator.free(self.nested_rows);
        self.string_arena_bytes = try testing.allocator.alloc(
            u8,
            scan_mod.default_string_arena_bytes,
        );
        errdefer testing.allocator.free(self.string_arena_bytes);
        self.nested_decode_arena_bytes = try testing.allocator.alloc(
            u8,
            512 * 1024,
        );
        errdefer testing.allocator.free(self.nested_decode_arena_bytes);
        self.nested_match_arena_bytes = try testing.allocator.alloc(
            u8,
            512 * 1024,
        );
        errdefer testing.allocator.free(self.nested_match_arena_bytes);
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
        testing.allocator.free(self.nested_rows);
        testing.allocator.free(self.result_rows);
        testing.allocator.free(self.nested_decode_arena_bytes);
        testing.allocator.free(self.nested_match_arena_bytes);
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
            .parameter_bindings = &.{},
            .allocator = testing.allocator,
            .result_rows = self.result_rows,
            .scratch_rows_a = self.scratch_rows_a,
            .scratch_rows_b = self.scratch_rows_b,
            .nested_rows = self.nested_rows,
            .string_arena_bytes = self.string_arena_bytes,
            .nested_decode_arena_bytes = self.nested_decode_arena_bytes,
            .nested_match_arena_bytes = self.nested_match_arena_bytes,
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

fn expectResultRowsEqual(a: *const QueryResult, b: *const QueryResult) !void {
    try testing.expectEqual(a.row_count, b.row_count);
    var row_idx: u16 = 0;
    while (row_idx < a.row_count) : (row_idx += 1) {
        const lhs = a.rows[row_idx];
        const rhs = b.rows[row_idx];
        try testing.expectEqual(lhs.column_count, rhs.column_count);
        try testing.expectEqual(lhs.row_id, rhs.row_id);
        var col_idx: u16 = 0;
        while (col_idx < lhs.column_count) : (col_idx += 1) {
            try testing.expectEqual(
                std.math.Order.eq,
                row_mod.compareValues(lhs.values[col_idx], rhs.values[col_idx]),
            );
        }
    }
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
    try testing.expect(std.mem.indexOf(u8, msg, "undefined parameter in where expression") != null);
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
        SortStrategy.in_memory_merge,
        result.stats.plan.sort_strategy,
    );
    try testing.expectEqual(
        GroupStrategy.none,
        result.stats.plan.group_strategy,
    );
    try testing.expectEqual(@as(u8, 0), result.stats.plan.nested_relation_count);
    try testing.expectEqual(@as(u8, 0), result.stats.plan.nested_join_nested_loop_count);
    try testing.expectEqual(@as(u8, 0), result.stats.plan.nested_join_hash_in_memory_count);
    try testing.expectEqual(@as(u8, 0), result.stats.plan.nested_join_hash_spill_count);
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
        JoinStrategy.hash_in_memory,
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
    try testing.expectEqual(@as(u8, 0), result.stats.plan.nested_join_nested_loop_count);
    try testing.expectEqual(@as(u8, 1), result.stats.plan.nested_join_hash_in_memory_count);
    try testing.expectEqual(@as(u8, 0), result.stats.plan.nested_join_hash_spill_count);
}

test "execute nested relation with child operators uses hash in-memory strategy" {
    var env: ExecTestEnv = undefined;
    try env.init();
    defer env.deinit();

    const post_model = try env.catalog.addModel("Post");
    _ = try env.catalog.addColumn(post_model, "id", .i64, false);
    _ = try env.catalog.addColumn(post_model, "user_id", .i64, false);
    env.catalog.models[post_model].heap_first_page_id = 127;
    env.catalog.models[post_model].total_pages = 1;
    _ = try env.catalog.addAssociation(
        env.model_id,
        "posts",
        AssociationKind.has_many,
        "Post",
    );
    try env.catalog.resolveAssociations();

    const post_page = try env.pool.pin(127);
    heap_mod.HeapPage.init(post_page);
    env.pool.unpin(127, true);

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

    const src = "User |> sort(id asc) { id posts |> where(id > 0) |> sort(id asc) { id } }";
    const tok = tokenizer_mod.tokenize(src);
    const p = parser_mod.parse(&tok, src);
    try testing.expect(!p.has_error);
    var result = try execute(&env.makeCtx(tx, &snap, &p.ast, &tok, src));
    defer result.deinit();

    try testing.expect(!result.has_error);
    try testing.expectEqual(@as(u16, 3), result.row_count);
    try testing.expectEqual(@as(i64, 1), result.rows[0].values[0].i64);
    try testing.expectEqual(@as(i64, 10), result.rows[0].values[1].i64);
    try testing.expectEqual(@as(i64, 1), result.rows[1].values[0].i64);
    try testing.expectEqual(@as(i64, 20), result.rows[1].values[1].i64);
    try testing.expectEqual(@as(i64, 2), result.rows[2].values[0].i64);
    try testing.expectEqual(@as(i64, 15), result.rows[2].values[1].i64);
    try testing.expectEqual(
        JoinStrategy.hash_in_memory,
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
    try testing.expectEqual(@as(u8, 0), result.stats.plan.nested_join_nested_loop_count);
    try testing.expectEqual(@as(u8, 1), result.stats.plan.nested_join_hash_in_memory_count);
    try testing.expectEqual(@as(u8, 0), result.stats.plan.nested_join_hash_spill_count);
}

test "execute nested relation without child operators uses hash in-memory strategy" {
    var env: ExecTestEnv = undefined;
    try env.init();
    defer env.deinit();

    const post_model = try env.catalog.addModel("Post");
    _ = try env.catalog.addColumn(post_model, "id", .i64, false);
    _ = try env.catalog.addColumn(post_model, "user_id", .i64, false);
    env.catalog.models[post_model].heap_first_page_id = 123;
    env.catalog.models[post_model].total_pages = 1;
    _ = try env.catalog.addAssociation(
        env.model_id,
        "posts",
        AssociationKind.has_many,
        "Post",
    );
    try env.catalog.resolveAssociations();

    const post_page = try env.pool.pin(123);
    heap_mod.HeapPage.init(post_page);
    env.pool.unpin(123, true);

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

    const src = "User |> sort(id asc) { id posts { id } }";
    const tok = tokenizer_mod.tokenize(src);
    const p = parser_mod.parse(&tok, src);
    try testing.expect(!p.has_error);
    var result = try execute(&env.makeCtx(tx, &snap, &p.ast, &tok, src));
    defer result.deinit();

    try testing.expect(!result.has_error);
    try testing.expectEqual(@as(u16, 3), result.row_count);
    try testing.expectEqual(@as(i64, 1), result.rows[0].values[0].i64);
    try testing.expectEqual(@as(i64, 20), result.rows[0].values[1].i64);
    try testing.expectEqual(@as(i64, 1), result.rows[1].values[0].i64);
    try testing.expectEqual(@as(i64, 10), result.rows[1].values[1].i64);
    try testing.expectEqual(@as(i64, 2), result.rows[2].values[0].i64);
    try testing.expectEqual(@as(i64, 15), result.rows[2].values[1].i64);
    try testing.expectEqual(
        JoinStrategy.hash_in_memory,
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
    try testing.expectEqual(@as(u8, 0), result.stats.plan.nested_join_nested_loop_count);
    try testing.expectEqual(@as(u8, 1), result.stats.plan.nested_join_hash_in_memory_count);
    try testing.expectEqual(@as(u8, 0), result.stats.plan.nested_join_hash_spill_count);
    try testing.expectEqual(@as(u8, 1), result.stats.plan.nested_relation_count);
}

test "execute nested relation collector path with child operators uses hash in-memory strategy" {
    var env: ExecTestEnv = undefined;
    try env.init();
    defer env.deinit();

    const post_model = try env.catalog.addModel("Post");
    _ = try env.catalog.addColumn(post_model, "id", .i64, false);
    _ = try env.catalog.addColumn(post_model, "user_id", .i64, false);
    env.catalog.models[post_model].heap_first_page_id = 128;
    env.catalog.models[post_model].total_pages = 1;
    _ = try env.catalog.addAssociation(
        env.model_id,
        "posts",
        AssociationKind.has_many,
        "Post",
    );
    try env.catalog.resolveAssociations();

    const post_page = try env.pool.pin(128);
    heap_mod.HeapPage.init(post_page);
    env.pool.unpin(128, true);

    const tx = try env.tm.begin();
    var snap = try env.tm.snapshot(tx);
    defer snap.deinit();

    var user_id: u16 = 1;
    while (user_id <= 48) : (user_id += 1) {
        var user_buf: [160]u8 = undefined;
        const user_src = try std.fmt.bufPrint(
            user_buf[0..],
            "User |> insert(id = {d}, name = \"user-{d}-payload\", active = true)",
            .{ user_id, user_id },
        );
        const user_tok = tokenizer_mod.tokenize(user_src);
        const user_p = parser_mod.parse(&user_tok, user_src);
        try testing.expect(!user_p.has_error);
        var user_r = try execute(&env.makeCtx(tx, &snap, &user_p.ast, &user_tok, user_src));
        defer user_r.deinit();
        try testing.expect(!user_r.has_error);

        var post_buf: [128]u8 = undefined;
        const post_src = try std.fmt.bufPrint(
            post_buf[0..],
            "Post |> insert(id = {d}, user_id = {d})",
            .{ @as(u32, 2000) + user_id, user_id },
        );
        const post_tok = tokenizer_mod.tokenize(post_src);
        const post_p = parser_mod.parse(&post_tok, post_src);
        try testing.expect(!post_p.has_error);
        var post_r = try execute(&env.makeCtx(tx, &snap, &post_p.ast, &post_tok, post_src));
        defer post_r.deinit();
        try testing.expect(!post_r.has_error);
    }

    const src = "User |> sort(id asc) { id posts |> where(id > 0) |> sort(id asc) { id } }";
    const tok = tokenizer_mod.tokenize(src);
    const p = parser_mod.parse(&tok, src);
    try testing.expect(!p.has_error);
    var ctx = env.makeCtx(tx, &snap, &p.ast, &tok, src);
    ctx.work_memory_bytes_per_slot = 256;
    var result = try execute(&ctx);
    defer result.deinit();

    try testing.expect(!result.has_error);
    try testing.expectEqual(
        JoinStrategy.hash_in_memory,
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
}

test "execute nested relation collector path without child operators uses hash in-memory strategy" {
    var env: ExecTestEnv = undefined;
    try env.init();
    defer env.deinit();

    const post_model = try env.catalog.addModel("Post");
    _ = try env.catalog.addColumn(post_model, "id", .i64, false);
    _ = try env.catalog.addColumn(post_model, "user_id", .i64, false);
    env.catalog.models[post_model].heap_first_page_id = 124;
    env.catalog.models[post_model].total_pages = 1;
    _ = try env.catalog.addAssociation(
        env.model_id,
        "posts",
        AssociationKind.has_many,
        "Post",
    );
    try env.catalog.resolveAssociations();

    const post_page = try env.pool.pin(124);
    heap_mod.HeapPage.init(post_page);
    env.pool.unpin(124, true);

    const tx = try env.tm.begin();
    var snap = try env.tm.snapshot(tx);
    defer snap.deinit();

    var user_id: u16 = 1;
    while (user_id <= 48) : (user_id += 1) {
        var user_buf: [160]u8 = undefined;
        const user_src = try std.fmt.bufPrint(
            user_buf[0..],
            "User |> insert(id = {d}, name = \"user-{d}-payload\", active = true)",
            .{ user_id, user_id },
        );
        const user_tok = tokenizer_mod.tokenize(user_src);
        const user_p = parser_mod.parse(&user_tok, user_src);
        try testing.expect(!user_p.has_error);
        var user_r = try execute(&env.makeCtx(tx, &snap, &user_p.ast, &user_tok, user_src));
        defer user_r.deinit();
        try testing.expect(!user_r.has_error);

        var post_buf: [128]u8 = undefined;
        const post_src = try std.fmt.bufPrint(
            post_buf[0..],
            "Post |> insert(id = {d}, user_id = {d})",
            .{ @as(u32, 1000) + user_id, user_id },
        );
        const post_tok = tokenizer_mod.tokenize(post_src);
        const post_p = parser_mod.parse(&post_tok, post_src);
        try testing.expect(!post_p.has_error);
        var post_r = try execute(&env.makeCtx(tx, &snap, &post_p.ast, &post_tok, post_src));
        defer post_r.deinit();
        try testing.expect(!post_r.has_error);
    }

    const src = "User |> sort(id asc) { id posts { id } }";
    const tok = tokenizer_mod.tokenize(src);
    const p = parser_mod.parse(&tok, src);
    try testing.expect(!p.has_error);
    var ctx = env.makeCtx(tx, &snap, &p.ast, &tok, src);
    ctx.work_memory_bytes_per_slot = 256;
    var result = try execute(&ctx);
    defer result.deinit();

    try testing.expect(!result.has_error);
    try testing.expectEqual(
        JoinStrategy.hash_in_memory,
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
    try testing.expectEqual(@as(u8, 0), result.stats.plan.nested_join_nested_loop_count);
    try testing.expectEqual(@as(u8, 1), result.stats.plan.nested_join_hash_in_memory_count);
    try testing.expectEqual(@as(u8, 0), result.stats.plan.nested_join_hash_spill_count);
}

test "execute nested relation with child operators uses hash spill strategy when right side exceeds flat fit" {
    var env: ExecTestEnv = undefined;
    try env.init();
    defer env.deinit();

    const post_model = try env.catalog.addModel("Post");
    _ = try env.catalog.addColumn(post_model, "id", .i64, false);
    _ = try env.catalog.addColumn(post_model, "user_id", .i64, false);
    env.catalog.models[post_model].heap_first_page_id = 129;
    env.catalog.models[post_model].total_pages = 1;
    _ = try env.catalog.addAssociation(
        env.model_id,
        "posts",
        AssociationKind.has_many,
        "Post",
    );
    try env.catalog.resolveAssociations();

    const post_page = try env.pool.pin(129);
    heap_mod.HeapPage.init(post_page);
    env.pool.unpin(129, true);

    const tx = try env.tm.begin();
    var snap = try env.tm.snapshot(tx);
    defer snap.deinit();

    const user_src = "User |> insert(id = 1, name = \"Alice\", active = true)";
    const user_tok = tokenizer_mod.tokenize(user_src);
    const user_p = parser_mod.parse(&user_tok, user_src);
    try testing.expect(!user_p.has_error);
    var user_r = try execute(&env.makeCtx(tx, &snap, &user_p.ast, &user_tok, user_src));
    defer user_r.deinit();
    try testing.expect(!user_r.has_error);

    var post_id: u32 = 1;
    while (post_id <= scan_mod.scan_batch_size + 32) : (post_id += 1) {
        const owner_id: u32 = if (post_id <= 200) 1 else 2;
        var post_buf: [128]u8 = undefined;
        const post_src = try std.fmt.bufPrint(
            post_buf[0..],
            "Post |> insert(id = {d}, user_id = {d})",
            .{ post_id, owner_id },
        );
        const post_tok = tokenizer_mod.tokenize(post_src);
        const post_p = parser_mod.parse(&post_tok, post_src);
        try testing.expect(!post_p.has_error);
        var post_r = try execute(&env.makeCtx(tx, &snap, &post_p.ast, &post_tok, post_src));
        defer post_r.deinit();
        try testing.expect(!post_r.has_error);
    }

    const src = "User |> where(id == 1) { id posts |> where(id > 0) |> sort(id asc) |> limit(3) { id } }";
    const tok = tokenizer_mod.tokenize(src);
    const p = parser_mod.parse(&tok, src);
    try testing.expect(!p.has_error);
    var result = try execute(&env.makeCtx(tx, &snap, &p.ast, &tok, src));
    defer result.deinit();

    try testing.expect(!result.has_error);
    try testing.expectEqual(@as(u16, 3), result.row_count);
    try testing.expectEqual(@as(i64, 1), result.rows[0].values[1].i64);
    try testing.expectEqual(@as(i64, 2), result.rows[1].values[1].i64);
    try testing.expectEqual(@as(i64, 3), result.rows[2].values[1].i64);
    try testing.expectEqual(
        JoinStrategy.hash_spill,
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
    try testing.expectEqual(@as(u8, 0), result.stats.plan.nested_join_nested_loop_count);
    try testing.expectEqual(@as(u8, 0), result.stats.plan.nested_join_hash_in_memory_count);
    try testing.expectEqual(@as(u8, 1), result.stats.plan.nested_join_hash_spill_count);
}

test "execute nested relation without child operators uses hash spill strategy when right side exceeds flat fit" {
    var env: ExecTestEnv = undefined;
    try env.init();
    defer env.deinit();

    const post_model = try env.catalog.addModel("Post");
    _ = try env.catalog.addColumn(post_model, "id", .i64, false);
    _ = try env.catalog.addColumn(post_model, "user_id", .i64, false);
    env.catalog.models[post_model].heap_first_page_id = 125;
    env.catalog.models[post_model].total_pages = 1;
    _ = try env.catalog.addAssociation(
        env.model_id,
        "posts",
        AssociationKind.has_many,
        "Post",
    );
    try env.catalog.resolveAssociations();

    const post_page = try env.pool.pin(125);
    heap_mod.HeapPage.init(post_page);
    env.pool.unpin(125, true);

    const tx = try env.tm.begin();
    var snap = try env.tm.snapshot(tx);
    defer snap.deinit();

    const user_src = "User |> insert(id = 1, name = \"Alice\", active = true)";
    const user_tok = tokenizer_mod.tokenize(user_src);
    const user_p = parser_mod.parse(&user_tok, user_src);
    try testing.expect(!user_p.has_error);
    var user_r = try execute(&env.makeCtx(tx, &snap, &user_p.ast, &user_tok, user_src));
    defer user_r.deinit();
    try testing.expect(!user_r.has_error);

    var post_id: u32 = 1;
    while (post_id <= scan_mod.scan_batch_size + 32) : (post_id += 1) {
        const owner_id: u32 = if (post_id <= 200) 1 else 2;
        var post_buf: [128]u8 = undefined;
        const post_src = try std.fmt.bufPrint(
            post_buf[0..],
            "Post |> insert(id = {d}, user_id = {d})",
            .{ post_id, owner_id },
        );
        const post_tok = tokenizer_mod.tokenize(post_src);
        const post_p = parser_mod.parse(&post_tok, post_src);
        try testing.expect(!post_p.has_error);
        var post_r = try execute(&env.makeCtx(tx, &snap, &post_p.ast, &post_tok, post_src));
        defer post_r.deinit();
        try testing.expect(!post_r.has_error);
    }

    const src = "User |> where(id == 1) { id posts { id } }";
    const tok = tokenizer_mod.tokenize(src);
    const p = parser_mod.parse(&tok, src);
    try testing.expect(!p.has_error);
    var result = try execute(&env.makeCtx(tx, &snap, &p.ast, &tok, src));
    defer result.deinit();

    try testing.expect(!result.has_error);
    try testing.expect(result.row_count > 0);
    try testing.expectEqual(
        JoinStrategy.hash_spill,
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

test "execute nested hash spill with alternating left partitions is deterministic across repeated runs" {
    var env: ExecTestEnv = undefined;
    try env.init();
    defer env.deinit();

    const post_model = try env.catalog.addModel("Post");
    _ = try env.catalog.addColumn(post_model, "id", .i64, false);
    _ = try env.catalog.addColumn(post_model, "user_id", .i64, false);
    env.catalog.models[post_model].heap_first_page_id = 126;
    env.catalog.models[post_model].total_pages = 1;
    _ = try env.catalog.addAssociation(
        env.model_id,
        "posts",
        AssociationKind.has_many,
        "Post",
    );
    try env.catalog.resolveAssociations();

    const post_page = try env.pool.pin(126);
    heap_mod.HeapPage.init(post_page);
    env.pool.unpin(126, true);

    const tx = try env.tm.begin();
    var snap = try env.tm.snapshot(tx);
    defer snap.deinit();

    var user_id: u16 = 1;
    while (user_id <= 10) : (user_id += 1) {
        var user_buf: [160]u8 = undefined;
        const user_src = try std.fmt.bufPrint(
            user_buf[0..],
            "User |> insert(id = {d}, name = \"U-{d}\", active = true)",
            .{ user_id, user_id },
        );
        const user_tok = tokenizer_mod.tokenize(user_src);
        const user_p = parser_mod.parse(&user_tok, user_src);
        try testing.expect(!user_p.has_error);
        var user_r = try execute(&env.makeCtx(tx, &snap, &user_p.ast, &user_tok, user_src));
        defer user_r.deinit();
        try testing.expect(!user_r.has_error);
    }

    var post_id: u32 = 1;
    while (post_id <= 5000) : (post_id += 1) {
        const owner_id: u32 = ((post_id - 1) % 10) + 1;
        var post_buf: [128]u8 = undefined;
        const post_src = try std.fmt.bufPrint(
            post_buf[0..],
            "Post |> insert(id = {d}, user_id = {d})",
            .{ post_id, owner_id },
        );
        const post_tok = tokenizer_mod.tokenize(post_src);
        const post_p = parser_mod.parse(&post_tok, post_src);
        try testing.expect(!post_p.has_error);
        var post_r = try execute(&env.makeCtx(tx, &snap, &post_p.ast, &post_tok, post_src));
        defer post_r.deinit();
        try testing.expect(!post_r.has_error);
    }

    const src = "User |> where(id <= 6) |> sort(id asc) { id posts { id } }";
    const tok = tokenizer_mod.tokenize(src);
    const p = parser_mod.parse(&tok, src);
    try testing.expect(!p.has_error);

    var result_a = try execute(&env.makeCtx(tx, &snap, &p.ast, &tok, src));
    defer result_a.deinit();
    try testing.expect(!result_a.has_error);
    try testing.expectEqual(
        JoinStrategy.hash_spill,
        result_a.stats.plan.join_strategy,
    );
    try testing.expect(result_a.row_count > 0);

    var result_b = try execute(&env.makeCtx(tx, &snap, &p.ast, &tok, src));
    defer result_b.deinit();
    try testing.expect(!result_b.has_error);
    try testing.expectEqual(
        JoinStrategy.hash_spill,
        result_b.stats.plan.join_strategy,
    );

    try expectResultRowsEqual(&result_a, &result_b);
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
    try testing.expectEqual(@as(u8, 2), result.stats.plan.nested_relation_count);
    try testing.expectEqual(@as(u8, 0), result.stats.plan.nested_join_nested_loop_count);
    try testing.expectEqual(@as(u8, 2), result.stats.plan.nested_join_hash_in_memory_count);
    try testing.expectEqual(@as(u8, 0), result.stats.plan.nested_join_hash_spill_count);
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
