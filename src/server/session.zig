//! Server session boundary: request parse/execute/serialize flow.
//!
//! Responsibilities in this file:
//! - Handles one request from raw query text through executor output bytes.
//! - Integrates parse/tokenize errors, execution errors, and boundary error classes.
//! - Serves accepted connections with bounded request/response buffers.
//! - Emits deterministic wire-format responses for tests and clients.
const std = @import("std");
const bootstrap_mod = @import("../runtime/bootstrap.zig");
const request_mod = @import("../runtime/request.zig");
const pool_mod = @import("pool.zig");
const parser_mod = @import("../parser/parser.zig");
const tokenizer_mod = @import("../parser/tokenizer.zig");
const ast_mod = @import("../parser/ast.zig");
const exec_mod = @import("../executor/executor.zig");
const mutation_mod = @import("../executor/mutation.zig");
const transport_mod = @import("transport.zig");
const runtime_errors = @import("../runtime/error_taxonomy.zig");
const row_mod = @import("../storage/row.zig");
const catalog_mod = @import("../catalog/catalog.zig");
const heap_mod = @import("../storage/heap.zig");
const disk_mod = @import("../simulator/disk.zig");

const BootstrappedRuntime = bootstrap_mod.BootstrappedRuntime;
const Catalog = catalog_mod.Catalog;
const OverflowReclaimStatsSnapshot = catalog_mod.OverflowReclaimStatsSnapshot;
const ConnectionPool = pool_mod.ConnectionPool;
const PoolConn = pool_mod.PoolConn;
const PoolStats = pool_mod.PoolStats;
const NodeTag = ast_mod.NodeTag;
const Ast = ast_mod.Ast;
const NodeIndex = ast_mod.NodeIndex;
const null_node = ast_mod.null_node;
const Value = row_mod.Value;
const ColumnType = row_mod.ColumnType;
const compareValues = row_mod.compareValues;
const Acceptor = transport_mod.Acceptor;
const Connection = transport_mod.Connection;

pub const SessionError = request_mod.RequestError || error{ResponseTooLarge};
pub const ServeError = SessionError ||
    transport_mod.AcceptError ||
    transport_mod.ConnectionError;

pub const SessionResponse = struct {
    bytes_written: usize,
    is_query_error: bool,
    had_mutation: bool,
};

/// Deterministic request/session boundary used by server-side handlers.
///
/// This path tokenizes/parses a query string, executes through a checked-out
/// pool connection, then serializes response bytes.
pub const Session = struct {
    runtime: *BootstrappedRuntime,
    catalog: *Catalog,

    pub fn init(
        runtime: *BootstrappedRuntime,
        catalog: *Catalog,
    ) Session {
        std.debug.assert(runtime.static_allocator.isSealed());
        return .{ .runtime = runtime, .catalog = catalog };
    }

    pub fn handleRequest(
        self: *Session,
        pool: *const ConnectionPool,
        pool_conn: *const PoolConn,
        source: []const u8,
        out: []u8,
    ) SessionError!SessionResponse {
        std.debug.assert(source.len > 0);
        std.debug.assert(out.len > 0);
        std.debug.assert(pool_conn.checked_out);
        std.debug.assert(self.runtime.static_allocator.isSealed());
        var stream = std.io.fixedBufferStream(out);
        const writer = stream.writer();

        const tokens = tokenizer_mod.tokenize(source);
        if (tokens.has_error) {
            const message = fixedMessage(tokens.error_message[0..]);
            writer.print("ERR tokenize: {s}\n", .{message}) catch
                return error.ResponseTooLarge;
            return .{
                .bytes_written = stream.pos,
                .is_query_error = true,
                .had_mutation = false,
            };
        }

        const parsed = parser_mod.parse(&tokens, source);
        if (parsed.has_error) {
            const message = fixedMessage(parsed.error_message[0..]);
            writer.print("ERR parse: {s}\n", .{message}) catch
                return error.ResponseTooLarge;
            return .{
                .bytes_written = stream.pos,
                .is_query_error = true,
                .had_mutation = false,
            };
        }
        if (missingCrudReturningBlock(&parsed.ast)) {
            writer.writeAll(
                "ERR query: returning block required for CRUD statements; use {} for no returned rows\n",
            ) catch return error.ResponseTooLarge;
            return .{
                .bytes_written = stream.pos,
                .is_query_error = true,
                .had_mutation = false,
            };
        }
        const include_inspect = astHasInspectOp(&parsed.ast);

        var result = try request_mod.executeWithPoolConn(
            self.runtime,
            .{
                .catalog = self.catalog,
                .pool_conn = pool_conn,
                .ast = &parsed.ast,
                .tokens = &tokens,
                .source = source,
            },
        );
        defer result.deinit();

        const pool_stats: ?PoolStats = if (include_inspect) pool.snapshotStats() else null;
        try serializeQueryResult(
            writer,
            &result,
            self.catalog,
            pool_stats,
            &parsed.ast,
            &tokens,
            source,
        );
        const had_mutation =
            result.stats.rows_inserted > 0 or
            result.stats.rows_updated > 0 or
            result.stats.rows_deleted > 0;
        return .{
            .bytes_written = stream.pos,
            .is_query_error = result.has_error,
            .had_mutation = had_mutation,
        };
    }

    /// Serve all requests from one accepted connection using bounded buffers.
    pub fn serveConnection(
        self: *Session,
        connection: Connection,
        pool: *ConnectionPool,
        request_buf: []u8,
        response_buf: []u8,
    ) ServeError!void {
        std.debug.assert(request_buf.len > 0);
        std.debug.assert(response_buf.len > 0);
        while (true) {
            const request_opt = try connection.readRequest(request_buf);
            const request = request_opt orelse break;
            if (request.len > request_buf.len) return error.RequestTooLarge;

            var pool_conn = pool.checkout() catch |err| {
                const class = runtime_errors.classifySessionBoundary(err);
                const boundary_msg = try serializeBoundaryError(
                    response_buf,
                    class,
                    err,
                );
                try connection.writeResponse(boundary_msg);
                continue;
            };

            const response = self.handleRequest(
                pool,
                &pool_conn,
                request,
                response_buf,
            ) catch |err| {
                const class = runtime_errors.classifySessionBoundary(err);
                const boundary_msg = try serializeBoundaryError(
                    response_buf,
                    class,
                    err,
                );
                mutation_mod.rollbackOverflowReclaimEntriesForTx(
                    self.catalog,
                    pool_conn.tx_id,
                );
                pool.abortCheckin(&pool_conn) catch |abort_err| {
                    std.log.err(
                        "pool abort checkin failed: slot={d} err={s}",
                        .{ pool_conn.slot_index, @errorName(abort_err) },
                    );
                    @panic("pool abort checkin failed");
                };
                try connection.writeResponse(boundary_msg);
                continue;
            };

            if (response.is_query_error) {
                mutation_mod.rollbackOverflowReclaimEntriesForTx(
                    self.catalog,
                    pool_conn.tx_id,
                );
                pool.abortCheckin(&pool_conn) catch |abort_err| {
                    std.log.err(
                        "pool abort checkin failed: slot={d} err={s}",
                        .{ pool_conn.slot_index, @errorName(abort_err) },
                    );
                    @panic("pool abort checkin failed");
                };
            } else {
                const tx_id = pool_conn.tx_id;
                if (response.had_mutation) {
                    mutation_mod.commitOverflowReclaimEntriesForTx(
                        self.catalog,
                        &self.runtime.pool,
                        &self.runtime.wal,
                        tx_id,
                        1,
                    ) catch |reclaim_err| {
                        mutation_mod.rollbackOverflowReclaimEntriesForTx(
                            self.catalog,
                            tx_id,
                        );
                        pool.abortCheckin(&pool_conn) catch |abort_err| {
                            std.log.err(
                                "pool abort checkin failed: slot={d} err={s}",
                                .{ pool_conn.slot_index, @errorName(abort_err) },
                            );
                            @panic("pool abort checkin failed");
                        };
                        var stream = std.io.fixedBufferStream(response_buf);
                        const writer = stream.writer();
                        writer.print(
                            "ERR class={s} code={s}\n",
                            .{
                                @tagName(runtime_errors.classifyMutation(reclaim_err)),
                                @errorName(reclaim_err),
                            },
                        ) catch return error.ResponseTooLarge;
                        const boundary_msg = response_buf[0..stream.pos];
                        try connection.writeResponse(boundary_msg);
                        continue;
                    };
                }
                pool.checkin(&pool_conn) catch |checkin_err| {
                    std.log.err(
                        "pool checkin failed: slot={d} err={s}",
                        .{ pool_conn.slot_index, @errorName(checkin_err) },
                    );
                    @panic("pool checkin failed");
                };
            }
            std.debug.assert(response.bytes_written <= response_buf.len);
            try connection.writeResponse(
                response_buf[0..response.bytes_written],
            );
        }
    }

    /// Accept and serve all currently pending connections.
    pub fn serveAcceptedConnections(
        self: *Session,
        acceptor: Acceptor,
        pool: *ConnectionPool,
        request_buf: []u8,
        response_buf: []u8,
    ) ServeError!usize {
        var served_connections: usize = 0;
        while (true) {
            const connection = (try acceptor.accept()) orelse break;
            try self.serveConnection(
                connection,
                pool,
                request_buf,
                response_buf,
            );
            served_connections += 1;
        }
        return served_connections;
    }
};

fn fixedMessage(message: []const u8) []const u8 {
    const end = std.mem.indexOfScalar(u8, message, 0) orelse message.len;
    return message[0..end];
}

fn serializeBoundaryError(
    out: []u8,
    class: runtime_errors.ErrorClass,
    err: runtime_errors.SessionBoundaryError,
) error{ResponseTooLarge}![]const u8 {
    var stream = std.io.fixedBufferStream(out);
    const writer = stream.writer();
    writer.print(
        "ERR class={s} code={s}\n",
        .{ @tagName(class), @errorName(err) },
    ) catch return error.ResponseTooLarge;
    return out[0..stream.pos];
}

fn serializeQueryResult(
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

const max_protocol_fields = row_mod.max_columns;

const SelectionEntryKind = enum {
    scalar,
    nested,
};

const SelectionEntry = struct {
    kind: SelectionEntryKind,
    token_idx: u16,
    column_pos: u16,
};

const TreeProjection = struct {
    root_model_id: catalog_mod.ModelId,
    entry_count: u16 = 0,
    entries: [max_protocol_fields]SelectionEntry = undefined,
    root_scalar_count: u16 = 0,
    root_scalar_positions: [max_protocol_fields]u16 = undefined,
    nested_field_token: u16 = 0,
    nested_model_id: catalog_mod.ModelId = 0,
    nested_scalar_count: u16 = 0,
    nested_scalar_tokens: [max_protocol_fields]u16 = undefined,
    nested_scalar_positions: [max_protocol_fields]u16 = undefined,
};

fn serializeTreeProtocol(
    writer: anytype,
    result: *const exec_mod.QueryResult,
    projection: *const TreeProjection,
    catalog: *const Catalog,
    tokens: *const tokenizer_mod.TokenizeResult,
    source: []const u8,
) error{ResponseTooLarge}!void {
    try writeShape(writer, projection, catalog, tokens, source);

    if (result.row_count == 0) return;

    var row_index: u16 = 0;
    while (row_index < result.row_count) {
        const group_start = row_index;
        var group_end = row_index + 1;
        while (group_end < result.row_count and
            rowsShareRoot(result, projection, group_start, group_end))
        {
            group_end += 1;
        }

        var entry_index: u16 = 0;
        while (entry_index < projection.entry_count) : (entry_index += 1) {
            if (entry_index > 0) {
                writer.writeAll(",") catch return error.ResponseTooLarge;
            }
            const entry = projection.entries[entry_index];
            switch (entry.kind) {
                .scalar => try writeProtocolValue(
                    writer,
                    result.rows[group_start].values[entry.column_pos],
                ),
                .nested => try writeNestedList(
                    writer,
                    result,
                    projection,
                    group_start,
                    group_end,
                ),
            }
        }
        writer.writeAll("\n") catch return error.ResponseTooLarge;
        row_index = group_end;
    }
}

fn buildTreeProjection(
    ast: *const Ast,
    tokens: *const tokenizer_mod.TokenizeResult,
    source: []const u8,
    catalog: *const Catalog,
) ?TreeProjection {
    const pipeline = getTopPipeline(ast) orelse return null;
    const pipeline_node = ast.getNode(pipeline);
    if (pipeline_node.tag != .pipeline) return null;
    const source_node = ast.getNode(pipeline_node.data.binary.lhs);
    if (source_node.tag != .pipe_source) return null;

    const source_model_name = tokens.getText(source_node.data.token, source);
    const source_model_id = catalog.findModel(source_model_name) orelse return null;
    const selection = pipeline_node.extra;
    if (selection == 0 or selection >= ast.node_count) return null;
    const selection_node = ast.getNode(selection);
    if (selection_node.tag != .selection_set) return null;

    var projection = TreeProjection{
        .root_model_id = source_model_id,
    };

    const source_schema = &catalog.models[source_model_id].row_schema;
    var projection_col_pos: u16 = 0;
    var nested_count: u16 = 0;
    var field = selection_node.data.unary;
    while (field != null_node) {
        const node = ast.getNode(field);
        switch (node.tag) {
            .select_field => {
                const field_name = tokens.getText(node.data.token, source);
                _ = source_schema.findColumn(field_name) orelse return null;
                projection.entries[projection.entry_count] = .{
                    .kind = .scalar,
                    .token_idx = node.data.token,
                    .column_pos = projection_col_pos,
                };
                projection.entry_count += 1;
                projection.root_scalar_positions[projection.root_scalar_count] = projection_col_pos;
                projection.root_scalar_count += 1;
                projection_col_pos += 1;
            },
            .select_nested => {
                if (nested_count > 0) return null;
                nested_count += 1;

                const relation_name = tokens.getText(node.extra, source);
                const assoc_id = catalog.findAssociation(source_model_id, relation_name) orelse
                    return null;
                const assoc = &catalog.models[source_model_id].associations[assoc_id];
                if (assoc.target_model_id == catalog_mod.null_model) return null;

                projection.entries[projection.entry_count] = .{
                    .kind = .nested,
                    .token_idx = node.extra,
                    .column_pos = 0,
                };
                projection.entry_count += 1;
                projection.nested_field_token = node.extra;
                projection.nested_model_id = assoc.target_model_id;

                const nested_selection = getNestedSelection(ast, field) orelse return null;
                var nested_field = ast.getNode(nested_selection).data.unary;
                while (nested_field != null_node) {
                    const nested_node = ast.getNode(nested_field);
                    if (nested_node.tag != .select_field) return null;
                    projection.nested_scalar_tokens[projection.nested_scalar_count] = nested_node.data.token;
                    projection.nested_scalar_positions[projection.nested_scalar_count] = projection_col_pos;
                    projection.nested_scalar_count += 1;
                    projection_col_pos += 1;
                    nested_field = nested_node.next;
                }
            },
            else => return null,
        }
        field = node.next;
    }

    if (nested_count != 1) return null;
    if (projection.root_scalar_count == 0) return null;
    if (projection.entry_count == 0 or projection.nested_scalar_count == 0) return null;
    return projection;
}

fn countProtocolRootRows(
    result: *const exec_mod.QueryResult,
    projection: *const TreeProjection,
) u16 {
    if (result.row_count == 0) return 0;
    var root_count: u16 = 0;
    var row_idx: u16 = 0;
    while (row_idx < result.row_count) {
        root_count += 1;
        var next_idx = row_idx + 1;
        while (next_idx < result.row_count and
            rowsShareRoot(result, projection, row_idx, next_idx))
        {
            next_idx += 1;
        }
        row_idx = next_idx;
    }
    return root_count;
}

fn getTopPipeline(ast: *const Ast) ?NodeIndex {
    if (ast.root == null_node) return null;
    const root = ast.getNode(ast.root);
    if (root.tag != .root) return null;
    const first_stmt = root.data.unary;
    if (first_stmt == null_node) return null;
    const first = ast.getNode(first_stmt);
    if (first.tag == .pipeline) return first_stmt;
    if (first.tag == .let_binding and first.data.unary != null_node) {
        const bound = ast.getNode(first.data.unary);
        if (bound.tag == .pipeline) return first.data.unary;
    }
    return null;
}

fn getNestedSelection(ast: *const Ast, nested_node: NodeIndex) ?NodeIndex {
    const nested = ast.getNode(nested_node);
    if (nested.tag != .select_nested) return null;
    if (nested.data.unary == null_node) return null;
    const nested_pipeline = ast.getNode(nested.data.unary);
    if (nested_pipeline.tag != .pipeline) return null;
    const selection = nested_pipeline.extra;
    if (selection == 0 or selection >= ast.node_count) return null;
    if (ast.getNode(selection).tag != .selection_set) return null;
    return selection;
}

fn rowsShareRoot(
    result: *const exec_mod.QueryResult,
    projection: *const TreeProjection,
    lhs_idx: u16,
    rhs_idx: u16,
) bool {
    const lhs = result.rows[lhs_idx];
    const rhs = result.rows[rhs_idx];
    var i: u16 = 0;
    while (i < projection.root_scalar_count) : (i += 1) {
        const pos = projection.root_scalar_positions[i];
        if (compareValues(lhs.values[pos], rhs.values[pos]) != .eq) return false;
    }
    return true;
}

fn writeShape(
    writer: anytype,
    projection: *const TreeProjection,
    catalog: *const Catalog,
    tokens: *const tokenizer_mod.TokenizeResult,
    source: []const u8,
) error{ResponseTooLarge}!void {
    writer.writeAll("{") catch return error.ResponseTooLarge;
    const root_schema = &catalog.models[projection.root_model_id].row_schema;
    const nested_schema = &catalog.models[projection.nested_model_id].row_schema;

    var entry_index: u16 = 0;
    while (entry_index < projection.entry_count) : (entry_index += 1) {
        if (entry_index > 0) {
            writer.writeAll(",") catch return error.ResponseTooLarge;
        }
        const entry = projection.entries[entry_index];
        switch (entry.kind) {
            .scalar => {
                const field_name = tokens.getText(entry.token_idx, source);
                const col_idx = root_schema.findColumn(field_name) orelse
                    return error.ResponseTooLarge;
                const col = root_schema.columns[col_idx];
                writer.print(
                    "{s}:{s}",
                    .{ field_name, protocolTypeName(col.column_type) },
                ) catch return error.ResponseTooLarge;
            },
            .nested => {
                const relation_name = tokens.getText(projection.nested_field_token, source);
                writer.print("{s}:[{{", .{relation_name}) catch return error.ResponseTooLarge;
                var nested_i: u16 = 0;
                while (nested_i < projection.nested_scalar_count) : (nested_i += 1) {
                    if (nested_i > 0) {
                        writer.writeAll(",") catch return error.ResponseTooLarge;
                    }
                    const field_name = tokens.getText(
                        projection.nested_scalar_tokens[nested_i],
                        source,
                    );
                    const col_idx = nested_schema.findColumn(field_name) orelse
                        return error.ResponseTooLarge;
                    const col = nested_schema.columns[col_idx];
                    writer.print(
                        "{s}:{s}",
                        .{ field_name, protocolTypeName(col.column_type) },
                    ) catch return error.ResponseTooLarge;
                }
                writer.writeAll("}]") catch return error.ResponseTooLarge;
            },
        }
    }
    writer.writeAll("}\n") catch return error.ResponseTooLarge;
}

fn writeNestedList(
    writer: anytype,
    result: *const exec_mod.QueryResult,
    projection: *const TreeProjection,
    start_row: u16,
    end_row: u16,
) error{ResponseTooLarge}!void {
    writer.writeAll("[") catch return error.ResponseTooLarge;
    var emitted_any = false;
    var row_idx = start_row;
    while (row_idx < end_row) : (row_idx += 1) {
        if (isNestedNullRow(result.rows[row_idx].values[0..], projection)) continue;
        if (emitted_any) {
            writer.writeAll(",") catch return error.ResponseTooLarge;
        }
        emitted_any = true;
        writer.writeAll("[") catch return error.ResponseTooLarge;
        var col_idx: u16 = 0;
        while (col_idx < projection.nested_scalar_count) : (col_idx += 1) {
            if (col_idx > 0) {
                writer.writeAll(",") catch return error.ResponseTooLarge;
            }
            const pos = projection.nested_scalar_positions[col_idx];
            try writeProtocolValue(writer, result.rows[row_idx].values[pos]);
        }
        writer.writeAll("]") catch return error.ResponseTooLarge;
    }
    writer.writeAll("]") catch return error.ResponseTooLarge;
}

fn isNestedNullRow(
    row_values: []const Value,
    projection: *const TreeProjection,
) bool {
    var col_idx: u16 = 0;
    while (col_idx < projection.nested_scalar_count) : (col_idx += 1) {
        const pos = projection.nested_scalar_positions[col_idx];
        if (row_values[pos] != .null_value) return false;
    }
    return true;
}

fn protocolTypeName(column_type: ColumnType) []const u8 {
    return switch (column_type) {
        .i8 => "i8",
        .i16 => "i16",
        .u8 => "u8",
        .u16 => "u16",
        .u32 => "u32",
        .u64 => "u64",
        .i64 => "i64",
        .i32 => "i32",
        .f64 => "f64",
        .bool => "bool",
        .string => "str",
        .timestamp => "ts",
    };
}

fn writeProtocolValue(
    writer: anytype,
    value: Value,
) error{ResponseTooLarge}!void {
    switch (value) {
        .string => |v| try writeQuotedString(writer, v),
        else => try serializeValue(writer, value),
    }
}

fn writeQuotedString(
    writer: anytype,
    value: []const u8,
) error{ResponseTooLarge}!void {
    writer.writeAll("\"") catch return error.ResponseTooLarge;
    for (value) |byte| {
        switch (byte) {
            '\\' => writer.writeAll("\\\\") catch return error.ResponseTooLarge,
            '"' => writer.writeAll("\\\"") catch return error.ResponseTooLarge,
            '\n' => writer.writeAll("\\n") catch return error.ResponseTooLarge,
            '\r' => writer.writeAll("\\r") catch return error.ResponseTooLarge,
            '\t' => writer.writeAll("\\t") catch return error.ResponseTooLarge,
            else => writer.writeByte(byte) catch return error.ResponseTooLarge,
        }
    }
    writer.writeAll("\"") catch return error.ResponseTooLarge;
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

fn astHasInspectOp(ast: *const ast_mod.Ast) bool {
    var i: u16 = 0;
    while (i < ast.node_count) : (i += 1) {
        if (ast.nodes[i].tag == NodeTag.op_inspect) return true;
    }
    return false;
}

fn missingCrudReturningBlock(ast: *const ast_mod.Ast) bool {
    if (ast.root == null_node) return false;
    const root = ast.getNode(ast.root);
    if (root.tag != .root) return false;

    var stmt = root.data.unary;
    while (stmt != null_node) {
        const node = ast.getNode(stmt);
        if (node.tag == .pipeline and node.extra == 0) {
            return true;
        }
        if (node.tag == .let_binding) {
            const bound = node.data.unary;
            if (bound != null_node) {
                const bound_node = ast.getNode(bound);
                if (bound_node.tag == .pipeline and bound_node.extra == 0) {
                    return true;
                }
            }
        }
        stmt = node.next;
    }
    return false;
}

fn serializeValue(
    writer: anytype,
    value: Value,
) error{ResponseTooLarge}!void {
    switch (value) {
        .i8 => |v| writer.print("{d}", .{v}) catch
            return error.ResponseTooLarge,
        .i16 => |v| writer.print("{d}", .{v}) catch
            return error.ResponseTooLarge,
        .i64 => |v| writer.print("{d}", .{v}) catch
            return error.ResponseTooLarge,
        .i32 => |v| writer.print("{d}", .{v}) catch
            return error.ResponseTooLarge,
        .u8 => |v| writer.print("{d}", .{v}) catch
            return error.ResponseTooLarge,
        .u16 => |v| writer.print("{d}", .{v}) catch
            return error.ResponseTooLarge,
        .u32 => |v| writer.print("{d}", .{v}) catch
            return error.ResponseTooLarge,
        .u64 => |v| writer.print("{d}", .{v}) catch
            return error.ResponseTooLarge,
        .f64 => |v| writer.print("{d}", .{v}) catch
            return error.ResponseTooLarge,
        .bool => |v| writer.writeAll(if (v) "true" else "false") catch
            return error.ResponseTooLarge,
        .string => |v| writer.writeAll(v) catch
            return error.ResponseTooLarge,
        .timestamp => |v| writer.print("{d}", .{v}) catch
            return error.ResponseTooLarge,
        .null_value => writer.writeAll("null") catch
            return error.ResponseTooLarge,
    }
}

fn initUserModel(catalog: *Catalog, runtime: *BootstrappedRuntime) !void {
    const model_id = try catalog.addModel("User");
    _ = try catalog.addColumn(model_id, "id", .i64, false);
    _ = try catalog.addColumn(model_id, "name", .string, true);
    _ = try catalog.addColumn(model_id, "active", .bool, true);

    catalog.models[model_id].heap_first_page_id = 100;
    catalog.models[model_id].total_pages = 1;

    const page = try runtime.pool.pin(100);
    heap_mod.HeapPage.init(page);
    runtime.pool.unpin(100, true);
}

test "session request path releases leased query slot after serialization" {
    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    const backing_memory = try std.testing.allocator.alloc(
        u8,
        256 * 1024 * 1024,
    );
    defer std.testing.allocator.free(backing_memory);

    var runtime = try BootstrappedRuntime.init(
        backing_memory,
        disk.storage(),
        .{ .max_query_slots = 1 },
    );

    var catalog = Catalog{};
    try initUserModel(&catalog, &runtime);

    var session = Session.init(&runtime, &catalog);
    var pool = ConnectionPool.init(&runtime);
    var response_buf: [1024]u8 = undefined;

    var first_conn = try pool.checkout();
    const first = try session.handleRequest(
        &pool,
        &first_conn,
        "User {}",
        response_buf[0..],
    );
    try pool.checkin(&first_conn);
    try std.testing.expect(!first.is_query_error);
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=0 deleted_rows=0\n",
        response_buf[0..first.bytes_written],
    );

    var second_conn = try pool.checkout();
    const second = try session.handleRequest(
        &pool,
        &second_conn,
        "User {}",
        response_buf[0..],
    );
    try pool.checkin(&second_conn);
    try std.testing.expect(!second.is_query_error);
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=0 deleted_rows=0\n",
        response_buf[0..second.bytes_written],
    );
}

test "session request path serializes query results" {
    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    const backing_memory = try std.testing.allocator.alloc(
        u8,
        256 * 1024 * 1024,
    );
    defer std.testing.allocator.free(backing_memory);

    var runtime = try BootstrappedRuntime.init(
        backing_memory,
        disk.storage(),
        .{ .max_query_slots = 1 },
    );

    var catalog = Catalog{};
    try initUserModel(&catalog, &runtime);

    var session = Session.init(&runtime, &catalog);
    var pool = ConnectionPool.init(&runtime);
    var response_buf: [1024]u8 = undefined;

    var insert_conn = try pool.checkout();
    _ = try session.handleRequest(
        &pool,
        &insert_conn,
        "User |> insert(id = 1, name = \"Alice\", active = true) {}",
        response_buf[0..],
    );
    try pool.checkin(&insert_conn);

    var read_conn = try pool.checkout();
    const result = try session.handleRequest(
        &pool,
        &read_conn,
        "User { id name active }",
        response_buf[0..],
    );
    try pool.checkin(&read_conn);
    try std.testing.expect(!result.is_query_error);
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,Alice,true\n",
        response_buf[0..result.bytes_written],
    );
}

test "session rejects CRUD pipeline without explicit returning block" {
    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    const backing_memory = try std.testing.allocator.alloc(
        u8,
        256 * 1024 * 1024,
    );
    defer std.testing.allocator.free(backing_memory);

    var runtime = try BootstrappedRuntime.init(
        backing_memory,
        disk.storage(),
        .{ .max_query_slots = 1 },
    );

    var catalog = Catalog{};
    try initUserModel(&catalog, &runtime);

    var session = Session.init(&runtime, &catalog);
    var pool = ConnectionPool.init(&runtime);
    var response_buf: [1024]u8 = undefined;

    var conn = try pool.checkout();
    defer pool.checkin(&conn) catch {
        @panic("pool checkin failed");
    };

    const result = try session.handleRequest(
        &pool,
        &conn,
        "User |> where(id = 1)",
        response_buf[0..],
    );
    try std.testing.expect(result.is_query_error);
    try std.testing.expectEqualStrings(
        "ERR query: returning block required for CRUD statements; use {} for no returned rows\n",
        response_buf[0..result.bytes_written],
    );
}

test "session inspect appends execution and pool stats" {
    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    const backing_memory = try std.testing.allocator.alloc(
        u8,
        256 * 1024 * 1024,
    );
    defer std.testing.allocator.free(backing_memory);

    var runtime = try BootstrappedRuntime.init(
        backing_memory,
        disk.storage(),
        .{ .max_query_slots = 1 },
    );

    var catalog = Catalog{};
    try initUserModel(&catalog, &runtime);

    var session = Session.init(&runtime, &catalog);
    var pool = ConnectionPool.init(&runtime);
    var response_buf: [1024]u8 = undefined;

    var conn = try pool.checkout();
    const result = try session.handleRequest(
        &pool,
        &conn,
        "User |> inspect {}",
        response_buf[0..],
    );
    try pool.checkin(&conn);
    try std.testing.expect(!result.is_query_error);

    const output = response_buf[0..result.bytes_written];
    try std.testing.expect(
        std.mem.indexOf(
            u8,
            output,
            "OK returned_rows=0 inserted_rows=0 updated_rows=0 deleted_rows=0\n",
        ) != null,
    );
    try std.testing.expect(
        std.mem.indexOf(
            u8,
            output,
            "INSPECT exec rows_scanned=",
        ) != null,
    );
    try std.testing.expect(
        std.mem.indexOf(
            u8,
            output,
            "INSPECT pool policy=reject size=1 checked_out=1 pinned=0 exhausted_total=0\n",
        ) != null,
    );
    try std.testing.expect(
        std.mem.indexOf(
            u8,
            output,
            "INSPECT overflow reclaim_queue_depth=0 reclaim_enqueued_total=0 reclaim_dequeued_total=0 reclaim_chains_total=0 reclaim_pages_total=0 reclaim_failures_total=0\n",
        ) != null,
    );
    try std.testing.expect(
        std.mem.indexOf(
            u8,
            output,
            "INSPECT plan source_model=User pipeline=inspect join_strategy=none join_order=none materialization=none sort_strategy=none group_strategy=none nested_relations=0\n",
        ) != null,
    );
    try std.testing.expect(
        std.mem.indexOf(
            u8,
            output,
            "INSPECT explain sort=not_applied group=not_applied\n",
        ) != null,
    );
}

test "session accept loop routes multiple connections through handleRequest" {
    const TestConnection = struct {
        requests: []const []const u8,
        response_log: [4][128]u8 = undefined,
        response_lens: [4]usize = [_]usize{0} ** 4,
        read_index: usize = 0,
        write_index: usize = 0,

        fn connection(self: *@This()) Connection {
            return .{
                .ptr = @ptrCast(self),
                .vtable = &vtable,
            };
        }

        const vtable = Connection.VTable{
            .readRequest = &readRequest,
            .writeResponse = &writeResponse,
        };

        fn readRequest(
            ptr: *anyopaque,
            out: []u8,
        ) transport_mod.ConnectionError!?[]const u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.read_index >= self.requests.len) return null;
            const req = self.requests[self.read_index];
            if (req.len > out.len) return error.RequestTooLarge;
            @memcpy(out[0..req.len], req);
            self.read_index += 1;
            return out[0..req.len];
        }

        fn writeResponse(
            ptr: *anyopaque,
            data: []const u8,
        ) transport_mod.ConnectionError!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.write_index >= self.response_log.len) {
                return error.WriteFailed;
            }
            if (data.len > self.response_log[self.write_index].len) {
                return error.ResponseTooLarge;
            }
            @memcpy(self.response_log[self.write_index][0..data.len], data);
            self.response_lens[self.write_index] = data.len;
            self.write_index += 1;
        }
    };

    const TestAcceptor = struct {
        connections: []*TestConnection,
        accept_index: usize = 0,

        fn acceptor(self: *@This()) Acceptor {
            return .{
                .ptr = @ptrCast(self),
                .vtable = &vtable,
            };
        }

        const vtable = Acceptor.VTable{
            .accept = &accept,
        };

        fn accept(ptr: *anyopaque) transport_mod.AcceptError!?Connection {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.accept_index >= self.connections.len) return null;
            const conn = self.connections[self.accept_index];
            self.accept_index += 1;
            return conn.connection();
        }
    };

    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    const backing_memory = try std.testing.allocator.alloc(
        u8,
        256 * 1024 * 1024,
    );
    defer std.testing.allocator.free(backing_memory);

    var runtime = try BootstrappedRuntime.init(
        backing_memory,
        disk.storage(),
        .{ .max_query_slots = 1 },
    );

    var catalog = Catalog{};
    try initUserModel(&catalog, &runtime);

    var conn_a = TestConnection{
        .requests = &[_][]const u8{
            "User |> insert(id = 1, name = \"Alice\", active = true) {}",
            "User { id name active }",
        },
    };
    var conn_b = TestConnection{
        .requests = &[_][]const u8{
            "User { id name active }",
        },
    };

    var connection_ptrs = [_]*TestConnection{
        &conn_a,
        &conn_b,
    };
    var acceptor_state = TestAcceptor{
        .connections = connection_ptrs[0..],
    };

    var session = Session.init(&runtime, &catalog);
    var pool = ConnectionPool.init(&runtime);
    var request_buf: [256]u8 = undefined;
    var response_buf: [256]u8 = undefined;

    const served = try session.serveAcceptedConnections(
        acceptor_state.acceptor(),
        &pool,
        request_buf[0..],
        response_buf[0..],
    );
    try std.testing.expectEqual(@as(usize, 2), served);

    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        conn_a.response_log[0][0..conn_a.response_lens[0]],
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,Alice,true\n",
        conn_a.response_log[1][0..conn_a.response_lens[1]],
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,Alice,true\n",
        conn_b.response_log[0][0..conn_b.response_lens[0]],
    );
}

test "session accept loop emits classified boundary error on pool exhaustion" {
    const TestConnection = struct {
        request: []const u8,
        response_log: [2][128]u8 = undefined,
        response_lens: [2]usize = [_]usize{0} ** 2,
        read_done: bool = false,
        write_index: usize = 0,

        fn connection(self: *@This()) Connection {
            return .{
                .ptr = @ptrCast(self),
                .vtable = &vtable,
            };
        }

        const vtable = Connection.VTable{
            .readRequest = &readRequest,
            .writeResponse = &writeResponse,
        };

        fn readRequest(
            ptr: *anyopaque,
            out: []u8,
        ) transport_mod.ConnectionError!?[]const u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.read_done) return null;
            if (self.request.len > out.len) return error.RequestTooLarge;
            @memcpy(out[0..self.request.len], self.request);
            self.read_done = true;
            return out[0..self.request.len];
        }

        fn writeResponse(
            ptr: *anyopaque,
            data: []const u8,
        ) transport_mod.ConnectionError!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.write_index >= self.response_log.len) {
                return error.WriteFailed;
            }
            if (data.len > self.response_log[self.write_index].len) {
                return error.ResponseTooLarge;
            }
            @memcpy(self.response_log[self.write_index][0..data.len], data);
            self.response_lens[self.write_index] = data.len;
            self.write_index += 1;
        }
    };

    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    const backing_memory = try std.testing.allocator.alloc(
        u8,
        256 * 1024 * 1024,
    );
    defer std.testing.allocator.free(backing_memory);

    var runtime = try BootstrappedRuntime.init(
        backing_memory,
        disk.storage(),
        .{ .max_query_slots = 1 },
    );

    var catalog = Catalog{};
    try initUserModel(&catalog, &runtime);

    var conn = TestConnection{
        .request = "User {}",
    };

    var session = Session.init(&runtime, &catalog);
    var pool = ConnectionPool.init(&runtime);
    var held_conn = try pool.checkout();
    defer pool.checkin(&held_conn) catch {
        @panic("held pool conn release failed");
    };

    var request_buf: [64]u8 = undefined;
    var response_buf: [128]u8 = undefined;

    try session.serveConnection(
        conn.connection(),
        &pool,
        request_buf[0..],
        response_buf[0..],
    );

    try std.testing.expectEqual(@as(usize, 1), conn.write_index);
    try std.testing.expectEqualStrings(
        "ERR class=resource_exhausted code=PoolExhausted\n",
        conn.response_log[0][0..conn.response_lens[0]],
    );
}
