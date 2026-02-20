const std = @import("std");
const bootstrap_mod = @import("../runtime/bootstrap.zig");
const request_mod = @import("../runtime/request.zig");
const pool_mod = @import("pool.zig");
const parser_mod = @import("../parser/parser.zig");
const tokenizer_mod = @import("../parser/tokenizer.zig");
const ast_mod = @import("../parser/ast.zig");
const exec_mod = @import("../executor/executor.zig");
const transport_mod = @import("transport.zig");
const tiger_errors = @import("../tiger/error_taxonomy.zig");
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
const Value = row_mod.Value;
const Acceptor = transport_mod.Acceptor;
const Connection = transport_mod.Connection;

pub const SessionError = request_mod.RequestError || error{ResponseTooLarge};
pub const ServeError = SessionError ||
    transport_mod.AcceptError ||
    transport_mod.ConnectionError;

pub const SessionResponse = struct {
    bytes_written: usize,
    is_query_error: bool,
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
        try serializeQueryResult(writer, &result, self.catalog, pool_stats);
        return .{
            .bytes_written = stream.pos,
            .is_query_error = result.has_error,
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
                const class = tiger_errors.classifySessionBoundary(err);
                const boundary_msg = try serializeBoundaryError(
                    response_buf,
                    class,
                    err,
                );
                try connection.writeResponse(boundary_msg);
                continue;
            };
            defer pool.checkin(&pool_conn) catch |err| {
                std.log.err(
                    "pool checkin failed: slot={d} err={s}",
                    .{ pool_conn.slot_index, @errorName(err) },
                );
                @panic("pool checkin failed");
            };

            const response = self.handleRequest(
                pool,
                &pool_conn,
                request,
                response_buf,
            ) catch |err| {
                const class = tiger_errors.classifySessionBoundary(err);
                const boundary_msg = try serializeBoundaryError(
                    response_buf,
                    class,
                    err,
                );
                try connection.writeResponse(boundary_msg);
                continue;
            };
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
    class: tiger_errors.ErrorClass,
    err: tiger_errors.SessionBoundaryError,
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
) error{ResponseTooLarge}!void {
    std.debug.assert(result.row_count <= result.rows.len);
    if (result.getError()) |message| {
        writer.print("ERR query: {s}\n", .{message}) catch
            return error.ResponseTooLarge;
        return;
    }

    writer.print("OK rows={d}\n", .{result.row_count}) catch
        return error.ResponseTooLarge;

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

fn serializeValue(
    writer: anytype,
    value: Value,
) error{ResponseTooLarge}!void {
    switch (value) {
        .bigint => |v| writer.print("{d}", .{v}) catch
            return error.ResponseTooLarge,
        .int => |v| writer.print("{d}", .{v}) catch
            return error.ResponseTooLarge,
        .float => |v| writer.print("{d}", .{v}) catch
            return error.ResponseTooLarge,
        .boolean => |v| writer.writeAll(if (v) "true" else "false") catch
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
    _ = try catalog.addColumn(model_id, "id", .bigint, false);
    _ = try catalog.addColumn(model_id, "name", .string, true);
    _ = try catalog.addColumn(model_id, "active", .boolean, true);

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
        "User",
        response_buf[0..],
    );
    try pool.checkin(&first_conn);
    try std.testing.expect(!first.is_query_error);
    try std.testing.expectEqualStrings(
        "OK rows=0\n",
        response_buf[0..first.bytes_written],
    );

    var second_conn = try pool.checkout();
    const second = try session.handleRequest(
        &pool,
        &second_conn,
        "User",
        response_buf[0..],
    );
    try pool.checkin(&second_conn);
    try std.testing.expect(!second.is_query_error);
    try std.testing.expectEqualStrings(
        "OK rows=0\n",
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
        "User |> insert(id = 1, name = \"Alice\", active = true)",
        response_buf[0..],
    );
    try pool.checkin(&insert_conn);

    var read_conn = try pool.checkout();
    const result = try session.handleRequest(
        &pool,
        &read_conn,
        "User",
        response_buf[0..],
    );
    try pool.checkin(&read_conn);
    try std.testing.expect(!result.is_query_error);
    try std.testing.expectEqualStrings(
        "OK rows=1\n1,Alice,true\n",
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
        "User |> inspect",
        response_buf[0..],
    );
    try pool.checkin(&conn);
    try std.testing.expect(!result.is_query_error);

    const output = response_buf[0..result.bytes_written];
    try std.testing.expect(std.mem.indexOf(u8, output, "OK rows=0\n") != null);
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
            "User |> insert(id = 1, name = \"Alice\", active = true)",
            "User",
        },
    };
    var conn_b = TestConnection{
        .requests = &[_][]const u8{
            "User",
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
        "OK rows=0\n",
        conn_a.response_log[0][0..conn_a.response_lens[0]],
    );
    try std.testing.expectEqualStrings(
        "OK rows=1\n1,Alice,true\n",
        conn_a.response_log[1][0..conn_a.response_lens[1]],
    );
    try std.testing.expectEqualStrings(
        "OK rows=1\n1,Alice,true\n",
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
        .request = "User",
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
