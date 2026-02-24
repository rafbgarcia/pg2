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
const serialization_mod = @import("serialization.zig");
const mutation_mod = @import("../executor/mutation.zig");
const transport_mod = @import("transport.zig");
const runtime_errors = @import("../runtime/error_taxonomy.zig");
const row_mod = @import("../storage/row.zig");
const catalog_mod = @import("../catalog/catalog.zig");
const heap_mod = @import("../storage/heap.zig");
const disk_mod = @import("../simulator/disk.zig");

const BootstrappedRuntime = bootstrap_mod.BootstrappedRuntime;
const Catalog = catalog_mod.Catalog;
const ConnectionPool = pool_mod.ConnectionPool;
const PoolConn = pool_mod.PoolConn;
const PoolStats = pool_mod.PoolStats;
const NodeTag = ast_mod.NodeTag;
const null_node = ast_mod.null_node;
const Value = row_mod.Value;
const Acceptor = transport_mod.Acceptor;
const Connection = transport_mod.Connection;
const serializeQueryResult = serialization_mod.serializeQueryResult;

pub const SessionError = request_mod.RequestError || error{ResponseTooLarge};
pub const ServeError = SessionError ||
    transport_mod.AcceptError ||
    transport_mod.ConnectionError;

pub const SessionResponse = struct {
    bytes_written: usize,
    is_query_error: bool,
    had_mutation: bool,
};

pub const PinTransition = enum(u8) {
    none,
    began,
    ended,
};

pub const DispatchResult = struct {
    bytes_written: usize,
    pin_transition: PinTransition = .none,
};

pub const SessionPinState = struct {
    active: bool = false,
    pool_conn: PoolConn = undefined,
};

const TxControl = enum(u8) {
    none,
    begin,
    commit,
    rollback,
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

    /// Runs one request through explicit tx control handling and standard
    /// pool checkout/execute/checkin behavior.
    pub fn dispatchRequestForSession(
        self: *Session,
        pool: *ConnectionPool,
        pin_state: *SessionPinState,
        request: []const u8,
        response_buf: []u8,
    ) SessionError!DispatchResult {
        const control = classifyTxControl(request);
        switch (control) {
            .none => {},
            .begin => return self.handleBegin(pool, pin_state, response_buf),
            .commit => return self.handleCommit(pool, pin_state, response_buf),
            .rollback => return self.handleRollback(pool, pin_state, response_buf),
        }

        if (pin_state.active) {
            return self.dispatchPinnedRequest(
                pool,
                pin_state,
                request,
                response_buf,
            );
        }

        return self.dispatchAutoCommitRequest(
            pool,
            request,
            response_buf,
        );
    }

    /// Backward-compatible single-request boundary for non-reactor callers.
    pub fn dispatchRequest(
        self: *Session,
        pool: *ConnectionPool,
        request: []const u8,
        response_buf: []u8,
    ) SessionError!usize {
        var pin_state = SessionPinState{};
        const result = try self.dispatchRequestForSession(
            pool,
            &pin_state,
            request,
            response_buf,
        );
        if (pin_state.active) {
            self.cleanupPinnedSession(pool, &pin_state);
        }
        return result.bytes_written;
    }

    pub fn cleanupPinnedSession(
        self: *Session,
        pool: *ConnectionPool,
        pin_state: *SessionPinState,
    ) void {
        if (!pin_state.active) return;
        const tx_id = pin_state.pool_conn.tx_id;
        mutation_mod.rollbackOverflowReclaimEntriesForTx(
            self.catalog,
            tx_id,
        );
        pool.rollbackPinned(&pin_state.pool_conn) catch |rollback_err| {
            std.log.err(
                "pool rollbackPinned failed: slot={d} err={s}",
                .{ pin_state.pool_conn.slot_index, @errorName(rollback_err) },
            );
            @panic("pool rollbackPinned failed");
        };
        pin_state.* = .{};
    }

    fn dispatchAutoCommitRequest(
        self: *Session,
        pool: *ConnectionPool,
        request: []const u8,
        response_buf: []u8,
    ) SessionError!DispatchResult {
        var pool_conn = pool.checkout() catch |err| {
            const class = runtime_errors.classifySessionBoundary(err);
            const boundary_msg = try serializeBoundaryError(
                response_buf,
                class,
                err,
            );
            return .{ .bytes_written = boundary_msg.len };
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
            return .{ .bytes_written = boundary_msg.len };
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
                    return .{ .bytes_written = stream.pos };
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
        return .{ .bytes_written = response.bytes_written };
    }

    fn dispatchPinnedRequest(
        self: *Session,
        pool: *ConnectionPool,
        pin_state: *SessionPinState,
        request: []const u8,
        response_buf: []u8,
    ) SessionError!DispatchResult {
        std.debug.assert(pin_state.active);
        const response = self.handleRequest(
            pool,
            &pin_state.pool_conn,
            request,
            response_buf,
        ) catch |err| {
            const class = runtime_errors.classifySessionBoundary(err);
            const boundary_msg = try serializeBoundaryError(
                response_buf,
                class,
                err,
            );
            return .{ .bytes_written = boundary_msg.len };
        };

        if (response.is_query_error) {
            mutation_mod.rollbackOverflowReclaimEntriesForTx(
                self.catalog,
                pin_state.pool_conn.tx_id,
            );
        }
        std.debug.assert(response.bytes_written <= response_buf.len);
        return .{ .bytes_written = response.bytes_written };
    }

    fn handleBegin(
        self: *Session,
        pool: *ConnectionPool,
        pin_state: *SessionPinState,
        response_buf: []u8,
    ) SessionError!DispatchResult {
        _ = self;
        if (pin_state.active) {
            return .{
                .bytes_written = try serializeTxControlError(
                    response_buf,
                    "TransactionAlreadyActive",
                ),
            };
        }
        var pool_conn = pool.checkout() catch |err| {
            const class = runtime_errors.classifySessionBoundary(err);
            const boundary_msg = try serializeBoundaryError(
                response_buf,
                class,
                err,
            );
            return .{ .bytes_written = boundary_msg.len };
        };
        pool.pin(&pool_conn) catch |pin_err| {
            const class = runtime_errors.classifySessionBoundary(pin_err);
            const boundary_msg = serializeBoundaryError(
                response_buf,
                class,
                pin_err,
            ) catch "ERR class=internal code=ResponseTooLarge\n";
            pool.abortCheckin(&pool_conn) catch |abort_err| {
                std.log.err(
                    "pool abort checkin failed: slot={d} err={s}",
                    .{ pool_conn.slot_index, @errorName(abort_err) },
                );
                @panic("pool abort checkin failed");
            };
            return .{ .bytes_written = boundary_msg.len };
        };
        pin_state.* = .{
            .active = true,
            .pool_conn = pool_conn,
        };
        return .{
            .bytes_written = try serializeTxControlOk(response_buf, "BEGIN"),
            .pin_transition = .began,
        };
    }

    fn handleCommit(
        self: *Session,
        pool: *ConnectionPool,
        pin_state: *SessionPinState,
        response_buf: []u8,
    ) SessionError!DispatchResult {
        if (!pin_state.active) {
            return .{
                .bytes_written = try serializeTxControlError(
                    response_buf,
                    "TransactionNotActive",
                ),
            };
        }
        const tx_id = pin_state.pool_conn.tx_id;
        mutation_mod.commitOverflowReclaimEntriesForTx(
            self.catalog,
            &self.runtime.pool,
            &self.runtime.wal,
            tx_id,
            std.math.maxInt(usize),
        ) catch |reclaim_err| {
            self.cleanupPinnedSession(pool, pin_state);
            var stream = std.io.fixedBufferStream(response_buf);
            const writer = stream.writer();
            writer.print(
                "ERR class={s} code={s}\n",
                .{
                    @tagName(runtime_errors.classifyMutation(reclaim_err)),
                    @errorName(reclaim_err),
                },
            ) catch return error.ResponseTooLarge;
            return .{
                .bytes_written = stream.pos,
                .pin_transition = .ended,
            };
        };
        pool.unpin(&pin_state.pool_conn) catch |unpin_err| {
            const class = runtime_errors.classifySessionBoundary(unpin_err);
            const boundary_msg = try serializeBoundaryError(
                response_buf,
                class,
                unpin_err,
            );
            pin_state.* = .{};
            return .{
                .bytes_written = boundary_msg.len,
                .pin_transition = .ended,
            };
        };
        self.runtime.wal.forceFlush() catch |flush_err| {
            const class = runtime_errors.classifySessionBoundary(flush_err);
            const boundary_msg = try serializeBoundaryError(
                response_buf,
                class,
                flush_err,
            );
            pin_state.* = .{};
            return .{
                .bytes_written = boundary_msg.len,
                .pin_transition = .ended,
            };
        };
        pin_state.* = .{};
        return .{
            .bytes_written = try serializeTxControlOk(response_buf, "COMMIT"),
            .pin_transition = .ended,
        };
    }

    fn handleRollback(
        self: *Session,
        pool: *ConnectionPool,
        pin_state: *SessionPinState,
        response_buf: []u8,
    ) SessionError!DispatchResult {
        if (!pin_state.active) {
            return .{
                .bytes_written = try serializeTxControlError(
                    response_buf,
                    "TransactionNotActive",
                ),
            };
        }
        self.cleanupPinnedSession(pool, pin_state);
        return .{
            .bytes_written = try serializeTxControlOk(response_buf, "ROLLBACK"),
            .pin_transition = .ended,
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
        var pin_state = SessionPinState{};
        defer self.cleanupPinnedSession(pool, &pin_state);
        while (true) {
            const request_opt = connection.readRequest(request_buf) catch |err| switch (err) {
                error.WouldBlock => continue,
                else => return err,
            };
            const request = request_opt orelse break;
            if (request.len > request_buf.len) return error.RequestTooLarge;
            const dispatch_result = try self.dispatchRequestForSession(
                pool,
                &pin_state,
                request,
                response_buf,
            );
            try writeResponseRetry(
                connection,
                response_buf[0..dispatch_result.bytes_written],
            );
        }

        // Session closing — force-flush any deferred WAL commits
        // so all transactions from this session are durable.
        self.runtime.wal.forceFlush() catch |err| {
            std.log.err("WAL force flush on session close failed: {s}", .{@errorName(err)});
            @panic("WAL force flush failed on session close");
        };
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

fn writeResponseRetry(
    connection: Connection,
    response: []const u8,
) transport_mod.ConnectionError!void {
    while (true) {
        connection.writeResponse(response) catch |err| switch (err) {
            error.WouldBlock => continue,
            else => return err,
        };
        return;
    }
}

fn classifyTxControl(request: []const u8) TxControl {
    const trimmed = std.mem.trim(u8, request, " \t\r\n");
    if (trimmed.len == 0) return .none;
    if (std.ascii.eqlIgnoreCase(trimmed, "BEGIN")) return .begin;
    if (std.ascii.eqlIgnoreCase(trimmed, "COMMIT")) return .commit;
    if (std.ascii.eqlIgnoreCase(trimmed, "ROLLBACK")) return .rollback;
    return .none;
}

fn serializeTxControlOk(
    out: []u8,
    tx_op: []const u8,
) error{ResponseTooLarge}!usize {
    var stream = std.io.fixedBufferStream(out);
    const writer = stream.writer();
    writer.print(
        "OK tx={s}\n",
        .{tx_op},
    ) catch return error.ResponseTooLarge;
    return stream.pos;
}

fn serializeTxControlError(
    out: []u8,
    code: []const u8,
) error{ResponseTooLarge}!usize {
    var stream = std.io.fixedBufferStream(out);
    const writer = stream.writer();
    writer.print(
        "ERR class=invalid_request code={s}\n",
        .{code},
    ) catch return error.ResponseTooLarge;
    return stream.pos;
}

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

pub fn serializeValue(
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

test "session marks mutation requests with had_mutation=true" {
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
        "User |> insert(id = 1, name = \"Alice\", active = true) {}",
        response_buf[0..],
    );
    try pool.checkin(&conn);

    try std.testing.expect(!result.is_query_error);
    try std.testing.expect(result.had_mutation);
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
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
        "User |> where(id == 1)",
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
            "INSPECT pool policy=reject size=1 checked_out=1 pinned=0 exhausted_total=0 pin_invariant_ok=true\n",
        ) != null,
    );
    const pool_line = extractInspectLine(output, "INSPECT pool ") orelse
        return error.TestUnexpectedResult;
    const pool_size = parseInspectU64(pool_line, "size") orelse
        return error.TestUnexpectedResult;
    const pool_checked_out = parseInspectU64(pool_line, "checked_out") orelse
        return error.TestUnexpectedResult;
    const pool_pinned = parseInspectU64(pool_line, "pinned") orelse
        return error.TestUnexpectedResult;
    try std.testing.expect(pool_pinned <= pool_checked_out);
    try std.testing.expect(pool_checked_out <= pool_size);
    try std.testing.expect(
        std.mem.indexOf(u8, pool_line, "pin_invariant_ok=true") != null,
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
            "INSPECT spill temp_pages_allocated=0 temp_pages_reclaimed=0 temp_bytes_written=0 temp_bytes_read=0\n",
        ) != null,
    );
    try std.testing.expect(
        std.mem.indexOf(
            u8,
            output,
            "INSPECT plan source_model=User pipeline=inspect join_strategy=none join_order=none materialization=none sort_strategy=none group_strategy=none nested_relations=0 nested_join_nested_loop=0 nested_join_hash_in_memory=0 nested_join_hash_spill=0\n",
        ) != null,
    );
    try std.testing.expect(
        std.mem.indexOf(
            u8,
            output,
            "INSPECT explain sort=not_applied group=not_applied nested_join_breakdown=nested_loop:0,hash_in_memory:0,hash_spill:0\n",
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
            .close = &close,
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

        fn close(_: *anyopaque) void {}
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
            .close = &close,
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

        fn close(_: *anyopaque) void {}
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

fn extractInspectLine(output: []const u8, prefix: []const u8) ?[]const u8 {
    const start = std.mem.indexOf(u8, output, prefix) orelse return null;
    const rest = output[start..];
    const end_rel = std.mem.indexOfScalar(u8, rest, '\n') orelse return null;
    return rest[0 .. end_rel + 1];
}

fn parseInspectU64(line: []const u8, key: []const u8) ?u64 {
    var iter = std.mem.tokenizeScalar(u8, line, ' ');
    while (iter.next()) |token| {
        if (!std.mem.startsWith(u8, token, key)) continue;
        if (token.len <= key.len + 1) return null;
        if (token[key.len] != '=') continue;
        return std.fmt.parseInt(u64, token[key.len + 1 ..], 10) catch null;
    }
    return null;
}
