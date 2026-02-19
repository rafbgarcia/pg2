const std = @import("std");
const bootstrap_mod = @import("../runtime/bootstrap.zig");
const request_mod = @import("../runtime/request.zig");
const parser_mod = @import("../parser/parser.zig");
const tokenizer_mod = @import("../parser/tokenizer.zig");
const exec_mod = @import("../executor/executor.zig");
const transport_mod = @import("transport.zig");
const row_mod = @import("../storage/row.zig");
const catalog_mod = @import("../catalog/catalog.zig");
const tx_mod = @import("../mvcc/transaction.zig");
const heap_mod = @import("../storage/heap.zig");
const disk_mod = @import("../simulator/disk.zig");

const BootstrappedRuntime = bootstrap_mod.BootstrappedRuntime;
const Catalog = catalog_mod.Catalog;
const TxId = tx_mod.TxId;
const Snapshot = tx_mod.Snapshot;
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
/// This path tokenizes/parses a query string, executes through runtime-leased
/// buffers, serializes response bytes, and only then releases the leased slot.
pub const Session = struct {
    runtime: *BootstrappedRuntime,
    catalog: *Catalog,

    pub fn init(
        runtime: *BootstrappedRuntime,
        catalog: *Catalog,
    ) Session {
        return .{ .runtime = runtime, .catalog = catalog };
    }

    pub fn handleRequest(
        self: *Session,
        tx_id: TxId,
        snapshot: *const Snapshot,
        source: []const u8,
        out: []u8,
    ) SessionError!SessionResponse {
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

        var execution = try request_mod.executeWithLeasedQueryBuffers(
            self.runtime,
            .{
                .catalog = self.catalog,
                .tx_id = tx_id,
                .snapshot = snapshot,
                .ast = &parsed.ast,
                .tokens = &tokens,
                .source = source,
            },
        );
        defer execution.deinit();

        try serializeQueryResult(writer, &execution.result);
        return .{
            .bytes_written = stream.pos,
            .is_query_error = execution.result.has_error,
        };
    }

    /// Serve all requests from one accepted connection using bounded buffers.
    pub fn serveConnection(
        self: *Session,
        connection: Connection,
        tx_id: TxId,
        snapshot: *const Snapshot,
        request_buf: []u8,
        response_buf: []u8,
    ) ServeError!void {
        while (true) {
            const request_opt = try connection.readRequest(request_buf);
            const request = request_opt orelse break;
            if (request.len > request_buf.len) return error.RequestTooLarge;

            const response = try self.handleRequest(
                tx_id,
                snapshot,
                request,
                response_buf,
            );
            try connection.writeResponse(
                response_buf[0..response.bytes_written],
            );
        }
    }

    /// Accept and serve all currently pending connections.
    pub fn serveAcceptedConnections(
        self: *Session,
        acceptor: Acceptor,
        tx_id: TxId,
        snapshot: *const Snapshot,
        request_buf: []u8,
        response_buf: []u8,
    ) ServeError!usize {
        var served_connections: usize = 0;
        while (true) {
            const connection = (try acceptor.accept()) orelse break;
            try self.serveConnection(
                connection,
                tx_id,
                snapshot,
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

fn serializeQueryResult(
    writer: anytype,
    result: *const exec_mod.QueryResult,
) error{ResponseTooLarge}!void {
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
        var column_index: usize = 0;
        while (column_index < row.column_count) : (column_index += 1) {
            if (column_index > 0) {
                writer.writeAll(",") catch return error.ResponseTooLarge;
            }
            try serializeValue(writer, row.values[column_index]);
        }
        writer.writeAll("\n") catch return error.ResponseTooLarge;
    }
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

    const tx_id = try runtime.tx_manager.begin();
    var snapshot = try runtime.tx_manager.snapshot(tx_id);
    defer snapshot.deinit();

    var session = Session.init(&runtime, &catalog);
    var response_buf: [1024]u8 = undefined;

    const first = try session.handleRequest(
        tx_id,
        &snapshot,
        "User",
        response_buf[0..],
    );
    try std.testing.expect(!first.is_query_error);
    try std.testing.expectEqualStrings(
        "OK rows=0\n",
        response_buf[0..first.bytes_written],
    );

    const second = try session.handleRequest(
        tx_id,
        &snapshot,
        "User",
        response_buf[0..],
    );
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

    const tx_id = try runtime.tx_manager.begin();
    var snapshot = try runtime.tx_manager.snapshot(tx_id);
    defer snapshot.deinit();

    var session = Session.init(&runtime, &catalog);
    var response_buf: [1024]u8 = undefined;

    _ = try session.handleRequest(
        tx_id,
        &snapshot,
        "User |> insert(id = 1, name = \"Alice\", active = true)",
        response_buf[0..],
    );

    const result = try session.handleRequest(
        tx_id,
        &snapshot,
        "User",
        response_buf[0..],
    );
    try std.testing.expect(!result.is_query_error);
    try std.testing.expectEqualStrings(
        "OK rows=1\n1,Alice,true\n",
        response_buf[0..result.bytes_written],
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

    const tx_id = try runtime.tx_manager.begin();
    var snapshot = try runtime.tx_manager.snapshot(tx_id);
    defer snapshot.deinit();

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
    var request_buf: [256]u8 = undefined;
    var response_buf: [256]u8 = undefined;

    const served = try session.serveAcceptedConnections(
        acceptor_state.acceptor(),
        tx_id,
        &snapshot,
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
