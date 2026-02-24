//! Deterministic transport progress-contract tests for server scheduling.
const std = @import("std");
const pg2 = @import("pg2");

const transport_mod = pg2.server.transport;
const Connection = transport_mod.Connection;
const ConnectionError = transport_mod.ConnectionError;

test "connection progress contract supports deterministic would-block retry" {
    var scripted = ScriptedConnection{
        .request = "User {}\n",
        .read_would_block_budget = 2,
        .write_would_block_budget = 2,
    };
    const conn = scripted.connection();

    var request_buf: [64]u8 = undefined;
    const request = try readUntilFrame(conn, request_buf[0..], 16);
    try std.testing.expectEqualStrings("User {}", request);
    try std.testing.expectEqual(@as(usize, 2), scripted.read_would_block_count);

    try writeUntilFlushed(conn, "OK\n", 16);
    try std.testing.expectEqual(@as(usize, 2), scripted.write_would_block_count);
    try std.testing.expectEqualStrings("OK\n", scripted.last_response[0..scripted.last_response_len]);

    // No further requests after the one scripted frame.
    const none = try readUntilOptionalFrame(conn, request_buf[0..], 4);
    try std.testing.expect(none == null);
}

test "connection close is idempotent and preserves first close side effects" {
    var scripted = ScriptedConnection{
        .request = "User {}\n",
    };
    const conn = scripted.connection();

    conn.close();
    conn.close();

    try std.testing.expect(scripted.closed);
    try std.testing.expectEqual(@as(u32, 2), scripted.close_call_count);
}

const ScriptedConnection = struct {
    request: []const u8,
    request_served: bool = false,
    read_would_block_budget: u8 = 0,
    read_would_block_count: usize = 0,
    write_would_block_budget: u8 = 0,
    write_would_block_count: usize = 0,
    closed: bool = false,
    close_call_count: u32 = 0,
    last_response: [64]u8 = undefined,
    last_response_len: usize = 0,
    pending_response: [64]u8 = undefined,
    pending_response_len: usize = 0,
    pending_response_sent: usize = 0,

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

    fn readRequest(ptr: *anyopaque, out: []u8) ConnectionError!?[]const u8 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.closed) return null;
        if (self.request_served) return null;
        if (self.read_would_block_budget > 0) {
            self.read_would_block_budget -= 1;
            self.read_would_block_count += 1;
            return error.WouldBlock;
        }

        if (self.request.len == 0 or self.request[self.request.len - 1] != '\n') {
            return error.ReadFailed;
        }

        const line_len = self.request.len - 1;
        if (line_len > out.len) return error.RequestTooLarge;
        @memcpy(out[0..line_len], self.request[0..line_len]);
        self.request_served = true;
        return out[0..line_len];
    }

    fn writeResponse(ptr: *anyopaque, data: []const u8) ConnectionError!void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.closed) return error.WriteFailed;
        if (data.len > self.pending_response.len) return error.ResponseTooLarge;

        if (self.pending_response_len == 0) {
            @memcpy(self.pending_response[0..data.len], data);
            self.pending_response_len = data.len;
            self.pending_response_sent = 0;
        } else {
            const pending = self.pending_response[0..self.pending_response_len];
            if (!std.mem.eql(u8, pending, data)) return error.WriteFailed;
        }

        if (self.write_would_block_budget > 0) {
            self.write_would_block_budget -= 1;
            self.write_would_block_count += 1;
            return error.WouldBlock;
        }

        self.pending_response_sent = self.pending_response_len;
        @memcpy(
            self.last_response[0..self.pending_response_len],
            self.pending_response[0..self.pending_response_len],
        );
        self.last_response_len = self.pending_response_len;
        self.pending_response_len = 0;
        self.pending_response_sent = 0;
    }

    fn close(ptr: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.close_call_count += 1;
        self.closed = true;
    }
};

fn readUntilFrame(
    conn: Connection,
    request_buf: []u8,
    max_polls: usize,
) ![]const u8 {
    const request = try readUntilOptionalFrame(conn, request_buf, max_polls);
    return request orelse error.ReadFailed;
}

fn readUntilOptionalFrame(
    conn: Connection,
    request_buf: []u8,
    max_polls: usize,
) !?[]const u8 {
    var polls: usize = 0;
    while (polls < max_polls) : (polls += 1) {
        const result = conn.readRequest(request_buf) catch |err| switch (err) {
            error.WouldBlock => continue,
            else => return err,
        };
        return result;
    }
    return error.WouldBlock;
}

fn writeUntilFlushed(
    conn: Connection,
    response: []const u8,
    max_polls: usize,
) !void {
    var polls: usize = 0;
    while (polls < max_polls) : (polls += 1) {
        conn.writeResponse(response) catch |err| switch (err) {
            error.WouldBlock => continue,
            else => return err,
        };
        return;
    }
    return error.WouldBlock;
}
