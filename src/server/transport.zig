//! Transport-agnostic server connection interfaces.
//!
//! Responsibilities in this file:
//! - Defines `Connection` request/response framing contract.
//! - Defines `Acceptor` contract for pending-connection retrieval.
//! - Provides a stable abstraction shared by TCP and io_uring backends.
pub const AcceptError = error{
    AcceptFailed,
};

pub const ConnectionError = error{
    WouldBlock,
    ReadFailed,
    WriteFailed,
    RequestTooLarge,
    ResponseTooLarge,
};

/// Transport-agnostic accepted connection.
///
/// `readRequest` returns the next complete request frame copied into `out`,
/// or `null` when the peer closed the connection.
pub const Connection = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        readRequest: *const fn (
            ptr: *anyopaque,
            out: []u8,
        ) ConnectionError!?[]const u8,
        writeResponse: *const fn (
            ptr: *anyopaque,
            data: []const u8,
        ) ConnectionError!void,
        close: *const fn (ptr: *anyopaque) void,
    };

    pub fn readRequest(
        self: Connection,
        out: []u8,
    ) ConnectionError!?[]const u8 {
        return self.vtable.readRequest(self.ptr, out);
    }

    pub fn writeResponse(
        self: Connection,
        data: []const u8,
    ) ConnectionError!void {
        return self.vtable.writeResponse(self.ptr, data);
    }

    pub fn close(self: Connection) void {
        self.vtable.close(self.ptr);
    }
};

/// Transport accept loop abstraction.
///
/// `accept` returns `null` when there are currently no more pending
/// connections to serve.
pub const Acceptor = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        accept: *const fn (ptr: *anyopaque) AcceptError!?Connection,
    };

    pub fn accept(self: Acceptor) AcceptError!?Connection {
        return self.vtable.accept(self.ptr);
    }
};
