const std = @import("std");

pub const page_size = 8192;

pub const StorageError = error{
    ReadError,
    WriteError,
    FsyncError,
};

/// Abstraction over disk I/O. All storage engine code goes through this
/// interface so the simulation harness can replace it.
///
/// Production: wraps preadv/pwritev + fdatasync.
/// Simulation: in-memory byte array with fault injection.
pub const Storage = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        read: *const fn (ptr: *anyopaque, page_id: u64, buf: *[page_size]u8) StorageError!void,
        write: *const fn (ptr: *anyopaque, page_id: u64, data: *const [page_size]u8) StorageError!void,
        fsync: *const fn (ptr: *anyopaque) StorageError!void,
    };

    pub fn read(self: Storage, page_id: u64, buf: *[page_size]u8) StorageError!void {
        return self.vtable.read(self.ptr, page_id, buf);
    }

    pub fn write(self: Storage, page_id: u64, data: *const [page_size]u8) StorageError!void {
        return self.vtable.write(self.ptr, page_id, data);
    }

    pub fn fsync(self: Storage) StorageError!void {
        return self.vtable.fsync(self.ptr);
    }
};

/// Clock interface. Core code never accesses the system clock directly.
///
/// Production: wraps std.time.nanoTimestamp.
/// Simulation: manually advanced by the scheduler.
pub const Clock = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        now: *const fn (ptr: *anyopaque) u64,
    };

    pub fn now(self: Clock) u64 {
        return self.vtable.now(self.ptr);
    }
};

/// Network interface for replication.
///
/// Production: TCP sockets.
/// Simulation: in-memory message queue with configurable faults.
pub const PeerId = u64;

pub const Message = struct {
    from: PeerId,
    to: PeerId,
    data: []const u8,
};

pub const Network = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        send: *const fn (ptr: *anyopaque, to: PeerId, data: []const u8) void,
        recv: *const fn (ptr: *anyopaque) ?Message,
    };

    pub fn send(self: Network, to: PeerId, data: []const u8) void {
        self.vtable.send(self.ptr, to, data);
    }

    pub fn recv(self: Network) ?Message {
        return self.vtable.recv(self.ptr);
    }
};

// --- Production implementations ---

/// Real clock backed by the OS monotonic timer.
pub const RealClock = struct {
    pub fn clock(self: *RealClock) Clock {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &vtable,
        };
    }

    const vtable = Clock.VTable{
        .now = &nowImpl,
    };

    fn nowImpl(_: *anyopaque) u64 {
        return @intCast(@max(0, std.time.nanoTimestamp()));
    }
};

test "RealClock returns monotonic time" {
    var rc = RealClock{};
    const c = rc.clock();
    const t1 = c.now();
    const t2 = c.now();
    try std.testing.expect(t2 >= t1);
}
