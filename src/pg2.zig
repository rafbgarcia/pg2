pub const storage = struct {
    pub const io = @import("storage/io.zig");
    pub const page = @import("storage/page.zig");
    pub const buffer_pool = @import("storage/buffer_pool.zig");
    pub const wal = @import("storage/wal.zig");
    pub const heap = @import("storage/heap.zig");
};

pub const mvcc = struct {
    pub const transaction = @import("mvcc/transaction.zig");
    pub const undo = @import("mvcc/undo.zig");
};

pub const simulator = struct {
    pub const disk = @import("simulator/disk.zig");
    pub const clock = @import("simulator/clock.zig");
};

comptime {
    // Force test discovery in all imported modules.
    _ = storage.io;
    _ = storage.page;
    _ = storage.buffer_pool;
    _ = storage.wal;
    _ = storage.heap;
    _ = mvcc.transaction;
    _ = mvcc.undo;
    _ = simulator.disk;
    _ = simulator.clock;
}
