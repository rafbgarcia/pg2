pub const storage = struct {
    pub const io = @import("storage/io.zig");
    pub const page = @import("storage/page.zig");
    pub const buffer_pool = @import("storage/buffer_pool.zig");
    pub const wal = @import("storage/wal.zig");
    pub const heap = @import("storage/heap.zig");
    pub const btree = @import("storage/btree.zig");
    pub const row = @import("storage/row.zig");
};

pub const mvcc = struct {
    pub const transaction = @import("mvcc/transaction.zig");
    pub const undo = @import("mvcc/undo.zig");
};

pub const parser = struct {
    pub const tokenizer = @import("parser/tokenizer.zig");
    pub const ast = @import("parser/ast.zig");
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
    _ = storage.btree;
    _ = storage.row;
    _ = mvcc.transaction;
    _ = mvcc.undo;
    _ = parser.tokenizer;
    _ = parser.ast;
    _ = simulator.disk;
    _ = simulator.clock;
}
