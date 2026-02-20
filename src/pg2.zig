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
    pub const expression = @import("parser/expression.zig");
    pub const parse = @import("parser/parser.zig");
};

pub const catalog = struct {
    pub const meta = @import("catalog/catalog.zig");
    pub const schema_loader = @import("catalog/schema_loader.zig");
};

pub const executor = struct {
    pub const exec = @import("executor/executor.zig");
    pub const capacity = @import("executor/capacity.zig");
    pub const scan = @import("executor/scan.zig");
    pub const filter = @import("executor/filter.zig");
    pub const mutation = @import("executor/mutation.zig");
};

pub const simulator = struct {
    pub const disk = @import("simulator/disk.zig");
    pub const clock = @import("simulator/clock.zig");
    pub const fault_matrix = @import("simulator/fault_matrix.zig");
    pub const fk_fault_matrix = @import("simulator/fk_fault_matrix.zig");
};

pub const runtime = struct {
    pub const bootstrap = @import("runtime/bootstrap.zig");
    pub const config = @import("runtime/config.zig");
    pub const request = @import("runtime/request.zig");
};

pub const server = struct {
    pub const session = @import("server/session.zig");
    pub const e2e_specs = @import("server/e2e_specs.zig");
    pub const pool = @import("server/pool.zig");
    pub const transport = @import("server/transport.zig");
    pub const tcp_transport = @import("server/tcp_transport.zig");
    pub const io_uring_transport = @import("server/io_uring_transport.zig");
};

pub const tiger = struct {
    pub const error_taxonomy = @import("tiger/error_taxonomy.zig");
    pub const static_allocator = @import("tiger/static_allocator.zig");
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
    _ = parser.expression;
    _ = parser.parse;
    _ = catalog.meta;
    _ = catalog.schema_loader;
    _ = executor.exec;
    _ = executor.capacity;
    _ = executor.scan;
    _ = executor.filter;
    _ = executor.mutation;
    _ = simulator.disk;
    _ = simulator.clock;
    _ = simulator.fault_matrix;
    _ = simulator.fk_fault_matrix;
    _ = runtime.bootstrap;
    _ = runtime.config;
    _ = runtime.request;
    _ = server.session;
    _ = server.e2e_specs;
    _ = server.pool;
    _ = server.transport;
    _ = server.tcp_transport;
    _ = server.io_uring_transport;
    _ = tiger.error_taxonomy;
    _ = tiger.static_allocator;
}
