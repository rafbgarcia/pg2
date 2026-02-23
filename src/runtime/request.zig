//! Request execution adapter at runtime boundary.
//!
//! Responsibilities in this file:
//! - Converts checked-out pool/runtime state into executor context.
//! - Executes parsed requests through a lease-owned transaction snapshot.
//! - Centralizes request-to-executor wiring used by server session handling.
const std = @import("std");
const bootstrap_mod = @import("bootstrap.zig");
const exec_mod = @import("../executor/executor.zig");
const catalog_mod = @import("../catalog/catalog.zig");
const pool_mod = @import("../server/pool.zig");
const parser_mod = @import("../parser/parser.zig");
const tokenizer_mod = @import("../parser/tokenizer.zig");
const disk_mod = @import("../simulator/disk.zig");

const BootstrappedRuntime = bootstrap_mod.BootstrappedRuntime;
const Catalog = catalog_mod.Catalog;
const PoolConn = pool_mod.PoolConn;
const ConnectionPool = pool_mod.ConnectionPool;
const Ast = @import("../parser/ast.zig").Ast;
const TokenizeResult = tokenizer_mod.TokenizeResult;

pub const RequestError = pool_mod.PoolError || error{OutOfMemory};

pub const ExecuteRequest = struct {
    catalog: *Catalog,
    pool_conn: *const PoolConn,
    ast: *const Ast,
    tokens: *const TokenizeResult,
    source: []const u8,
    parameter_bindings: []const exec_mod.ParameterBinding = &.{},
};

pub fn executeWithPoolConn(
    runtime: *BootstrappedRuntime,
    request: ExecuteRequest,
) RequestError!exec_mod.QueryResult {
    const ctx = makeExecContext(runtime, request);
    return exec_mod.execute(&ctx);
}

fn makeExecContext(
    runtime: *BootstrappedRuntime,
    request: ExecuteRequest,
) exec_mod.ExecContext {
    const statement_timestamp_micros: i64 = @intCast(@max(@as(i64, 0), std.time.microTimestamp()));
    return .{
        .catalog = request.catalog,
        .pool = &runtime.pool,
        .wal = &runtime.wal,
        .tx_manager = &runtime.tx_manager,
        .undo_log = &runtime.undo_log,
        .tx_id = request.pool_conn.tx_id,
        .snapshot = &request.pool_conn.snapshot,
        .ast = request.ast,
        .tokens = request.tokens,
        .source = request.source,
        .statement_timestamp_micros = statement_timestamp_micros,
        .parameter_bindings = request.parameter_bindings,
        .allocator = runtime.static_allocator.allocator(),
        .result_rows = request.pool_conn.query_buffers.result_rows,
        .scratch_rows_a = request.pool_conn.query_buffers.scratch_rows_a,
        .scratch_rows_b = request.pool_conn.query_buffers.scratch_rows_b,
        .string_arena_bytes = request.pool_conn.query_buffers.string_arena_bytes,
        .storage = runtime.storage,
        .query_slot_index = request.pool_conn.query_buffers.slot_index,
        .collector = request.pool_conn.query_buffers.collector,
        .work_memory_bytes_per_slot = request.pool_conn.query_buffers.work_memory_bytes_per_slot,
    };
}

test "executeWithPoolConn uses pool lease and caller controls release" {
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
    defer runtime.deinit();

    var catalog = Catalog{};
    var pool = ConnectionPool.init(&runtime);
    var conn = try pool.checkout();

    const source = "User";
    const tokens = tokenizer_mod.tokenize(source);
    const parsed = parser_mod.parse(&tokens, source);
    try std.testing.expect(!parsed.has_error);

    var result = try executeWithPoolConn(
        &runtime,
        .{
            .catalog = &catalog,
            .pool_conn = &conn,
            .ast = &parsed.ast,
            .tokens = &tokens,
            .source = source,
        },
    );
    defer result.deinit();

    try std.testing.expect(result.has_error);
    try std.testing.expectError(
        error.PoolExhausted,
        pool.checkout(),
    );

    try pool.checkin(&conn);
    var reused = try pool.checkout();
    try std.testing.expectEqual(@as(u16, 0), reused.slot_index);
    try pool.checkin(&reused);
}
