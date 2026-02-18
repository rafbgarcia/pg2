const std = @import("std");
const bootstrap_mod = @import("bootstrap.zig");
const exec_mod = @import("../executor/executor.zig");
const catalog_mod = @import("../catalog/catalog.zig");
const tx_mod = @import("../mvcc/transaction.zig");
const parser_mod = @import("../parser/parser.zig");
const tokenizer_mod = @import("../parser/tokenizer.zig");
const disk_mod = @import("../simulator/disk.zig");

const BootstrappedRuntime = bootstrap_mod.BootstrappedRuntime;
const QueryBuffers = bootstrap_mod.QueryBuffers;
const Catalog = catalog_mod.Catalog;
const TxId = tx_mod.TxId;
const Snapshot = tx_mod.Snapshot;
const Ast = @import("../parser/ast.zig").Ast;
const TokenizeResult = tokenizer_mod.TokenizeResult;

pub const RequestError = bootstrap_mod.QueryBufferError || error{OutOfMemory};

pub const ExecuteRequest = struct {
    catalog: *Catalog,
    tx_id: TxId,
    snapshot: *const Snapshot,
    ast: *const Ast,
    tokens: *const TokenizeResult,
    source: []const u8,
};

/// Holds a leased runtime query slot and the query result backed by that slot.
/// Call deinit() after the caller finishes consuming result rows.
pub const LeasedExecution = struct {
    runtime: *BootstrappedRuntime,
    slot_index: u16,
    result: exec_mod.QueryResult,

    pub fn deinit(self: *LeasedExecution) void {
        self.result.deinit();
        self.runtime.releaseQueryBuffers(self.slot_index) catch {
            @panic("leased execution slot release failed");
        };
        self.* = undefined;
    }
};

pub fn executeWithLeasedQueryBuffers(
    runtime: *BootstrappedRuntime,
    request: ExecuteRequest,
) RequestError!LeasedExecution {
    const buffers = try runtime.acquireQueryBuffers();
    errdefer {
        runtime.releaseQueryBuffers(buffers.slot_index) catch {
            @panic("query slot release failed after execution error");
        };
    }

    const ctx = makeExecContext(runtime, request, buffers);
    const result = try exec_mod.execute(&ctx);

    return .{
        .runtime = runtime,
        .slot_index = buffers.slot_index,
        .result = result,
    };
}

fn makeExecContext(
    runtime: *BootstrappedRuntime,
    request: ExecuteRequest,
    buffers: QueryBuffers,
) exec_mod.ExecContext {
    return .{
        .catalog = request.catalog,
        .pool = &runtime.pool,
        .wal = &runtime.wal,
        .tx_manager = &runtime.tx_manager,
        .undo_log = &runtime.undo_log,
        .tx_id = request.tx_id,
        .snapshot = request.snapshot,
        .ast = request.ast,
        .tokens = request.tokens,
        .source = request.source,
        .allocator = runtime.static_allocator.allocator(),
        .result_rows = buffers.result_rows,
        .scratch_rows_a = buffers.scratch_rows_a,
        .scratch_rows_b = buffers.scratch_rows_b,
    };
}

test "executeWithLeasedQueryBuffers holds slot until deinit" {
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

    const tx_id = try runtime.tx_manager.begin();
    var snapshot = try runtime.tx_manager.snapshot(tx_id);
    defer snapshot.deinit();

    const source = "User";
    const tokens = tokenizer_mod.tokenize(source);
    const parsed = parser_mod.parse(&tokens, source);
    try std.testing.expect(!parsed.has_error);

    var execution = try executeWithLeasedQueryBuffers(
        &runtime,
        .{
            .catalog = &catalog,
            .tx_id = tx_id,
            .snapshot = &snapshot,
            .ast = &parsed.ast,
            .tokens = &tokens,
            .source = source,
        },
    );

    try std.testing.expect(execution.result.has_error);
    try std.testing.expectError(
        error.NoQuerySlotAvailable,
        runtime.acquireQueryBuffers(),
    );

    execution.deinit();

    const reused = try runtime.acquireQueryBuffers();
    try std.testing.expectEqual(@as(u16, 0), reused.slot_index);
    try runtime.releaseQueryBuffers(reused.slot_index);
}
