//! Startup planner deriving runtime capacities from memory and vCPU input.
//!
//! Responsibilities in this file:
//! - Converts startup envelope (`--memory`, detected vCPUs) into runtime config.
//! - Encodes throughput-first policy ratios/floors as internal constants.
//! - Produces deterministic capacities for bootstrap and parser bounds.
const std = @import("std");
const bootstrap_mod = @import("bootstrap.zig");
const io_mod = @import("../storage/io.zig");
const scan_mod = @import("../executor/scan.zig");
const spill_collector_mod = @import("../executor/spill_collector.zig");
const tokenizer_mod = @import("../parser/tokenizer.zig");
const ast_mod = @import("../parser/ast.zig");

const BootstrapConfig = bootstrap_mod.BootstrapConfig;
const ResultRow = scan_mod.ResultRow;
const scan_batch_size = scan_mod.scan_batch_size;
const SpillingResultCollector = spill_collector_mod.SpillingResultCollector;

pub const PlannerError = error{
    InvalidInput,
    InsufficientMemoryBudget,
    Overflow,
};

pub const PlannerPolicy = struct {
    // Ratios in permille.
    shared_buffer_pool_ratio_permille: u16 = 420,
    shared_wal_ratio_permille: u16 = 80,
    shared_undo_ratio_permille: u16 = 120,

    min_query_string_arena_bytes_per_slot: usize = 512 * 1024,
    min_work_memory_bytes_per_slot: usize = 256 * 1024,
    min_parser_bytes_per_slot: usize = 32 * 1024,
    min_temp_pages_per_slot: u64 = 64,
    min_wal_buffer_capacity_bytes: usize = 64 * 1024,
    min_undo_data_bytes: usize = 64 * 1024,
    min_undo_entries: u32 = 256,
    hard_max_query_slots: u16 = 1024,

    tx_active_multiplier: u16 = 4,
    tx_active_floor: u16 = 256,
    tx_state_multiplier: u32 = 256,

    hard_max_tokens: u16 = tokenizer_mod.max_tokens,
    hard_max_ast_nodes: u16 = ast_mod.max_ast_nodes,
};

pub const Plan = struct {
    memory_budget_bytes: usize,
    detected_vcpus: u16,
    max_query_slots: u16,
    max_active_transactions: u16,
    max_tx_states: u32,
    query_pool_bytes: usize,
    work_memory_bytes_per_slot: usize,
    temp_pages_per_query_slot: u64,
    max_tokens_effective: u16,
    max_ast_nodes_effective: u16,
    hard_max_tokens: u16,
    hard_max_ast_nodes: u16,
    bootstrap: BootstrapConfig,
};

pub fn planFromMemory(
    memory_budget_bytes: usize,
    detected_vcpus: u16,
) PlannerError!Plan {
    return planWithPolicy(memory_budget_bytes, detected_vcpus, .{});
}

pub fn planWithPolicy(
    memory_budget_bytes: usize,
    detected_vcpus: u16,
    policy: PlannerPolicy,
) PlannerError!Plan {
    if (memory_budget_bytes == 0) return error.InvalidInput;
    if (detected_vcpus == 0) return error.InvalidInput;
    if (policy.hard_max_tokens == 0 or policy.hard_max_ast_nodes == 0) {
        return error.InvalidInput;
    }

    const buffer_pool_bytes = ratio(memory_budget_bytes, policy.shared_buffer_pool_ratio_permille);
    const wal_budget_bytes = @max(
        ratio(memory_budget_bytes, policy.shared_wal_ratio_permille),
        policy.min_wal_buffer_capacity_bytes,
    );
    const undo_budget_bytes = @max(
        ratio(memory_budget_bytes, policy.shared_undo_ratio_permille),
        policy.min_undo_data_bytes,
    );
    const query_pool_bytes = memory_budget_bytes -
        @min(memory_budget_bytes, buffer_pool_bytes + wal_budget_bytes + undo_budget_bytes);

    const cpu_limited_slots = @min(detected_vcpus, policy.hard_max_query_slots);
    if (cpu_limited_slots == 0) return error.InvalidInput;

    const per_slot_fixed_floor_base = perSlotFixedBytes(policy.tx_active_floor) +
        policy.min_query_string_arena_bytes_per_slot +
        policy.min_work_memory_bytes_per_slot +
        policy.min_parser_bytes_per_slot;
    const memory_limited_slots_approx = @max(@as(usize, 1), query_pool_bytes / per_slot_fixed_floor_base);
    var slots: u16 = @intCast(@min(@as(usize, cpu_limited_slots), memory_limited_slots_approx));
    if (slots == 0) slots = 1;

    while (slots > 0) : (slots -= 1) {
        const tx_caps = deriveTxCaps(slots, policy);
        const slot_fixed = perSlotFixedBytes(tx_caps.max_active_transactions);
        const per_slot_budget = query_pool_bytes / @as(usize, slots);
        const min_per_slot_budget = slot_fixed +
            policy.min_query_string_arena_bytes_per_slot +
            policy.min_work_memory_bytes_per_slot +
            policy.min_parser_bytes_per_slot;
        if (per_slot_budget < min_per_slot_budget) continue;

        const spill_budget = per_slot_budget - slot_fixed;
        const string_arena_bytes_per_slot = @max(
            policy.min_query_string_arena_bytes_per_slot,
            spill_budget / 4,
        );
        const parser_bytes_per_slot = @max(
            policy.min_parser_bytes_per_slot,
            spill_budget / 8,
        );
        const consumed = string_arena_bytes_per_slot + parser_bytes_per_slot;
        if (consumed >= spill_budget) continue;
        const work_memory_bytes_per_slot = @max(
            policy.min_work_memory_bytes_per_slot,
            spill_budget - consumed,
        );

        const parser = deriveParserCaps(
            parser_bytes_per_slot,
            policy.hard_max_tokens,
            policy.hard_max_ast_nodes,
        );

        const temp_pages_per_query_slot = @max(
            policy.min_temp_pages_per_slot,
            @as(u64, @intCast(work_memory_bytes_per_slot / io_mod.page_size)),
        );
        const wal_buffer_capacity_bytes = wal_budget_bytes;
        const wal_flush_threshold_bytes = wal_buffer_capacity_bytes / 2;
        const undo_data_bytes = @max(policy.min_undo_data_bytes, undo_budget_bytes * 3 / 4);
        const undo_entries_budget = undo_budget_bytes - @min(undo_budget_bytes, undo_data_bytes);
        const undo_entries = @max(
            policy.min_undo_entries,
            @as(u32, @intCast(undo_entries_budget / @sizeOf(@import("../mvcc/undo.zig").UndoEntry))),
        );
        const buffer_pool_frames = @max(@as(usize, 1), buffer_pool_bytes / io_mod.page_size);
        if (buffer_pool_frames > std.math.maxInt(u16)) return error.Overflow;

        const bootstrap: BootstrapConfig = .{
            .buffer_pool_frames = @intCast(buffer_pool_frames),
            .undo_max_entries = undo_entries,
            .undo_max_data_bytes = @intCast(@min(undo_data_bytes, std.math.maxInt(u32))),
            .wal_buffer_capacity_bytes = wal_buffer_capacity_bytes,
            .wal_flush_threshold_bytes = wal_flush_threshold_bytes,
            .max_query_slots = slots,
            .max_active_transactions = tx_caps.max_active_transactions,
            .max_tx_states = tx_caps.max_tx_states,
            .query_string_arena_bytes_per_slot = string_arena_bytes_per_slot,
            .temp_pages_per_query_slot = temp_pages_per_query_slot,
            .work_memory_bytes_per_slot = work_memory_bytes_per_slot,
            .max_tokens_effective = parser.max_tokens_effective,
            .max_ast_nodes_effective = parser.max_ast_nodes_effective,
            .hard_max_tokens = policy.hard_max_tokens,
            .hard_max_ast_nodes = policy.hard_max_ast_nodes,
        };

        return .{
            .memory_budget_bytes = memory_budget_bytes,
            .detected_vcpus = detected_vcpus,
            .max_query_slots = slots,
            .max_active_transactions = tx_caps.max_active_transactions,
            .max_tx_states = tx_caps.max_tx_states,
            .query_pool_bytes = query_pool_bytes,
            .work_memory_bytes_per_slot = work_memory_bytes_per_slot,
            .temp_pages_per_query_slot = temp_pages_per_query_slot,
            .max_tokens_effective = parser.max_tokens_effective,
            .max_ast_nodes_effective = parser.max_ast_nodes_effective,
            .hard_max_tokens = policy.hard_max_tokens,
            .hard_max_ast_nodes = policy.hard_max_ast_nodes,
            .bootstrap = bootstrap,
        };
    }

    return error.InsufficientMemoryBudget;
}

const TxCaps = struct {
    max_active_transactions: u16,
    max_tx_states: u32,
};

const ParserCaps = struct {
    max_tokens_effective: u16,
    max_ast_nodes_effective: u16,
};

fn deriveTxCaps(slots: u16, policy: PlannerPolicy) TxCaps {
    const scaled = @as(u32, slots) * @as(u32, policy.tx_active_multiplier);
    const capped = @min(scaled, @as(u32, std.math.maxInt(u16)));
    const max_active_transactions = @max(
        policy.tx_active_floor,
        @as(u16, @intCast(capped)),
    );
    const max_tx_states: u32 = @max(
        @as(u32, 1),
        @as(u32, max_active_transactions) * policy.tx_state_multiplier,
    );
    return .{
        .max_active_transactions = max_active_transactions,
        .max_tx_states = max_tx_states,
    };
}

fn deriveParserCaps(
    parser_bytes_per_slot: usize,
    hard_max_tokens: u16,
    hard_max_ast_nodes: u16,
) ParserCaps {
    const token_budget = parser_bytes_per_slot / 2;
    const ast_budget = parser_bytes_per_slot - token_budget;
    const token_cap = @max(@as(usize, 128), token_budget / @sizeOf(tokenizer_mod.Token));
    const ast_cap = @max(@as(usize, 256), ast_budget / @sizeOf(ast_mod.AstNode));
    return .{
        .max_tokens_effective = @intCast(@min(@as(usize, hard_max_tokens), token_cap)),
        .max_ast_nodes_effective = @intCast(@min(@as(usize, hard_max_ast_nodes), ast_cap)),
    };
}

fn perSlotFixedBytes(max_active_transactions: u16) usize {
    const row_arrays = @as(usize, @sizeOf(ResultRow)) * scan_batch_size * 4;
    const nested_arenas = 1024 * 1024;
    const collector = @sizeOf(SpillingResultCollector);
    const snapshot_ids = @as(usize, max_active_transactions) * @sizeOf(u64);
    return row_arrays + nested_arenas + collector + snapshot_ids + 1;
}

fn ratio(total: usize, permille: u16) usize {
    return total * permille / 1000;
}

test "planner derives slots from memory and vcpus deterministically" {
    const tiny = planFromMemory(256 * 1024 * 1024, 8) catch unreachable;
    const large = planFromMemory(1024 * 1024 * 1024, 8) catch unreachable;

    try std.testing.expect(tiny.max_query_slots >= 1);
    try std.testing.expect(large.max_query_slots >= tiny.max_query_slots);
    try std.testing.expect(large.max_query_slots <= 8);
}

test "planner scales per-slot work memory inversely with slot count" {
    const one_slot = planFromMemory(512 * 1024 * 1024, 1) catch unreachable;
    const four_slots = planFromMemory(512 * 1024 * 1024, 4) catch unreachable;

    try std.testing.expect(one_slot.max_query_slots <= four_slots.max_query_slots);
    try std.testing.expect(one_slot.work_memory_bytes_per_slot >= four_slots.work_memory_bytes_per_slot);
    try std.testing.expect(one_slot.temp_pages_per_query_slot >= four_slots.temp_pages_per_query_slot);
}

test "planner keeps parser effective caps within hard caps" {
    const plan = try planFromMemory(1024 * 1024 * 1024, 8);
    try std.testing.expect(plan.max_tokens_effective <= plan.hard_max_tokens);
    try std.testing.expect(plan.max_ast_nodes_effective <= plan.hard_max_ast_nodes);
    try std.testing.expect(plan.max_tokens_effective > 0);
    try std.testing.expect(plan.max_ast_nodes_effective > 0);
}

test "planner fails for impossible memory envelope" {
    try std.testing.expectError(
        error.InsufficientMemoryBudget,
        planFromMemory(2 * 1024 * 1024, 1),
    );
}
