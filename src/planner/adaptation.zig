//! Deterministic checkpoint adaptation rules (WF07 phase 3 foundation).
const std = @import("std");
const types = @import("types.zig");

pub const AdaptationResult = struct {
    checkpoint: types.Checkpoint,
    degraded: bool = false,
    reason: types.ReasonCode = .none,
};

fn wouldExceedJoinBuildBudget(
    snapshot: *const types.PlannerInputSnapshot,
    counters: *const types.CheckpointCounters,
) bool {
    const rows = @as(u128, counters.join_build_rows);
    const width = @as(u128, snapshot.average_row_width_bytes);
    const budget = @as(u128, snapshot.join_build_budget_bytes);
    return rows * width > budget;
}

pub fn adaptAtCheckpoint(
    snapshot: *const types.PlannerInputSnapshot,
    checkpoint: types.Checkpoint,
    counters: *const types.CheckpointCounters,
    decisions: *types.PhysicalDecisionSet,
) types.PlannerError!AdaptationResult {
    try snapshot.validate();

    var result = AdaptationResult{ .checkpoint = checkpoint };

    // Sort degrade rule: bytes_accumulated > work_memory * 3/4.
    if (
        decisions.sort_strategy == .in_memory_merge and
        counters.bytes_accumulated > (snapshot.work_memory_bytes_per_slot * 3) / 4
    ) {
        decisions.sort_strategy = .external_merge;
        decisions.sort_reason = .SORT_EXTERNAL_REQUIRED_BY_ROWFLOW;
        result.degraded = true;
        result.reason = decisions.sort_reason;
    }

    // Group degrade rule: estimated groups above cap.
    if (
        decisions.group_strategy == .in_memory_linear and
        counters.group_count_estimate > snapshot.aggregate_groups_cap
    ) {
        decisions.group_strategy = .hash_spill;
        decisions.group_reason = .GROUP_HASH_SPILL_GROUP_CAP_EXCEEDED;
        result.degraded = true;
        result.reason = decisions.group_reason;
    }

    // Join degrade rule: build side bytes above budget.
    if (
        decisions.join_strategy == .hash_in_memory and
        wouldExceedJoinBuildBudget(snapshot, counters)
    ) {
        decisions.join_strategy = .hash_spill;
        decisions.join_reason = .JOIN_HASH_SPILL_RIGHT_EXCEEDS_BUILD_WINDOW;
        result.degraded = true;
        result.reason = decisions.join_reason;
    }

    // Safety contract: if any degradation happened, require bounded materialization.
    if (result.degraded and decisions.materialization_mode == .none) {
        decisions.materialization_mode = .bounded_row_buffers;
        decisions.materialization_reason = .MATERIALIZE_BOUNDED_REQUIRED;
    }

    // Streaming is degrade-only in this phase. If enabled, force disable.
    if (decisions.streaming_mode == .enabled) {
        decisions.streaming_mode = .disabled;
        decisions.streaming_reason = .STREAMING_DISABLED_RISK_UNBOUNDED;
        if (!result.degraded) {
            result.degraded = true;
            result.reason = decisions.streaming_reason;
        }
    }

    return result;
}

test "sort degrade triggers when rowflow bytes exceed threshold" {
    var snapshot: types.PlannerInputSnapshot = .{
        .query_shape_fingerprint = 1,
        .catalog_snapshot_id = 2,
        .runtime_counters_snapshot_id = 3,
        .capacity_profile_id = 4,
        .work_memory_bytes_per_slot = 1024,
        .aggregate_groups_cap = 64,
        .join_build_budget_bytes = 2048,
        .average_row_width_bytes = 32,
    };
    var decisions: types.PhysicalDecisionSet = .{
        .sort_strategy = .in_memory_merge,
        .sort_reason = .SORT_IN_MEMORY_WITHIN_BUDGET,
    };
    const counters: types.CheckpointCounters = .{
        .bytes_accumulated = 900,
    };

    const result = try adaptAtCheckpoint(&snapshot, .post_filter, &counters, &decisions);
    try std.testing.expect(result.degraded);
    try std.testing.expectEqual(types.SortStrategy.external_merge, decisions.sort_strategy);
    try std.testing.expectEqual(
        types.ReasonCode.SORT_EXTERNAL_REQUIRED_BY_ROWFLOW,
        decisions.sort_reason,
    );
}

test "degrade-only monotonicity never upgrades sort back to in-memory" {
    var snapshot: types.PlannerInputSnapshot = .{
        .query_shape_fingerprint = 1,
        .catalog_snapshot_id = 2,
        .runtime_counters_snapshot_id = 3,
        .capacity_profile_id = 4,
        .work_memory_bytes_per_slot = 1024,
        .aggregate_groups_cap = 64,
        .join_build_budget_bytes = 2048,
        .average_row_width_bytes = 32,
    };
    var decisions: types.PhysicalDecisionSet = .{
        .sort_strategy = .in_memory_merge,
        .sort_reason = .SORT_IN_MEMORY_WITHIN_BUDGET,
    };

    const high_counters: types.CheckpointCounters = .{ .bytes_accumulated = 900 };
    _ = try adaptAtCheckpoint(&snapshot, .post_filter, &high_counters, &decisions);
    try std.testing.expectEqual(types.SortStrategy.external_merge, decisions.sort_strategy);

    const low_counters: types.CheckpointCounters = .{ .bytes_accumulated = 1 };
    _ = try adaptAtCheckpoint(&snapshot, .pre_join, &low_counters, &decisions);
    try std.testing.expectEqual(types.SortStrategy.external_merge, decisions.sort_strategy);
}
