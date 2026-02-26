//! Deterministic replay coverage for planner parallel schedule traces.
const std = @import("std");
const pg2 = @import("pg2");

const types = pg2.planner.types;
const planner = pg2.planner.planner;
const parallel = pg2.planner.parallel;

fn snapshotWithParallelGate(enabled: bool) types.PlannerInputSnapshot {
    return .{
        .query_shape_fingerprint = 0x77,
        .catalog_snapshot_id = 101,
        .runtime_counters_snapshot_id = 102,
        .capacity_profile_id = 103,
        .work_memory_bytes_per_slot = 2048,
        .aggregate_groups_cap = 64,
        .join_build_budget_bytes = 4096,
        .average_row_width_bytes = 64,
        .feature_gate_mask = if (enabled) types.feature_gate_parallel_policy else 0,
        .operator_sequence = blk: {
            var seq = [_]types.OpTag{.none} ** types.max_operator_sequence;
            seq[0] = .where_filter;
            seq[1] = .group_op;
            seq[2] = .sort_op;
            break :blk seq;
        },
        .relation_ids_sorted = blk: {
            var rel = [_]u32{0} ** types.max_relations;
            rel[0] = 1;
            rel[1] = 4;
            break :blk rel;
        },
    };
}

test "sim parallel schedule trace is deterministic under fixed inputs" {
    const snapshot = snapshotWithParallelGate(true);
    const decisions = try planner.planInitial(&snapshot);
    const trace_a = try parallel.buildScheduleTrace(&snapshot, &decisions);
    const trace_b = try parallel.buildScheduleTrace(&snapshot, &decisions);
    try std.testing.expectEqual(trace_a.mode, trace_b.mode);
    try std.testing.expectEqual(trace_a.task_count, trace_b.task_count);
    try std.testing.expectEqualSlices(
        parallel.ParallelTask,
        trace_a.tasks[0..trace_a.task_count],
        trace_b.tasks[0..trace_b.task_count],
    );
}

test "sim parallel policy defaults to sequential when feature gate disabled" {
    const snapshot = snapshotWithParallelGate(false);
    const decisions = try planner.planInitial(&snapshot);
    try std.testing.expectEqual(types.ParallelMode.sequential, decisions.parallel_mode);
    const trace = try parallel.buildScheduleTrace(&snapshot, &decisions);
    try std.testing.expectEqual(types.ParallelMode.sequential, trace.mode);
    try std.testing.expectEqual(@as(u8, 0), trace.task_count);
}
