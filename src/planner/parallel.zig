//! Deterministic planner-level parallel scheduling policy.
const std = @import("std");
const types = @import("types.zig");

pub const max_schedule_tasks: usize = 64;

pub const ParallelTask = struct {
    relation_id: u32,
    op: types.OpTag,
};

pub const ParallelScheduleTrace = struct {
    mode: types.ParallelMode = .sequential,
    task_count: u8 = 0,
    tasks: [max_schedule_tasks]ParallelTask = undefined,
};

fn opRank(op: types.OpTag) u8 {
    return switch (op) {
        .none => 0,
        .where_filter => 1,
        .having_filter => 2,
        .group_op => 3,
        .limit_op => 4,
        .offset_op => 5,
        .insert_op => 6,
        .update_op => 7,
        .delete_op => 8,
        .sort_op => 9,
        .inspect_op => 10,
    };
}

fn lessThan(_: void, a: ParallelTask, b: ParallelTask) bool {
    if (a.relation_id != b.relation_id) return a.relation_id < b.relation_id;
    if (opRank(a.op) != opRank(b.op)) return opRank(a.op) < opRank(b.op);
    return @intFromEnum(a.op) < @intFromEnum(b.op);
}

pub fn buildScheduleTrace(
    snapshot: *const types.PlannerInputSnapshot,
    decisions: *const types.PhysicalDecisionSet,
) types.PlannerError!ParallelScheduleTrace {
    try snapshot.validate();
    var trace = ParallelScheduleTrace{
        .mode = decisions.parallel_mode,
    };

    if (decisions.parallel_mode == .sequential) {
        return trace;
    }

    for (snapshot.relation_ids_sorted) |relation_id| {
        if (relation_id == 0) break;
        for (snapshot.operator_sequence) |op| {
            if (op == .none) continue;
            if (trace.task_count >= max_schedule_tasks) break;
            trace.tasks[trace.task_count] = .{
                .relation_id = relation_id,
                .op = op,
            };
            trace.task_count += 1;
        }
    }

    std.sort.pdq(ParallelTask, trace.tasks[0..trace.task_count], {}, lessThan);
    return trace;
}

test "parallel schedule trace deterministic for identical snapshot + decisions" {
    const snapshot: types.PlannerInputSnapshot = .{
        .query_shape_fingerprint = 1,
        .catalog_snapshot_id = 2,
        .runtime_counters_snapshot_id = 3,
        .capacity_profile_id = 4,
        .work_memory_bytes_per_slot = 1024,
        .aggregate_groups_cap = 32,
        .join_build_budget_bytes = 2048,
        .average_row_width_bytes = 64,
        .max_query_slots = 8,
        .feature_gate_mask = types.feature_gate_parallel_policy,
        .operator_sequence = blk: {
            var seq = [_]types.OpTag{.none} ** types.max_operator_sequence;
            seq[0] = .where_filter;
            seq[1] = .group_op;
            seq[2] = .sort_op;
            break :blk seq;
        },
        .relation_ids_sorted = blk: {
            var rel = [_]u32{0} ** types.max_relations;
            rel[0] = 2;
            rel[1] = 9;
            break :blk rel;
        },
    };
    const decisions: types.PhysicalDecisionSet = .{
        .parallel_mode = .enabled,
    };
    const a = try buildScheduleTrace(&snapshot, &decisions);
    const b = try buildScheduleTrace(&snapshot, &decisions);
    try std.testing.expectEqual(a.mode, b.mode);
    try std.testing.expectEqual(a.task_count, b.task_count);
    try std.testing.expectEqualSlices(
        ParallelTask,
        a.tasks[0..a.task_count],
        b.tasks[0..b.task_count],
    );
}
