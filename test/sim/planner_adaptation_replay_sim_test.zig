//! Deterministic replay coverage for planner checkpoint adaptation traces.
const std = @import("std");
const pg2 = @import("pg2");

const planner_types = pg2.planner.types;
const planner_logic = pg2.planner.planner;
const planner_adaptation = pg2.planner.adaptation;
const planner_fingerprint = pg2.planner.fingerprint;

const ReplayTrace = struct {
    snapshot_fingerprint: u64,
    decision_fingerprints: [4]u64,
};

fn runReplayTrace(seed: u64) !ReplayTrace {
    var snapshot: planner_types.PlannerInputSnapshot = .{
        .seed = seed,
        .query_shape_fingerprint = 0xA11CE,
        .catalog_snapshot_id = 41,
        .runtime_counters_snapshot_id = 77,
        .capacity_profile_id = 123,
        .work_memory_bytes_per_slot = 1024,
        .aggregate_groups_cap = 32,
        .join_build_budget_bytes = 2048,
        .average_row_width_bytes = 64,
        .max_query_slots = 8,
        .operator_sequence = blk: {
            var seq = [_]planner_types.OpTag{.none} ** planner_types.max_operator_sequence;
            seq[0] = .where_filter;
            seq[1] = .group_op;
            seq[2] = .sort_op;
            break :blk seq;
        },
        .relation_ids_sorted = blk: {
            var relations = [_]u32{0} ** planner_types.max_relations;
            relations[0] = 2;
            relations[1] = 9;
            break :blk relations;
        },
    };

    var decisions = try planner_logic.planInitial(&snapshot);
    var trace = ReplayTrace{
        .snapshot_fingerprint = try planner_fingerprint.snapshotFingerprint(&snapshot),
        .decision_fingerprints = [_]u64{0} ** 4,
    };

    const checkpoints = [_]struct {
        name: planner_types.Checkpoint,
        counters: planner_types.CheckpointCounters,
    }{
        .{
            .name = .pre_scan,
            .counters = .{},
        },
        .{
            .name = .post_filter,
            .counters = .{
                .rows_seen = 64,
                .rows_after_filter = 48,
                .bytes_accumulated = 900,
                .spill_pages_used = 2,
                .group_count_estimate = 48,
                .join_build_rows = 4,
                .join_probe_rows = 48,
            },
        },
        .{
            .name = .pre_join,
            .counters = .{
                .rows_seen = 64,
                .rows_after_filter = 48,
                .bytes_accumulated = 1200,
                .spill_pages_used = 3,
                .group_count_estimate = 64,
                .join_build_rows = 40,
                .join_probe_rows = 48,
            },
        },
        .{
            .name = .post_group,
            .counters = .{
                .rows_seen = 64,
                .rows_after_filter = 12,
                .bytes_accumulated = 1200,
                .spill_pages_used = 4,
                .group_count_estimate = 40,
                .join_build_rows = 40,
                .join_probe_rows = 12,
            },
        },
    };

    for (checkpoints, 0..) |entry, i| {
        _ = try planner_adaptation.adaptAtCheckpoint(
            &snapshot,
            entry.name,
            &entry.counters,
            &decisions,
        );
        trace.decision_fingerprints[i] = planner_fingerprint.decisionFingerprint(&decisions);
    }
    return trace;
}

test "sim planner adaptation checkpoint traces are deterministic under replay seed" {
    const run1 = try runReplayTrace(7);
    const run2 = try runReplayTrace(7);
    try std.testing.expectEqual(run1.snapshot_fingerprint, run2.snapshot_fingerprint);
    try std.testing.expectEqualSlices(
        u64,
        run1.decision_fingerprints[0..],
        run2.decision_fingerprints[0..],
    );
}

test "sim planner snapshot fingerprint changes with different seed while decisions stay stable" {
    const run_a = try runReplayTrace(7);
    const run_b = try runReplayTrace(8);
    try std.testing.expect(run_a.snapshot_fingerprint != run_b.snapshot_fingerprint);
    try std.testing.expectEqualSlices(
        u64,
        run_a.decision_fingerprints[0..],
        run_b.decision_fingerprints[0..],
    );
}
