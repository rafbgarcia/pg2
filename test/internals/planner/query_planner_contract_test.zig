//! Internal contracts for WF07 query planner foundations.
const std = @import("std");
const pg2 = @import("pg2");

const planner_mod = pg2.planner.planner;
const fingerprint_mod = pg2.planner.fingerprint;
const types = pg2.planner.types;

test "identical input fingerprint yields identical decision fingerprint" {
    const snapshot: types.PlannerInputSnapshot = .{
        .query_shape_fingerprint = 0xDEADBEEF,
        .catalog_snapshot_id = 100,
        .runtime_counters_snapshot_id = 101,
        .capacity_profile_id = 102,
        .relation_ids_sorted = blk: {
            var ids = [_]u32{0} ** types.max_relations;
            ids[0] = 7;
            ids[1] = 11;
            break :blk ids;
        },
    };

    const input_a = try fingerprint_mod.snapshotFingerprint(&snapshot);
    const input_b = try fingerprint_mod.snapshotFingerprint(&snapshot);
    try std.testing.expectEqual(input_a, input_b);

    const decisions_a = try planner_mod.planInitial(&snapshot);
    const decisions_b = try planner_mod.planInitial(&snapshot);
    const decision_hash_a = fingerprint_mod.decisionFingerprint(&decisions_a);
    const decision_hash_b = fingerprint_mod.decisionFingerprint(&decisions_b);
    try std.testing.expectEqual(decision_hash_a, decision_hash_b);
}

test "missing required snapshot fields fail closed with deterministic codes" {
    const missing_query_shape: types.PlannerInputSnapshot = .{
        .catalog_snapshot_id = 1,
        .runtime_counters_snapshot_id = 2,
        .capacity_profile_id = 3,
    };
    try std.testing.expectError(
        error.MissingQueryShapeFingerprint,
        planner_mod.planInitial(&missing_query_shape),
    );

    const missing_catalog: types.PlannerInputSnapshot = .{
        .query_shape_fingerprint = 1,
        .runtime_counters_snapshot_id = 2,
        .capacity_profile_id = 3,
    };
    try std.testing.expectError(
        error.MissingCatalogSnapshotId,
        planner_mod.planInitial(&missing_catalog),
    );
}
