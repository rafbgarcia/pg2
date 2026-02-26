//! Deterministic planner snapshot/decision fingerprints.
const std = @import("std");
const types = @import("types.zig");

pub fn snapshotFingerprint(snapshot: *const types.PlannerInputSnapshot) !u64 {
    try snapshot.validate();
    const bytes = std.mem.asBytes(snapshot);
    var hasher = std.hash.Wyhash.init(0);
    hasher.update(bytes);
    return hasher.final();
}

pub fn decisionFingerprint(decision: *const types.PhysicalDecisionSet) u64 {
    const bytes = std.mem.asBytes(decision);
    var hasher = std.hash.Wyhash.init(0);
    hasher.update(bytes);
    return hasher.final();
}

test "identical snapshots produce identical fingerprints" {
    const snapshot: types.PlannerInputSnapshot = .{
        .query_shape_fingerprint = 0xABCD,
        .catalog_snapshot_id = 12,
        .runtime_counters_snapshot_id = 13,
        .capacity_profile_id = 14,
        .work_memory_bytes_per_slot = 1024,
        .aggregate_groups_cap = 256,
        .join_build_budget_bytes = 4096,
        .average_row_width_bytes = 64,
        .max_query_slots = 8,
    };

    const a = try snapshotFingerprint(&snapshot);
    const b = try snapshotFingerprint(&snapshot);
    try std.testing.expectEqual(a, b);
}
