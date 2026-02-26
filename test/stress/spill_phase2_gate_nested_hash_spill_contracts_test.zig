//! Phase 2 gate integration tests for nested hash spill composition/failure.
const std = @import("std");
const spill = @import("spill_phase2_gate_helpers.zig");

const FeatureEnv = spill.FeatureEnv;
const spill_boundary_row_count = spill.spill_boundary_row_count;
const insertPostsForSingleUser = spill.insertPostsForSingleUser;
const runMixedRootAndNestedHashSpillScenario = spill.runMixedRootAndNestedHashSpillScenario;
const runRootSortSpillAndNestedHashSpillScenario = spill.runRootSortSpillAndNestedHashSpillScenario;

test "mixed root spill and nested hash spill preserves per-parent results under tight temp budgets" {
    var result_buf: [512 * 1024]u8 = undefined;
    const result = try runMixedRootAndNestedHashSpillScenario(&result_buf);

    try std.testing.expect(std.mem.startsWith(u8, result, "OK returned_rows=4097 "));
    try std.testing.expect(std.mem.indexOf(u8, result, "{id:i64,posts:[{id:i64}]}") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\n1,[[1]]\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\n4097,[[4097]]\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "spill_triggered=true") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "join_strategy=hash_spill") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "nested_join_hash_spill=1") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "nested_join_hash_in_memory=0") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "nested_join_breakdown=nested_loop:0,hash_in_memory:0,hash_spill:1") != null);
}

test "mixed root spill and nested hash spill is deterministic under tight temp budgets" {
    var run1_buf: [512 * 1024]u8 = undefined;
    var run2_buf: [512 * 1024]u8 = undefined;
    const run1 = try runMixedRootAndNestedHashSpillScenario(&run1_buf);
    const run2 = try runMixedRootAndNestedHashSpillScenario(&run2_buf);
    try std.testing.expectEqualStrings(run1, run2);
    try std.testing.expect(std.mem.indexOf(u8, run1, "planner_policy_version=1") != null);
    try std.testing.expect(std.mem.indexOf(u8, run1, "planner_snapshot_fingerprint=") != null);
    try std.testing.expect(std.mem.indexOf(u8, run1, "planner_decision_fingerprint=") != null);
    try std.testing.expect(std.mem.indexOf(u8, run1, "INSPECT checkpoint name=pre_scan ") != null);
    try std.testing.expect(std.mem.indexOf(u8, run1, "INSPECT checkpoint name=post_filter ") != null);
    try std.testing.expect(std.mem.indexOf(u8, run1, "INSPECT checkpoint name=pre_join ") != null);
}

test "root sort spill and nested hash spill compose correctly under tight budget" {
    var result_buf: [128 * 1024]u8 = undefined;
    const result = try runRootSortSpillAndNestedHashSpillScenario(&result_buf);

    try std.testing.expect(std.mem.startsWith(u8, result, "OK returned_rows=3 "));
    try std.testing.expect(std.mem.indexOf(u8, result, "{id:i64,posts:[{id:i64}]}") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\n4097,[[4097]]\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\n4096,[[4096]]\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\n4095,[[4095]]\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "spill_triggered=true") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "join_strategy=hash_spill") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "sort_strategy=external_merge") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "nested_join_breakdown=nested_loop:0,hash_in_memory:0,hash_spill:1") != null);
}

test "nested hash spill fails closed when temp page budget is exhausted" {
    var env: FeatureEnv = undefined;
    try env.initWithConfig(.{
        .max_query_slots = 1,
        .work_memory_bytes_per_slot = 256,
        .temp_pages_per_query_slot = 1,
    });
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  reference(posts, id, Post.user_id, withoutReferentialIntegrity)
        \\}
        \\Post {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(user_id, i64, notNull)
        \\}
    );

    _ = try executor.run("User |> insert(id = 1) {}");

    try insertPostsForSingleUser(executor, spill_boundary_row_count, false);

    const result = try executor.run(
        "User |> where(id == 1) |> inspect { id posts |> limit(1) { id } }",
    );
    try std.testing.expect(std.mem.startsWith(u8, result, "ERR query: "));
    try std.testing.expect(std.mem.indexOf(
        u8,
        result,
        "nested relation hash spill temp page budget exhausted",
    ) != null);
}
