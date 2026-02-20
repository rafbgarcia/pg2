//! Executor capacity contracts for bounded operator behavior.
//!
//! Responsibilities in this file:
//! - Defines explicit max limits for pipeline, sort, aggregate, and join paths.
//! - Provides default capacity bundles for execution planning/runtime.
//! - Encodes compile-time assertions that keep limits internally consistent.
const std = @import("std");
const scan_mod = @import("scan.zig");

/// Capacity contracts for executor runtime/operator intermediates.
///
/// These limits are intentionally explicit and conservative so new
/// operator work (sort/aggregate/join) inherits bounded defaults.
pub const max_pipeline_operators: usize = 32;

// Sort operator contracts (for upcoming implementation).
pub const max_sort_rows: usize = scan_mod.max_result_rows;
pub const max_sort_keys: usize = 8;
pub const max_sort_scratch_bytes: usize = 256 * 1024;

// Aggregate operator contracts (for upcoming implementation).
pub const max_aggregate_groups: usize = 4096;
pub const max_group_keys: usize = 8;
pub const max_group_aggregate_exprs: usize = 4;
pub const max_aggregate_state_bytes: usize = 256 * 1024;

// Join operator contracts.
pub const max_join_build_rows: usize = scan_mod.max_result_rows;
pub const max_join_output_rows: usize = scan_mod.max_result_rows;
pub const max_join_state_bytes: usize = 512 * 1024;

pub const OperatorCapacities = struct {
    sort_rows: usize,
    sort_keys: usize,
    sort_scratch_bytes: usize,
    aggregate_groups: usize,
    group_keys: usize,
    group_aggregate_exprs: usize,
    aggregate_state_bytes: usize,
    join_build_rows: usize,
    join_output_rows: usize,
    join_state_bytes: usize,

    pub fn defaults() OperatorCapacities {
        return .{
            .sort_rows = max_sort_rows,
            .sort_keys = max_sort_keys,
            .sort_scratch_bytes = max_sort_scratch_bytes,
            .aggregate_groups = max_aggregate_groups,
            .group_keys = max_group_keys,
            .group_aggregate_exprs = max_group_aggregate_exprs,
            .aggregate_state_bytes = max_aggregate_state_bytes,
            .join_build_rows = max_join_build_rows,
            .join_output_rows = max_join_output_rows,
            .join_state_bytes = max_join_state_bytes,
        };
    }
};

comptime {
    std.debug.assert(max_pipeline_operators > 0);
    std.debug.assert(max_sort_rows > 0);
    std.debug.assert(max_sort_rows <= scan_mod.max_result_rows);
    std.debug.assert(max_sort_keys > 0);
    std.debug.assert(max_aggregate_groups > 0);
    std.debug.assert(max_group_keys > 0);
    std.debug.assert(max_group_aggregate_exprs > 0);
    std.debug.assert(max_join_build_rows > 0);
    std.debug.assert(max_join_output_rows > 0);
}

test "default capacity contracts remain bounded by scan result ceiling" {
    const c = OperatorCapacities.defaults();
    try std.testing.expect(c.sort_rows <= scan_mod.max_result_rows);
    try std.testing.expect(c.join_build_rows <= scan_mod.max_result_rows);
    try std.testing.expect(c.join_output_rows <= scan_mod.max_result_rows);
    try std.testing.expect(c.sort_keys >= 1);
    try std.testing.expect(c.aggregate_groups >= 1);
    try std.testing.expect(c.group_keys >= 1);
    try std.testing.expect(c.group_aggregate_exprs >= 1);
}
