//! Server session-path E2E test suite entrypoint.
//!
//! Responsibilities in this file:
//! - Aggregates all server E2E spec modules for test discovery.
//! - Defines the canonical milestone-focused E2E scope under one import root.
comptime {
    _ = @import("constraints/duplicate_key.zig");
    _ = @import("constraints/foreign_key_violation.zig");
    _ = @import("constraints/not_null.zig");
    _ = @import("constraints/type_mismatch.zig");
    _ = @import("constraints/unknown_field.zig");
    _ = @import("delete.zig");
    _ = @import("insert.zig");
    _ = @import("overflow_reclaim_crash_matrix.zig");
    _ = @import("overflow_reclaim_drain_policy.zig");
    _ = @import("overflow_replay_tx_markers.zig");
    _ = @import("string_overflow.zig");
    _ = @import("select.zig");
    _ = @import("update.zig");
}
