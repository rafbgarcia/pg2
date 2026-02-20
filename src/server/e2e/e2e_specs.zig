//! Server session-path E2E test suite entrypoint.
//!
//! Responsibilities in this file:
//! - Aggregates all server E2E spec modules for test discovery.
//! - Defines the canonical milestone-focused E2E scope under one import root.
comptime {
    _ = @import("delete.zig");
    _ = @import("insert.zig");
    _ = @import("string_overflow.zig");
    _ = @import("select.zig");
    _ = @import("update.zig");
}
