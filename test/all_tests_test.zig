//! Unified test root for unit, feature, and internals suites.
//!
//! Responsibilities in this file:
//! - Imports pg2 module graph so inline unit tests are discovered.
//! - Imports feature-oriented session-path scenarios under test/features.
//! - Imports internal durability and maintenance scenarios under test/internals.
const std = @import("std");
const pg2 = @import("pg2");

comptime {
    _ = pg2;
    _ = @import("test_discovery_guard_test.zig");
    _ = @import("features/features_specs_test.zig");
    _ = @import("internals/internals_specs_test.zig");
}

test "all tests module compiles" {
    try std.testing.expect(true);
}
