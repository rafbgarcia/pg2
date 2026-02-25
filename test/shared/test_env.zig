//! Backward-compatible module path for legacy imports.
const harness = @import("../harness/feature_env.zig");

pub const insert = harness.insert;
pub const TestExecutor = harness.TestExecutor;
pub const FeatureEnv = harness.FeatureEnv;
