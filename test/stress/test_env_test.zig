//! Stress harness aliases the shared test harness so both suites execute
//! identical setup and request semantics.
const shared = @import("test_shared");

pub const TestExecutor = shared.TestExecutor;
pub const FeatureEnv = shared.FeatureEnv;
