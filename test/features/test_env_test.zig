//! Feature harness aliases the shared test harness so all suites keep the same
//! runtime setup and request semantics.
const shared = @import("test_shared");

pub const TestExecutor = shared.TestExecutor;
pub const FeatureEnv = shared.FeatureEnv;
