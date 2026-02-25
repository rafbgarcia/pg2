//! Internal-suite harness boundary.
//!
//! Responsibilities in this file:
//! - Provides internal tests a stable harness import path.
//! - Prevents direct coupling from internals to feature suite wrappers.
//! - Keeps room for stricter internal-only setup over time.
const feature = @import("test_shared");

pub const InternalEnv = feature.FeatureEnv;
pub const InternalExecutor = feature.TestExecutor;

// Transitional aliases for existing internal tests; prefer InternalEnv/InternalExecutor
// in newly-written tests.
pub const FeatureEnv = feature.FeatureEnv;
pub const TestExecutor = feature.TestExecutor;
