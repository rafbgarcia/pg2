//! Feature coverage for final-statement response shape.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature final expression statement returns composite payload as final response" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    const result = try executor.run(
        \\let total = 2
        \\{ total: total, ok: true, labels: ["a", "b"] }
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n{\"total\":2,\"ok\":true,\"labels\":[\"a\",\"b\"]}\n",
        result,
    );
}
