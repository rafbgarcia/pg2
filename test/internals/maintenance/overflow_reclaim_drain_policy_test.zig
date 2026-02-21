//! Internal coverage for overflow reclaim drain execution policy.
//!
//! Responsibilities in this file:
//! - Verifies reclaim drain executes only on successful write commit boundaries.
//! - Validates fixed one-chain drain budget progression across committed writes.
const std = @import("std");
const pg2 = @import("pg2");
const overflow_mod = pg2.storage.overflow;
const internal = @import("../../features/test_env_test.zig");

test "internal overflow multi-chain unlink drains one committed chain per successful write commit boundary" {
    var env: internal.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\  field(bio, string, notNull)
        \\}
    );
    env.catalog.overflow_page_allocator = try overflow_mod.PageIdAllocator.initWithBounds(13_500, 16);

    var long_name_a: [1200]u8 = undefined;
    @memset(long_name_a[0..], 'n');
    var long_bio_a: [1200]u8 = undefined;
    @memset(long_bio_a[0..], 'b');
    var insert_req_buf: [2800]u8 = undefined;
    const insert_req = try std.fmt.bufPrint(
        insert_req_buf[0..],
        "User |> insert(id = 1, name = \"{s}\", bio = \"{s}\") {{}}",
        .{ long_name_a[0..], long_bio_a[0..] },
    );
    var result = try executor.run(insert_req);
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    var long_name_b: [1200]u8 = undefined;
    @memset(long_name_b[0..], 'x');
    var long_bio_b: [1200]u8 = undefined;
    @memset(long_bio_b[0..], 'y');
    var update_req_buf: [3000]u8 = undefined;
    const update_req = try std.fmt.bufPrint(
        update_req_buf[0..],
        "User |> where(id = 1) |> update(name = \"{s}\", bio = \"{s}\") {{}}",
        .{ long_name_b[0..], long_bio_b[0..] },
    );
    result = try executor.run(update_req);
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=1 deleted_rows=0\n",
        result,
    );

    result = try executor.run("User |> inspect {}");
    try std.testing.expect(
        std.mem.indexOf(
            u8,
            result,
            "INSPECT overflow reclaim_queue_depth=1 reclaim_enqueued_total=2 reclaim_dequeued_total=1 reclaim_chains_total=1 reclaim_pages_total=1 reclaim_failures_total=0\n",
        ) != null,
    );

    // Read-only requests do not advance reclaim drain.
    result = try executor.run("User |> inspect {}");
    try std.testing.expect(
        std.mem.indexOf(
            u8,
            result,
            "INSPECT overflow reclaim_queue_depth=1 reclaim_enqueued_total=2 reclaim_dequeued_total=1 reclaim_chains_total=1 reclaim_pages_total=1 reclaim_failures_total=0\n",
        ) != null,
    );

    // A subsequent write commit boundary drains one more committed chain.
    result = try executor.run("User |> insert(id = 2, name = \"n\", bio = \"b\") {}");
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run("User |> inspect {}");
    try std.testing.expect(
        std.mem.indexOf(
            u8,
            result,
            "INSPECT overflow reclaim_queue_depth=0 reclaim_enqueued_total=2 reclaim_dequeued_total=2 reclaim_chains_total=2 reclaim_pages_total=2 reclaim_failures_total=0\n",
        ) != null,
    );
}
