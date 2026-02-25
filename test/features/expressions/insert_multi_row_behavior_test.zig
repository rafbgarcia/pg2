//! Feature coverage for multi-row insert behavior through server session path.
const std = @import("std");
const insert = @import("insert_helpers.zig");

const FeatureEnv = insert.feature.FeatureEnv;
const buildBulkUserInsertRequest = insert.buildBulkUserInsertRequest;
const buildBulkUserWithEmailInsertRequest = insert.buildBulkUserWithEmailInsertRequest;

test "feature multi-row insert returns explicit insert count via session path" {
    var env: FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\  field(active, bool, notNull)
        \\}
    );

    const result = try executor.run(
        "User |> insert((id = 1, name = \"Alice\", active = true), (id = 2, name = \"Bob\", active = false), (id = 3, name = \"Carol\", active = true)) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=3 updated_rows=0 deleted_rows=0\n",
        result,
    );
}

test "feature multi-row insert returning yields all inserted rows" {
    var env: FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\  field(active, bool, notNull)
        \\}
    );

    const result = try executor.run(
        "User |> insert((id = 1, name = \"Alice\", active = true), (id = 2, name = \"Bob\", active = false), (id = 3, name = \"Carol\", active = true)) { id name }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=3 inserted_rows=3 updated_rows=0 deleted_rows=0\n1,Alice\n2,Bob\n3,Carol\n",
        result,
    );
}

test "feature multi-row insert duplicate key fails closed and inserts nothing" {
    var env: FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\  field(active, bool, notNull)
        \\}
    );

    var result = try executor.run(
        "User |> insert((id = 1, name = \"Alice\", active = true), (id = 1, name = \"Bob\", active = false)) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: message=\"insert failed\" phase=execution code=DuplicateKey path=query line=1 col=1\n",
        result,
    );

    result = try executor.run("User { id name }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=0 deleted_rows=0\n",
        result,
    );
}

test "feature multi-row insert large batch keeps PK index correctness" {
    var env: FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\  field(active, bool, notNull)
        \\}
    );

    var insert_buf: [8 * 1024]u8 = undefined;
    const insert_req = try buildBulkUserInsertRequest(insert_buf[0..], 1, 100);
    var result = try executor.run(insert_req);
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=100 updated_rows=0 deleted_rows=0\n",
        result,
    );

    var where_buf: [128]u8 = undefined;
    var expected_buf: [128]u8 = undefined;
    var id: usize = 1;
    while (id <= 100) : (id += 1) {
        const query = try std.fmt.bufPrint(
            where_buf[0..],
            "User |> where(id == {d}) {{ id name }}",
            .{id},
        );
        result = try executor.run(query);
        const expected = try std.fmt.bufPrint(
            expected_buf[0..],
            "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n{d},user-{d}\n",
            .{ id, id },
        );
        try std.testing.expectEqualStrings(expected, result);
    }
}

test "feature multi-row insert large batch maintains non-PK unique index entries" {
    var env: FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(email, string, notNull)
        \\  field(name, string, notNull)
        \\  index(idx_email, [email], unique)
        \\}
    );

    var insert_buf: [12 * 1024]u8 = undefined;
    const insert_req = try buildBulkUserWithEmailInsertRequest(insert_buf[0..], 1, 80);
    var result = try executor.run(insert_req);
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=80 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run(
        "User |> insert(id = 999, email = \"user-40@test.com\", name = \"dup\") {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: message=\"insert failed\" phase=execution code=DuplicateKey path=query line=1 col=1\n",
        result,
    );
}

test "feature multi-row insert token-bound batch preserves heap and unique index correctness" {
    var env: FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(email, string, notNull)
        \\  field(name, string, notNull)
        \\  index(idx_email, [email], unique)
        \\}
    );

    var insert_buf: [128 * 1024]u8 = undefined;
    const insert_req = try buildBulkUserWithEmailInsertRequest(insert_buf[0..], 1, 170);
    var result = try executor.run(insert_req);
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=170 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run("User |> where(id == 1) { id email }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,user-1@test.com\n",
        result,
    );
    result = try executor.run("User |> where(id == 85) { id email }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n85,user-85@test.com\n",
        result,
    );
    result = try executor.run("User |> where(id == 170) { id email }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n170,user-170@test.com\n",
        result,
    );

    // Duplicate email proves non-PK unique index entries were maintained.
    result = try executor.run(
        "User |> insert(id = 5001, email = \"user-85@test.com\", name = \"dup\") {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: message=\"insert failed\" phase=execution code=DuplicateKey path=query line=1 col=1\n",
        result,
    );
}

test "feature multi-row insert reports tokenizer cap for oversized statements" {
    var env: FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\  field(active, bool, notNull)
        \\}
    );

    var insert_buf: [96 * 1024]u8 = undefined;
    const oversized_insert = try buildBulkUserInsertRequest(insert_buf[0..], 1, 350);
    const result = try executor.run(oversized_insert);
    try std.testing.expectEqualStrings(
        "ERR tokenize: tokenizer hard-cap exhausted (hard_max_tokens=4096)\n",
        result,
    );
}
