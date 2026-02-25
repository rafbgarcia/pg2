//! Feature coverage for multi-row insert constraints/default semantics.
const std = @import("std");
const insert = @import("insert_helpers.zig");

const FeatureEnv = insert.feature.FeatureEnv;
const buildBulkUserNullableEmailInsertRequest = insert.buildBulkUserNullableEmailInsertRequest;

test "feature multi-row insert rejects PK duplicate against existing row" {
    var env: FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\}
    );

    var result = try executor.run(
        "User |> insert(id = 1, name = \"existing\") {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run(
        "User |> insert((id = 1, name = \"dup\"), (id = 2, name = \"new\")) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: message=\"insert failed\" phase=execution code=DuplicateKey path=query line=1 col=1\n",
        result,
    );

    result = try executor.run("User { id name }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,existing\n",
        result,
    );
}

test "feature multi-row insert enforces FK constraints for valid and invalid batches" {
    var env: FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\}
        \\
        \\Post {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(user_id, i64, notNull)
        \\  reference(author, user_id, User.id, withReferentialIntegrity(onDeleteRestrict, onUpdateCascade))
        \\}
    );

    var result = try executor.run("User |> insert((id = 1), (id = 2)) {}");
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=2 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run(
        "Post |> insert((id = 10, user_id = 1), (id = 11, user_id = 2)) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=2 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run(
        "Post |> insert((id = 12, user_id = 1), (id = 13, user_id = 999)) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: message=\"insert failed\" phase=execution code=ReferentialIntegrityViolation path=query line=1 col=1\n",
        result,
    );

    result = try executor.run("Post { id user_id }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n10,1\n11,2\n",
        result,
    );
}

test "feature multi-row insert applies defaults for omitted columns" {
    var env: FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(tier, string, notNull, default, "free")
        \\  field(marketing_opt_in, bool, notNull, default, false)
        \\}
    );

    const result = try executor.run(
        "User |> insert((id = 1), (id = 2, tier = \"pro\"), (id = 3)) { id tier marketing_opt_in }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=3 inserted_rows=3 updated_rows=0 deleted_rows=0\n1,free,false\n2,pro,false\n3,free,false\n",
        result,
    );
}

test "feature multi-row insert non-PK unique in-batch duplicate fails closed" {
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

    var result = try executor.run(
        "User |> insert((id = 1, email = \"dup@test.com\", name = \"A\"), (id = 2, email = \"dup@test.com\", name = \"B\")) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: message=\"insert failed\" phase=execution code=DuplicateKey path=query line=1 col=1\n",
        result,
    );

    result = try executor.run("User { id email }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=0 deleted_rows=0\n",
        result,
    );
}

test "feature multi-row insert nullable unique skips null keys and still rejects duplicates" {
    var env: FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(email, string, nullable)
        \\  field(name, string, notNull)
        \\  index(idx_email, [email], unique)
        \\}
    );

    var result = try executor.run(
        "User |> insert((id = 1, email = null, name = \"A\"), (id = 2, email = null, name = \"B\")) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=2 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run(
        "User |> insert((id = 3, email = \"dup@test.com\", name = \"C\"), (id = 4, email = \"dup@test.com\", name = \"D\")) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: message=\"insert failed\" phase=execution code=DuplicateKey path=query line=1 col=1\n",
        result,
    );

    result = try executor.run("User { id email name }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,null,A\n2,null,B\n",
        result,
    );
}

test "feature multi-row insert token-bound mixed nullable unique keys stay correct" {
    var env: FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(email, string, nullable)
        \\  field(name, string, notNull)
        \\  index(idx_email, [email], unique)
        \\}
    );

    const row_count: usize = 170;
    var insert_buf: [128 * 1024]u8 = undefined;
    const insert_req = try buildBulkUserNullableEmailInsertRequest(
        insert_buf[0..],
        1,
        row_count,
        false,
    );
    var result = try executor.run(insert_req);
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=170 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run("User |> where(id == 1) { id email }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,null\n",
        result,
    );

    result = try executor.run("User |> where(id == 2) { id email }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n2,user-2@test.com\n",
        result,
    );

    const dup_req = try buildBulkUserNullableEmailInsertRequest(
        insert_buf[0..],
        10001,
        row_count,
        true,
    );
    result = try executor.run(dup_req);
    try std.testing.expectEqualStrings(
        "ERR query: message=\"insert failed\" phase=execution code=DuplicateKey path=query line=1 col=1\n",
        result,
    );

    result = try executor.run("User |> where(id == 10001) { id }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=0 deleted_rows=0\n",
        result,
    );
}
