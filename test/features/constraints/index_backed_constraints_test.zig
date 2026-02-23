//! Feature coverage for index-backed constraint enforcement (Phase 4).
//!
//! Validates that B+ tree indexes enforce unique constraints on non-PK
//! columns and that foreign-key checks use the PK index path.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "non-PK unique constraint enforced via B+ tree rejects duplicate" {
    var env: feature.FeatureEnv = undefined;
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
        "User |> insert(id = 1, email = \"alice@test.com\", name = \"Alice\") {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run(
        "User |> insert(id = 2, email = \"alice@test.com\", name = \"Bob\") {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: insert failed; class=fatal; code=DuplicateKey\n",
        result,
    );
}

test "non-PK unique constraint enforced via B+ tree accepts unique values" {
    var env: feature.FeatureEnv = undefined;
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
        "User |> insert(id = 1, email = \"alice@test.com\", name = \"Alice\") {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run(
        "User |> insert(id = 2, email = \"bob@test.com\", name = \"Bob\") {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run(
        "User |> sort(id asc) { id email name }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n" ++
            "1,alice@test.com,Alice\n" ++
            "2,bob@test.com,Bob\n",
        result,
    );
}

test "INSERT maintains all unique indexes — B+ tree find returns correct RowId" {
    var env: feature.FeatureEnv = undefined;
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
        "User |> insert(id = 1, email = \"alice@test.com\", name = \"Alice\") {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run(
        "User |> insert(id = 2, email = \"bob@test.com\", name = \"Bob\") {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    // Third insert reuses an email already present in the B+ tree index.
    // The uniqueness check must find it and reject the insert.
    result = try executor.run(
        "User |> insert(id = 3, email = \"alice@test.com\", name = \"Charlie\") {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: insert failed; class=fatal; code=DuplicateKey\n",
        result,
    );
}

test "DELETE leaves unique index entries intact, re-insert succeeds after delete" {
    var env: feature.FeatureEnv = undefined;
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
        "User |> insert(id = 1, email = \"alice@test.com\", name = \"Alice\") {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run(
        "User |> where(id == 1) |> delete {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=0 deleted_rows=1\n",
        result,
    );

    // Re-insert with the same email must succeed: the MVCC check sees the
    // deleted row as invisible, cleans up the dead B+ tree entry, and
    // allows both the uniqueness check and the index insert to pass.
    result = try executor.run(
        "User |> insert(id = 2, email = \"alice@test.com\", name = \"Alice2\") {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );
}

test "FK check uses PK index — valid FK passes" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\}
        \\Post {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(user_id, i64, notNull)
        \\  reference(author, user_id, User.id, withReferentialIntegrity(onDeleteRestrict, onUpdateCascade))
        \\}
    );

    var result = try executor.run(
        "User |> insert(id = 1) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run(
        "Post |> insert(id = 10, user_id = 1) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );
}

test "FK check uses PK index — invalid FK fails" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\}
        \\Post {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(user_id, i64, notNull)
        \\  reference(author, user_id, User.id, withReferentialIntegrity(onDeleteRestrict, onUpdateCascade))
        \\}
    );

    const result = try executor.run(
        "Post |> insert(id = 10, user_id = 999) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: insert failed; class=fatal; code=ReferentialIntegrityViolation\n",
        result,
    );
}
