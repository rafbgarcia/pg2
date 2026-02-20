meta:
  id: 06_referential_cascade
  gate: Gate 4 (referential integrity)
  intent: Cascading delete/update propagates to child records.

schema:
  User {
    field(id, bigint, notNull, primaryKey)
    field(name, string, notNull)
  }

  Post {
    field(id, bigint, notNull, primaryKey)
    field(user_id, bigint, notNull)
    field(title, string, notNull)

    reference(author, user_id, User.id, withReferentialIntegrity(onDeleteCascade, onUpdateCascade))
  }

steps:
  - request: User |> insert(id = 1, name = "Alice")
    expect_exact: |
      OK rows=0
  - request: Post |> insert(id = 10, user_id = 1, title = "a")
    expect_exact: |
      OK rows=0
  - request: Post |> insert(id = 11, user_id = 1, title = "b")
    expect_exact: |
      OK rows=0

  - request: User |> where(id = 1) |> delete
    expect_exact: |
      OK rows=0

  - request: Post
    expect_exact: |
      OK rows=0
