meta:
  id: 07_referential_set_null
  gate: Gate 4 (referential integrity)
  intent: set-null action nulls child foreign key on parent key change/delete.

schema:
  User {
    field(id, bigint, notNull, primaryKey)
    field(name, string, notNull)
  }

  Post {
    field(id, bigint, notNull, primaryKey)
    field(user_id, bigint, nullable)
    field(title, string, notNull)

    reference(author, user_id, User.id, withReferentialIntegrity(onDeleteSetNull, onUpdateSetNull))
  }

steps:
  - request: User |> insert(id = 1, name = "Alice")
    expect_exact: |
      OK rows=0
  - request: Post |> insert(id = 10, user_id = 1, title = "a")
    expect_exact: |
      OK rows=0

  - request: User |> where(id = 1) |> delete
    expect_exact: |
      OK rows=0

  - request: Post |> where(id = 10)
    expect_exact: |
      OK rows=1
      10,null,a
