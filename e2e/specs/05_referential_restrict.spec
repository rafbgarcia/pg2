meta:
  id: 05_referential_restrict
  gate: Gate 4 (referential integrity)
  intent: Restrict blocks invalid parent delete/update when child rows exist.

schema:
  User {
    field(id, bigint, notNull, primaryKey)
    field(name, string, notNull)
  }

  Post {
    field(id, bigint, notNull, primaryKey)
    field(user_id, bigint, notNull)
    field(title, string, notNull)

    reference(author, user_id, User.id, withReferentialIntegrity(onDeleteRestrict, onUpdateRestrict))
  }

steps:
  - request: User |> insert(id = 1, name = "Alice")
    expect_exact: |
      OK rows=0
  - request: Post |> insert(id = 100, user_id = 1, title = "hello")
    expect_exact: |
      OK rows=0

  - request: User |> where(id = 1) |> delete
    expect_contains: "ERR query:"

  - request: User |> where(id = 1) |> update(id = 2)
    expect_contains: "ERR query:"
