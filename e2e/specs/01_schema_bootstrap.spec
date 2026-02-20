meta:
  id: 01_schema_bootstrap
  gate: Gate 2 (Schema lifecycle)
  intent: Schema apply succeeds, and initial read/write path is live.

schema:
  User {
    field(id, bigint, notNull, primaryKey)
    field(name, string, nullable)
    field(active, boolean, notNull)
    index(idx_user_id, [id], unique)
  }

steps:
  - request: User
    expect_exact: |
      OK rows=0

  - request: User |> insert(id = 1, name = "Alice", active = true)
    expect_exact: |
      OK rows=0

  - request: User
    expect_exact: |
      OK rows=1
      1,Alice,true
