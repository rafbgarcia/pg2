meta:
  id: 03_filter_sort_limit_offset
  gate: Gate 3 (CRUD/query surface)
  intent: Read operators apply in expected order and return deterministic rows.

schema:
  User {
    field(id, bigint, notNull, primaryKey)
    field(name, string, notNull)
    field(active, boolean, notNull)
  }

steps:
  - request: User |> insert(id = 1, name = "Charlie", active = true)
    expect_exact: |
      OK rows=0
  - request: User |> insert(id = 2, name = "Alice", active = true)
    expect_exact: |
      OK rows=0
  - request: User |> insert(id = 3, name = "Bob", active = false)
    expect_exact: |
      OK rows=0

  - request: User |> where(active = true) |> sort(name asc)
    expect_exact: |
      OK rows=2
      2,Alice,true
      1,Charlie,true

  - request: User |> sort(name asc) |> offset(1) |> limit(1)
    expect_exact: |
      OK rows=1
      3,Bob,false
