meta:
  id: 02_basic_crud
  gate: Gate 3 (CRUD surface)
  intent: Insert, update, delete, and read are correct end-to-end.

schema:
  User {
    field(id, bigint, notNull, primaryKey)
    field(name, string, nullable)
    field(active, boolean, notNull)
  }

steps:
  - request: User |> insert(id = 1, name = "Alice", active = true)
    expect_exact: |
      OK rows=0

  - request: User |> where(id = 1) |> update(name = "Alicia")
    expect_exact: |
      OK rows=0

  - request: User |> where(id = 1)
    expect_exact: |
      OK rows=1
      1,Alicia,true

  - request: User |> where(id = 1) |> delete
    expect_exact: |
      OK rows=0

  - request: User
    expect_exact: |
      OK rows=0
