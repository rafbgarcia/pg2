meta:
  id: 09_error_classification
  gate: Gate 7 (ops/observability floor)
  intent: Session boundary and query errors are explicit and machine-classifiable.

schema:
  User {
    field(id, bigint, notNull, primaryKey)
    field(name, string, nullable)
  }

steps:
  - request: User |> where(
    expect_contains: "ERR parse:"

  - request: UnknownModel
    expect_contains: "ERR query:"

  - request: User |> insert(id = 1, name = "Alice")
    expect_exact: |
      OK rows=0
