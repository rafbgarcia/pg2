meta:
  id: 10_inspect_output
  gate: Gate 3 and Gate 7 (inspect + observability)
  intent: Inspect mode returns readable execution and pool stats.

schema:
  User {
    field(id, bigint, notNull, primaryKey)
    field(name, string, nullable)
    field(active, boolean, notNull)
  }

steps:
  - request: User |> inspect
    expect_contains: "OK rows=0"
  - request: User |> inspect
    expect_contains: "INSPECT exec rows_scanned="
  - request: User |> inspect
    expect_contains: "INSPECT pool policy="
  - request: User |> inspect
    expect_contains: "INSPECT plan source_model=User pipeline=inspect join_strategy=none join_order=none materialization=none nested_relations=0"
