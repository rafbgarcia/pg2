meta:
  id: 04_group_aggregates
  gate: Gate 3 (query surface)
  intent: Grouping and aggregate expressions are correct and stable.

schema:
  Order {
    field(id, bigint, notNull, primaryKey)
    field(user_id, bigint, notNull)
    field(amount, bigint, notNull)
  }

steps:
  - request: Order |> insert(id = 1, user_id = 10, amount = 50)
    expect_exact: |
      OK rows=0
  - request: Order |> insert(id = 2, user_id = 10, amount = 25)
    expect_exact: |
      OK rows=0
  - request: Order |> insert(id = 3, user_id = 11, amount = 100)
    expect_exact: |
      OK rows=0

  - request: Order |> group(user_id) |> sort(user_id asc) { user_id order_count: count(*) total: sum(amount) }
    expect_exact: |
      OK rows=2
      10,2,75
      11,1,100
