meta:
  id: 08_restart_recovery
  gate: Gate 5 (MVCC + recovery)
  intent: Data committed before crash survives restart via WAL/recovery replay.

schema:
  User {
    field(id, bigint, notNull, primaryKey)
    field(name, string, notNull)
  }

steps:
  - phase: before_restart
    request: User |> insert(id = 1, name = "Alice")
    expect_exact: |
      OK rows=0

  - phase: before_restart
    request: User |> insert(id = 2, name = "Bob")
    expect_exact: |
      OK rows=0

  - phase: restart_server
    action: crash_and_restart

  - phase: after_restart
    request: User |> sort(id asc)
    expect_exact: |
      OK rows=2
      1,Alice
      2,Bob
