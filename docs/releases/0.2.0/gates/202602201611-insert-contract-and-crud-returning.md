# Quality Gate Artifact: 202602201611-insert-contract-and-crud-returning

- Artifact ID: `202602201611-insert-contract-and-crud-returning`
- Commit: `9335780` (required real committed SHA)
- Release: `0.2.0`
- Title: `Insert response contract and mandatory CRUD returning blocks`
- Scope: `Lock server session response contract for CRUD and enforce fail-closed duplicate-key insert behavior.`

## PR Checklist

- What invariant was added or changed?
  - All top-level CRUD pipeline statements must include an explicit returning block, including `{}` when no rows are requested (`src/server/session.zig`).
  - Successful responses now expose explicit non-overloaded counters: `returned_rows`, `inserted_rows`, `updated_rows`, `deleted_rows` (`src/server/session.zig`).
  - Insert now fails closed on duplicate primary/unique keys with deterministic `DuplicateKey` mutation error mapping (`src/executor/mutation.zig`, `src/tiger/error_taxonomy.zig`).

- What is the crash-consistency contract for the modified path?
  - Existing write durability and overflow reclaim crash contract is unchanged; this increment changes request validation and response encoding at the session boundary plus pre-insert uniqueness validation before heap/WAL mutation (`src/server/session.zig`, `src/executor/mutation.zig`).

- Which error classes can now be returned?
  - `fatal` now explicitly includes `DuplicateKey` for insert conflicts (`src/tiger/error_taxonomy.zig`).
  - Session boundary now returns deterministic query errors when CRUD returning block is missing (`src/server/session.zig`).

- Does this change modify any persistent format or protocol?
  - Persistent format: `none`
  - Protocol: `yes` (success header changed from `OK rows=<n>` to `OK returned_rows=<n> inserted_rows=<n> updated_rows=<n> deleted_rows=<n>`; CRUD requests without returning block now fail closed).

- Which deterministic crash/fault tests were added?
  - Added/updated server-path deterministic E2E coverage for new response contract and duplicate-key behavior:
    - `src/server/e2e/insert.zig`
    - `src/server/e2e/select.zig`
    - `src/server/e2e/update.zig`
    - `src/server/e2e/delete.zig`
    - `src/server/e2e/string_overflow.zig`
    - `src/server/e2e/overflow_reclaim_drain_policy.zig`
    - `src/server/e2e/overflow_replay_tx_markers.zig`
    - `src/server/e2e/overflow_reclaim_crash_matrix.zig`
  - Added session boundary test for fail-closed missing-returning behavior:
    - `src/server/session.zig`

- Which performance baseline or threshold was updated (if any)?
  - `none` (no planner/storage threshold change in this increment).
