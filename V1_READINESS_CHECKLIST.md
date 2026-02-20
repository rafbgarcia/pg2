# pg2 V1 Readiness Checklist

This checklist defines the minimum bar for a **real** v1 release suitable for
small production projects on a single Linux primary.
Roadmap sequencing lives in `TODO.md`.

## Gate to Milestone Mapping

- Gate 4 + Gate 8 align with TODO Milestone 1.
- Gate 5 + Gate 8 align with TODO Milestone 2.
- Gate 3 + Gate 7 + Gate 8 align with TODO Milestone 3.
- Release Decision Quality Gates line aligns with TODO Milestone 4.

## Target Scope (V1)

- Linux-only server runtime.
- Single primary only (no replication/failover requirements in v1 gate).
- Explicitly versioned on-disk formats.
- Deterministic tests as the release gate for correctness.

## Readiness Gates

- [ ] Gate 1: Runtime durability path is real
  - [ ] `src/main.zig` boots with real durable `Storage` backend for server mode.
  - [ ] Startup performs WAL/recovery replay before accepting traffic.
  - [ ] Clean shutdown path flushes WAL and data pages in contract order.
  - [ ] No simulator-only storage objects on production runtime path.

- [ ] Gate 2: Schema lifecycle is end-to-end
  - [ ] Schema definitions are accepted through the server request path.
  - [ ] Schema load applies catalog + heap/index initialization correctly.
  - [ ] Invalid schema fails closed with classified errors.
  - [ ] Supported/unsupported features are explicit in user-facing docs.

- [ ] Gate 3: CRUD + query surface is stable
  - [x] Insert/update/delete/read work end-to-end via server protocol.
    - Covered in `src/server/e2e/insert.zig`, `src/server/e2e/update.zig`, `src/server/e2e/delete.zig`, `src/server/e2e/select.zig`.
    - Heap mutation foundation now includes deterministic auto-compaction-on-shortfall tests in `src/storage/heap.zig` for update growth and insert retry paths.
    - Overflow string path is now integrated end-to-end:
      - dedicated overflow page-id region allocator (`src/storage/overflow.zig`, `src/catalog/catalog.zig`),
      - row inline-vs-overflow pointer encoding (`src/storage/row.zig`),
      - mutation spill on >1024B strings (`src/executor/mutation.zig`),
      - read-path overflow materialization into bounded query string arena (`src/executor/scan.zig`),
      - deterministic overflow exhaustion and spill roundtrip tests,
      - deterministic replace/delete reclaim queue + WAL lifecycle ordering tests,
      - crash/restart WAL recovery decode coverage for spill/replace/delete lifecycle,
      - session-path overflow E2E coverage in `src/server/e2e/overflow.zig`.
    - Read-path string materialization now uses bounded per-query arena bytes (runtime-configured), eliminating page-slice lifetime hazards in scan results.
  - [ ] `where`, `sort`, `limit`, `offset`, `group` are covered by E2E tests.
  - [x] `inspect` output is deterministic and documented.
  - [ ] Query boundary errors are stable and classified.

- [ ] Gate 4: Referential integrity contract is enforced
  - [x] `withReferentialIntegrity(onDeleteX, onUpdateY)` enforced on all write paths.
  - [x] Missing RI actions are rejected.
  - [x] Unsupported actions (set default) fail closed.
  - [ ] Restrict/cascade/set-null behavior has deterministic E2E coverage.

- [x] Gate 5: MVCC + recovery correctness hardening
  - [x] Crash/restart tests cover WAL + undo interaction points.
  - [x] Rollback edge cases covered with deterministic seeds.
  - [x] Invariant docs updated for visibility and recovery ordering.
  - [x] Simulator crash matrix executes real recovery assertions.

- [ ] Gate 6: Backup/restore minimum viability
  - [ ] Base backup command works on v1 runtime path.
  - [ ] WAL archive/replay supports point-in-time recovery within retention.
  - [ ] Restore drills are scripted and run in CI/nightly.

- [ ] Gate 7: Ops and observability floor
  - [ ] Health endpoint/command reports startup state and recovery completion.
  - [ ] WAL lag/replay and recovery metrics are visible.
  - [ ] Error class distribution is queryable/logged.
  - [ ] Clear runbook for backup, restore, and recovery validation.

- [ ] Gate 8: V1 E2E specification suite passes
  - [ ] `e2e/specs/01_schema_bootstrap.spec`
  - [ ] `e2e/specs/02_basic_crud.spec` (CRUD flow now covered via operation tests in `src/server/e2e/`, including row-growth update regression)
  - [ ] `e2e/specs/03_filter_sort_limit_offset.spec` (partial query coverage in `src/server/e2e_specs.zig`; offset/limit assertion still pending)
  - [ ] `e2e/specs/04_group_aggregates.spec`
  - [ ] `e2e/specs/05_referential_restrict.spec`
  - [ ] `e2e/specs/06_referential_cascade.spec`
  - [ ] `e2e/specs/07_referential_set_null.spec`
  - [ ] `e2e/specs/08_restart_recovery.spec`
  - [ ] `e2e/specs/09_error_classification.spec`
  - [ ] `e2e/specs/10_inspect_output.spec`

## Release Decision

- [ ] All gates above checked.
- [ ] `zig build test` passes in CI on Linux.
- [ ] `zig build sim` passes with deterministic crash/recovery assertions.
- [x] Quality Gates PR gate artifacts completed for every gate-changing PR.
  - Source of truth index/template: `docs/quality-gates/README.md`, `docs/quality-gates/TEMPLATE.md`
  - Enforcement: `AGENTS.md` delivery workflow + stop condition.
