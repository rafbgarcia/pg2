# Milestone Tracker: String Overflow + Auto-Compaction Foundation

Last updated: 2026-02-20
Owner: Codex session (handoff-ready)

## Scope

Build a production-grade string storage path that keeps common text fast inline while safely handling large values:

1. Auto page compaction on update/insert shortfall (foundation already started).
2. Hybrid string storage:
   - Inline small strings.
   - Spill large strings to overflow pages.
3. Deterministic and crash-safe reclaim for replaced/deleted overflow chains.

## Confirmed Product Decisions

These are confirmed in chat with the user:

- Encoding: UTF-8.
- Inline threshold target for `string`: 1024 bytes.
- Overflow model: single overflow chain per spilled field value.
- Reclaim policy: immediate logical unlink + deterministic reclaim pipeline (not synchronous full physical reclaim inside mutation path).

## Clarification to Carry Forward

- "String has no declared max" remains a logical type contract.
- Physical storage policy is separate and uses byte thresholds, not character counts.
- 1024 is bytes, so number of characters depends on UTF-8 code points.

## Milestone Done Criteria (Exit Criteria)

This milestone is done only when all conditions below are true:

1. Scope completion:
   - Auto-compaction + hybrid inline/overflow + deterministic reclaim behavior are complete through server session E2E coverage.
2. Remaining core gaps closed:
   - Overflow lifecycle WAL is applied in full page/data replay recovery paths (not decode-only coverage).
   - Transaction abort/rollback semantics are explicitly defined and tested for overflow create/relink/unlink/reclaim lifecycle.
   - Reclaim drain budget semantics are explicitly defined and tested for mutations that unlink multiple overflow chains.
3. Quality-gate discipline:
   - Every core DB increment in this milestone has a corresponding artifact in `docs/quality-gates/` and `docs/quality-gates/README.md` is updated.
   - No placeholder commit markers remain in quality-gate or milestone docs.
4. User-facing docs parity:
   - Any user-visible behavior or error/inspect output changes are documented in `user-facing-docs/` in the same increment.
5. Validation:
   - `zig build test` passes at milestone close with overflow E2E coverage included.

## Known Test State

- `zig build test` passes for this increment (includes overflow lifecycle recovery replay unit + server E2E idempotence coverage).

## Next Logical Chunk

1. Durable replay integration (completed in `505fff1`):
   - Added `src/storage/recovery.zig` replay path for committed/legacy-replayable overflow lifecycle WAL.
   - Added crash/restart validation through server session E2E and replay idempotence checks.
2. Tx-level abort semantics (open):
   - Define and test overflow lifecycle behavior under transaction abort/rollback with explicit undo/reclaim ordering.
   - Acceptance:
     - Deterministic tests cover abort after create, abort after relink intent, and abort after unlink enqueue.
     - Tests assert no leaked reachable chains and no reclaimed-live-chain behavior.
3. Queue-drain budget semantics (open):
   - Define/document expected backlog progression when one mutation unlinks multiple overflow chains.
   - Acceptance:
     - Tests verify deterministic backlog depth progression and counter behavior under fixed per-mutation reclaim budget.
     - `INSPECT overflow ...` counters/queue depth reflect documented behavior.

## Next-Session Kickoff (Concrete)

Completed in committed chunks `c5548a0`, `fd81f61`, `ad8ccd7`, and `505fff1`:

1. Overflow reclaim pipeline.
2. WAL lifecycle contract for create/relink/unlink/reclaim.
3. Deterministic crash/restart coverage for spill/replace/delete.
4. Malformed/cyclic chain fail-closed reclaim coverage.
5. Inspect-level reclaim backlog/throughput visibility.
6. Recovery replay application for overflow lifecycle WAL with idempotent reclaim semantics.

Next session should execute in order:

1. Add tx-abort lifecycle matrix tests for overflow create/relink/unlink/reclaim ordering guarantees.
2. Lock reclaim budget semantics with deterministic multi-chain unlink tests + inspect assertions.
3. Decide and implement strict tx-marker replay policy (current replay supports legacy mutation WAL without tx begin/commit markers to avoid dropping replay).
4. Update quality-gate artifact(s), `docs/quality-gates/README.md`, and user-facing docs for any behavior changes.
5. Run the AGENTS placeholder hygiene check before finalizing docs.

## Fresh Codex Handoff Commands

Use these first in a new session:

1. `git status --short`
2. `zig build test`
3. `git log -6 --stat`
4. `git show --name-only --stat 505fff1`
5. `git show --name-only --stat ad8ccd7`
6. `git show --name-only --stat fd81f61`
7. `git show --name-only --stat c5548a0`
8. `rg -n "overflow_chain_create|overflow_chain_relink|overflow_chain_unlink|overflow_chain_reclaim|replay|recover" src`
9. `rg -n "INSPECT overflow|overflow_reclaim_stats|snapshotOverflowReclaimStats|overflow_reclaim_queue" src docs user-facing-docs`
10. Run the AGENTS placeholder hygiene check command.
11. Continue from "Next Logical Chunk" acceptance criteria above.
