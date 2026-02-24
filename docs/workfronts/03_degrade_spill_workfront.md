# Workfront 03: Degrade-First Execution and Spill

## Objective

Queries must degrade under memory pressure (spill to temp storage) before failing, while preserving exact SQL semantics.

## Session Handoff Snapshot (2026-02-24)

### Decision Lock

- Active path is **Option A**: full production-grade semantics for nested spill (no phased semantic compromises).
- Nested operators are **per-parent** (user expectation): child `WHERE/GROUP/HAVING/SORT/LIMIT/OFFSET` apply independently for each parent row.
- Root-level operators remain global to the full root row set.

### Current Status

- ✅ Temp spill foundation exists and is deterministic (`Storage`-backed temp pages, per-slot isolation, telemetry).
- ✅ Scan/materialization degrade path exists (collector-backed spill with byte-budget behavior).
- ✅ Collector-backed correctness slices landed for root pipeline (`LIMIT/OFFSET/HAVING`, projection, external-sort+limit regressions).
- ✅ Nested semantics corrected to per-parent for in-memory paths.
- ✅ Nested child scanning is chunked and allocation-free at runtime via preallocated workspace.
- ✅ External sort and hash aggregate now accept explicit collector handles (not hard-wired to `ctx.collector`), enabling composition for parent-local spill.
- ✅ Per-parent nested child subsets now spill via parent-local collectors (no in-memory subset truncation/failure behavior).
- ✅ Nested per-parent operator order under spill is wired and covered: `WHERE -> GROUP BY -> HAVING -> ORDER BY -> OFFSET -> LIMIT`.
- ✅ Nested spill + aggregate `HAVING` path fixed and replay-deterministic under test.
- ✅ Phase 6 started: nested hash join fast path exists for **no-child-operator** nested selections with deterministic left-major output.
- ✅ `hash_in_memory` nested strategy is wired for flat-left and collector-left paths when right side is flat-fit.
- ✅ `hash_spill` nested strategy is wired for flat-left and collector-left paths when right side exceeds flat-fit (deterministic partition spill/read path).
- ✅ Spill partition probe performance improved with per-partition page chaining + partition-local hash cache (with deterministic fallback).
- ✅ Determinism coverage now includes repeated-run nested `hash_spill` with alternating left-key partition access.
- ✅ Nested hash join now applies to child-operator pipelines (`WHERE/GROUP/HAVING/SORT/OFFSET/LIMIT`) for both flat-left and collector-left parent paths.
- ✅ Stress coverage now includes mixed root spill + nested hash spill under tight temp/storage budgets.
- ✅ `INSPECT plan`/`INSPECT explain` now expose nested join strategy breakdown counters (`nested_loop`, `hash_in_memory`, `hash_spill`).

### Commits Already Landed (latest relevant)

- `c64c2a7`: decouple spill operators from global collector binding.
- `4258bae`: allocation-free chunked nested child scans with preallocated workspace.
- `801a136`: per-parent nested child operator semantics.
- `38d805d`, `6e39f43`, `70d88b6`, `6437c47`: collector-backed semantic correctness and regression coverage.
- `eb8dfbc`: per-parent nested spill execution path and contracts.
- `4f34fdf`: nested spill aggregate `HAVING` state-budget correctness fix.
- `8d474db`: nested temp-region isolation hardening + determinism coverage.
- `ae7ea23`: in-memory nested hash-join fast paths (`hash_in_memory`) + integration wiring/tests.
- `a4b5636`: deterministic hash-join spill partition primitives.
- `f8be935`: executor nested `hash_spill` path for large right-side no-op nested joins.
- `2b2cbf0`: partition probe cache for nested `hash_spill`.
- `a20026f`: spill page chaining per partition for faster partition iteration.
- `9fa86e3`: deterministic replay coverage for alternating-partition nested `hash_spill`.
- `084558d`: nested hash-join execution for child-operator pipelines (flat-left and collector-left) + tests.
- `23298fb`: mixed root spill + nested hash spill stress coverage under tight budgets.
- `7576907`: inspect/telemetry expansion for nested join strategy breakdown across query shapes.

## Non-Negotiables

1. Use `Storage` abstraction for spill/temp I/O.
2. No silent truncation or semantic drift under spill.
3. Deterministic behavior under simulation/replay.
4. Fail closed only on hard-stop conditions (I/O/corruption/exhaustion), never wrong results.

## Phase Plan (Logical, Current Truth)

## Phase 1: Temp/Spill Storage Foundation ✅ Completed

### Delivered

- `src/storage/temp.zig`: temp page allocator/manager, page format, per-slot page regions.
- Temp pages bypass WAL and buffer pool (query-scoped ephemeral data).
- Spill telemetry counters in `ExecStats` + `INSPECT spill` surfacing.

### Gate

- Unit/integration/determinism coverage for temp storage behavior is in place and passing.

## Phase 2: Scan/Materialization Degrade Path ✅ Completed

### Delivered

- Compact spill row format: `src/storage/spill_row.zig`.
- `SpillingResultCollector`: hot-batch + spill pages + iterator + reset.
- Chunked scan loop feeding collector; byte-budget-based degradation.
- Executor/runtime plumbing for per-slot collector and spill budget.

### Gate

- Large scans spill and complete without truncation; deterministic replay and telemetry tests pass.

## Phase 3: Root Spill Correctness Contract ✅ Completed (for current supported root ops)

### Delivered

- Row-set contract for post-scan outputs (`flat` vs spill stream/window semantics).
- Collector-backed `LIMIT/OFFSET/HAVING` correctness.
- Collector-backed projection correctness (including computed projection expressions).
- Spill + external-sort + downstream limit regression coverage.

### Gate

- No known wrong-result risk in currently supported collector-backed root operator combinations covered by tests.

## Phase 4: Nested Semantics Baseline ✅ Completed

### Delivered

- Per-parent nested selection semantics implemented for flat and collector-backed parent paths.
- Tree protocol serialization/counting made collector-aware.
- Child scans chunked over child model (not one-batch capped).
- Runtime nested execution uses preallocated workspace (no runtime allocation in sealed runtime).

### Remaining Limitation

- Per-parent child subsets no longer fail at the prior in-memory subset cap.
- Remaining nested spill/join expansion work is in Phase 6 / WF13.

## Phase 5: Per-Parent Nested Spill ✅ Completed

### Goal

Remove per-parent in-memory subset limits by making nested child pipelines spill-capable per parent while preserving per-parent semantics.

### Locked Design

- Parent-local child rows use a parent-scoped spill collector (or equivalent spill descriptor), reset per parent.
- Nested operator order is unchanged and applied per parent: `WHERE -> GROUP BY -> HAVING -> ORDER BY -> OFFSET -> LIMIT`.
- `HAVING` is per-parent because grouping is per parent.
- Root-level sort/having semantics are not reused for nested scope; execution context is explicitly parent-local.
- Reuse existing spill-capable operator engines (external sort/hash aggregate) via explicit collector injection.

### Implementation Slices

1. **Parent-local row source contract**

- Introduce a small nested pipeline row source/output descriptor usable by nested operators (flat slice or spill iterator/descriptor).
- Keep it separate from root serialization contract to avoid coupling bugs.

2. **Parent-local spill collector integration**

- Materialize child matches for one parent into parent-local collector when subset exceeds workspace cap.
- Ensure strict per-parent reset/reclaim behavior.

3. **Per-parent spill-aware operator execution**

- Wire nested `WHERE/GROUP/HAVING/SORT/OFFSET/LIMIT` to consume parent-local descriptor and produce parent-local descriptor.
- Use explicit collector parameter for external sort and hash aggregate.

4. **Tree protocol emission from per-parent descriptor**

- Emit nested child arrays from the final parent-local descriptor with exact row counts.
- Preserve existing empty-child behavior.

5. **Hard failure boundaries**

- Replace current per-parent subset cap failures with spill behavior.
- Keep explicit hard failures only for true resource exhaustion/corruption.

### Phase 5 Gate (must pass)

- New integration tests where a single parent has child cardinality far above in-memory subset cap and still returns correct nested results.
- Coverage for each per-parent operator under spill:
  - nested `WHERE`
  - nested `GROUP BY + HAVING`
  - nested `ORDER BY`
  - nested `OFFSET/LIMIT`
  - combinations (e.g. `WHERE + SORT + LIMIT`, `GROUP + HAVING + SORT`)
- Determinism test for repeated runs with same seed.
- Full suite: `zig build test --summary all`.

### Gate Status

- ✅ One-parent-many-children overflow case succeeds with correct nested output.
- ✅ Nested spill coverage exists for `WHERE`, `GROUP BY + HAVING`, `ORDER BY`, `OFFSET/LIMIT`, and combinations.
- ✅ Determinism coverage exists for nested spill + aggregate `HAVING` replay.
- ✅ Full suite currently passing: `zig build test --summary all`.

## Phase 6: Spill-Aware Hash Join (Cross-Tracked with WF13) 🚧 In Progress

### Status

- Implemented nested hash-join execution for both:
  - **no-child-operator nested selections**
  - **child-operator nested selections**
    (`WHERE/GROUP/HAVING/SORT/OFFSET/LIMIT`)
- Strategy coverage:
  - `hash_in_memory` for flat-fit right side.
  - `hash_spill` for oversized right side using deterministic partition spill.
  - Works for both flat-left and collector-left parent paths.
- Determinism, mixed-spill stress, and inspect strategy-breakdown coverage are landed.
- Full Phase 6 still has one cleanup slice remaining (see below).
- Detailed design work is tracked in `docs/workfronts/13_nested_spill_hash_join_workfront.md`.

### Completed in Phase 6 so far

1. Nested join operator groundwork (`hash_join.zig`) and deterministic in-memory left-join hash execution.
2. Executor integration for nested no-op child pipelines on both flat-left and collector-left paths.
3. Deterministic spill partition primitives + right-side partitioned nested probing path.
4. Spill probe optimizations: partition-local hash cache + per-partition page chains.
5. Determinism regression for alternating-partition nested `hash_spill`.
6. Executor integration for child-operator nested pipelines on both flat-left and collector-left paths.
7. Stress coverage for mixed root spill + nested hash spill under tight budgets.
8. Telemetry/explain expansion with explicit nested join strategy breakdown counters.

### Remaining Phase 6 slices

1. Eliminate/retire the remaining nested-loop fallback path in nested selection join execution (or make fallback reachability explicit and bounded), now that hash paths cover supported nested query shapes.

### WF03 Ownership

- WF03 owns executor spill contracts and correctness guardrails.
- WF13 provides detailed join/nested-spill algorithm work; implementation must still satisfy WF03 gates/non-negotiables.

## Immediate Next Step (for fresh Codex session)

1. Implement the final Phase 6 cleanup slice: remove or explicitly hard-bound nested-loop fallback reachability in nested selection join execution.
2. Keep WF03 guardrails explicit in tests: no cross-parent semantic bleed, no serialized rows outside final descriptor, fail-closed only on hard boundaries.
3. Keep `INSPECT` strategy-breakdown counters consistent with actual path selection after fallback cleanup.

## Verification Command

- `zig build test --summary all`

## Hard-Stop Conditions

- Stop immediately if any design step implies global (cross-parent) semantics for nested operators.
- Stop immediately if any path can serialize rows not produced by the final operator descriptor.
- Stop immediately if tests encode truncated behavior as expected behavior.
