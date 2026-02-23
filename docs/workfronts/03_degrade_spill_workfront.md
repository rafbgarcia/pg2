# Workfront 03: Degrade-First Execution and Spill

## Objective

Queries must degrade under memory pressure (spill to temp storage) before failing, while preserving exact SQL semantics.

## Session Handoff Snapshot (2026-02-23)

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
- ❌ Per-parent nested child subsets still have in-memory caps (subset rows + match arena). No per-parent spill engine yet.

### Commits Already Landed (latest relevant)

- `c64c2a7`: decouple spill operators from global collector binding.
- `4258bae`: allocation-free chunked nested child scans with preallocated workspace.
- `801a136`: per-parent nested child operator semantics.
- `38d805d`, `6e39f43`, `70d88b6`, `6437c47`: collector-backed semantic correctness and regression coverage.

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

- Per-parent child matches are still bounded by in-memory subset capacity and per-parent match arena size.

## Phase 5: Per-Parent Nested Spill 🚧 Active / Next Major Implementation

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

## Phase 6: Spill-Aware Hash Join (Cross-Tracked with WF13) ⏳ Pending

### Status

- Not implemented in WF03 yet.
- Detailed design work is tracked in `docs/workfronts/13_nested_spill_hash_join_workfront.md`.

### WF03 Ownership

- WF03 owns executor spill contracts and correctness guardrails.
- WF13 provides detailed join/nested-spill algorithm work; implementation must still satisfy WF03 gates/non-negotiables.

## Immediate Next Step (for fresh Codex session)

1. Implement Phase 5 Slice 1: parent-local nested row source/output descriptor.
2. Thread descriptor through nested execution path in `src/executor/executor.zig` (no behavior change yet).
3. Add a focused failing test in `test/internals/spill/` for one-parent-many-children overflow case to lock expected behavior before full spill wiring.
4. Then implement Slice 2 + 3 in the same branch.

## Verification Command

- `zig build test --summary all`

## Hard-Stop Conditions

- Stop immediately if any design step implies global (cross-parent) semantics for nested operators.
- Stop immediately if any path can serialize rows not produced by the final operator descriptor.
- Stop immediately if tests encode truncated behavior as expected behavior.
