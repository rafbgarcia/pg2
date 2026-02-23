# Workfront 13: Nested Selection Performance-First (Spill-Aware Hash Join)

## Objective

Implement a production-ready, spill-aware nested selection join path that scales beyond `scan_batch_size` and avoids O(n\*m) nested-loop behavior under large result sets.

## Why This Exists

- Current nested selection path materializes right-side rows into a bounded in-memory buffer (`scan_batch_size`), which is not spill-scalable.
- Workfront 03 already introduced spill foundations and a `RowSet` contract (`flat` vs `spill`), making this the right point to add a performant nested join engine rather than extending bounded nested-loop behavior.

## Non-Negotiables

1. No silent truncation at any stage.
2. Deterministic behavior under simulation (fixed hash seed, deterministic partition ordering).
3. Spill I/O must use existing temp/storage abstractions only.
4. Fail only on hard-stop boundaries (temp budget exhaustion, storage faults, corruption/invariant violations).

## Current Baseline (for Fresh Sessions)

- Spill path foundations and row-set contract are in place:
  - `src/executor/executor.zig`: `RowSet`, spill windows, collector-backed LIMIT/OFFSET/HAVING/projection handling.
- Nested selection still routes through bounded join path:
  - `src/executor/executor.zig`: `applySingleNestedSelectionJoin` and `applyNestedSelectionJoin`.
  - `src/executor/joins.zig`: bounded nested-loop implementations.
- Regression coverage exists for spill semantics and fail-closed nested spill behavior:
  - `test/stress/spill_phase2_gate_test.zig`.

## Design Decisions Locked

- **Performance-first implementation uses hash join (with spill) as the primary nested selection engine.**
  - No interim O(n\*m) fallback for large spill datasets.
- **Nested semantics are per-parent and must remain so in spill paths.**
  - Child operators (`where/sort/limit/offset/having/group`) apply to each parent's child subset independently, then reattach to that parent.
  - Global child operators across all parents are not allowed in this workfront.
- **Use the `RowSet` contract end-to-end for nested join inputs/outputs.**
  - Nested join must accept either `flat` or `spill` row sets for both sides.
- **Grace-hash partition spill for oversized build side.**
  - Deterministic partition assignment via `std.hash.Wyhash` seed `0`.
- **No runtime allocator usage in executor hot path.**
  - Any nested spill workspace must be preallocated per query slot and passed through runtime/query buffers.
- **Keep current semantics for unsupported join predicates.**
  - Nested selection remains key-equality based as today; do not widen SQL semantics in this workfront.

## Scope

### Phase 1: Nested Join Operator API

- Add `hash_join.zig` operator module for spill-aware hash join over `RowSet`.
- Define input/output descriptors that work with `RowSet`.
- Move nested selection orchestration in `executor.zig` to call the new operator.
- Add explicit per-parent join API contract:
  - input parent stream + child stream + join key mapping
  - output grouped rows preserving parent association for tree protocol serialization.

### Phase 2: In-Memory Hash Join Fast Path

- Build/probe hash join for `flat` inputs.
- Support left join output semantics used by nested selection (emit unmatched left rows with null-filled right projection).
- Preserve deterministic output ordering policy (documented and tested).
- Apply child operators per parent (not globally) in this path.

### Phase 3: Spill-Aware Hash Join

- Add Grace-hash partitioning for oversized build/probe streams.
- Spill partitions via existing temp page facilities.
- Partition processing must be deterministic and bounded.
- Ensure per-parent semantics survive spill:
  - partition and process by join key while preserving parent identity and per-parent child operator behavior.

### Phase 4: Pipeline Integration

- Replace bounded nested-loop callsites with `RowSet`-based hash join path.
- Remove fail-closed nested selection guard for spill when gate tests pass.
- Keep bounded nested-loop only for tiny/no-spill paths if explicitly justified and covered.

### Phase 5: Telemetry + Explainability

- Extend plan/inspect strategy reporting for nested join strategy choice (in-memory hash vs hash spill).
- Add explain strings in serialization for operator visibility.

## Gate

- Unit tests:
  - Hash table/probe correctness, collision handling, null-key behavior.
  - Partition assignment determinism with fixed seed.
  - Spill partition read/write roundtrip and error boundaries.
- Integration tests:
  - Nested selection with left/right datasets > `scan_batch_size` returns complete results.
  - Nested selection with both sides spilling remains correct and deterministic.
  - Nested `limit/offset/sort` is verified per parent under spill (example: each parent returns its own top N children).
  - Mixed query with scan spill + sort spill + nested selection completes under default temp budget.
- Regression:
  - Existing feature/internals/stress suites remain green.
  - Replace current fail-closed nested spill test with correctness tests.

## Suggested Slice Plan (Commit-Oriented)

1. Operator API + in-memory hash join skeleton + unit tests.
2. Spill partition path + deterministic tests.
3. Executor nested integration + remove fail-closed guard.
4. Telemetry/explain + gate/stress coverage.

## Out of Scope

- General query planner join reordering.
- Composite/non-equality join predicates.
- Full multi-way join optimization.
