# Workfront 07: Adaptive Query Planning Policy

## Objective
Define and implement a dedicated planning module for pg2 physical execution policy without introducing a traditional pre-execution cost-based optimizer.

## Why
- Today, planning behavior is partially embedded in executor paths and inspect metadata.
- Policy ownership (join/materialize/spill/stream decisions) should be explicit and testable.
- Features like multi-statement variables need stable planning contracts, not ad-hoc executor branching.

## Dependencies
- Depends on `docs/workfronts/02_self_tune_memory_workfront.md` for memory/admission-derived capacity inputs.
- Depends on `docs/workfronts/03_degrade_spill_workfront.md` for spill/degrade execution primitives.
- Must provide contracts consumed by `docs/workfronts/06_variables_and_multi_statement_workfront.md`.

## Non-Goals
- No classic static cost model with cardinality selectivity estimation as primary decision source.
- No rule to reorder user-visible statement semantics.
- No non-deterministic adaptive behavior.

## Phase 1: Planner Module and Contracts
### Scope
- Introduce planner namespace (for example `src/planner/`) with explicit plan-policy API.
- Separate logical query shape from physical decision outputs.
- Define deterministic planner inputs:
  - operator chain
  - bounded runtime counters and catalog stats
  - capacity limits from runtime config

### Gate
- Unit tests for planner inputs/outputs with stable serialized fixtures.

## Phase 2: Physical Decision Policy v1
### Scope
- Formalize deterministic policies for:
  - join strategy and order
  - materialization mode
  - sort/group strategy
  - spill trigger thresholds
  - response streaming mode selection
- Keep policy simple and inspectable; no hidden heuristics.

### Gate
- Deterministic tests prove same inputs always produce same physical plan decision set.

## Phase 3: Runtime-Observed Adaptation Rules
### Scope
- Define when planner decisions use catalog constants vs observed rowflow at runtime.
- Add explicit decision checkpoints (pre-scan, post-filter, pre-join, post-group).
- Require each checkpoint to emit reason codes for inspect/explain output.

### Gate
- Tests cover both branches:
  - catalog-driven decisions
  - observed-rowflow-driven decisions

## Phase 4: Inspect/Explain Contract
### Scope
- Stabilize `INSPECT plan` fields and reason-code vocabulary.
- Add plain-language explain mapping for every major physical decision.
- Ensure deterministic output order and formatting for test stability.

### Gate
- Session/integration tests assert stable inspect+explain output under seeded scenarios.

## Phase 5: Parallelization Policy (Explicit Opt-In)
### Scope
- Define planner-level policy for what may run in parallel (if runtime supports it).
- Default remains sequential unless explicitly enabled by config/feature gate.
- Preserve deterministic scheduling contracts under simulation.

### Gate
- Feature-gated tests prove equivalent query semantics between sequential and parallel-enabled modes.

## Phase 6: Planning Test Surface
### Scope
- Add dedicated tests under:
  - `test/internals/planner/` for decision-policy units
  - `test/features/queries/` for user-visible inspect behavior
- Include regression tests for plan stability and reason-code drift.

### Gate
- `zig build test` passes with planner tests enabled.
- Planner decision snapshots are stable across repeated seeded runs.
