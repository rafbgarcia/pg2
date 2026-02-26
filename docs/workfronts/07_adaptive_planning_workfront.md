# Workfront 07: Adaptive Query Planning Policy

## Objective

Define and implement a dedicated planning module for pg2 physical execution policy without introducing a traditional pre-execution cost-based optimizer.

The planner must be deterministic, inspectable, and safe under pressure. Adaptive behavior is allowed only through explicit, testable checkpoints with fail-closed semantics.

## Why

- Today, planning behavior is partially embedded in executor paths and inspect metadata.
- Policy ownership (join/materialize/spill/stream decisions) should be explicit and testable.
- Features like multi-statement variables need stable planning contracts, not ad-hoc executor branching.

## Dependencies

- Depends on `docs/workfronts/02_self_tune_memory_workfront.md` for memory/admission-derived capacity inputs.
- Depends on `docs/workfronts/03_degrade_spill_workfront.md` for spill/degrade execution primitives.
- Must converge planner contracts used by `docs/workfronts/06_variables_and_multi_statement_workfront.md`; WF06 may ship first, and WF07 then formalizes/owns those contracts without semantic drift.

## Implementation Status (2026-02-26)

- Implemented:
  - New query planner module surface under `src/planner/` with:
    - immutable snapshot + decision schemas
    - deterministic snapshot/decision fingerprint utilities
    - deterministic initial policy derivation (`planInitial`)
    - checkpoint adaptation module with degrade-only transitions (`adaptAtCheckpoint`)
  - Runtime startup planner split started:
    - startup capacity planner moved to `src/runtime/capacity_planner.zig`
    - `src/runtime/planner.zig` now acts as compatibility wrapper during migration
  - Executor plan stats now seed from planner snapshot/decision contracts.
  - Checkpoint adaptation is now wired in read-pipeline execution at:
    - `pre_scan`
    - `post_filter`
    - `post_group` (group and non-group paths; non-group emits deterministic no-op checkpoint)
    - `pre_join`
  - Inspect serialization now includes:
    - planner policy/snapshot/decision fingerprints
    - per-decision reason codes
    - deterministic checkpoint chronology with prior/new decision fingerprints
    - plain-language explain detail line for major decisions:
      - join
      - materialization
      - streaming
      - parallel mode
      - scheduler path
  - Parallelization policy foundation landed:
    - planner-level `parallel_mode` decision (default `sequential`, feature-gated `enabled`)
    - deterministic parallel schedule-trace builder under `src/planner/parallel.zig`
    - executor/inspect now expose deterministic schedule-trace metadata:
      - `parallel_schedule_task_count`
      - `parallel_schedule_fingerprint`
    - executor now routes `parallel_mode=enabled` through a deterministic serial scheduler path (`parallel_scheduler_path=scheduled_serial`) while preserving current sequential semantics
- Tests:
  - internal planner contract tests added under `test/internals/planner/`
  - user-visible inspect contract coverage added under:
    - `test/features/queries/planner_inspect_contract_test.zig`
  - deterministic replay coverage for planner adaptation traces added in:
    - `test/sim/planner_adaptation_replay_sim_test.zig`
    - `test/stress/spill_phase2_gate_nested_hash_spill_contracts_test.zig`
  - deterministic replay coverage for parallel schedule traces added in:
    - `test/sim/planner_parallel_schedule_sim_test.zig`
  - semantic-equivalence coverage for planner parallel mode gate added in executor tests (parallel-mode enabled vs disabled yields identical result rows while execution remains sequential)
  - full `zig build unit --summary all` and `zig build test --summary all` passing after integration
- Verification:
  - `zig build sim --summary all` passing with planner adaptation replay checks
  - `zig build stress --summary all` passing with planner checkpoint/fingerprint assertions in mixed spill scenarios
- Remaining:
  - replace deterministic serial scheduler path with real parallel execution while preserving deterministic scheduling traces and semantic equivalence guarantees

## Fresh Session Handoff Snapshot (2026-02-26)

### Decision Lock

- Checkpoint chronology must reflect real execution order (never synthetic display order):
  - `pre_scan -> post_filter -> post_group -> pre_join`
- Adaptation remains degrade-only and deterministic.
- Parallel execution policy is feature-gated; default behavior remains sequential and fail-closed.

### Delivered Through This Session

- Planner foundations:
  - `src/planner/types.zig`
  - `src/planner/fingerprint.zig`
  - `src/planner/planner.zig`
  - `src/planner/adaptation.zig`
  - `src/planner/parallel.zig`
- Runtime split:
  - startup capacity planner moved to `src/runtime/capacity_planner.zig`
  - `src/runtime/planner.zig` retained as compatibility wrapper
- Executor/inspect integration:
  - plan stats seeded from planner snapshot + decision contracts
  - checkpoint traces recorded and serialized
  - plain-language explain details emitted via `INSPECT explain_detail`
  - parallel policy metadata and deterministic schedule-trace metadata emitted
  - deterministic serial scheduler path activated when `parallel_mode=enabled`
- Tests added/extended:
  - internals planner contract tests
  - sim adaptation replay determinism
  - sim parallel schedule determinism
  - stress planner checkpoint/fingerprint assertions
  - feature-level inspect contract test (`test/features/queries/planner_inspect_contract_test.zig`)

### Relevant Commits (newest first)

- `0c05bf8` Extend inspect explain with join/materialization/parallel details
- `6740b2d` Route parallel mode through deterministic serial scheduler path
- `ce3991e` Add feature-level planner inspect contract coverage
- `1a49bca` Expose deterministic parallel schedule trace metadata in inspect
- `7413389` Add parallel-mode equivalence coverage and gate wiring
- `a3ee059` Thread planner feature gates through executor snapshot context
- `2eca3ea` Add planner parallel-mode policy foundation and schedule traces
- `8514e84` Align checkpoint chronology with execution order across query paths
- `3a8afcc` Add sim and stress replay checks for planner checkpoint traces
- `157b739` Assert checkpoint inspect ordering and update WF07 status
- `bd6ccaf` Wire planner checkpoints and inspect chronology output
- `430d4fb` Add checkpoint adaptation rules and snapshot capacity fields
- `265c775` Seed executor plan stats from planner decisions and reasons
- `119b400` Add WF07 planner foundation and split capacity planner

### Immediate Next Step (single-threaded priority)

1. Replace `parallel_scheduler_path=scheduled_serial` with true parallel execution while preserving:
   - deterministic schedule traces for fixed seeds
   - semantic equivalence with sequential mode
   - fail-closed behavior under capacity pressure

### Acceptance Gates (must all pass)

- `zig build unit --summary all`
- `zig build test --summary all`
- `zig build sim --summary all`
- `zig build stress --summary all`

### Hard-Stop Reminders

- Stop if any path introduces non-deterministic scheduling without seeded/stable tie-breaks.
- Stop if any checkpoint order/meaning diverges from actual execution stage order.
- Stop if parallel mode can return different rows than sequential mode for the same query/snapshot.

## Non-Goals

- No classic static cost model with cardinality selectivity estimation as primary decision source.
- No opaque heuristics that cannot be explained via inspect reason codes.
- No mid-operator opportunistic re-planning outside named checkpoints.

## First-Principles Design Constraints (Hard)

1. Same planner input fingerprint must produce the same decision set.
2. Planner decisions are immutable between checkpoints.
3. Adaptation is degrade-only for the current query (safety over oscillation).
4. If an adaptive decision cannot be made confidently, choose the safer bounded mode (materialize/spill/stream-safe path), never an optimistic unbounded path.
5. Every physical decision must have a machine-stable reason code and deterministic tie-break rule.
6. Planner must be memory-budget aware by construction; no policy may violate active budget limits.

## Planner Input Contract (Hard)

Define a planner input snapshot captured once at plan start, with explicit epoch/version fields:

- logical operator chain / query shape fingerprint
- catalog stats snapshot id (or version)
- bounded runtime counters snapshot id
- capacity profile from runtime config/admission
- feature gates/config flags
- deterministic seed and planner policy version

Rules:

- Planning reads only the captured snapshot, never live mutable state.
- Any checkpoint adaptation reads only checkpoint-local observed counters plus original snapshot.
- Tie-breakers must use deterministic ordering (for example stable relation id ordering, then lexical operator id ordering).

## Design Locks (Resolved 2026-02-26)

The following decisions are now locked and must be treated as hard contract unless a later explicit revision is approved.

### 1) Snapshot schema and fingerprint serialization (Hard)

Planner snapshot must be a strict versioned struct with fixed-width fields and canonical ordering:

- `snapshot_schema_version: u16`
- `policy_version: u16`
- `seed: u64`
- `query_shape_fingerprint: u128`
- `catalog_snapshot_id: u64`
- `runtime_counters_snapshot_id: u64`
- `capacity_profile_id: u64`
- `feature_gate_mask: u64`
- `operator_sequence: [max_operators]OpTag` (unused tail zero-filled)
- `relation_ids_sorted: [max_relations]u32` (ascending; unused tail zero-filled)

Fingerprinting contract:

- Hash algorithm: deterministic `Wyhash` with seed `0`.
- Input bytes: canonical little-endian encoding of the full snapshot struct in field order.
- No variable-length or text-normalization-dependent hashing in planner contracts.
- Any binary layout change or field semantic change requires `snapshot_schema_version` bump.

### 2) Reason-code vocabulary and policy versioning (Hard)

Planner decisions must use a closed machine-stable reason-code enum. Initial required vocabulary:

- `JOIN_HASH_IN_MEMORY_CAPACITY_OK`
- `JOIN_HASH_SPILL_RIGHT_EXCEEDS_BUILD_WINDOW`
- `SORT_IN_MEMORY_WITHIN_BUDGET`
- `SORT_EXTERNAL_REQUIRED_BY_ROWFLOW`
- `GROUP_LINEAR_WITHIN_GROUP_CAP`
- `GROUP_HASH_SPILL_GROUP_CAP_EXCEEDED`
- `MATERIALIZE_BOUNDED_REQUIRED`
- `STREAMING_ENABLED_SAFE`
- `STREAMING_DISABLED_RISK_UNBOUNDED`
- `DEGRADE_MONOTONIC_GUARD`

Versioning contract:

- Any reason-code addition/removal/semantic change requires explicit `policy_version` bump.
- Fixture drift from reason-code or decision semantics must be intentional and reviewable.

### 3) Degrade lattice (Hard)

Adaptation monotonicity is defined by a strict degrade lattice; upgrades are forbidden within a query:

- Join: `hash_in_memory -> hash_spill`
- Sort: `in_memory_merge -> external_merge`
- Group: `in_memory_linear -> hash_spill`
- Materialization: `none -> bounded_row_buffers`
- Streaming: `enabled -> disabled`

No transition may move upward in risk after a prior degradation.

### 4) Checkpoint observed counters and threshold policy (Hard)

Checkpoint adaptation may only use bounded deterministic counters:

- `rows_seen`
- `rows_after_filter`
- `bytes_accumulated`
- `spill_pages_used`
- `group_count_estimate` (bounded deterministic estimator)
- `join_build_rows`
- `join_probe_rows`

Thresholds must be pure deterministic functions of snapshot capacities (no wall clock, no ambient mutable state).
Initial required threshold forms:

- sort degrade when `bytes_accumulated > work_memory_bytes_per_slot * 3 / 4`
- group degrade when `group_count_estimate > aggregate_groups_cap`
- join degrade when `join_build_rows * avg_row_width > join_build_budget`

### 5) Inspect/explain schema additions and ordering (Hard)

`INSPECT plan` / `INSPECT explain` must include:

- `planner_policy_version`
- `planner_snapshot_fingerprint`
- `planner_decision_fingerprint`
- decision family outputs with `chosen_strategy`, `reason_code`, and deterministic `tie_break_key`
- checkpoint chronology in fixed order:
  - `pre_scan`
  - `post_filter`
  - `post_group`
  - `pre_join`
- per-checkpoint transition record:
  - `prior_decision`
  - `new_decision`
  - `reason_code`
  - `degraded=true|false`

Formatting and section order must be deterministic for fixture stability.
Chronology must reflect real execution stage order, not a synthetic display-only order.

## Phase 1: Planner Module and Contracts

Status: ✅ Complete

### Scope

- Introduce planner namespace (for example `src/planner/`) with explicit plan-policy API.
- Separate logical query shape from physical decision outputs.
- Define immutable planner input snapshot and plan decision schema.
- Add decision fingerprinting utility used by tests (`input_fingerprint -> decision_fingerprint`).

### Gate

- Unit tests for planner inputs/outputs with stable serialized fixtures.
- Property test: identical input fingerprint always yields identical decision fingerprint.
- Negative tests: missing snapshot/version fields fail planning with deterministic error codes.

## Phase 2: Physical Decision Policy v1

Status: ✅ Complete (v1 baseline landed; further refinement may extend policy detail without changing completion state)

### Scope

- Formalize deterministic policies for:
  - join strategy and order
  - materialization mode
  - sort/group strategy
  - spill trigger thresholds
  - response streaming mode selection
- Keep policy simple and inspectable; no hidden heuristics.
- Define deterministic tie-break hierarchy for each decision family.

### Gate

- Deterministic tests prove same inputs always produce same physical plan decision set.
- Exhaustive table-driven tests cover tie-break paths per decision family.
- Memory-budget safety tests prove selected policy never exceeds declared budget envelope.

## Phase 3: Runtime-Observed Adaptation Rules

Status: 🟡 In Progress

### Scope

- Define explicit checkpoint contract: pre-scan, post-filter, pre-join, post-group.
- At each checkpoint, adaptation may only move to an equal-or-safer bounded mode for the current query.
- Forbid oscillation: once degraded at checkpoint N, later checkpoints cannot revert to a riskier mode.
- Require each checkpoint decision to emit reason codes for inspect/explain output.

### Gate

- Tests cover both branches:
  - catalog-driven decisions
  - observed-rowflow-driven decisions
- Monotonicity tests verify degrade-only behavior across checkpoints.
- Replay tests verify identical observed checkpoint inputs produce identical adaptation decisions.

## Phase 4: Inspect/Explain Contract

Status: 🟡 In Progress

### Scope

- Stabilize `INSPECT plan` fields and reason-code vocabulary.
- Add plain-language explain mapping for every major physical decision.
- Ensure deterministic output order and formatting for test stability.
- Include checkpoint chronology and prior-decision reference in inspect output.

### Gate

- Session/integration tests assert stable inspect+explain output under seeded scenarios.
- Backward-compatibility is not required at this project stage, but once this phase lands, reason-code drift must be explicit via policy-version bump.

## Phase 5: Parallelization Policy

Status: 🟡 In Progress

### Scope

- Define planner-level policy for what may run in parallel.
- Default remains sequential unless explicitly enabled by config/feature gate.
- Define deterministic scheduler contract for simulation/replay (task ordering, tie-breakers, seed usage).
- Parallel plan must preserve semantic equivalence with sequential plan.

### Gate

- Feature-gated tests prove equivalent query semantics between sequential and parallel-enabled modes.
- Deterministic simulation tests show reproducible task schedule traces under fixed seeds.

## Phase 6: Planning Test Surface

Status: 🟡 In Progress

### Scope

- Add dedicated tests under:
  - `test/internals/planner/` for decision-policy units
  - `test/features/queries/` for user-visible inspect behavior
  - deterministic simulation scenarios for checkpoint adaptation and parallel scheduling
- Include regression tests for plan stability and reason-code drift.

### Gate

- `zig build unit --summary all` passes with planner tests enabled.
- `zig build test --summary all` passes with planner tests enabled.
- `zig build sim --summary all` passes and repeats are stable under fixed seeds.
- `zig build stress --summary all` passes for planner/executor pressure scenarios.
- Planner decision snapshots are stable across repeated seeded runs.

## Exit Criteria (Workfront Complete)

All phases are complete only when:

1. Planner policy is the sole owner of physical decision selection.
2. Executor no longer carries hidden policy branches that bypass planner contracts.
3. Deterministic simulation reproduces identical planning/adaptation traces for fixed seeds.
4. Inspect output can explain every major decision and adaptation checkpoint.

## Hard Stop Conditions

Pause implementation and resolve design before coding further if any of these happen:

- A required decision lacks deterministic tie-break definition.
- A policy path requires reading mutable live stats outside snapshot/checkpoint contracts.
- An adaptation path would increase risk after prior degradation (non-monotonic behavior).
- A test depends on wall-clock timing or non-seeded randomness.
