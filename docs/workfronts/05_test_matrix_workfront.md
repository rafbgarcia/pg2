# Workfront 05: Test Matrix and Reliability Gates

## Objective
Ensure the new concurrency/self-tune/spill/advisor behavior is validated across deterministic memory and load profiles.

## Foundation Status (2026-02-25)
- Lane split established under `test/{features,internals,sim,stress}` with explicit harness ownership in `test/harness`.
- Suite generation and discovery enforcement remain script-driven and deterministic.
- Internal tests are required to import `test/harness/internal_env.zig` instead of feature wrapper paths.

## Next Structural Gate
### Scope
- Migrate legacy inline `src/*` tests into explicit `test/unit` lane or wire an explicit `unit` build target with passing ownership.

### Gate
- Every active test used for release confidence must be discoverable from a `test/<lane>` suite root.

## Migration Progress (2026-02-25)
- `zig build unit` now compiles the legacy inline `src/*` test surface from `src/pg2.zig`.
- `zig build unit-run` is available for execution burn-down while migration continues.

## Phase 1: Memory Profile Matrix
### Profiles
1. `tiny_fail` (expected startup fail)
2. `small_degrade`
3. `default`
4. `large`

### Gate
- CI/test scripts run profile matrix and validate expected pass/fail.

## Phase 2: Concurrency Matrix
### Profiles
1. `1 vCPU / concurrency=1`
2. `2 vCPU / concurrency=2`
3. `N vCPU / concurrency override mismatch`

### Gate
- Tests prove queueing and fairness under mixed client workloads.

## Phase 3: Fault Matrix for Spill and Queue
### Scope
- Inject storage write/read/fsync faults during spill.
- Inject queue timeout stress and long transaction pinning.

### Gate
- Errors are correctly classified and deterministic.
- No hangs or resource leaks after faults.

## Phase 4: Advisor Validation Matrix
### Scope
- Scenario tests where advisor should and should not trigger recommendations.

### Gate
- Stable recommendation IDs and deterministic evidence output.
