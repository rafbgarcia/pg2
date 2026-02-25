# Workfront 16: Test Architecture and Reliability

## Objective
Refactor the test system to enforce one deterministic source of truth for test infrastructure, explicit suite cost tiers, and scalable organization that keeps fast feedback fast while preserving deep reliability coverage.

## Why This Exists
- Current tests pass and are reasonably fast, but test infrastructure has started to drift by suite, which can hide correctness differences.
- Test discovery and helper patterns are increasingly manual/duplicated, making growth expensive and error-prone.
- The project needs production-grade database confidence with deterministic behavior across feature, internals, stress, and simulation paths.

## Design Principles (First Principles)
1. Semantics before speed: all suites should share core correctness semantics; performance knobs must be explicit and local.
2. One source of truth: test runtime setup, execution helpers, and assertions should live in shared infrastructure.
3. Cost-tiered feedback loops: quick checks run by default; heavy checks are explicit and gated.
4. Determinism always: no probabilistic tests; replayable seeds; stable output contracts.
5. File ownership clarity: each test file should validate one capability family.

## Current Snapshot (2026-02-25)
- `zig build test --summary all`: `235/235` passed, ~4s.
- `zig build stress --summary all`: `22/22` passed, ~8s.
- `zig build sim --summary all`: `1/1` passed.
- Structural risks observed:
  - Drift between `test/features/test_env_test.zig` and `test/stress/test_env_test.zig` semantics.
  - Large monolithic files in stress/features internals.
  - Repeated local helper functions and setup boilerplate across many files.

## Progress Updates
- 2026-02-25: Removed feature/stress harness semantic drift by introducing shared module `test/shared/test_env.zig`, then re-exporting from `test/features/test_env_test.zig` and `test/stress/test_env_test.zig`.
- 2026-02-25: Added explicit `runSeed` path in shared harness (`run` still forces flush; `runSeed` skips per-request flush) so stress load tuning remains explicit without changing correctness setup semantics.
- 2026-02-25: Audited non-feature test roots and aligned `test/stress/runtime_rss_gate_test.zig` schema/index initialization to shared harness semantics (missing-PK-index metadata + initialize all unique index trees), removing the remaining setup drift.
- 2026-02-25: Started Phase 4 monolith decomposition by splitting `test/stress/spill_phase2_gate_test.zig` into focused modules (`scan`, `collector`, `nested_selection`, `nested_hash_spill`) plus shared helper module `test/stress/spill_phase2_gate_helpers.zig`, preserving stress suite coverage.

## Scope
1. Consolidate shared test runtime/helpers under `test/shared/`.
2. Separate suite roots by execution cost and purpose, not by ad hoc folder history.
3. Split oversized scenario files into focused modules.
4. Reduce repeated assertions and setup glue while keeping test intent explicit.
5. Add matrix-oriented suite steps aligned with reliability gates.

## Out of Scope
1. Behavior changes to database user-facing semantics.
2. Relaxing determinism requirements.
3. Rewriting the simulator model or replacing existing fault injection architecture.

## Non-Negotiables
1. Every phase keeps `zig build test --summary all` green.
2. No hidden semantics differences between suites due to duplicated harness code.
3. Stress and simulation remain opt-in, deterministic, and reproducible.
4. New structure must improve, not reduce, clarity of user-facing feature coverage under `test/features/`.
5. Keep `test/features/` as the canonical location for 1:1 user-facing feature contracts.

## Proposed Target Structure
```text
test/
  all_tests_test.zig
  quick_specs_test.zig
  shared/
    session_env.zig
    assertions.zig
    builders.zig
    fixtures.zig
  features/
    ...
  internals/
    ...
  stress/
    ...
  sim_specs_test.zig (or keep internals/simulation with explicit root)
```

Notes:
- `test/shared/` is preferred over `test/harness/` because this folder holds broader reusable testing infrastructure, not only orchestration logic.
- Keep feature user-facing tests in `test/features` as the canonical product behavior contract.
- Cost tiers (`quick`, `stress`, `sim`, optional `soak`) are primarily execution roots/build steps; directories remain domain-oriented where that preserves clarity and existing contracts.

## Phase 1: Shared Infrastructure Unification
### Scope
- Create `test/shared/session_env.zig` as the single runtime/executor environment for feature, internals, and stress tests.
- Move shared setup/teardown and request execution logic into reusable APIs with explicit config.
- Replace suite-local env copies with lightweight wrappers/import redirects.

### Gate
- No behavior diffs in existing tests.
- `test/features/*`, `test/internals/*`, and `test/stress/*` compile against shared env.
- Both default and stress suites pass with unchanged expectations.

## Phase 2: Shared Assertions and Builders
### Scope
- Introduce `test/shared/assertions.zig` for common checks (`expectContains`, canonical response predicates, deterministic diagnostics checks).
- Introduce `test/shared/builders.zig` for repeated request/schema generation patterns.
- Add shared batch-insert builders for repeated chunked multi-row insert construction/execution patterns used across feature/internals/stress tests.
- Remove duplicated local helpers from feature and stress files where equivalent shared helpers exist.
- Keep scenario-specific insert builders local when their shape/assertions are intentionally specialized.

### Gate
- Reduced helper duplication without reduced readability.
- Shared batch-insert helper is used by multiple suites without changing test intent.
- Representative files across features/internals/stress migrated and passing.

## Phase 3: Suite Taxonomy and Build Steps
### Scope
- Formalize suite steps around cost tiers and intent:
  - `test` (quick + essential deterministic checks)
  - `stress` (heavy deterministic load/gates)
  - `sim` (deterministic simulator/fault matrix)
  - optional `soak` (if introduced later)
- Add/maintain explicit quick-root aggregation (for example `quick_specs_test.zig`) so feedback-loop cost is encoded in execution entrypoints, not inferred from file names.
- Ensure suite roots are explicit and discoverable.
- Keep stress out of default fast loop.
- Adopt generated suite roots as the discovery mechanism (Option 1):
  - `test/tools/test_suite_parser.zig` scans test files for suite annotations (for example `//! @suite: quick`).
  - Parser enforces exactly one required suite annotation per test file.
  - Parser ignores helper/generated paths (for example `test/shared/**`, `test/tools/**`, `test/suites/**`) and only validates runnable `*_test.zig` sources.
  - Parser validates suite names against a single source-of-truth enum/set of allowed suites (`quick`, `stress`, `sim`, optional `soak`), owned by `test/tools/test_suite_parser.zig`.
  - Parser generates `test/suites/<suite>_specs_test.zig` import roots deterministically.
  - `zig build <suite>` runs parser before compiling tests for that suite.
  - `zig build test` runs parser for all suites before executing tests.
  - Suite step inclusion policy: all declared suites are first-class build steps; no hidden excluded suite.

### CI Contract For Missing Imports / Drift
- CI runs parser in check mode for all suites (no writes) and fails if generated output differs from repository files.
- CI fails if a test file is missing suite annotation (unless explicitly ignored by policy).
- CI fails on unknown/invalid suite annotation values.
- After passing check mode, CI executes each suite in parallel (one suite per job/matrix entry).

### Parser Contract (Concrete)
- Annotation format: exactly one line per test file, `//! @suite: <name>`.
- Multiple suite tags in one file are invalid.
- Missing suite tag in a runnable `*_test.zig` file is invalid.
- Unknown suite name is invalid.
- Generated import ordering is deterministic (stable sorted by normalized relative path).
- Modes:
  - `generate`: writes `test/suites/<suite>_specs_test.zig`.
  - `check`: does not write; exits non-zero if generated content differs.

### Gate
- Existing command compatibility preserved where practical.
- Build step descriptions accurately reflect included suites.
- Developer workflow documented and unambiguous.
- Generated suite files are deterministic and reproducible.
- CI catches suite-import drift before test execution.

## Phase 4: Monolith File Decomposition
### Scope
- Split oversized test files into behavior-focused modules (for example spill gates by capability: completeness, safety valve, determinism, nested spill composition).
- Maintain one capability cluster per file with minimal cross-file helper leakage.

### Gate
- Large files reduced to manageable focused modules.
- No coverage loss; suite counts remain stable or increase.

## Phase 5: Matrix and Reliability Gates
### Scope
- Align test execution with Workfront 05 matrix goals:
  - memory profiles
  - concurrency profiles
  - spill/queue fault matrix
  - advisor determinism matrix
- Add explicit runner/grouping conventions so matrix dimensions are intentional and repeatable.

### Gate
- Matrix dimensions are runnable via documented commands.
- Deterministic pass/fail expectations are encoded and testable.

## Phase 6: Guardrails and Maintenance
### Scope
- Add lightweight checks to prevent drift:
  - consistency checks for suite entrypoints/import lists
  - policy for where new shared helpers belong
  - naming and placement conventions for new tests
- Document "when to create a new file vs extend existing file".

### Gate
- New tests can be added with minimal ceremony and no structure ambiguity.
- Drift vectors from current state are explicitly blocked.

## Implementation Order
1. Phase 1 (highest risk, highest leverage)
2. Phase 2
3. Phase 3
4. Phase 4
5. Phase 5
6. Phase 6

## Exit Criteria
1. One shared deterministic test infrastructure under `test/shared/` is the canonical source.
2. No suite-specific semantics drift unless explicitly intentional and documented.
3. Test taxonomy maps cleanly to quick/stress/sim reliability goals.
4. Oversized files are decomposed into focused capability modules.
5. Workfront 05 matrix coverage is executable and documented.
