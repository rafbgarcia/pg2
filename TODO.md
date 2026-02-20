# pg2 Next Milestones

This checklist is for fresh Codex sessions to continue high-priority implementation work in order.
Release-readiness gates live in `V1_READINESS_CHECKLIST.md`.

## Milestone to Gate Mapping

- Milestone 1 (FK + referential actions) -> Gate 4, Gate 8
- Milestone 2 (MVCC + recovery hardening) -> Gate 5, Gate 8
- Milestone 3 (introspection expansion) -> Gate 3, Gate 7, Gate 8
- Milestone 4 (Tiger Style PR gate) -> Release Decision (Tiger Style artifacts)

- [x] Milestone 1: Foreign keys with explicit referential actions
  - Scope: Add FK declaration/validation and enforce explicit `ON DELETE` / `ON UPDATE` semantics with fail-closed behavior for missing or unsupported actions.
  - Deliverables:
    - Parser + catalog support for FK metadata and actions.
    - Executor enforcement on insert/update/delete paths.
    - Clear error classes/messages for integrity violations and unsupported configurations.
    - Documentation updates in `docs/QUERY_LANGUAGE.md` and any relevant architecture docs.
  - Done when:
    - End-to-end tests cover valid actions and violations.
    - Missing action declarations are rejected explicitly.
    - Deterministic behavior is preserved in simulator tests.

- [x] Milestone 2: Transactional correctness hardening (MVCC + recovery)
  - Scope: Expand correctness guarantees for conflict handling, rollback paths, and WAL/undo crash consistency under deterministic faults.
  - Deliverables:
    - [x] Additional tests for write-write conflicts.
    - [x] Rollback edge-case deterministic tests.
    - [x] Fault-injection tests for crash/restart during WAL+undo interactions.
    - [x] Explicit invariant checks/documentation for recovery ordering and visibility rules.
  - Done when:
    - [x] `zig build test` and `zig build sim` include new regression cases.
    - [x] Replay after crash is deterministic and state-consistent across seeds.

- [ ] Milestone 3: Planner/executor introspection expansion
  - Scope: Improve `inspect` output to explain physical decisions (join strategy/order, materialization, sort/aggregation choices) in plain language.
  - Deliverables:
    - [x] Structured introspection data for key planner/executor decisions.
    - [x] User-facing explanation strings tied to runtime stats and query shape.
    - [x] Tests that assert introspection stability/quality for representative queries.
      - Added deterministic `INSPECT plan ...` output for source model, pipeline operator chain, join strategy/order, materialization mode, sort/group physical strategies, and nested relation count.
      - Added `INSPECT explain ...` plain-language strategy explanations for sort/group paths.
  - Done when:
    - Common query plans are explainable without reading internals.
    - Introspection output is deterministic and test-covered.

- [ ] Milestone 4: Tiger Style PR gate completion for each milestone
  - Scope: Enforce project robustness standards on every milestone PR.
  - Deliverables:
    - Completed checklist from `docs/TIGER_STYLE.md` per PR:
      - Invariant changes
      - Crash-consistency contract
      - Error class changes
      - Persistent format/protocol impact
      - Deterministic crash/fault tests
      - Performance baseline/threshold impact
    - Artifact index and initial milestone coverage:
      - `docs/tiger-gates/README.md`
      - `docs/tiger-gates/810a60c-inspect-plan-metadata.md`
      - `docs/tiger-gates/fa983f5-inspect-sort-group-strategy.md`
  - Done when:
    - Every merged milestone links a completed Tiger Style gate artifact.
