# pg2 Next Milestones

This checklist is for fresh Codex sessions to continue high-priority implementation work in order.

- [ ] Milestone 1: Foreign keys with explicit referential actions
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

- [ ] Milestone 2: Transactional correctness hardening (MVCC + recovery)
  - Scope: Expand correctness guarantees for conflict handling, rollback paths, and WAL/undo crash consistency under deterministic faults.
  - Deliverables:
    - Additional tests for write-write conflicts and rollback edge cases.
    - Fault-injection tests for crash/restart during WAL+undo interactions.
    - Explicit invariant checks/documentation for recovery ordering and visibility rules.
  - Done when:
    - `zig build test` and `zig build sim` include new regression cases.
    - Replay after crash is deterministic and state-consistent across seeds.

- [ ] Milestone 3: Planner/executor introspection expansion
  - Scope: Improve `inspect` output to explain physical decisions (join strategy/order, materialization, sort/aggregation choices) in plain language.
  - Deliverables:
    - Structured introspection data for key planner/executor decisions.
    - User-facing explanation strings tied to runtime stats and query shape.
    - Tests that assert introspection stability/quality for representative queries.
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
  - Done when:
    - Every merged milestone links a completed Tiger Style gate artifact.
