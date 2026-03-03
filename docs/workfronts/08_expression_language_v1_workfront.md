# Workfront 08: Expression Language v1 Readiness

## Objective

Finish expression-language v1 with production-grade, deterministic behavior across:

- Feature coverage shape (dedicated files per capability)
- Diagnostics contracts (parser/evaluator/mutation paths)
- Parameter binding integration on the session/request path

This workfront is now implementation-focused. Product/design decisions for list runtime model and parameter binding surface are already locked.

## Snapshot (2026-03-02)

Already true:

- Broad expression behavior coverage exists in:
  - `test/features/expressions/where_test.zig`
  - `test/features/expressions/select_test.zig`
  - `test/features/expressions/update_test.zig`
  - `test/features/expressions/precedence_parentheses_test.zig`
  - `test/features/expressions/in_test.zig`
- `test/features/expressions/parameters_test.zig` exists and is imported.
- Structured runtime parameter bindings are wired (`ExecuteRequest.parameter_bindings`), but session-path success coverage is still missing.

Not yet complete:

- Phase 5 dedicated-file matrix is incomplete.
- Diagnostics normalization/hardening (`D01..D05`) is incomplete.
- Session/request successful parameter-binding integration tests (`P08-03`) are incomplete.

## Locked Decisions (Do Not Reopen In This Workfront)

### D-08-01 Runtime List Value Model (Accepted 2026-03-02)

v1 does **not** introduce first-class list expression values.

v1 supports lists only via:

- List literals in membership: `in(value, [a, b, c])`
- Request-scoped `let` list variables (including spill-backed lists): `in(value, ids)`

Unsupported list-producing expression surfaces must fail closed with deterministic diagnostics.

Supported in v1:

```pg2
let userIds = User |> where(active == true) { id }
User |> where(in(id, userIds)) { id }
```

Examples intentionally deferred from v1:

```pg2
User |> where(in(id, coalesce($ids, [1, 2, 3]))) { id }
User |> sort(id asc) { id tags: [status, "active"] }
User |> where(length(intersect(role_ids, $allowed_roles)) > 0) { id }
```

### D-08-02 Parameter Binding Input Surface (Accepted 2026-03-02)

Adopt Candidate A for v1:

- `$param` is query reference syntax only.
- Values are supplied out-of-band via structured request/session binding payload.
- No query-text binding syntax (`params(...)`, preambles, inline binding directives) in v1.

v1 binding contract:

- Canonical binding key includes `$` prefix (for example `$target_id`).
- Duplicate keys in one payload are request errors (fail closed before execution).
- Missing key produces deterministic undefined-parameter diagnostics.
- Null binding values are allowed and use existing null semantics.
- Parameter namespace is independent from columns/`let` names (no fallback/shadow resolution).

## Phase 5: Feature Test Matrix Consolidation

### Gate

- Every listed file exists, is imported in `test/features/features_specs_test.zig`, and passes:
  - `zig build test-all --summary all`
- Behavior parity preserved (this phase is structure/coverage movement, not semantics changes).

### Tasks

- [x] `T05-00` Move existing assertions into dedicated files below (no semantic changes in this slice).
- [x] `T06` `test/features/expressions/lt_test.zig`
- [x] `T07` `test/features/expressions/lte_test.zig`
- [x] `T08` `test/features/expressions/gt_test.zig`
- [x] `T09` `test/features/expressions/gte_test.zig`
- [x] `T10` `test/features/expressions/equality_test.zig`
- [x] `T11` `test/features/expressions/inequality_test.zig`
- [x] `T12` `test/features/expressions/boolean_logic_test.zig` (include explicit short-circuit proofs)
- [x] `T14` `test/features/expressions/logical_not_test.zig`
- [x] `T15` `test/features/expressions/logical_and_test.zig`
- [x] `T16` `test/features/expressions/logical_or_test.zig`
- [ ] `T21` `test/features/expressions/semantics/null_semantics_test.zig`
- [ ] `T22` `test/features/expressions/contexts/cross_context_test.zig`
- [ ] `T23` `test/features/expressions/diagnostics/diagnostics_test.zig`

## Phase 6: Diagnostics and Hardening

### Gate

- Diagnostics are explicit, deterministic, and context-aware.
- Mutation-path expression failures always include assignment path (`path=update.<field>`).

### Tasks

- [ ] `D01` Normalize parser errors for invalid legacy logical/membership textual forms (shape errors, not keyword errors).
- [ ] `D02` Normalize evaluator errors for null arithmetic, type mismatch, and invalid predicate result.
- [ ] `D03` Ensure mutation-path diagnostics include precise assignment path for expression failures.
- [ ] `D04` Add regression tests for fail-closed behavior on unsupported textual expression shapes.
- [ ] `D05` Add membership-specific evaluator diagnostics for incompatible operand/list element types in `in(value, list)`.

## Phase 8: Parameter Binding Integration (Candidate A)

### Gate

- Session/request path supports successful structured parameter bindings across all required contexts.
- Conflict/missing/null behavior is deterministic and tested.

### Tasks

- [ ] `P08-03` Add session/request integration tests for successful bound-parameter execution.
  - Minimum contexts: `where`, `update` assignments, computed `select`, `sort(expr)`, `having`
  - Include deterministic undefined-parameter failures on each relevant path
  - Keep transport behavior in `test/features/expressions/parameters_test.zig`

## Execution Order (Next Slices)

1. `T05-00` + comparator/equality file split (`T06..T11`) with no semantic changes.
2. Logical operator contracts (`T12`, `T14`, `T15`, `T16`) including short-circuit proof tests.
3. Null/cross-context/diagnostics capability files (`T21`, `T22`, `T23`).
4. Diagnostics normalization/hardening (`D01..D05`) with exact error contract assertions.
5. Candidate A integration completion (`P08-03`) on session/request path.

## Completion Criteria

This workfront is complete when all are true:

- All remaining tasks above are checked.
- `scripts/generate_test_suites.sh` has been run after adding new feature files.
- `zig build test-all --summary all` passes.
- Documented contracts in this file match implemented behavior and tests (no stale decision text).
