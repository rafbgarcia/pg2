# Workfront 08: Expression Language v1 Readiness

## Objective

Deliver production-ready expression semantics for pg2 across parsing, execution, diagnostics, and feature tests.

## Phase 5: Feature Test Matrix

### Gate

- Every listed file exists, is imported in feature suite, and passes.
- Recommended foldering for clarity at scale:
  - Keep all expression behavior under `test/features/expressions/`.
  - Split by concern: `stdlib/`, `semantics/`, `contexts/`, `diagnostics/`.
  - For built-ins, use one file per function (avoids broad multi-function files and makes failures easier to localize).

### Tasks

- [ ] `T06` `test/features/expressions/lt_test.zig` (side note: less-than `<` comparison semantics and type/null behavior)
- [ ] `T07` `test/features/expressions/lte_test.zig` (side note: less-than-or-equal `<=` comparison semantics and type/null behavior)
- [ ] `T08` `test/features/expressions/gt_test.zig` (side note: greater-than `>` comparison semantics and type/null behavior)
- [ ] `T09` `test/features/expressions/gte_test.zig` (side note: greater-than-or-equal `>=` comparison semantics and type/null behavior)
- [ ] `T10` `test/features/expressions/equality_test.zig` (side note: equality `==` semantics including null comparison behavior)
- [ ] `T11` `test/features/expressions/inequality_test.zig` (side note: inequality `!=` semantics including null comparison behavior)
- [ ] `T12` `test/features/expressions/boolean_logic_test.zig` (side note: boolean operator semantics for `!`, `&&`, `||` including short-circuit and null interactions)
- [ ] `T14` `test/features/expressions/logical_not_test.zig` (side note: unary logical negation `!` semantics and parse shape)
- [ ] `T15` `test/features/expressions/logical_and_test.zig` (side note: conjunction `&&` semantics and parse shape)
- [ ] `T16` `test/features/expressions/logical_or_test.zig` (side note: disjunction `||` semantics and parse shape)
- [ ] `T21` `test/features/expressions/semantics/null_semantics_test.zig` (side note: null propagation and boolean/null truth-table behavior across arithmetic, comparisons, and predicates)
- [ ] `T22` `test/features/expressions/contexts/cross_context_test.zig` (side note: same expression semantics in `where`, `update`, computed `select`, `sort(expr)`, and `having`)
- [ ] `T23` `test/features/expressions/diagnostics/diagnostics_test.zig` (side note: deterministic fail-closed parser/evaluator errors with precise messages/locations for invalid shapes and type/null violations)

## Phase 6: Diagnostics and Hardening

### Gate

- Diagnostics are explicit, deterministic, and context-aware.

### Tasks

- [ ] `D01` Normalize parser error messages for invalid legacy logical/membership textual forms (shape errors, not keyword errors).
- [ ] `D02` Normalize evaluator errors for null arithmetic, type mismatch, and invalid predicate result.
- [ ] `D03` Ensure mutation-path diagnostics include precise assignment path for expression failures.
- [ ] `D04` Add regression tests for fail-closed behavior on unsupported textual expression shapes.
- [ ] `D05` Add membership-specific evaluator diagnostics that identify incompatible operand/list element types for `in(value, list)` failures.

## Phase 7: Runtime Value Model Decision

### Gate

- Runtime expression value model for collections is explicit before extending membership to variable/subquery-backed list sources.

### Tasks

- [ ] `R01` Decide whether expression runtime values include first-class list values beyond direct list literals.
- [ ] `R02` If approved, design `Value` representation and evaluator contracts for list-carrying expressions (including parameter/subquery binding paths).
- [ ] `R03` If deferred, document explicit v1 boundary and fail-closed behavior for unsupported list-producing expression forms.

## Phase 8: Pending Product Decision

### Gate

- Product decision captured explicitly before implementation.

### Tasks

- [ ] `P08-02` Decide user-facing parameter binding input surface for `$param`.
  - Candidate A: structured runtime/session API payload only (bindings separate from query text).
  - Candidate B: query-text binding syntax (for example dedicated `params(...)` stage or preamble binding form).
  - Candidate C: support both, with one documented default path.
  - Must define deterministic conflict rules (duplicate keys, shadowing, missing keys, and null handling) and fail-closed diagnostics.
- [ ] `P08-03` After `P08-02` is decided, add session/request integration tests for successful bound-parameter execution.
  - Cover at minimum: `where`, `update` assignments, computed `select`, `sort(expr)`, and deterministic undefined-parameter errors.
  - Dependency: do not start this task until `P08-02` is explicitly accepted in product sign-off notes.
  - Keep one capability file for binding transport behavior in `test/features/expressions/parameters_test.zig`.
