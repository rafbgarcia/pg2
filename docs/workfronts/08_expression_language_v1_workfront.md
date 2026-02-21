# Workfront 08: Expression Language v1 Readiness

## Objective
Deliver production-ready expression semantics for pg2 across parsing, execution, diagnostics, and feature tests.

## Why
- Current feature coverage includes only addition in `test/features/expressions/addition_test.zig`.
- Parser AST supports expression forms that are not fully executed yet.
- Fresh sessions need a deterministic, itemized backlog they can pick from without rediscovery.

## Confirmed User-Facing Language Rule (2026-02-21)
- Use single-word operators/functions only.
- Multi-word SQL spellings are not accepted.
- Required forms:
  - `notIn` (not `not in`)
  - `isNull` (not `is null`)
  - `isNotNull` (not `is not null`)
- Do not silently alias legacy SQL spellings; fail closed with explicit diagnostics.

## Current Gaps Snapshot
- Evaluator does not explicitly handle `expr_in`, `expr_not_in`, `expr_parameter`, `expr_list` node semantics in row predicate evaluation paths.
- `lower`, `upper`, `trim` are placeholders.
- `now()` is placeholder and must be wired to injected clock semantics.
- No dedicated feature files for most expression capabilities.

## Scope Boundaries
- In scope:
  - Expression behavior in `where`, `update`, computed `select`, `sort(expr)`, `group/having`.
  - Parser/tokenizer/evaluator alignment for supported expression forms.
  - Deterministic, typed errors with fail-closed behavior.
  - One feature file per capability under `test/features/expressions/`.
- Out of scope:
  - `between`, `like`, regex, `case`, cast syntax.
  - Non-deterministic clock access in core code.

## Entry Points
- Parser/tokenizer:
  - `src/parser/tokenizer.zig`
  - `src/parser/expression.zig`
  - `src/parser/parser_ops.zig`
  - `src/parser/parser_test.zig`
- Evaluator/executor:
  - `src/executor/filter.zig`
  - `src/executor/executor.zig`
  - `src/executor/mutation.zig`
- Feature tests:
  - `test/features/expressions/`
  - `test/features/features_specs_test.zig`

## Pickup Workflow (for fresh Codex sessions)
1. Pick exactly one unchecked item from the task list below.
2. Implement parser/executor/test changes for that item only.
3. Add or update one dedicated feature file for that capability.
4. Run targeted tests, then `zig build test`.
5. Mark the item `[x]` and add a dated note under "Implementation Log".
6. Commit with message prefix: `expressions:` followed by task id.

## Phase 1: Language Surface and Parsing
### Gate
- Tokenizer/parser accept only single-word expression operators/functions.
- Parser tests cover valid forms and fail-closed invalid legacy forms.

### Tasks
- [ ] `E01` Add tokenizer keyword support for `notIn`.
  - Accept: tokenized as one operator token.
  - Reject: split `not in`.
- [ ] `E02` Add tokenizer keyword support for `isNull`.
- [ ] `E03` Add tokenizer keyword support for `isNotNull`.
- [ ] `E04` Extend expression parser for `notIn`.
  - Ensure precedence with comparison/logical operators is explicit and tested.
- [ ] `E05` Extend expression parser for unary null predicates.
  - `field isNull`
  - `field isNotNull`
- [ ] `E06` Parser regression tests for legacy SQL spellings rejection.
  - `not in`, `is null`, `is not null` must return explicit parse errors.

## Phase 2: Evaluator Semantics
### Gate
- Evaluator handles all parsed expression node forms and enforces type/null rules.

### Tasks
- [ ] `E07` Implement evaluator semantics for `expr_in`.
  - LHS scalar membership against RHS list.
  - Type mismatch and null behavior defined and tested.
- [ ] `E08` Implement evaluator semantics for `expr_not_in`.
- [ ] `E09` Implement evaluator semantics for list literals in membership checks.
- [ ] `E10` Implement parameter expression evaluation (`expr_parameter`) with explicit binding source.
  - Undefined parameter must fail closed with deterministic error.
- [ ] `E11` Implement `isNull` evaluator semantics.
- [ ] `E12` Implement `isNotNull` evaluator semantics.

## Phase 3: Built-in Functions and Deterministic Time
### Gate
- Scalar functions provide real behavior (not placeholders).
- Time behavior uses injected clock path, not system clock.

### Tasks
- [ ] `E13` Implement `lower` behavior.
- [ ] `E14` Implement `upper` behavior.
- [ ] `E15` Implement `trim` behavior.
- [ ] `E16` Validate `length`, `coalesce`, `abs`, `sqrt`, `round` arity and type rules with exhaustive tests.
- [ ] `E17` Replace placeholder `now()` with injected clock semantics.
  - Add deterministic test clock wiring and tests.

## Phase 4: Cross-Context Consistency
### Gate
- Same expression semantics apply consistently in all supported execution contexts.

### Tasks
- [ ] `E18` `where` expression parity suite.
- [ ] `E19` `update` assignment expression parity suite.
- [ ] `E20` computed `select` expression parity suite.
- [ ] `E21` `sort(expr)` expression parity suite.
- [ ] `E22` `having` expression parity suite (with aggregates).

## Phase 5: Feature Test Matrix (One Capability Per File)
### Gate
- Every listed file exists, is imported in feature suite, and passes.

### Tasks
- [ ] `T01` `test/features/expressions/subtraction_test.zig`
- [ ] `T02` `test/features/expressions/multiplication_test.zig`
- [ ] `T03` `test/features/expressions/division_test.zig`
- [ ] `T04` `test/features/expressions/unary_minus_test.zig`
- [ ] `T05` `test/features/expressions/precedence_parentheses_test.zig`
- [ ] `T06` `test/features/expressions/comparisons_test.zig`
- [ ] `T07` `test/features/expressions/boolean_logic_test.zig`
- [ ] `T08` `test/features/expressions/in_test.zig`
- [ ] `T09` `test/features/expressions/not_in_test.zig` (uses `notIn`)
- [ ] `T10` `test/features/expressions/is_null_test.zig` (uses `isNull`)
- [ ] `T11` `test/features/expressions/is_not_null_test.zig` (uses `isNotNull`)
- [ ] `T12` `test/features/expressions/parameters_test.zig`
- [ ] `T13` `test/features/expressions/functions_numeric_test.zig`
- [ ] `T14` `test/features/expressions/functions_string_test.zig`
- [ ] `T15` `test/features/expressions/functions_time_test.zig`
- [ ] `T16` `test/features/expressions/null_semantics_test.zig`
- [ ] `T17` `test/features/expressions/cross_context_test.zig`
- [ ] `T18` `test/features/expressions/diagnostics_test.zig`
- [ ] `T19` Import all new expression files in `test/features/features_specs_test.zig`.

## Phase 6: Diagnostics and Hardening
### Gate
- Diagnostics are explicit, deterministic, and context-aware.

### Tasks
- [ ] `D01` Normalize parser error messages for invalid single-word operator usage.
- [ ] `D02` Normalize evaluator errors for null arithmetic, type mismatch, and invalid predicate result.
- [ ] `D03` Ensure mutation-path diagnostics include precise assignment path for expression failures.
- [ ] `D04` Add regression tests for fail-closed behavior on unsupported spellings and unsupported expression shapes.

## Implementation Log
- `YYYY-MM-DD`: (task id) short note, commit hash, tests run.

