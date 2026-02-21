# Workfront 08: Expression Language v1 Readiness

## Objective
Deliver production-ready expression semantics for pg2 across parsing, execution, diagnostics, and feature tests.

## Why
- Current feature coverage includes only addition in `test/features/expressions/addition_test.zig`.
- Parser AST supports expression forms that are not fully executed yet.
- Fresh sessions need a deterministic, itemized backlog they can pick from without rediscovery.

## Confirmed User-Facing Language Rule (2026-02-21)
- Use symbolic logical operators only:
  - Unary negation: `!`
  - Conjunction: `&&`
  - Disjunction: `||`
- Membership is stdlib function only: `in(value, list)`.
- Negated membership is written as `!in(value, list)`.
- Keep query pipeline `|>` only at query/operator level in v1.
- `and`, `or`, `not`, and `in` are not keywords; they are plain identifiers and valid as user-defined names.
- Do not implement reserved-token handling or keyword-specific rejection paths for `and`/`or`/`not`/`in`.
- No backward compatibility aliases for legacy textual logical/membership forms.
- Legacy spellings (for example `a and b`, `status not in [...]`) must fail closed as invalid expression shape.

## Current Gaps Snapshot
- Evaluator does not explicitly handle membership function semantics and `expr_parameter`/`expr_list` node semantics in row predicate evaluation paths.
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
- Tokenizer/parser accept `!`, `&&`, `||` and function-form membership `in(value, list)`.
- Parser tests cover valid symbolic forms and fail-closed invalid legacy textual forms, without keyword-specific parsing branches.

### Tasks
- [x] `E01` Add tokenizer support for symbolic logical operators.
  - Accept: `!`, `&&`, `||`.
  - Ensure `not`, `and`, `or`, `in` tokenize as plain identifiers (no dedicated token kinds).
- [x] `E02` Remove parser support for keyword logical operators.
  - Remove unary/binary logical parsing via textual `not`/`and`/`or`; support symbolic forms only.
  - Do not add keyword-specific fallback/rejection logic for textual forms.
- [x] `E03` Remove parser support for infix membership operators.
  - `in` is function-form only; infix and camelCase legacy forms fail closed.
- [ ] `E04` Parse membership only as stdlib call: `in(value, list)`.
  - Enforce argument count and argument shape at parse boundary where possible.
- [ ] `E06` Precedence tests for symbolic boolean logic.
  - Ensure `!` binds tighter than comparison, `&&` tighter than `||`, with explicit parentheses cases.

## Phase 2: Evaluator Semantics
### Gate
- Evaluator handles all parsed expression node forms and enforces type/null rules.

### Tasks
- [ ] `E07` Implement evaluator semantics for `in(value, list)`.
  - Scalar membership against list literal/expression.
  - Type mismatch and null behavior defined and tested.
- [ ] `E08` Implement evaluator semantics for `!in(value, list)`.
- [ ] `E09` Implement evaluator semantics for list literals in function-based membership checks.
- [ ] `E10` Implement parameter expression evaluation (`expr_parameter`) with explicit binding source.
  - Undefined parameter must fail closed with deterministic error.
- [ ] `E11` Normalize null-comparison behavior under symbolic boolean operators.
- [ ] `E12` Add evaluator regressions for removed legacy logical/membership forms.

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
- [ ] `T06` `test/features/expressions/lt_test.zig`
- [ ] `T06` `test/features/expressions/lte_test.zig`
- [ ] `T06` `test/features/expressions/gt_test.zig`
- [ ] `T06` `test/features/expressions/gte_test.zig`
- [ ] `T06` `test/features/expressions/different_test.zig` (i.e. !=)
- [ ] `T07` `test/features/expressions/boolean_logic_test.zig`
- [ ] `T08` `test/features/expressions/in_test.zig` (includes `!in(value, list)` cases)
- [ ] `T10` `test/features/expressions/symbolic_logic_tokens_test.zig` (covers `!`, `&&`, `||`)
- [ ] `T12` `test/features/expressions/parameters_test.zig`
- [ ] `T13` `test/features/expressions/functions_numeric_test.zig` (side note: numeric builtin behavior and type/arity validation; e.g. `abs`, `sqrt`, `round`)
- [ ] `T14` `test/features/expressions/functions_string_test.zig` (side note: string builtin behavior and edge cases; e.g. `lower`, `upper`, `trim`, `length`)
- [ ] `T15` `test/features/expressions/functions_time_test.zig` (side note: deterministic time function behavior via injected clock; `now()` and related comparisons)
- [ ] `T16` `test/features/expressions/null_semantics_test.zig` (side note: null propagation and boolean/null truth-table behavior across arithmetic, comparisons, and predicates)
- [ ] `T17` `test/features/expressions/cross_context_test.zig` (side note: same expression semantics in `where`, `update`, computed `select`, `sort(expr)`, and `having`)
- [ ] `T18` `test/features/expressions/diagnostics_test.zig` (side note: deterministic fail-closed parser/evaluator errors with precise messages/locations for invalid shapes and type/null violations)
- [ ] `T19` Import all new expression files in `test/features/features_specs_test.zig`.

## Phase 6: Diagnostics and Hardening
### Gate
- Diagnostics are explicit, deterministic, and context-aware.

### Tasks
- [ ] `D01` Normalize parser error messages for invalid legacy logical/membership textual forms (shape errors, not keyword errors).
- [ ] `D02` Normalize evaluator errors for null arithmetic, type mismatch, and invalid predicate result.
- [ ] `D03` Ensure mutation-path diagnostics include precise assignment path for expression failures.
- [ ] `D04` Add regression tests for fail-closed behavior on unsupported textual expression shapes.

## Phase 7: Pending Product Decision
### Gate
- Product decision captured explicitly before implementation.

### Tasks
- [ ] `P01` Evaluate expression-level pipeline syntax for function composition in expressions.
  - Candidate (pending): `status |> in([a, b, c])`.
  - Keep query-level pipeline `|>` behavior unchanged.
  - Do not implement until explicit product sign-off.
