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
- Expression equality operator is `==` (not `=`).
- `=` is assignment/config syntax only (for example `insert(...)`, `update(...)`, and schema option payloads), not expression comparison.
- Membership is stdlib function only: `in(value, list)`.
- Negated membership is written as `!in(value, list)`.
- Keep query pipeline `|>` only at query/operator level in v1.
- `and`, `or`, `not`, and `in` are not keywords; they are plain identifiers and valid as user-defined names.
- Do not implement reserved-token handling or keyword-specific rejection paths for `and`/`or`/`not`/`in`.
- No backward compatibility aliases for legacy textual logical/membership forms.
- Legacy spellings (for example `a and b`, `status not in [...]`) must fail closed as invalid expression shape.

## Current Gaps Snapshot
- Expression equality currently uses `=` in parser/evaluator/tests and must be migrated to `==`.
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
- Tokenizer/parser accept `==` for expression equality, and reject `=` in expression contexts.
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
- [x] `E04` Parse membership only as stdlib call: `in(value, list)`.
  - Enforce argument count and argument shape at parse boundary where possible.
- [x] `E05` Switch expression equality syntax from `=` to `==`.
  - Tokenizer: emit dedicated `==` token for expression equality.
  - Parser: use `==` for comparison nodes and fail closed on `=` in expression positions.
  - Keep `=` valid only for assignment/config grammar positions.
  - Add parser regressions for accepted `==` and rejected `=` in expression contexts (`where`, `having`, computed `select`, `sort(expr)`, assignment RHS expressions).
- [x] `E06` Precedence tests for symbolic boolean logic.
  - Ensure `!` binds tighter than comparison, `&&` tighter than `||`, with explicit parentheses cases.

## Phase 2: Evaluator Semantics
### Gate
- Evaluator handles all parsed expression node forms and enforces type/null rules.

### Tasks
- [x] `E07` Implement evaluator semantics for `in(value, list)`.
  - Scalar membership against list literal/expression.
  - Type mismatch and null behavior defined and tested.
- [x] `E08` Implement evaluator semantics for `!in(value, list)`.
- [x] `E09` Implement evaluator semantics for list literals in function-based membership checks.
- [ ] `E10` Implement parameter expression evaluation (`expr_parameter`) with explicit binding source.
  - Undefined parameter must fail closed with deterministic error.
- [x] `E11` Normalize null-comparison behavior under symbolic boolean operators.
- [ ] `E12` Add evaluator regressions for removed legacy logical/membership forms.
- [x] `E12a` Normalize evaluator equality semantics for `==` (and `!=`) after parser migration.
  - Type/null behavior, deterministic errors, and fail-closed invalid predicate outputs.

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
- [x] `E18` `where` expression parity suite.
- [x] `E19` `update` assignment expression parity suite.
- [x] `E20` computed `select` expression parity suite.
- [x] `E21` `sort(expr)` expression parity suite.
- [ ] `E22` `having` expression parity suite (with aggregates).

## Phase 5: Feature Test Matrix (One Capability Per File)
### Gate
- Every listed file exists, is imported in feature suite, and passes.
- Recommended foldering for clarity at scale:
  - Keep all expression behavior under `test/features/expressions/`.
  - Split by concern: `stdlib/`, `semantics/`, `contexts/`, `diagnostics/`.
  - For built-ins, use one file per function (avoids broad multi-function files and makes failures easier to localize).

### Tasks
- [x] `T01` `test/features/expressions/subtraction_test.zig` (side note: binary subtraction `a - b`; includes numeric type/null behavior)
- [ ] `T02` `test/features/expressions/multiplication_test.zig` (side note: binary multiplication `a * b`; includes numeric type/null behavior)
- [ ] `T03` `test/features/expressions/division_test.zig` (side note: binary division `a / b`; includes divide-by-zero and numeric type/null behavior)
- [ ] `T04` `test/features/expressions/unary_minus_test.zig` (side note: unary negation `-a`/`-(expr)`; distinct from binary subtraction)
- [ ] `T05` `test/features/expressions/precedence_parentheses_test.zig` (side note: operator precedence and explicit grouping across arithmetic/comparison/boolean operators)
- [ ] `T06` `test/features/expressions/lt_test.zig` (side note: less-than `<` comparison semantics and type/null behavior)
- [ ] `T07` `test/features/expressions/lte_test.zig` (side note: less-than-or-equal `<=` comparison semantics and type/null behavior)
- [ ] `T08` `test/features/expressions/gt_test.zig` (side note: greater-than `>` comparison semantics and type/null behavior)
- [ ] `T09` `test/features/expressions/gte_test.zig` (side note: greater-than-or-equal `>=` comparison semantics and type/null behavior)
- [ ] `T10` `test/features/expressions/equality_test.zig` (side note: equality `==` semantics including null comparison behavior)
- [ ] `T11` `test/features/expressions/inequality_test.zig` (side note: inequality `!=` semantics including null comparison behavior)
- [ ] `T12` `test/features/expressions/boolean_logic_test.zig` (side note: boolean operator semantics for `!`, `&&`, `||` including short-circuit and null interactions)
- [ ] `T13` `test/features/expressions/in_test.zig` (includes `!in(value, list)` cases)
- [ ] `T14` `test/features/expressions/logical_not_test.zig` (side note: unary logical negation `!` semantics and parse shape)
- [ ] `T15` `test/features/expressions/logical_and_test.zig` (side note: conjunction `&&` semantics and parse shape)
- [ ] `T16` `test/features/expressions/logical_or_test.zig` (side note: disjunction `||` semantics and parse shape)
- [ ] `T17` `test/features/expressions/parameters_test.zig` (side note: parameter binding semantics, undefined-parameter failures, and deterministic diagnostics)
- [ ] `T18` `test/features/expressions/stdlib/abs_test.zig`, `test/features/expressions/stdlib/sqrt_test.zig`, `test/features/expressions/stdlib/round_test.zig` (side note: numeric builtin behavior and type/arity validation; one file per function)
- [ ] `T19` `test/features/expressions/stdlib/lower_test.zig`, `test/features/expressions/stdlib/upper_test.zig`, `test/features/expressions/stdlib/trim_test.zig`, `test/features/expressions/stdlib/length_test.zig`, `test/features/expressions/stdlib/coalesce_test.zig` (side note: string/null-handling builtins with edge cases; one file per function)
- [ ] `T20` `test/features/expressions/stdlib/now_test.zig` (side note: deterministic time function behavior via injected clock; no system clock in core code)
- [ ] `T21` `test/features/expressions/semantics/null_semantics_test.zig` (side note: null propagation and boolean/null truth-table behavior across arithmetic, comparisons, and predicates)
- [ ] `T22` `test/features/expressions/contexts/cross_context_test.zig` (side note: same expression semantics in `where`, `update`, computed `select`, `sort(expr)`, and `having`)
- [ ] `T23` `test/features/expressions/diagnostics/diagnostics_test.zig` (side note: deterministic fail-closed parser/evaluator errors with precise messages/locations for invalid shapes and type/null violations)
- [ ] `T24` Import all new expression files in `test/features/features_specs_test.zig`. (side note: keep feature suite discovery complete and deterministic)

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
- [ ] Evaluate expression-level pipeline syntax for function composition in expressions.
  - Candidate (pending): `status |> in([a, b, c])`.
  - Keep query-level pipeline `|>` behavior unchanged.
  - Do not implement until explicit product sign-off.

## Implementation Log
- 2026-02-21: Completed `E20` by implementing runtime projection support for top-level computed select fields (`select_computed`) in `src/executor/executor.zig` and adding `test/features/expressions/computed_select_test.zig`. Coverage includes parity between `where` and computed projection for composed boolean/arithmetic/membership expressions, null equality semantics in computed output (`status == null || status != null`), and fail-closed computed projection errors for incompatible comparison types. Imported in `test/features/features_specs_test.zig` and validated with `zig build test`.
- 2026-02-21: Completed `E19` by adding `test/features/expressions/update_assignment_test.zig` with dedicated parity coverage for expression evaluation in `update(...)` assignments: composed boolean/arithmetic/membership expressions aligned with `where` outcomes, null equality semantics (`status == null || status != null`) assigned through update paths, fail-closed incompatible comparison typing (`string == i64`), and fail-closed null arithmetic operand diagnostics with assignment path (`path=update.flag`). Imported the file in `test/features/features_specs_test.zig` and validated via `zig build test`.
- 2026-02-21: Completed `E11` and `E12a` by normalizing evaluator null/comparison semantics in `src/executor/filter.zig`: symbolic boolean operators now evaluate with null-aware three-valued logic instead of fail-closed type mismatch on null operands; equality operators now apply explicit null semantics (`null == null` true, mixed null `!=` true) and fail closed on incompatible non-numeric cross-type comparisons. Added evaluator regressions for null-aware boolean behavior, null equality/ordering outcomes, and equality type-mismatch failures. Revalidated `test/features/expressions/where_test.zig` against the original parity expectations that depend on these semantics.
- 2026-02-21: Completed `E18` by adding `test/features/expressions/where_test.zig` with dedicated parity coverage for `where(...)` across arithmetic/comparison predicates, boolean precedence/parentheses behavior, membership composition (`in`/`!in`), direct and negated boolean-column predicates, and fail-closed handling for non-boolean predicate outputs. Imported the suite in `test/features/features_specs_test.zig` and validated with `zig build test`.
- 2026-02-21: Reorganized expression feature folder intent: operator/context behavior remains at `test/features/expressions/*.zig` (including `sort_test.zig`), while standard-library builtin coverage is tracked under `test/features/expressions/stdlib/` (renamed from `functions/` in workfront references).
- 2026-02-21: Completed `E21` by extending sort-key parsing to accept general expression keys (not only aggregate/builtin-led forms), while preserving bare-column sort syntax. Added parser coverage for arithmetic sort expressions and validated runtime parity through feature tests using `sort(base + extra ...)`, `sort(base - extra ...)`, and `sort(in(status, [...]) ...)` in `test/features/expressions/`. Kept computed-select assertions out of this change because computed projection shaping remains gated under `E20`.
- 2026-02-21: Completed `T01` by adding `test/features/expressions/subtraction_test.zig` with dedicated feature coverage for representative numeric subtraction (`i64`, `u64`, `f64`) plus fail-closed mutation diagnostics for type mismatch, constrained integer underflow (`IntegerOutOfRange` with assignment path), and null arithmetic operands. Imported the new file in `test/features/features_specs_test.zig` and validated via `zig build test`.
- 2026-02-21: Added explicit follow-up planning for membership diagnostics (`D05`) and runtime list value-model decisioning (`R01`-`R03`) so future variable/subquery-backed membership support is gated by an explicit product/runtime model decision.
- 2026-02-21: Completed `E07`, `E08`, and `E09` by adding dedicated evaluator handling for membership function calls (`in(value, list)`) with list-literal element evaluation and null-aware semantics: `in(null, list) -> null`, no-match with null element -> `null`, direct match -> `true`, null-free miss -> `false`. Enforced fail-closed type mismatch for incompatible non-null membership comparisons and added unit coverage in `src/executor/filter.zig` plus feature coverage in `test/features/expressions/in_test.zig` (including `!in(...)` behavior and assignment-path type mismatch failure).
- 2026-02-21: Completed `E05` by introducing a dedicated `==` tokenizer token for expression equality, migrating expression parsing/evaluation from `=` to `==`, and keeping `=` only for assignment/config grammar. Added parser regressions that reject `=` in expression contexts (`where`, computed `select`, `sort(expr)`, assignment RHS expression) and migrated query/test fixtures using expression predicates to `==`.
- 2026-02-21: Product decision captured: expression equality uses `==`; `=` is assignment/config-only syntax. Added migration tasks for tokenizer/parser/evaluator and feature coverage, including fail-closed rejection of `=` in expression contexts.
- 2026-02-21: Completed `E06` by adding parser/expression precedence coverage for symbolic boolean logic. Added AST-shape assertions proving `!` binds tighter than comparison, `&&` binds tighter than `||`, and parentheses override default grouping. Added parser-level fail-closed regressions for legacy textual logical forms (`and`/`or`/`not`) in `where(...)`.
- 2026-02-21: Completed `E04` by enforcing parse-time membership shape for `in(value, list)` only (exactly two args; second arg must be list literal), threading source text through parser/expression entry points so `in` remains a plain identifier token while membership-call validation stays explicit and fail-closed. Added parser/expression regression tests for valid form and invalid arity/shape.
