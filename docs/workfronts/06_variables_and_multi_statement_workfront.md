# Workfront 06: Variables and Multi-Statement Execution

## Objective
Support request-scoped variables (`let`) and multiple statements in a single request for reads and mutations (`select`, `insert`, `update`, `delete`) with deterministic, production-safe semantics.

## Why
- Current parser accepts multiple statements and `let`, but executor only runs the first pipeline and ignores `let`.
- Real workflows need query chaining (for example: derive ids, then update/delete using those ids).
- Request-level atomicity must be explicit and testable before this can be safely promoted.

## Proposed Query Shape
```pg2
let userIds = User |> where(active = true) { id }
User |> where(id in userIds) |> update(active = false) {}
User |> where(id = 123) |> delete {}
User |> where(active = true) |> count() { count }
```

## Scope Boundaries
- In scope:
  - `let` for scalar values and rowset-derived single-column lists (for `in` usage).
  - Sequential statement execution in one request.
  - Variable references in predicates and mutation expressions.
  - Deterministic rollback behavior when any statement fails.
- Out of scope (separate workfront):
  - User-defined functions/pipes execution semantics.
  - Cross-request/session variables.
  - Optimizer-level statement reordering.

## Decision Gate (User-Facing Semantics)
Decision confirmed (2026-02-21): Option A.

Option A: atomic request block by default.
- All statements in one request run in the same transaction.
- Any error aborts the whole request.
- Matches safety expectations for newcomers and prevents partial writes.

Option B: statement-level auto-commit inside a request.
- Each statement commits independently.
- Later failures do not rollback earlier statements.
- Lower surprise for SQL users used to autocommit, but easier to create partial-state incidents.

Follow-up: keep Option B as a future explicit opt-in mode only.

## Phase 1: Language and AST Contracts
### Scope
- Define variable value kinds:
  - scalar (`u64`, `i64`, `bool`, `string`, `timestamp`, `null`)
  - list of scalars (single-typed, nullable policy explicit)
  - rowset handle (internal only; must be materialized before cross-statement use)
- Define statement boundary rules and diagnostics.
- Add explicit parse/semantic errors for:
  - undefined variable
  - duplicate variable name in same request
  - invalid variable type in predicate context

### Gate
- Parser/semantic tests prove deterministic diagnostics for all invalid forms.

## Phase 2: Execution Engine for Statement Lists
### Scope
- Replace `findPipeline` single-statement path with statement iterator.
- Introduce request execution context with variable store and deterministic memory bounds.
- Execute statements in source order; capture per-statement stats and final response policy.
- Ensure `let` does not produce rows directly; it only mutates variable state.

### Gate
- Multi-statement read-only request executes all statements in order.
- `let` values can be consumed by later statements in the same request.

## Phase 3: Mutation Semantics and Transaction Guarantees
### Scope
- Wire multi-statement execution into current pool transaction lifecycle.
- On first statement error:
  - abort request transaction
  - clear overflow reclaim staged state for tx
  - emit deterministic failing statement index in error payload
- Ensure mutation counters and inspect output are well-defined for batched statements.

### Gate
- Failure in statement N rolls back mutations from statements `1..N-1`.
- Deterministic tests for update/delete chains using variable-driven `in` filters.

## Phase 4: Protocol and Result Shaping
### Scope
- Decide and implement response format:
  - final statement result only, or
  - per-statement framed results
- Add statement index + phase + error code to serialized errors.
- Preserve backwards-compatible one-statement output shape where possible.

### Gate
- Session tests prove stable wire output for success and failure across multi-statement requests.

## Phase 5: Feature Test Matrix (One File Per Capability)
Create dedicated files under `test/features/variables_and_multi_statement/`:
1. `let_scalar_test.zig`
2. `let_list_from_query_test.zig`
3. `multi_statement_read_chain_test.zig`
4. `multi_statement_mutation_chain_test.zig`
5. `multi_statement_atomic_rollback_test.zig`
6. `undefined_variable_test.zig`
7. `duplicate_variable_test.zig`
8. `invalid_variable_type_usage_test.zig`
9. `statement_error_index_test.zig`
10. `response_shape_multi_statement_test.zig`

### Gate
- Feature suite passes under `zig build test`.
- At least one deterministic fault/recovery scenario added under `test/internals/` for rollback correctness.
