# Workfront 06: Variables and Multi-Statement Execution

## Objective
Support request-scoped variables (`let`) and multiple statements in a single request for reads and mutations (`select`, `insert`, `update`, `delete`) with deterministic, production-safe semantics.

## Why
- Current parser accepts multiple statements and `let`; this workfront completes `let` execution semantics, variable usage, and final-expression return behavior.
- Real workflows need query chaining (for example: derive ids, then update/delete using those ids).
- Request-level atomicity must be explicit and testable before this can be safely promoted.

## Dependencies
- Hard dependency: `docs/workfronts/03_degrade_spill_workfront.md` is treated as complete for this workfront's execution semantics.
- Assumption: spill/temp storage paths are available for deterministic variable materialization beyond in-memory thresholds.

## Implementation Status (2026-02-25)
- Implemented:
  - Top-level expression statements (`expr_stmt`) and object literals for arbitrary final return payloads.
  - Request-scoped `let` execution for scalar and single-column list materialization.
  - Variable resolution in read and mutation expression paths (including `in(field, var_list)`).
  - Deterministic ambiguous identifier fail-closed behavior (column vs variable).
  - Deterministic statement-indexed errors for multi-statement requests.
  - Final-statement-only response contract with expression payload serialization.
  - Feature coverage added under `test/features/variables_and_multi_statement/`.
- Remaining blocker:
  - Spill-backed persistence for variable materialization across statements is not yet implemented.
  - Current implementation is bounded in-memory for variable materialization; this is deterministic, but does not yet satisfy full Phase 5 spill behavior.

## Proposed Query Shape
```pg2
let userIds = User |> where(active == true) { id }
User |> where(in(id, userIds)) |> update(active = false) {}
User |> where(id == 123) |> delete {}
User |> where(active == true) |> count() { count }
```

Last statement is the return

## Allow for arbitrary return
Examples:

```pg2
1 + 1 // equivalent SQL `SELECT 1 + 1`
```

```pg2
let sum = User |> count()
sum // return e.g. int 10
```

```pg2
{
  total_users: User |> count()
  posts: User { id post_count: posts |> count() }
} // returns {total_users: 123, posts: [{id: 1, post_count: 10}, {id: 2, post_count: 3}] }
```

## Scope Boundaries
- In scope:
  - `let` for scalar values and rowset-derived single-column lists (for `in` usage).
  - Sequential statement execution in one request.
  - Variable references in predicates and mutation expressions.
  - Deterministic rollback behavior when any statement fails.
  - Shared request snapshot semantics and read-your-own-writes guarantees.
  - Final-statement-only response contract for multi-statement requests.
  - Deterministic statement-indexed errors.
- Out of scope (separate workfront):
  - User-defined functions/pipes execution semantics.
  - Cross-request/session variables.
  - Optimizer-level statement reordering.
  - New parallel executor for statement fan-out.

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

Additional confirmed decisions (2026-02-21):
- All statements in one request share one transaction snapshot baseline.
- Multi-statement responses serialize only the final statement's row payload.
- Errors include deterministic failing `statement_index`.

Additional confirmed decisions (2026-02-23):
- If an identifier could resolve to both a column and a `let` variable in the same expression context, fail closed with an explicit ambiguous-identifier error (no precedence fallback).

Additional confirmed decisions (2026-02-25):
- Top-level statements support arbitrary return expressions (not only model-started pipelines); final statement can return scalar/object/list payloads.
- `let` variables are materialized as scalar or single-column scalar-list values for cross-statement use; no persisted rowset-handle variable kind is exposed across statement boundaries.
- Variable memory behavior uses deterministic thresholds with spill-backed materialization as the default over hard-fail-on-size.
- Multi-statement mutation counters remain aggregated across statements, while response rows remain final-statement-only.
  - Error payloads prioritize immediate user comprehension with a clear `message` plus deterministic context (`statement_index`, `phase`, `code`, `path`, `line`, `col` when available).
- Newly completed:
  - Spill-backed `let` list materialization for rowsets exceeding in-memory variable list capacity.
  - Cross-statement `in(field, var_list)` correctness when `var_list` is spill-backed.
  - Deterministic spill-read failure propagation in predicate evaluation paths (including mutation predicates).
  - Deterministic spill write-fault fail-closed behavior with successful subsequent recovery request.

### Error Contract (clarity-first)
Canonical shape:
`ERR query: message="<human-readable explanation>" statement_index=<n> phase=<parse|semantic|execution|mutation> code=<Code> path=<context> line=<line> col=<col>`

Guidelines:
- `message` must stand alone and explain exactly what is wrong using user source terms.
- Include a compact source snippet for semantic/execution errors when available.
- Keep machine-stable fields deterministic for tests (`statement_index`, `phase`, `code`, `path`, `line`, `col`).

Examples:
- `ERR query: message="\"a\" is undefined in \"where(a == 1)\"; define it with let before use" statement_index=1 phase=semantic code=UndefinedVariable path=where line=2 col=21`
- `ERR query: message="\"id\" is ambiguous in \"where(id == 1)\"; it matches both column and let variable" statement_index=2 phase=semantic code=AmbiguousIdentifier path=where line=3 col=15`
- `ERR query: message="variable \"user_ids\" has type list<string> but in(id, user_ids) expects list<i64>" statement_index=3 phase=semantic code=VariableTypeMismatch path=where.in line=4 col=24`

## Phase 1: Language and AST Contracts
### Scope
- Define variable value kinds:
  - scalar (`u64`, `i64`, `bool`, `string`, `timestamp`, `null`)
  - list of scalars (single-typed, nullable policy explicit)
  - transient rowset (internal, statement-local only; must be materialized to scalar/list before binding into request-scope variable store)
- Define statement boundary rules and diagnostics.
- Add explicit parse/semantic errors for:
  - undefined variable
  - duplicate variable name in same request
  - ambiguous identifier resolution (column vs `let` variable name collision)
  - invalid variable type in predicate context
  - variable value/type incompatibility for declared usage context

### Gate
- Parser/semantic tests prove deterministic diagnostics for all invalid forms.
- Status: Complete (parser/AST changes and tests merged).

## Phase 2: Execution Engine for Statement Lists
### Scope
- Replace `findPipeline` single-statement path with statement iterator.
- Introduce request execution context with variable store and deterministic memory bounds.
- Execute statements in source order; capture per-statement stats and final response policy.
- Ensure `let` does not produce rows directly; it only mutates variable state.
- Shared snapshot: every statement in the request sees the same baseline snapshot plus in-request writes.

### Gate
- Multi-statement read-only request executes all statements in order.
- `let` values can be consumed by later statements in the same request.
- Read-your-own-writes is deterministic across statement boundaries.
- Status: Complete.

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
- Status: Complete.

## Phase 4: Protocol and Result Shaping
### Scope
- Decide and implement response format:
  - final statement result only (confirmed default)
- Add clarity-first message + deterministic metadata (`statement_index`, `phase`, `code`, `path`, `line`, `col`) to serialized errors.
- Preserve backwards-compatible one-statement output shape where possible.

### Gate
- Session tests prove stable wire output for success and failure across multi-statement requests.
- Status: Complete.

## Phase 5: Variable Materialization Under Spill-Ready Runtime
### Scope
- Define deterministic in-memory limits for variable materialization.
- Use Workfront 03 spill/temp mechanisms when variable materialization exceeds in-memory thresholds.
- Preserve deterministic execution and deterministic error classification for storage hard failures.

### Gate
- Deterministic tests verify:
  - bounded in-memory behavior
  - deterministic spill behavior for variable materialization
  - deterministic hard-failure behavior for spill storage faults
- Status: Complete.

## Phase 6: Feature Test Matrix (One File Per Capability)
Create dedicated files under `test/features/variables_and_multi_statement/`:
1. `let_test.zig`
2. `let_list_from_query_test.zig` (covered in `let_test.zig`)
3. `multi_statement_read_chain_test.zig`
4. `multi_statement_mutation_chain_test.zig`
5. `multi_statement_atomic_rollback_test.zig`
6. `undefined_variable_test.zig`
7. `duplicate_variable_test.zig`
8. `invalid_variable_type_usage_test.zig`
9. `statement_error_index_test.zig`
10. `response_shape_multi_statement_test.zig`
11. `shared_snapshot_read_your_writes_test.zig`
12. `variable_memory_boundaries_test.zig`

Current coverage implemented:
- `let_test.zig`
- `multi_statement_read_chain_test.zig`
- `multi_statement_mutation_chain_test.zig`
- `multi_statement_atomic_rollback_test.zig`
- `undefined_variable_test.zig`
- `duplicate_variable_test.zig`
- `invalid_variable_type_usage_test.zig`
- `statement_error_index_test.zig`
- `response_shape_multi_statement_test.zig`
- `shared_snapshot_read_your_writes_test.zig`

### Gate
- Feature suite passes under `zig build test`.
- At least one deterministic fault/recovery scenario added under `test/internals/` for rollback correctness.
- Status: Complete (`variable_memory_boundaries_test.zig` and `variable_spill_fault_test.zig` added and passing).
