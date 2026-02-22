# Workfront 09: Module Decomposition

## Objective
Split oversized source modules into focused, single-responsibility files to reduce cognitive load, improve testability, and lower per-session token cost when working with the codebase.

## Non-Negotiables
1. No behavioral changes — pure mechanical extraction. Every existing test must pass unmodified after each phase.
2. No circular dependencies introduced. Every new module must slot into a clean acyclic dependency graph.
3. Inline tests move with the code they exercise. `test/features/` and `test/internals/` tests remain untouched.
4. Each phase is a single commit that compiles and passes `zig build test` before the next phase starts.

## Phase 1: Executor — Operator Extraction ✅
### Scope
Extract algorithm-heavy operator implementations out of `src/executor/executor.zig` (2770 prod lines, 8+ concerns):

- `src/executor/aggregation.zig` — `GroupRuntime`, `AggregateState`, `AggregateDescriptor`, `applyGroup()`, aggregate state machine, group key matching (~840 lines).
- `src/executor/sorting.zig` — `SortKeyDescriptor`, `applySort()`, insertion sort, multi-key comparison (~780 lines).
- `src/executor/joins.zig` — `JoinDescriptor`, `executeInnerJoinBounded()`, `executeLeftJoinBounded()` (~160 lines).
- `src/executor/projections.zig` — `ProjectionDescriptor`, `applyFlatColumnProjection()`, nested selection helpers (~160 lines).

After extraction `executor.zig` retains orchestration, `ExecContext`, `QueryResult`, stats, and operator dispatch (~800 lines).

### Gate
- `zig build test` passes with zero test modifications.
- No new public API — extracted modules are `@import`ed only by `executor.zig`.

## Phase 2: Mutation — Subsystem Extraction ✅
### Scope
Extract distinct subsystems out of `src/executor/mutation.zig` (2545 prod lines, 6 concerns):

- `src/executor/overflow_chains.zig` — Overflow page read/write/reclaim lifecycle, WAL integration, `OverflowChainStats` (~340 lines).
- `src/executor/referential_integrity.zig` — FK validation, cascade DELETE/UPDATE, referencing row queries (~350 lines).
- `src/executor/value_builder.zig` — Expression evaluation for assignments, type coercion, defaults, parameter binding (~390 lines).
- `src/executor/constraints.zig` — PK/unique index enforcement (~80 lines).

After extraction `mutation.zig` retains `executeInsert/Update/Delete` orchestration and MVCC/undo integration (~1250 lines).

### Gate
- `zig build test` passes with zero test modifications.
- No new public API — extracted modules are `@import`ed only by `mutation.zig`.

## Phase 3: B-Tree — Page and Split Extraction ✅
### Scope
Extract page-level and split logic out of `src/storage/btree.zig` (1238 prod lines, 5 concerns):

- `src/storage/btree_page.zig` — `LeafNode`, `InternalNode` structs and all cell-level operations (~350 lines).
- `src/storage/btree_split.zig` — `splitAndInsert()`, `insertIntoParent()`, `splitInternal()`, `splitRoot()`, split helpers (~260 lines).

After extraction `btree.zig` retains `BTree` struct, find/insert/delete/rangeScan, `RangeScanIterator` (~600 lines).

### Gate
- `zig build test` passes with zero test modifications.
- No new public API — extracted modules are `@import`ed only by `btree.zig`.

## Phase 4: Filter — Operation Extraction
### Scope
Extract operator families out of `src/executor/filter.zig` (1132 prod lines, 6 concerns):

- `src/executor/numeric_ops.zig` — Arithmetic, overflow checks, type coercion, numeric comparison (~250 lines).
- `src/executor/builtin_functions.zig` — Scalar function dispatch and string helpers (~170 lines).

After extraction `filter.zig` retains the stack-based expression evaluator, AST traversal, context types, and logical operations (~400 lines).

### Gate
- `zig build test` passes with zero test modifications.
- No new public API — extracted modules are `@import`ed only by `filter.zig`.

## Phase 5: Session — Protocol Extraction
### Scope
Extract serialization logic out of `src/server/session.zig` (941 prod lines, 5 concerns):

- `src/server/tree_protocol.zig` — `TreeProjection`, `SelectionEntry`, nested relation serialization, row grouping, shape output (~300 lines).
- `src/server/serialization.zig` — `serializeQueryResult()`, `serializeInspectStats()`, value formatting, plan explanation helpers (~150 lines).

After extraction `session.zig` retains `Session` struct, `handleRequest()`, `serveConnection()`, error types (~120 lines).

### Gate
- `zig build test` passes with zero test modifications.
- No new public API — extracted modules are `@import`ed only by `session.zig`.
