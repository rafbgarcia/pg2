# Workfront 10: Iterator Execution Model

## Objective

Evolve the executor from batch-based operator calls on fixed buffers to a composable pull-based iterator (Volcano) model, enabling streaming execution and complex query features.

## Why This Matters

The current executor orchestrates operators by calling functions on flat `[]ResultRow` buffers in sequence (`applyPostScanOperators` in `executor.zig`). This works for the current feature set (scan → filter → group → sort → limit → project) but has structural limits:

1. **Operator composition is manual.** Adding a new operator means modifying the executor's switch-case dispatch and carefully threading buffers. There's no general way to compose operators into arbitrary pipelines.
2. **Streaming is impossible.** Each operator must fully materialize its output before the next operator starts. A `LIMIT 10` on a sorted 1M-row table still sorts all 1M rows before returning 10.
3. **Complex features need nested pipelines.** Subqueries, CTEs, window functions, and UNION all require operators that consume from sub-pipelines — this doesn't fit the "call functions in sequence on a shared buffer" model.

## When to Execute

**Not urgent.** The batch model is correct and adequate for the current scope (Workfronts 01-09). This workfront becomes necessary when any of these features are prioritized:

- Subqueries in SELECT, WHERE, or FROM (correlated or uncorrelated)
- Common Table Expressions (WITH clauses)
- Window functions (ROW_NUMBER, RANK, LEAD/LAG, etc.)
- UNION / INTERSECT / EXCEPT
- Streaming LIMIT optimization (stop pulling after N rows)

## Entry Gate (Sequential Lane)

Start this workfront only when at least one trigger feature above is committed as an active product goal. If no trigger feature is active, treat this workfront as deferred and do not preempt higher-priority foundational workfronts.

## Design Direction

### Pull-Based Operator Interface

Each operator implements a common interface:

```
Operator {
    /// Open the operator (initialize state, open children).
    fn open(self: *Operator, ctx: *ExecContext) Error!void;

    /// Pull the next row. Returns false when exhausted.
    fn next(self: *Operator, out: *ResultRow, arena: *StringArena) Error!bool;

    /// Close the operator (release resources, close children).
    fn close(self: *Operator) void;
}
```

Operators form a tree: a SortOperator pulls from a FilterOperator which pulls from a ScanOperator. The root operator feeds into result serialization.

### Spill Integration

Operators that spill (sort, hash aggregate, hash join) buffer internally using temp pages, same as Workfront 03's architecture. The iterator interface doesn't change the spill mechanism — it changes how operators *compose*, not how they manage memory.

### Migration Strategy

The batch-based fast path (small results, no spill) should remain as an optimization. The iterator model activates for queries that need it (complex pipelines, streaming LIMIT). This avoids regressing simple query latency.

### Compatibility with Existing Workfronts

- **WF03 (spill)**: Spill-capable operators (external sort, hash aggregate, hash join) can be wrapped as iterators without changing their internal spill logic.
- **WF07 (adaptive planning)**: The planner would produce an operator tree instead of a flat operator list. Plan inspection (`INSPECT`) shows the tree structure.
- **WF09 (module decomposition)**: Operator extraction into separate files (already planned) is a prerequisite — each file becomes an iterator implementation.

## Phases (Tentative)

### Phase 1: Operator Interface and Leaf Operators
- Define the `Operator` interface (Zig tagged union or vtable).
- Implement `ScanOperator` (wraps chunked table scan), `FilterOperator` (wraps WHERE evaluation).
- Existing batch path remains the default. Iterator path is opt-in for testing.

### Phase 2: Full-Input Operators as Iterators
- Wrap sort, group, join as iterators. They pull all input during `open()`, then emit rows via `next()`.
- This is structurally identical to the batch model but uses the iterator interface.

### Phase 3: Streaming Optimizations
- `LimitOperator` stops pulling after N rows (no full materialization).
- `ProjectOperator` transforms rows on-the-fly.
- Pipeline breakers (sort, group) remain as-is; only non-blocking operators benefit from streaming.

### Phase 4: Nested Pipelines
- Subquery operators (pull from a sub-pipeline).
- CTE materialization (shared scan across multiple references).
- Window function operators.

## Non-Goals for This Workfront

- Parallel execution (multiple threads pulling from partitioned operators). This is a separate concern.
- Vectorized execution (columnar batches instead of row-at-a-time). Possible future optimization but orthogonal to the iterator model.
