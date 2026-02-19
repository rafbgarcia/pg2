# Phase 3: Query Layer — Implementation Plan

## Context

Phase 1 (foundation) and Phase 2 (storage engine) are complete. The storage layer provides: 8KB pages with CRC checksums, a buffer pool with clock-sweep eviction, WAL with crash recovery, B+ tree indexes, slotted-page heap storage, and undo-log MVCC with snapshot isolation. Phase 3 builds the query layer on top: parser, catalog, and executor.

### Milestone 5: Sort + Aggregation + Joins

**`src/executor/sort.zig`** — Materializes into arena, sorts in-place.

**`src/executor/aggregate.zig`** — Grouping and accumulation.

**`src/executor/join.zig`** — Runtime-adaptive join for nested relations.

### Milestone 6: Integration + End-to-End Tests

- Update `src/pg2.zig` with new module re-exports and comptime test discovery
- End-to-end test: parse schema → load catalog → parse query → execute against SimulatedDisk → verify results and stats
- Test the full query examples from the language spec:
  - basic scan
  - pipeline
  - nested relations + scopes
  - aggregation
  - mutation
  - Let bindings, index access, computed fields

## Key Design Decisions

1. **Flat AST with node indices** — not pointers. Fixed-capacity array. Nodes link via `NodeIndex` (u16). Bounded, copyable, no heap allocation after init.

2. **Shunting-yard for expressions** — not Pratt parsing (which is recursive). Two explicit stacks (operator + output), single iterative loop. Follows no-recursion rule.

3. **Explicit nesting stack for selection sets** — instead of recursive descent. `max_nesting_depth = 16`. Parser pushes context when entering `{ ... }`, pops on `}`.

4. **Row encoding as storage-layer module** — `src/storage/row.zig` bridges typed Values and raw heap bytes. Available to both catalog (schema) and executor (row access).

5. **Catalog in its own directory** — neither storage nor parser nor executor. All three depend on it. Fixed-capacity, sealed after schema loading.

6. **Iterator-style execution but no virtual dispatch** — executor pre-plans a flat array of `OpDescriptor` entries, runs them in sequence. Avoids both recursion and dynamic dispatch.

## Verification

After each milestone:

```bash
zig build test    # All inline tests pass (existing + new)
zig build         # Clean build, no warnings
```
