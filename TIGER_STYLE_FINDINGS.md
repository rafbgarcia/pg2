# Tiger Style Findings Tracker

## Context

This project is mid-implementation and we are intentionally investing early in Tiger Style hardening to avoid building features on top of weak foundations.
The goal is to make correctness, determinism, and recovery guarantees first-class now, so later phases (server/replication/query features) inherit strong baseline behavior instead of requiring risky rewrites.

This tracker exists to keep continuity across sessions:
- Preserve why each hardening task matters.
- Show what is already completed vs what is still open.
- Guide next work toward highest-leverage production safety improvements.

## How To Use This Tracker

- Treat `done` items as landed baseline.
- Treat `partial` items as active migrations that should be finished before adding adjacent complexity.
- Prioritize `pending` items that affect durability/corruption/recovery boundaries first.
- When a task is completed, update status and add concrete file refs for traceability.

## Completed So Far

1. WAL crash recovery metadata/envelope implemented.
Status: `done`
Notes: Added durable recovery envelope with magic/version/checksum and `recover()` flow.
Refs: `src/storage/wal.zig`

2. Row decode corruption hardening implemented.
Status: `done`
Notes: Added checked decode APIs (`decodeColumnChecked` / `decodeRowChecked`) with bounds validation and `error.Corruption`.
Refs: `src/storage/row.zig`, `src/executor/scan.zig`

3. Global Tiger error taxonomy scaffold implemented.
Status: `partial`
Notes: Added `ErrorClass` and mappers for buffer pool / scan / mutation / tx manager / WAL.
Refs: `src/tiger/error_taxonomy.zig`
Remaining: enforce mapping coverage for all public subsystem errors and wire at boundaries.

4. `unreachable` cleanup in touched runtime code.
Status: `partial`
Notes: Replaced key runtime `unreachable` sites with explicit panic/error handling in touched files.
Refs: `src/storage/btree.zig`, `src/storage/row.zig`, `src/simulator/main.zig`
Remaining: full codebase sweep.

5. WAL partial-page read error swallowing fixed.
Status: `done`
Notes: Replaced silent catch with explicit WAL read error path.
Refs: `src/storage/wal.zig`

## Remaining Findings

1. Schema name buffer overflow guard missing.
Status: `done`
Refs: `src/storage/row.zig` (`RowSchema.addColumn`)

2. B-tree structural decode/boundary validation incomplete.
Status: `done`
Refs: `src/storage/btree.zig` node read helpers
Notes: Added structural validation checks and explicit `error.Corruption` return paths before B-tree traversal/scan reads; expanded malformed-page tests for bad leaf pointers and internal key-length overflows.

3. Unbounded B-tree traversal loops need explicit limits.
Status: `done`
Refs: `src/storage/btree.zig` traversal loops
Notes: Added bounded root-to-leaf depth guards (`max_btree_depth`) and sibling-hop caps (`max_leaf_sibling_hops`) with corruption regressions for over-depth traversal and cyclic sibling chains.

4. Checked integer arithmetic in executor expression engine incomplete.
Status: `done`
Refs: `src/executor/filter.zig`
Notes: Replaced unchecked integer arithmetic with checked ops and explicit `error.NumericOverflow` handling (binary arithmetic, unary negate, `abs`), with regression tests for overflow edges.

5. Persistent format versioning still incomplete outside WAL envelope.
Status: `done`
Refs: `src/storage/page.zig` and broader on-disk structs
Notes: Added page-header on-disk format metadata (`format_version` + `format_magic`) with explicit deserialize validation and incompatibility tests; added row payload format header (`magic` + `version`) with checked decode validation tests; added heap slotted-header format metadata and B-tree leaf/internal payload format metadata with corruption-path validation tests.

6. Static allocation/allocator sealing policy not yet enforced.
Status: `partial`
Refs: runtime alloc paths across executor/storage/mvcc
Notes: Removed hot-path dynamic allocator usage from B-tree split logic by using fixed-capacity split scratch buffers derived from page-size bounds; removed `BTree` allocator dependency from runtime API/state after split-path sealing (`src/storage/btree.zig`).
Remaining: apply similar allocator-sealing strategy across executor result buffering, WAL decode ownership, and other core runtime allocation paths.

7. Deterministic fault injection matrix incomplete.
Status: `partial`
Refs: `src/simulator/disk.zig`, simulation tests
Notes: Added deterministic one-shot fault injection controls for Nth read/write/fsync in `SimulatedDisk` with regression tests for each path.
Remaining: expand matrix to partial writes/bitflip corruption and end-to-end recovery scenarios using seeded schedules.

## Current Build State

- `zig build test` passing after the completed changes.

## Session Handoff Template

Use this at the end of each Codex session to keep continuity.

```
### Session Handoff - YYYY-MM-DD

- Goal this session:
- Completed:
  - 
- In progress:
  - 
- Blockers / decisions needed:
  - 
- Tests run:
  - 
- Next recommended step:
  1. 
```
