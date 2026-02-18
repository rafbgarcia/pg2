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
   Notes: Removed hot-path dynamic allocator usage from B-tree split logic by using fixed-capacity split scratch buffers derived from page-size bounds; removed `BTree` allocator dependency from runtime API/state after split-path sealing (`src/storage/btree.zig`). Added bounded no-allocation WAL decode path `readFromInto` with caller-owned record/payload buffers and explicit capacity errors (`src/storage/wal.zig`), reducing recovery-path dependence on heap-owned record payload copies.
   Remaining: migrate runtime call sites to the sealed decode path by default, then apply similar allocator-sealing strategy across executor result buffering and other core runtime allocation paths.

7. Deterministic fault injection matrix incomplete.
   Status: `partial`
   Refs: `src/simulator/disk.zig`, simulation tests
   Notes: Added deterministic one-shot fault injection controls for Nth read/write/fsync plus partial-write and bitflip-on-write corruption in `SimulatedDisk` with regression tests for each path; added buffer-pool propagation tests validating deterministic `StorageRead`/`StorageWrite`/`StorageFsync` error surfacing; added WAL end-to-end torn-write recovery regression (`recover` + `readFrom`) and a new seeded fault-matrix module covering multi-step replay-deterministic scenarios (partial WAL write + crash + recover, data-page bitflip + checksum detection, WAL fsync failure + WAL-gated page flush).
   Remaining: broaden seeded matrix coverage to more interleavings and longer schedules.

## Current Build State

- `zig build test` passing after the completed changes.

### Session Handoff - 2026-02-18

- Goal this session:
  - Continue Tiger-style hardening by moving deterministic fault testing from single-fault injection toward seeded multi-step scenarios, then advance allocator sealing in WAL decode paths.
- Completed:
  - Added seeded replay-deterministic fault-matrix scenarios in `src/simulator/fault_matrix.zig` for:
    - partial WAL write + crash + recover
    - data-page bitflip + checksum enforcement
    - WAL fsync failure + WAL-gated page flush behavior
  - Wired fault-matrix module into test discovery (`src/pg2.zig`).
  - Extended `SimulatedDisk` with deterministic one-shot `partialWriteAt` and `bitflipWriteAt` injection APIs and tests (`src/simulator/disk.zig`).
  - Added WAL torn-write recovery regression (`src/storage/wal.zig`).
  - Added bounded no-allocation WAL decode API `readFromInto` with explicit `RecordBufferTooSmall` / `PayloadBufferTooSmall` errors and tests (`src/storage/wal.zig`).
  - Updated WAL error mappings in taxonomy/boundary adapters (`src/tiger/error_taxonomy.zig`, `src/executor/mutation.zig`, `src/storage/btree.zig`).
- In progress:
  - Allocator-sealing migration is still partial at the system level; runtime paths now have a sealed WAL decode option but call-site migration is not complete.
- Blockers / decisions needed:
  - Decide when to switch runtime recovery/read paths to use `readFromInto` by default (and define fixed capacities/budgets for those call sites).
- Tests run:
  - `zig build test`
- Next recommended step:
  1. Migrate runtime WAL recovery/read callers to `readFromInto` with explicit bounded buffers and capacity contracts.
  2. Apply same sealing strategy to executor result buffering paths (replace dynamic growth with fixed arena/limits where possible).
  3. Expand seeded fault-matrix scenarios to longer interleavings with multiple injected faults per schedule.
