# Milestone Tracker: String Overflow + Auto-Compaction Foundation

Last updated: 2026-02-20
Owner: Codex session (handoff-ready)

## Scope

Build a production-grade string storage path that keeps common text fast inline while safely handling large values:

1. Auto page compaction on update/insert shortfall (foundation already started).
2. Hybrid string storage:
   - Inline small strings.
   - Spill large strings to overflow pages.
3. Deterministic and crash-safe reclaim for replaced/deleted overflow chains.

## Confirmed Product Decisions

These are confirmed in chat with the user:

- Encoding: UTF-8.
- Inline threshold target for `string`: 1024 bytes.
- Overflow model: single overflow chain per spilled field value.
- Reclaim policy: immediate logical unlink + deterministic reclaim pipeline (not synchronous full physical reclaim inside mutation path).

## Clarification to Carry Forward

- "String has no declared max" remains a logical type contract.
- Physical storage policy is separate and uses byte thresholds, not character counts.
- 1024 is bytes, so number of characters depends on UTF-8 code points.

## Current Code Status (This Session)

### Implemented in committed chunk `5f07acf`

- Added deterministic compaction primitives:
  - `HeapPage.fragmented_bytes`
  - `HeapPage.compact`
  - internal threshold helper `maybe_compact_for_required_space`
- Wired auto-compaction retry into:
  - `HeapPage.insert` when contiguous free space is insufficient.
  - `HeapPage.update` growth path when contiguous free space is insufficient.
- Added heap tests for:
  - update auto-compacts when fragmented bytes cover growth shortfall.
  - insert auto-compacts when fragmented bytes cover insert shortfall.

### Implemented in committed chunk `73e509b`

- Overflow storage foundation:
  - Added page type `.overflow` in `src/storage/page.zig`.
  - Added `src/storage/overflow.zig` with:
    - versioned overflow page header (`magic/version`),
    - single-chunk payload + next-page pointer model,
    - deterministic tests for init, roundtrip, capacity bounds, and corrupt format rejection.
  - Wired module into test discovery via `src/pg2.zig`.

### Implemented in committed chunk `a3970a5`

- Bounded string materialization arena:
  - Added per-query bounded string arena bytes in runtime bootstrap:
    - `BootstrapConfig.query_string_arena_bytes_per_slot` (default 4 MiB).
    - Query buffers now include `string_arena_bytes`.
  - Added bounded `StringArena` in `src/executor/scan.zig`; scan decode copies strings into arena-backed memory.
  - `ScanResult` now owns string storage for allocator-returning scan APIs (`tableScan`, `indexRange`) to guarantee lifetime safety.
  - Executor read path threads per-query arena to `tableScanInto` calls (including nested relation scans).

### Implemented in current increment (`<pending-commit>`)

- Dedicated overflow page-id region allocator:
  - Added deterministic allocator state in `src/storage/overflow.zig` with default dedicated region bounds, ownership checks, and fail-closed `RegionExhausted`.
  - Catalog now owns allocator state (`catalog.overflow_page_allocator`) so allocation is explicit and deterministic across mutation/read paths.
- Row format inline-vs-overflow pointer encoding:
  - Bumped row format version to v2 (`src/storage/row.zig`).
  - String fixed slot now encodes explicit storage kind:
    - inline tag + in-row var-data offset, or
    - overflow tag + first overflow page id.
  - Added `decodeColumnStorageChecked` to expose overflow references to higher layers.
  - Kept legacy v1 inline-string decode compatibility for already-encoded rows.
- Insert/update spill path with 1024-byte threshold:
  - Mutation path now marks oversized strings (>1024 bytes) for spill.
  - Overflow chains are written into dedicated region pages and row encoding stores overflow pointers.
  - Added bounded pre-check for update fit before spill to fail closed on page-capacity shortfall.
- Read-path overflow resolution into bounded arena:
  - Scan decode now resolves overflow chains into `StringArena` using bounded per-query bytes.
  - Overflow chain traversal is fail-closed on:
    - out-of-region page ids,
    - unexpected page type/format,
    - excessive traversal hops (cycle/corruption guard).
- Deterministic tests added:
  - Row encode/decode coverage for overflow pointer slot and legacy v1 compatibility.
  - Mutation/scan roundtrip for spilled insert and spilled update.
  - Deterministic overflow-region exhaustion rejection.

## Known Test State

- `zig build test` passes for this increment.

## Next Logical Chunk

1. Confirm overflow page-id allocation strategy:
   - dedicated page-id region (recommended),
   - global free-list allocator,
   - per-model region allocator.
2. Row format integration:
   - Extend string fixed-slot encoding to support inline vs overflow-pointer variants.
   - Keep backwards-compatible row format behavior decisions explicit (likely version bump).
3. Mutation path integration:
   - On insert/update, apply 1024-byte inline policy for strings.
   - Allocate/write overflow pages and set in-row pointers when spilling.
4. Durability integration:
   - Define WAL payload contract for overflow chain create/relink/unlink.
   - Add deterministic crash/fault tests for spill + update + unlink/recover.

## Open Design Confirmation Needed Before Overflow Coding

Confirmed: use same DB storage file with distinct overflow page type/region (not a separate OS file) for v1.
Confirmed: overflow page-id allocation strategy is `dedicated page-id region` for v1.

## Next-Session Kickoff (Concrete)

Completed in this increment (all items 1..6). Next focus should move to:

1. Overflow reclaim pipeline:
   - Define deterministic unlink + reclaim queue behavior for replaced/deleted overflow chains.
2. WAL durability contract for overflow chain lifecycle:
   - Define create/relink/unlink redo+undo payload contract.
3. Crash/restart deterministic fault matrix:
   - Add spill/update/delete crash points and recovery assertions.

## Fresh Codex Handoff Commands

Use these first in a new session:

1. `git status --short`
2. `zig build test`
3. `rg -n "overflow|StringArena|query_string_arena_bytes_per_slot" src/storage src/executor src/runtime`
4. Continue from "Next Logical Chunk" above.
