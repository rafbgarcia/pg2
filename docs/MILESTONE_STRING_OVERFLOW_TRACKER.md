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

### Not implemented yet

- No overflow pointer integration in row format yet.
- No reclaim queue/GC path yet.
- No mutation/WAL integration for overflow chains yet.

## Known Test State

- `zig build test` passes for committed auto-compaction increment.
- `zig build test` also passes after overflow page foundation module changes in this in-progress chunk.

## New In-Progress Chunk: Overflow Storage Foundation

Implemented (not yet committed in this tracker section):

- Added page type `.overflow` in `src/storage/page.zig`.
- Added `src/storage/overflow.zig` with:
  - versioned overflow page header (`magic/version`),
  - single-chunk payload + next-page pointer model,
  - deterministic tests for init, roundtrip, capacity bounds, and corrupt format rejection.
- Wired module into test discovery via `src/pg2.zig`.

## Next Logical Chunk

1. Commit overflow storage foundation with Tiger artifact.
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

## Fresh Codex Handoff Commands

Use these first in a new session:

1. `git status --short`
2. `zig build test`
3. `rg -n "compact|fragmented_bytes|maybe_compact_for_required_space" src/storage/heap.zig`
4. Continue from "Next Logical Chunk" above.
