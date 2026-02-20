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

### Implemented in `src/storage/heap.zig`

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

- No overflow storage structures yet (no pointer format, no overflow page type, no reclaim queue).
- No new server E2E coverage for compaction threshold path in this chunk.
- No Tiger artifact/doc update for this compaction threshold increment yet.

## Known Test State

- `zig build test` passed after heap changes before adding large E2E stress case.
- A large E2E stress test in `src/server/e2e/update.zig` was removed because it hit existing runtime memory ceilings (`OutOfMemory`) unrelated to heap compaction correctness.
- Last interrupted run happened during a fresh `zig build test`; re-run required for final confirmation in next chunk.

## Next Logical Chunk

1. Re-run full test suite:
   - `zig build test`
2. If green:
   - Write Tiger artifact for this increment (core DB code changed in `src/storage`).
   - Update `docs/tiger-gates/README.md`.
   - Update readiness/progress tracking docs.
   - Commit as one increment.
3. Start overflow implementation design+scaffold:
   - Define overflow pointer encoding in row format.
   - Define overflow page/chunk format and version fields.
   - Define WAL record payload contract for overflow create/relink/unlink.
   - Add deterministic crash/fault tests before feature completion.

## Open Design Confirmation Needed Before Overflow Coding

Confirm one storage-layout decision before implementation:

- Overflow pages as:
  1. Separate page type/segment inside same DB storage file, or
  2. Separate physical file.

Recommendation for v1: option 1 (same file, distinct page type/region) for lower operational complexity and tighter recovery semantics.

## Fresh Codex Handoff Commands

Use these first in a new session:

1. `git status --short`
2. `zig build test`
3. `rg -n "compact|fragmented_bytes|maybe_compact_for_required_space" src/storage/heap.zig`
4. Continue from "Next Logical Chunk" above.

