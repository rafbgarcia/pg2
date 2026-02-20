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

### Implemented in committed chunk `76598c4`

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

### Implemented in committed chunk `c5548a0`

- Deterministic overflow reclaim pipeline:
  - Added catalog-owned deterministic reclaim queue (`catalog.overflow_reclaim_queue`).
  - Update/delete paths now:
    - decode old-row overflow roots,
    - append logical unlink records,
    - enqueue reclaim work deterministically,
    - drain reclaim with fixed per-mutation budget (1 chain).
  - Reclaim rewrites overflow pages back to `.free` with zeroed content.
- Overflow WAL lifecycle contract:
  - Added explicit WAL record types for overflow lifecycle:
    - `overflow_chain_create`
    - `overflow_chain_relink`
    - `overflow_chain_unlink`
    - `overflow_chain_reclaim`
  - Chain metadata payload contract is explicit and bounded for create/unlink/reclaim.
  - Relink metadata payload contract is explicit and bounded for row-pointer publication events.
- Crash/restart deterministic coverage:
  - Added mutation deterministic tests for:
    - replace path WAL ordering,
    - delete path unlink/reclaim ordering,
    - crash + restart WAL recover decode coverage for spill/replace/delete lifecycle.
  - Added server-path E2E coverage for overflow insert/update/read and delete.
- Malformed/extreme hardening:
  - Reclaim traversal is fail-closed on:
    - out-of-region ids,
    - non-overflow page type in chain,
    - malformed page format,
    - excessive hop count / cyclic chain behavior.
  - Added deterministic corruption test for cyclic overflow chain reclaim.

### Implemented in committed chunk `pending commit`

- Overflow reclaim observability through session inspect:
  - Added catalog-owned reclaim counters:
    - `enqueued_total`
    - `dequeued_total`
    - `reclaimed_chains_total`
    - `reclaimed_pages_total`
    - `reclaim_failures_total`
  - Added `Catalog.snapshotOverflowReclaimStats()` with queue-depth snapshot.
  - Session inspect output now includes:
    - `INSPECT overflow reclaim_queue_depth=... reclaim_enqueued_total=... reclaim_dequeued_total=... reclaim_chains_total=... reclaim_pages_total=... reclaim_failures_total=...`
- Reclaim lifecycle wiring:
  - Overflow unlink/reclaim enqueue/dequeue/success/failure paths now update deterministic counters.
- Test and docs coverage:
  - Added server-path E2E spec for backlog depth + throughput counters.
  - Added catalog unit test for reclaim stats snapshot semantics.
  - Updated query/user-facing docs for the additional inspect line.

## Known Test State

- `zig build test` passes for this increment (including overflow reclaim and new E2E overflow tests).

## Next Logical Chunk

1. Durable replay integration:
   - Integrate overflow lifecycle records into a full data-page WAL replay path (not only WAL envelope+decode recovery).
2. Tx-level abort semantics:
   - Define and test overflow lifecycle behavior under transaction abort/rollback with explicit undo/reclaim ordering.
3. Queue-drain budget semantics:
   - Define/document expected backlog progression when one mutation unlinks multiple overflow chains.

## Next-Session Kickoff (Concrete)

Completed in committed chunks `c5548a0` and `pending commit`:

1. Overflow reclaim pipeline.
2. WAL lifecycle contract for create/relink/unlink/reclaim.
3. Deterministic crash/restart coverage for spill/replace/delete.
4. Malformed/cyclic chain fail-closed reclaim coverage.
5. Inspect-level reclaim backlog/throughput visibility.

Next session should move to:

1. Integrate overflow lifecycle WAL into full page replay/recovery.
2. Add tx-abort lifecycle tests for overflow create/relink/unlink/reclaim.
3. Define/test explicit reclaim-drain budget semantics for multi-overflow-field mutations.

## Fresh Codex Handoff Commands

Use these first in a new session:

1. `git status --short`
2. `zig build test`
3. `git log -2 --stat`
4. `git show --name-only --stat c5548a0`
5. `git show --name-only --stat pending_commit_sha`
6. `rg -n "INSPECT overflow|overflow_reclaim_stats|snapshotOverflowReclaimStats|overflow_reclaim_queue" src docs user-facing-docs`
7. Continue from "Next Logical Chunk" above.
