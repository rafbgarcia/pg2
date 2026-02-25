# Workfront 12: Storage Reclamation Without VACUUM

## Objective
Eliminate the need for a traditional background VACUUM process by reclaiming dead storage (tombstoned heap slots, orphaned overflow page chains, stale B+ tree entries) inline with normal transaction processing, using epoch-based or eager strategies that are deterministic, safe under concurrent snapshots, and require no background threads.

## Progress Update (2026-02-25)
- Phase 2 slot-reclamation plumbing is in place (queueing, commit/abort queue state, WAL `reclaim_slot`, heap reclaimed-slot reuse path).
- A rollback correctness flaw was identified and resolved by implementing physical heap rollback on abort from undo pre-images before abort maintenance.
- Commit/abort maintenance (`undo_log.truncate(...)` + `tx_manager.cleanupBefore(...)`) remains enabled at pool boundaries, with rollback no longer depending on retained aborted visibility history.
- Reactor pinning tests now assert post-cleanup terminal state (`!= .active`) instead of requiring retained `.aborted` state outside the tx-state retention window.

## Why
- Traditional VACUUM (as in PostgreSQL) is a major source of operational complexity: table bloat, autovacuum tuning, wraparound dangers, and unpredictable I/O spikes. pg2's mission is to eliminate this class of operational burden entirely.
- pg2 currently tombstones deleted heap slots (`Slot.deleted_len = 0` in `heap.zig`) but never reclaims them. Over time, tables accumulate dead slots that waste space and degrade scan performance.
- Overflow page chains (used for large strings, tracked via `string_overflow_page_id` in `row.zig`) are orphaned when their owning row is deleted. There is no free list to return these pages for reuse.
- B+ tree indexes do not remove entries for committed-deleted rows. This is correct for MVCC visibility (the heap handles it), but over time the index accumulates unbounded dead entries that degrade lookup and range scan performance.
- pg2 has no background threads by design (all work is inline or deterministically scheduled). Any reclamation strategy must fit this constraint.
- All behavior must be deterministic under simulation replay, ruling out approaches that depend on wall-clock timers or nondeterministic background scheduling.

## Dependencies
- **Workfront 11 (Write Performance)** must be completed first. It establishes B+ tree index maintenance patterns (insert/delete wiring, key encoding) that reclamation builds on.
- **Workfront 10 (Iterator Execution Model)** is not strictly required but would simplify opportunistic cleanup during scans if the iterator interface is available.

## Non-Goals
- Online table compaction (rewriting live rows into dense pages). This is a future optimization on top of slot reclamation.
- Page-level defragmentation within a heap page (compacting free space between live rows). Useful but separate.
- Automatic index rebuild or rebalancing. Dead entry cleanup is in scope; structural rebalancing is not.
- Multi-version index structures (e.g., Bw-tree). The B+ tree remains the sole index structure.
- Reclamation of catalog/schema pages. Only user data pages are in scope.

## Phase 1: Design Investigation and Decision

### Design Decisions
- **Survey the design space.** Document tradeoffs for each reclamation strategy in the context of pg2's constraints (no background threads, deterministic simulation, undo-log MVCC, snapshot isolation):
  - **Traditional VACUUM**: periodic background scan of all pages, reclaims dead tuples. Ruled out by the no-background-threads constraint, but documented as the baseline for comparison.
  - **Eager reclamation on commit/abort**: when a transaction commits a DELETE (or aborts an INSERT), immediately reclaim the dead slot if no active snapshot can see the old version. Simplest approach but may add latency to commit path.
  - **Epoch-based reclamation**: maintain a global epoch counter advanced when the oldest active snapshot changes. Dead rows are tagged with the epoch at which they became reclaimable. Any subsequent transaction whose epoch exceeds the tag can reclaim the slot inline. Amortizes cost but requires epoch bookkeeping.
  - **Opportunistic cleanup during scans**: when a table scan or index scan encounters a dead row that is invisible to all active snapshots, reclaim it on the spot. Piggybacks on existing I/O but only reclaims pages that are actually read.
  - **Hybrid**: combine epoch tagging with opportunistic scan cleanup and eager commit-time reclamation for the common case. This is the expected outcome but the investigation must confirm it.
- **Identify what needs reclaiming**, with per-resource analysis:
  - Heap slots: tombstoned rows from committed DELETEs. The slot's space can be reused for new inserts on the same page.
  - Overflow page chains: pages linked from deleted rows via `string_overflow_page_id`. Must be returned to a free list or page allocator.
  - B+ tree entries: index entries whose target row has been committed-deleted and is invisible to all snapshots.
  - Undo log entries: the existing ring-buffer truncation policy (`UndoLog` in `undo.zig`) already handles this via `oldest_active` tracking. Confirm it is sufficient or identify gaps.
- **Analyze pg2's current MVCC model** for reclamation safety:
  - `TxManager.oldest_active` provides the low-water mark: no snapshot can see rows deleted by transactions that committed before this ID.
  - The undo log's `row_heads` map links (page_id, slot) to undo chain entries. Reclamation must not free a slot while its undo chain is still needed.
  - Snapshot visibility (`Snapshot.isVisible`) determines whether a dead row's pre-image is still required by any active transaction.

### Scope
- Produce a design decision document (can be a section appended to this workfront or a standalone doc) with:
  - Evaluation of each strategy against pg2's constraints.
  - Chosen approach with rationale.
  - Reclamation safety invariant: formal statement of when a dead resource is safe to reclaim.
  - Impact on the commit/abort/scan hot paths (expected overhead).
  - Interaction with WAL (are reclamation actions WAL-logged? they must be for crash recovery).

### Gate
- Design decision documented and reviewed.
- Reclamation safety invariant formally stated.
- Chosen approach confirmed to be compatible with: no background threads, deterministic simulation, undo-log MVCC, snapshot isolation, WAL-based crash recovery.

## Phase 2: Heap Slot Reclamation

### Design Decisions
- **Reclamation trigger.** When a transaction commits a DELETE, the tombstoned slot is added to a per-page reclaimable list, tagged with the committing transaction's ID. A slot becomes reclaimable when `TxManager.oldest_active` exceeds the committing transaction's ID (meaning no active snapshot can see the deleted row's pre-image).
- **Reclamation site.** Slot reclamation happens at well-defined points in the transaction lifecycle: (a) at commit time for the deleting transaction's own slots if no concurrent snapshots exist, and (b) at the start of any subsequent transaction that touches the same page, checking the reclaimable list against the current `oldest_active`.
- **Slot reuse.** A reclaimed slot's space is made available for new row inserts on the same page. The slot entry in the page header is marked as free (available for reuse by `HeapPage.insertRow`). This requires extending the heap page format to distinguish between "never used" and "reclaimed" slots, or simply allowing insert to reuse any slot with `deleted_len`.
- **Undo log interaction.** Before reclaiming a slot, verify that no undo chain entries reference it from a still-needed snapshot. The undo log's existing truncation based on `oldest_active` should ensure this, but the invariant must be explicitly checked.
- **Undo lifecycle hygiene.** Commit/abort boundaries must immediately run undo maintenance (`undo_log.truncate(tx_manager.getOldestActive())` and `tx_manager.cleanupBefore(oldest_active)`) so long-running uptime does not retain stale undo history or drift the tx-state base window.
- **WAL logging.** Slot reclamation is a physical page modification and must be WAL-logged for crash recovery. A new WAL record type (e.g., `reclaim_slot`) records the page ID and slot index.

### Scope
- Add a reclaimable-slot tracking structure (per-page or global, based on Phase 1 decision).
- Implement the reclamation check at the chosen trigger points.
- Extend `HeapPage` to support slot reuse after reclamation.
- Add WAL record type for slot reclamation.
- Wire reclamation into the DELETE commit path and the page-access path.
- Wire undo maintenance into connection/session commit and abort paths (not only tests/simulation paths).

### Gate
- Tombstoned slots are reclaimed without any background process.
- Slot space is reused by subsequent inserts on the same page.
- Reclamation never frees a slot visible to any active snapshot (verified by targeted concurrency tests).
- WAL replay correctly recovers reclaimed slots.
- All existing feature and stress tests pass.
- Deterministic: identical reclamation behavior under simulation replay.

## Phase 3: Overflow Page Chain Reclamation

### Design Decisions
- **Overflow pages follow their owning slot.** When a heap slot is reclaimed (Phase 2), any overflow page chain linked from that row is also reclaimable. The chain is walked and each page is returned to the page allocator.
- **Page free list.** Introduce a page-level free list (or extend the existing page allocator) to track pages returned by overflow chain reclamation. New allocations check the free list before extending the data file. The free list itself must be persisted (WAL-logged or stored in a dedicated page) for crash recovery.
- **Chain walking at reclamation time.** When reclaiming a slot, decode the row to find overflow page IDs (columns with `string_slot_overflow_tag`), walk each chain, and free every page in it. This adds I/O to the reclamation path but only for rows that actually have overflow data.
- **WAL logging.** Each freed overflow page is WAL-logged (e.g., `free_overflow_page` record) so crash recovery can reconstruct the free list.

### Scope
- Implement a page free list structure, persisted via WAL.
- Extend the page allocator to check the free list before allocating new pages.
- Wire overflow chain walking into the slot reclamation path from Phase 2.
- WAL record types for overflow page freeing and free list updates.

### Gate
- Overflow pages from deleted rows are returned to the free list and reused by subsequent allocations.
- No overflow page leaks under stress: a test that inserts and deletes many rows with large strings shows stable (not growing) file size or free list size.
- WAL replay correctly recovers the free list state.
- All existing tests pass.
- Deterministic under simulation replay.

## Phase 4: B+ Tree Dead Entry Cleanup

### Design Decisions
- **When to clean.** Dead index entries (entries whose target heap row is committed-deleted and invisible to all snapshots) are cleaned up at two points:
  - **At reclamation time**: when a heap slot is reclaimed (Phase 2), its corresponding B+ tree entry is also removed. This is the primary cleanup path since the reclamation trigger already proves no snapshot needs the entry.
  - **Opportunistic during scans**: when an index scan (point lookup or range scan) encounters an entry whose target row is tombstoned and reclaimable, it removes the entry in-line. This catches entries that survived a crash between heap reclamation and index cleanup.
- **No structural rebalancing.** Removing a B+ tree entry may leave underfull nodes. Structural rebalancing (merge/redistribute) is not in scope for this workfront. The B+ tree tolerates underfull nodes without correctness issues; it is a space/performance tradeoff acceptable for now.
- **Consistency with Workfront 11.** Workfront 11 establishes the pattern for B+ tree delete on row DELETE. This phase extends it to ensure entries are cleaned up even if the original delete's index removal was deferred or lost to a crash.

### Scope
- Wire B+ tree entry removal into the heap slot reclamation path (Phase 2 trigger).
- Add opportunistic dead-entry removal during index scans.
- WAL-log index entry removals triggered by reclamation (distinct from the original DELETE's index removal, which is already WAL-logged by Workfront 11).

### Gate
- B+ tree does not accumulate unbounded dead entries from committed deletes.
- After a workload of insert-delete cycles, index size stabilizes (does not grow without bound).
- Point lookups and range scans do not return results for reclaimed rows.
- All existing tests pass.
- Deterministic under simulation replay.

## Phase 5: Reclamation Under Concurrency

### Design Decisions
- **Long-running snapshots.** A snapshot that remains open for a long time pins the `oldest_active` watermark, preventing reclamation of any rows deleted after it began. This is correct behavior (the snapshot genuinely needs those versions), but it can cause temporary bloat. pg2 should expose this situation through observability (e.g., a system view showing oldest active snapshot age and estimated reclaimable-but-pinned space).
- **Reclamation ordering.** When multiple dead slots on the same page are reclaimable, they should be reclaimed in a single pass to avoid repeated page fetches. The reclamation check should batch by page.
- **Concurrent page access.** Reclamation modifies page contents (clearing slots, updating headers). Under the current single-threaded event loop model this is safe (no concurrent page access). If pg2 later moves to concurrent page access (Workfront 01 - server concurrency), reclamation must acquire appropriate page latches. Design the reclamation path with latching points identified, even if latches are no-ops today.
- **Undo log ring buffer pressure.** If `oldest_active` is pinned by a long-running snapshot, the undo log ring buffer may fill up. The existing `UndoLog` ring buffer has bounded capacity (`max_entries`). If reclamation is blocked and the ring buffer fills, new transactions that need undo space must wait or abort. Document this interaction and ensure the error path is clean.

### Scope
- Add concurrent-workload stress tests: multiple transactions inserting, deleting, and querying simultaneously, with varying snapshot lifetimes.
- Add observability for reclamation state: reclaimable-but-pinned slot count, oldest active snapshot age, free list size.
- Verify that the reclamation path is safe to call from any transaction context (no reentrancy issues, no deadlocks on page access).
- Test edge cases: snapshot opened before a delete, held open through reclamation of other rows, then used to read the deleted row (must still see the pre-delete version via undo log).
- Document latching points for future concurrency support.

### Gate
- Concurrent workload stress tests show no table bloat (reclaimable space is reclaimed once pinning snapshots close) and no visibility bugs.
- Long-running snapshot correctly prevents reclamation of rows it needs, and reclamation resumes when the snapshot closes.
- No assertion failures, panics, or data corruption under concurrent stress.
- Observability output correctly reports reclamation state.
- All existing tests pass.
- Deterministic under simulation replay with concurrent transaction scheduling.
