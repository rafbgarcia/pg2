# Workfront 12: Storage Reclamation Without VACUUM

## Objective
Eliminate the need for a traditional background VACUUM process by reclaiming dead storage (tombstoned heap slots, orphaned overflow page chains, stale B+ tree entries) inline with normal transaction processing, using epoch-based or eager strategies that are deterministic, safe under concurrent snapshots, and require no background threads.

## Progress Update (2026-02-25)
- Phase 2 slot-reclamation plumbing is in place:
  - heap slot state split into tombstoned vs reclaimed, with deterministic reclaimed-slot reuse;
  - slot reclaim queue with tx-aware pending/committed lifecycle;
  - commit/rollback session wiring for slot reclaim queue boundaries;
  - WAL `reclaim_slot` record + replay application in recovery;
  - inspect surface for heap reclaim queue/counters.
- A rollback correctness flaw was identified and resolved by implementing physical heap rollback on abort from undo pre-images before abort maintenance.
- Commit/abort maintenance (`undo_log.truncate(...)` + `tx_manager.cleanupBefore(...)`) remains enabled at pool boundaries, with rollback no longer depending on retained aborted visibility history.
- Reactor pinning tests now assert post-cleanup terminal state (`!= .active`) instead of requiring retained `.aborted` state outside the tx-state retention window.
- Phase 3 overflow allocator foundation landed:
  - persisted overflow allocator metadata (`free_list_head`, `next_page_id`) on a dedicated metadata page;
  - deterministic LIFO free-list reuse wired into overflow allocation path;
  - free-list push/pop WAL records and replay handling added;
  - reclaim path now routes freed overflow pages into reusable allocator state (not monotonic-only growth);
  - churn coverage added to assert reclaimed overflow pages are reused across insert/delete cycles.
- Phase 4 foundation started:
  - bounded tx-aware index reclaim metadata queue added (enqueue/commit/abort/dequeue lifecycle);
  - delete path now captures deterministic reclaim metadata (`model_id/index_id/row_id/encoded key`) and WAL `index_reclaim_enqueue`;
  - slot-reclaim drain now performs reclaim-time index delete + WAL `index_reclaim_delete`;
  - inspect surface now exposes `index_reclaim` queue/counter stats;
  - scan module now has opt-in `indexFindWithCleanup` / `indexRangeScanIntoWithCleanup` APIs for opportunistic cleanup while preserving read-only no-side-effect defaults.
- Phase 5 observability foundation started:
  - inspect now emits deterministic tx counters: `active_count`, `oldest_active_tx_id`, `next_tx_id`, `base_tx_id`.
  - added deterministic maintenance test for long-lived snapshot blocking + post-close reclaim resume path.
- Crash/replay matrix expanded:
  - added slot-reclaim replay idempotency coverage;
  - added index-reclaim WAL marker replay coverage.
- Full test suite is green after these changes (`zig build test --summary all` passes on 2026-02-25).

## Status Snapshot (2026-02-25)

### Phase Status
- **Phase 1 (Design Investigation and Decision):** complete
  - Explicit strategy decision and rejected alternatives are now locked in this document.
  - Final invariant text is formally stated and used as hard-stop criteria.
- **Phase 2 (Heap Slot Reclamation):** mostly complete
  - Implemented and covered by tests:
    - reclaim queue + tx lifecycle hooks
    - safe reclaim gating by `oldest_active`
    - reclaimed-slot reuse
    - WAL logging and replay support for slot reclaim
    - runtime commit/abort undo maintenance at pool/session boundaries
  - Remaining to fully close:
    - explicit, documented proof/tests for “never reclaim visible-to-any-active-snapshot” under targeted long-lived-snapshot scenarios
    - expanded crash matrix focused on `reclaim_slot` replay semantics
- **Phase 3 (Overflow Page Chain Reclamation):** complete
  - Implemented and covered by tests:
    - persisted allocator metadata (`free_list_head` + `next_page_id`);
    - deterministic LIFO free-list reuse;
    - free-list WAL push/pop + replay handling;
    - reclaim path writes freed pages back into allocator reuse flow;
    - churn test proving reclaimed pages are reused.
- **Phase 4 (B+ Tree Dead Entry Cleanup):** partial
  - Implemented:
    - reclaim-time cleanup hook from slot-reclaim drain to B+ tree delete;
    - bounded metadata queue for delete-time key capture;
    - WAL records for enqueue/delete metadata lifecycle;
    - inspect counters for index-reclaim queue/throughput;
    - opt-in generic point/range scan cleanup API surface.
  - **Not complete against this workfront:** opt-in scan cleanup is not yet wired into a write-context caller path, and crash/replay matrix for index reclaim WAL path still needs explicit assertions.
- **Phase 5 (Reclamation Under Concurrency):** partial
  - Some server/concurrency surfaces exist; baseline pinning coverage exists.
  - Added deterministic tx inspect counters to expose reclaim watermark pressure without wall-clock metrics.
  - Added targeted snapshot-pinning maintenance coverage for block/resume behavior.
  - **Not complete against this workfront:** full mixed-concurrency stress matrix and explicit per-resource pinned-by-snapshot pressure counters are still pending.

### Current Gap-to-Gate Summary
- **Complete now:** foundational slot reclaim + rollback safety + WAL/replay plumbing for slot reclaim.
- **Major remaining gates:**
  - wire opt-in generic scan cleanup path into a production write-context caller and prove bounded dead-entry growth under churn;
  - phase-5 concurrency/observability matrix for pinned snapshots and reclaim resumption;
  - expand replay matrix from marker/count coverage to full index-delete post-restart correctness assertions.

## Fresh Session Execution Plan (Ordered, Full-Completion Path)

This section is the handoff contract for a fresh Codex session. Execute in order.

1. **Phase 4 completion: index dead-entry cleanup parity**
   - Add reclaim-time index cleanup hook for rows becoming reclaimable.
   - Add opportunistic dead-entry deletion in generic index point/range scan paths (not only uniqueness checks).
   - Keep behavior MVCC-safe and idempotent under retries/replay.
   - Gate: dead index entries do not grow unbounded under churn; lookup/range correctness preserved.

2. **Phase 5 completion: long-lived snapshot + observability hardening**
   - Add targeted stress suite for reclaim blocked by long-lived snapshots, then resumed reclaim after snapshot close.
   - Add observability fields for pinned-by-snapshot reclaim pressure and progress (queue depth alone is insufficient).
   - Gate: deterministic tests prove reclaim is blocked only when required and resumes correctly.

3. **Crash/recovery completion matrix**
   - Add focused crash/replay scenarios for `reclaim_slot`, overflow free-list reuse, and index cleanup paths.
   - Verify idempotent replay and post-restart consistency of heap/overflow/index surfaces.
   - Gate: replay-focused matrix passes with deterministic results.

4. **Final full-gate validation**
   - Run `zig build test --summary all`.
   - Ensure this workfront’s phases are updated from partial to complete only when all phase gates are met.
   - Gate: full suite green + all phase gate checkboxes satisfied in this doc.

## Non-Negotiable Invariants (Hard-Stop If Violated)

1. **Snapshot safety:** No reclamation may free data that could be visible to any active snapshot.
2. **Abort correctness:** Aborted mutations must never become visible, regardless of undo truncation timing.
3. **WAL recoverability:** Every physical reclamation state transition must be recoverable and replay-idempotent.
4. **Determinism:** Reclaim behavior must be deterministic under simulation replay (no time-based or nondeterministic scheduling).
5. **Bounded operation:** Reclaim paths must honor bounded/static allocation contracts (no unbounded growth path introduced silently).

If any design/change makes these ambiguous or unprovable, stop and resolve the design explicitly before coding.

## Required Test Matrix Additions (Explicit)

Add or extend tests to cover all items below before marking full completion:

1. **Long-lived snapshot reclaim blocking**
   - Snapshot opened before delete/reclaim candidate creation.
   - Reclaim attempts while snapshot is open must not reclaim visible history.
   - After snapshot close, reclaim resumes and succeeds.

2. **Overflow reuse under churn**
   - Large-string insert/delete cycles with overflow chains.
   - Freed overflow pages are reused by later inserts.
   - Allocation cursor/file-growth behavior remains bounded relative to churn.

3. **Index dead-entry cleanup coverage**
   - Reclaim-time cleanup path validates stale entries are removed.
   - Generic index point scan and range scan opportunistic cleanup path validates stale entries are removed.
   - No false deletion of live index entries.

4. **Crash/replay idempotency**
   - Replay tests for `reclaim_slot`, overflow free-list updates, and index cleanup WAL records.
   - Re-applying replay on same WAL segment yields stable final state.

5. **Concurrency/pinning stress**
   - Mixed workload with concurrent readers/writers and pinned transactions.
   - Assertions for no visibility regressions, no leaked resources, and eventual reclaim progress.

## Definition Of Done By Phase (Full, Not Partial)

1. **Phase 1 done when:**
   - Final strategy and invariants are explicitly documented and reviewed.
2. **Phase 2 done when:**
   - Existing slot reclaim + abort rollback guarantees remain green with no open correctness gaps.
3. **Phase 3 done when:**
   - Overflow reclaimed pages are actually reusable via persisted allocator contract, with replay proof.
4. **Phase 4 done when:**
   - Dead index cleanup is tied to reclaim events and present in generic scan paths, with bounded-growth evidence.
5. **Phase 5 done when:**
   - Long-lived snapshot blocking/resume behavior and reclaim-pressure observability are implemented and tested.
6. **Workfront done when:**
   - All phase gates above pass and `zig build test --summary all` is green.

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

### Final Strategy Decision (Locked 2026-02-25)
- **Selected strategy:** hybrid inline reclamation:
  - heap slot reclaim gated by `oldest_active`;
  - overflow reclaim via persisted LIFO free-list allocator;
  - reclaim-time index cleanup plus opt-in opportunistic scan cleanup hooks for write contexts;
  - no background workers.
- **Rejected alternatives:**
  - background VACUUM (violates no-background-thread constraint);
  - scan-only opportunistic cleanup (insufficient bounded-growth guarantees);
  - WAL-only allocator reconstruction without persisted allocator metadata (unsafe once WAL is truncated).
- **Allocator contract (locked):**
  - persisted allocator metadata stores `free_list_head` + `next_page_id`;
  - free-list updates are WAL-logged (`overflow_free_list_push/pop`) and replayed deterministically;
  - read-only scan paths remain side-effect free.
- **Observability contract (locked):**
  - tx/counter based only; no wall-clock age in correctness paths/tests.

### Final Safety Invariants
1. **Snapshot safety:** No reclamation may free data that could be visible to any active snapshot.
2. **Abort correctness:** Aborted mutations must never become visible, regardless of undo truncation timing.
3. **WAL recoverability:** Every physical reclamation state transition must be recoverable and replay-idempotent.
4. **Determinism:** Reclaim behavior must be deterministic under simulation replay (no time-based or nondeterministic scheduling).
5. **Bounded operation:** Reclaim paths must honor bounded/static allocation contracts (no unbounded growth path introduced silently).

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
