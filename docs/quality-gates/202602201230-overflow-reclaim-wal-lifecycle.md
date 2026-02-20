# Quality Gate Artifact: 202602201230-overflow-reclaim-wal-lifecycle

- Artifact ID: `202602201230-overflow-reclaim-wal-lifecycle`
- Commit: `TBD (this increment commit)`
- Title: `Deterministic overflow reclaim queue + overflow WAL lifecycle contract`
- Scope: `Adds deterministic unlink/reclaim queue semantics, explicit overflow WAL lifecycle records (create/relink/unlink/reclaim), crash/restart coverage, and malformed-chain hardening.`

## PR Checklist

- What invariant was added or changed?
  - `Overflow chains replaced or deleted from visible rows must be logically unlinked first, then reclaimed through catalog-owned deterministic queue processing.`
  - `Reclaim queue state is explicit (`catalog.overflow_reclaim_queue`) and fail-closed on invalid roots, duplicates, and full-capacity exhaustion.`
  - `Overflow reclaim traversal must remain within the allocator-owned region, valid overflow page type/format, and bounded hop count; violations map to corruption.`

- What is the crash-consistency contract for the modified path?
  - `Overflow chain lifecycle is explicitly WAL-described with deterministic ordering:`
    - `create` (after chain page writes),
    - row-pointer publication (`insert`/`update`) then `relink`,
    - logical unlink (`unlink`) for replaced/deleted chains,
    - bounded deterministic reclaim (`reclaim`).
  - `Mutation path keeps existing undo-before-heap-update ordering; overflow reclaim is decoupled via queue and drained with fixed budget per mutation path.`
  - `Current restart validation is WAL-envelope + WAL-decode recovery assertions; full physical page replay integration remains a follow-up.`

- Which error classes can now be returned?
  - `New mutation error: OverflowReclaimQueueFull (classified as resource_exhausted).`
  - `Malformed overflow chains/pointers/pages during reclaim/read continue to fail closed as corruption-class behavior.`

- Does this change modify any persistent format or protocol?
  - Persistent format: `none (row/page format unchanged in this increment)`
  - Protocol: `none (wire framing unchanged); query behavior now includes deterministic overflow unlink/reclaim lifecycle semantics`

- Which deterministic crash/fault tests were added?
  - `src/executor/mutation.zig`
    - `test "overflow WAL lifecycle is deterministic for replace path"`
    - `test "overflow WAL lifecycle includes unlink and reclaim on delete"`
    - `test "overflow lifecycle WAL records survive crash and restart recovery"`
    - `test "reclaim drain fails closed on cyclic overflow chain corruption"`
  - `src/server/e2e/overflow.zig`
    - `test "e2e overflow insert update and read via session path"`
    - `test "e2e overflow delete drains reclaim queue deterministically"`

- Which performance baseline or threshold was updated (if any)?
  - `Reclaim drain uses fixed deterministic budget of one chain per mutation path to avoid unbounded in-path reclaim work.`
  - `Inline spill threshold remains 1024 bytes (unchanged).`
