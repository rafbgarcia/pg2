# Quality Gate Artifact: 202602201500-overflow-lifecycle-replay-recovery

- Artifact ID: `202602201500-overflow-lifecycle-replay-recovery`
- Commit: `505fff1` (required real committed SHA)
- Title: `overflow lifecycle replay recovery`
- Scope: `Adds deterministic replay of overflow lifecycle WAL records into recovered page state with idempotent reclaim semantics and crash/restart E2E coverage.`

## PR Checklist

- What invariant was added or changed?
  - Overflow lifecycle WAL replay now validates metadata fail-closed and applies committed/legacy-replayable `overflow_chain_reclaim` records to page state during recovery. Re-running replay over the same WAL does not mutate already reclaimed chains.

- What is the crash-consistency contract for the modified path?
  - Recovery decodes durable WAL records, validates overflow metadata, and applies reclaim transitions (`overflow` -> `free`) idempotently. If reclaimed pages were already persisted pre-crash, replay is a no-op for that chain; if not, replay applies the reclaim. Any malformed metadata or allocator-region mismatch fails closed as corruption.

- Which error classes can now be returned?
  - `Corruption` from recovery replay on malformed overflow lifecycle payloads or invalid page-id ownership.
  - Existing storage/WAL read errors propagated from `BufferPool` and `Wal`.

- Does this change modify any persistent format or protocol?
  - Persistent format: `none` (reuses existing WAL record types/payloads).
  - Protocol: `none` (internal recovery behavior only).

- Which deterministic crash/fault tests were added?
  - `src/storage/recovery.zig` adds `replayCommittedOverflowLifecycle reclaims chain and is idempotent`.
  - `src/server/e2e/overflow.zig` adds `e2e overflow reclaim WAL replay restores page state after crash and is idempotent`.

- Which performance baseline or threshold was updated (if any)?
  - `none` (recovery scan adds no runtime hot-path overhead to normal mutations).
