# Quality Gate Artifact: 202602202030-overflow-reclaim-tx-rollback-contract

- Artifact ID: `202602202030-overflow-reclaim-tx-rollback-contract`
- Commit: `f7f33a2e8d872435dc9140589b4d8f3183613a95`
- Title: `overflow reclaim queue tx rollback contract`
- Scope: `Makes overflow reclaim queue entries transaction-scoped so abort removes pending reclaim intents and committed-only drain prevents reclaiming live chains.`

## PR Checklist

- What invariant was added or changed?
  - `Overflow reclaim queue entries are now tx-owned with explicit state transitions: pending -> committed (on tx commit) or removed (on tx abort).`
  - `Physical reclaim only dequeues committed head entries, so pending/aborted entries can never reclaim overflow pages.`
  - `Server session now aborts tx on query error and rolls back pending overflow reclaim queue intents before releasing the lease.`

- What is the crash-consistency contract for the modified path?
  - `Unlink intents are still WAL-recorded at mutation time, but reclaim is only WAL-recorded after tx commit hook marks queue entries committed and drains deterministic budget.`
  - `If a tx aborts, pending queue entries are dropped and no reclaim WAL record is emitted for those entries.`
  - `Recovery replay therefore cannot reclaim chains for aborted queue intents because reclaim records are absent and aborted lifecycle records remain non-replayable.`

- Which error classes can now be returned?
  - `none` (no new external error class names; existing mutation/session error classification remains in place).

- Does this change modify any persistent format or protocol?
  - Persistent format: `none` (no page/WAL binary format change).
  - Protocol: `none` (response line shapes unchanged; tx error path behavior tightened).

- Which deterministic crash/fault tests were added?
  - `src/storage/overflow.zig`
    - `test "reclaim queue abort removes only pending entries for tx"`
    - `test "reclaim queue blocks dequeue when head tx is pending"`
  - `src/executor/mutation.zig`
    - `test "reclaim queue preserves ordering across tx rollback and commit"`
    - `test "abort rollback prevents reclaim of live overflow chain"`
  - `src/server/pool.zig`
    - `test "abortCheckin aborts transaction and releases slot for reuse"`

- Which performance baseline or threshold was updated (if any)?
  - `none` (queue operations add bounded metadata transitions; no threshold constant changed in this increment).
