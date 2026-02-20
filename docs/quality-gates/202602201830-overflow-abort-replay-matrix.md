# Quality Gate Artifact: 202602201830-overflow-abort-replay-matrix

- Artifact ID: `202602201830-overflow-abort-replay-matrix`
- Commit: `TBD (this increment commit)` (required real committed SHA)
- Title: `overflow lifecycle abort replay matrix`
- Scope: `Adds deterministic recovery replay coverage asserting aborted transactions do not apply overflow lifecycle side effects (create/relink/unlink/reclaim).`

## PR Checklist

- What invariant was added or changed?
  - `Overflow lifecycle WAL records from aborted transactions are non-replayable, including reclaim records, so recovery must never reclaim a chain from an aborted tx.`
  - `Replay-time safety is fail-closed: malformed metadata still errors as corruption; valid aborted lifecycle records are ignored.`

- What is the crash-consistency contract for the modified path?
  - `Recovery replays only committed (or legacy replayable without tx markers) overflow lifecycle WAL records.`
  - `If a transaction writes overflow lifecycle records and ends with tx_abort, replay does not apply reclaim and does not mutate overflow page state for that tx's lifecycle records.`
  - `This prevents replay-time reclaimed-live-chain behavior when an aborted tx emitted unlink/reclaim lifecycle intents.`

- Which error classes can now be returned?
  - `none` (no new public error classes; existing `Corruption` and storage/WAL replay errors remain unchanged).

- Does this change modify any persistent format or protocol?
  - Persistent format: `none` (no WAL/page format changes).
  - Protocol: `none` (no server wire/output changes).

- Which deterministic crash/fault tests were added?
  - `src/storage/recovery.zig`
    - `test "replayCommittedOverflowLifecycle skips aborted tx after overflow create"`
    - `test "replayCommittedOverflowLifecycle skips aborted tx after overflow relink intent"`
    - `test "replayCommittedOverflowLifecycle skips aborted tx after overflow unlink enqueue and reclaim record"`

- Which performance baseline or threshold was updated (if any)?
  - `none` (test-only increment; no hot-path mutation or replay algorithm change).
