# Quality Gate Artifact: 202602202520-overflow-reclaim-durable-write-commit-replay-matrix

- Artifact ID: `202602202520-overflow-reclaim-durable-write-commit-replay-matrix`
- Commit: `d92e3089200f4f2063a756a192980d650a234807`
- Title: `durable reclaim via write-commit drain plus repeated replay matrix`
- Scope: `Finalizes reclaim durability semantics by draining only on successful write commits and validates durable multi-chain reclaim across crash/restart and repeated idempotent replay cycles.`

## PR Checklist

- What invariant was added or changed?
  - `Overflow reclaim drain now executes only when a request performed mutations (insert/update/delete), preventing read-only requests from draining queue state outside write-commit durability context.`
  - `Every drained chain is reclaimed under a write tx that emits durable tx_commit WAL, keeping reclaim records inside strict tx marker envelope.`

- What is the crash-consistency contract for the modified path?
  - `Crash recovery replay remains WAL-driven and applies only durable reclaim records.`
  - `With write-commit-only drain, drained chains in the matrix scenario produce durable reclaim WAL before tx_commit and replay reclaims all unlinked roots after restart.`
  - `Repeated replay cycles are idempotent: first pass applies reclaim, second pass reports only idempotent skips.`

- Which error classes can now be returned?
  - `none` (no new public error classes; response classification unchanged).

- Does this change modify any persistent format or protocol?
  - Persistent format: `none` (no WAL/page binary format change).
  - Protocol: `none` (response shape unchanged; internal drain trigger semantics tightened).

- Which deterministic crash/fault tests were added?
  - `src/server/e2e/overflow_reclaim_crash_matrix.zig`
    - `test "e2e crash matrix: follow-up write commit drains backlog and replay reclaims all unlinked roots"`
    - `test "e2e crash matrix: repeated replay cycles remain idempotent after durable multi-chain reclaim"`
  - `src/server/e2e/overflow_reclaim_drain_policy.zig`
    - `test "e2e overflow multi-chain unlink drains one committed chain per successful write commit boundary"` (updated for write-only drain trigger)

- Which performance baseline or threshold was updated (if any)?
  - `none` (drain budget remains fixed at one chain; trigger condition narrowed to successful write commits).
