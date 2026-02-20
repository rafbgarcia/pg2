# Quality Gate Artifact: 202602201400-overflow-reclaim-budget-backlog-progression

- Artifact ID: `202602201400-overflow-reclaim-budget-backlog-progression`
- Commit: `eda13615f4f2ee86522dc3a4343be4c2a8f2ef2d`
- Release: `0.1.0`
- Title: `overflow reclaim budget backlog progression`
- Scope: `Defines and validates deterministic reclaim backlog progression when one mutation unlinks multiple overflow chains under the fixed drain budget.`

## PR Checklist

- What invariant was added or changed?
  - `Overflow reclaim drain remains fixed at one committed chain per successful request boundary.`
  - `When one mutation unlinks multiple chains, the queue depth and reclaim counters progress deterministically across subsequent request boundaries.`

- What is the crash-consistency contract for the modified path?
  - `No new WAL record types or ordering rules were introduced.`
  - `Logical unlink records are emitted on mutation, reclaim records are emitted only when committed entries are drained by the bounded reclaim hook.`

- Which error classes can now be returned?
  - `none` (no new externally visible error classes were added in this increment).

- Does this change modify any persistent format or protocol?
  - Persistent format: `none`.
  - Protocol: `none` (inspect output format unchanged; semantics are now explicitly documented and covered).

- Which deterministic crash/fault tests were added?
  - `src/server/e2e/string_overflow.zig`
    - `test "e2e overflow multi-chain unlink drains one committed chain per request boundary"`

- Which performance baseline or threshold was updated (if any)?
  - `none` (drain budget remains fixed at one chain per boundary; this increment documents and validates the existing threshold).
