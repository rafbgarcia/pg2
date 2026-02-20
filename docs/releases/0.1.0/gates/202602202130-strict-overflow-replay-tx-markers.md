# Quality Gate Artifact: 202602202130-strict-overflow-replay-tx-markers

- Artifact ID: `202602202130-strict-overflow-replay-tx-markers`
- Commit: `48fe07be06be8a188fdea7cb1c39486a2c35b900`
- Title: `strict tx-marker policy for overflow lifecycle replay`
- Scope: `Enforces fail-closed recovery replay when overflow lifecycle WAL mutations are missing tx markers and wires session tx lifecycle to emit begin/commit/abort WAL records.`

## PR Checklist

- What invariant was added or changed?
  - `Any overflow lifecycle mutation record is replay-eligible only when its tx has an explicit WAL transaction envelope; markerless mutation WAL is treated as corruption.`
  - `Server session tx lifecycle now writes tx_begin at checkout, tx_commit at checkin, and tx_abort on abortCheckin so replay classification remains deterministic and strict.`

- What is the crash-consistency contract for the modified path?
  - `Replay applies overflow lifecycle records only for txs with begin+commit markers.`
  - `Txs with begin but no terminal marker are treated as in-flight at crash and are not replayed in overflow lifecycle cleanup.`
  - `Markerless overflow lifecycle WAL is rejected fail-closed as corruption to avoid ambiguous replay behavior.`

- Which error classes can now be returned?
  - `corruption`: strict replay now returns corruption when overflow lifecycle mutation records are missing tx markers.
  - `retryable/resource/fatal`: unchanged mapping surface except session boundary now explicitly classifies WAL errors from pool checkin/abort paths.

- Does this change modify any persistent format or protocol?
  - Persistent format: `none` (no WAL/page binary layout change; tx markers were existing record types).
  - Protocol: `none` (no response line-shape change; behavior is in recovery safety policy).

- Which deterministic crash/fault tests were added?
  - `src/storage/recovery.zig`
    - `test "replayCommittedOverflowLifecycle fails closed when tx markers are missing for mutation records"`
  - `src/server/e2e/overflow_replay_tx_markers.zig`
    - `test "e2e overflow replay fails closed for lifecycle record without tx markers"`
  - `src/storage/recovery.zig`
    - existing replay tests updated to include explicit tx_begin markers for aborted tx scenarios.

- Which performance baseline or threshold was updated (if any)?
  - `none` (strict marker classification is bounded by caller-supplied WAL decode buffers; no threshold constants changed).
