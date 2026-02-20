# Quality Gate Artifact: 202602202230-overflow-reclaim-commit-hook-policy

- Artifact ID: `202602202230-overflow-reclaim-commit-hook-policy`
- Commit: `48fe07be06be8a188fdea7cb1c39486a2c35b900`
- Title: `overflow reclaim drain commit-hook execution policy`
- Scope: `Finalizes reclaim drain execution to commit-hook only, with reclaim WAL emitted before tx_commit and deterministic one-chain drain budget per successful commit boundary.`

## PR Checklist

- What invariant was added or changed?
  - `Overflow reclaim drain is executed only on successful commit path; query/mutation execution path does not perform direct reclaim drain.`
  - `Reclaim WAL append for drained chains now occurs before transaction close so drained records are inside the committing tx envelope.`
  - `If reclaim drain fails, the request path aborts tx and fails closed instead of committing and then reporting reclaim failure.`

- What is the crash-consistency contract for the modified path?
  - `Reclaim records are emitted only from the commit path and are enclosed by tx markers (begin + commit/abort).`
  - `Failed reclaim drain does not produce a committed tx; tx is aborted and pending queue intents are rolled back for that tx.`
  - `Deterministic budget remains one committed chain per successful commit boundary.`

- Which error classes can now be returned?
  - `none` (no new public error classes; existing session-boundary and query error mapping applies).

- Does this change modify any persistent format or protocol?
  - Persistent format: `none` (no WAL/page binary format changes).
  - Protocol: `none` (response shape unchanged; commit-path failure handling tightened).

- Which deterministic crash/fault tests were added?
  - `src/server/e2e/overflow_reclaim_drain_policy.zig`
    - `test "e2e overflow multi-chain unlink drains one committed chain per commit boundary"`
  - `src/server/e2e/test_env.zig`
    - request harness commit ordering updated to mirror session commit-hook behavior.

- Which performance baseline or threshold was updated (if any)?
  - `none` (drain budget remains fixed at one chain; only execution hook placement/order changed).
