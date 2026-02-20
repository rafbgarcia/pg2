# Tiger Gate Artifact: a1c44df

- Commit: `a1c44df`
- Title: `sim: add deterministic rollback visibility edge matrix scenario`
- Scope: Add deterministic simulation coverage for rollback visibility edge
  behavior in MVCC/recovery paths.

## PR Checklist

- What invariant was added or changed?
  - Aborted head versions must never become snapshot-visible.
  - Readers must deterministically resolve to the correct prior committed
    version across replay.

- What is the crash-consistency contract for the modified path?
  - Contract unchanged. This commit adds simulator verification for existing
    rollback/replay semantics.

- Which error classes can now be returned?
  - None new. No public error taxonomy changes.

- Does this change modify any persistent format or protocol?
  - Persistent format: none.
  - Protocol: none.

- Which deterministic crash/fault tests were added?
  - `src/simulator/fault_matrix.zig:1265`
    `test "seeded schedule: rollback visibility edge remains replay-deterministic"`

- Which performance baseline or threshold was updated (if any)?
  - None.
