# Tiger Gate Artifact: e5c45f5

- Commit: `e5c45f5`
- Title: `sim: add WAL+undo crash visibility consistency scenario`
- Scope: Add deterministic simulation scenario validating pre-crash undo
  visibility against post-restart persisted visibility.

## PR Checklist

- What invariant was added or changed?
  - If a mutation fails before WAL durability, undo-based pre-crash visibility
    and post-restart visibility must match deterministically.

- What is the crash-consistency contract for the modified path?
  - Contract unchanged. This commit strengthens verification of existing
    WAL+undo ordering/visibility rules.

- Which error classes can now be returned?
  - None new.

- Does this change modify any persistent format or protocol?
  - Persistent format: none.
  - Protocol: none.

- Which deterministic crash/fault tests were added?
  - `src/simulator/fault_matrix.zig:1273`
    `test "seeded schedule: WAL+undo crash visibility consistency remains replay-deterministic"`

- Which performance baseline or threshold was updated (if any)?
  - None.
