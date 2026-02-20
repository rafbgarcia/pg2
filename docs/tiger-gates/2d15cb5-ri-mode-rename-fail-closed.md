# Tiger Gate Artifact: 2d15cb5

- Commit: `2d15cb5`
- Title: `rename ri mode field and codify fail-closed integrity principle`
- Scope: Clarify RI mode naming and harden fail-closed integrity semantics.

## PR Checklist

- What invariant was added or changed?
  - RI mode semantics are explicit and fail closed when integrity configuration
    is missing/invalid.
  - Mutation/catalog paths must not infer unspecified RI behavior.

- What is the crash-consistency contract for the modified path?
  - No storage durability contract change.
  - Integrity decisioning logic is tightened to reject unsupported/implicit
    configurations before execution proceeds.

- Which error classes can now be returned?
  - Explicit invalid-configuration failures for missing/unsupported RI mode.

- Does this change modify any persistent format or protocol?
  - Persistent format: none.
  - Protocol: none.

- Which deterministic crash/fault tests were added?
  - No new fault-matrix scenario in this commit.
  - Deterministic validation/coverage in catalog + mutation tests remains:
    - `src/catalog/catalog.zig:908`
    - `src/executor/mutation.zig:1916`

- Which performance baseline or threshold was updated (if any)?
  - None.
