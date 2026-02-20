# Quality Gate Artifact: e5de781

- Commit: `e5de781`
- Title: `enforce referential integrity actions in mutation paths`
- Scope: Enforce RI actions on insert/update/delete execution paths.

## PR Checklist

- What invariant was added or changed?
  - RI action contract is enforced for all mutation paths when association mode
    is `with_referential_integrity`.
  - Parent/child write behavior must respect declared restrict/cascade/set-null
    semantics.

- What is the crash-consistency contract for the modified path?
  - Crash-consistency storage contract unchanged.
  - Mutation decision path now applies explicit RI constraints before/while
    executing writes.

- Which error classes can now be returned?
  - Integrity-violation outcomes from mutation execution are now explicit under
    RI action enforcement.

- Does this change modify any persistent format or protocol?
  - Persistent format: none.
  - Protocol: none.

- Which deterministic crash/fault tests were added?
  - Mutation-path regression coverage:
    - `src/executor/mutation.zig:1486`
    - `src/executor/mutation.zig:1517`
    - `src/executor/mutation.zig:1590`
    - `src/executor/mutation.zig:1672`
    - `src/executor/mutation.zig:1747`
    - `src/executor/mutation.zig:1832`

- Which performance baseline or threshold was updated (if any)?
  - None.
