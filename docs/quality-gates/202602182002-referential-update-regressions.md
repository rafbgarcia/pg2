# Quality Gate Artifact: 993fc8a

- Commit: `993fc8a`
- Title: `expand referential update action regression coverage`
- Scope: Strengthen regression coverage for referential update actions.

## PR Checklist

- What invariant was added or changed?
  - Update-side RI actions (`restrict`, `cascade`, `set_null`) remain explicit
    and deterministic for parent key changes.

- What is the crash-consistency contract for the modified path?
  - No durability protocol change.
  - This commit primarily strengthens regression checks over existing behavior.

- Which error classes can now be returned?
  - None new; existing RI violation/error classification remains.

- Does this change modify any persistent format or protocol?
  - Persistent format: none.
  - Protocol: none.

- Which deterministic crash/fault tests were added?
  - Update-path regression coverage in mutation tests:
    - `src/executor/mutation.zig:1672`
    - `src/executor/mutation.zig:1747`
    - `src/executor/mutation.zig:1832`

- Which performance baseline or threshold was updated (if any)?
  - None.
