# Quality Gate Artifact: 202602201921-schema-parenthesis-enforcement

- Artifact ID: `202602201921-schema-parenthesis-enforcement`
- Commit: `632bc45`
- Release: `0.2.0`
- Title: `Fail-closed schema parser contract for parenthesized declarations`
- Scope: `Remove legacy non-parenthesized schema parsing fallbacks and enforce one obvious syntax for field/index declarations.`

## PR Checklist

- What invariant was added or changed?
  - Schema declarations are now parenthesized-only:
    - `field(...)` is required.
    - `index(...)` / `uniqueIndex(...)` are required.
  - Legacy non-parenthesized forms are rejected fail-closed:
    - `field name type ...`
    - `index col1, col2`
  - Parser tests updated to align with parenthesized schema examples.
  - New parser regression tests added to assert legacy syntax rejection.

- What is the crash-consistency contract for the modified path?
  - No crash-consistency behavior change in this increment.
  - Scope is parser grammar and user-facing syntax contract enforcement.

- Which error classes can now be returned?
  - No new error classes introduced.
  - Existing parse-time `UnexpectedToken` now deterministically covers rejected legacy syntax.

- Does this change modify any persistent format or protocol?
  - Persistent format: `none`
  - Protocol: `none`

- Which deterministic crash/fault tests were added?
  - None.
  - Deterministic parser regression tests added:
    - `parse schema field rejects non-parenthesized syntax`
    - `parse schema index rejects non-parenthesized syntax`

- Which performance baseline or threshold was updated (if any)?
  - `none` (grammar contract hardening increment).
