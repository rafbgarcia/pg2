# Quality Gate Artifact: 202602202030-nullable-insert-e2e

- Artifact ID: `202602202030-nullable-insert-e2e`
- Commit: `08a87b8`
- Release: `0.2.0`
- Title: `Explicit schema nullability + nullable insert matrix coverage`
- Scope: `Require explicit field nullability (`notNull` or `nullable`) and add dedicated server-session E2E nullable/default scenarios.`

## PR Checklist

- What invariant was added or changed?
  - Schema definitions now fail closed unless every field explicitly declares exactly one nullability constraint:
    - `notNull` or `nullable` is required,
    - missing nullability is rejected,
    - conflicting `notNull` + `nullable` is rejected.
  - Parser/tokenizer/schema loader updated for `nullable` keyword handling.
  - Added dedicated E2E coverage for nullable semantics on insert:
    - omitted nullable fields persist `null`,
    - explicit `null` assignment to nullable fields succeeds,
    - nullable fields with defaults apply defaults only when omitted,
    - explicit `null` bypasses default and remains `null`,
    - explicit values override defaults.
  - Added test files:
    - `src/server/e2e/constraints/nullable.zig`
    - `src/server/e2e/constraints/nullable_with_default.zig`
  - Suite aggregation updated in `src/server/e2e/e2e_specs.zig`.

- What is the crash-consistency contract for the modified path?
  - No crash contract behavior change in this increment; this is schema-validation + E2E coverage work over existing insert/default/null handling.

- Which error classes can now be returned?
  - No new error classes introduced.

- Does this change modify any persistent format or protocol?
  - Persistent format: `none`
  - Protocol: `none`

- Which deterministic crash/fault tests were added?
  - None in this increment.
  - Deterministic server-session E2E tests added:
    - `e2e insert allows omitted nullable field and persists null`
    - `e2e insert allows explicit null assignment to nullable field`
    - `e2e insert applies default for omitted nullable field`
    - `e2e insert explicit null bypasses default on nullable field`
    - `e2e insert explicit value overrides default on nullable field`

- Which performance baseline or threshold was updated (if any)?
  - `none` (coverage-focused increment).
