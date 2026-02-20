# Quality Gate Artifact: 202602202000-insert-default-values

- Artifact ID: `202602202000-insert-default-values`
- Commit: `186e6e0`
- Release: `0.2.0`
- Title: `Insert omitted-field default values`
- Scope: `Support typed schema defaults and apply them only for omitted insert fields through server session path.`

## PR Checklist

- What invariant was added or changed?
  - Schema loader now parses typed `default` literals for fields and persists defaults in catalog metadata.
  - Insert execution now distinguishes omitted fields from explicitly assigned values:
    - omitted fields use schema defaults when present,
    - explicitly assigned `null` remains explicit null and does not auto-default.
  - New dedicated E2E file:
    - `src/server/e2e/constraints/default_values.zig`

- What is the crash-consistency contract for the modified path?
  - No WAL record format or replay contract change.
  - Defaults are resolved before row encode/write and therefore participate in the existing insert durability contract unchanged.

- Which error classes can now be returned?
  - No new error class introduced.
  - Existing `NullNotAllowed` behavior remains deterministic when a required column is explicitly null or omitted without a default.

- Does this change modify any persistent format or protocol?
  - Persistent format: `none`
  - Protocol: `none`

- Which deterministic crash/fault tests were added?
  - None in this increment.
  - Deterministic server-session E2E tests added:
    - `e2e insert applies schema defaults for omitted fields`
    - `e2e insert keeps explicit null semantics even when default exists`

- Which performance baseline or threshold was updated (if any)?
  - `none` (behavior and correctness increment).
