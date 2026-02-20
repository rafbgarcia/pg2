# Quality Gate Artifact: a1ebf01

- Commit: `a1ebf01`
- Title: `Fail closed on implicit belongs_to integrity`
- Scope: Reject implicit `belongs_to` integrity policy; require explicit RI mode.

## PR Checklist

- What invariant was added or changed?
  - `belongs_to` without explicit RI policy is invalid and rejected.
  - Integrity semantics must be declared explicitly; no implicit defaults.

- What is the crash-consistency contract for the modified path?
  - No crash/durability protocol change.
  - Change is fail-closed validation in catalog/schema loading.

- Which error classes can now be returned?
  - Explicit invalid-configuration failures for implicit `belongs_to` integrity
    configuration.

- Does this change modify any persistent format or protocol?
  - Persistent format: none.
  - Protocol: schema-level behavior tightened for `belongs_to`.

- Which deterministic crash/fault tests were added?
  - Validation tests:
    - `src/catalog/catalog.zig:908`
    - `src/catalog/schema_loader.zig:409`

- Which performance baseline or threshold was updated (if any)?
  - None.
