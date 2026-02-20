# Quality Gate Artifact: ace72c4

- Commit: `ace72c4`
- Title: `Harden foreign-key config validation`
- Scope: Tighten FK/RI configuration validation across catalog/schema/mutation.

## PR Checklist

- What invariant was added or changed?
  - FK config must be valid and explicit before association resolution and
    mutation enforcement.
  - Unsupported RI action combinations/configs are rejected fail closed.

- What is the crash-consistency contract for the modified path?
  - No crash-consistency contract change; validation and execution checks only.

- Which error classes can now be returned?
  - Explicit invalid-configuration and integrity violation outcomes are surfaced
    earlier and more consistently.

- Does this change modify any persistent format or protocol?
  - Persistent format: none.
  - Protocol: none.

- Which deterministic crash/fault tests were added?
  - Validation/mutation coverage anchored in:
    - `src/catalog/catalog.zig:839`
    - `src/catalog/catalog.zig:862`
    - `src/catalog/catalog.zig:885`
    - `src/catalog/schema_loader.zig:473`
    - `src/executor/mutation.zig:1916`

- Which performance baseline or threshold was updated (if any)?
  - None.
