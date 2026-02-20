# Quality Gate Artifact: 85de518

- Artifact ID: `202602181803-reference-ri-syntax`
- Commit: `85de518`
- Title: `schema: add explicit reference RI syntax`
- Scope: Add explicit RI policy/action syntax to schema parser/catalog loading.

## PR Checklist

- What invariant was added or changed?
  - Relationship integrity policy is explicit in schema (`withReferentialIntegrity(...)` or `withoutReferentialIntegrity`).
  - Referential delete/update actions are parsed as declared metadata, not inferred.

- What is the crash-consistency contract for the modified path?
  - No crash-consistency contract change. Parser/catalog metadata path only.

- Which error classes can now be returned?
  - No new runtime error class mapping; parse/schema validation failures are
    explicitly surfaced on invalid RI syntax.

- Does this change modify any persistent format or protocol?
  - Persistent format: none.
  - Protocol: schema/query language surface changed to include explicit RI syntax.

- Which deterministic crash/fault tests were added?
  - None in fault matrix for this commit.
  - Deterministic parser/schema coverage exists in:
    - `src/parser/tokenizer.zig:623`
    - `src/catalog/schema_loader.zig:432`

- Which performance baseline or threshold was updated (if any)?
  - None.
