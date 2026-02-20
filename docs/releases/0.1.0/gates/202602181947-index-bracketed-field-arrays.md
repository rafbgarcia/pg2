# Quality Gate Artifact: eb64d65

- Artifact ID: `202602181947-index-bracketed-field-arrays`
- Commit: `eb64d65`
- Title: `schema: require bracketed index field arrays`
- Scope: Make index declaration shape explicit with bracketed field arrays.

## PR Checklist

- What invariant was added or changed?
  - Index field lists must use bracketed array syntax in schema.
  - Invalid index declaration shape is rejected explicitly.

- What is the crash-consistency contract for the modified path?
  - None. Parser/schema-loading change only.

- Which error classes can now be returned?
  - No new runtime error class mapping.
  - Parse/schema boundary returns explicit invalid schema errors for malformed
    index declarations.

- Does this change modify any persistent format or protocol?
  - Persistent format: none.
  - Protocol: schema language syntax tightened for index declarations.

- Which deterministic crash/fault tests were added?
  - None. This is schema-surface validation, not recovery/fault handling.

- Which performance baseline or threshold was updated (if any)?
  - None.
