# Quality Gate Artifact: 202602201647-insert-wide-row-and-late-duplicate

- Artifact ID: `202602201647-insert-wide-row-and-late-duplicate`
- Commit: `ec6d84b` (required real committed SHA)
- Release: `0.2.0`
- Title: `Insert wide-row and late-duplicate edge-case E2E coverage`
- Scope: `Add deterministic insert E2E scenarios for wide-row schemas and duplicate-key failure near tail of high-volume workloads.`

## PR Checklist

- What invariant was added or changed?
  - Insert E2E now validates wide-row insert/read behavior at the current catalog schema width boundary (`128` total fields per model).
  - Insert E2E now validates fail-closed duplicate-key behavior after substantial successful inserts and verifies continued write-path health post-error.

- What is the crash-consistency contract for the modified path?
  - No crash-consistency contract change in this increment; coverage-only changes validating existing insert behavior.

- Which error classes can now be returned?
  - No new error classes were introduced.
  - Existing `DuplicateKey` fail-closed behavior receives additional stress-path coverage.

- Does this change modify any persistent format or protocol?
  - Persistent format: `none`
  - Protocol: `none`

- Which deterministic crash/fault tests were added?
  - None in this increment.
  - Added deterministic server-path E2E mutation tests:
    - `e2e insert supports 128 total fields with deterministic readback`
    - `e2e insert duplicate key fails closed late in high-volume workload`
    - file: `src/server/e2e/mutations/insert.zig`

- Which performance baseline or threshold was updated (if any)?
  - `none` (no explicit threshold update; behavioral stress coverage expanded).
