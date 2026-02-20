# Tiger Gate Artifact: 2026-02-20-e2e-spec-03-filter-sort-limit-offset-server-path

- Commit: `<pending>`
- Title: `Mirror e2e/specs/03_filter_sort_limit_offset.spec in Zig server-path test`
- Scope: `Adds a deterministic session-path scenario test for filter/sort/offset/limit parity with the human-readable E2E spec.`

## PR Checklist

- What invariant was added or changed?
  - `The server session boundary now has a spec-shaped regression test that asserts exact response bytes for filter/sort/offset/limit sequencing from schema-loaded catalog state.`

- What is the crash-consistency contract for the modified path?
  - `Unchanged. This increment adds tests only and does not modify WAL/undo/flush ordering behavior.`

- Which error classes can now be returned?
  - `Unchanged. No new production error classes were introduced.`

- Does this change modify any persistent format or protocol?
  - Persistent format: `none`
  - Protocol: `none (test assertions only validate existing response format)`

- Which deterministic crash/fault tests were added?
  - `None. Added deterministic session-path scenario coverage in src/server/e2e_specs.zig.`

- Which performance baseline or threshold was updated (if any)?
  - `none`
