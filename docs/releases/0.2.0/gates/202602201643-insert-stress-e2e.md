# Quality Gate Artifact: 202602201643-insert-stress-e2e

- Artifact ID: `202602201643-insert-stress-e2e`
- Commit: `b6dd526` (required real committed SHA)
- Release: `0.2.0`
- Title: `Insert high-volume and large-row stress E2E coverage`
- Scope: `Add deterministic server session-path insert stress coverage for high-volume and oversized-row scenarios.`

## PR Checklist

- What invariant was added or changed?
  - Insert session-path E2E now covers sustained sequential insert workloads with deterministic addressability checks at low/mid/high key ranges.
  - Insert session-path E2E now covers oversized payload inserts and deterministic read-back validation to ensure large rows survive full write/read path.
  - Coverage is organized by single-scenario tests in `src/server/e2e/mutations/insert.zig`.

- What is the crash-consistency contract for the modified path?
  - No crash contract behavior change in this increment; this is coverage-only work validating existing insert durability/visibility behavior under larger workloads.

- Which error classes can now be returned?
  - No new user-visible error classes introduced.

- Does this change modify any persistent format or protocol?
  - Persistent format: `none`
  - Protocol: `none`

- Which deterministic crash/fault tests were added?
  - None in this increment.
  - Deterministic server-path E2E stress tests added:
    - `e2e insert high-volume sequential requests remain queryable via session path`
    - `e2e insert large-row payloads remain readable via session path`
    - file: `src/server/e2e/mutations/insert.zig`

- Which performance baseline or threshold was updated (if any)?
  - `none` (no explicit performance threshold added yet; coverage establishes behavioral stress baseline).
