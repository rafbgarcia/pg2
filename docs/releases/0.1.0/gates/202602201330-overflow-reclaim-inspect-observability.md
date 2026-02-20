# Quality Gate Artifact: 202602201330-overflow-reclaim-inspect-observability

- Artifact ID: `202602201330-overflow-reclaim-inspect-observability`
- Commit: `fd81f61` (required real committed SHA)
- Title: `overflow reclaim inspect observability`
- Scope: `Adds deterministic overflow reclaim backlog/throughput counters and exposes them through server inspect output with E2E coverage.`

## PR Checklist

- What invariant was added or changed?
  - `Catalog` now maintains monotonic overflow reclaim lifecycle counters (`enqueued`, `dequeued`, `reclaimed_chains`, `reclaimed_pages`, `reclaim_failures`) and exposes queue depth snapshots. `inspect` now reports these counters deterministically.

- What is the crash-consistency contract for the modified path?
  - No persistent bytes changed. Counters are in-memory observability state and reset on process restart. Overflow reclaim ordering and WAL contracts remain unchanged.

- Which error classes can now be returned?
  - `none` (no new public error variants; existing overflow reclaim failures keep prior mappings).

- Does this change modify any persistent format or protocol?
  - Persistent format: `none`
  - Protocol: `none` (textual inspect diagnostics extended with an additional `INSPECT overflow ...` line only).

- Which deterministic crash/fault tests were added?
  - `src/server/e2e/overflow.zig` adds `e2e inspect exposes overflow reclaim backlog and throughput counters`.

- Which performance baseline or threshold was updated (if any)?
  - `none` (diagnostic counter increments only).
