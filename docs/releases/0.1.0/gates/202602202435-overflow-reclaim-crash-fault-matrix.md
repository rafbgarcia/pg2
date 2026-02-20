# Quality Gate Artifact: 202602202435-overflow-reclaim-crash-fault-matrix

- Artifact ID: `202602202435-overflow-reclaim-crash-fault-matrix`
- Commit: `95a1e4d531c6b6d898c50580835e5a7761181cac`
- Title: `overflow reclaim crash/fault matrix coverage`
- Scope: `Adds server-path crash/restart matrix coverage around commit-hook reclaim semantics and captures a durability gap where replay applies only durable reclaim WAL records.`

## PR Checklist

- What invariant was added or changed?
  - `Reclaim replay remains strictly WAL-driven: only durable overflow reclaim lifecycle records are replay-applied after crash.`
  - `Commit-hook execution policy is validated via matrix checkpoints; in-memory queue state is not used during restart recovery.`

- What is the crash-consistency contract for the modified path?
  - `Crash recovery applies reclaim only for chains with durable committed reclaim WAL records.`
  - `Matrix coverage demonstrates a gap: a drained in-memory chain without a durable reclaim WAL record is not reclaimed by replay after crash.`

- Which error classes can now be returned?
  - `none` (test-and-doc increment; no new public error classes introduced).

- Does this change modify any persistent format or protocol?
  - Persistent format: `none`.
  - Protocol: `none`.

- Which deterministic crash/fault tests were added?
  - `src/server/e2e/overflow_reclaim_crash_matrix.zig`
    - `test "e2e crash matrix: crash after update commit replays one committed reclaim and keeps unrecorded unlink overflow"`
    - `test "e2e crash matrix: follow-up commit drains in-memory backlog but replay only applies durable reclaim WAL"`
  - `src/server/e2e/e2e_specs.zig`
    - imports crash-matrix module for server E2E test discovery.

- Which performance baseline or threshold was updated (if any)?
  - `none` (coverage-only increment; no threshold constants changed).
