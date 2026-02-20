# Quality Gate Artifacts

This directory stores Quality Gates PR gate artifacts for gate-changing
implementation commits.

Each artifact answers the mandatory checklist in `docs/QUALITY_GATES.md`:

- Invariant changes
- Crash-consistency contract
- Error class changes
- Persistent format/protocol impact
- Deterministic crash/fault tests
- Performance baseline/threshold impact

## How To Add One

1. Copy `docs/quality-gates/TEMPLATE.md`.
2. Name the new file `YYYYMMDDHHMM-<short-slug>.md`.
3. Fill all checklist fields explicitly (`none` with reason where applicable).
4. Add the new file to the artifact list below.
5. `Commit:` must reference a real commit SHA. Never leave `TBD`.
   - If SHA is unknown before the first commit, make an immediate docs follow-up commit that updates the artifact to the real SHA.

## Artifacts

- `docs/quality-gates/202602181803-reference-ri-syntax.md`
- `docs/quality-gates/202602181947-index-bracketed-field-arrays.md`
- `docs/quality-gates/202602181954-ri-enforcement-in-mutations.md`
- `docs/quality-gates/202602181959-ri-mode-rename-fail-closed.md`
- `docs/quality-gates/202602182002-referential-update-regressions.md`
- `docs/quality-gates/202602192100-harden-fk-config-validation.md`
- `docs/quality-gates/202602192102-fail-closed-implicit-belongs-to.md`
- `docs/quality-gates/202602192118-deterministic-fk-crash-restart.md`
- `docs/quality-gates/202602192127-rollback-visibility-edge-matrix.md`
- `docs/quality-gates/202602192133-wal-undo-crash-visibility.md`
- `docs/quality-gates/202602192135-milestone2-recovery-invariant-tracking.md`
- `docs/quality-gates/202602192137-write-write-visibility-matrix.md`
- `docs/quality-gates/202602192142-inspect-plan-metadata.md`
- `docs/quality-gates/202602192145-inspect-sort-group-strategy.md`
- `docs/quality-gates/202602200000-bounded-query-string-arena.md`
- `docs/quality-gates/202602200001-e2e-simple-server-tests-structure.md`
- `docs/quality-gates/202602200002-e2e-spec-03-filter-sort-limit-offset-server-path.md`
- `docs/quality-gates/202602200003-heap-auto-compaction-threshold.md`
- `docs/quality-gates/202602200004-overflow-inline-pointer-spill-read-path.md`
- `docs/quality-gates/202602200005-overflow-page-foundation.md`
- `docs/quality-gates/202602200006-row-growth-update-crud-fix.md`
- `docs/quality-gates/202602201230-overflow-reclaim-wal-lifecycle.md`
- `docs/quality-gates/202602201330-overflow-reclaim-inspect-observability.md`
- `docs/quality-gates/202602201500-overflow-lifecycle-replay-recovery.md`
- `docs/quality-gates/202602201545-e2e-string-overflow-file-rename.md`
- `docs/quality-gates/README.md`
- `docs/quality-gates/TEMPLATE.md`
