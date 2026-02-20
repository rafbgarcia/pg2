# Tiger Gate Artifacts

This directory stores Tiger Style PR gate artifacts for gate-changing
implementation commits.

Each artifact answers the mandatory checklist in `docs/TIGER_STYLE.md`:

- Invariant changes
- Crash-consistency contract
- Error class changes
- Persistent format/protocol impact
- Deterministic crash/fault tests
- Performance baseline/threshold impact

## How To Add One

1. Copy `docs/tiger-gates/TEMPLATE.md`.
2. Name the new file `<commit>-<short-slug>.md`.
3. Fill all checklist fields explicitly (`none` with reason where applicable).
4. Add the new file to the artifact list below.

## Artifacts

- `docs/tiger-gates/TEMPLATE.md`
- `docs/tiger-gates/a1c44df-rollback-visibility-edge-matrix.md`
- `docs/tiger-gates/e5c45f5-wal-undo-crash-visibility.md`
- `docs/tiger-gates/890c767-write-write-visibility-matrix.md`
- `docs/tiger-gates/a22b91c-milestone2-recovery-invariant-tracking.md`
- `docs/tiger-gates/810a60c-inspect-plan-metadata.md`
- `docs/tiger-gates/fa983f5-inspect-sort-group-strategy.md`
