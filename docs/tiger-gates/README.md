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
