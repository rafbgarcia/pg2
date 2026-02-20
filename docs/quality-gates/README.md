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
4. `Commit:` must reference a real commit SHA. Never leave `TBD`.
   - If SHA is unknown before the first commit, make an immediate docs follow-up commit that updates the artifact to the real SHA.
