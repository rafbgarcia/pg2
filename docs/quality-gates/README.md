# Quality Gate Artifacts

This directory stores per-increment gate evidence for core DB changes.
Policy stays in `docs/QUALITY_GATES.md`; details live here.

## Required Content (per artifact)

Each artifact must answer the six mandatory gate questions:

1. Invariant change
2. Crash-consistency contract
3. Error class changes
4. Persistent format/protocol impact
5. Deterministic crash/fault tests
6. Performance baseline/threshold impact

## How To Add One

1. Copy `docs/quality-gates/TEMPLATE.md`.
2. Name file `YYYYMMDDHHMM-<short-slug>.md`.
3. Fill all fields explicitly (`none` with reason where applicable).
4. Set `Commit:` to a real SHA before closing the session.

## Hygiene Checks

```bash
rg -n 'Commit: .*TBD|Commit: <pending>|<sha>|<answer>|<none or details>' docs/quality-gates
```

Expected matches should only appear in `docs/quality-gates/TEMPLATE.md`.
