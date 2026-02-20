# Query Surface (Current)

This document describes the currently documented query surface for pg2 via the server session path.

Status labels:
- `supported`: covered by implemented server-path behavior.
- `in_progress`: partially implemented and/or partially covered.
- `not_yet_gated`: present in parser/executor surface but not yet release-gated by v1 E2E specs.

## Schema

- `supported`: model and field declarations (`field(...)`) through schema definitions.
- `supported`: `reference(...)` metadata with explicit RI mode.
- `supported`: fail-closed RI configuration for unsupported referential actions (`set default` rejected).

## CRUD

- `supported`: `insert(...)`
- `supported`: `where(...) |> update(...)` including row-growth update flow.
- `supported`: `where(...) |> delete`
- `supported`: model scan reads (`User`) and filtered reads (`User |> where(...)`)

String storage behavior (current):
- `supported`: strings above 1024 bytes are stored through overflow chains transparently.
- `supported`: replace/delete of spilled strings use deterministic logical unlink + reclaim pipeline.
- `supported`: reclaim drain budget is fixed at one committed overflow chain per successful commit boundary; multi-chain unlinks advance backlog deterministically across subsequent committed transactions.
- `supported`: malformed overflow chain reads fail closed as corruption-class errors.
- `supported`: overflow lifecycle recovery replay requires strict transaction markers (`tx_begin` + terminal marker); legacy markerless lifecycle WAL fails closed as corruption.
- `in_progress`: crash replay currently reclaims only chains with durable `overflow_chain_reclaim` WAL records; newly observed matrix coverage shows unlinked chains without durable reclaim records remain overflow after restart.

## Read Pipeline Operators

- `supported`: `where(...)`
- `in_progress`: `sort(...)`
- `in_progress`: `limit(...)`
- `in_progress`: `offset(...)`
- `in_progress`: `group(...)`
- `supported`: `inspect`
  - `inspect` includes `INSPECT overflow ...` with reclaim queue depth and reclaim throughput counters.

Notes:
- Sorting/filter examples are covered in server-path tests.
- `limit` and `offset` are implemented but still tracked as pending full v1 E2E spec completion.
- Group/aggregate behavior exists in executor code and is treated as not yet release-gated for v1.

## Referential Integrity

- `supported`: explicit `withReferentialIntegrity(onDeleteX, onUpdateY)` requirement.
- `supported`: missing RI actions fail closed (no implicit defaults).
- `supported`: `onDeleteRestrict`, `onDeleteCascade`, `onDeleteSetNull`.
- `supported`: `onUpdateRestrict`, `onUpdateCascade`, `onUpdateSetNull`.
- `supported`: `onDeleteSetDefault` and `onUpdateSetDefault` are explicitly rejected.

## Release-Gated Reality Check

Until all v1 E2E specs pass, treat this query surface as "current behavior", not "final compatibility contract".
