# Query Surface (Current)

This document describes the currently documented query surface for pg2 via the server session path.

Status labels:
- `supported`: covered by implemented server-path behavior.
- `in_progress`: partially implemented and/or partially covered.
- `not_yet_gated`: present in parser/executor surface but not yet release-gated by v1 E2E specs.

## Schema

- `supported`: model and field declarations (`field(...)`) through schema definitions.
- `supported`: schema `field` declarations are parenthesized-only; legacy `field name type ...` syntax is rejected fail-closed.
- `supported`: schema `index` and `uniqueIndex` declarations are parenthesized-only; legacy `index col1, col2` syntax is rejected fail-closed.
- `supported`: fail-closed explicit nullability on every field (`notNull` or `nullable` is required; implicit nullable is rejected).
- `supported`: field defaults via `default` literal in schema definitions (for example `field(status, string, notNull, default, "pending")`).
- `supported`: `reference(...)` metadata with explicit RI mode.
- `supported`: fail-closed RI configuration for unsupported referential actions (`set default` rejected).
- `supported`: maximum `128` total fields (columns) per model, including primary key and reference columns; model definitions above this limit fail closed.

## CRUD

- `supported`: `insert(...)`
- `supported`: omitted `insert(...)` fields use schema defaults when declared; explicitly assigned `null` remains `null` and does not trigger defaults.
- `supported`: `where(...) |> update(...)` including row-growth update flow.
- `supported`: `where(...) |> delete`
- `supported`: model scan reads and filtered reads with explicit returning blocks (for example `User { id }`, `User |> where(...) { id }`).
- `supported`: CRUD statements without returning blocks are rejected fail-closed (`use {} for no returned rows`).

String storage behavior (current):
- `supported`: strings above 1024 bytes are stored through overflow chains transparently.
- `supported`: replace/delete of spilled strings use deterministic logical unlink + reclaim pipeline.
- `supported`: reclaim drain budget is fixed at one committed overflow chain per successful write commit boundary; multi-chain unlinks advance backlog deterministically across subsequent committed writes.
- `supported`: malformed overflow chain reads fail closed as corruption-class errors.
- `supported`: overflow lifecycle recovery replay requires strict transaction markers (`tx_begin` + terminal marker); legacy markerless lifecycle WAL fails closed as corruption.
- `supported`: crash replay reclaims chains strictly from durable `overflow_chain_reclaim` WAL records, and write-commit drain coverage now verifies durable multi-chain reclaim across crash/restart with idempotent repeated replay.

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
