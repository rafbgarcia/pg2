# Workfront 04: Advisor and Observability

## Status

🚧 IN PROGRESS

## Objective

Build a production-grade advisor module that consumes runtime/query metrics and emits actionable guidance.

## Decision Lock (2026-02-26)

These decisions are locked for current implementation unless explicitly changed.

1. Primary user interface is a one-off CLI command: `pg2 advise`.
2. `pg2 advise` outputs plain text only for now.
3. Keep v1 surface simple:
   - No additional flags in v1.
   - Show all triggered advisories in deterministic order.
4. Persist **raw metrics**, not derived metrics.
   - Example: persist `rows_scanned` and `rows_matched`.
   - Compute derived values (ratios/percentiles/threshold checks) at read/evaluation time in `pg2 advise`.
5. Persist advisor input metrics in a dedicated file under the storage root:
   - `.pg2/advisor_metrics.pg2`
6. Existing metrics already exist in code but are currently ephemeral:
   - query-level execution stats (`ExecStats`)
   - runtime queue/backpressure stats (`RuntimeInspectStats`)
   - inspect serialization output
   - this workfront adds persisted advisor metrics consumption.
7. Selectivity advisory scope is generalized (not full-table-scan-only):
   - rule targets predicate-driven operations (`SELECT`, `UPDATE`, `DELETE` with filtering)
   - trigger when `rows_matched / rows_scanned < 0.50` with sufficient repeated evidence
   - exclude `INSERT` from this advisory family
   - recommendation text points to index consideration.

## Module Direction

- New module namespace: `src/advisor/`.
- Four layers:
  1. metrics ingestion (from runtime/query execution boundaries)
  2. raw metrics persistence (`advisor_metrics.pg2`)
  3. aggregation/rule evaluation (compute derived values on read)
  4. CLI surface (`pg2 advise`)

## Non-Negotiables

1. Advisor logic must never affect query correctness or storage correctness.
2. Advisor persistence failures are fail-closed for advisor only (no crash/corruption in core query path).
3. Deterministic behavior for identical metric streams.
4. Bounded/static memory behavior in runtime paths (Tiger Style discipline).
5. Stable, versioned on-disk format for advisor metrics file.

## Phase 1: Metrics Contract and Persistence Foundation

### Scope

- Define canonical advisor metric record schema (raw values only), versioned.
- Add writer path to persist raw metric records to `.pg2/advisor_metrics.pg2`.
- Ingest from existing runtime/query stats without changing core semantics.
- Include operation type and whether predicate filtering was present.

### Minimum raw fields (v1)

- request metadata: timestamp, operation kind
- execution counters: `rows_scanned`, `rows_matched`, `rows_returned`
- mutation counters: `rows_updated`, `rows_deleted`, `rows_inserted`
- plan indicators: scan strategy, join strategy, spill triggered
- spill/temp counters: temp pages/bytes read/written
- queue/backpressure snapshot values

### Gate

- Deterministic unit tests for encode/decode and schema invariants.
- Corruption handling tests for advisor file parser/reader.
- Runtime integration tests proving writer path does not alter query results.

## Phase 2: Rule Evaluation Engine (Raw-to-Derived at Read Time)

### Scope

- Read persisted raw metrics and compute derived values in-memory for one pass.
- Deterministic advisory generation over full persisted history.
- Stable recommendation contract:
  - rule id
  - severity
  - confidence
  - evidence fields
  - action text

### v1 rules

1. queue pressure => suggest more CPU/memory or lower concurrency
2. high spill ratio => suggest more memory/work memory tuning
3. repeated low-selectivity predicates => suggest index consideration
4. latency spike detection

### Gate

- Trigger/non-trigger tests for each rule using synthetic persisted raw metrics.
- Deterministic order and stable text output contract tests.

## Phase 3: CLI Surface (`pg2 advise`)

### Scope

- Add `pg2 advise` command in `src/main.zig`.
- Command reads `.pg2/advisor_metrics.pg2`, evaluates rules, prints all advisories as text.
- No flags in v1.

### Output contract (v1)

- If advisories exist: print all advisory blocks in deterministic order.
- If none: print a deterministic "no advisories" line.

### Gate

- End-to-end CLI tests for:
  - advisory file present with triggers
  - advisory file present with no triggers
  - missing advisor metrics file
  - corrupted advisor metrics file

## Fresh Session Handoff

1. Check `src/executor/executor.zig` (`ExecStats`), `src/server/diagnostics.zig` (`RuntimeInspectStats`), and request/session boundaries for ingestion points.
2. Implement advisor metric schema and file reader/writer in `src/advisor/` first.
3. Wire persistence from runtime/query boundary with bounded overhead.
4. Add `pg2 advise` command path and v1 text renderer.
5. Add deterministic tests, then run:
   - `zig build test-all --summary all`

## Hard-Stop Conditions

- Stop if any design implies advisor persistence can corrupt or block core data/WAL flow.
- Stop if any advisory rule depends on metrics not represented in persisted raw schema.
- Stop if output contract is not deterministic for identical metric files.
- Stop on ambiguous threshold semantics that affect recommendation correctness.
