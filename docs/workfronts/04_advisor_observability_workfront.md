# Workfront 04: Advisor and Observability

## Status

🚧 IN PROGRESS

### Current Implementation Snapshot (2026-02-26)

- ✅ `src/advisor/metrics.zig` landed:
  - versioned on-disk raw metrics file format
  - deterministic append/read path for `.pg2/advisor_metrics.pg2`
  - corruption/truncation detection tests
- ✅ `src/advisor/rules.zig` landed with v1 low-selectivity advisory:
  - evaluates persisted raw metrics
  - targets predicate-driven `SELECT`/`UPDATE`/`DELETE`
  - triggers when `rows_matched / rows_scanned < 0.50`
  - emits index-consideration action text
- ✅ `pg2 advise` command landed (plain text, no flags).
- ✅ Session ingestion wiring landed:
  - persists raw metric records on successful executed requests
  - uses storage-root-local advisor metrics file
  - fail-closed behavior (advisor append failures do not break query execution)
- ✅ Gate run after landing: `zig build test-all --summary all` passed.
- ✅ Decision lock extension (critical-path hardening):
  - no advisory rule evaluation on statement path
  - statement path performs bounded enqueue only
  - advisor persistence is asynchronous background flush
  - queue overflow drops metrics (increments dropped counter) instead of blocking
  - no keep/drop rule logic on statement path (preserve denominators and avoid coupling)
- ⏳ Remaining from original scope:
  - queue pressure rule
  - spill ratio rule
  - latency spike rule
  - parse/plan/execute/serialize phase timing ingestion

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
6. No synchronous advisory file I/O on the critical statement execution path.

## Phase 1: Metrics Contract and Persistence Foundation

### Progress

- ✅ DONE: versioned raw metric schema and persistence file (`advisor_metrics.pg2`)
- ✅ DONE: async sink with bounded queue and non-blocking enqueue from statement path
- ✅ DONE: queue-overflow drop behavior (no request-path blocking)
- ✅ DONE: deterministic persistence/corruption tests
- ⏳ MISSING: raw phase timing fields (`parse_ns`, `plan_ns`, `execute_ns`, `serialize_ns`, `total_ns`)

### Scope

- Define canonical advisor metric record schema (raw values only), versioned.
- Add async writer path to persist raw metric records to `.pg2/advisor_metrics.pg2`.
- Ingest from existing runtime/query stats without changing core semantics.
- Include operation type and whether predicate filtering was present.
- Introduce bounded in-memory queue for statement-path metric handoff.

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
- Runtime integration tests proving enqueue path does not alter query results.
- Runtime tests proving queue overflow drops metrics instead of blocking.

## Phase 2: Rule Evaluation Engine (Raw-to-Derived at Read Time)

### Progress

- ✅ DONE: rule engine framework + deterministic text formatter
- ✅ DONE: low-selectivity predicate advisory rule
- ⏳ MISSING: queue pressure rule
- ⏳ MISSING: spill ratio rule
- ⏳ MISSING: latency spike rule
- ⏳ MISSING: synthetic trigger/non-trigger tests for missing rules

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

### Progress

- ✅ DONE: `pg2 advise` command in `src/main.zig`
- ✅ DONE: plain text deterministic output with all triggered advisories
- ✅ DONE: deterministic no-advisory output line
- ⏳ MISSING: explicit CLI tests for missing/corrupt advisor file paths and trigger/no-trigger matrix

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

### Already Landed

1. `docs` decision locks and phase plan updates.
2. `src/advisor/metrics.zig`: versioned raw metrics file format and reader/writer.
3. `src/advisor/sink.zig`: bounded queue + async background writer.
4. `src/advisor/rules.zig`: low-selectivity rule and text formatter.
5. `src/main.zig`: `pg2 advise` command and sink startup in server mode.
6. `src/server/session.zig`: enqueue-only metric capture path.

### Next Session Start Here

1. Add phase timing raw fields to `MetricRecord` and populate in session/request boundary.
2. Implement rule: queue pressure (using persisted queue/backpressure counters).
3. Implement rule: spill ratio (using persisted spill/temp counters).
4. Implement rule: latency spike (using persisted latency fields after step 1).
5. Add deterministic tests for trigger/non-trigger per new rule.
6. Add/expand CLI-level tests for missing/corrupt metrics file behavior and multi-rule output.
7. Run gate:
   - `zig build test-all --summary all`

## Hard-Stop Conditions

- Stop if any design implies advisor persistence can corrupt or block core data/WAL flow.
- Stop if any advisory rule depends on metrics not represented in persisted raw schema.
- Stop if output contract is not deterministic for identical metric files.
- Stop on ambiguous threshold semantics that affect recommendation correctness.
