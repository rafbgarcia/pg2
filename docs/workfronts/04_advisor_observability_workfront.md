# Workfront 04: Advisor and Observability

## Objective
Build an extensible advisor module that consumes runtime/query metrics and emits actionable guidance.

## Module Direction
- New module namespace: `src/advisor/`.
- Three layers:
  1. metrics ingestion
  2. rolling aggregates/statistics (window + lifetime)
  3. rule engine producing typed recommendations

## Phase 1: Metrics Contract
### Scope
- Define canonical metric events and snapshots:
  - request lifecycle timing (parse/plan/execute/serialize)
  - mutation counts and latency
  - plan characteristics (full scan, join strategy, sort/group)
  - queue/backpressure stats
  - spill/temp I/O stats (raw counters produced by Workfront 03 on `ExecStats`; this phase defines the ingestion contract, not the counters themselves)

### Gate
- Deterministic metric unit tests with stable field schema.

## Phase 2: Aggregation Engine
### Scope
- Implement rolling windows and cumulative counters.
- Add percentile/average helpers for latencies.

### Gate
- Deterministic aggregation tests with synthetic event streams.

## Phase 3: Rule Engine v1
### Scope
- Implement recommendation rules with IDs/severity/evidence:
  - queue pressure => suggest more CPU/memory
  - high spill ratio => suggest more memory
  - repeated full scans on selective predicates => suggest index candidates
  - sudden latency spike detection

### Gate
- Rule trigger/non-trigger tests for each rule.

## Phase 4: Surface
### Scope
- Expose advisor output in inspect/diagnostics endpoint.
- Include recent evidence and confidence markers.

### Gate
- End-to-end tests that verify emitted advice text and referenced metrics.
