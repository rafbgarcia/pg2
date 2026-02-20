# E2E Specs (Human-Readable)

This folder defines scenario specs for pg2 v1 readiness.

Goals:

- Keep scenarios readable by humans first.
- Keep expected outputs explicit and diffable.
- Tie every scenario to a concrete readiness gate.

## Spec Format

Each `.spec` file contains:

- `meta` section: scenario id, gate, and intent.
- `schema` section: schema text to apply before the scenario.
- `steps` section: newline-delimited requests and expected responses.

Conventions:

- Requests are sent as one line per query over the server protocol.
- Response format uses current session serialization:
  - success: `OK rows=<n>` followed by zero or more CSV-like rows
  - query error: `ERR query: <message>`
  - boundary error: `ERR class=<class> code=<code>`
- `expect_contains` means substring match.
- `expect_exact` means full output equality.

## Initial Scenario Set

- `e2e/specs/01_schema_bootstrap.spec`
- `e2e/specs/02_basic_crud.spec`
- `e2e/specs/03_filter_sort_limit_offset.spec`
- `e2e/specs/04_group_aggregates.spec`
- `e2e/specs/05_referential_restrict.spec`
- `e2e/specs/06_referential_cascade.spec`
- `e2e/specs/07_referential_set_null.spec`
- `e2e/specs/08_restart_recovery.spec`
- `e2e/specs/09_error_classification.spec`
- `e2e/specs/10_inspect_output.spec`

These scenarios are the first production-readiness acceptance pack for v1.
