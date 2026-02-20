# Errors And Responses (Current)

pg2 server responses are line-oriented text.

## Success Shape

- Query success starts with: `OK rows=<n>`
- Each returned row is comma-separated on its own line.
- `inspect` appends `INSPECT ...` lines after rows.
  - includes `INSPECT overflow ...` reclaim backlog/throughput counters.

Example:

```text
OK rows=2
2,Alice,true
1,Charlie,true
```

## Error Shape

There are three top-level query-processing error prefixes:

- `ERR tokenize: <message>`
- `ERR parse: <message>`
- `ERR query: <message>`

Session boundary/resource failures return:

- `ERR class=<error_class> code=<error_code>`

Current error classes:
- `retryable`
- `resource_exhausted`
- `corruption`
- `fatal`

## Practical Guidance

- For `ERR tokenize` or `ERR parse`: treat as client/query-shape errors and fix the query text.
- For `ERR query`: inspect message details (for example missing model/column, unsupported function, RI violation, capacity limits).
- Failed queries are fail-closed for transaction-scoped overflow reclaim intents: pending reclaim entries from that request are aborted, not reclaimed.
- Overflow reclaim backlog progression is deterministic: at most one committed overflow chain is reclaimed per successful commit boundary.
- For `ERR class=resource_exhausted ...`: retry may succeed after load decreases or capacity is adjusted.
- For `ERR class=corruption ...`: treat as operational incident.
- For `ERR class=fatal ...`: treat as deterministic invalid operation/configuration unless docs state retry behavior.

## Current Overflow-Related Error Codes

When surfaced through `ERR query: ... class=<x>; code=<CodeName>`:

- `OverflowRegionExhausted` (`resource_exhausted`):
  - overflow page-id region cannot allocate new chain pages.
- `OverflowReclaimQueueFull` (`resource_exhausted`):
  - deterministic reclaim backlog reached configured queue capacity.
- `Corruption` (`corruption`):
  - malformed overflow pointer/page/chain detected (fail-closed).

## Stability Note

Error prefix format and class/code boundary format are the stable integration points for clients.
Specific free-text message wording should be treated as informative but potentially evolving.
