# pg2 Query Language

## Design Principles

1. **You write what. The DB handles how.** The developer writes the logical query — which tables, which filters, which joins. The DB handles physical execution — join order, join strategy, memory management. No cost-based optimizer. Every automatic decision is visible in the stats.
2. **Pipeline syntax.** Data flows top-to-bottom through operators (`where`, `select`, `sort`, `join`), like Unix pipes.
3. **Always-on stats.** Every query returns execution stats: rows per operator, pages read, time, cache hits, and the reasoning behind every automatic decision. No EXPLAIN needed.
4. **Progressive disclosure.** The DB makes good physical decisions by default. Stats show exactly what it chose and why. Developers learn by observing, not by needing upfront knowledge of join internals or memory management.

## Syntax Overview

### Reading Data

```
-- Read all rows
orders

-- Index exact match
orders.idx_user_id |> find(5)

-- Index range
orders.idx_created_at |> between("2024-01-01", "2024-12-31")

-- Filter rows
orders |> where(total > 100)

-- Select columns
orders |> select(id, user_id, total)

-- Combine: find, where, select
orders.idx_user_id
  |> find(5)
  |> where(total > 100 AND status = "shipped")
  |> select(id, total, created_at)
  |> sort(created_at DESC)
  |> limit(10)
```

### Joins

The DB picks the join strategy at runtime based on actual row counts. The developer writes which tables to join and (optionally) the join condition. When tables are connected by a `ref`, the join condition can be omitted.

```
-- When a ref connects the two tables, on: can be omitted
orders |> where(status = "pending")
  |> join(customers)

-- Explicit condition (always works, required when multiple refs exist)
orders |> where(status = "pending")
  |> join(customers, on: orders.customer_id = customers.id)

-- Join with a filtered right side
orders.idx_user_id |> find(5)
  |> join(line_items, on: orders.id = line_items.order_id)
  |> select(orders.id, line_items.product_id, line_items.qty)
```

### `join` — Runtime-Adaptive Strategy Selection

The DB picks a join strategy at runtime based on **observed** input sizes. It doesn't predict — it starts consuming the left input, observes the actual row count (after any upstream filters, joins, or transforms), and then commits to a strategy.

The rules, applied in order once the left input size is known:

1. **Index exists on join key for the right side** AND **left input is small** (observed rows < 1000) → nested loop with index lookup.
2. **Both inputs are sorted on the join key** (e.g., both come from index range scans on the join key) → merge join.
3. **Smaller side fits in memory** (observed or estimated size < `buffer_pool_size * 0.25`) → hash join, hash table built from the smaller side.
4. **Neither side fits** → hash join with spill-to-disk (partitioned hash join).

Because the decision uses actual row counts — not table-level estimates — it handles selective filters correctly. A query that scans 1M orders but filters down to 10 rows will get a nested loop, not a hash join.

```
Stats:
  join  orders × customers  strategy: nested loop (index)
        observed: left input 12 rows (after filter)
        → 12 index lookups on customers.idx_id
        → 12 rows, 0.1ms
```

The stats always show which strategy was chosen and why. The developer learns join internals through observation — no upfront knowledge required.

### Automatic Join Ordering

When a query includes multiple `join` operators, the DB determines the execution order. The developer writes joins in whatever logical order makes sense for readability — the DB reorders them for performance.

The ordering heuristic:
1. Start from the table with the most selective `where` (fewest rows after filtering — observed at runtime).
2. Join to the table connected by ref that produces the smallest intermediate result (using ref cardinality direction: many-to-one joins don't increase row count, one-to-many joins do).
3. Repeat for remaining joins.

```
Stats:
  join order    (runtime): orders → customers → line_items
                reason: orders filtered to 340 rows; customers is many-to-one (340 output);
                        line_items is one-to-many (est. 1700 via ref + distinct_count)
  join          orders × customers   strategy: nested loop (index) → 340 rows, 0.4ms
  join          result × line_items  strategy: hash join → 1683 rows, 1.2ms
```

The stats show the chosen order and reasoning. If the DB picks a suboptimal order, creating intermediate `let` bindings gives the developer a way to structure the query so the DB has better information at each step.

### Aggregation

```
orders
  |> group(status)
  |> aggregate(count(*), sum(total))

orders.idx_user_id |> find(5)
  |> aggregate(count(*), avg(total), max(total))
```

### Mutations

```
-- Insert
insert into orders (user_id, total, status)
  values (5, 99.50, "pending")

-- Update: must specify how to find the rows
orders.idx_user_id |> find(5)
  |> where(status = "pending")
  |> update(status = "cancelled")

-- Delete
orders.idx_order_id |> find(1234)
  |> delete
```

### Schema

```
create table orders (
  id       u64 primary,
  user_id  u64 ref users.id,
  total    f64,
  status   text,
  created  timestamp
)

create index idx_user_id on orders (user_id)
create index idx_created on orders (created)

drop index idx_created
drop table orders
```

### Reference Keys

Reference keys (`ref`) declare relationships between tables. They are used by the generic `join` to infer join conditions automatically. **They do not enforce referential integrity** — no checks on insert, update, or delete. They are pure schema metadata.

```
-- Inline in create table
create table orders (
  id          u64 primary,
  customer_id u64 ref customers.id,
  total       f64,
  status      text
)

-- Standalone (add to existing table)
create ref orders.customer_id -> customers.id

-- Remove
drop ref orders.customer_id -> customers.id
```

When exactly one ref connects two tables, the generic `join` infers the `on:` condition:

```
-- These are equivalent (given ref: orders.customer_id -> customers.id)
orders |> join(customers)
orders |> join(customers, on: orders.customer_id = customers.id)
```

When multiple refs connect the same pair of tables, `on:` is required:

```
create table transfers (
  id       u64 primary,
  from_id  u64 ref accounts.id,
  to_id    u64 ref accounts.id,
  amount   f64
)

transfers |> join(accounts)
-- Error: 2 refs between transfers and accounts (from_id, to_id).
--        Specify on: explicitly.

transfers |> join(accounts, on: from_id = accounts.id)  -- OK
```

Refs are queryable:

```
refs(orders)

Result:
  ┌──────────────┬───────────────┐
  │ column       │ references    │
  ├──────────────┼───────────────┤
  │ customer_id  │ customers.id  │
  └──────────────┴───────────────┘
```

## Catalog Stats

The DB maintains lightweight stats for every table and index. All stats are O(1) to read — no sampling, no ANALYZE command, no stale statistics.

### What the DB Tracks

| Stat | Maintained how | Cost |
|---|---|---|
| `row_count` per table | +1 on insert, -1 on delete | O(1) per mutation |
| `row_count` per index | +1/-1 on index entry insert/delete | O(1) per mutation |
| `avg_row_size` per table | Running average, updated on insert/update | O(1) per mutation |
| `min`/`max` per indexed column | Updated on insert; lazy-corrected on delete | O(1) amortized |
| `distinct_count` per indexed column | HyperLogLog sketch, updated on insert | O(1) per mutation, ~approximate |
| `total_pages` per table | Updated on page allocation/deallocation | O(1) |

### Querying Stats

Stats are queryable directly — they're not hidden internals.

```
-- Get stats for a table
stats(orders)

Result:
  ┌─────────────┬──────────────┬───────────────┬─────────────┐
  │ row_count   │ avg_row_size │ total_pages   │ size_bytes  │
  ├─────────────┼──────────────┼───────────────┼─────────────┤
  │ 1200000     │ 64 bytes     │ 9375          │ 75 MB       │
  └─────────────┴──────────────┴───────────────┴─────────────┘

-- Get stats for an index
stats(orders.idx_user_id)

Result:
  ┌─────────────┬──────────┬──────────┬────────────────┐
  │ entries     │ distinct │ min      │ max            │
  ├─────────────┼──────────┼──────────┼────────────────┤
  │ 1200000     │ ~45000   │ 1        │ 52000          │
  └─────────────┴──────────┴──────────┴────────────────┘
```

### How the DB Uses Stats

Stats serve as a baseline for automatic decisions when inputs haven't been transformed (e.g., an unfiltered table scan feeding a `let` binding). When inputs have been through filters or other operators, the DB uses **runtime observation** of actual row counts instead. In both cases, the stats output shows the values that drove each choice. The developer can always override by specifying explicit operators.

The formula for estimating result set size in memory:

```
estimated_bytes = row_count * row_size
```

Where `row_size` is computed from the columns in the output at that point in the pipeline:
- **Fixed-size columns** (`u64` = 8B, `f64` = 8B, `bool` = 1B, `timestamp` = 8B): exact size from the schema. No stats needed.
- **Variable-size columns** (`text`): `avg_row_size` running average from the catalog, per column.
- **Mixed**: exact for fixed columns + average for variable columns.

If a query projects only fixed-size columns (e.g., `|> select(id, user_id, total)` → `u64 + u64 + f64` = 24 bytes), the estimate is exact with zero stat lookups. The running average is only consulted for `text` or other variable-size types.

This is compared against the buffer pool memory budget to decide whether to stream or materialize. Both the estimate and the budget are visible in stats output.

## Execution Stats

Every query response includes stats. This is not opt-in — it's always there.

```
Result: 3 rows
  ┌────┬───────┬────────────┐
  │ id │ total │ created_at │
  ├────┼───────┼────────────┤
  │ 87 │ 250.0 │ 2024-03-15 │
  │ 42 │ 180.5 │ 2024-06-01 │
  │ 91 │ 150.0 │ 2024-09-22 │
  └────┴───────┴────────────┘

Stats:
  find          idx_user_id = 5    → 47 rows, 3 pages, 0.02ms
  where         total > 100        → 12 rows passed (25%), 0.01ms
  sort          created_at DESC    → 12 rows sorted, 0.01ms
  limit         10                 → 3 rows emitted
  total                            → 6 pages read (3 cached), 0.08ms
```

The stats line per operator shows:
- Input/output row counts (so you can see selectivity at each step)
- Pages touched
- Wall clock time for that operator
- Cache hit ratio

### Hints and Warnings

When the DB detects an obviously suboptimal choice, stats include warnings with concrete suggestions:

```
Stats:
  line_items    (full read)        → 500000 rows, 12000 pages, 340ms
  where         order_id = 42      → 3 rows passed (0.0006%), 120ms
  total                            → 12000 pages read, 460ms
    ⚠ where passed 3 of 500000 rows (0.0006%) — consider creating an index on line_items.order_id

Stats:
  join       orders × line_items   strategy: nested loop → 48 rows, 312ms
    ⚠ right side scanned 500000 rows per outer row — consider adding an index on line_items.order_id
```

Warnings are based on concrete post-execution stats, not estimates. The DB is not guessing — it saw the actual selectivity and is reporting a fact. The developer decides whether to act on it.

This is the core DX advantage: you never need EXPLAIN. The stats show exactly what happened — every automatic decision, every operator's performance — and what you might want to change.

## Automatic Optimizations

The DB applies mechanical optimizations that never change the query's semantics — only the physical execution. These are unconditionally correct. Every optimization is visible in the stats output.

**Join ordering.** When a query has multiple `join` operators, the DB determines the optimal execution order based on ref metadata, catalog stats, and runtime observation. Stats show the chosen order and reasoning.

**Redundant sort elimination.** If the input is already sorted on the requested key (e.g., from an index scan), `sort` is a no-op and is skipped.

**Limit pushdown.** `limit(n)` short-circuits upstream operators — they stop after producing `n` matching rows instead of processing the entire input.

**Projection pushdown.** If downstream operators only need certain columns, upstream operators avoid reading and carrying unused columns through the pipeline.

**Dead `let` elimination.** A `let` binding that is never referenced is not executed.

**Count from catalog.** `<table> |> aggregate(count(*))` with no `where` returns the catalog's `row_count` directly. Zero I/O.

**Range tightening.** A `where` on an indexed column that follows a `between` on the same column folds into the range bounds:

```
orders.idx_created |> between("2024-01-01", "2024-12-31")
  |> where(created > "2024-06-01")
-- tightened to: between("2024-06-01", "2024-12-31"), where removed
```

## Operator Reference

| Operator | Input | Description |
|---|---|---|
| `<table>` | none | Read all rows from a table |
| `<index> \|> find(val)` | none | Exact-match index lookup |
| `<index> \|> between(lo, hi)` | none | Index range scan (inclusive) |
| `where(predicate)` | row stream | Keep rows matching predicate |
| `select(cols...)` | row stream | Select specific columns |
| `sort(col [ASC\|DESC])` | row stream | Sort rows (materializes) |
| `limit(n)` | row stream | Take first n rows |
| `offset(n)` | row stream | Skip first n rows |
| `join(right [, on:])` | row stream | Join — DB picks strategy at runtime; `on:` optional when a unique ref exists |
| `group(cols...)` | row stream | Group by columns |
| `aggregate(fns...)` | row stream | Compute aggregates (count, sum, avg, min, max) |
| `unique` | row stream | Deduplicate rows |
| `stats(<table>)` | none | Query catalog stats for a table |
| `stats(<index>)` | none | Query catalog stats for an index |
| `refs(<table>)` | none | Query reference keys for a table |

## Parser Implementation Notes

The parser should be a hand-written recursive descent parser (not a parser generator). This keeps dependencies at zero and is straightforward in Zig. The grammar is simple enough — pipeline of operators, each with arguments.

The AST represents the logical query: a linked list of operator nodes, each containing its parameters. The executor walks this list, optimizing physical execution (join order, join strategy) at runtime.

### Build Steps

1. Implement a tokenizer (lexer) for the pg2 language.
2. Implement a recursive descent parser that produces an AST.
3. Implement basic semantic validation (table exists, column names valid, types compatible).
4. Implement the executor: a `next()` iterator interface on each operator node.
5. Implement runtime stats collection in each operator.
6. Implement runtime-adaptive strategy selection in the `join` operator.
7. Implement automatic join ordering using ref metadata and catalog stats.
8. Implement reference key catalog and implicit join condition resolution.
9. Wire up: parse → validate → execute → return results + stats.
