# pg2 Query Language (with Variables)

Everything from QUERY_LANGUAGE.md applies. This version adds `let` bindings, scalar parameters, and multi-step query scripts.

## Why Variables

Without variables, complex queries become deeply nested and unreadable — the same problem SQL has with subqueries. Variables let you name intermediate results, reuse them, and build queries step by step, the way you'd write normal code.

```
-- Without variables: nested, hard to follow
orders.idx_user_id |> find(5)
  |> where(status = "shipped")
  |> join(line_items, on: orders.id = line_items.order_id)
  |> where(line_items.qty > 2)
  |> join(products, on: line_items.product_id = products.id)
  |> where(products.category = "electronics")
  |> select(orders.id, products.name, line_items.qty)

-- With variables: flat, readable, debuggable
let user_orders = orders.idx_user_id
  |> find(5)
  |> where(status = "shipped")

let bulk_items = line_items
  |> where(qty > 2)

let electronics = products
  |> where(category = "electronics")

user_orders
  |> join(bulk_items, on: id = order_id)
  |> join(electronics, on: product_id = products.id)
  |> select(orders.id, products.name, line_items.qty)
```

The second version is not just easier to read — each `let` binding can be tested independently during development. Just run the binding and reference it as the final expression to see what it produces:

```
let user_orders = orders.idx_user_id
  |> find(5)
  |> where(status = "shipped")

user_orders   -- last expression is returned to the client
```

Build each step in isolation, verify it, then compose.

## Let Bindings

```
let <name> = <pipeline>
```

### Semantics

**`let`** = "I want this result. The DB handles dataflow." The DB decides whether to stream, parallelize, share across multiple consumers, or materialize — based on catalog stats and available memory. The developer writes the logical query; the DB handles physical execution.

```
let active = users |> where(active = true)

active |> join(orders, on: users.id = orders.user_id)
active |> join(sessions, on: users.id = sessions.user_id)
-- DB sees 'active' used twice → may tee the stream or materialize,
-- depending on estimated size vs available memory
```

The DB's automatic decisions are always visible in the stats output.

### Stats for Let Bindings

Stats show each binding's execution separately, including any automatic decisions the DB made:

```
Stats:
  [user_orders]
    find          idx_user_id = 5     → 47 rows, 3 pages, 0.02ms
    where         status = "shipped"  → 12 rows passed (25%), 0.01ms
    dataflow: streamed (est. 12 rows × 24B exact = 288B ≪ budget)

  [bulk_items]
    line_items    (full read)         → 50000 rows, 1200 pages, 8.4ms
    where         qty > 2             → 310 rows passed (0.6%), 3.1ms
    dataflow: materialized (est. 50000 rows × 48B exact = 2.4MB, referenced 1×)

  [electronics]
    products      (full read)         → 8000 rows, 200 pages, 1.2ms
    where         category = "elec.." → 1200 rows passed (15%), 0.5ms
    dataflow: streamed (est. 1200 rows × 72B ~avg = 86KB ≪ budget)

  [result]
    join          on: id = order_id   strategy: hash join → 28 rows, 0.3ms
    join          on: product_id = .. strategy: hash join → 19 rows, 0.1ms
    select        3 columns           → 19 rows, 0.01ms

  total                               → 1403 pages read (890 cached), 13.6ms
  memory                              → buffer pool: 128MB, peak usage: 3.1MB
```

Each `let` binding gets its own stats block. The `dataflow:` line shows the DB's automatic decision and the numbers that drove it. You can immediately see that `bulk_items` is the expensive step and that adding an index on `line_items.order_id` would help.

### Adaptive Dataflow

The DB uses catalog stats, runtime observation, and runtime configuration to make dataflow decisions for `let` bindings. These are simple threshold rules, not cost-based optimization.

**Inputs:**
- `row_count` — for unfiltered inputs: exact, from catalog (O(1)). For filtered/transformed inputs: **observed at runtime** by consuming the pipeline and counting actual output rows.
- `row_size` — computed from the columns in the output at that pipeline stage:
  - Fixed-size columns (`u64` = 8B, `f64` = 8B, `bool` = 1B, `timestamp` = 8B): exact from schema, no stats needed.
  - Variable-size columns (`text`): per-column running average from catalog.
  - If the pipeline projects only fixed-size columns, the size is exact with zero stat lookups.
- `buffer_pool_size` — runtime configuration
- `reference_count` — how many times the binding is referenced in the script (known at parse time)

**Rules:**

| Condition | Decision |
|---|---|
| `row_count * row_size < buffer_pool_size * 0.25` AND `reference_count > 1` | Materialize (fits in memory, reused) |
| `row_count * row_size < buffer_pool_size * 0.25` AND `reference_count == 1` | Stream (fits in memory, used once — no point buffering) |
| `row_count * row_size >= buffer_pool_size * 0.25` AND `reference_count > 1` | Materialize with spill-to-disk |
| `row_count * row_size >= buffer_pool_size * 0.25` AND `reference_count == 1` | Stream (never materialize if used once) |

**Parallelism:** independent `let` bindings (no data dependency between them) can execute concurrently. The DB identifies independent bindings at parse time and reports parallel execution in the stats:

```
Stats:
  [user_orders]  ─┐
    ...            │ parallel
  [bulk_items]   ─┘
    ...
  [result]
    join ...
```

**Stream tee-ing:** if a `let` binding is referenced by two consumers and the estimated size is small enough to buffer the delta between consumer rates, the DB runs the pipeline once and fans the output to both consumers. If the buffer pressure exceeds a threshold, it falls back to materializing. The stats show this:

```
  [active_users]  tee'd to 2 consumers (buffer peak: 12KB)
```

All of these decisions are deterministic given the same catalog stats and configuration. No randomness, no heuristics that change behavior unpredictably.

## Scalar Parameters

`$name` declares a scalar parameter. These are typed and bound at query execution time.

```
let $uid: u64 = 5
let $min_total: f64 = 100.0
let $status: text = "shipped"

orders.idx_user_id
  |> find($uid)
  |> where(total > $min_total AND status = $status)
  |> select(id, total, created_at)
```

### Why Not Just Inline Values?

1. **Prepared statements.** The server can parse the query once and re-execute with different parameter values, skipping re-parsing.
2. **Injection safety.** Parameters are typed values, not string interpolation.
3. **Readability.** Named parameters document intent: `$min_total` is clearer than a bare `100.0` buried in a predicate.

### Client-Side Binding

Parameters can be passed from the client driver:

```python
# Python client pseudocode
conn.execute("""
    orders.idx_user_id
      |> find($uid)
      |> where(total > $min_total)
""", uid=5, min_total=100.0)
```

## Multi-Step Scripts

Multiple statements in sequence, separated by newlines. Each statement can be a `let` binding, a query, or a mutation. The last expression is the result returned to the client.

```
-- Find the user's most expensive pending order and cancel it
let $uid: u64 = 42

let pending = orders.idx_user_id
  |> find($uid)
  |> where(status = "pending")

let target = pending
  |> sort(total DESC)
  |> limit(1)

-- This is a mutation that uses the 'target' binding
target |> update(status = "cancelled")
```

Stats show all steps, including the mutation:

```
Stats:
  [pending]
    find          idx_user_id = 42   → 8 rows, 1 page, 0.01ms
    where         status = "pending" → 3 rows passed (37%), 0.01ms

  [target]
    sort          total DESC         → 3 rows sorted, 0.01ms
    limit         1                  → 1 row emitted

  [mutation]
    update        status = "cancel.."→ 1 row updated, 1 page written, 0.03ms
    wal_write     1 record           → 0.02ms

  total                              → 2 pages read, 1 page written, 0.08ms
```

## Inspect Mode

During development, add `|> inspect` at any point in a pipeline to see intermediate results without changing the query's behavior. It passes rows through unchanged but prints them.

```
let pending = orders.idx_user_id
  |> find(42)
  |> inspect              -- prints all rows at this point
  |> where(status = "pending")
  |> inspect              -- prints rows after where

pending |> sort(total DESC) |> limit(1)
```

Output:

```
[inspect @ line 3]  8 rows:
  {id: 10, user_id: 42, total: 250.0, status: "pending"}
  {id: 11, user_id: 42, total: 50.0,  status: "shipped"}
  {id: 14, user_id: 42, total: 180.0, status: "pending"}
  ...

[inspect @ line 5]  3 rows:
  {id: 10, user_id: 42, total: 250.0, status: "pending"}
  {id: 14, user_id: 42, total: 180.0, status: "pending"}
  {id: 19, user_id: 42, total: 90.0,  status: "pending"}

Result: 1 row
  {id: 10, user_id: 42, total: 250.0, status: "pending"}
```

This is the database equivalent of `console.log` debugging — zero friction, always available.

## Conditional Logic

Simple `if/else` on scalar values. This is not a full programming language — it's just enough to express conditional query plans.

```
let $use_index: bool = true

let results = if $use_index then
    orders.idx_user_id |> find(5)
  else
    orders |> where(user_id = 5)

results |> select(id, total)
```

This lets developers A/B test execution strategies explicitly:

```
-- "Is the index actually faster for this query?"
let $uid: u64 = 5

let via_index = orders.idx_user_id |> find($uid) |> where(total > 100)
let via_scan  = orders |> where(user_id = $uid AND total > 100)

-- Run both paths and compare stats
via_index |> select(id, total) |> inspect
via_scan  |> select(id, total) |> inspect
```

## Operator Reference (Additions)

| Operator | Input | Description |
|---|---|---|
| `let <name> = <pipeline>` | none | Bind a row stream — DB handles dataflow automatically |
| `let $name: type = value` | none | Declare a typed scalar parameter |
| `inspect` | row stream | Print intermediate rows (passthrough) |
| `if <cond> then <a> else <b>` | none | Conditional plan selection |

## Security

### Injection is Structurally Impossible

SQL injection works because SQL mixes code and data in the same string. A malicious input becomes part of the query syntax. pg2 eliminates this by design.

**The wire protocol separates query text from parameter values.** They are two distinct fields. The server never interpolates parameter values into the query string.

```
Wire protocol message:
  field 1 (query):  "orders.idx_user_id |> find($uid) |> where(status = $s)"
  field 2 (params): {uid: 5, status: "shipped"}
```

The parser sees `$uid` as a typed placeholder. A malicious value like `5) |> delete; users` is just the string `"5) |> delete; users"` — it's a parameter value, not query text. It never reaches the parser.

**In client driver usage, all dynamic values must be `$param` references.** The driver API enforces this:

```python
# Python driver
conn.execute(
    "orders.idx_user_id |> find($uid) |> where(status = $s)",
    uid=5, status="shipped"
)
```

There is no API for sending a query string with interpolated values. The unsafe path doesn't exist.

**For table and column names** (which can't be parameterized): the parser validates them against the catalog at parse time. If a table or column doesn't exist, it's a validation error. You can't reference arbitrary objects.

### What About REPL / Interactive Mode?

In interactive mode (direct connection, development), inline literal values are allowed for convenience:

```
orders.idx_user_id |> find(5)
```

This is fine because the developer is typing the query directly. There's no untrusted input being interpolated. The wire protocol for interactive mode uses a single-field message (query text only, no params field).

### No ORM Required

The security guarantee is at the protocol and parser level. Client drivers enforce it. ORMs and query builders can exist for convenience, but they're not needed for safety. This is a deliberate DX choice: security is the database's responsibility, not the application's.

## Implementation Notes

### Scoping

Variables are lexically scoped to the script. No nested scopes, no closures, no functions. This is deliberately simple — the query language is not a general-purpose programming language.

### Let Dataflow Implementation

The executor tracks reference count and estimated result size at plan time. At execution time, it picks a dataflow strategy (stream, materialize, tee) based on the adaptive dataflow rules. The choice is recorded and reported in stats.

### Parameter Binding

The parser produces parameter placeholders in the AST. Before execution, the server resolves all `$name` references to concrete values from either inline `let $name` declarations or client-provided bindings. Unresolved parameters are a validation error, not a runtime error.
