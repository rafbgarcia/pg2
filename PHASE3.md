# Phase 3: Query Layer — Implementation Plan

## Context

Phase 1 (foundation) and Phase 2 (storage engine) are complete. The storage layer provides: 8KB pages with CRC checksums, a buffer pool with clock-sweep eviction, WAL with crash recovery, B+ tree indexes, slotted-page heap storage, and undo-log MVCC with snapshot isolation. Phase 3 builds the query layer on top: parser, catalog, and executor.

## New Files

```
src/
  storage/
    row.zig              # Row encoding/decoding (typed values ↔ heap bytes)
  parser/
    tokenizer.zig        # Single-pass tokenizer → fixed-capacity token array
    ast.zig              # AST node types (flat array with index references)
    expression.zig       # Shunting-yard expression parser (no recursion)
    parser.zig           # Pipeline/statement parser (iterative, explicit nesting stack)
  catalog/
    catalog.zig          # Model metadata, field defs, associations, indexes, O(1) stats
    schema_loader.zig    # Populate catalog from parsed schema AST
  executor/
    executor.zig         # Query orchestration, pipeline planning
    scan.zig             # Table scan, index find, index range scan
    filter.zig           # Expression evaluation, predicate checking
    sort.zig             # In-arena sort (bounded merge sort)
    aggregate.zig        # Group-by, count/sum/avg/min/max accumulators
    mutation.zig         # Insert, update, delete (heap + index + WAL + undo)
    join.zig             # Runtime-adaptive join strategy selection + execution
```

Update `src/pg2.zig` to re-export all new modules and add them to comptime test discovery.

## Build Order (6 milestones, each independently testable)

### Milestone 1: Row Encoding + Tokenizer + AST types ✅

**`src/storage/row.zig`** — Bridge between raw heap bytes and typed values.

- `ColumnType` enum: `bigint` (i64, 8B), `int` (i32, 4B), `float` (f64, 8B), `boolean` (bool, 1B), `string` (variable, u16-length-prefixed), `timestamp` (i64, 8B)
- `Value` tagged union: one variant per type + `null_value`
- `RowSchema` struct: column definitions, fixed_size_bytes total, null_bitmap_bytes
- `encodeRow(schema, values, buf) → bytes_written` — null bitmap + fixed columns + variable-length data
- `decodeColumn(schema, row_data, col_index) → Value` — decode single column
- `compareValues(a, b) → Order` — for sorting and predicates
- Tests: roundtrip every type, null handling, mixed schemas, value comparison

**`src/parser/tokenizer.zig`** — Single-pass state machine, fills fixed-capacity token buffer.

- `Token` struct: `token_type: TokenType`, `start: u32`, `len: u16`, `line: u16`
- `TokenType` enum: ~60 variants covering literals, identifiers (snake_case vs PascalCase), keywords (`where`, `sort`, `let`, `fn`, `pipe`, `field`, `hasMany`, etc.), operators (`|>`, `=`, `!=`, `<=`, `>=`, `+`, `-`, `*`, `/`), punctuation (`{}()[],.:`), aggregate names, built-in scalar function names, duration suffixes
- `max_tokens = 4096` — fixed-capacity buffer, no allocation
- PascalCase detection: first char uppercase → `model_name` token. `$` prefix → `parameter` token
- Keyword lookup: after scanning identifier text, check against sorted keyword list
- Comments: `--` to end of line (skipped)
- Tests: each token type, edge cases (empty, max tokens, unterminated string, comments, multi-line)

**`src/parser/ast.zig`** — Flat node array with index references (no pointers, no allocation).

- `max_ast_nodes = 1024`, `NodeIndex = u16`, `null_node = maxInt(u16)`
- `NodeTag` enum: ~40 tags covering pipeline, operators, expressions, selection sets, schema constructs, mutations, control flow, introspection
- `AstNode` struct: `tag: NodeTag` + `data: NodeData` (union with variants for binary/unary expressions, pipeline chains, selection fields, literals, etc.)
- Linked lists via `next: NodeIndex` fields for operator chains, field lists, argument lists
- `Ast` struct: fixed array of nodes + `node_count` + `root` index
- Tests: node allocation, capacity overflow, linked list traversal

### Milestone 2: Expression Parser + Main Parser ✅

**`src/parser/expression.zig`** — Shunting-yard with explicit fixed-capacity stacks.

- Operator stack (`max_operator_stack = 64`) and output stack (`max_output_stack = 128`)
- Precedence table: `not`(1) > `*,/`(2) > `+,-`(3) > `=,!=,<,>,<=,>=`(4) > `in,not in`(5) > `and`(6) > `or`(7)
- Handles: binary ops, unary `not`, parentheses, function calls (`lower(email)`), aggregate calls (`count(*)`), list literals (`[1, 2, 3]`), duration expressions (combines `integer` + `.` + `days`)
- End-of-expression detection: stops at unmatched `)`, `|>`, `{`, `}`, `asc`, `desc`, `end_of_input`
- Tests: precedence correctness, parens override, function calls, nested expressions, `in`/`not in`, duration

**`src/parser/parser.zig`** — Iterative parser with explicit nesting stack (`max_nesting_depth = 16`).

- Top-level dispatch loop: `let` → binding, `fn` → function def, `pipe` → macro def, `stats(` → introspection, `PascalCase` → schema or query (disambiguate by first token in braces: `field`/`hasMany`/etc. → schema, else → query)
- Pipeline parsing: simple loop — parse source (`Model`, `Model.index`, let ref), consume `|>` tokens, parse operator, link into chain
- Selection set parsing: explicit `nesting_stack` — push context when entering nested `{ ... }`, pop when hitting `}`. Handles nested relations with their own pipelines.
- Each operator parser: `where(expr)`, `sort(key [asc|desc], ...)`, `limit(expr)`, `offset(expr)`, `group(field, ...)`, `insert(field = expr, ...)`, `update(field = expr, ...)`, `delete`, `unique`, `inspect`, scope reference
- Error reporting: line number from token, descriptive messages
- Tests: simple queries, pipelines, nested selection sets (2-3 levels), schema definitions, let bindings, mutations, all operator types, error cases

### Milestone 3: Catalog + Schema Loading ✅

**`src/catalog/catalog.zig`** — Fixed-capacity metadata store.

- Capacity limits: `max_models = 256`, `max_columns_per_model = 128`, `max_indexes_per_model = 32`, `max_associations_per_model = 32`, `max_scopes_per_model = 32`
- `ModelInfo`: columns, indexes, associations, scopes, heap_first_page_id, row_schema, plus O(1) stats (row_count, avg_row_size_bytes, total_pages)
- `IndexInfo`: column_ids, btree_root_page_id, plus O(1) stats (entry_count, distinct_count via HyperLogLog, min/max values)
- `AssociationInfo`: kind (has_one/has_many/belongs_to), target model, foreign key column, local column
- All names stored in a contiguous name buffer (`max_name_bytes = 64 * 1024`)
- Lookup functions: `findModel(name)`, `findColumn(model, name)`, `findIndex(model, name)`, `findAssociation(model, name)`, `findScope(model, name)`
- Stats update functions: `incrementRowCount`, `decrementRowCount`, `updateAvgRowSize`, `updateIndexStats` — called by executor on mutations
- `seal()` — after schema loading, prevents further additions
- Tests: build catalog for User+Post schema, verify lookups, test capacity limits

**`src/catalog/schema_loader.zig`** — Parse schema AST → populate catalog.

- `loadSchema(catalog, ast, source)` — walks schema AST nodes, calls catalog add methods
- Validation: `primaryKey` requires `notNull`, association targets must exist, no duplicate names
- Derives `RowSchema` from column definitions for row encoding
- Tests: load User+Post schema, verify all fields/associations/indexes/scopes, error cases

### Milestone 4: Basic Executor (scan + filter + mutations) ✅

**`src/executor/executor.zig`** — Query orchestration.

- `ExecContext`: holds references to catalog, buffer_pool, wal, tx_manager, undo_log, clock, plus per-query arena and transaction state
- `execute(ast) → QueryResult` — dispatches based on AST root node type
- `QueryResult`: rows (encoded in arena) + row_count + `ExecStats`
- `ExecStats`: per-operator row counts, pages read/written, timing, strategy descriptions
- Arena reset after each query (no allocation, just pointer reset)

**`src/executor/scan.zig`** — Data access operators.

- `tableScan(ctx, model_id)` — iterate all heap pages, decode rows, check MVCC visibility via undo_log.findVisible + snapshot
- `indexFind(ctx, model_id, index_id, key)` — btree.find → heap read → visibility check
- `indexRange(ctx, model_id, index_id, lo, hi)` — btree.rangeScan iterator → heap reads → visibility checks

**`src/executor/filter.zig`** — Expression evaluation.

- `evaluateExpression(ctx, ast, node_index, row, schema) → Value` — iterative AST walk with explicit stack (no recursion). Handles binary ops, unary not, column refs, literals, function calls, aggregates.
- `evaluatePredicate(ctx, ast, node_index, row, schema) → bool` — wraps evaluateExpression, asserts boolean result
- Built-in scalar functions: `now()`, `lower()`, `upper()`, `trim()`, `length()`, `abs()`, `sqrt()`, `round()`, `coalesce()`

**`src/executor/mutation.zig`** — Write operations.

- `executeInsert(ctx, model_id, values)` — encode row → find/allocate heap page → HeapPage.insert → update btree indexes → WAL append → update catalog stats
- `executeUpdate(ctx, model_id, row_id, old_data, new_values)` — push undo → HeapPage.update → update indexes → WAL append
- `executeDelete(ctx, model_id, row_id, old_data)` — push undo → HeapPage.delete → update indexes → WAL append
- Tests: insert rows, read them back via scan, filter with predicates, update, delete, verify MVCC visibility

### Milestone 5: Sort + Aggregation + Joins

**`src/executor/sort.zig`** — Materializes into arena, sorts in-place.

- Bounded by arena memory budget — if rows exceed budget, return explicit error
- Insertion sort for ≤64 rows, merge sort for larger inputs
- Multi-key sort support (for `sort(field1 asc, field2 desc)`)

**`src/executor/aggregate.zig`** — Grouping and accumulation.

- Group-by: hash groups into arena-allocated buckets (fixed max bucket count)
- Accumulators: `count` (u64 counter), `sum` (f64/i64), `avg` (sum + count), `min`/`max` (Value comparison)
- Whole-table aggregation: when selection set has only aggregates, treat entire input as one group
- Post-group `where`: filter on aggregate values (the HAVING equivalent)

**`src/executor/join.zig`** — Runtime-adaptive join for nested relations.

- `chooseJoinStrategy(left_count, index_exists, left_sorted, right_sorted, smaller_side_bytes, budget_bytes) → JoinStrategy`
- Strategy 1: Index exists + left < 1000 rows → nested loop with index lookup
- Strategy 2: Both sorted on join key → merge join
- Strategy 3: Smaller side < 25% memory budget → hash join
- Strategy 4: Neither fits → error for now (partitioned hash join deferred to later)
- Stats record which strategy was chosen and why

### Milestone 6: Integration + End-to-End Tests

- Update `src/pg2.zig` with new module re-exports and comptime test discovery
- End-to-end test: parse schema → load catalog → parse query → execute against SimulatedDisk → verify results and stats
- Test the full query examples from the language spec:
  - `User { id email name }` (basic scan)
  - `User |> where(active = true) |> sort(name asc) |> limit(10) { id email }` (pipeline)
  - `User |> active { id, posts |> published { id title } }` (nested relations + scopes)
  - `Post |> group(author_id) |> sort(count(*) desc) |> limit(10) { author_id, post_count: count(*) }` (aggregation)
  - `User |> insert(email = "a@b.com", name = "A") { id email }` (mutation)
  - Let bindings, index access, computed fields

## Key Design Decisions

1. **Flat AST with node indices** — not pointers. Fixed-capacity array (`max_ast_nodes = 1024`). Nodes link via `NodeIndex` (u16). Bounded, copyable, no heap allocation after init.

2. **Shunting-yard for expressions** — not Pratt parsing (which is recursive). Two explicit stacks (operator + output), single iterative loop. Follows no-recursion rule.

3. **Explicit nesting stack for selection sets** — instead of recursive descent. `max_nesting_depth = 16`. Parser pushes context when entering `{ ... }`, pops on `}`.

4. **Row encoding as storage-layer module** — `src/storage/row.zig` bridges typed Values and raw heap bytes. Available to both catalog (schema) and executor (row access).

5. **Catalog in its own directory** — neither storage nor parser nor executor. All three depend on it. Fixed-capacity, sealed after schema loading.

6. **Iterator-style execution but no virtual dispatch** — executor pre-plans a flat array of `OpDescriptor` entries, runs them in sequence. Avoids both recursion and dynamic dispatch.

## Verification

After each milestone:

```bash
zig build test    # All inline tests pass (existing + new)
```

After milestone 6:

```bash
zig build test    # Full end-to-end: parse → catalog → execute → verify results
zig build         # Clean build, no warnings
```
