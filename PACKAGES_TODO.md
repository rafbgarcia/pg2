# Packages & UDFs — Implementation Plan

Target: user-defined types, user-defined functions, and a package system where packages are local `.pg2` files in the project (no registry, no fetching, no package manager). See `docs/PACKAGES.md` for the full design.

## Current State

- Built-in functions (`abs`, `sqrt`, `lower`, etc.) are hardcoded across four layers: tokenizer keywords (`src/parser/tokenizer.zig`), parser recognition (`src/parser/expression.zig` — `isFunctionToken()`), AST nodes (`src/parser/ast.zig`), and executor dispatch (`src/executor/filter.zig` — `applyBuiltinFunction()` switch).
- `kw_fn` token type and `fn_def` AST node exist but are unused — no parsing or execution code.
- The catalog (`src/catalog/catalog.zig`) stores models, fields, references, scopes, and indexes. No function or type entries.
- Expression evaluation uses a work-stack evaluator in `src/executor/filter.zig`.
- Scalar types are limited to: `bigint`, `int`, `float`, `boolean`, `string`, `timestamp`.

---

## Milestone 1: Dynamic Function Registry

Move built-in functions from hardcoded switches to catalog entries. This is the foundation for everything else.

- [ ] Add a `CatalogFunction` struct to the catalog: `{ name, param_types, return_type, kind: enum { builtin, user_defined }, builtin_tag (for native fns), body_ast (for UDFs) }`.
- [ ] Pre-register all existing built-in functions (`abs`, `sqrt`, `round`, `length`, `lower`, `upper`, `trim`, `coalesce`, `now`) as catalog entries during catalog init.
- [ ] Modify the parser's function recognition: replace `isFunctionToken()` with a catalog lookup. An identifier followed by `(` that matches a catalog function name should be parsed as `expr_function_call`. The tokenizer should no longer need `fn_lower`, `fn_abs`, etc. as separate token types — they become regular identifiers resolved against the catalog.
- [ ] Modify `applyBuiltinFunction()` in `src/executor/filter.zig`: dispatch via `CatalogFunction.builtin_tag` enum instead of token type. The function's catalog entry tells the executor which native implementation to call.
- [ ] Verify all existing tests pass — this is a pure refactor with no behavior change.

## Milestone 2: User-Defined Composite Types

- [ ] Add `type` as a keyword in the tokenizer.
- [ ] Parse `type Name { field(...) ... }` declarations in the statement parser. Produce a new AST node (e.g., `stmt_type_def`). Composite types contain `field(...)` declarations, same syntax as model fields.
- [ ] Add a `CatalogType` entry to the catalog: `{ name, kind: enum { scalar, composite, enum_type }, fields (for composite) }`. Pre-register existing scalar types (`bigint`, `int`, `float`, `boolean`, `string`, `timestamp`).
- [ ] Schema loader: process `type` declarations and register them in the catalog before processing model declarations (types must resolve before they're used as field types).
- [ ] Field type resolution: when a model field references a type name that isn't a built-in scalar, look it up in the catalog's type registry.
- [ ] Storage: composite type values are serialized inline in rows. Define a serialization format (e.g., field values concatenated in declaration order, with a null bitmap prefix for nullable fields).
- [ ] Executor: support dot access on composite fields (e.g., `billing_address.city`). This means resolving `a.b` where `a` is a composite-typed field into a nested field read.
- [ ] Add tests: define a composite type, use it in a model, insert/read rows with composite values, filter on nested fields.

## Milestone 3: Enum Types

- [ ] Parse `type Name = enum(val1, val2, ...)` declarations. Produce a `stmt_enum_def` AST node (or reuse `stmt_type_def` with a flag).
- [ ] Store enum types in `CatalogType` with `kind = .enum_type` and a list of variant names.
- [ ] Storage: enum values stored as integers (variant index). The catalog maps names to indices.
- [ ] Expression support: `EnumName.variant` resolves to the integer value. Comparisons work on the underlying integer.
- [ ] Add tests: define an enum, use in a model, insert, filter by enum value.

## Milestone 4: User-Defined Functions (Expression-Only)

Simple UDFs whose bodies are single expressions (no `let`, no `if`, no `fold` yet).

- [ ] Parse `function name(param: Type, ...) -> ReturnType { expr }` declarations. Produce a `stmt_function_def` AST node containing: function name, parameter list (name + type pairs), return type, body expression.
- [ ] Register parsed functions in the catalog as `CatalogFunction` with `kind = .user_defined` and a reference to the body expression AST.
- [ ] Executor: when a UDF is called, look up the catalog entry, bind arguments to parameter names, evaluate the body expression. Argument binding means: for each parameter, push a name→value mapping that the expression evaluator can resolve when it encounters an identifier.
- [ ] Type checking: validate that argument types match parameter types at parse time (or at minimum, at execution time with a clear error).
- [ ] Add tests: define a function, call it in a query, call it in a `where` clause, call a function that calls another function.

## Milestone 5: Extended Expression Language

Add `let`, `if/then/else`, and `fold` to the expression language so function bodies can express real logic.

- [ ] **Let bindings**: parse `let name = expr` inside function bodies. Semantics: evaluate `expr`, bind result to `name` for the remainder of the body. Produces an AST node like `expr_let` with `name`, `value_expr`, and `continuation_expr`.
- [ ] **Conditionals**: parse `if expr then expr else expr`. Produces `expr_if` AST node with three children: condition, then-branch, else-branch. Must return a value (expression-level, not statement-level).
- [ ] **Fold**: parse `fold(collection, initial, fn(acc, elem) { body })`. This is the iteration primitive. Produces `expr_fold` AST node. The executor iterates over the collection, threading the accumulator through each invocation of the body.
- [ ] **Range**: `range(start, end)` as a built-in that produces a list of integers. Used with `fold` for index-based iteration.
- [ ] **List indexing**: parse `expr[expr]` for index access on `list(T)` values.
- [ ] Evaluation: extend the work-stack evaluator in `filter.zig` to handle `expr_let`, `expr_if`, `expr_fold`.
- [ ] Bounded execution: enforce max fold iterations (configurable, default e.g. 10000). Fail with an explicit error when exceeded.
- [ ] Add tests: let bindings, nested lets, if/else, fold over a list, fold with range, list indexing, exceeding fold limit produces error.

## Milestone 6: `list(T)` Type and Literals

- [ ] Parse `list(T)` as a parameterized type in field declarations.
- [ ] Storage: list values serialized as `[count (u32), element, element, ...]`. Elements use the same serialization as the inner type.
- [ ] Parse list literals: `[expr, expr, ...]`.
- [ ] Built-in `length(list)` works on lists (already exists for strings — extend to lists).
- [ ] Add tests: model with a list field, insert list values, read them back, use `length()` and indexing.

## Milestone 7: `use()` and Package Files

Packages are local `.pg2` files in the project. `use()` reads them from disk — no network access.

- [ ] Parse `package(name, version = "X.Y.Z")` as a top-level declaration (metadata only, marks a file as a package).
- [ ] Parse `use(path)` as a top-level declaration. `path` is a relative file path (e.g., `use("pg2_packages/pg2-geo.pg2")`).
- [ ] `use()` resolution: read the file from disk relative to the schema file's directory, parse it with the standard parser, register its types and functions in the catalog, then continue with the main schema. Error if the file does not exist.
- [ ] Package validation: a file with a `package(...)` header may contain type and function declarations but not model declarations (models belong to the application schema).
- [ ] Namespace: imported types and functions are available unqualified. Name collisions between packages are an error at parse time.
- [ ] Circular `use()` detection: error if A uses B and B uses A.
- [ ] Add tests: create a package file, reference it with `use()`, verify types and functions resolve. Test missing file error. Test name collision error. Test circular use error.

---

## Build Order

```
Milestone 1 (dynamic function registry)
    │
    ├── Milestone 2 (composite types)
    │       │
    │       ├── Milestone 3 (enum types)
    │       │
    │       └── Milestone 6 (list type)
    │
    └── Milestone 4 (simple UDFs)
            │
            └── Milestone 5 (let/if/fold)
                    │
                    └── Milestone 7 (use() + package files)
```

Milestones 2 and 4 can proceed in parallel after Milestone 1. Milestone 5 depends on 4. Milestones 3 and 6 can be done anytime after 2. Milestone 7 depends on 5 and 2.

## Verification

After each milestone: `zig build test` must pass. Milestones that add new syntax should include parser round-trip tests. Milestones that add execution features should include end-to-end tests (parse → execute → verify result).

## Files Likely Touched

| File | Milestones | Action |
|---|---|---|
| `src/catalog/catalog.zig` | 1, 2, 3, 4, 7 | Add CatalogFunction, CatalogType, package registry |
| `src/parser/tokenizer.zig` | 1, 2, 3, 4, 5 | Add keywords (`type`, `function`, `let`, `if`, `then`, `else`, `fold`, `fn`, `use`, `package`); refactor function token types to identifiers |
| `src/parser/expression.zig` | 1, 4, 5, 6 | Catalog-based function resolution; let/if/fold/indexing parsing |
| `src/parser/ast.zig` | 2, 3, 4, 5, 6 | New node types for type defs, function defs, let, if, fold |
| `src/parser/parser.zig` | 2, 3, 4, 7 | Parse type/function/package/use declarations |
| `src/executor/filter.zig` | 1, 4, 5, 6 | Dynamic dispatch; UDF evaluation; let/if/fold evaluation |
| `src/executor/executor.zig` | 2, 6 | Composite field access; list handling |
| `src/storage/row.zig` (or equivalent) | 2, 3, 6 | Composite/enum/list serialization |
