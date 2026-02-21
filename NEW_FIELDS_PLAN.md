# New Field Types Plan

This file tracks implementation of Rust-like field type names and feature tests.

## Confirmed V1 Decisions

- Use Rust-style type names only (hard switch).
- Do not support legacy aliases (`bigint`, `int`, `boolean`) in schema syntax.
- Defer `i128` and `u128` to post-V1.
- Include `f64` in V1.

## Execution Rules

- Implement one field at a time.
- For each field:
  - Add parser/type-system support.
  - Add execution/storage support.
  - Add dedicated feature test file under `test/features/field_types/`.
  - Run tests and mark as complete only when green.

## Field Rollout Checklist

- [x] `bool`
- [x] `string`
- [x] `i8`
- [x] `i16`
- [x] `i32`
- [x] `i64`
- [x] `u8`
- [x] `u16`
- [x] `u32`
- [x] `u64`
- [x] `timestamp`
- [x] `f64`

## Per-Field Test Files Checklist

- [x] `test/features/field_types/bool_test.zig`
- [x] `test/features/field_types/string_test.zig` (existing file retained and updated for Rust naming)
- [x] `test/features/field_types/i8_test.zig`
- [x] `test/features/field_types/i16_test.zig`
- [x] `test/features/field_types/i32_test.zig`
- [x] `test/features/field_types/i64_test.zig`
- [x] `test/features/field_types/u8_test.zig`
- [x] `test/features/field_types/u16_test.zig`
- [x] `test/features/field_types/u32_test.zig`
- [x] `test/features/field_types/u64_test.zig`
- [x] `test/features/field_types/timestamp_test.zig`
- [x] `test/features/field_types/f64_test.zig`

## Notes To Confirm Before Implementation

- No open type-set decisions.
