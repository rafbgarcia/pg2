//! Server session-path feature test suite entrypoint.
//!
//! Responsibilities in this file:
//! - Aggregates all feature scenario modules for test discovery.
//! - Defines the canonical milestone-focused feature scope under one import root.
comptime {
    _ = @import("constraints/default_values_test.zig");
    _ = @import("constraints/duplicate_key_test.zig");
    _ = @import("constraints/foreign_key_violation_test.zig");
    _ = @import("constraints/nullable_test.zig");
    _ = @import("constraints/not_null_test.zig");
    _ = @import("constraints/type_sensitive_defaults_test.zig");
    _ = @import("constraints/index_backed_constraints_test.zig");
    _ = @import("schema_definition/reference_test.zig");
    _ = @import("semantic_validations/type_mismatch_test.zig");
    _ = @import("semantic_validations/unknown_field_test.zig");
    _ = @import("field_types/bool_test.zig");
    _ = @import("field_types/i8_test.zig");
    _ = @import("field_types/i16_test.zig");
    _ = @import("field_types/i32_test.zig");
    _ = @import("field_types/i64_test.zig");
    _ = @import("field_types/u8_test.zig");
    _ = @import("field_types/u16_test.zig");
    _ = @import("field_types/u32_test.zig");
    _ = @import("field_types/u64_test.zig");
    _ = @import("field_types/f64_test.zig");
    _ = @import("field_types/string_test.zig");
    _ = @import("field_types/timestamp_test.zig");
    _ = @import("expressions/delete_test.zig");
    _ = @import("expressions/insert_test.zig");
    _ = @import("expressions/select_test.zig");
    _ = @import("expressions/update_test.zig");
    _ = @import("expressions/addition_test.zig");
    _ = @import("expressions/division_test.zig");
    _ = @import("expressions/having_test.zig");
    _ = @import("expressions/in_test.zig");
    _ = @import("expressions/multiplication_test.zig");
    _ = @import("expressions/parameters_test.zig");
    _ = @import("expressions/precedence_parentheses_test.zig");
    _ = @import("expressions/sort_test.zig");
    _ = @import("expressions/stdlib/abs_test.zig");
    _ = @import("expressions/stdlib/coalesce_test.zig");
    _ = @import("expressions/stdlib/length_test.zig");
    _ = @import("expressions/stdlib/lower_test.zig");
    _ = @import("expressions/stdlib/current_timestamp_test.zig");
    _ = @import("expressions/stdlib/round_test.zig");
    _ = @import("expressions/stdlib/sqrt_test.zig");
    _ = @import("expressions/stdlib/trim_test.zig");
    _ = @import("expressions/stdlib/upper_test.zig");
    _ = @import("expressions/subtraction_test.zig");
    _ = @import("expressions/unary_minus_test.zig");
    _ = @import("expressions/where_test.zig");
    _ = @import("variables_and_multi_statement/multi_statement_read_chain_test.zig");
    _ = @import("variables_and_multi_statement/multi_statement_mutation_chain_test.zig");
    _ = @import("variables_and_multi_statement/multi_statement_atomic_rollback_test.zig");
    _ = @import("server_concurrency/multi_worker_progress_test.zig");
    _ = @import("server_concurrency/transaction_pinning_queue_pressure_test.zig");
}
