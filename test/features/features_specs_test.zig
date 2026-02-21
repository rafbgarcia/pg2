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
    _ = @import("mutations/delete_test.zig");
    _ = @import("mutations/insert_test.zig");
    _ = @import("queries/select_test.zig");
    _ = @import("mutations/update_test.zig");
    _ = @import("expressions/addition_test.zig");
    _ = @import("expressions/in_test.zig");
    _ = @import("expressions/subtraction_test.zig");
    _ = @import("expressions/functions/sort_test.zig");
}
