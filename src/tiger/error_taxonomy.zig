const scan_mod = @import("../executor/scan.zig");
const mutation_mod = @import("../executor/mutation.zig");
const buffer_pool_mod = @import("../storage/buffer_pool.zig");
const wal_mod = @import("../storage/wal.zig");
const tx_mod = @import("../mvcc/transaction.zig");

/// Stable machine-actionable Tiger error classes.
pub const ErrorClass = enum(u8) {
    retryable,
    resource_exhausted,
    corruption,
    fatal,
};

pub fn classifyBufferPool(err: buffer_pool_mod.BufferPoolError) ErrorClass {
    return switch (err) {
        error.AllFramesPinned => .retryable,
        error.ChecksumMismatch => .corruption,
        error.StorageRead => .retryable,
        error.StorageWrite => .retryable,
        error.StorageFsync => .retryable,
        error.WalNotFlushed => .retryable,
    };
}

pub fn classifyScan(err: scan_mod.ScanError) ErrorClass {
    return switch (err) {
        error.AllFramesPinned => .retryable,
        error.ChecksumMismatch => .corruption,
        error.Corruption => .corruption,
        error.StorageRead => .retryable,
        error.StorageWrite => .retryable,
        error.StorageFsync => .retryable,
        error.WalNotFlushed => .retryable,
        error.ResultOverflow => .resource_exhausted,
        error.OutOfMemory => .resource_exhausted,
    };
}

pub fn classifyMutation(err: mutation_mod.MutationError) ErrorClass {
    return switch (err) {
        error.AllFramesPinned => .retryable,
        error.ChecksumMismatch => .corruption,
        error.Corruption => .corruption,
        error.StorageRead => .retryable,
        error.StorageWrite => .retryable,
        error.StorageFsync => .retryable,
        error.WalNotFlushed => .retryable,
        error.PageFull => .resource_exhausted,
        error.RowTooLarge => .resource_exhausted,
        error.BufferTooSmall => .resource_exhausted,
        error.TypeMismatch => .fatal,
        error.NullNotAllowed => .fatal,
        error.ColumnNotFound => .fatal,
        error.InvalidLiteral => .fatal,
        error.StackOverflow => .resource_exhausted,
        error.StackUnderflow => .fatal,
        error.DivisionByZero => .fatal,
        error.NumericOverflow => .fatal,
        error.UnknownFunction => .fatal,
        error.NullInPredicate => .fatal,
        error.ResultOverflow => .resource_exhausted,
        error.WalWriteError => .retryable,
        error.WalFsyncError => .retryable,
        error.OutOfMemory => .resource_exhausted,
        error.UndoLogFull => .resource_exhausted,
    };
}

pub fn classifyTxManager(err: tx_mod.TxManagerError) ErrorClass {
    return switch (err) {
        error.TooManyActiveTransactions => .resource_exhausted,
        error.TxStateWindowFull => .resource_exhausted,
        error.TransactionNotActive => .fatal,
    };
}

pub fn classifyWal(err: wal_mod.WalError) ErrorClass {
    return switch (err) {
        error.OutOfMemory => .resource_exhausted,
        error.PayloadTooLarge => .resource_exhausted,
        error.RecordBufferTooSmall => .resource_exhausted,
        error.PayloadBufferTooSmall => .resource_exhausted,
        error.WalReadError => .retryable,
        error.WalWriteError => .retryable,
        error.WalFsyncError => .retryable,
        error.InvalidEnvelope => .corruption,
        error.CorruptEnvelope => .corruption,
        error.UnsupportedEnvelopeVersion => .fatal,
    };
}

test "scan corruption class mapping" {
    try @import("std").testing.expectEqual(
        ErrorClass.corruption,
        classifyScan(error.Corruption),
    );
}
