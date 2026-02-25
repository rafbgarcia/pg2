//! Server runtime diagnostics shared between reactor and session inspect output.
pub const RuntimeInspectStats = struct {
    queue_depth: usize = 0,
    workers_busy: usize = 0,
    pool_pinned: usize = 0,
    requests_enqueued_total: u64 = 0,
    requests_dispatched_total: u64 = 0,
    requests_completed_total: u64 = 0,
    queue_full_total: u64 = 0,
    queue_timeout_total: u64 = 0,
    max_queue_wait_ticks: u64 = 0,
    max_pin_wait_ticks: u64 = 0,
    max_pin_duration_ticks: u64 = 0,
};

pub const TxInspectStats = struct {
    active_count: u16 = 0,
    oldest_active_tx_id: u64 = 0,
    next_tx_id: u64 = 0,
    base_tx_id: u64 = 0,
};
