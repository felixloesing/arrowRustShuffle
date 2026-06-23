use std::sync::atomic::{AtomicUsize, Ordering};

/// Manages global memory usage across all active partitions in the server.
/// Triggers global spills and applies backpressure based on configurable thresholds.
pub struct MemoryManager {
    limit_bytes: usize,
    used_bytes: AtomicUsize,
    high_watermark_bytes: usize,
    low_watermark_bytes: usize,
    hard_limit_bytes: usize,
}

impl MemoryManager {
    pub fn new(
        limit_bytes: usize,
        high_watermark_pct: u8,
        low_watermark_pct: u8,
        hard_limit_pct: u8,
    ) -> Self {
        let high_watermark_bytes = (limit_bytes as f64 * (high_watermark_pct as f64 / 100.0)) as usize;
        let low_watermark_bytes = (limit_bytes as f64 * (low_watermark_pct as f64 / 100.0)) as usize;
        let hard_limit_bytes = (limit_bytes as f64 * (hard_limit_pct as f64 / 100.0)) as usize;
        Self {
            limit_bytes,
            used_bytes: AtomicUsize::new(0),
            high_watermark_bytes,
            low_watermark_bytes,
            hard_limit_bytes,
        }
    }

    /// Reserves memory in bytes when a batch is pushed.
    pub fn reserve(&self, bytes: usize) {
        self.used_bytes.fetch_add(bytes, Ordering::SeqCst);
    }

    /// Releases reserved memory in bytes when a partition spills or drops.
    /// Uses a Compare-and-Swap (CAS) loop with saturating subtraction to ensure thread safety.
    pub fn release(&self, bytes: usize) {
        let mut current = self.used_bytes.load(Ordering::SeqCst);
        loop {
            let new_val = current.saturating_sub(bytes);
            match self.used_bytes.compare_exchange_weak(
                current,
                new_val,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }

    /// Returns the current amount of globally used memory.
    pub fn used_bytes(&self) -> usize {
        self.used_bytes.load(Ordering::SeqCst)
    }

    /// Returns the global memory limit.
    pub fn limit_bytes(&self) -> usize {
        self.limit_bytes
    }

    /// Checks if memory usage has crossed the high watermark.
    pub fn is_over_high_watermark(&self) -> bool {
        self.used_bytes() >= self.high_watermark_bytes
    }

    /// Checks if memory usage has crossed the hard limit.
    pub fn is_over_hard_limit(&self) -> bool {
        self.used_bytes() >= self.hard_limit_bytes
    }

    /// Checks if memory usage is safely below the low watermark.
    pub fn is_below_low_watermark(&self) -> bool {
        self.used_bytes() < self.low_watermark_bytes
    }
}
