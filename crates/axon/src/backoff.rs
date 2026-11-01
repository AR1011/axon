use std::time::Duration;

use crate::trace::random_u64;

/// Jittered exponential backoff: `initial` after the first failure, doubled after each one in a row, up to `max`.
/// A supervisor's restarts and the cluster's dials wait by it.
#[derive(Clone, Debug)]
pub struct Backoff {
    pub initial: Duration,
    pub max: Duration,
    /// The fraction of each delay cut away at random (`0.0..=1.0`), so that a delay is never longer than `max`.
    pub jitter: f64,
}

impl Default for Backoff {
    fn default() -> Self {
        Self { initial: Duration::from_millis(100), max: Duration::from_secs(30), jitter: 0.2 }
    }
}

impl Backoff {
    /// The wait after this many failures in a row. The jitter draws on [`random_u64`], seeded where a simulation runs.
    pub fn delay(&self, failures: u32) -> Duration {
        let base = self.initial.saturating_mul(1u32.checked_shl(failures.saturating_sub(1)).unwrap_or(u32::MAX));
        let cut = (random_u64() >> 11) as f64 / (1u64 << 53) as f64 * self.jitter.clamp(0.0, 1.0);
        base.min(self.max).mul_f64(1.0 - cut)
    }
}
