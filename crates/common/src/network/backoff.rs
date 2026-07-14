//! Exponential backoff with optional jitter (nautilus-style).

use std::time::Duration;

/// Exponential backoff for reconnect delays.
#[derive(Clone, Debug)]
pub struct ExponentialBackoff {
    delay_initial: Duration,
    delay_max: Duration,
    delay_current: Duration,
    factor: f64,
    jitter_ms: u64,
    immediate_reconnect: bool,
}

impl ExponentialBackoff {
    /// Create a new backoff.
    ///
    /// # Errors
    /// Returns an error if parameters are invalid.
    pub fn new(
        delay_initial: Duration,
        delay_max: Duration,
        factor: f64,
        jitter_ms: u64,
        immediate_first: bool,
    ) -> anyhow::Result<Self> {
        if delay_initial.is_zero() {
            anyhow::bail!("delay_initial must be non-zero");
        }
        if delay_max < delay_initial {
            anyhow::bail!("delay_max must be >= delay_initial");
        }
        if !(1.0..=100.0).contains(&factor) {
            anyhow::bail!("factor must be in [1.0, 100.0]");
        }

        Ok(Self {
            delay_initial,
            delay_max,
            delay_current: delay_initial,
            factor,
            jitter_ms,
            immediate_reconnect: immediate_first,
        })
    }

    /// Next delay (with jitter), advancing internal state.
    pub fn next_duration(&mut self) -> Duration {
        if self.immediate_reconnect && self.delay_current == self.delay_initial {
            self.immediate_reconnect = false;
            return Duration::ZERO;
        }

        let jitter = if self.jitter_ms == 0 {
            0
        } else {
            // Cheap deterministic-ish jitter from nanos; good enough for reconnect spacing.
            (std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.subsec_nanos() as u64)
                .unwrap_or(0))
                % (self.jitter_ms + 1)
        };

        let delay = self.delay_current + Duration::from_millis(jitter);

        let next_ms = (self.delay_current.as_secs_f64() * self.factor * 1000.0) as u64;
        let next = Duration::from_millis(next_ms).min(self.delay_max);
        self.delay_current = next.max(self.delay_initial);

        delay
    }

    /// Reset to initial delay (after a successful reconnect).
    pub fn reset(&mut self) {
        self.delay_current = self.delay_initial;
        self.immediate_reconnect = false;
    }
}
