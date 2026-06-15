use serde::{Deserialize, Serialize};

/// A precise floating-point accumulator using the [Kahan Summation Algorithm](https://en.wikipedia.org/wiki/Kahan_summation_algorithm).
/// Implemented using move semantics for functional, immutable state transitions.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct KahanSum {
    sum: f64,
    c: f64,
}

impl KahanSum {
    /// Consumes the current state and yields a new [`KahanSum`] containing the updated running totals.
    pub fn add(self, value: f64) -> Self {
        let y = value - self.c;
        let t = self.sum + y;
        let c = (t - self.sum) - y;

        Self { sum: t, c }
    }

    /// Returns the current mathematically compensated sum.
    pub fn value(self) -> f64 {
        self.sum
    }

    /// Yields a fresh, zeroed-out state.
    pub fn reset(self) -> Self {
        Self::default()
    }
}
