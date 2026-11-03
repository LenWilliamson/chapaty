use std::collections::VecDeque;

use serde::{Deserialize, Serialize};

use crate::indicator::{
    config::{EmaWindow, RmaWindow, SmaWindow},
    streaming::StreamingIndicator,
};

// ================================================================================================
// SHARED: Exponential Weighted Mean (Base Logic)
// ================================================================================================

/// How an exponentially weighted mean computes its first value.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub enum EwmSeed {
    /// The first output is the plain average of the first `window` inputs
    /// (Wilder's original definition).
    Sma,
    /// Start from the first input. The first output appears after `window`
    /// inputs.
    #[default]
    FirstValue,
}

/// Helper for EMA-like calculations (Standard EMA and Wilder's Smoothing).
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct StreamingEwm {
    alpha: f64,
    window_size: usize,
    seed: EwmSeed,
    seed_sum: f64,
    count: usize,
    current_mean: Option<f64>,
}

impl StreamingEwm {
    #[must_use]
    pub const fn new(alpha: f64, window_size: usize, seed: EwmSeed) -> Self {
        Self {
            alpha,
            window_size,
            seed,
            seed_sum: 0.0,
            count: 0,
            current_mean: None,
        }
    }
}

impl StreamingIndicator for StreamingEwm {
    type Input = f64;
    type Output<'a> = Option<f64>;

    #[expect(
        clippy::cast_precision_loss,
        reason = "window size is always small, well under f64's exact integer range"
    )]
    fn update(&mut self, value: Self::Input) -> Self::Output<'_> {
        self.count = self.count.saturating_add(1);

        match (self.current_mean, self.seed) {
            (Some(prev), _) => {
                self.current_mean = Some((1.0 - self.alpha).mul_add(prev, self.alpha * value));
            }
            (None, EwmSeed::FirstValue) => {
                self.current_mean = Some(value);
            }
            (None, EwmSeed::Sma) => {
                self.seed_sum += value;
                if self.count >= self.window_size {
                    self.current_mean = Some(self.seed_sum / self.window_size as f64);
                }
            }
        }

        if self.count >= self.window_size {
            self.current_mean
        } else {
            None
        }
    }

    fn reset(&mut self) {
        self.seed_sum = 0.0;
        self.count = 0;
        self.current_mean = None;
    }
}

// ================================================================================================
// SMA: Simple Moving Average
// ================================================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingSma {
    window_size: usize,
    buffer: VecDeque<f64>,
    sum: f64,
}

impl StreamingSma {
    #[must_use]
    pub fn new(window_size: SmaWindow) -> Self {
        let size = window_size.0 as usize;
        Self {
            window_size: size,
            buffer: VecDeque::with_capacity(size + 1),
            sum: 0.0,
        }
    }
}

impl StreamingIndicator for StreamingSma {
    type Input = f64;
    type Output<'a> = Option<f64>;

    #[expect(
        clippy::expect_used,
        reason = "the ring-buffer length is bounded by the window size, which always fits in u32"
    )]
    fn update(&mut self, value: Self::Input) -> Self::Output<'_> {
        self.buffer.push_back(value);
        self.sum += value;

        if self.buffer.len() > self.window_size
            && let Some(removed) = self.buffer.pop_front()
        {
            self.sum -= removed;
        }

        if self.buffer.len() >= self.window_size {
            let len_u32 =
                u32::try_from(self.buffer.len()).expect("SMA window length exceeds u32 range");
            Some(self.sum / f64::from(len_u32))
        } else {
            None
        }
    }

    fn reset(&mut self) {
        self.buffer.clear();
        self.sum = 0.0;
    }
}

// ================================================================================================
// EMA: Exponential Moving Average
// ================================================================================================

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct StreamingEma {
    inner: StreamingEwm,
}

impl StreamingEma {
    /// Creates a streaming EMA with the given window size.
    ///
    /// # Panics
    ///
    /// Panics if `window_size` is zero.
    #[must_use]
    pub fn new(window_size: EmaWindow, seed: EwmSeed) -> Self {
        assert!(
            window_size.0 > 0,
            "EMA window size must be greater than zero"
        );

        let alpha = 2.0 / (f64::from(window_size.0) + 1.0);
        Self {
            inner: StreamingEwm::new(alpha, usize::from(window_size.0), seed),
        }
    }
}

impl StreamingIndicator for StreamingEma {
    type Input = f64;
    type Output<'a> = Option<f64>;

    fn update(&mut self, value: Self::Input) -> Self::Output<'_> {
        self.inner.update(value)
    }

    fn reset(&mut self) {
        self.inner.reset();
    }
}

// ================================================================================================
// RMA: Relative Moving Average
// ================================================================================================

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct StreamingRma {
    inner: StreamingEwm,
}

impl StreamingRma {
    /// Creates a streaming RMA with the given window size.
    ///
    /// # Panics
    ///
    /// Panics if `window_size` is zero.
    #[must_use]
    pub fn new(window_size: RmaWindow, seed: EwmSeed) -> Self {
        assert!(
            window_size.0 > 0,
            "RMA window size must be greater than zero"
        );

        let alpha = 1.0 / f64::from(window_size.0);
        Self {
            inner: StreamingEwm::new(alpha, usize::from(window_size.0), seed),
        }
    }
}

impl StreamingIndicator for StreamingRma {
    type Input = f64;
    type Output<'a> = Option<f64>;

    fn update(&mut self, value: Self::Input) -> Self::Output<'_> {
        self.inner.update(value)
    }

    fn reset(&mut self) {
        self.inner.reset();
    }
}

#[cfg(test)]
mod tests {
    #![expect(
        clippy::unwrap_used,
        reason = "tests assert against known-valid fixtures; unwrap surfaces failures as panics that fail the test"
    )]
    use super::*;

    /// Compare two floats with a small tolerance (EMA math isn't exact).
    fn approx(a: f64, b: f64) {
        assert!((a - b).abs() < 1e-9, "expected {b}, got {a}");
    }

    // ==========================================
    // SMA: Simple Moving Average
    // ==========================================

    #[test]
    fn sma_returns_none_until_window_is_full() {
        let mut sma = StreamingSma::new(SmaWindow(3));
        assert_eq!(sma.update(1.0), None); // 1 value
        assert_eq!(sma.update(2.0), None); // 2 values
        assert_eq!(sma.update(3.0), Some(2.0)); // full: (1+2+3)/3
    }

    #[test]
    fn sma_slides_the_window() {
        let mut sma = StreamingSma::new(SmaWindow(3));
        sma.update(1.0);
        sma.update(2.0);
        sma.update(3.0); // [1,2,3] -> 2.0
        assert_eq!(sma.update(4.0), Some(3.0)); // [2,3,4] -> 3.0
        assert_eq!(sma.update(5.0), Some(4.0)); // [3,4,5] -> 4.0
    }

    #[test]
    fn sma_window_of_one_returns_each_value() {
        let mut sma = StreamingSma::new(SmaWindow(1));
        assert_eq!(sma.update(7.0), Some(7.0));
        assert_eq!(sma.update(9.0), Some(9.0));
    }

    #[test]
    fn sma_reset_clears_state() {
        let mut sma = StreamingSma::new(SmaWindow(2));
        sma.update(10.0);
        sma.update(20.0); // Some(15.0)
        sma.reset();
        // Back to needing a full window again, and old values are gone.
        assert_eq!(sma.update(1.0), None);
        assert_eq!(sma.update(3.0), Some(2.0)); // (1+3)/2, not influenced by 10/20
    }

    // ==========================================
    // EMA: Exponential Moving Average
    // ==========================================

    #[test]
    fn ema_returns_none_until_window_is_reached() {
        let mut ema = StreamingEma::new(EmaWindow(3), EwmSeed::FirstValue);
        assert_eq!(ema.update(1.0), None);
        assert_eq!(ema.update(2.0), None);
        assert!(ema.update(3.0).is_some()); // count >= window_size
    }

    #[test]
    fn ema_seeds_with_first_value() {
        // window 1 emits immediately; the first output is just the first input.
        let mut ema = StreamingEma::new(EmaWindow(1), EwmSeed::FirstValue);
        assert_eq!(ema.update(5.0), Some(5.0));
    }

    #[test]
    fn ema_applies_alpha_recursively() {
        // alpha = 2 / (2 + 1) = 0.666...
        // v1=10 seeds mean=10 (no output yet, count 1 < 2)
        // v2=20 -> 0.6667*20 + 0.3333*10 = 16.6667 (count 2 >= 2 -> emitted)
        let mut ema = StreamingEma::new(EmaWindow(2), EwmSeed::FirstValue);
        let alpha = 2.0 / 3.0;
        assert_eq!(ema.update(10.0), None);
        let expected = (1.0_f64 - alpha).mul_add(10.0, alpha * 20.0);
        approx(ema.update(20.0).unwrap(), expected);
    }

    #[test]
    fn ema_constant_input_converges_to_that_constant() {
        // Feeding the same value forever keeps the mean at that value.
        let mut ema = StreamingEma::new(EmaWindow(5), EwmSeed::FirstValue);
        let mut last = None;
        for _ in 0..20 {
            last = ema.update(42.0);
        }
        approx(last.unwrap(), 42.0);
    }

    #[test]
    fn ema_reset_clears_state() {
        let mut ema = StreamingEma::new(EmaWindow(2), EwmSeed::FirstValue);
        ema.update(100.0);
        ema.update(200.0); // emits something
        ema.reset();
        // After reset, the next value re-seeds from scratch.
        assert_eq!(ema.update(1.0), None); // count 1 < 2 again
        approx(
            ema.update(3.0).unwrap(),
            (1.0_f64 / 3.0).mul_add(1.0, (2.0 / 3.0) * 3.0),
        );
    }

    // ==========================================
    // EWM (shared core)
    // ==========================================

    #[test]
    fn ewm_with_alpha_one_tracks_latest_value() {
        // alpha = 1 means "no memory": output is always the newest input.
        let mut ewm = StreamingEwm::new(1.0, 1, EwmSeed::FirstValue);
        assert_eq!(ewm.update(5.0), Some(5.0));
        assert_eq!(ewm.update(9.0), Some(9.0));
        assert_eq!(ewm.update(2.0), Some(2.0));
    }

    #[test]
    fn ewm_with_alpha_zero_holds_first_value() {
        // alpha = 0 means "all memory": mean stays at the seed forever.
        let mut ewm = StreamingEwm::new(0.0, 1, EwmSeed::FirstValue);
        assert_eq!(ewm.update(7.0), Some(7.0));
        assert_eq!(ewm.update(100.0), Some(7.0));
    }

    #[test]
    fn ewm_respects_window_warmup() {
        let mut ewm = StreamingEwm::new(0.5, 3, EwmSeed::FirstValue);
        assert_eq!(ewm.update(1.0), None);
        assert_eq!(ewm.update(2.0), None);
        assert!(ewm.update(3.0).is_some());
    }

    #[test]
    fn ewm_first_value_seed_starts_from_first_input() {
        // alpha 0.5, window 3, inputs 2, 4, 6: 2 -> 3 -> 4.5
        let mut ewm = StreamingEwm::new(0.5, 3, EwmSeed::FirstValue);
        assert_eq!(ewm.update(2.0), None);
        assert_eq!(ewm.update(4.0), None);
        approx(ewm.update(6.0).unwrap(), 4.5);
    }

    #[test]
    fn ewm_sma_seed_starts_from_mean_of_first_window() {
        // First output is mean(2, 4, 6) = 4.0, then 0.5 * 4 + 0.5 * 10 = 7.0
        let mut ewm = StreamingEwm::new(0.5, 3, EwmSeed::Sma);
        assert_eq!(ewm.update(2.0), None);
        assert_eq!(ewm.update(4.0), None);
        approx(ewm.update(6.0).unwrap(), 4.0);
        approx(ewm.update(10.0).unwrap(), 7.0);
    }

    #[test]
    fn ewm_sma_seed_ignores_alpha_during_warmup() {
        // alpha 0 would freeze a FirstValue mean at 2.0, but the SMA seed is
        // 4.0.
        let mut ewm = StreamingEwm::new(0.0, 3, EwmSeed::Sma);
        ewm.update(2.0);
        ewm.update(4.0);
        approx(ewm.update(6.0).unwrap(), 4.0);
        approx(ewm.update(100.0).unwrap(), 4.0); // alpha 0 holds the seed
    }

    #[test]
    fn ewm_sma_seed_reset_forgets_old_inputs() {
        let mut ewm = StreamingEwm::new(0.5, 3, EwmSeed::Sma);
        for v in [10.0, 20.0, 30.0] {
            ewm.update(v);
        }
        ewm.reset();
        assert_eq!(ewm.update(1.0), None);
        assert_eq!(ewm.update(2.0), None);
        approx(ewm.update(3.0).unwrap(), 2.0); // not influenced by 10/20/30
    }

    #[test]
    fn rma_sma_seed_matches_wilder() {
        // window 3, inputs 3, 6, 9, 12: seed = 6, then 6 + (12 - 6) / 3 = 8
        let mut rma = StreamingRma::new(RmaWindow(3), EwmSeed::Sma);
        assert_eq!(rma.update(3.0), None);
        assert_eq!(rma.update(6.0), None);
        approx(rma.update(9.0).unwrap(), 6.0);
        approx(rma.update(12.0).unwrap(), 8.0);
    }

    #[test]
    fn rma_first_value_seed_differs_from_sma_seed() {
        // 3 -> 4 -> 5.666... -> 7.777...
        let mut rma = StreamingRma::new(RmaWindow(3), EwmSeed::FirstValue);
        rma.update(3.0);
        rma.update(6.0);
        approx(rma.update(9.0).unwrap(), 17.0 / 3.0);
        approx(rma.update(12.0).unwrap(), 70.0 / 9.0);
    }

    #[test]
    fn rma_window_of_one_returns_each_value() {
        for seed in [EwmSeed::Sma, EwmSeed::FirstValue] {
            let mut rma = StreamingRma::new(RmaWindow(1), seed);
            assert_eq!(rma.update(5.0), Some(5.0));
            assert_eq!(rma.update(9.0), Some(9.0));
        }
    }

    #[test]
    #[should_panic(expected = "greater than zero")]
    fn rma_zero_window_panics() {
        let _rma = StreamingRma::new(RmaWindow(0), EwmSeed::Sma);
    }
}
