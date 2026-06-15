use std::collections::VecDeque;

use serde::{Deserialize, Serialize};

use crate::indicator::{
    config::{EmaWindow, SmaWindow},
    streaming::StreamingIndicator,
};

// ================================================================================================
// SHARED: Exponential Weighted Mean (Base Logic)
// ================================================================================================

/// Internal helper for EMA-like calculations (Standard EMA and Wilder's Smoothing).
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub(crate) struct StreamingEwm {
    alpha: f64,
    current_mean: Option<f64>,
    window_size: usize,
    count: usize,
}

impl StreamingEwm {
    pub(crate) fn new(alpha: f64, window_size: usize) -> Self {
        Self {
            alpha,
            current_mean: None,
            window_size,
            count: 0,
        }
    }
}

impl StreamingIndicator for StreamingEwm {
    type Input = f64;
    type Output<'a> = Option<f64>;

    fn update(&mut self, value: Self::Input) -> Self::Output<'_> {
        self.count += 1;

        match self.current_mean {
            None => {
                // First trade: initialize the mean
                self.current_mean = Some(value);
            }
            Some(prev) => {
                // Recursive update
                self.current_mean = Some(self.alpha * value + (1.0 - self.alpha) * prev);
            }
        }

        if self.count >= self.window_size {
            self.current_mean
        } else {
            None
        }
    }

    fn reset(&mut self) {
        self.current_mean = None;
        self.count = 0;
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

    fn update(&mut self, value: Self::Input) -> Self::Output<'_> {
        self.buffer.push_back(value);
        self.sum += value;

        if self.buffer.len() > self.window_size
            && let Some(removed) = self.buffer.pop_front()
        {
            self.sum -= removed;
        }

        if self.buffer.len() >= self.window_size {
            Some(self.sum / self.buffer.len() as f64)
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
    pub fn new(window_size: EmaWindow) -> Self {
        let size = window_size.0 as usize;
        // Standard EMA Alpha = 2 / (Span + 1)
        let alpha = 2.0 / (size as f64 + 1.0);
        Self {
            inner: StreamingEwm::new(alpha, size),
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

#[cfg(test)]
mod tests {
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
        let mut ema = StreamingEma::new(EmaWindow(3));
        assert_eq!(ema.update(1.0), None);
        assert_eq!(ema.update(2.0), None);
        assert!(ema.update(3.0).is_some()); // count >= window_size
    }

    #[test]
    fn ema_seeds_with_first_value() {
        // window 1 emits immediately; the first output is just the first input.
        let mut ema = StreamingEma::new(EmaWindow(1));
        assert_eq!(ema.update(5.0), Some(5.0));
    }

    #[test]
    fn ema_applies_alpha_recursively() {
        // alpha = 2 / (2 + 1) = 0.666...
        // v1=10 seeds mean=10 (no output yet, count 1 < 2)
        // v2=20 -> 0.6667*20 + 0.3333*10 = 16.6667 (count 2 >= 2 -> emitted)
        let mut ema = StreamingEma::new(EmaWindow(2));
        let alpha = 2.0 / 3.0;
        assert_eq!(ema.update(10.0), None);
        let expected = alpha * 20.0 + (1.0 - alpha) * 10.0;
        approx(ema.update(20.0).unwrap(), expected);
    }

    #[test]
    fn ema_constant_input_converges_to_that_constant() {
        // Feeding the same value forever keeps the mean at that value.
        let mut ema = StreamingEma::new(EmaWindow(5));
        let mut last = None;
        for _ in 0..20 {
            last = ema.update(42.0);
        }
        approx(last.unwrap(), 42.0);
    }

    #[test]
    fn ema_reset_clears_state() {
        let mut ema = StreamingEma::new(EmaWindow(2));
        ema.update(100.0);
        ema.update(200.0); // emits something
        ema.reset();
        // After reset, the next value re-seeds from scratch.
        assert_eq!(ema.update(1.0), None); // count 1 < 2 again
        approx(
            ema.update(3.0).unwrap(),
            (2.0 / 3.0) * 3.0 + (1.0 / 3.0) * 1.0,
        );
    }

    // ==========================================
    // EWM (shared core)
    // ==========================================

    #[test]
    fn ewm_with_alpha_one_tracks_latest_value() {
        // alpha = 1 means "no memory": output is always the newest input.
        let mut ewm = StreamingEwm::new(1.0, 1);
        assert_eq!(ewm.update(5.0), Some(5.0));
        assert_eq!(ewm.update(9.0), Some(9.0));
        assert_eq!(ewm.update(2.0), Some(2.0));
    }

    #[test]
    fn ewm_with_alpha_zero_holds_first_value() {
        // alpha = 0 means "all memory": mean stays at the seed forever.
        let mut ewm = StreamingEwm::new(0.0, 1);
        assert_eq!(ewm.update(7.0), Some(7.0));
        assert_eq!(ewm.update(100.0), Some(7.0));
    }

    #[test]
    fn ewm_respects_window_warmup() {
        let mut ewm = StreamingEwm::new(0.5, 3);
        assert_eq!(ewm.update(1.0), None);
        assert_eq!(ewm.update(2.0), None);
        assert!(ewm.update(3.0).is_some());
    }
}
