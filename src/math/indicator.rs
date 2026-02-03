use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

/// A trait for incremental indicators.
/// Designed to be object-safe so agents can hold `Box<dyn StreamingIndicator>`.
pub trait StreamingIndicator: std::fmt::Debug + Send + Sync {
    /// Update the indicator with the latest scalar value (e.g., close price).
    /// Returns `Some(value)` if the indicator is warm (enough data seen), otherwise `None`.
    fn update(&mut self, value: f64) -> Option<f64>;

    /// Reset the internal state to clear history (e.g., for a new trading session).
    fn reset(&mut self);
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
    pub fn new(window_size: u16) -> Self {
        let size = window_size as usize;
        Self {
            window_size: size,
            buffer: VecDeque::with_capacity(size),
            sum: 0.0,
        }
    }
}

impl StreamingIndicator for StreamingSma {
    fn update(&mut self, value: f64) -> Option<f64> {
        // 1. Add new value to window
        self.buffer.push_back(value);
        self.sum += value;

        // 2. Remove old value if we exceeded window size
        if self.buffer.len() > self.window_size {
            // Safety: We just pushed, so unwrap is safe, but idiomatic rust prefers matching
            if let Some(removed) = self.buffer.pop_front() {
                self.sum -= removed;
            }
        }

        // 3. Check readiness
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
// SHARED: Exponential Weighted Mean (Base Logic)
// ================================================================================================

/// Internal helper for EMA-like calculations (Standard EMA and Wilder's Smoothing).
/// Implements the recursive formula: $y_t = \alpha * x_t + (1 - \alpha) * y_{t-1}$.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StreamingEwm {
    alpha: f64,
    current_mean: f64,
    initialized: bool,
    window_size: usize,
    count: usize,
}

impl StreamingEwm {
    fn new(alpha: f64, window_size: usize) -> Self {
        Self {
            alpha,
            current_mean: 0.0,
            initialized: false,
            window_size,
            count: 0,
        }
    }

    fn update(&mut self, value: f64) -> Option<f64> {
        if !self.initialized {
            // Per Polars/Pandas `adjust=false`: initialize with the first value
            self.current_mean = value;
            self.initialized = true;
            self.count = 1;
        } else {
            // Recursive update: Mean = Alpha * Val + (1 - Alpha) * Prev
            self.current_mean = self.alpha * value + (1.0 - self.alpha) * self.current_mean;
            self.count += 1;
        }

        if self.count >= self.window_size {
            Some(self.current_mean)
        } else {
            None
        }
    }

    fn reset(&mut self) {
        self.initialized = false;
        self.current_mean = 0.0;
        self.count = 0;
    }
}

// ================================================================================================
// EMA: Exponential Moving Average
// ================================================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingEma {
    inner: StreamingEwm,
}

impl StreamingEma {
    pub fn new(window_size: u16) -> Self {
        // Standard EMA Alpha = 2 / (Span + 1)
        let alpha = 2.0 / (window_size as f64 + 1.0);
        Self {
            inner: StreamingEwm::new(alpha, window_size as usize),
        }
    }
}

impl StreamingIndicator for StreamingEma {
    fn update(&mut self, value: f64) -> Option<f64> {
        self.inner.update(value)
    }

    fn reset(&mut self) {
        self.inner.reset();
    }
}

// ================================================================================================
// RSI: Relative Strength Index
// ================================================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingRsi {
    prev_price: Option<f64>,
    avg_gain: StreamingEwm,
    avg_loss: StreamingEwm,
}

impl StreamingRsi {
    pub fn new(window_size: u16) -> Self {
        // Wilder's Smoothing Alpha = 1 / N
        // This differs from standard EMA!
        let alpha = 1.0 / (window_size as f64);
        let win = window_size as usize;

        Self {
            prev_price: None,
            avg_gain: StreamingEwm::new(alpha, win),
            avg_loss: StreamingEwm::new(alpha, win),
        }
    }
}

impl StreamingIndicator for StreamingRsi {
    fn update(&mut self, value: f64) -> Option<f64> {
        let prev = match self.prev_price {
            Some(p) => p,
            None => {
                // First trade: just store the price, we cannot calculate delta yet
                self.prev_price = Some(value);
                return None;
            }
        };

        // 1. Calculate Delta
        let delta = value - prev;
        self.prev_price = Some(value);

        // 2. Separate Gain/Loss
        let (gain, loss) = if delta > 0.0 {
            (delta, 0.0)
        } else {
            (0.0, delta.abs())
        };

        // 3. Update Wilder's Smoothers
        // We capture the Option from both. If both are Some, we have enough data.
        let g_val = self.avg_gain.update(gain);
        let l_val = self.avg_loss.update(loss);

        match (g_val, l_val) {
            (Some(avg_gain), Some(avg_loss)) => {
                // 4. Calculate RSI
                // Prevent division by zero if avg_loss is 0 (Monotonic Up-trend)
                if avg_loss == 0.0 {
                    if avg_gain == 0.0 {
                        // Flat line
                        Some(50.0)
                    } else {
                        // Pure gain
                        Some(100.0)
                    }
                } else {
                    let rs = avg_gain / avg_loss;
                    Some(100.0 - (100.0 / (1.0 + rs)))
                }
            }
            _ => None,
        }
    }

    fn reset(&mut self) {
        self.prev_price = None;
        self.avg_gain.reset();
        self.avg_loss.reset();
    }
}
