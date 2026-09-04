use serde::{Deserialize, Serialize};

use crate::indicator::{
    config::RsiWindow,
    streaming::{StreamingIndicator, moving_averages::StreamingEwm},
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct StreamingRsi {
    prev_price: Option<f64>,
    avg_gain: StreamingEwm,
    avg_loss: StreamingEwm,
}

impl StreamingRsi {
    /// Creates a streaming RSI with Wilder smoothing over `window_size`.
    ///
    /// # Panics
    /// Panics if `window_size` cannot be represented as `u32`.
    #[must_use]
    pub fn new(window_size: RsiWindow) -> Self {
        let size = window_size.0 as usize;
        // Wilder's Smoothing Alpha = 1 / N
        let size_u32 = u32::from(window_size.0);
        let alpha = 1.0 / f64::from(size_u32);
        let win = size;

        Self {
            prev_price: None,
            avg_gain: StreamingEwm::new(alpha, win),
            avg_loss: StreamingEwm::new(alpha, win),
        }
    }
}

impl StreamingIndicator for StreamingRsi {
    type Input = f64;
    type Output<'a> = Option<f64>;

    fn update(&mut self, value: Self::Input) -> Self::Output<'_> {
        let Some(prev) = self.prev_price else {
            self.prev_price = Some(value);
            return None;
        };

        let delta = value - prev;
        self.prev_price = Some(value);

        let (gain, loss) = if delta > 0.0 {
            (delta, 0.0)
        } else {
            (0.0, delta.abs())
        };

        let g_val = self.avg_gain.update(gain);
        let l_val = self.avg_loss.update(loss);

        match (g_val, l_val) {
            (Some(avg_gain), Some(avg_loss)) => {
                // Prevent division by zero if avg_loss is 0 (Monotonic
                // Up-trend)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_value_only_seeds_and_returns_none() {
        // The first price has no predecessor, so there's no delta to score yet.
        let mut rsi = StreamingRsi::new(RsiWindow(3));
        assert_eq!(rsi.update(100.0), None);
    }

    #[test]
    fn warmup_takes_window_plus_one_prices() {
        // Price 1 seeds prev_price. Prices 2..=N+1 each produce one delta, and
        // the inner EWMs need `window_size` deltas before they emit. So
        // with window 3: 1 seed + 3 deltas = 4 prices before the first
        // RSI value.
        let mut rsi = StreamingRsi::new(RsiWindow(3));
        assert_eq!(rsi.update(10.0), None); // seed
        assert_eq!(rsi.update(11.0), None); // delta 1
        assert_eq!(rsi.update(12.0), None); // delta 2
        assert!(rsi.update(13.0).is_some()); // delta 3 -> first output
    }

    #[test]
    fn pure_uptrend_gives_100() {
        // Every delta is a gain, avg_loss stays 0 -> the "pure gain" branch ->
        // 100.
        let mut rsi = StreamingRsi::new(RsiWindow(3));
        let mut last = None;
        for p in [1.0, 2.0, 3.0, 4.0, 5.0] {
            last = rsi.update(p);
        }
        assert_eq!(last, Some(100.0));
    }

    #[test]
    fn pure_downtrend_gives_0() {
        // Every delta is a loss, avg_gain stays 0 -> rs = 0 -> 100 - 100/1 = 0.
        let mut rsi = StreamingRsi::new(RsiWindow(3));
        let mut last = None;
        for p in [5.0, 4.0, 3.0, 2.0, 1.0] {
            last = rsi.update(p);
        }
        assert_eq!(last, Some(0.0));
    }

    #[test]
    fn flat_line_gives_50() {
        // No movement: both averages 0 -> the explicit flat-line branch -> 50.
        let mut rsi = StreamingRsi::new(RsiWindow(3));
        let mut last = None;
        for _ in 0..5 {
            last = rsi.update(100.0);
        }
        assert_eq!(last, Some(50.0));
    }

    #[test]
    fn output_stays_within_bounds() {
        // Whatever the input, RSI must land in [0, 100].
        let mut rsi = StreamingRsi::new(RsiWindow(4));
        let prices = [10.0, 12.0, 11.0, 15.0, 9.0, 20.0, 8.0, 13.0, 14.0, 7.0];
        for p in prices {
            if let Some(v) = rsi.update(p) {
                assert!((0.0..=100.0).contains(&v), "RSI {v} out of bounds");
            }
        }
    }

    #[test]
    fn reset_clears_state() {
        let mut rsi = StreamingRsi::new(RsiWindow(2));
        // Drive it to a pure-uptrend reading.
        for p in [1.0, 2.0, 3.0, 4.0] {
            rsi.update(p);
        }
        rsi.reset();
        // After reset the next price only re-seeds: no leftover prev_price or
        // averages.
        assert_eq!(rsi.update(50.0), None); // seed again
        assert_eq!(rsi.update(49.0), None); // delta 1, EWM not yet full (window 2)
        assert!(rsi.update(48.0).is_some()); // delta 2 -> emits, uninfluenced by old data
    }
}
