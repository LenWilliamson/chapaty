use serde::{Deserialize, Serialize};

use crate::{
    data::{domain::Price, event::Ohlcv},
    indicator::{
        config::{RmaWindow, RsiWindow},
        streaming::{
            StreamingIndicator,
            moving_averages::{EwmSeed, StreamingRma},
        },
    },
};

// ================================================================================================
// RSI: Relative Strength Index
// ================================================================================================

/// Value of the RSI when the window contains no movement at all, i.e. both
/// the average gain and the average loss are zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum RsiFlat {
    /// Report the neutral value `50.0`.
    #[default]
    Neutral,
    /// Report `0.0`.
    Zero,
    /// Report `100.0`.
    Max,
    /// Report no value (`None`) while the window is flat.
    Undefined,
}

impl RsiFlat {
    const fn value(self) -> Option<f64> {
        match self {
            Self::Neutral => Some(50.0),
            Self::Zero => Some(0.0),
            Self::Max => Some(100.0),
            Self::Undefined => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct StreamingRsi {
    prev_price: Option<f64>,
    avg_gain: StreamingRma,
    avg_loss: StreamingRma,
    flat: RsiFlat,
}

impl StreamingRsi {
    /// Creates a streaming RSI over `window_size`.
    ///
    /// # Panics
    ///
    /// Panics if `window_size` is zero.
    #[must_use]
    pub fn new(window_size: RsiWindow, seed: EwmSeed, flat: RsiFlat) -> Self {
        let w = RmaWindow(window_size.0);
        Self {
            prev_price: None,
            avg_gain: StreamingRma::new(w, seed),
            avg_loss: StreamingRma::new(w, seed),
            flat,
        }
    }
}

impl StreamingIndicator for StreamingRsi {
    type Input = f64;
    type Output<'a> = Option<f64>;

    fn update(&mut self, value: Self::Input) -> Self::Output<'_> {
        let prev = self.prev_price.replace(value)?;

        let delta = value - prev;
        let (gain, loss) = if delta > 0.0 {
            (delta, 0.0)
        } else {
            (0.0, -delta)
        };

        let avg_gain = self.avg_gain.update(gain);
        let avg_loss = self.avg_loss.update(loss);

        let (Some(avg_gain), Some(avg_loss)) = (avg_gain, avg_loss) else {
            return None; // still warming up
        };
        rsi_from_averages(avg_gain, avg_loss, self.flat)
    }

    fn reset(&mut self) {
        self.prev_price = None;
        self.avg_gain.reset();
        self.avg_loss.reset();
    }
}

// ================================================================================================
// On Balance Volume RSI: On Balance Volume Relative Strength Index (ObvRsi)
// ================================================================================================

/// RSI built from On Balance Volume deltas instead of price deltas.
///
/// Every candle either adds its volume (close up), subtracts its volume
/// (close down), or does nothing (close unchanged) to a running total.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct StreamingObvRsi {
    prev_price: Option<Price>,
    /// `up` is the RMA of the volume on up-candles
    up: StreamingRma,
    /// `down` is the RMA of the volume on down-candles
    down: StreamingRma,
    flat: RsiFlat,
}

impl StreamingObvRsi {
    /// Creates a streaming On Balance Volume RSI over `window_size`.
    ///
    /// # Panics
    ///
    /// Panics if `window_size` is zero.
    #[must_use]
    pub fn new(window_size: RsiWindow, seed: EwmSeed, flat: RsiFlat) -> Self {
        let w = RmaWindow(window_size.0);
        Self {
            prev_price: None,
            up: StreamingRma::new(w, seed),
            down: StreamingRma::new(w, seed),
            flat,
        }
    }
}

impl StreamingIndicator for StreamingObvRsi {
    type Input = Ohlcv;
    type Output<'a> = Option<f64>;

    fn update(&mut self, input: Self::Input) -> Self::Output<'_> {
        let Ohlcv { close, volume, .. } = input;
        let prev = self.prev_price.replace(close)?;

        let (gain, loss) = if close > prev {
            (volume.0, 0.0)
        } else if close < prev {
            (0.0, volume.0)
        } else {
            (0.0, 0.0)
        };

        let up = self.up.update(gain);
        let down = self.down.update(loss);

        let (Some(up), Some(down)) = (up, down) else {
            return None; // still warming up
        };
        rsi_from_averages(up, down, self.flat)
    }

    fn reset(&mut self) {
        self.prev_price = None;
        self.up.reset();
        self.down.reset();
    }
}

// ================================================================================================
// Helper Functions
// ================================================================================================

/// Computes the RSI from smoothed gains and losses.
///
/// This is the classic `100 - 100 / (1 + rs)` with `rs = avg_gain / avg_loss`,
/// rewritten to avoid dividing by `avg_loss`:
///
/// ```text
/// 100 - 100 / (1 + gain/loss)
///   = 100 - 100 * loss / (loss + gain)       // 1 + gain/loss = (loss + gain) / loss
///   = 100 * gain / (gain + loss)
/// ```
///
/// A zero `avg_loss` then yields `100.0` and a zero `avg_gain` yields `0.0`
/// without special cases. Only when both are zero is the result `0/0`;
/// `flat` decides what is returned in that case.
fn rsi_from_averages(avg_gain: f64, avg_loss: f64, flat: RsiFlat) -> Option<f64> {
    let total = avg_gain + avg_loss;
    if total <= 0.0 {
        flat.value()
    } else {
        Some(100.0 * avg_gain / total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn approx(a: f64, b: f64) {
        assert!((a - b).abs() < 1e-9, "expected {b}, got {a}");
    }

    #[test]
    fn first_value_only_seeds_and_returns_none() {
        // The first price has no predecessor, so there's no delta to score yet.
        let mut rsi = StreamingRsi::new(RsiWindow(3), EwmSeed::FirstValue, RsiFlat::Neutral);
        assert_eq!(rsi.update(100.0), None);
    }

    #[test]
    fn warmup_takes_window_plus_one_prices() {
        // Price 1 seeds prev_price. Prices 2..=N+1 each produce one delta, and
        // the inner EWMs need `window_size` deltas before they emit. So
        // with window 3: 1 seed + 3 deltas = 4 prices before the first
        // RSI value.
        let mut rsi = StreamingRsi::new(RsiWindow(3), EwmSeed::FirstValue, RsiFlat::Neutral);
        assert_eq!(rsi.update(10.0), None); // seed
        assert_eq!(rsi.update(11.0), None); // delta 1
        assert_eq!(rsi.update(12.0), None); // delta 2
        assert!(rsi.update(13.0).is_some()); // delta 3 -> first output
    }

    #[test]
    fn pure_uptrend_gives_100() {
        // Every delta is a gain, avg_loss stays 0 -> the "pure gain" branch ->
        // 100.
        let mut rsi = StreamingRsi::new(RsiWindow(3), EwmSeed::FirstValue, RsiFlat::Neutral);
        let mut last = None;
        for p in [1.0, 2.0, 3.0, 4.0, 5.0] {
            last = rsi.update(p);
        }
        assert_eq!(last, Some(100.0));
    }

    #[test]
    fn pure_downtrend_gives_0() {
        // Every delta is a loss, avg_gain stays 0 -> rs = 0 -> 100 - 100/1 = 0.
        let mut rsi = StreamingRsi::new(RsiWindow(3), EwmSeed::FirstValue, RsiFlat::Neutral);
        let mut last = None;
        for p in [5.0, 4.0, 3.0, 2.0, 1.0] {
            last = rsi.update(p);
        }
        assert_eq!(last, Some(0.0));
    }

    #[test]
    fn flat_line_gives_50() {
        // No movement: both averages 0 -> the explicit flat-line branch -> 50.
        let mut rsi = StreamingRsi::new(RsiWindow(3), EwmSeed::FirstValue, RsiFlat::Neutral);
        let mut last = None;
        for _ in 0..5 {
            last = rsi.update(100.0);
        }
        assert_eq!(last, Some(50.0));
    }

    #[test]
    fn output_stays_within_bounds() {
        // Whatever the input, RSI must land in [0, 100].
        let mut rsi = StreamingRsi::new(RsiWindow(4), EwmSeed::FirstValue, RsiFlat::Neutral);
        let prices = [10.0, 12.0, 11.0, 15.0, 9.0, 20.0, 8.0, 13.0, 14.0, 7.0];
        for p in prices {
            if let Some(v) = rsi.update(p) {
                assert!((0.0..=100.0).contains(&v), "RSI {v} out of bounds");
            }
        }
    }

    #[test]
    fn reset_clears_state() {
        let mut rsi = StreamingRsi::new(RsiWindow(2), EwmSeed::FirstValue, RsiFlat::Neutral);
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

    #[test]
    fn rsi_from_averages_handles_edges() {
        approx(rsi_from_averages(3.0, 1.0, RsiFlat::Neutral).unwrap(), 75.0);
        assert_eq!(rsi_from_averages(2.0, 0.0, RsiFlat::Neutral), Some(100.0));
        assert_eq!(rsi_from_averages(0.0, 2.0, RsiFlat::Neutral), Some(0.0));
    }

    #[test]
    fn rsi_flat_setting_controls_the_zero_zero_case() {
        assert_eq!(rsi_from_averages(0.0, 0.0, RsiFlat::Neutral), Some(50.0));
        assert_eq!(rsi_from_averages(0.0, 0.0, RsiFlat::Zero), Some(0.0));
        assert_eq!(rsi_from_averages(0.0, 0.0, RsiFlat::Max), Some(100.0));
        assert_eq!(rsi_from_averages(0.0, 0.0, RsiFlat::Undefined), None);
    }

    #[test]
    fn rsi_flat_line_follows_the_flat_setting() {
        let mut rsi = StreamingRsi::new(RsiWindow(3), EwmSeed::FirstValue, RsiFlat::Undefined);
        let mut last = Some(0.0);
        for _ in 0..6 {
            last = rsi.update(100.0);
        }
        assert_eq!(last, None);
    }

    #[test]
    fn rsi_seed_changes_early_values() {
        // prices 10, 12, 11, 14 -> deltas +2, -1, +3 (window 3)
        let prices = [10.0, 12.0, 11.0, 14.0];

        let mut sma = StreamingRsi::new(RsiWindow(3), EwmSeed::Sma, RsiFlat::Neutral);
        let mut first = StreamingRsi::new(RsiWindow(3), EwmSeed::FirstValue, RsiFlat::Neutral);
        let (mut a, mut b) = (None, None);
        for p in prices {
            a = sma.update(p);
            b = first.update(p);
        }
        approx(a.unwrap(), 100.0 * 5.0 / 6.0); // avg gain 5/3, avg loss 1/3
        approx(b.unwrap(), 100.0 * 17.0 / 19.0); // avg gain 17/9, avg loss 2/9
    }
}
