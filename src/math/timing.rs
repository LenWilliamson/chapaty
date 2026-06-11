use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

use crate::{data::event::Ohlcv, math::StreamingIndicator, ring_buffer::RingBuffer};

// ================================================================================================
// TD X Sequential
// ================================================================================================

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum TdDirection {
    /// "Buy Setup": Triggered when a sequence of lower closes completes. Expect a bounce.
    BullishReversal,
    /// "Sell Setup": Triggered when a sequence of higher closes completes. Expect a drop.
    BearishReversal,
}

/// Represents the relationship between the current close and the historical close $N$ bars ago.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
pub enum PriceRelationship {
    /// The current close is strictly higher than the historical close.
    /// Acts as a Bearish Price Flip, which is required to begin tracking a `BullishReversal` (Buy Setup).
    Higher,
    /// The current close is strictly lower than the historical close.
    /// Acts as a Bullish Price Flip, which is required to begin tracking a `BearishReversal` (Sell Setup).
    Lower,
    /// The close is perfectly equal, or the indicator has just been initialized (neutral state).
    /// If an invalid float (`NaN`) is encountered, it gracefully defaults to this state.
    #[default]
    Flat,
}

impl From<Option<Ordering>> for PriceRelationship {
    fn from(cmp: Option<Ordering>) -> Self {
        match cmp {
            Some(Ordering::Less) => PriceRelationship::Lower,
            Some(Ordering::Greater) => PriceRelationship::Higher,
            Some(Ordering::Equal) | None => PriceRelationship::Flat,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
enum InternalSetupState {
    /// No active sequence is being tracked (e.g., price is flat or sequence broken).
    #[default]
    Neutral,
    /// Actively tracking a sequence of higher or lower closes.
    Tracking {
        direction: TdDirection,
        count: usize,
    },
}

impl InternalSetupState {
    fn new_bullish() -> Self {
        Self::Tracking {
            direction: TdDirection::BullishReversal,
            count: 1,
        }
    }

    fn new_bearish() -> Self {
        Self::Tracking {
            direction: TdDirection::BearishReversal,
            count: 1,
        }
    }

    fn increment(self) -> Self {
        match self {
            Self::Tracking { direction, count } => Self::Tracking {
                direction,
                count: count + 1,
            },
            Self::Neutral => Self::Neutral,
        }
    }
}

// ================================================================================================
// Indicator: TD Sequential Setup
// ================================================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingTdXSequential {
    target_count: usize,
    buffer: RingBuffer<f64>,
    state: InternalSetupState,
    last_cmp: PriceRelationship,
}

impl StreamingTdXSequential {
    pub fn new(lookback: usize, target_count: usize) -> Self {
        Self {
            target_count,
            buffer: RingBuffer::new(lookback), // Capacity is exactly N
            state: InternalSetupState::default(),
            last_cmp: PriceRelationship::default(),
        }
    }

    pub fn td9() -> Self {
        Self::new(4, 9)
    }
    pub fn td13() -> Self {
        Self::new(4, 13)
    }
}

impl StreamingTdXSequential {
    /// Evaluates if the current sequence count constitutes a completed TD Setup.
    ///
    /// In standard TD methodology, a setup completes at exactly `target_count` (e.g., 9).
    /// By checking exact multiples (e.g., 18, 27), we seamlessly handle "Setup Recycling",
    /// ensuring extreme trend breakouts continue to emit valid exhaustion signals
    /// rather than being silently ignored.
    fn is_setup_completion(&self, count: usize) -> bool {
        count > 0 && count % self.target_count == 0
    }
}

impl StreamingIndicator for StreamingTdXSequential {
    type Input = Ohlcv;
    type Output<'a> = Option<TdDirection>;

    fn update(&mut self, candle: Self::Input) -> Self::Output<'_> {
        let close = candle.close.0;
        let Some(historical_close) = self.buffer.push(close) else {
            return None;
        };
        let current_cmp = PriceRelationship::from(close.partial_cmp(&historical_close));

        self.state = match current_cmp {
            PriceRelationship::Lower => match self.state {
                InternalSetupState::Tracking {
                    direction: TdDirection::BullishReversal,
                    ..
                } => self.state.increment(),
                _ if self.last_cmp == PriceRelationship::Higher => {
                    InternalSetupState::new_bullish()
                }
                _ => InternalSetupState::Neutral,
            },
            PriceRelationship::Higher => match self.state {
                InternalSetupState::Tracking {
                    direction: TdDirection::BearishReversal,
                    ..
                } => self.state.increment(),
                _ if self.last_cmp == PriceRelationship::Lower => InternalSetupState::new_bearish(),
                _ => InternalSetupState::Neutral,
            },
            PriceRelationship::Flat => InternalSetupState::Neutral,
        };

        self.last_cmp = current_cmp;

        if let InternalSetupState::Tracking { direction, count } = self.state {
            if self.is_setup_completion(count) {
                return Some(direction);
            }
        }

        None
    }

    fn reset(&mut self) {
        self.buffer.clear();
        self.state = InternalSetupState::default();
        self.last_cmp = PriceRelationship::default();
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};

use crate::data::domain::{Price, Quantity};

use super::*;

    /// Parse RFC3339 timestamp string to DateTime<Utc>.
    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    /// Builds a flat OHLCV candle whose only meaningful field for this indicator
    /// is `close`. Timestamps are fixed at 2026-06-11 ~17:30 UTC.
    fn candle(close: f64) -> Ohlcv {
        Ohlcv {
            open_timestamp: ts("2026-06-11T17:30:00Z"),
            close_timestamp: ts("2026-06-11T17:31:00Z"),
            open: Price(close),
            high: Price(close),
            low: Price(close),
            close: Price(close),
            volume: Quantity(0.0),
            quote_asset_volume: None,
            number_of_trades: None,
            taker_buy_base_asset_volume: None,
            taker_buy_quote_asset_volume: None,
        }
    }

    /// Feeds a slice of closes and collects the emitted signal per bar.
    fn feed(td: &mut StreamingTdXSequential, closes: &[f64]) -> Vec<Option<TdDirection>> {
        closes.iter().map(|&c| td.update(candle(c))).collect()
    }

    /// A pure decline never produces a bearish price flip, so per DeMark no buy
    /// setup ever begins. (Lookback 1 used so each bar compares to the prior close.)
    #[test]
    fn monotonic_decline_without_flip_emits_nothing() {
        let mut td = StreamingTdXSequential::new(1, 3);
        let out = feed(&mut td, &[50.0, 40.0, 30.0, 20.0, 10.0]);
        assert!(out.iter().all(|o| o.is_none()));
        assert_eq!(td.state, InternalSetupState::Neutral);
    }

    /// A higher close followed by a lower close is the bearish flip that opens a
    /// buy setup at count 1.
    #[test]
    fn bearish_flip_starts_buy_setup() {
        let mut td = StreamingTdXSequential::new(1, 9);
        feed(&mut td, &[10.0, 20.0, 19.0]); // up, then the first lower close = flip
        assert_eq!(
            td.state,
            InternalSetupState::Tracking {
                direction: TdDirection::BullishReversal,
                count: 1
            }
        );
    }

    /// Flip + two more consecutive lower closes reaches target 3 and emits.
    #[test]
    fn completes_buy_setup_and_emits_bullish() {
        let mut td = StreamingTdXSequential::new(1, 3);
        let out = feed(&mut td, &[10.0, 20.0, 19.0, 18.0, 17.0]);
        assert_eq!(
            out,
            vec![None, None, None, None, Some(TdDirection::BullishReversal)]
        );
    }

    /// Symmetric sell-setup case: a bullish flip then rising closes.
    #[test]
    fn completes_sell_setup_and_emits_bearish() {
        let mut td = StreamingTdXSequential::new(1, 3);
        let out = feed(&mut td, &[20.0, 10.0, 11.0, 12.0, 13.0]);
        assert_eq!(
            out,
            vec![None, None, None, None, Some(TdDirection::BearishReversal)]
        );
    }

    /// A flip in the opposite direction destroys the in-progress setup and starts
    /// the other side at count 1.
    #[test]
    fn opposite_flip_resets_to_other_direction() {
        let mut td = StreamingTdXSequential::new(1, 5);
        feed(&mut td, &[10.0, 20.0, 19.0, 18.0]); // bullish, count 2
        assert_eq!(
            td.state,
            InternalSetupState::Tracking {
                direction: TdDirection::BullishReversal,
                count: 2
            }
        );

        // 25.0 > 18.0 (higher) right after a lower close = bullish flip -> sell setup.
        assert_eq!(td.update(candle(25.0)), None);
        assert_eq!(
            td.state,
            InternalSetupState::Tracking {
                direction: TdDirection::BearishReversal,
                count: 1
            }
        );
    }

    /// An exactly-equal close fails the strict comparison and breaks the run.
    #[test]
    fn flat_close_breaks_sequence() {
        let mut td = StreamingTdXSequential::new(1, 5);
        feed(&mut td, &[10.0, 20.0, 19.0]); // bullish, count 1
        assert_eq!(td.update(candle(19.0)), None); // 19 == 19 -> Flat
        assert_eq!(td.state, InternalSetupState::Neutral);
    }

    /// The `count % target == 0` rule re-emits on each multiple ("Setup Recycling").
    #[test]
    fn recycles_and_re_emits_at_second_multiple() {
        let mut td = StreamingTdXSequential::new(1, 3);
        let out = feed(&mut td, &[10.0, 20.0, 19.0, 18.0, 17.0, 16.0, 15.0, 14.0]);
        let signal_count = out.iter().filter(|o| o.is_some()).count();
        assert_eq!(signal_count, 2);
        assert_eq!(out[4], Some(TdDirection::BullishReversal)); // count 3
        assert_eq!(out[7], Some(TdDirection::BullishReversal)); // count 6
    }

    /// Until the ring buffer is full there is no historical close to compare to,
    /// and the very first comparison has no prior relationship to flip from.
    #[test]
    fn warmup_blocks_comparison_until_buffer_full() {
        let mut td = StreamingTdXSequential::td9(); // lookback 4
        let out = feed(&mut td, &[10.0, 10.0, 10.0, 10.0, 11.0]);
        assert!(out.iter().all(|o| o.is_none()));
        assert_eq!(td.state, InternalSetupState::Neutral);
    }

    /// A `NaN` close compares as `None` -> `Flat`; it must not panic and must
    /// break any active sequence.
    #[test]
    fn nan_close_is_treated_as_flat_and_does_not_panic() {
        let mut td = StreamingTdXSequential::new(1, 3);
        let out = feed(&mut td, &[10.0, 20.0, f64::NAN]);
        assert_eq!(out, vec![None, None, None]);
        assert_eq!(td.state, InternalSetupState::Neutral);
    }

    /// `reset` clears the buffer (so warm-up restarts) and the state machine.
    #[test]
    fn reset_restores_warmup_and_neutral_state() {
        let mut td = StreamingTdXSequential::new(1, 3);
        feed(&mut td, &[10.0, 20.0, 19.0, 18.0]);
        td.reset();
        assert_eq!(td.state, InternalSetupState::Neutral);
        assert_eq!(td.last_cmp, PriceRelationship::Flat);
        // Buffer empty again: the next candle is just a warm-up bar.
        assert_eq!(td.update(candle(5.0)), None);
    }
}
