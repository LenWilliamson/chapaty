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
    use super::*;

    #[test]
    fn td9_requires_minimum_buffer_before_counting() {
        let mut td = StreamingTdXSequential::td9(); // Lookback 4, Target 9

        // Feed 4 bars (Not enough to compare C_current to C_current-4)
        assert_eq!(td.update(10.0), None);
        assert_eq!(td.update(10.0), None);
        assert_eq!(td.update(10.0), None);
        assert_eq!(td.update(10.0), None);

        // 5th bar triggers the first comparison.
        assert_eq!(td.update(11.0), None);

        // State should now internally be tracking BearishReversal with count 1
        assert_eq!(
            td.state,
            InternalSetupState::Tracking {
                direction: TdDirection::BearishReversal,
                count: 1
            }
        );
    }

    #[test]
    fn td9_completes_bullish_reversal_setup() {
        // We use a small target count (3) for an easier test setup
        let mut td = StreamingTdXSequential::new(2, 3);

        // Fill historical buffer: T0, T1
        assert_eq!(td.update(100.0), None);
        assert_eq!(td.update(95.0), None);

        // Setup 1: 90.0 < 100.0 (T0)
        assert_eq!(td.update(90.0), None);
        // Setup 2: 85.0 < 95.0 (T1)
        assert_eq!(td.update(85.0), None);
        // Setup 3 (Target Hit): 80.0 < 90.0 (T2) -> Emits Signal!
        assert_eq!(td.update(80.0), Some(TdDirection::BullishReversal));

        // Setup 4: 75.0 < 85.0 (T3) -> Continues tracking internally, but no signal emitted
        assert_eq!(td.update(75.0), None);
    }

    #[test]
    fn td9_resets_sequence_on_flip() {
        let mut td = StreamingTdXSequential::new(2, 3);

        // T0, T1
        td.update(100.0);
        td.update(95.0);

        // Bullish 1
        td.update(90.0);
        // Bullish 2
        td.update(85.0);

        assert_eq!(
            td.state,
            InternalSetupState::Tracking {
                direction: TdDirection::BullishReversal,
                count: 2
            }
        );

        // Flip: Price suddenly spikes above T2 (90.0).
        // 95.0 > 90.0 -> Bearish 1. The Bullish setup is destroyed.
        assert_eq!(td.update(95.0), None);

        assert_eq!(
            td.state,
            InternalSetupState::Tracking {
                direction: TdDirection::BearishReversal,
                count: 1
            }
        );
    }

    #[test]
    fn td9_breaks_sequence_on_flat_price() {
        let mut td = StreamingTdXSequential::new(2, 3);

        td.update(100.0);
        td.update(100.0);

        // Bullish 1
        td.update(90.0);

        // Exact same price as T1 (100.0 == 100.0) -> Sequence Broken
        assert_eq!(td.update(100.0), None);

        assert_eq!(td.state, InternalSetupState::Neutral);
    }
}
