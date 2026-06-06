use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, collections::VecDeque};

use crate::math::StreamingIndicator;

// ================================================================================================
// Output Type
// ================================================================================================

/// Represents the direction of the expected reversal upon a completed TD Setup.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum TdDirection {
    /// "Buy Setup": Triggered when a sequence of lower closes completes. Expect a bounce.
    BullishReversal,
    /// "Sell Setup": Triggered when a sequence of higher closes completes. Expect a drop.
    BearishReversal,
}

// ================================================================================================
// Internal State Machine
// ================================================================================================

/// Internal state tracking for the TD Sequential.
///
/// By using a private enum, we avoid disjointed fields (like `count` and `Option<direction>`)
/// and make invalid states strictly unrepresentable.
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

// ================================================================================================
// Indicator: TD Sequential Setup (Variable X)
// ================================================================================================

/// TD X Sequential (Setup Phase).
///
/// This indicator counts consecutive bars where the close is strictly higher or lower
/// than the close $N$ bars ago ($Close_{current} > Close_{current - n}$).
///
/// A completed setup (traditionally 9) suggests exhaustion of the current trend
/// and a high probability of a price reversal or significant pullback.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingTdXSequential {
    lookback: usize,
    target_count: usize,
    buffer: VecDeque<f64>,
    state: InternalSetupState,
}

impl StreamingTdXSequential {
    /// Creates a custom TD Sequential indicator.
    pub fn new(lookback: usize, target_count: usize) -> Self {
        Self {
            lookback,
            target_count,
            // +2 capacity prevents O(N) reallocation because we push BEFORE we pop in update()
            buffer: VecDeque::with_capacity(lookback + 2),
            state: InternalSetupState::default(),
        }
    }

    /// Industry standard TD 9 Setup.
    /// Compares the current close to the close 4 bars ago, targeting a count of 9.
    pub fn td9() -> Self {
        Self::new(4, 9)
    }

    /// Advanced alternative: Some traders use a 13-count setup for longer timeframes.
    pub fn td13() -> Self {
        Self::new(4, 13)
    }
}

impl StreamingIndicator for StreamingTdXSequential {
    type Input = f64; // The Close price
    type Output<'a> = Option<TdDirection>;

    /// Evaluates the close price.
    ///
    /// Returns `None` while the sequence is building or broken.
    /// Returns `Some(TdDirection)` exactly on the bar the setup count reaches the target.
    fn update(&mut self, close: Self::Input) -> Self::Output<'_> {
        self.buffer.push_back(close);

        // Keep buffer size strictly at `lookback + 1` (Current + N historical bars)
        if self.buffer.len() > self.lookback + 1 {
            self.buffer.pop_front();
        }

        // Implicit check: We can't start counting until we have enough historical data
        if self.buffer.len() < self.lookback + 1 {
            return None;
        }

        let historical_close = *self.buffer.front().expect("Buffer length validated above");

        // Evaluate state transition using idiomatic float comparison
        self.state = match close.partial_cmp(&historical_close) {
            Some(Ordering::Less) => {
                // Price dropping -> Bullish Reversal Setup
                let next_count = match self.state {
                    InternalSetupState::Tracking {
                        direction: TdDirection::BullishReversal,
                        count,
                    } => count + 1,
                    _ => 1, // Reset or start fresh
                };
                InternalSetupState::Tracking {
                    direction: TdDirection::BullishReversal,
                    count: next_count,
                }
            }
            Some(Ordering::Greater) => {
                // Price rising -> Bearish Reversal Setup
                let next_count = match self.state {
                    InternalSetupState::Tracking {
                        direction: TdDirection::BearishReversal,
                        count,
                    } => count + 1,
                    _ => 1, // Reset or start fresh
                };
                InternalSetupState::Tracking {
                    direction: TdDirection::BearishReversal,
                    count: next_count,
                }
            }
            _ => {
                // Exact same close or NaN -> Sequence broken
                InternalSetupState::Neutral
            }
        };

        // Output exactly and only when the target count is hit
        if let InternalSetupState::Tracking { direction, count } = self.state {
            if count == self.target_count {
                return Some(direction);
            }
        }

        None
    }

    fn reset(&mut self) {
        self.buffer.clear();
        self.state = InternalSetupState::default();
    }
}

// ================================================================================================
// Unit Tests
// ================================================================================================

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
