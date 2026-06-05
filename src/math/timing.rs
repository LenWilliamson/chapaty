use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

use crate::math::StreamingIndicator;

// ================================================================================================
// Output Type
// ================================================================================================

/// Represents the direction of the expected reversal upon a completed TD Setup.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum TdDirection {
    /// "Buy Setup": Triggered when a sequence of lower closes completes. Expect a bounce.
    BullishReversal
    /// "Sell Setup": Triggered when a sequence of higher closes completes. Expect a drop.
    BearishReversal,
}

// ================================================================================================
// Internal State Machine
// ================================================================================================

/// Internal state tracking for the TD Sequential.
/// By using a private enum, we avoid disjointed fields (like `count` and `Option<direction>`)
/// and make invalid states strictly unrepresentable. We never need a boolean flag.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
enum InternalSetupState {
    /// No active sequence is being tracked (e.g., price is flat).
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
/// This indicator counts consecutive bars where the Close is higher/lower than the Close `N` bars ago.
/// A completed setup (usually 9) suggests exhaustion of the current trend and a high probability
/// of a reversal or significant pullback.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingTdXSequential {
    lookback: usize,
    target_count: usize,
    buffer: VecDeque<f64>,
    state: InternalSetupState,
}

impl StreamingTdXSequential {
    pub fn new(lookback: usize, target_count: usize) -> Self {
        Self {
            lookback,
            target_count,
            buffer: VecDeque::with_capacity(lookback + 1),
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
    /// Output is completely implicit: It stays `None` while building.
    /// It only returns `Some(TdDirection)` on the exact bar the setup completes.
    type Output<'a> = Option<TdDirection>;

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

        // Evaluate state transition
        let next_state = if close < historical_close {
            // Price dropping -> Bullish Reversal Setup
            let new_count = match self.state {
                InternalSetupState::Tracking {
                    direction: TdDirection::BullishReversal,
                    count,
                } => count + 1,
                _ => 1, // Reset or start fresh
            };
            InternalSetupState::Tracking {
                direction: TdDirection::BullishReversal,
                count: new_count,
            }
        } else if close > historical_close {
            // Price rising -> Bearish Reversal Setup
            let new_count = match self.state {
                InternalSetupState::Tracking {
                    direction: TdDirection::BearishReversal,
                    count,
                } => count + 1,
                _ => 1, // Reset or start fresh
            };
            InternalSetupState::Tracking {
                direction: TdDirection::BearishReversal,
                count: new_count,
            }
        } else {
            // Exact same close -> Sequence broken
            InternalSetupState::Neutral
        };

        // Commit the state
        self.state = next_state;

        // Output exactly and only when the target count is hit (Implicit Readiness)
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
