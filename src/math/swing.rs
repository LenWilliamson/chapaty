use crate::data::{domain::PriceSource, event::MarketEvent};
use std::collections::VecDeque;

/// Represents the raw geometric shape of a pivot.
///
/// # Why do we need this?
/// `SwingDirection` is required for the **ZigZag Alternation Filter**.
/// To enforce strict alternation (High -> Low -> High -> Low), the algorithm
/// needs to know the absolute geometry of the current pivot to check if it
/// violates the sequence (e.g., detecting two `High`s in a row).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwingDirection {
    High,
    Low,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExtremeTiebreaker {
    /// The most recently formed extreme wins.
    #[default]
    Latest,
    /// The first formed extreme wins.
    Earliest,
}

/// Represents the relative trend classification of a pivot.
///
/// # Why do we need this?
/// While `SwingDirection` tells us the shape, `SwingClassification` provides
/// the **trend context**. A `High` only becomes a `HigherHigh` when compared
/// to the previously recorded `High`. This is what generates the actual
/// trading signals (like identifying a bullish structure break).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwingClassification {
    HigherHigh,
    LowerHigh,
    EqualHigh,
    HigherLow,
    LowerLow,
    EqualLow,
    /// The first pivot of a sequence, or a pivot that cannot be compared yet.
    UnclassifiedHigh,
    UnclassifiedLow,
}

pub struct SwingPoint {
    /// The exact candle that formed the extreme.
    pub candle: Ohlcv,
    /// The exact price value of the extreme (Wick or Body, based on PriceSource).
    pub price: Price,
    pub direction: SwingDirection,
    pub classification: SwingClassification,
}

impl MarketEvent for SwingPoint {
    fn point_in_time(&self) -> DateTime<Utc> {
        self.candle.point_in_time()
    }
}

/// Defines how the indicator handles consecutive pivots of the same direction
/// (e.g., detecting two Swing Highs in a row without a Swing Low in between).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AlternationMode {
    /// Forces strict `High -> Low -> High -> Low` sequences (ZigZag behavior).
    ///
    /// If the algorithm detects a new High, but the last confirmed pivot was ALSO a High,
    /// it evaluates both and only keeps the one with the more extreme price.
    /// The lesser pivot is discarded as market noise.
    ///
    ///
    /// Forces strict `High -> Low -> High -> Low` sequences (ZigZag behavior).
    /// Discards consecutive pivots of the same direction, keeping only the most extreme.
    #[default]
    Strict,

    /// No alternation filtering. Every detected pivot is kept and classified.
    ///
    /// If the algorithm detects two Highs in a row, the second High is simply
    /// classified against the first High (resulting in a HH or LH), regardless
    /// of the missing Low. This is closer to raw fractal market structure.
    ///
    ///
    /// Emits every valid pivot that satisfies the left/right bar constraints.
    /// Consecutive pivots of the same direction are kept and classified sequentially.
    Unfiltered,
}

/// A streaming indicator that identifies market structure points (Higher Highs, Lower Lows)
/// using a ZigZag alternation filter to eliminate market noise.
pub struct StreamingHhll {
    /// The lookback and lookforward requirement for a pivot, expressed as a single period.
    ///
    /// # The Math
    /// A `zig_zag_period` of `N` strictly translates to:
    /// - `left_bars = N`
    /// - `right_bars = N`
    ///
    /// The algorithm requires a symmetrical rolling window of `2N + 1` candles.
    /// To confirm a pivot at candle index `T`, the algorithm must evaluate candles
    /// up to index `T + N`.
    ///
    /// **Note:** This introduces an inherent lookahead delay of `N` bars.
    /// A pivot formed at time `T` will not be emitted by the indicator until time `T + N`.
    zig_zag_period: u16,

    /// Determines whether to evaluate wicks (HighLow) or bodies (CloseOpen).
    source: PriceSource,
    tiebreaker: ExtremeTiebreaker,
    /// The rule governing how consecutive identical pivots are handled.
    alternation_mode: AlternationMode,

    // --- Internal State ---
    /// The rolling window buffer required to evaluate `left_bars` and `right_bars`.
    /// Its maximum capacity will be `(zig_zag_period * 2) + 1`.
    buffer: VecDeque<Candle>,

    /// The most recent, fully confirmed pivot that survived the ZigZag alternation filter.
    ///
    /// The most recent pivot to emerge from the buffer.
    /// In `Strict` mode, this pivot is "volatile" — if a new pivot of the same
    /// direction appears, this one might be overwritten.
    latest_pivot: Option<SwingPoint>,

    /// Memory of the last *High* (used to classify new Highs into HH or LH).
    ///
    /// The last safely locked-in High.
    /// In `Strict` mode, this acts as the "Anchor". If `latest_pivot` (which is a High)
    /// gets overwritten by an even higher peak, we use this anchor to classify the new peak.
    anchor_high: Option<SwingPoint>,

    /// Memory of the last *Low* (used to classify new Lows into HL or LL).
    ///
    /// The last safely locked-in Low.
    /// Acts as the anchor for classifying new Lows.
    anchor_low: Option<SwingPoint>,
}

/*
*
* fn to copute teh slope betwen two points called "show zig_zag" we keep the termionlogy 1:1 to trading view HHLL indicator
* fn to copute teh slope betwen two bands called "show upper_lower_band" we keep the termionlogy 1:1 to trading view HHLL indicator
*
* We want "alerts" for:
* new high
* new low
* higher high
* higher low
* lower high
* lower low

// Inside the update() function, after finding a valid High at index T:

match self.alternation_mode {
    AlternationMode::Strict => {
        if let Some(last_pivot) = self.last_confirmed_pivot {
            if last_pivot.direction == SwingDirection::High {
                // ALTERNATION BROKEN! Two Highs in a row.
                if new_high_price > last_pivot.price {
                    // 1. Overwrite last_pivot with the new, higher peak
                    // 2. Re-classify it against the High BEFORE last_pivot
                } else {
                    // 1. Ignore this new high completely (Market noise)
                }
            } else {
                // Alternation maintained (Last was a Low).
                // Classify normally and update last_confirmed_pivot.
            }
        }
    },
    AlternationMode::Unfiltered => {
        // Ignore last_confirmed_pivot entirely.
        // Just compare new_high against self.last_high to get HH/LH.
        // Update self.last_high.
    }
}

*
*/

impl StreamingHhll {
    pub fn new(period: u16, source: PriceSource, alternation: AlternationMode) -> Self {
        Self {
            zig_zag_period: period,
            source,
            tiebreaker: ExtremeTiebreaker::Latest, // Hardcoded for default, or pass via args
            alternation,
            buffer: VecDeque::with_capacity((period * 2 + 1) as usize),
            latest_pivot: None,
            anchor_high: None,
            anchor_low: None,
        }
    }

    /// Helper to extract the correct price based on the PriceSource rule
    fn extract_price(&self, candle: &Ohlcv, is_high: bool) -> Price {
        match (self.source, is_high) {
            (PriceSource::HighLow, true) => candle.high,
            (PriceSource::HighLow, false) => candle.low,
            (PriceSource::CloseOpen, true) => {
                if candle.close > candle.open {
                    candle.close
                } else {
                    candle.open
                }
            }
            (PriceSource::CloseOpen, false) => {
                if candle.close < candle.open {
                    candle.close
                } else {
                    candle.open
                }
            }
        }
    }
}

impl StreamingIndicator for StreamingHhll {
    type Input = Ohlcv;
    type Output = Option<SwingPoint>; // Emits Some when a pivot is confirmed at T - N

    fn update(&mut self, candle: Ohlcv) -> Option<SwingPoint> {
        self.buffer.push_back(candle);
        let window_size = (self.zig_zag_period * 2 + 1) as usize;

        if self.buffer.len() > window_size {
            self.buffer.pop_front();
        }

        if self.buffer.len() < window_size {
            return None; // Still warming up
        }

        // The candidate is exactly in the middle of the buffer
        let mid_idx = self.zig_zag_period as usize;
        let candidate = self.buffer[mid_idx];

        let candidate_high = self.extract_price(&candidate, true);
        let candidate_low = self.extract_price(&candidate, false);

        let mut is_swing_high = true;
        let mut is_swing_low = true;

        // Mathematical Window Check (O(N) locally, but tiny N)
        for (i, c) in self.buffer.iter().enumerate() {
            if i == mid_idx {
                continue;
            }

            let p_high = self.extract_price(c, true);
            let p_low = self.extract_price(c, false);

            // Apply Tiebreaker logic for flat tops/bottoms
            let is_left = i < mid_idx;
            match self.tiebreaker {
                ExtremeTiebreaker::Earliest => {
                    if (is_left && candidate_high <= p_high)
                        || (!is_left && candidate_high < p_high)
                    {
                        is_swing_high = false;
                    }
                    if (is_left && candidate_low >= p_low) || (!is_left && candidate_low > p_low) {
                        is_swing_low = false;
                    }
                }
                ExtremeTiebreaker::Latest => {
                    if (is_left && candidate_high < p_high)
                        || (!is_left && candidate_high <= p_high)
                    {
                        is_swing_high = false;
                    }
                    if (is_left && candidate_low > p_low) || (!is_left && candidate_low >= p_low) {
                        is_swing_low = false;
                    }
                }
            }
        }

        // --- State Machine Updates (O(1) logic) ---

        if is_swing_high {
            let mut new_pivot = SwingPoint {
                time: candidate.close_timestamp,
                price: candidate_high,
                direction: SwingDirection::High,
                classification: SwingClassification::UnclassifiedHigh,
            };

            match self.alternation {
                AlternationMode::Strict => {
                    if let Some(latest) = self.latest_pivot {
                        if latest.direction == SwingDirection::High {
                            // Alternation Broken: Two highs in a row.
                            if new_pivot.price > latest.price {
                                // Overwrite old high. Classify against the safely locked anchor.
                                if let Some(anchor) = self.anchor_high {
                                    new_pivot.classification = if new_pivot.price > anchor.price {
                                        SwingClassification::HigherHigh
                                    } else {
                                        SwingClassification::LowerHigh
                                    }; // omitting Equal logic for brevity
                                }
                                self.latest_pivot = Some(new_pivot);
                                return Some(new_pivot);
                            }
                            return None; // New high was lower, discarded as noise.
                        } else {
                            // Alternation Maintained. Lock the previous low.
                            self.anchor_low = Some(latest);
                        }
                    }
                }
                AlternationMode::Unfiltered => {} // Skip interference
            }

            // Standard Classification (Alternation maintained or Unfiltered)
            if let Some(anchor) = self.anchor_high {
                new_pivot.classification = if new_pivot.price > anchor.price {
                    SwingClassification::HigherHigh
                } else {
                    SwingClassification::LowerHigh
                };
            }

            self.latest_pivot = Some(new_pivot);
            self.anchor_high = Some(new_pivot); // Update anchor instantly in unfiltered, safely in strict
            return Some(new_pivot);
        }

        // ... (Inverse logic applied for is_swing_low) ...

        None
    }

    fn reset(&mut self) {
        self.buffer.clear();
        self.latest_pivot = None;
        self.anchor_high = None;
        self.anchor_low = None;
    }
}
