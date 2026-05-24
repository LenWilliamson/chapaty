use std::{cmp::Ordering, collections::VecDeque};

use crate::{
    data::{
        domain::{CandleDirection, CandleDirectionIter, Price, PriceSource},
        event::{MarketEvent, Ohlcv},
    },
    math::StreamingIndicator,
};
use chrono::{DateTime, Utc};

/// Represents the geometric type of a single pivot point.
///
/// [`PivotType`] is required for the [`AlternationMode`] filter.
/// To enforce alternation (High -> Low -> High -> Low), the algorithm
/// needs to know the type of the current pivot to check if it
/// violates the sequence (e.g., detecting two `High`s in a row).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PivotType {
    High,
    Low,
}

/// Represents the relative sequence that defines the market's overall direction.
///
/// While [`PivotType`] tells us the basic shape (peak or trough),
/// [`MarketStructureSequence`] provides the **trend context** by comparing it
/// to historical pivots.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketStructureSequence {
    HigherHigh,
    LowerHigh,
    EqualHigh,
    HigherLow,
    LowerLow,
    EqualLow,
    UnclassifiedHigh,
    UnclassifiedLow,
}

/// Defines how the indicator handles consecutive pivots of the same type
/// (e.g., detecting two `PivotType::High`s in a row without a `PivotType::Low` in between).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AlternationMode {
    /// Forces alternating `High -> Low -> High -> Low` sequences (ZigZag behavior).
    ///
    /// If the algorithm detects a new `PivotType::High`, but the last confirmed pivot
    /// was also a `PivotType::High`, it evaluates both and only keeps the one with
    /// the higher price. The lesser pivot is discarded as market noise.
    #[default]
    Alternating,

    /// No alternation filtering. Every detected [`PivotType`] is kept.
    ///
    /// If the algorithm detects two `PivotType::High`s in a row, the second `PivotType::High`
    /// is simply classified against the first `PivotType::High` (resulting in a HH or LH),
    /// regardless of the missing `PivotType::Low`.
    Consecutive,
}

/// Represents structural breakthrough events in market microstructure.
///
/// These events are triggered when an updated or newly confirmed [`PivotPoint`]
/// breaches a historical macro level stored in either `anchor_high` or `anchor_low`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketStructureEvent {
    /// **Trend Continuation** (Break of Structure).
    ///
    /// Occurs when price breaks through a structural barrier in the **same** direction
    /// as the established trend, extending the current market expansion.
    ///
    /// # Geometric Triggers:
    /// - **Bullish BOS**: A new `PivotPoint` closes above the previous `anchor_high`,
    ///   confirming a `MarketStructureSequence::HigherHigh`.
    /// - **Bearish BOS**: A new `PivotPoint` closes below the previous `anchor_low`,
    ///   confirming a `MarketStructureSequence::LowerLow`.
    BreakOfStructure,

    /// **Trend Reversal** (Market Structure Shift / Change of Character).
    ///
    /// Occurs when price breaches a key structural level in the **opposite** direction
    /// of the established trend, signaling a supply/demand flip and structural failure.
    ///
    /// # Geometric Triggers:
    /// - **Bullish MSS**: Price aggressively breaks *above* the last macro `anchor_high`
    ///   while the market was heavily entrenched in a downtrend sequence.
    /// - **Bearish MSS**: Price aggressively breaks *below* the last macro `anchor_low`
    ///   while the market was heavily entrenched in an uptrend sequence.
    MarketStructureShift,

    /// **No Structural Change**.
    ///
    /// The incoming streaming tick or newly formed local vertex did not break out of
    /// the current trading range established by `anchor_high` and `anchor_low`.
    ///
    /// This represents internal noise, a standard retracement, or a minor sub-structure pivot.
    NoChange,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExtremeTiebreaker {
    /// The most recently formed extreme wins.
    #[default]
    Latest,
    /// The first formed extreme wins.
    Earliest,
}

#[derive(Debug, Clone, Copy)]
pub struct PivotPoint {
    pub ohlcv_candle: Ohlcv,
    pub price: Price,
    pub price_source: PriceSource,
    pub pivot_type: PivotType,
    pub trend: MarketStructureSequence,
}

impl MarketEvent for PivotPoint {
    fn point_in_time(&self) -> DateTime<Utc> {
        self.ohlcv_candle.point_in_time()
    }
}

impl PivotPoint {
    /// Generates a linear interpolation function based on bar indices.
    ///
    /// Returns a zero-allocation closure that takes a target bar index (usize)
    /// and returns the interpolated/extrapolated price at that index.
    pub fn price_line_by_index(
        &self,
        target: &PivotPoint,
        self_index: usize,
        target_index: usize,
    ) -> impl Fn(usize) -> Price {
        let p0 = self.price.0;
        let p1 = target.price.0;

        let dx = (target_index as f64) - (self_index as f64);
        let m = if dx == 0.0 { 0.0 } else { (p1 - p0) / dx };

        move |x: usize| -> Price {
            let current_dx = (x as f64) - (self_index as f64);
            Price(p0 + m * current_dx)
        }
    }

    /// Generates a linear interpolation function based on exact timestamps.
    ///
    /// Returns a zero-allocation closure that takes a target point in time
    /// (DateTime<Utc>) and returns the interpolated/extrapolated price.
    /// Uses chrono::Duration to safely compute the time deltas in milliseconds.
    pub fn price_line_by_time(&self, target: &PivotPoint) -> impl Fn(DateTime<Utc>) -> Price {
        let p0 = self.price.0;
        let p1 = target.price.0;

        let t0 = self.point_in_time();
        let t1 = target.point_in_time();

        let dx = (t1 - t0).num_milliseconds() as f64;
        let m = if dx == 0.0 { 0.0 } else { (p1 - p0) / dx };

        move |t: DateTime<Utc>| -> Price {
            let current_dx = (t - t0).num_milliseconds() as f64;
            Price(p0 + m * current_dx)
        }
    }
}

/// The lookback and lookforward requirement for a pivot.
/// Default is 5.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ZigZagPeriod {
    pub left_bars: u16,
    pub right_bars: u16,
}

impl Default for ZigZagPeriod {
    fn default() -> Self {
        Self {
            left_bars: 5,
            right_bars: 5,
        }
    }
}

impl ZigZagPeriod {
    fn buffer_size(&self) -> usize {
        (self.left_bars + self.right_bars + 1) as usize
    }

    fn mid_index(&self) -> usize {
        self.left_bars as usize
    }
}

/// A HigherHigh/LowerLow indicator that identifies market structure points on a stream of OHLCV data.
/// It can be configured with a ZigZag alternation filter to eliminate market noise.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamingHhll {
    zig_zag_period: ZigZagPeriod,
    price_source: PriceSource,
    tiebreaker: ExtremeTiebreaker,
    alternation_mode: AlternationMode,

    // === Internal State ===
    /// The size of the rolling window buffer required to evaluate `left_bars` and `right_bars`.
    window_size: usize,

    /// The rolling window buffer required to evaluate `left_bars` and `right_bars`.
    buffer: VecDeque<Ohlcv>,

    /// The active pivot currently tracking the trailing edge of the market structure.
    ///
    /// If [`AlternationMode::Alternating`] is active, this pivot remains mutable.
    /// If a consecutive vertex of the same [`PivotType`] appears, this [`PivotPoint`]
    /// may be overwritten or extended based on the [`ExtremeTiebreaker`].
    active_pivot: Option<PivotPoint>,

    /// The historical, safely locked-in `PivotType::High` used as a baseline for relative classification.
    ///
    /// When a new High vertex is detected, it is compared against this anchor to determine if it
    /// is a `HigherHigh`, `LowerHigh`, or `EqualHigh`.
    anchor_high: Option<PivotPoint>,

    /// The historical, safely locked-in `PivotType::Low` used as a baseline for relative classification.
    ///
    /// When a new Low vertex is detected, it is compared against this anchor to determine if it
    /// is a `HigherLow`, `LowerLow`, or `EqualLow`.
    anchor_low: Option<PivotPoint>,

    /// Chronological history of all safely locked-in pivots.
    ///
    /// This vector guarantees perfect time-order. To iterate from latest to earliest,
    /// you simply call `self.history.iter().rev()`.
    history: Vec<PivotPoint>,
}

impl Default for StreamingHhll {
    fn default() -> Self {
        let zig_zag_period = ZigZagPeriod::default();
        let window_size = zig_zag_period.buffer_size();
        Self {
            zig_zag_period,
            price_source: PriceSource::default(),
            tiebreaker: ExtremeTiebreaker::default(),
            alternation_mode: AlternationMode::default(),
            window_size,
            buffer: VecDeque::with_capacity(window_size),
            active_pivot: None,
            anchor_high: None,
            anchor_low: None,
            history: Vec::new(),
        }
    }
}

impl StreamingHhll {
    pub fn with_zig_zag_period(self, zig_zag_period: ZigZagPeriod) -> Self {
        let window_size = zig_zag_period.buffer_size();
        Self {
            zig_zag_period,
            window_size,
            buffer: VecDeque::with_capacity(window_size),
            ..self
        }
    }

    pub fn with_price_source(self, price_source: PriceSource) -> Self {
        Self {
            price_source,
            ..self
        }
    }

    pub fn with_tiebreaker(self, tiebreaker: ExtremeTiebreaker) -> Self {
        Self { tiebreaker, ..self }
    }

    pub fn with_alternation_mode(self, alternation_mode: AlternationMode) -> Self {
        Self {
            alternation_mode,
            ..self
        }
    }

    pub fn history(&self) -> &[PivotPoint] {
        &self.history
    }
}

impl StreamingHhll {
    fn candidate(&self) -> Ohlcv {
        let mid_idx = self.zig_zag_period.mid_index();
        self.buffer[mid_idx]
    }

    /// Extracts the relevant Y-axis value for peak finding.
    fn extract_price(&self, candle: Ohlcv, pivot_type: PivotType) -> Price {
        let candle_direction = candle.direction();
        match (self.price_source, pivot_type, candle_direction) {
            (PriceSource::HighLow, PivotType::High, _) => candle.high,
            (PriceSource::HighLow, PivotType::Low, _) => candle.low,
            (PriceSource::CloseOpen, PivotType::High, CandleDirection::Bullish) => candle.close,
            (PriceSource::CloseOpen, PivotType::High, CandleDirection::Bearish) => candle.open,
            (PriceSource::CloseOpen, PivotType::Low, CandleDirection::Bullish) => candle.open,
            (PriceSource::CloseOpen, PivotType::Low, CandleDirection::Bearish) => candle.close,
            _ => candle.close,
        }
    }

    /// Yields the left side of the rolling window (before the candidate).
    fn left_partition(&self) -> impl Iterator<Item = Ohlcv> + '_ {
        self.buffer
            .iter()
            .take(self.zig_zag_period.left_bars as usize)
            .copied()
    }

    /// Yields the right side of the rolling window (after the candidate).
    fn right_partition(&self) -> impl Iterator<Item = Ohlcv> + '_ {
        self.buffer
            .iter()
            .rev()
            .take(self.zig_zag_period.right_bars as usize)
            .copied()
    }

    /// Checks if the candidate price is a valid extremum against its neighbors.
    fn check_extremum(&self, pivot_type: PivotType) -> bool {
        let mid_idx = self.zig_zag_period.mid_index();
        let candidate = self.buffer[mid_idx];
        let candidate_price = self.extract_price(candidate, pivot_type);

        // Determine which side of the window requires a STRICT inequality based on the tiebreaker.
        let (strict_left, strict_right) = match self.tiebreaker {
            ExtremeTiebreaker::Earliest => (true, false),
            ExtremeTiebreaker::Latest => (false, true),
        };

        let is_valid = |neighbor: Ohlcv, strict: bool| -> bool {
            let neighbor_price = self.extract_price(neighbor, pivot_type);
            match (pivot_type, strict) {
                (PivotType::High, true) => candidate_price > neighbor_price,
                (PivotType::High, false) => candidate_price >= neighbor_price,
                (PivotType::Low, true) => candidate_price < neighbor_price,
                (PivotType::Low, false) => candidate_price <= neighbor_price,
            }
        };

        self.left_partition().all(|c| is_valid(c, strict_left))
            && self.right_partition().all(|c| is_valid(c, strict_right))
    }

    fn process_high(&mut self) -> Option<(MarketStructureEvent, PivotPoint)> {
        let candidate = self.candidate();
        let price = self.extract_price(candidate, PivotType::High);
        let mut new_pivot = PivotPoint {
            ohlcv_candle: candidate,
            price,
            price_source: self.price_source,
            pivot_type: PivotType::High,
            trend: MarketStructureSequence::UnclassifiedHigh,
        };

        if let Some(latest) = self.active_pivot {
            match (self.alternation_mode, latest.pivot_type) {
                (AlternationMode::Alternating, PivotType::High) => {
                    // Alternation broken: Two highs in a row. Keep the highest.
                    if new_pivot.price > latest.price {
                        // Keep new, but don't lock it as an anchor yet.
                    } else {
                        return None;
                    }
                }
                (AlternationMode::Alternating, PivotType::Low) => {
                    self.anchor_low = Some(latest);
                    self.history.push(latest);
                }
                (AlternationMode::Consecutive, PivotType::High) => {
                    self.anchor_high = Some(latest);
                    self.history.push(latest);
                }
                (AlternationMode::Consecutive, PivotType::Low) => {
                    self.anchor_low = Some(latest);
                    self.history.push(latest);
                }
            }
        }

        let mut event = MarketStructureEvent::NoChange;

        // Expressive matching using the standard library's `PartialOrd` trait
        if let Some(anchor) = self.anchor_high {
            match new_pivot.price.partial_cmp(&anchor.price) {
                Some(Ordering::Greater) => {
                    new_pivot.trend = MarketStructureSequence::HigherHigh;
                    event = match self.anchor_low.map(|l| l.trend) {
                        Some(MarketStructureSequence::LowerLow) => {
                            MarketStructureEvent::MarketStructureShift
                        }
                        _ => MarketStructureEvent::BreakOfStructure,
                    };
                }
                Some(Ordering::Less) => new_pivot.trend = MarketStructureSequence::LowerHigh,
                _ => new_pivot.trend = MarketStructureSequence::EqualHigh,
            }
        }

        self.active_pivot = Some(new_pivot);
        Some((event, new_pivot))
    }

    fn process_low(&mut self) -> Option<(MarketStructureEvent, PivotPoint)> {
        let candidate = self.candidate();
        let price = self.extract_price(candidate, PivotType::Low);
        let mut new_pivot = PivotPoint {
            ohlcv_candle: candidate,
            price,
            price_source: self.price_source,
            pivot_type: PivotType::Low,
            trend: MarketStructureSequence::UnclassifiedLow,
        };

        if let Some(latest) = self.active_pivot {
            match (self.alternation_mode, latest.pivot_type) {
                (AlternationMode::Alternating, PivotType::Low) => {
                    if new_pivot.price < latest.price {
                        // Keep new, but don't lock it as an anchor yet.
                    } else {
                        return None;
                    }
                }
                (AlternationMode::Alternating, PivotType::High) => {
                    self.anchor_high = Some(latest);
                    self.history.push(latest);
                }
                (AlternationMode::Consecutive, PivotType::High) => {
                    self.anchor_high = Some(latest);
                    self.history.push(latest);
                }
                (AlternationMode::Consecutive, PivotType::Low) => {
                    self.anchor_low = Some(latest);
                    self.history.push(latest);
                }
            }
        }

        let mut event = MarketStructureEvent::NoChange;

        if let Some(anchor) = self.anchor_low {
            match new_pivot.price.partial_cmp(&anchor.price) {
                Some(Ordering::Less) => {
                    new_pivot.trend = MarketStructureSequence::LowerLow;
                    event = match self.anchor_high.map(|h| h.trend) {
                        Some(MarketStructureSequence::HigherHigh) => {
                            MarketStructureEvent::MarketStructureShift
                        }
                        _ => MarketStructureEvent::BreakOfStructure,
                    };
                }
                Some(Ordering::Greater) => new_pivot.trend = MarketStructureSequence::HigherLow,
                _ => new_pivot.trend = MarketStructureSequence::EqualLow,
            }
        }

        self.active_pivot = Some(new_pivot);
        Some((event, new_pivot))
    }
}

impl StreamingIndicator for StreamingHhll {
    type Input = Ohlcv;
    type Output = Option<(MarketStructureEvent, PivotPoint)>;

    fn update(&mut self, candle: Self::Input) -> Self::Output {
        self.buffer.push_back(candle);

        if self.buffer.len() < self.window_size {
            return None;
        }
        if self.buffer.len() > self.window_size {
            self.buffer.pop_front();
        }

        let is_swing_high = self.check_extremum(PivotType::High);
        let is_swing_low = self.check_extremum(PivotType::Low);

        match (is_swing_high, is_swing_low) {
            (true, true) => {
                // The candidate is BOTH a Swing High and a Swing Low (Mega Bar).
                let candidate = self.candidate();
                match candidate.direction() {
                    CandleDirection::Bullish => self.process_high(),
                    CandleDirection::Bearish => self.process_low(),
                    CandleDirection::Doji => {
                        // Assumption: Extend the current market structure
                        match self.active_pivot.map(|p| p.pivot_type) {
                            Some(PivotType::Low) => self.process_low(),
                            Some(PivotType::High) => self.process_high(),
                            None => None, // A doji candle and no history.
                        }
                    }
                }
            }
            (true, false) => self.process_high(),
            (false, true) => self.process_low(),
            (false, false) => None,
        }
    }

    fn reset(&mut self) {
        self.buffer.clear();
        self.active_pivot = None;
        self.anchor_high = None;
        self.anchor_low = None;
        self.history.clear();
    }
}

/*
*
*
*
* Implementation Plan Batch Indicator:
*
Here is the complete implementation plan for your **Batch Indicators**.

To maintain 100% logical equivalence with your Streaming indicators, we must split the work correctly. Polars is used for the heavy-lifting vectorized discovery (finding candidates instantly), and a fast Rust pre-pass over your AoS handles the path-dependent state (Alternation, Tiebreakers, and FVG mitigations) before the Gym environment begins.

===

### Part 1: Higher High Lower Low (Swing Points)

Because `AlternationMode::Strict` (ZigZag) is a path-dependent state machine (the validity of a High depends on the survival of the previous Low), doing this purely in Polars requires highly complex, slow custom aggregations. The optimal batch approach is a Polars candidate-search followed by a Rust filter.

#### Step 1: Polars Transformation (Candidate Discovery)

We use Polars to find every raw peak and valley in the dataset. To avoid lookahead bias in the Gym, we calculate them centered, but we do not map them to the Gym's observation space until `T + N`.

```python
import polars as pl

# N is your zig_zag_period (e.g., 5)
window = (N * 2) + 1

df = df.with_columns([
    # Find rolling max/min over the centered window
    pl.col("high").rolling_max(window_size=window, center=True).alias("roll_max"),
    pl.col("low").rolling_min(window_size=window, center=True).alias("roll_min"),
]).with_columns([
    # Flag candidates where the price equals the local extreme
    (pl.col("high") == pl.col("roll_max")).alias("is_candidate_high"),
    (pl.col("low") == pl.col("roll_min")).alias("is_candidate_low"),
])

```

#### Step 2: Rust AoS Pre-pass (State & Alignment)

Before episode 0, iterate over your `Box<[Ohlcv]>` and the boolean columns generated by Polars to finalize the state. This takes less than 10ms for a million rows.

1. **Tiebreaker Logic:** If Polars flagged multiple consecutive candles as `is_candidate_high` (a flat top), use your `ExtremeTiebreaker` enum. If `Latest`, ignore all flags except the last one in the cluster.
2. **Alternation Logic:** Run your `Strict` or `Unfiltered` logic just like the streaming version. If a candidate survives, classify it (`HH`, `LL`, etc.) against the `anchor`.
3. **Lookahead Alignment (Crucial):** If a `SwingPoint` is confirmed at index `T`, it cannot be known to the Gym until index `T + N`. You will write the resulting `SwingPoint` into a sidecar array exactly `N` steps forward: `precomputed_swings[T + N] = Some(swing_point)`.
4. **O(1) Lookup:** During `env.step(t)`, your agent simply reads `precomputed_swings[t]`. It requires zero computation.

===


*/
