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
        let candidate = self.candidate();
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

    #[tracing::instrument(skip(self), fields(ts = %self.candidate().close_timestamp))]
    fn process_high(&mut self) -> Option<(MarketStructureEvent, PivotPoint)> {
        let candidate = self.candidate();
        let current_high_price = self.extract_price(candidate, PivotType::High);

        if let Some(active) = self.active_pivot {
            match (self.alternation_mode, active.pivot_type) {
                (AlternationMode::Alternating, PivotType::High) => {
                    let overwrite = match self.tiebreaker {
                        // If Earliest is active, the first peak should hold its ground.
                        ExtremeTiebreaker::Earliest => current_high_price > active.price,
                        // If Latest is active, the second peak should replace the first one.
                        ExtremeTiebreaker::Latest => current_high_price >= active.price,
                    };

                    if !overwrite {
                        return None;
                    }
                }
                (AlternationMode::Alternating, PivotType::Low) => {
                    self.anchor_low = Some(active);
                    self.history.push(active);
                }
                (AlternationMode::Consecutive, PivotType::High) => {
                    self.anchor_high = Some(active);
                    self.history.push(active);
                }
                (AlternationMode::Consecutive, PivotType::Low) => {
                    self.anchor_low = Some(active);
                    self.history.push(active);
                }
            }
        }

        let (trend, event) = match self.anchor_high {
            Some(anchor) => match current_high_price.partial_cmp(&anchor.price) {
                Some(Ordering::Greater) => {
                    let market_structure_event = match self.anchor_low.map(|l| l.trend) {
                        Some(MarketStructureSequence::LowerLow) => {
                            MarketStructureEvent::MarketStructureShift
                        }
                        _ => MarketStructureEvent::BreakOfStructure,
                    };
                    (MarketStructureSequence::HigherHigh, market_structure_event)
                }
                Some(Ordering::Less) => (
                    MarketStructureSequence::LowerHigh,
                    MarketStructureEvent::NoChange,
                ),
                Some(Ordering::Equal) => (
                    MarketStructureSequence::EqualHigh,
                    MarketStructureEvent::NoChange,
                ),
                None => {
                    tracing::warn!(
                        reason = "nan_detected",
                        candidate_price = ?current_high_price,
                        anchor_price = ?anchor.price,
                        "Invalid float (NaN) detected. Discarding pivot to prevent state poisoning."
                    );
                    return None;
                }
            },
            None => (
                MarketStructureSequence::UnclassifiedHigh,
                MarketStructureEvent::NoChange,
            ),
        };

        let new_pivot = PivotPoint {
            ohlcv_candle: candidate,
            price: current_high_price,
            price_source: self.price_source,
            pivot_type: PivotType::High,
            trend,
        };

        self.active_pivot = Some(new_pivot);

        if event != MarketStructureEvent::NoChange {
            tracing::debug!(
                event = ?event,
                trend = ?trend,
                price = ?current_high_price,
                "Market Structure Extracted"
            );
        }

        Some((event, new_pivot))
    }

    #[tracing::instrument(skip(self), fields(ts = %self.candidate().close_timestamp))]
    fn process_low(&mut self) -> Option<(MarketStructureEvent, PivotPoint)> {
        let candidate = self.candidate();
        let current_low_price = self.extract_price(candidate, PivotType::Low);

        if let Some(latest) = self.active_pivot {
            match (self.alternation_mode, latest.pivot_type) {
                (AlternationMode::Alternating, PivotType::Low) => {
                    let overwrite = match self.tiebreaker {
                        // If Earliest is active, the first peak should hold its ground.
                        ExtremeTiebreaker::Earliest => current_low_price < latest.price,
                        // If Latest is active, the second peak should replace the first one.
                        ExtremeTiebreaker::Latest => current_low_price <= latest.price,
                    };

                    if !overwrite {
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

        let (trend, event) = match self.anchor_low {
            Some(anchor) => match current_low_price.partial_cmp(&anchor.price) {
                Some(Ordering::Less) => {
                    let market_structure_event = match self.anchor_high.map(|h| h.trend) {
                        Some(MarketStructureSequence::HigherHigh) => {
                            MarketStructureEvent::MarketStructureShift
                        }
                        _ => MarketStructureEvent::BreakOfStructure,
                    };
                    (MarketStructureSequence::LowerLow, market_structure_event)
                }
                Some(Ordering::Greater) => (
                    MarketStructureSequence::HigherLow,
                    MarketStructureEvent::NoChange,
                ),
                Some(Ordering::Equal) => (
                    MarketStructureSequence::EqualLow,
                    MarketStructureEvent::NoChange,
                ),
                None => {
                    tracing::warn!(
                        reason = "nan_detected",
                        candidate_price = ?current_low_price,
                        anchor_price = ?anchor.price,
                        "Invalid float (NaN) detected. Discarding pivot to prevent state poisoning."
                    );
                    return None;
                }
            },
            None => (
                MarketStructureSequence::UnclassifiedLow,
                MarketStructureEvent::NoChange,
            ),
        };

        let new_pivot = PivotPoint {
            ohlcv_candle: candidate,
            price: current_low_price,
            price_source: self.price_source,
            pivot_type: PivotType::Low,
            trend,
        };

        self.active_pivot = Some(new_pivot);

        if event != MarketStructureEvent::NoChange {
            tracing::debug!(
                event = ?event,
                trend = ?trend,
                price = ?current_low_price,
                "Market Structure Extracted"
            );
        }

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

#[cfg(test)]
mod tests {
    use crate::data::domain::Quantity;

    use super::*;
    use std::f64::EPSILON;

    // ==========================================
    // === 1. Mocks & Helpers ===
    // ==========================================

    /// Parse RFC3339 timestamp string to DateTime<Utc>.
    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    /// A rapid builder for OHLCV candles to keep our test trajectories readable.
    fn candle(time: &str, open: f64, high: f64, low: f64, close: f64) -> Ohlcv {
        Ohlcv {
            open_timestamp: ts(time),
            close_timestamp: ts(time),
            open: Price(open),
            high: Price(high),
            low: Price(low),
            close: Price(close),
            volume: Quantity(100.0),
            quote_asset_volume: None,
            number_of_trades: None,
            taker_buy_base_asset_volume: None,
            taker_buy_quote_asset_volume: None,
        }
    }

    /// Helper to assert floats with epsilon tolerance
    fn assert_f64_eq(a: f64, b: f64) {
        assert!((a - b).abs() < EPSILON, "Expected {} to equal {}", a, b);
    }

    // ==========================================
    // === 2. Geometric Math Tests ===
    // ==========================================

    #[test]
    fn test_pivot_point_interpolation() {
        let p1 = PivotPoint {
            ohlcv_candle: candle("2026-05-24T15:00:00Z", 100., 100., 100., 100.),
            price: Price(100.0),
            price_source: PriceSource::HighLow,
            pivot_type: PivotType::Low,
            trend: MarketStructureSequence::LowerLow,
        };

        // Target pivot is exactly 10 bars (and 10 minutes) later, price has risen by 50.
        let p2 = PivotPoint {
            ohlcv_candle: candle("2026-05-24T15:10:00Z", 150., 150., 150., 150.),
            price: Price(150.0),
            price_source: PriceSource::HighLow,
            pivot_type: PivotType::Low,
            trend: MarketStructureSequence::HigherLow,
        };

        // --- 1. Test Index Based Interpolation ---
        // Slope = (150 - 100) / (20 - 10) = 5.0 per bar
        let line_by_idx = p1.price_line_by_index(&p2, 10, 20);

        assert_f64_eq(line_by_idx(10).0, 100.0); // Start point
        assert_f64_eq(line_by_idx(15).0, 125.0); // Exact midpoint
        assert_f64_eq(line_by_idx(20).0, 150.0); // Target point
        assert_f64_eq(line_by_idx(25).0, 175.0); // Extrapolation into the future!

        // --- 2. Test Time Based Interpolation ---
        let line_by_time = p1.price_line_by_time(&p2);

        assert_f64_eq(line_by_time(ts("2026-05-24T15:00:00Z")).0, 100.0); // Start
        assert_f64_eq(line_by_time(ts("2026-05-24T15:05:00Z")).0, 125.0); // Midpoint (5 mins)
        assert_f64_eq(line_by_time(ts("2026-05-24T15:10:00Z")).0, 150.0); // Target
        assert_f64_eq(line_by_time(ts("2026-05-24T15:20:00Z")).0, 200.0); // Extrapolation into future
    }

    #[test]
    fn test_pivot_point_flat_line_and_zero_division() {
        let p1 = PivotPoint {
            ohlcv_candle: candle("2026-05-24T15:00:00Z", 100., 100., 100., 100.),
            price: Price(100.0),
            ..p1 // Fill rest with defaults
        };
        let p2 = PivotPoint {
            ohlcv_candle: candle("2026-05-24T15:00:00Z", 100., 100., 100., 100.),
            price: Price(100.0),
            ..p1
        };

        // Same index/time should result in flat line, NOT a NaN/Inf panic
        let line = p1.price_line_by_index(&p2, 5, 5);
        assert_f64_eq(line(10).0, 100.0);
    }

    // ==========================================
    // === 3. Indicator Microstructure Tests ===
    // ==========================================

    fn create_indicator(left: u16, right: u16, tiebreaker: ExtremeTiebreaker) -> StreamingHhll {
        StreamingHhll::default()
            .with_zig_zag_period(ZigZagPeriod {
                left_bars: left,
                right_bars: right,
            })
            .with_tiebreaker(tiebreaker)
            .with_alternation_mode(AlternationMode::Alternating)
            .with_price_source(PriceSource::HighLow)
    }

    #[test]
    fn test_basic_swing_high_detection() {
        // Window size 5: 2 left, 1 candidate, 2 right.
        let mut hhll = create_indicator(2, 2, ExtremeTiebreaker::Latest);

        let trajectory = vec![
            candle("2026-05-24T15:01:00Z", 10., 10., 10., 10.), // L1
            candle("2026-05-24T15:02:00Z", 15., 15., 15., 15.), // L2
            candle("2026-05-24T15:03:00Z", 20., 20., 20., 20.), // Peak (Candidate)
            candle("2026-05-24T15:04:00Z", 15., 15., 15., 15.), // R1
        ];

        for c in trajectory {
            assert!(
                hhll.update(c).is_none(),
                "Should not emit before right window is full"
            );
        }

        // Pushing R2 completes the right window for the Candidate (20.0).
        // It should immediately trigger the Swing High evaluation.
        let event = hhll
            .update(candle("2026-05-24T15:05:00Z", 10., 10., 10., 10.))
            .unwrap();

        assert_eq!(event.1.pivot_type, PivotType::High);
        assert_eq!(event.1.price.0, 20.0);
        assert_eq!(
            event.1.ohlcv_candle.open_timestamp,
            ts("2026-05-24T15:03:00Z")
        );
    }

    #[test]
    fn test_tiebreaker_double_top() {
        let trajectory = vec![
            candle("2026-05-24T15:01:00Z", 10., 10., 10., 10.),
            candle("2026-05-24T15:02:00Z", 20., 20., 20., 20.), // Peak 1
            candle("2026-05-24T15:03:00Z", 20., 20., 20., 20.), // Peak 2 (Double Top)
            candle("2026-05-24T15:04:00Z", 10., 10., 10., 10.),
            candle("2026-05-24T15:05:00Z", 10., 10., 10., 10.),
            candle("2026-05-24T15:06:00Z", 10., 10., 10., 10.),
        ];

        // Test Earliest: Should capture Peak 1
        let mut hhll_early = create_indicator(2, 2, ExtremeTiebreaker::Earliest);
        let mut early_result = None;
        for &c in &trajectory {
            if let Some(res) = hhll_early.update(c) {
                early_result = Some(res);
            }
        }
        assert_eq!(
            early_result.unwrap().1.ohlcv_candle.open_timestamp,
            ts("2026-05-24T15:02:00Z")
        );

        // Test Latest: Should capture Peak 2
        let mut hhll_late = create_indicator(2, 2, ExtremeTiebreaker::Latest);
        let mut late_result = None;
        for &c in &trajectory {
            if let Some(res) = hhll_late.update(c) {
                late_result = Some(res);
            }
        }
        assert_eq!(
            late_result.unwrap().1.ohlcv_candle.open_timestamp,
            ts("2026-05-24T15:03:00Z")
        );
    }

    #[test]
    fn test_outside_bar_mega_bar_resolution() {
        let mut hhll = create_indicator(1, 1, ExtremeTiebreaker::Latest);

        // Pre-fill history to make the last confirmed pivot a HIGH.
        // This means the algorithm should be looking for a LOW next.
        hhll.active_pivot = Some(PivotPoint {
            ohlcv_candle: candle("2026-05-24T14:00:00Z", 50., 50., 50., 50.),
            price: Price(50.0),
            price_source: PriceSource::HighLow,
            pivot_type: PivotType::High,
            trend: MarketStructureSequence::UnclassifiedHigh,
        });

        // 1. Send Left window
        hhll.update(candle("2026-05-24T15:01:00Z", 20., 20., 20., 20.));

        // 2. Send Mega Bar (Candidate)
        // High is higher than neighbors, Low is lower than neighbors. Doji close.
        hhll.update(candle("2026-05-24T15:02:00Z", 25., 30., 10., 25.));

        // 3. Send Right window (Triggers candidate evaluation)
        let event = hhll
            .update(candle("2026-05-24T15:03:00Z", 20., 20., 20., 20.))
            .unwrap();

        // Because active_pivot was a HIGH, and the Mega Bar was a Doji (trend inertia),
        // the state machine assumes the LOW happened chronologically after the HIGH.
        assert_eq!(event.1.pivot_type, PivotType::Low);
        assert_eq!(event.1.price.0, 10.0);
    }

    #[test]
    fn test_alternation_filter_overwrites_noise() {
        let mut hhll = create_indicator(1, 1, ExtremeTiebreaker::Latest);

        // Candle 1, 2, 3: First Peak at 20
        hhll.update(candle("1", 10., 10., 10., 10.));
        hhll.update(candle("2", 20., 20., 20., 20.));
        let p1 = hhll.update(candle("3", 15., 15., 15., 15.)).unwrap().1;
        assert_eq!(p1.price.0, 20.0);
        assert_eq!(p1.pivot_type, PivotType::High);

        // Candle 4: Dips slightly, but not enough to form a valid Swing Low
        // (assume it doesn't trigger a low in a wider window, but for this tiny window it might).
        // Let's force two Highs in a row by just looking at the active_pivot state.

        // Candle 4, 5, 6: Second Peak at 30 (Higher than the first!)
        hhll.update(candle("4", 15., 15., 15., 15.));
        hhll.update(candle("5", 30., 30., 30., 30.));
        let p2 = hhll.update(candle("6", 10., 10., 10., 10.)).unwrap().1;

        // The indicator should emit a new event, but under the hood,
        // because Alternation is active, anchor_high is still None (unconfirmed).
        assert_eq!(p2.price.0, 30.0);
        assert_eq!(p2.pivot_type, PivotType::High);

        // Ensure the lesser peak was overwritten and NOT pushed to history
        assert_eq!(hhll.history().len(), 0);
    }

    /// Tests the "Mega Bar" (Outside Bar) anomaly when the candidate closes as a Doji.
    ///
    /// # The Microstructure Logic
    /// When a candidate is mathematically BOTH a Swing High and a Swing Low (an outside bar),
    /// the algorithm must choose which extremum logically extends the current market structure.
    /// If the candle is a Doji (Open == Close, indicating no directional conviction),
    /// the algorithm relies on "trend inertia."
    ///
    /// # State Machine Interaction
    /// This test verifies that if the last active pivot was a `PivotType::Low`, passing a
    /// Mega Doji routes into `self.process_low()`, intentionally triggering the
    /// **Alternation Broken** (Low -> Low) path:
    ///
    /// 1. **Trend Extension:** If the Doji's low sweeps deeper than the active macro low,
    ///    it updates the extreme, keeping the downtrend alive.
    /// 2. **Internal Noise:** If the Doji's low is NOT deeper than the active macro low,
    ///    the state machine safely discards it as high-volatility internal consolidation,
    ///    returning `None`.
    #[test]
    fn test_outside_bar_doji_trend_inertia() {
        let mut hhll = StreamingHhll::default()
            .with_zig_zag_period(ZigZagPeriod {
                left_bars: 1,
                right_bars: 1,
            })
            .with_tiebreaker(ExtremeTiebreaker::Latest)
            .with_alternation_mode(AlternationMode::Alternating)
            .with_price_source(PriceSource::HighLow);

        // =========================================================================
        // SCENARIO 1: Mega Doji EXTENDS the established Low (Sweeps Liquidity)
        // =========================================================================

        // Hardcode the active pivot to a Low at price 10.0
        hhll.active_pivot = Some(PivotPoint {
            ohlcv_candle: candle("2026-05-24T14:00:00Z", 10., 10., 10., 10.),
            price: Price(10.0),
            price_source: PriceSource::HighLow,
            pivot_type: PivotType::Low,
            trend: MarketStructureSequence::UnclassifiedLow,
        });

        // 1. Push Left Window
        assert!(
            hhll.update(candle("2026-05-24T15:01:00Z", 20., 20., 15., 18.))
                .is_none()
        );

        // 2. Push Mega Doji Candidate (High > neighbors, Low < neighbors, Open == Close)
        // High is 30, Low is 5. Note that Candidate Low (5.0) < Active Pivot Low (10.0).
        assert!(
            hhll.update(candle("2026-05-24T15:02:00Z", 20., 30., 5., 20.))
                .is_none()
        );

        // 3. Push Right Window -> Triggers evaluation of the Mega Doji
        let event = hhll.update(candle("2026-05-24T15:03:00Z", 20., 20., 15., 18.));

        // Assert: The Doji triggered `process_low()`. Alternation broke (Low -> Low),
        // but because 5.0 < 10.0, it successfully overwrites the active pivot.
        assert!(
            event.is_some(),
            "Expected Doji to extend the Low, but it returned None"
        );
        let (_, pivot) = event.unwrap();
        assert_eq!(pivot.pivot_type, PivotType::Low);
        assert_eq!(pivot.price.0, 5.0, "Expected new active Low to be 5.0");

        // =========================================================================
        // SCENARIO 2: Mega Doji fails to extend the Low (Internal Noise)
        // =========================================================================
        hhll.reset();

        // Hardcode the active pivot to a very deep, established macro Low at price 1.0
        hhll.active_pivot = Some(PivotPoint {
            ohlcv_candle: candle("2026-05-24T16:00:00Z", 1., 1., 1., 1.),
            price: Price(1.0),
            price_source: PriceSource::HighLow,
            pivot_type: PivotType::Low,
            trend: MarketStructureSequence::UnclassifiedLow,
        });

        // 1. Push Left Window
        assert!(
            hhll.update(candle("2026-05-24T17:01:00Z", 20., 20., 15., 18.))
                .is_none()
        );

        // 2. Push Mega Doji Candidate (High > neighbors, Low < neighbors, Open == Close)
        // High is 30, Low is 5. Note that Candidate Low (5.0) is NOT < Active Pivot Low (1.0).
        assert!(
            hhll.update(candle("2026-05-24T17:02:00Z", 20., 30., 5., 20.))
                .is_none()
        );

        // 3. Push Right Window -> Triggers evaluation of the Mega Doji
        let event_noise = hhll.update(candle("2026-05-24T17:03:00Z", 20., 20., 15., 18.));

        // Assert: The Doji triggered `process_low()`. Alternation broke (Low -> Low).
        // Because 5.0 is NOT strictly less than 1.0, it correctly discards the Doji as internal noise.
        assert!(
            event_noise.is_none(),
            "Expected Doji to be discarded as noise, but it emitted an event"
        );
    }

    /// Tests Macro Tiebreaker Resolution: Earliest.
    ///
    /// # Scenario
    /// The market prints a High at 20.0. The price dips slightly, but fails to print
    /// a valid structural Low. It then rallies back to exactly 20.0, forming a Double Top.
    ///
    /// # Expected Behavior
    /// Because `Alternating` mode is active, the algorithm must choose which of the two
    /// consecutive Highs to keep. Under `ExtremeTiebreaker::Earliest`, it should reject
    /// the second peak and maintain the first one.
    #[test]
    fn test_macro_tiebreaker_equal_peaks_earliest() {
        let mut hhll = StreamingHhll::default()
            .with_zig_zag_period(ZigZagPeriod {
                left_bars: 1,
                right_bars: 1,
            })
            .with_alternation_mode(AlternationMode::Alternating)
            .with_tiebreaker(ExtremeTiebreaker::Earliest)
            .with_price_source(PriceSource::HighLow);

        let trajectory = vec![
            candle("2026-05-24T10:00:00Z", 10., 10., 10., 10.),
            candle("2026-05-24T10:01:00Z", 20., 20., 20., 20.), // Peak 1
            candle("2026-05-24T10:02:00Z", 15., 15., 15., 15.),
            candle("2026-05-24T10:03:00Z", 15., 15., 15., 15.), // Flat dip (No swing low)
            candle("2026-05-24T10:04:00Z", 20., 20., 20., 20.), // Peak 2 (Double Top)
            candle("2026-05-24T10:05:00Z", 10., 10., 10., 10.),
        ];

        let mut events = Vec::new();
        for c in trajectory {
            if let Some(event) = hhll.update(c) {
                events.push(event);
            }
        }

        // Only Peak 1 should have been emitted. Peak 2 evaluates in the state machine
        // but `20.0 > 20.0` is false, so it is discarded.
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].1.ohlcv_candle.open_timestamp,
            ts("2026-05-24T10:01:00Z")
        );
        assert_eq!(
            hhll.active_pivot.unwrap().ohlcv_candle.open_timestamp,
            ts("2026-05-24T10:01:00Z")
        );
    }

    /// Tests Macro Tiebreaker Resolution: Latest.
    ///
    /// # Expected Behavior
    /// In a Double Top scenario under `ExtremeTiebreaker::Latest`, the algorithm should
    /// emit the first peak, but when the second peak arrives, the state machine evaluates
    /// `20.0 >= 20.0` (True), usurping and overwriting the first peak with the Latest one.
    #[test]
    fn test_macro_tiebreaker_equal_peaks_latest() {
        let mut hhll = StreamingHhll::default()
            .with_zig_zag_period(ZigZagPeriod {
                left_bars: 1,
                right_bars: 1,
            })
            .with_alternation_mode(AlternationMode::Alternating)
            .with_tiebreaker(ExtremeTiebreaker::Latest)
            .with_price_source(PriceSource::HighLow);

        let trajectory = vec![
            candle("2026-05-24T10:00:00Z", 10., 10., 10., 10.),
            candle("2026-05-24T10:01:00Z", 20., 20., 20., 20.), // Peak 1
            candle("2026-05-24T10:02:00Z", 15., 15., 15., 15.),
            candle("2026-05-24T10:03:00Z", 15., 15., 15., 15.), // Flat dip (No swing low)
            candle("2026-05-24T10:04:00Z", 20., 20., 20., 20.), // Peak 2 (Double Top)
            candle("2026-05-24T10:05:00Z", 10., 10., 10., 10.),
        ];

        let mut events = Vec::new();
        for c in trajectory {
            if let Some(event) = hhll.update(c) {
                events.push(event);
            }
        }

        // Peak 1 is emitted first. Then Peak 2 arrives and is ALSO emitted because
        // it overwrites Peak 1 as the new active_pivot.
        assert_eq!(events.len(), 2);
        assert_eq!(
            events[0].1.ohlcv_candle.open_timestamp,
            ts("2026-05-24T10:01:00Z")
        ); // First emission
        assert_eq!(
            events[1].1.ohlcv_candle.open_timestamp,
            ts("2026-05-24T10:04:00Z")
        ); // Overwrite emission

        // The active pivot currently tracking the market should be Peak 2.
        assert_eq!(
            hhll.active_pivot.unwrap().ohlcv_candle.open_timestamp,
            ts("2026-05-24T10:04:00Z")
        );
    }
}
