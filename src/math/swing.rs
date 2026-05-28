use std::{cmp::Ordering, collections::VecDeque};

use crate::{
    data::{
        domain::{CandleDirection, Price, PriceSource},
        event::{IndexedOhlcv, MarketEvent, Ohlcv},
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

impl From<MarketStructureSequence> for PivotType {
    fn from(sequence: MarketStructureSequence) -> Self {
        use MarketStructureSequence::*;
        match sequence {
            LowerHigh | HigherHigh | EqualHigh | UnclassifiedHigh => PivotType::High,
            HigherLow | LowerLow | EqualLow | UnclassifiedLow => PivotType::Low,
        }
    }
}

impl PivotType {
    /// Extracts the relevant price for peak/trough finding given the
    /// configured [`PriceSource`] and the candle's own direction.
    fn extract_price(self, candle: Ohlcv, source: PriceSource) -> Price {
        match (source, self, candle.direction()) {
            (PriceSource::HighLow, PivotType::High, _) => candle.high,
            (PriceSource::HighLow, PivotType::Low, _) => candle.low,

            (PriceSource::OpenClose, PivotType::High, CandleDirection::Bullish) => candle.close,
            (PriceSource::OpenClose, PivotType::High, CandleDirection::Bearish) => candle.open,
            (PriceSource::OpenClose, PivotType::High, CandleDirection::Doji) => candle.close,

            (PriceSource::OpenClose, PivotType::Low, CandleDirection::Bullish) => candle.open,
            (PriceSource::OpenClose, PivotType::Low, CandleDirection::Bearish) => candle.close,
            (PriceSource::OpenClose, PivotType::Low, CandleDirection::Doji) => candle.close,
        }
    }
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

impl MarketStructureSequence {
    pub fn as_pivot_type(&self) -> PivotType {
        (*self).into()
    }
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

/// Represents structural breakthrough events detected in the price series.
///
/// A `MarketStructureEvent` is emitted alongside every new confirmed pivot.
/// It answers the question: _"Did this new pivot break a meaningful prior level,
/// and if so, does it continue or contradict the prevailing trend?"_
///
/// # Background
///
/// Most price action consists of small zig-zags within a range. Occasionally,
/// a new swing high breaks above the previous swing high (or a new swing low
/// breaks below the previous swing low). Those breakouts are the structural
/// events this enum classifies.
///
/// The classification depends on two things:
/// 1. Whether the new pivot exceeds the most recent same-side pivot
///    (e.g. a new high above the last confirmed high).
/// 2. What the prevailing trend looked like just before the break, inferred
///    from the most recent opposite-side pivot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketStructureEvent {
    /// The new pivot extends the prevailing trend, or establishes the first directional trend.
    ///
    /// A _Break of Structure_ (BOS) is the classification for any new pivot
    /// that exceeds its confirmed same-side predecessor. It fires when:
    ///
    /// - **Continuation**: the market was already trending in the same direction
    ///   (e.g., a new Higher High after a recent Higher Low).
    /// - **Initiation**: the pivot establishes the first directional trend by breaking
    ///   the initial anchor (e.g., a Higher High occurs, but the prior opposite-side
    ///   pivot is still `UnclassifiedLow` or `EqualLow`).
    BreakOfStructure,

    /// The new pivot reverses a previously confirmed trend.
    ///
    /// Also known as a _Change of Character_ (CHoCH). This is the strict case:
    /// it fires only when the prior opposite-side pivot was itself a trend-confirming
    /// break, so there is concrete evidence of a trend to reverse.
    ///
    /// - **Bullish Shift**: a new swing high prints above the most recent swing high,
    ///   and the most recent low was a `LowerLow` (downtrend -> potential uptrend).
    /// - **Bearish Shift**: a new swing low prints below the most recent swing low,
    ///   and the most recent high was a `HigherHigh` (uptrend -> potential downtrend).
    MarketStructureShift,

    /// The new pivot did not break structure or shift the trend.
    ///
    /// Returned when the candidate forms a Lower High, Equal High, Higher Low, or
    /// Equal Low. This is also emitted for the **very first detected pivot** (unclassified),
    /// as there is no prior structure on that side to compare against. The pivot is
    /// still recorded and added to the history. It just doesn't represent a breakout.
    NoChange,
}

/// Tiebreaker policy when two adjacent bars share the same extreme price.
///
/// Affects two situations:
/// 1. **Plateaus inside the lookback window**: when the candidate bar ties with one of
///    its neighbors for the highest/lowest price, this rule decides whether the candidate
///    still qualifies as a pivot.
/// 2. **Conflicts under [`AlternationMode::Alternating`]**: when a new pivot of the same
///    type as the active one is detected and their prices are equal, this rule decides
///    which one wins.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExtremeTiebreaker {
    /// On a tie, the later bar wins. Pivots track the most recent occurrence
    /// of a repeated extreme.
    #[default]
    Latest,
    /// On a tie, the earlier bar wins. Pivots anchor to the first occurrence
    /// of a repeated extreme.
    Earliest,
}

/// A confirmed swing point in the price series.
///
/// Carries the originating candle, the price that triggered the pivot (which may be
/// `high`/`low` or `open`/`close` depending on the configured [`PriceSource`]),
/// and the trend-relative classification ([`MarketStructureSequence`]).
///
/// Note: The geometric type ([`PivotType::High`] or [`PivotType::Low`]) is intrinsically
/// implied by the `trend` and can be accessed via [`Self::pivot_type`].
#[derive(Debug, Clone, Copy)]
pub struct PivotPoint {
    pub indexed_candle: IndexedOhlcv,
    pub price: Price,
    pub price_source: PriceSource,
    pub trend: MarketStructureSequence,
}
impl MarketEvent for PivotPoint {
    fn point_in_time(&self) -> DateTime<Utc> {
        self.indexed_candle.point_in_time()
    }
}

impl PivotPoint {
    /// Returns the geometric type of the pivot, derived directly from its trend sequence.
    pub fn pivot_type(&self) -> PivotType {
        self.trend.into()
    }

    /// Generates a linear interpolation function based on bar indices.
    ///
    /// Returns a zero-allocation closure that takes a target bar index (usize)
    /// and returns the interpolated/extrapolated price at that index.
    pub fn price_line_by_index(&self, target: &PivotPoint) -> impl Fn(usize) -> Price {
        let p0 = self.price.0;
        let p1 = target.price.0;
        let x0 = self.indexed_candle.index as f64;
        let x1 = target.indexed_candle.index as f64;

        let dx = x1 - x0;
        let m = if dx == 0.0 { 0.0 } else { (p1 - p0) / dx };

        move |x: usize| -> Price {
            let current_dx = (x as f64) - x0;
            Price(p0 + m * current_dx)
        }
    }

    /// Generates a linear interpolation function based on exact point-in-time timestamps.
    ///
    /// Returns a zero-allocation closure that takes a target point in time
    /// (DateTime<Utc>) and returns the interpolated/extrapolated price.
    /// Uses chrono::Duration to safely compute the time deltas in milliseconds.
    pub fn price_line_by_point_in_time(
        &self,
        target: &PivotPoint,
    ) -> impl Fn(DateTime<Utc>) -> Price {
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

/// Lookback and lookforward window for swing detection.
///
/// A bar is treated as a candidate pivot only if it is the most extreme bar within
/// a window of `left_bars` preceding bars and `right_bars` following bars. Larger
/// values produce fewer, more significant pivots and smaller values are more responsive
/// but noisier. Default is a symmetric window with `5` bars on each side.
///
/// Note that the indicator must buffer `left_bars + right_bars + 1` candles before
/// it can emit its first result, since the candidate sits in the middle of the window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ZigZagPeriod {
    pub left_bars: u16,
    pub right_bars: u16,
}

impl Default for ZigZagPeriod {
    fn default() -> Self {
        Self::symmetric(5)
    }
}

impl ZigZagPeriod {
    /// Creates a `ZigZagPeriod` with equal lookback and lookforward windows.
    ///
    /// Sets both `left_bars` and `right_bars` to `bars`, producing a symmetric
    /// window where the candidate pivot sits exactly in the middle.
    ///
    /// The indicator will buffer `2 * bars + 1` candles before emitting its first
    /// result.
    pub fn symmetric(bars: u16) -> Self {
        Self {
            left_bars: bars,
            right_bars: bars,
        }
    }

    fn buffer_size(&self) -> usize {
        (self.left_bars + self.right_bars + 1) as usize
    }

    fn mid_index(&self) -> usize {
        self.left_bars as usize
    }
}

/// A streaming Higher-High / Lower-Low indicator over OHLCV bars.
///
/// Consumes a stream of [`IndexedOhlcv`] bars and emits a confirmed [`PivotPoint`]
/// together with a [`MarketStructureEvent`] whenever a new swing point is identified
/// and either continues or breaks the prevailing trend.
///
/// # How it works
///
/// 1. Each incoming bar is appended to an internal rolling window of size
///    `left_bars + right_bars + 1` (see [`ZigZagPeriod`]).
/// 2. The bar at the center of that window is the *candidate*. It is considered a
///    pivot if it is the most extreme bar in the window (highest for a swing high,
///    lowest for a swing low). Ties are resolved per [`ExtremeTiebreaker`].
/// 3. When a candidate qualifies, it is classified relative to the most recent
///    confirmed pivot of the same kind (Higher High / Lower High / Equal High, etc.)
///    and a [`MarketStructureEvent`] is emitted.
/// 4. The [`AlternationMode`] controls what happens when consecutive same-type
///    pivots appear without an intervening opposite-type pivot.
///
/// Because the candidate sits at the middle of the window, every emitted pivot is
/// confirmed with a lag of `right_bars` bars — that's the price of removing
/// hindsight bias from swing detection.
///
/// # Output stream
///
/// [`StreamingIndicator::update`] returns `Some((event, pivot))` whenever a pivot
/// is confirmed, and `None` while the window is still filling or no pivot is detected.
/// The full chronological history is available via [`Self::history`].
///
/// # Builder
///
/// ```ignore
/// let hhll = StreamingHhll::default()
///     .with_zig_zag_period(ZigZagPeriod { left_bars: 3, right_bars: 3 })
///     .with_price_source(PriceSource::HighLow)
///     .with_alternation_mode(AlternationMode::Alternating)
///     .with_tiebreaker(ExtremeTiebreaker::Latest);
/// ```
#[derive(Debug, Clone)]
pub struct StreamingHhll {
    zig_zag_period: ZigZagPeriod,
    price_source: PriceSource,
    tiebreaker: ExtremeTiebreaker,
    alternation_mode: AlternationMode,

    // === Internal State ===
    /// The size of the rolling window buffer required to evaluate `left_bars` and `right_bars`.
    window_size: usize,

    /// The rolling window buffer required to evaluate `left_bars` and `right_bars`.
    buffer: VecDeque<IndexedOhlcv>,

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
    fn candidate(&self) -> IndexedOhlcv {
        let mid_idx = self.zig_zag_period.mid_index();
        self.buffer[mid_idx]
    }

    /// Yields the left side of the rolling window (before the candidate).
    fn left_partition(&self) -> impl Iterator<Item = IndexedOhlcv> + '_ {
        self.buffer
            .iter()
            .take(self.zig_zag_period.left_bars as usize)
            .copied()
    }

    /// Yields the right side of the rolling window (after the candidate).
    fn right_partition(&self) -> impl Iterator<Item = IndexedOhlcv> + '_ {
        self.buffer
            .iter()
            .rev()
            .take(self.zig_zag_period.right_bars as usize)
            .copied()
    }

    /// Checks if the candidate price is a valid extremum against its neighbors.
    fn check_extremum(&self, pivot_type: PivotType) -> bool {
        let candidate = self.candidate();
        let candidate_price = pivot_type.extract_price(candidate.candle, self.price_source);

        // Determine which side of the window requires a STRICT inequality based on the tiebreaker.
        let (strict_left, strict_right) = match self.tiebreaker {
            ExtremeTiebreaker::Earliest => (true, false),
            ExtremeTiebreaker::Latest => (false, true),
        };

        let is_valid = |neighbor: IndexedOhlcv, strict: bool| -> bool {
            let neighbor_price = pivot_type.extract_price(neighbor.candle, self.price_source);
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

    #[tracing::instrument(skip(self), fields(ts = %self.candidate().candle.close_timestamp))]
    fn process_high(&mut self) -> Option<(MarketStructureEvent, PivotPoint)> {
        let candidate = self.candidate();
        let current_high_price = PivotType::High.extract_price(candidate.candle, self.price_source);

        if let Some(active) = self.active_pivot {
            // Is there an alternation conflict? (Two Highs in a row under Alternating mode)
            let is_alternation_conflict = self.alternation_mode == AlternationMode::Alternating
                && active.pivot_type() == PivotType::High;

            if is_alternation_conflict {
                // Conflict Resolution
                let overwrite = match self.tiebreaker {
                    // If Earliest is active, the first peak should hold its ground.
                    ExtremeTiebreaker::Earliest => current_high_price > active.price,
                    // If Latest is active, the second peak should replace the first one.
                    ExtremeTiebreaker::Latest => current_high_price >= active.price,
                };

                if !overwrite {
                    return None; // Discard candidate, maintain the existing peak.
                }
            } else {
                // State Lock & History Invariant
                // We unconditionally lock the previous pivot because it is confirmed.
                match active.pivot_type() {
                    PivotType::High => self.anchor_high = Some(active),
                    PivotType::Low => self.anchor_low = Some(active),
                }
                self.history.push(active);
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
            indexed_candle: candidate,
            price: current_high_price,
            price_source: self.price_source,
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

    #[tracing::instrument(skip(self), fields(ts = %self.candidate().candle.close_timestamp))]
    fn process_low(&mut self) -> Option<(MarketStructureEvent, PivotPoint)> {
        let candidate = self.candidate();
        let current_low_price = PivotType::Low.extract_price(candidate.candle, self.price_source);

        if let Some(active) = self.active_pivot {
            // Is there an alternation conflict? (Two Lows in a row under Alternating mode)
            let is_alternation_conflict = self.alternation_mode == AlternationMode::Alternating
                && active.pivot_type() == PivotType::Low;

            if is_alternation_conflict {
                // Conflict Resolution
                let overwrite = match self.tiebreaker {
                    // If Earliest is active, the first trough should hold its ground.
                    ExtremeTiebreaker::Earliest => current_low_price < active.price,
                    // If Latest is active, the second trough should replace the first one.
                    ExtremeTiebreaker::Latest => current_low_price <= active.price,
                };

                if !overwrite {
                    return None; // Discard candidate, maintain the existing trough.
                }
            } else {
                // State Lock & History Invariant
                // We unconditionally lock the previous pivot because it is confirmed.
                match active.pivot_type() {
                    PivotType::High => self.anchor_high = Some(active),
                    PivotType::Low => self.anchor_low = Some(active),
                }
                self.history.push(active);
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
            indexed_candle: candidate,
            price: current_low_price,
            price_source: self.price_source,
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
    type Input = IndexedOhlcv;
    type Output<'a> = Option<(MarketStructureEvent, PivotPoint)>;

    fn update(&mut self, indexed_candle: Self::Input) -> Self::Output<'_> {
        self.buffer.push_back(indexed_candle);

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
                match candidate.candle.direction() {
                    CandleDirection::Bullish => self.process_high(),
                    CandleDirection::Bearish => self.process_low(),
                    CandleDirection::Doji => {
                        // Assumption: Extend the current market structure
                        match self.active_pivot.map(|p| p.pivot_type()) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::domain::Quantity;
    use std::f64::EPSILON;

    /*
     *
     * Record unittest to handle the inial classification correct. Is it BOS or NoChange?
     * Unittest: impl From<MarketStructureSequence> for PivotType
     *
     */

    // ==========================================
    // === 1. Mocks & Helpers ===
    // ==========================================

    /// Parse RFC3339 timestamp string to DateTime<Utc>.
    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    /// A rapid builder for Indexed OHLCV candles to keep our test trajectories readable.
    fn candle(
        index: usize,
        time: &str,
        open: f64,
        high: f64,
        low: f64,
        close: f64,
    ) -> IndexedOhlcv {
        IndexedOhlcv {
            index,
            candle: Ohlcv {
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
            },
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
            indexed_candle: candle(10, "2026-05-24T15:00:00Z", 100., 100., 100., 100.),
            price: Price(100.0),
            price_source: PriceSource::HighLow,
            trend: MarketStructureSequence::LowerLow,
        };

        // Target pivot is exactly 10 bars (and 10 minutes) later, price has risen by 50.
        let p2 = PivotPoint {
            indexed_candle: candle(20, "2026-05-24T15:10:00Z", 150., 150., 150., 150.),
            price: Price(150.0),
            price_source: PriceSource::HighLow,
            trend: MarketStructureSequence::HigherLow,
        };

        // --- 1. Test Index Based Interpolation ---
        // Slope = (150 - 100) / (20 - 10) = 5.0 per bar
        let line_by_idx = p1.price_line_by_index(&p2);

        assert_f64_eq(line_by_idx(10).0, 100.0); // Start point
        assert_f64_eq(line_by_idx(15).0, 125.0); // Exact midpoint
        assert_f64_eq(line_by_idx(20).0, 150.0); // Target point
        assert_f64_eq(line_by_idx(25).0, 175.0); // Extrapolation into the future!

        // --- 2. Test Time Based Interpolation ---
        let line_by_time = p1.price_line_by_point_in_time(&p2);

        assert_f64_eq(line_by_time(ts("2026-05-24T15:00:00Z")).0, 100.0); // Start
        assert_f64_eq(line_by_time(ts("2026-05-24T15:05:00Z")).0, 125.0); // Midpoint (5 mins)
        assert_f64_eq(line_by_time(ts("2026-05-24T15:10:00Z")).0, 150.0); // Target
        assert_f64_eq(line_by_time(ts("2026-05-24T15:20:00Z")).0, 200.0); // Extrapolation into future
    }

    #[test]
    fn test_pivot_point_flat_line_and_zero_division() {
        let p1 = PivotPoint {
            indexed_candle: candle(5, "2026-05-24T15:00:00Z", 100., 100., 100., 100.),
            price: Price(100.0),
            price_source: PriceSource::HighLow,
            trend: MarketStructureSequence::LowerLow,
        };

        let p2 = PivotPoint {
            indexed_candle: candle(5, "2026-05-24T15:00:00Z", 100., 100., 100., 100.),
            price: Price(100.0),
            price_source: PriceSource::HighLow,
            trend: MarketStructureSequence::LowerLow,
        };

        // Same index/time should result in flat line, NOT a NaN/Inf panic
        let line = p1.price_line_by_index(&p2);
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
            candle(0, "2026-05-24T15:01:00Z", 10., 10., 10., 10.), // L1
            candle(1, "2026-05-24T15:02:00Z", 15., 15., 15., 15.), // L2
            candle(2, "2026-05-24T15:03:00Z", 20., 20., 20., 20.), // Peak (Candidate)
            candle(3, "2026-05-24T15:04:00Z", 15., 15., 15., 15.), // R1
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
            .update(candle(4, "2026-05-24T15:05:00Z", 10., 10., 10., 10.))
            .unwrap();

        assert_eq!(event.1.pivot_type(), PivotType::High);
        assert_eq!(event.1.price.0, 20.0);
        assert_eq!(
            event.1.indexed_candle.candle.open_timestamp,
            ts("2026-05-24T15:03:00Z")
        );
    }

    #[test]
    fn test_tiebreaker_double_top() {
        let trajectory = vec![
            candle(0, "2026-05-24T15:01:00Z", 10., 10., 10., 10.),
            candle(1, "2026-05-24T15:02:00Z", 20., 20., 20., 20.), // Peak 1
            candle(2, "2026-05-24T15:03:00Z", 20., 20., 20., 20.), // Peak 2 (Double Top)
            candle(3, "2026-05-24T15:04:00Z", 10., 10., 10., 10.),
            candle(4, "2026-05-24T15:05:00Z", 10., 10., 10., 10.),
            candle(5, "2026-05-24T15:06:00Z", 10., 10., 10., 10.),
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
            early_result.unwrap().1.indexed_candle.candle.open_timestamp,
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
            late_result.unwrap().1.indexed_candle.candle.open_timestamp,
            ts("2026-05-24T15:03:00Z")
        );
    }

    #[test]
    fn test_outside_bar_mega_bar_resolution() {
        let mut hhll = create_indicator(1, 1, ExtremeTiebreaker::Latest);

        // Pre-fill history to make the last confirmed pivot a HIGH.
        // This means the algorithm should be looking for a LOW next.
        hhll.active_pivot = Some(PivotPoint {
            indexed_candle: candle(0, "2026-05-24T14:00:00Z", 50., 50., 50., 50.),
            price: Price(50.0),
            price_source: PriceSource::HighLow,
            trend: MarketStructureSequence::UnclassifiedHigh,
        });

        // 1. Send Left window
        hhll.update(candle(1, "2026-05-24T15:01:00Z", 20., 20., 20., 20.));

        // 2. Send Mega Bar (Candidate)
        // High is higher than neighbors, Low is lower than neighbors. Doji close.
        hhll.update(candle(2, "2026-05-24T15:02:00Z", 25., 30., 10., 25.));

        // 3. Send Right window (Triggers candidate evaluation)
        let event = hhll
            .update(candle(3, "2026-05-24T15:03:00Z", 20., 20., 20., 20.))
            .unwrap();

        // Because active_pivot was a HIGH, and the Mega Bar was a Doji (trend inertia),
        // the state machine assumes the LOW happened chronologically after the HIGH.
        assert_eq!(event.1.pivot_type(), PivotType::Low);
        assert_eq!(event.1.price.0, 10.0);
    }

    #[test]
    fn test_alternation_filter_overwrites_noise() {
        let mut hhll = create_indicator(1, 1, ExtremeTiebreaker::Latest);

        // Candle 1, 2, 3: First Peak at 20
        hhll.update(candle(0, "2026-05-24T10:01:00Z", 10., 10., 10., 10.));
        hhll.update(candle(1, "2026-05-24T10:02:00Z", 20., 20., 20., 20.));
        let p1 = hhll
            .update(candle(2, "2026-05-24T10:03:00Z", 15., 15., 15., 15.))
            .unwrap()
            .1;
        assert_eq!(p1.price.0, 20.0);
        assert_eq!(p1.pivot_type(), PivotType::High);

        // Candle 4: Dips slightly, but not enough to form a valid Swing Low
        // (assume it doesn't trigger a low in a wider window, but for this tiny window it might).
        // Let's force two Highs in a row by just looking at the active_pivot state.

        // Candle 4, 5, 6: Second Peak at 30 (Higher than the first!)
        hhll.update(candle(3, "2026-05-24T10:04:00Z", 15., 15., 15., 15.));
        hhll.update(candle(4, "2026-05-24T10:05:00Z", 30., 30., 30., 30.));
        let p2 = hhll
            .update(candle(5, "2026-05-24T10:06:00Z", 10., 10., 10., 10.))
            .unwrap()
            .1;

        // The indicator should emit a new event, but under the hood,
        // because Alternation is active, anchor_high is still None (unconfirmed).
        assert_eq!(p2.price.0, 30.0);
        assert_eq!(p2.pivot_type(), PivotType::High);

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
            indexed_candle: candle(0, "2026-05-24T14:00:00Z", 10., 10., 10., 10.),
            price: Price(10.0),
            price_source: PriceSource::HighLow,
            trend: MarketStructureSequence::UnclassifiedLow,
        });

        // 1. Push Left Window
        assert!(
            hhll.update(candle(1, "2026-05-24T15:01:00Z", 20., 20., 15., 18.))
                .is_none()
        );

        // 2. Push Mega Doji Candidate (High > neighbors, Low < neighbors, Open == Close)
        // High is 30, Low is 5. Note that Candidate Low (5.0) < Active Pivot Low (10.0).
        assert!(
            hhll.update(candle(2, "2026-05-24T15:02:00Z", 20., 30., 5., 20.))
                .is_none()
        );

        // 3. Push Right Window -> Triggers evaluation of the Mega Doji
        let event = hhll.update(candle(3, "2026-05-24T15:03:00Z", 20., 20., 15., 18.));

        // Assert: The Doji triggered `process_low()`. Alternation broke (Low -> Low),
        // but because 5.0 < 10.0, it successfully overwrites the active pivot.
        assert!(
            event.is_some(),
            "Expected Doji to extend the Low, but it returned None"
        );
        let (_, pivot) = event.unwrap();
        assert_eq!(pivot.pivot_type(), PivotType::Low);
        assert_eq!(pivot.price.0, 5.0, "Expected new active Low to be 5.0");

        // =========================================================================
        // SCENARIO 2: Mega Doji fails to extend the Low (Internal Noise)
        // =========================================================================
        hhll.reset();

        // Hardcode the active pivot to a very deep, established macro Low at price 1.0
        hhll.active_pivot = Some(PivotPoint {
            indexed_candle: candle(0, "2026-05-24T16:00:00Z", 1., 1., 1., 1.),
            price: Price(1.0),
            price_source: PriceSource::HighLow,
            trend: MarketStructureSequence::UnclassifiedLow,
        });

        // 1. Push Left Window
        assert!(
            hhll.update(candle(1, "2026-05-24T17:01:00Z", 20., 20., 15., 18.))
                .is_none()
        );

        // 2. Push Mega Doji Candidate (High > neighbors, Low < neighbors, Open == Close)
        // High is 30, Low is 5. Note that Candidate Low (5.0) is NOT < Active Pivot Low (1.0).
        assert!(
            hhll.update(candle(2, "2026-05-24T17:02:00Z", 20., 30., 5., 20.))
                .is_none()
        );

        // 3. Push Right Window -> Triggers evaluation of the Mega Doji
        let event_noise = hhll.update(candle(3, "2026-05-24T17:03:00Z", 20., 20., 15., 18.));

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
            candle(0, "2026-05-24T10:00:00Z", 10., 10., 10., 10.),
            candle(1, "2026-05-24T10:01:00Z", 20., 20., 20., 20.), // Peak 1
            candle(2, "2026-05-24T10:02:00Z", 15., 15., 15., 15.),
            candle(3, "2026-05-24T10:03:00Z", 15., 15., 15., 15.), // Flat dip (No swing low)
            candle(4, "2026-05-24T10:04:00Z", 20., 20., 20., 20.), // Peak 2 (Double Top)
            candle(5, "2026-05-24T10:05:00Z", 10., 10., 10., 10.),
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
            events[0].1.indexed_candle.candle.open_timestamp,
            ts("2026-05-24T10:01:00Z")
        );
        assert_eq!(
            hhll.active_pivot
                .unwrap()
                .indexed_candle
                .candle
                .open_timestamp,
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
            candle(0, "2026-05-24T10:00:00Z", 10., 10., 10., 10.),
            candle(1, "2026-05-24T10:01:00Z", 20., 20., 20., 20.), // Peak 1
            candle(2, "2026-05-24T10:02:00Z", 15., 15., 15., 15.),
            candle(3, "2026-05-24T10:03:00Z", 15., 15., 15., 15.), // Flat dip (No swing low)
            candle(4, "2026-05-24T10:04:00Z", 20., 20., 20., 20.), // Peak 2 (Double Top)
            candle(5, "2026-05-24T10:05:00Z", 10., 10., 10., 10.),
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
            events[0].1.indexed_candle.candle.open_timestamp,
            ts("2026-05-24T10:01:00Z")
        ); // First emission
        assert_eq!(
            events[1].1.indexed_candle.candle.open_timestamp,
            ts("2026-05-24T10:04:00Z")
        ); // Overwrite emission

        // The active pivot currently tracking the market should be Peak 2.
        assert_eq!(
            hhll.active_pivot
                .unwrap()
                .indexed_candle
                .candle
                .open_timestamp,
            ts("2026-05-24T10:04:00Z")
        );
    }

    /// Tests the History Invariant under `Alternating` mode.
    ///
    /// # Expected Behavior
    /// In `Alternating` mode, if two Highs occur in a row, the lesser High is discarded
    /// (overwritten). It must NOT be pushed to the `history` vector.
    /// The `history` vector should only grow when a pivot is confirmed by an alternating pivot.
    #[test]
    fn test_history_invariant_alternating_mode() {
        let mut hhll = StreamingHhll::default()
            .with_zig_zag_period(ZigZagPeriod {
                left_bars: 1,
                right_bars: 1,
            })
            .with_alternation_mode(AlternationMode::Alternating)
            .with_tiebreaker(ExtremeTiebreaker::Latest)
            .with_price_source(PriceSource::HighLow);

        let trajectory = vec![
            candle(0, "2026-05-24T10:00:00Z", 10., 10., 10., 10.),
            candle(1, "2026-05-24T10:01:00Z", 20., 20., 20., 20.), // Peak 1 (High)
            candle(2, "2026-05-24T10:02:00Z", 15., 15., 15., 15.), // Dip (No confirmed Low yet)
            candle(3, "2026-05-24T10:03:00Z", 30., 30., 30., 30.), // Peak 2 (Higher High)
            candle(4, "2026-05-24T10:04:00Z", 10., 10., 10., 10.),
        ];

        for c in trajectory {
            let _ = hhll.update(c);
        }

        // Active pivot is Peak 2.
        assert_eq!(hhll.active_pivot.unwrap().price.0, 30.0);

        // History should be EMPTY. Peak 1 was a High, Peak 2 was a High.
        // Because alternation was broken, Peak 1 was discarded and never locked in.
        assert_eq!(
            hhll.history().len(),
            0,
            "History should not contain overwritten pivots"
        );

        // Now, push a confirmed Low to lock in Peak 2.
        let _ = hhll.update(candle(5, "2026-05-24T10:05:00Z", 5., 5., 5., 5.)); // Swing Low
        let _ = hhll.update(candle(6, "2026-05-24T10:06:00Z", 10., 10., 10., 10.)); // Right window to trigger Low

        // NOW Peak 2 should be safely locked in history.
        assert_eq!(hhll.history().len(), 1);
        assert_eq!(hhll.history()[0].price.0, 30.0);
        assert_eq!(hhll.history()[0].pivot_type(), PivotType::High);
    }

    /// Tests the History Invariant under `Consecutive` mode.
    ///
    /// # Expected Behavior
    /// In `Consecutive` mode, no filter is applied. Every single valid extreme detected
    /// by the window is instantly locked and pushed to the `history` vector, even if
    /// there are 5 Highs in a row.
    #[test]
    fn test_history_invariant_consecutive_mode() {
        let mut hhll = StreamingHhll::default()
            .with_zig_zag_period(ZigZagPeriod {
                left_bars: 1,
                right_bars: 1,
            })
            .with_alternation_mode(AlternationMode::Consecutive) // Unfiltered
            .with_tiebreaker(ExtremeTiebreaker::Latest)
            .with_price_source(PriceSource::HighLow);

        let trajectory = vec![
            candle(0, "2026-05-24T10:00:00Z", 10., 10., 10., 10.),
            candle(1, "2026-05-24T10:01:00Z", 20., 20., 20., 20.), // Peak 1
            candle(2, "2026-05-24T10:02:00Z", 15., 15., 15., 15.),
            candle(3, "2026-05-24T10:03:00Z", 30., 30., 30., 30.), // Peak 2
            candle(4, "2026-05-24T10:04:00Z", 10., 10., 10., 10.),
            candle(5, "2026-05-24T10:05:00Z", 40., 40., 40., 40.), // Peak 3
            candle(6, "2026-05-24T10:06:00Z", 10., 10., 10., 10.),
        ];

        for c in trajectory {
            let _ = hhll.update(c);
        }

        // Active pivot is tracking Peak 3.
        assert_eq!(hhll.active_pivot.unwrap().price.0, 40.0);

        // History should contain Peak 1 and Peak 2, despite them all being Highs.
        assert_eq!(
            hhll.history().len(),
            2,
            "Consecutive mode should lock all previous peaks"
        );
        assert_eq!(hhll.history()[0].price.0, 20.0);
        assert_eq!(hhll.history()[1].price.0, 30.0);
    }
}
