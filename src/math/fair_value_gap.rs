use std::{collections::VecDeque, fmt::Debug};

use chrono::{DateTime, Duration, Utc};

use crate::{
    data::{
        domain::Price,
        event::{IndexedOhlcv, MarketEvent, Ohlcv},
    },
    math::StreamingIndicator,
};

const LHS: usize = 0;
const MID: usize = 1;
const RHS: usize = 2;
const PATTERN_LENGTH: usize = 3;

/// Defines the time to live (ttl) condition under which a Fair Value Gap expires.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TtlPolicy {
    /// Expires after a specific number of bars have passed since creation.
    Bars(usize),
    /// Expires after a specific time duration has passed since creation.
    Time(Duration),
    /// Never expires automatically. Stays open until completely filled.
    #[default]
    Filled,
}

pub trait FairValueGapState: Debug + Clone + Send + Sync + 'static {}

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct OpenState {
    max_fill_percentage: f64,
    touch_count: u32,
}

impl OpenState {
    pub fn max_fill_percentage(&self) -> f64 {
        self.max_fill_percentage
    }

    pub fn touch_count(&self) -> u32 {
        self.touch_count
    }
}

impl FairValueGapState for OpenState {}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ClosedState {
    closed_time: DateTime<Utc>,
    touch_count: u32,
}

impl ClosedState {
    pub fn closed_time(&self) -> DateTime<Utc> {
        self.closed_time
    }

    pub const fn max_fill_percentage(&self) -> f64 {
        1.0
    }

    pub fn touch_count(&self) -> u32 {
        self.touch_count
    }
}

impl FairValueGapState for ClosedState {}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ExpiredState {
    expired_time: DateTime<Utc>,
    touch_count: u32,
    final_fill_percentage: f64,
}

impl ExpiredState {
    pub fn expired_time(&self) -> DateTime<Utc> {
        self.expired_time
    }
    pub fn final_fill_percentage(&self) -> f64 {
        self.final_fill_percentage
    }
    pub fn touch_count(&self) -> u32 {
        self.touch_count
    }
}

impl FairValueGapState for ExpiredState {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FairValueGapDirection {
    Bullish,
    Bearish,
}

/// Represents how a price candle interacted with a Fair Value Gap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GapInteraction {
    /// The candle's price range completely missed the gap (no overlap).
    Miss,
    /// The candle's price range entered the gap, but did not pierce the far boundary.
    Touch,
    /// The candle's price range completely pierced the required boundary to fill the gap.
    Fill,
}

impl GapInteraction {
    /// Returns true if the candle touched OR filled the gap.
    pub fn is_touch(&self) -> bool {
        matches!(self, Self::Touch | Self::Fill)
    }

    /// Returns true strictly if the candle filled the gap.
    pub fn is_fill(&self) -> bool {
        matches!(self, Self::Fill)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct FairValueGap<S: FairValueGapState> {
    direction: FairValueGapDirection,
    creation_time: DateTime<Utc>,
    creation_index: usize,
    top: Price,
    bottom: Price,
    window: [Ohlcv; 3],
    state: S,
}

#[derive(Debug, Clone, Copy)]
pub enum FairValueGapStatus {
    Open(FairValueGap<OpenState>),
    Closed(FairValueGap<ClosedState>),
    Expired(FairValueGap<ExpiredState>),
}

impl MarketEvent for FairValueGapStatus {
    fn point_in_time(&self) -> DateTime<Utc> {
        match self {
            FairValueGapStatus::Open(gap) => gap.point_in_time(),
            FairValueGapStatus::Closed(gap) => gap.point_in_time(),
            FairValueGapStatus::Expired(gap) => gap.point_in_time(),
        }
    }
}

impl<S: FairValueGapState> MarketEvent for FairValueGap<S> {
    fn point_in_time(&self) -> DateTime<Utc> {
        self.creation_time
    }
}

impl<S: FairValueGapState> FairValueGap<S> {
    pub fn direction(&self) -> FairValueGapDirection {
        self.direction
    }

    pub fn creation_time(&self) -> DateTime<Utc> {
        self.creation_time
    }

    /// Returns the index of the OHLCV candle that created this gap.
    pub fn creation_index(&self) -> usize {
        self.creation_index
    }

    pub fn top(&self) -> Price {
        self.top
    }

    pub fn bottom(&self) -> Price {
        self.bottom
    }

    pub fn state(&self) -> &S {
        &self.state
    }

    pub fn gap_size(&self) -> f64 {
        (self.top.0 - self.bottom.0).abs()
    }

    /// The three candles that formed this gap, chronological `[first, displacement, last]`.
    pub fn window(&self) -> &[Ohlcv; 3] {
        &self.window
    }

    /// The first (left) candle. Its extreme forms the near edge of the gap.
    pub fn first(&self) -> Ohlcv {
        self.window[LHS]
    }

    /// The middle candle. The impulse/displacement bar whose move opened the gap.
    pub fn displacement(&self) -> Ohlcv {
        self.window[MID]
    }

    /// The last (right) candle. Its extreme forms the far edge of the gap and
    /// carries the creation index/time.
    pub fn last(&self) -> Ohlcv {
        self.window[RHS]
    }

    /// The high of the displacement candle.
    ///
    /// This is the breakout extreme under the displacement reading of a bullish
    /// setup, where only the impulse bar counts and the surrounding candles are
    /// ignored. Equivalent to `self.displacement().high`.
    pub fn displacement_high(&self) -> Price {
        self.displacement().high
    }

    /// The low of the displacement candle.
    ///
    /// This is the breakout extreme under the displacement reading of a bearish
    /// setup. Equivalent to `self.displacement().low`.
    pub fn displacement_low(&self) -> Price {
        self.displacement().low
    }

    /// The highest high across all three candles of the window.
    ///
    /// This is the breakout extreme under the whole movement reading of a bullish
    /// setup, where the entire three candle leg is considered rather than the
    /// displacement bar alone. It is always greater than or equal to
    /// [`Self::displacement_high`].
    pub fn movement_high(&self) -> Price {
        let candles = self.window();
        Price(
            candles[LHS]
                .high
                .0
                .max(candles[MID].high.0)
                .max(candles[RHS].high.0),
        )
    }

    /// The lowest low across all three candles of the window.
    ///
    /// This is the breakout extreme under the whole movement reading of a bearish
    /// setup. It is always less than or equal to [`Self::displacement_low`].
    pub fn movement_low(&self) -> Price {
        let candles = self.window();
        Price(
            candles[LHS]
                .low
                .0
                .min(candles[MID].low.0)
                .min(candles[RHS].low.0),
        )
    }

    /// The open of the first candle. The start of the movement.
    pub fn movement_open(&self) -> Price {
        self.first().open
    }

    /// The close of the last candle. The end of the movement.
    pub fn movement_close(&self) -> Price {
        self.last().close
    }

    pub fn map<NewState: FairValueGapState, F>(self, f: F) -> FairValueGap<NewState>
    where
        F: FnOnce(S) -> NewState,
    {
        FairValueGap {
            direction: self.direction,
            creation_time: self.creation_time,
            creation_index: self.creation_index,
            top: self.top,
            bottom: self.bottom,
            window: self.window,
            state: f(self.state),
        }
    }

    /// Evaluates how a given candle's price action interacts with the gap's price zone.
    ///
    /// # The Overlap Logic (Filtering Breakaway Gaps)
    /// A candle's traded range is a continuous interval defined as `[low, high]`.
    /// The gap's price zone is defined as `[bottom, top]`.
    ///
    /// For a candle to interact with the gap, the market must have physically traded
    /// inside that zone. Mathematically, two 1-dimensional intervals `[A, B]` and
    /// `[C, D]` intersect if and only if `A < D` AND `B > C`.
    ///
    /// Applying this to our market data:
    /// `candle.low < gap.top` AND `candle.high > gap.bottom`
    ///
    /// **Why this is critical:**
    /// In markets that close (like traditional equities) or over weekends (like Forex),
    /// the price can open drastically lower or higher than the previous close.
    /// If a Bullish Gap exists at `[10.0, 15.0]`, and the market violently crashes
    /// overnight to open at `5.0` and wicks to a high of `8.0`, the price is technically
    /// "below" the gap. But because `candle.high (8.0)` is NOT `> gap.bottom (10.0)`,
    /// the overlap check correctly identifies that the market teleported _over_ the
    /// zone without ever actually trading inside it. It remains an untouched Miss.
    pub fn evaluate_interaction(&self, candle: &Ohlcv) -> GapInteraction {
        let overlaps = candle.low < self.top && candle.high > self.bottom;

        if !overlaps {
            return GapInteraction::Miss;
        }

        let is_filled = match self.direction {
            FairValueGapDirection::Bullish => candle.low <= self.bottom,
            FairValueGapDirection::Bearish => candle.high >= self.top,
        };

        if is_filled {
            GapInteraction::Fill
        } else {
            GapInteraction::Touch
        }
    }
}

impl FairValueGap<OpenState> {
    /// Evaluates the incoming indexed candle against the open gap, considering TTL.
    fn process_candle(self, indexed_candle: &IndexedOhlcv, ttl: TtlPolicy) -> FairValueGapStatus {
        let candle = &indexed_candle.candle;

        // 1. Evaluate Price Action First via the interaction helper
        let updated_gap = match self.evaluate_interaction(candle) {
            GapInteraction::Fill => {
                // Early return: If it fully fills, it closes immediately before TTL checks.
                return FairValueGapStatus::Closed(self.into_closed(candle.point_in_time()));
            }
            GapInteraction::Touch => {
                let gap_size = self.gap_size();
                let current_fill_pct = match self.direction {
                    FairValueGapDirection::Bullish => (self.top.0 - candle.low.0) / gap_size,
                    FairValueGapDirection::Bearish => (candle.high.0 - self.bottom.0) / gap_size,
                };
                self.with_partial_fill(current_fill_pct)
            }
            GapInteraction::Miss => self, // pass-through
        };

        // 2. Evaluate TTL Expiration
        let is_expired = match ttl {
            TtlPolicy::Bars(limit) => {
                indexed_candle
                    .index
                    .saturating_sub(updated_gap.creation_index())
                    >= limit
            }
            TtlPolicy::Time(limit) => {
                candle
                    .close_timestamp
                    .signed_duration_since(updated_gap.creation_time())
                    >= limit
            }
            TtlPolicy::Filled => false,
        };

        if is_expired {
            FairValueGapStatus::Expired(updated_gap.into_expired(candle.close_timestamp))
        } else {
            FairValueGapStatus::Open(updated_gap)
        }
    }

    fn with_partial_fill(self, fill_pct: f64) -> Self {
        let max_fill_percentage = self.state.max_fill_percentage.max(fill_pct.clamp(0.0, 1.0));
        self.map(|s| OpenState {
            max_fill_percentage,
            touch_count: s.touch_count + 1,
        })
    }

    fn into_closed(self, closed_time: DateTime<Utc>) -> FairValueGap<ClosedState> {
        self.map(|s| ClosedState {
            closed_time,
            touch_count: s.touch_count + 1,
        })
    }

    fn into_expired(self, expired_time: DateTime<Utc>) -> FairValueGap<ExpiredState> {
        self.map(|s| ExpiredState {
            expired_time,
            touch_count: s.touch_count,
            final_fill_percentage: s.max_fill_percentage,
        })
    }
}

#[derive(Debug, Clone)]
pub struct StreamingFairValueGap {
    min_gap_size: f64,
    ttl_policy: TtlPolicy,
    buffer: VecDeque<IndexedOhlcv>,
    active_gaps: Vec<FairValueGap<OpenState>>,
    closed_gaps: Vec<FairValueGap<ClosedState>>,
    expired_gaps: Vec<FairValueGap<ExpiredState>>,
}

impl Default for StreamingFairValueGap {
    fn default() -> Self {
        Self {
            min_gap_size: f64::EPSILON,
            ttl_policy: TtlPolicy::default(),
            buffer: VecDeque::with_capacity(PATTERN_LENGTH),
            active_gaps: Vec::new(),
            closed_gaps: Vec::new(),
            expired_gaps: Vec::new(),
        }
    }
}

impl StreamingFairValueGap {
    /// Sets the minimum gap size for the indicator.
    ///
    /// # Arguments
    /// * `min_gap_size` - The minimum gap size to set. Must be > 0.0.
    ///
    /// # Panics
    /// Panics if `min_gap_size` <= 0.0.
    pub fn with_min_gap_size(self, min_gap_size: f64) -> Self {
        assert!(
            min_gap_size > 0.0,
            "min_gap_size must be strictly positive (got {min_gap_size} which is <= 0.0)"
        );
        Self {
            min_gap_size,
            ..self
        }
    }

    pub fn with_ttl_policy(self, ttl_policy: TtlPolicy) -> Self {
        Self { ttl_policy, ..self }
    }

    // Accessors for agent state inspection...
    pub fn active_gaps(&self) -> &[FairValueGap<OpenState>] {
        &self.active_gaps
    }
    pub fn closed_gaps(&self) -> &[FairValueGap<ClosedState>] {
        &self.closed_gaps
    }
    pub fn expired_gaps(&self) -> &[FairValueGap<ExpiredState>] {
        &self.expired_gaps
    }

    fn detect_gap(&self) -> Option<FairValueGap<OpenState>> {
        if self.buffer.len() < PATTERN_LENGTH {
            return None;
        }

        let lhs = self.buffer[LHS].candle;
        let mid = self.buffer[MID].candle;
        let rhs = self.buffer[RHS].candle;
        let rhs_index = self.buffer[RHS].index;

        let gap_up = rhs.low.0 - lhs.high.0;
        let gap_down = lhs.low.0 - rhs.high.0;

        // A bullish and bearish gap can't coexist for the same candle pair.
        debug_assert!(
            !(gap_up >= self.min_gap_size && gap_down >= self.min_gap_size),
            "detected bullish and bearish gap simultaneously (gap_up={gap_up}, gap_down={gap_down})"
        );

        let (direction, top, bottom) = if gap_up >= self.min_gap_size {
            (FairValueGapDirection::Bullish, rhs.low, lhs.high)
        } else if gap_down >= self.min_gap_size {
            (FairValueGapDirection::Bearish, lhs.low, rhs.high)
        } else {
            return None;
        };

        Some(FairValueGap {
            direction,
            creation_time: rhs.close_timestamp,
            creation_index: rhs_index,
            top,
            bottom,
            window: [lhs, mid, rhs],
            state: OpenState::default(),
        })
    }
}

impl StreamingIndicator for StreamingFairValueGap {
    type Input = IndexedOhlcv;
    type Output<'a> = &'a [FairValueGap<OpenState>];

    fn update(&mut self, indexed_candle: Self::Input) -> Self::Output<'_> {
        // 1. Process active gaps against the new candle
        let ttl = self.ttl_policy;
        let closed_gaps = &mut self.closed_gaps;
        let expired_gaps = &mut self.expired_gaps;

        self.active_gaps.retain_mut(|gap_ref| {
            match gap_ref.process_candle(&indexed_candle, ttl) {
                FairValueGapStatus::Open(updated_gap) => {
                    *gap_ref = updated_gap;
                    true // Keep in active
                }
                FairValueGapStatus::Closed(closed_gap) => {
                    closed_gaps.push(closed_gap);
                    false // Remove from active
                }
                FairValueGapStatus::Expired(expired_gap) => {
                    expired_gaps.push(expired_gap);
                    false // Remove from active
                }
            }
        });

        // 2. Update buffer and detect new gaps
        if self.buffer.len() >= PATTERN_LENGTH {
            self.buffer.pop_front();
        }
        self.buffer.push_back(indexed_candle);

        if let Some(new_gap) = self.detect_gap() {
            self.active_gaps.push(new_gap);
        }

        self.active_gaps.as_slice()
    }

    fn reset(&mut self) {
        self.buffer.clear();
        self.active_gaps.clear();
        self.closed_gaps.clear();
        self.expired_gaps.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::{domain::Quantity, event::Ohlcv};

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
        assert!(high >= low, "Invalid mock candle: high {high} < low {low}");
        IndexedOhlcv {
            index,
            candle: Ohlcv {
                open_timestamp: ts(time),
                close_timestamp: ts(time),
                open: Price(open),
                high: Price(high),
                low: Price(low),
                close: Price(close),
                volume: Quantity(100.0), // Adjust if your Volume wrapper is different
                quote_asset_volume: None,
                number_of_trades: None,
                taker_buy_base_asset_volume: None,
                taker_buy_quote_asset_volume: None,
            },
        }
    }

    /// Helper to assert floats with epsilon tolerance
    fn assert_f64_eq(a: f64, b: f64) {
        assert!(
            (a - b).abs() < f64::EPSILON,
            "Expected {} to equal {}",
            a,
            b
        );
    }

    // ==========================================
    // === 2. Core Invariant Proofs ===
    // ==========================================

    #[test]
    fn simultaneous_bullish_and_bearish_gap_is_impossible() {
        // PROOF:
        // Bullish Gap requires: gap_up > 0 => C3.low > C1.high
        // Bearish Gap requires: gap_down > 0 => C1.low > C3.high
        // For a valid candle, High >= Low always.
        // If both gaps existed:
        // - gap_up: C3.low > C1.high > 0
        // - gap_down: C1.low > C3.high > 0
        //
        // As C1.low is greater than C3.high (gap_down) and C3.high >= C3.low (valid candle),
        // we get C1.low > C3.high >= C3.low > C1.high > 0, by extending the left side of the
        // inequality of gap_up.
        //
        // This transitively means C1.low > C1.high, which is a contradiction. Hence, the gap_up
        // and gap_down cannot both exist simultaneously.

        let mut fvg = StreamingFairValueGap::default().with_min_gap_size(0.1);

        // Feed an erratic sequence to ensure the math holds and the debug_assert never fires
        let trajectory = vec![
            candle(1, "2026-05-24T10:00:00Z", 50., 100., 10., 50.), // Massive range
            candle(2, "2026-05-24T10:01:00Z", 50., 50., 50., 50.),  // Inside doji
            candle(3, "2026-05-24T10:02:00Z", 10., 10., 10., 10.),  // Exact bottom touch
        ];

        for c in trajectory {
            let _ = fvg.update(c);
        }

        assert_eq!(fvg.active_gaps.len(), 0);
    }

    // ==========================================
    // === 3. Detection & Noise Filtering ===
    // ==========================================

    #[test]
    fn filters_noise_below_min_gap_size() {
        let mut indicator = StreamingFairValueGap::default().with_min_gap_size(2.0);

        // Gap size will be 11.0 - 10.0 = 1.0.
        // Since 1.0 < min_gap_size (2.0), it must be rejected as noise.
        indicator.update(candle(1, "2026-05-24T10:00:00Z", 10., 10., 5., 8.)); // C1 High = 10
        indicator.update(candle(2, "2026-05-24T10:01:00Z", 10., 12., 8., 11.)); // C2
        indicator.update(candle(3, "2026-05-24T10:02:00Z", 12., 15., 11., 14.)); // C3 Low = 11

        assert!(indicator.active_gaps.is_empty());
    }

    #[test]
    fn detects_bullish_and_bearish_fvgs() {
        let mut indicator = StreamingFairValueGap::default().with_min_gap_size(1.0);

        // === Bullish Sequence ===
        indicator.update(candle(1, "2026-05-24T10:00:00Z", 10., 10., 5., 8.)); // C1 High = 10
        indicator.update(candle(2, "2026-05-24T10:01:00Z", 10., 12., 8., 11.)); // C2
        indicator.update(candle(3, "2026-05-24T10:02:00Z", 15., 20., 15., 18.)); // C3 Low = 15

        assert_eq!(indicator.active_gaps.len(), 1);
        let gap = indicator.active_gaps[0];
        assert_eq!(gap.direction(), FairValueGapDirection::Bullish);
        assert_eq!(gap.bottom().0, 10.0);
        assert_eq!(gap.top().0, 15.0);
        assert_f64_eq(gap.gap_size(), 5.0);

        indicator.reset();

        // === Bearish Sequence ===
        indicator.update(candle(4, "2026-05-24T10:00:00Z", 20., 25., 20., 22.)); // C1 Low = 20
        indicator.update(candle(5, "2026-05-24T10:01:00Z", 18., 22., 15., 16.)); // C2
        indicator.update(candle(6, "2026-05-24T10:02:00Z", 12., 15., 10., 11.)); // C3 High = 15

        assert_eq!(indicator.active_gaps.len(), 1);
        let gap = indicator.active_gaps[0];
        assert_eq!(gap.direction(), FairValueGapDirection::Bearish);
        assert_eq!(gap.top().0, 20.0);
        assert_eq!(gap.bottom().0, 15.0);
        assert_f64_eq(gap.gap_size(), 5.0);
    }

    // ==========================================
    // === 4. State Management (Active/Hist) ===
    // ==========================================

    #[test]
    fn partial_fill_updates_active_state_and_clamps() {
        let mut indicator = StreamingFairValueGap::default().with_min_gap_size(1.0);

        // 1. Create Bullish Gap: Top=15.0, Bottom=10.0, Size=5.0
        indicator.update(candle(1, "2026-05-24T10:00:00Z", 10., 10., 5., 8.));
        indicator.update(candle(2, "2026-05-24T10:01:00Z", 10., 12., 8., 11.));
        indicator.update(candle(3, "2026-05-24T10:02:00Z", 15., 20., 15., 18.));

        // Verify Setup Assumption
        assert_eq!(
            indicator.active_gaps().len(),
            1,
            "Assumption failed: Bullish gap was not created"
        );
        let initial_gap = indicator.active_gaps()[0];
        assert_eq!(initial_gap.direction(), FairValueGapDirection::Bullish);
        assert_eq!(initial_gap.top().0, 15.0);
        assert_eq!(initial_gap.bottom().0, 10.0);
        assert_f64_eq(initial_gap.gap_size(), 5.0);

        // 2. Partial Fill: Wick down to 12.5 (50% fill)
        indicator.update(candle(4, "2026-05-24T10:03:00Z", 18., 18., 12.5, 17.));

        assert_eq!(indicator.active_gaps().len(), 1);
        assert_eq!(indicator.closed_gaps().len(), 0); // Still active

        let gap = indicator.active_gaps()[0];
        assert_eq!(gap.state().touch_count(), 1);
        assert_f64_eq(gap.state().max_fill_percentage(), 0.5); // (15 - 12.5) / 5

        // 3. Lesser Fill: Wick down to 14.0 (20% fill). Should NOT reduce max_fill.
        indicator.update(candle(5, "2026-05-24T10:04:00Z", 18., 18., 14.0, 17.));

        let gap = indicator.active_gaps()[0];
        assert_eq!(gap.state().touch_count(), 2);
        assert_f64_eq(gap.state().max_fill_percentage(), 0.5); // Retains 50% max
    }

    #[test]
    fn full_fill_migrates_gap_to_closed() {
        let mut indicator = StreamingFairValueGap::default().with_min_gap_size(1.0);

        // 1. Create Bearish Gap: Top=20.0, Bottom=15.0, Size=5.0
        indicator.update(candle(1, "2026-05-24T10:00:00Z", 20., 25., 20., 22.)); // C1 Low=20
        indicator.update(candle(2, "2026-05-24T10:01:00Z", 18., 22., 12., 16.)); // C2 Low down to 12
        indicator.update(candle(3, "2026-05-24T10:02:00Z", 12., 15., 10., 11.)); // C3 High=15

        // Verify Setup Assumption
        assert_eq!(
            indicator.active_gaps().len(),
            1,
            "Assumption failed: Bearish gap was not created"
        );
        let initial_gap = indicator.active_gaps()[0];
        assert_eq!(initial_gap.direction(), FairValueGapDirection::Bearish);
        assert_eq!(initial_gap.top().0, 20.0);
        assert_eq!(initial_gap.bottom().0, 15.0);
        assert_eq!(indicator.closed_gaps().len(), 0);

        // 2. Miss (Price drops further away from the gap)
        indicator.update(candle(4, "2026-05-24T10:03:00Z", 10., 12., 5., 8.));
        assert_eq!(indicator.active_gaps()[0].state().touch_count(), 0);

        // 3. Full Fill (Price violently rallies through Top of 20.0)
        indicator.update(candle(5, "2026-05-24T10:04:00Z", 12., 21., 12., 21.)); // High = 21 >= 20

        // Assert Migration
        assert_eq!(
            indicator.active_gaps().len(),
            0,
            "Gap should be removed from active pool"
        );
        assert_eq!(
            indicator.closed_gaps().len(),
            1,
            "Gap should be migrated to history"
        );

        let closed = indicator.closed_gaps()[0];
        assert_eq!(closed.direction(), FairValueGapDirection::Bearish);
        assert_f64_eq(closed.state().max_fill_percentage(), 1.0); // Full fill is exactly 1.0
        assert_eq!(closed.state().touch_count(), 1); // Only took 1 touch to close
        assert_eq!(closed.state().closed_time(), ts("2026-05-24T10:04:00Z")); // Time of the violating candle
    }

    #[test]
    fn boundary_exact_tick_is_a_miss() {
        let mut indicator = StreamingFairValueGap::default().with_min_gap_size(1.0);

        // Create Bullish Gap: Top=15.0, Bottom=10.0
        indicator.update(candle(1, "2026-05-26T10:01:00Z", 10., 10., 5., 8.));
        indicator.update(candle(2, "2026-05-26T10:02:00Z", 10., 12., 8., 11.));
        indicator.update(candle(3, "2026-05-26T10:03:00Z", 15., 20., 15., 18.));

        // Verify Setup Assumption
        let initial_gap = indicator.active_gaps()[0];
        assert_eq!(
            indicator.active_gaps().len(),
            1,
            "Assumption failed: Bullish gap was not created"
        );
        assert_eq!(initial_gap.direction(), FairValueGapDirection::Bullish);
        assert_eq!(initial_gap.top().0, 15.0);
        assert_eq!(initial_gap.bottom().0, 10.0);

        // Send a candle that wicks to EXACTLY 15.0
        // Because process_candle uses `candle.low < self.top`, this evaluates to false.
        // It is mathematically defined as a Miss, NOT a touch/partial fill.
        indicator.update(candle(4, "2026-05-26T10:04:00Z", 20., 20., 15.0, 20.));

        let gap = indicator.active_gaps()[0];
        assert_eq!(
            gap.state().touch_count(),
            0,
            "Exact tick overlap should not increment touches"
        );
        assert_f64_eq(gap.state().max_fill_percentage(), 0.0);
    }

    #[test]
    fn multiple_gaps_tracked_and_filled_independently() {
        let mut indicator = StreamingFairValueGap::default().with_min_gap_size(1.0);

        // 1. Create Bullish Gap A (10 -> 15)
        indicator.update(candle(1, "2026-05-26T10:01:00Z", 10., 10., 5., 8.));
        indicator.update(candle(2, "2026-05-26T10:02:00Z", 10., 20., 8., 11.)); // C2 High up to 20
        indicator.update(candle(3, "2026-05-26T10:03:00Z", 15., 22., 15., 18.)); // C3 High up to 22

        // Verify Setup Assumption A
        assert_eq!(
            indicator.active_gaps().len(),
            1,
            "Assumption failed: Gap A not created"
        );
        assert_eq!(
            indicator.active_gaps()[0].direction(),
            FairValueGapDirection::Bullish
        );
        assert_eq!(indicator.active_gaps()[0].top().0, 15.0);
        assert_eq!(indicator.active_gaps()[0].bottom().0, 10.0);

        // 2. Create Bullish Gap B (25 -> 30) further up the trend
        indicator.update(candle(4, "2026-05-26T10:04:00Z", 25., 25., 20., 22.));
        indicator.update(candle(5, "2026-05-26T10:05:00Z", 25., 28., 22., 26.));
        indicator.update(candle(6, "2026-05-26T10:06:00Z", 30., 35., 30., 32.));

        // Verify Setup Assumption B
        assert_eq!(
            indicator.active_gaps().len(),
            2,
            "Assumption failed: Gap B not created"
        );
        assert_eq!(
            indicator.active_gaps()[1].direction(),
            FairValueGapDirection::Bullish
        );
        assert_eq!(indicator.active_gaps()[1].top().0, 30.0);
        assert_eq!(indicator.active_gaps()[1].bottom().0, 25.0);

        // 3. Price drops to 20. This completely fills Gap B (25->30), but only misses Gap A (10->15)
        indicator.update(candle(7, "2026-05-26T10:07:00Z", 30., 30., 20., 25.));

        assert_eq!(indicator.active_gaps().len(), 1, "Gap B should be closed");
        assert_eq!(
            indicator.closed_gaps().len(),
            1,
            "Gap B should be in history"
        );

        // Verify Gap A is still active and untouched (passed by value since it is Copy)
        let active_gap = indicator.active_gaps()[0];
        assert_eq!(active_gap.bottom().0, 10.0);
        assert_eq!(active_gap.top().0, 15.0);
        assert_eq!(active_gap.state().touch_count(), 0);

        // Verify Gap B is closed (passed by value)
        let closed_gap = indicator.closed_gaps()[0];
        assert_eq!(closed_gap.bottom().0, 25.0);
        assert_eq!(closed_gap.top().0, 30.0);
        assert_eq!(closed_gap.state().touch_count(), 1);
        assert_f64_eq(closed_gap.state().max_fill_percentage(), 1.0);
    }

    // ==========================================
    // === 5. Time-To-Live (TTL) Expiration ===
    // ==========================================

    #[test]
    fn ttl_expires_after_n_bars() {
        // Expire if 2 or more bars have closed since creation
        let mut indicator = StreamingFairValueGap::default()
            .with_min_gap_size(1.0)
            .with_ttl_policy(TtlPolicy::Bars(2));

        // 1. Create Bullish Gap: Top=15.0, Bottom=10.0
        // C3 is the RHS candle, so creation_index = 3
        indicator.update(candle(1, "2026-05-26T10:01:00Z", 10., 10., 5., 8.));
        indicator.update(candle(2, "2026-05-26T10:02:00Z", 10., 20., 8., 11.)); // C2 High up to 20
        indicator.update(candle(3, "2026-05-26T10:03:00Z", 15., 20., 15., 18.));

        // Verify Setup Assumption
        assert_eq!(
            indicator.active_gaps().len(),
            1,
            "Assumption failed: Gap not created"
        );
        assert_eq!(
            indicator.active_gaps()[0].direction(),
            FairValueGapDirection::Bullish
        );
        assert_eq!(indicator.active_gaps()[0].creation_index(), 3);
        assert_eq!(indicator.expired_gaps().len(), 0);
        assert_eq!(indicator.closed_gaps().len(), 0, "No closed gaps at setup");

        // 2. Candle 4 (Index 4). Diff = 4 - 3 = 1 bar.
        // 1 < 2, so the gap remains active.
        indicator.update(candle(4, "2026-05-26T10:04:00Z", 20., 25., 20., 22.));

        assert_eq!(indicator.active_gaps().len(), 1);
        assert_eq!(indicator.expired_gaps().len(), 0);
        assert_eq!(
            indicator.closed_gaps().len(),
            0,
            "No closed gaps mid-flight"
        );

        // 3. Candle 5 (Index 5). Diff = 5 - 3 = 2 bars.
        // 2 >= 2, so the gap should immediately expire.
        indicator.update(candle(5, "2026-05-26T10:05:00Z", 20., 25., 20., 22.));

        assert_eq!(
            indicator.active_gaps().len(),
            0,
            "Gap should be removed from active"
        );
        assert_eq!(
            indicator.expired_gaps().len(),
            1,
            "Gap should be migrated to expired"
        );
        assert_eq!(
            indicator.closed_gaps().len(),
            0,
            "No closed gaps after expiration"
        );

        let expired = indicator.expired_gaps()[0];
        assert_eq!(expired.creation_index(), 3);
        assert_eq!(expired.state().expired_time(), ts("2026-05-26T10:05:00Z"));
    }

    #[test]
    fn ttl_expires_after_time_duration() {
        // Expire if 5 minutes have passed since creation
        let mut indicator = StreamingFairValueGap::default()
            .with_min_gap_size(1.0)
            .with_ttl_policy(TtlPolicy::Time(Duration::minutes(5)));

        // 1. Create Bullish Gap: Top=15.0, Bottom=10.0
        // C3 close_timestamp = "10:03:00Z"
        indicator.update(candle(1, "2026-05-26T10:01:00Z", 10., 10., 5., 8.));
        indicator.update(candle(2, "2026-05-26T10:02:00Z", 10., 20., 8., 11.)); // C2 High up to 20
        indicator.update(candle(3, "2026-05-26T10:03:00Z", 15., 20., 15., 18.));

        // Verify Setup Assumption
        assert_eq!(
            indicator.active_gaps().len(),
            1,
            "Assumption failed: Gap not created"
        );
        assert_eq!(
            indicator.active_gaps()[0].direction(),
            FairValueGapDirection::Bullish
        );
        assert_eq!(
            indicator.active_gaps()[0].creation_time(),
            ts("2026-05-26T10:03:00Z")
        );
        assert_eq!(indicator.closed_gaps().len(), 0, "No closed gaps at setup");

        // 2. Candle at 10:07:00Z. Diff = 4 mins.
        // 4 mins < 5 mins, so it remains active.
        indicator.update(candle(4, "2026-05-26T10:07:00Z", 20., 25., 20., 22.));
        assert_eq!(indicator.active_gaps().len(), 1);
        assert_eq!(
            indicator.closed_gaps().len(),
            0,
            "No closed gaps mid-flight"
        );

        // 3. Candle at 10:08:00Z. Diff = 5 mins.
        // 5 mins >= 5 mins, gap expires.
        indicator.update(candle(5, "2026-05-26T10:08:00Z", 20., 25., 20., 22.));
        assert_eq!(indicator.active_gaps().len(), 0);
        assert_eq!(indicator.expired_gaps().len(), 1);
        assert_eq!(
            indicator.closed_gaps().len(),
            0,
            "No closed gaps after expiration"
        );
    }

    #[test]
    fn expired_state_preserves_partial_fill_history() {
        let mut indicator = StreamingFairValueGap::default()
            .with_min_gap_size(1.0)
            .with_ttl_policy(TtlPolicy::Bars(2));

        // 1. Create Bullish Gap: Top=15.0, Bottom=10.0, Size=5.0
        indicator.update(candle(1, "2026-05-26T10:01:00Z", 10., 10., 5., 8.));
        indicator.update(candle(2, "2026-05-26T10:02:00Z", 10., 12., 8., 11.));
        indicator.update(candle(3, "2026-05-26T10:03:00Z", 15., 20., 15., 18.));

        // Verify Setup Assumption
        assert_eq!(
            indicator.active_gaps().len(),
            1,
            "Assumption failed: Gap not created"
        );
        assert_eq!(
            indicator.active_gaps()[0].direction(),
            FairValueGapDirection::Bullish
        );
        assert_eq!(indicator.closed_gaps().len(), 0, "No closed gaps at setup");
        assert_eq!(
            indicator.expired_gaps().len(),
            0,
            "No expired gaps at setup"
        );

        // 2. Partial Fill: Wick down to 12.5 (50% fill) on the very next bar
        // This is 1 bar after creation, so it does NOT expire yet.
        indicator.update(candle(4, "2026-05-26T10:04:00Z", 18., 18., 12.5, 17.));

        // Verify state prior to expiration
        assert_eq!(indicator.active_gaps().len(), 1);
        assert_eq!(indicator.active_gaps()[0].state().touch_count(), 1);

        // 3. Expiration: Next bar runs away but triggers the 2-bar expiration limit.
        indicator.update(candle(5, "2026-05-26T10:05:00Z", 20., 25., 20., 22.));

        assert_eq!(indicator.active_gaps().len(), 0);
        assert_eq!(indicator.closed_gaps().len(), 0);
        assert_eq!(indicator.expired_gaps().len(), 1);

        // Verify that the ExpiredState successfully inherited the fill data from OpenState
        let expired = indicator.expired_gaps()[0];
        assert_eq!(
            expired.state().touch_count(),
            1,
            "Should preserve the touch count before expiration"
        );
        assert_f64_eq(expired.state().final_fill_percentage(), 0.5);
    }

    #[test]
    fn ttl_policy_filled_never_expires() {
        // TtlPolicy::Filled ist der Standard. Die Lücke darf niemals von allein verfallen.
        let mut indicator = StreamingFairValueGap::default()
            .with_min_gap_size(1.0)
            .with_ttl_policy(TtlPolicy::Filled);

        // 1. Bullish Gap erstellen: Top=15.0, Bottom=10.0 (Creation Index = 3)
        indicator.update(candle(1, "2026-05-26T10:01:00Z", 10., 10., 5., 8.));
        indicator.update(candle(2, "2026-05-26T10:02:00Z", 10., 20., 8., 11.)); // C2 High up to 20
        indicator.update(candle(3, "2026-05-26T10:03:00Z", 15., 20., 15., 18.));

        // Verify Setup Assumption
        assert_eq!(
            indicator.active_gaps().len(),
            1,
            "Assumption failed: Gap not created"
        );
        assert_eq!(
            indicator.active_gaps()[0].direction(),
            FairValueGapDirection::Bullish
        );
        assert_eq!(indicator.active_gaps()[0].top().0, 15.0);

        // 2. Einen gewaltigen Sprung in die Zukunft simulieren (Index 1000, 10 Stunden später)
        // Der Preis bleibt weit über der Lücke, sodass sie nicht gefüllt wird.
        indicator.update(candle(1000, "2026-05-26T20:00:00Z", 20., 25., 20., 22.));

        assert_eq!(
            indicator.active_gaps().len(),
            1,
            "Gap with TtlPolicy::Filled must remain active indefinitely"
        );
        assert_eq!(indicator.expired_gaps().len(), 0);
    }

    // ==========================================
    // === 6. Edge Cases & Invariants ===
    // ==========================================

    #[test]
    fn simultaneous_full_fill_and_expiration_results_in_closed_gap() {
        // If a gap completely fills on the exact same candle that triggers its expiration,
        // the fill wins. The price action happened *during* the candle.
        let mut indicator = StreamingFairValueGap::default()
            .with_min_gap_size(1.0)
            .with_ttl_policy(TtlPolicy::Bars(2));

        // 1. Create Bullish Gap: Top=15.0, Bottom=10.0 (Creation Index = 3)
        indicator.update(candle(1, "2026-05-26T10:01:00Z", 10., 10., 5., 8.));
        indicator.update(candle(2, "2026-05-26T10:02:00Z", 10., 20., 8., 11.)); // C2 High up to 20
        indicator.update(candle(3, "2026-05-26T10:03:00Z", 15., 20., 15., 18.));

        // Verify Setup Assumption
        assert_eq!(
            indicator.active_gaps().len(),
            1,
            "Assumption failed: Gap not created"
        );
        assert_eq!(
            indicator.active_gaps()[0].direction(),
            FairValueGapDirection::Bullish
        );
        assert_eq!(indicator.active_gaps()[0].top().0, 15.0);
        assert_eq!(indicator.active_gaps()[0].bottom().0, 10.0);
        assert_eq!(
            indicator.closed_gaps().len(),
            0,
            "Assumption failed: closed_gaps should be empty"
        );
        assert_eq!(
            indicator.expired_gaps().len(),
            0,
            "Assumption failed: expired_gaps should be empty"
        );

        // 2. The very next candle misses the gap completely.
        indicator.update(candle(4, "2026-05-26T10:04:00Z", 20., 25., 20., 22.));

        // 3. The expiry candle! Index 5 triggers the 2-bar expiration.
        // AT THE EXACT SAME TIME, it has a violent wick down to 5.0, fully covering the gap.
        indicator.update(candle(5, "2026-05-26T10:05:00Z", 20., 20., 5.0, 10.));

        // Verify the invariants
        assert_eq!(indicator.active_gaps().len(), 0);
        assert_eq!(
            indicator.expired_gaps().len(),
            0,
            "Gap must NOT be expired. It was fully filled during the candle lifespan."
        );
        assert_eq!(
            indicator.closed_gaps().len(),
            1,
            "Gap MUST be closed because the fill happened before the candle closed."
        );

        let closed = indicator.closed_gaps()[0];
        assert_f64_eq(closed.state().max_fill_percentage(), 1.0);
    }

    #[test]
    fn simultaneous_partial_fill_and_expiration_preserves_final_action() {
        // If a gap partially fills on the exact same candle that triggers its expiration,
        // it must expire, BUT it must successfully capture the partial fill from its final moments.
        let mut indicator = StreamingFairValueGap::default()
            .with_min_gap_size(1.0)
            .with_ttl_policy(TtlPolicy::Bars(2));

        // 1. Create Bullish Gap: Top=15.0, Bottom=10.0 (Creation Index = 3)
        indicator.update(candle(1, "2026-05-26T10:01:00Z", 10., 10., 5., 8.));
        indicator.update(candle(2, "2026-05-26T10:02:00Z", 10., 20., 8., 11.)); // Raise C2 High to 20 to close C2-C4 distance
        indicator.update(candle(3, "2026-05-26T10:03:00Z", 15., 20., 15., 18.));

        // Verify Setup Assumption
        assert_eq!(
            indicator.active_gaps().len(),
            1,
            "Assumption failed: Gap not created"
        );
        assert_eq!(
            indicator.active_gaps()[0].direction(),
            FairValueGapDirection::Bullish
        );
        assert_eq!(indicator.active_gaps()[0].top().0, 15.0);
        assert_eq!(indicator.active_gaps()[0].bottom().0, 10.0);
        assert_eq!(
            indicator.closed_gaps().len(),
            0,
            "Assumption failed: closed_gaps should be empty"
        );
        assert_eq!(
            indicator.expired_gaps().len(),
            0,
            "Assumption failed: expired_gaps should be empty"
        );

        // 2. The very next candle misses the gap completely.
        indicator.update(candle(4, "2026-05-26T10:04:00Z", 20., 25., 20., 22.));

        // 3. The expiry candle! Index 5 triggers the 2-bar expiration.
        // It drops to 12.5, filling exactly 50% of the gap right before time runs out.
        indicator.update(candle(5, "2026-05-26T10:05:00Z", 20., 20., 12.5, 18.));

        // Verify the invariants
        assert_eq!(indicator.active_gaps().len(), 0);
        assert_eq!(indicator.closed_gaps().len(), 0);
        assert_eq!(indicator.expired_gaps().len(), 1);

        let expired = indicator.expired_gaps()[0];
        assert_eq!(
            expired.state().touch_count(),
            1,
            "Must register the touch from the expiring candle"
        );
        assert_f64_eq(expired.state().final_fill_percentage(), 0.5); // Correctly captured the 50% fill right before death
    }

    #[test]
    fn ttl_policy_filled_migrates_to_closed_on_full_fill() {
        // Rule: A gap with TtlPolicy::Filled can NEVER expire.
        // When it eventually fills, it must explicitly migrate to Closed.
        let mut indicator = StreamingFairValueGap::default()
            .with_min_gap_size(1.0)
            .with_ttl_policy(TtlPolicy::Filled);

        // 1. Create Bullish Gap: Top=15.0, Bottom=10.0
        indicator.update(candle(1, "2026-05-26T10:01:00Z", 10., 10., 5., 8.));
        indicator.update(candle(2, "2026-05-26T10:02:00Z", 10., 20., 8., 11.)); // C2 High up to 20
        indicator.update(candle(3, "2026-05-26T10:03:00Z", 15., 20., 15., 18.));

        // Verify Setup Assumption
        assert_eq!(
            indicator.active_gaps().len(),
            1,
            "Assumption failed: Gap not created"
        );
        assert_eq!(
            indicator.active_gaps()[0].direction(),
            FairValueGapDirection::Bullish
        );
        assert_eq!(indicator.active_gaps()[0].top().0, 15.0);
        assert_eq!(indicator.active_gaps()[0].bottom().0, 10.0);
        assert_eq!(
            indicator.closed_gaps().len(),
            0,
            "Assumption failed: closed_gaps should be empty"
        );
        assert_eq!(
            indicator.expired_gaps().len(),
            0,
            "Assumption failed: expired_gaps should be empty"
        );

        // 2. Advance far into the future (Index 1000) - Gap remains open
        indicator.update(candle(1000, "2026-05-26T20:00:00Z", 20., 25., 20., 22.));
        assert_eq!(indicator.active_gaps().len(), 1);
        assert_eq!(indicator.closed_gaps().len(), 0);
        assert_eq!(indicator.expired_gaps().len(), 0);

        // 3. Price finally crashes down and fills the gap
        indicator.update(candle(1001, "2026-05-26T20:01:00Z", 20., 20., 8.0, 10.));

        assert_eq!(
            indicator.active_gaps().len(),
            0,
            "Gap should be removed from active pool"
        );
        assert_eq!(
            indicator.expired_gaps().len(),
            0,
            "Gap with TtlPolicy::Filled must NEVER enter Expired state"
        );
        assert_eq!(
            indicator.closed_gaps().len(),
            1,
            "Gap MUST be correctly migrated to Closed state upon fill"
        );

        let closed = indicator.closed_gaps()[0];
        assert_f64_eq(closed.state().max_fill_percentage(), 1.0);
    }

    #[test]
    fn breakaway_gaps_do_not_touch_or_fill_fvg() {
        // This tests the non-continuous pricing invariant.
        // If the market completely teleports over the FVG zone without trading inside it,
        // the gap must remain open and untouched.
        let mut indicator = StreamingFairValueGap::default().with_min_gap_size(1.0);

        // ==========================================
        // SCENARIO A: Bullish FVG Bypassed
        // ==========================================

        // 1. Create Bullish Gap: Top=15.0, Bottom=10.0
        indicator.update(candle(1, "2026-05-26T10:01:00Z", 10., 10., 5., 8.));
        indicator.update(candle(2, "2026-05-26T10:02:00Z", 10., 12., 8., 11.));
        indicator.update(candle(3, "2026-05-26T10:03:00Z", 15., 20., 15., 18.));

        assert_eq!(indicator.active_gaps().len(), 1, "Bullish gap created");

        // 2. A massive gap DOWN completely below the FVG (High=8.0, Low=5.0)
        indicator.update(candle(4, "2026-05-26T10:04:00Z", 8., 8., 5., 6.));

        assert_eq!(
            indicator.active_gaps().len(),
            1,
            "Bullish gap must remain active because it was leaped over"
        );
        let bullish_gap = indicator.active_gaps()[0];
        assert_eq!(
            bullish_gap.state().touch_count(),
            0,
            "The market never traded inside the Bullish gap"
        );
        assert_f64_eq(bullish_gap.state().max_fill_percentage(), 0.0);

        indicator.reset();

        // ==========================================
        // SCENARIO B: Bearish FVG Bypassed
        // ==========================================

        // 1. Create Bearish Gap: Top=20.0, Bottom=15.0
        indicator.update(candle(1, "2026-05-26T10:01:00Z", 20., 25., 20., 22.));
        indicator.update(candle(2, "2026-05-26T10:02:00Z", 18., 25., 15., 16.)); // C2 High up to 25
        indicator.update(candle(3, "2026-05-26T10:03:00Z", 12., 15., 10., 11.));

        assert_eq!(indicator.active_gaps().len(), 1, "Bearish gap created");

        // 2. A massive gap UP completely above the FVG (High=30.0, Low=25.0)
        indicator.update(candle(4, "2026-05-26T10:04:00Z", 25., 30., 25., 28.));

        assert_eq!(
            indicator.active_gaps().len(),
            1,
            "Bearish gap must remain active because it was leaped over"
        );
        let bearish_gap = indicator.active_gaps()[0];
        assert_eq!(
            bearish_gap.state().touch_count(),
            0,
            "The market never traded inside the Bearish gap"
        );
        assert_f64_eq(bearish_gap.state().max_fill_percentage(), 0.0);
    }

    #[test]
    fn gap_interaction_evaluates_overlap_and_fills_correctly() {
        // === 1. Bullish Gap Setup (Top=15.0, Bottom=10.0) ===
        // Window indices must be the contiguous triple ending at creation_index.
        // creation_index = 2 => window = [0, 1, 2]. The candles form a real bullish
        // gap: rhs.low (15) > lhs.high (10), with bottom = lhs.high, top = rhs.low.
        let bullish_gap = FairValueGap {
            direction: FairValueGapDirection::Bullish,
            creation_time: ts("2026-05-24T10:02:00Z"),
            creation_index: 2,
            top: Price(15.0),
            bottom: Price(10.0),
            window: [
                candle(0, "2026-05-24T10:00:00Z", 8., 10., 5., 9.).candle, // lhs: high = 10 (gap bottom)
                candle(1, "2026-05-24T10:01:00Z", 9., 14., 9., 13.).candle, // mid: displacement impulse
                candle(2, "2026-05-24T10:02:00Z", 15., 18., 15., 16.).candle, // rhs: low = 15 (gap top)
            ],
            state: OpenState::default(),
        };

        // A. Bullish Miss (Price stays entirely above the gap)
        let miss_above = candle(3, "2026-05-24T10:03:00Z", 20., 25., 15.0, 22.).candle;
        let interaction = bullish_gap.evaluate_interaction(&miss_above);
        assert_eq!(interaction, GapInteraction::Miss);
        assert!(!interaction.is_touch());

        // B. Bullish Breakaway Miss (Price teleports completely below the gap)
        let breakaway_below = candle(4, "2026-05-24T10:04:00Z", 5., 8., 2., 6.).candle;
        let interaction = bullish_gap.evaluate_interaction(&breakaway_below);
        assert_eq!(interaction, GapInteraction::Miss);

        // C. Bullish Touch (Wick enters the gap: low is 12.0)
        let touch_candle = candle(5, "2026-05-24T10:05:00Z", 18., 18., 12., 15.).candle;
        let interaction = bullish_gap.evaluate_interaction(&touch_candle);
        assert_eq!(interaction, GapInteraction::Touch);
        assert!(interaction.is_touch());
        assert!(!interaction.is_fill());

        // D. Bullish Fill (Wick drops below the bottom of 10.0)
        let fill_candle = candle(6, "2026-05-24T10:06:00Z", 18., 18., 9., 15.).candle;
        let interaction = bullish_gap.evaluate_interaction(&fill_candle);
        assert_eq!(interaction, GapInteraction::Fill);
        assert!(interaction.is_touch()); // A fill MUST register as a touch
        assert!(interaction.is_fill());

        // === 2. Bearish Gap Setup (Top=20.0, Bottom=15.0) ===
        // creation_index = 2 => window = [0, 1, 2]. Real bearish gap:
        // lhs.low (20) > rhs.high (15), with top = lhs.low, bottom = rhs.high.
        let bearish_gap = FairValueGap {
            direction: FairValueGapDirection::Bearish,
            creation_time: ts("2026-05-24T10:02:00Z"),
            creation_index: 2,
            top: Price(20.0),
            bottom: Price(15.0),
            window: [
                candle(0, "2026-05-24T10:00:00Z", 22., 25., 20., 21.).candle, // lhs: low = 20 (gap top)
                candle(1, "2026-05-24T10:01:00Z", 21., 22., 16., 17.).candle, // mid: displacement impulse
                candle(2, "2026-05-24T10:02:00Z", 14., 15., 10., 11.).candle, // rhs: high = 15 (gap bottom)
            ],
            state: OpenState::default(),
        };

        // A. Bearish Miss (Price stays entirely below the gap)
        let miss_below = candle(3, "2026-05-24T10:03:00Z", 10., 15.0, 5., 12.).candle;
        assert_eq!(
            bearish_gap.evaluate_interaction(&miss_below),
            GapInteraction::Miss
        );

        // B. Bearish Breakaway Miss (Price teleports completely above the gap)
        let breakaway_above = candle(4, "2026-05-24T10:04:00Z", 25., 30., 22., 28.).candle;
        assert_eq!(
            bearish_gap.evaluate_interaction(&breakaway_above),
            GapInteraction::Miss
        );

        // C. Bearish Touch (Wick enters the gap: high is 18.0)
        let touch_bear = candle(5, "2026-05-24T10:05:00Z", 10., 18., 10., 12.).candle;
        assert_eq!(
            bearish_gap.evaluate_interaction(&touch_bear),
            GapInteraction::Touch
        );

        // D. Bearish Fill (Wick spikes above the top of 20.0)
        let fill_bear = candle(6, "2026-05-24T10:06:00Z", 10., 21., 10., 12.).candle;
        let interaction = bearish_gap.evaluate_interaction(&fill_bear);
        assert_eq!(interaction, GapInteraction::Fill);
        assert!(interaction.is_touch());
        assert!(interaction.is_fill());
    }

    // ==========================================
    // === 7. Breakout Window (Movement Candles) ===
    // ==========================================

    #[test]
    fn window_captures_three_candles_in_chronological_order() {
        let mut indicator = StreamingFairValueGap::default().with_min_gap_size(1.0);

        indicator.update(candle(1, "2026-05-24T10:00:00Z", 10., 10., 5., 8.)); // first (lhs)
        indicator.update(candle(2, "2026-05-24T10:01:00Z", 10., 18., 9., 17.)); // displacement (mid)
        indicator.update(candle(3, "2026-05-24T10:02:00Z", 15., 20., 15., 19.)); // last (rhs)

        assert_eq!(indicator.active_gaps().len(), 1);
        let gap = indicator.active_gaps()[0];

        assert_eq!(gap.first().close_timestamp, ts("2026-05-24T10:00:00Z"));
        assert_eq!(
            gap.displacement().close_timestamp,
            ts("2026-05-24T10:01:00Z")
        );
        assert_eq!(gap.last().close_timestamp, ts("2026-05-24T10:02:00Z"));

        // window() exposes the same three candles by index.
        assert_eq!(gap.window()[0].close_timestamp, gap.first().close_timestamp);
        assert_eq!(
            gap.window()[1].close_timestamp,
            gap.displacement().close_timestamp
        );
        assert_eq!(gap.window()[2].close_timestamp, gap.last().close_timestamp);

        // The last candle is the gap's creation candle.
        assert_eq!(gap.last().close_timestamp, gap.creation_time());
    }

    #[test]
    fn window_displacement_differs_from_movement_bullish() {
        let mut indicator = StreamingFairValueGap::default().with_min_gap_size(1.0);

        indicator.update(candle(1, "2026-05-24T10:00:00Z", 10., 10., 5., 8.));
        indicator.update(candle(2, "2026-05-24T10:01:00Z", 10., 18., 9., 17.)); // mid: high 18
        indicator.update(candle(3, "2026-05-24T10:02:00Z", 15., 20., 15., 19.)); // rhs pushes high to 20

        let gap = indicator.active_gaps()[0];
        assert_eq!(gap.direction(), FairValueGapDirection::Bullish);

        // Displacement reading == the middle candle only.
        assert_f64_eq(gap.displacement().high.0, 18.0);
        assert_f64_eq(gap.displacement().low.0, 9.0);

        // Whole-movement reading is genuinely different.
        let movement_high = gap
            .window()
            .iter()
            .map(|c| c.high.0)
            .fold(f64::MIN, f64::max);
        assert_f64_eq(movement_high, 20.0);
        assert!(movement_high > gap.displacement().high.0);
    }

    #[test]
    fn window_displacement_differs_from_movement_bearish() {
        let mut indicator = StreamingFairValueGap::default().with_min_gap_size(1.0);

        indicator.update(candle(1, "2026-05-24T10:00:00Z", 22., 25., 20., 21.)); // lhs low 20 (gap top)
        indicator.update(candle(2, "2026-05-24T10:01:00Z", 21., 22., 12., 13.)); // mid impulse low 12
        indicator.update(candle(3, "2026-05-24T10:02:00Z", 14., 15., 10., 11.)); // rhs high 15 (gap bottom), low 10

        let gap = indicator.active_gaps()[0];
        assert_eq!(gap.direction(), FairValueGapDirection::Bearish);

        assert_f64_eq(gap.displacement().low.0, 12.0);

        let movement_low = gap
            .window()
            .iter()
            .map(|c| c.low.0)
            .fold(f64::MAX, f64::min);
        assert_f64_eq(movement_low, 10.0);
        assert!(movement_low < gap.displacement().low.0);
    }

    #[test]
    fn window_survives_state_transitions() {
        // The window is fixed at creation and must be carried verbatim through
        // partial fills (active) and full fills (closed) via FairValueGap::map.
        let mut indicator = StreamingFairValueGap::default().with_min_gap_size(1.0);

        indicator.update(candle(1, "2026-05-24T10:00:00Z", 10., 10., 5., 8.));
        indicator.update(candle(2, "2026-05-24T10:01:00Z", 10., 18., 9., 17.));
        indicator.update(candle(3, "2026-05-24T10:02:00Z", 15., 20., 15., 19.));

        let disp_high = indicator.active_gaps()[0].displacement().high.0;
        let disp_ts = indicator.active_gaps()[0].displacement().close_timestamp;

        // Partial fill (wick to 12.5) -> still active, window intact.
        indicator.update(candle(4, "2026-05-24T10:03:00Z", 18., 18., 12.5, 17.));
        assert_eq!(indicator.active_gaps().len(), 1);
        assert_f64_eq(indicator.active_gaps()[0].displacement_high().0, disp_high);
        assert_eq!(
            indicator.active_gaps()[0].displacement().close_timestamp,
            disp_ts
        );

        // Full fill (wick below bottom 10.0) -> migrates to closed; map() must carry the window.
        indicator.update(candle(5, "2026-05-24T10:04:00Z", 18., 18., 9., 14.));
        assert_eq!(indicator.closed_gaps().len(), 1);
        assert_f64_eq(indicator.closed_gaps()[0].displacement_high().0, disp_high);
        assert_eq!(
            indicator.closed_gaps()[0].displacement().close_timestamp,
            disp_ts
        );
    }

    #[test]
    fn window_displacement_equals_movement_when_mid_is_extreme() {
        // Boundary case complementary to the two "differs" tests. When the
        // displacement candle is itself the most extreme bar of the window, the
        // displacement reading and the whole movement reading must coincide
        // exactly. This guards against movement_high/movement_low drifting away
        // from displacement under a future refactor, and confirms the max/min
        // fold returns the displacement bar's own value rather than perturbing it.
        let mut indicator = StreamingFairValueGap::default().with_min_gap_size(1.0);

        // mid carries both the window high (25) and the window low (9). lhs and
        // rhs stay strictly inside that range, so neither can win the max or min.
        indicator.update(candle(1, "2026-05-24T10:00:00Z", 10., 10., 9.5, 9.8)); // lhs: high 10 (gap bottom), low 9.5
        indicator.update(candle(2, "2026-05-24T10:01:00Z", 10., 25., 9., 24.)); // mid: high 25, low 9 (window extremes)
        indicator.update(candle(3, "2026-05-24T10:02:00Z", 15., 20., 15., 19.)); // rhs: low 15 (gap top), within mid range

        let gap = indicator.active_gaps()[0];
        assert_eq!(gap.direction(), FairValueGapDirection::Bullish);

        // High: displacement is the window maximum, so both readings agree.
        assert_f64_eq(gap.displacement_high().0, 25.0);
        assert_f64_eq(gap.movement_high().0, 25.0);
        assert_f64_eq(gap.displacement_high().0, gap.movement_high().0);

        // Low: displacement is also the window minimum, so both readings agree.
        assert_f64_eq(gap.displacement_low().0, 9.0);
        assert_f64_eq(gap.movement_low().0, 9.0);
        assert_f64_eq(gap.displacement_low().0, gap.movement_low().0);
    }

    #[test]
    fn window_movement_open_and_close_span_the_leg() {
        // movement_open is the open of the first candle and movement_close is the
        // close of the last candle, together bounding the leg from its start to
        // its end. This is the only test that reads these two accessors, so it
        // also guards their wiring to first()/last() through state transitions.
        let mut indicator = StreamingFairValueGap::default().with_min_gap_size(1.0);

        indicator.update(candle(1, "2026-05-24T10:00:00Z", 7., 10., 5., 8.)); // lhs: open 7
        indicator.update(candle(2, "2026-05-24T10:01:00Z", 10., 18., 9., 17.)); // mid
        indicator.update(candle(3, "2026-05-24T10:02:00Z", 15., 20., 15., 19.)); // rhs: close 19

        let gap = indicator.active_gaps()[0];
        assert_f64_eq(gap.movement_open().0, 7.0); // lhs.open
        assert_f64_eq(gap.movement_close().0, 19.0); // rhs.close

        // They must equal the open/close of the named boundary candles.
        assert_f64_eq(gap.movement_open().0, gap.first().open.0);
        assert_f64_eq(gap.movement_close().0, gap.last().close.0);

        // And they must survive migration to closed, like the rest of the window.
        let open_before = gap.movement_open().0;
        let close_before = gap.movement_close().0;

        indicator.update(candle(4, "2026-05-24T10:03:00Z", 18., 18., 9., 14.)); // full fill, wick below bottom 10.0
        assert_eq!(indicator.closed_gaps().len(), 1);
        assert_f64_eq(indicator.closed_gaps()[0].movement_open().0, open_before);
        assert_f64_eq(indicator.closed_gaps()[0].movement_close().0, close_before);
    }
}
