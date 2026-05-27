use std::{collections::VecDeque, fmt::Debug};

use chrono::{DateTime, Duration, Utc};

use crate::{
    data::{
        domain::Price,
        event::{IndexedOhlcv, MarketEvent},
    },
    math::StreamingIndicator,
};

const LHS: usize = 0;
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

#[derive(Debug, Clone, Copy)]
pub struct FairValueGap<S: FairValueGapState> {
    direction: FairValueGapDirection,
    creation_time: DateTime<Utc>,
    creation_index: usize,
    top: Price,
    bottom: Price,
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
            state: f(self.state),
        }
    }
}

impl FairValueGap<OpenState> {
    /// Evaluates the incoming indexed candle against the open gap, considering TTL.
    fn process_candle(self, indexed_candle: &IndexedOhlcv, ttl: TtlPolicy) -> FairValueGapStatus {
        let candle = &indexed_candle.candle;

        // 1. Evaluate TTL Expiration First
        let is_expired = match ttl {
            TtlPolicy::Bars(limit) => {
                indexed_candle.index.saturating_sub(self.creation_index) >= limit
            }
            TtlPolicy::Time(limit) => {
                candle
                    .close_timestamp
                    .signed_duration_since(self.creation_time)
                    >= limit
            }
            TtlPolicy::Filled => false,
        };

        if is_expired {
            return FairValueGapStatus::Expired(self.into_expired(candle.close_timestamp));
        }

        // 2. Evaluate Gap Interactions
        let gap_size = self.gap_size();
        let (is_touch, is_filled) = match self.direction {
            FairValueGapDirection::Bullish => {
                let touch = candle.low < self.top;
                let filled = candle.low <= self.bottom;
                (touch, filled)
            }
            FairValueGapDirection::Bearish => {
                let touch = candle.high > self.bottom;
                let filled = candle.high >= self.top;
                (touch, filled)
            }
        };

        if !is_touch {
            return FairValueGapStatus::Open(self);
        }

        if is_filled {
            return FairValueGapStatus::Closed(self.into_closed(candle.point_in_time()));
        }

        let current_fill_pct = match self.direction {
            FairValueGapDirection::Bullish => (self.top.0 - candle.low.0) / gap_size,
            FairValueGapDirection::Bearish => (candle.high.0 - self.bottom.0) / gap_size,
        };

        FairValueGapStatus::Open(self.with_partial_fill(current_fill_pct))
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
    pub fn historical_gaps(&self) -> &[FairValueGap<ClosedState>] {
        &self.closed_gaps
    }
    pub fn expired_gaps(&self) -> &[FairValueGap<ExpiredState>] {
        &self.expired_gaps
    }

    fn detect_gap(&self) -> Option<FairValueGap<OpenState>> {
        if self.buffer.len() < PATTERN_LENGTH {
            return None;
        }

        let lhs = &self.buffer[LHS].candle;
        let rhs = &self.buffer[RHS].candle;
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
            match gap_ref.clone().process_candle(&indexed_candle, ttl) {
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
    use std::f64::EPSILON;
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
        assert!((a - b).abs() < EPSILON, "Expected {} to equal {}", a, b);
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
        let gap = &indicator.active_gaps[0];
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
        let gap = &indicator.active_gaps[0];
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

        // 2. Partial Fill: Wick down to 12.5 (50% fill)
        indicator.update(candle(4, "2026-05-24T10:03:00Z", 18., 18., 12.5, 17.));

        assert_eq!(indicator.active_gaps.len(), 1);
        assert_eq!(indicator.closed_gaps.len(), 0); // Still active

        let gap = &indicator.active_gaps[0];
        assert_eq!(gap.state().touch_count(), 1);
        assert_f64_eq(gap.state().max_fill_percentage(), 0.5); // (15 - 12.5) / 5

        // 3. Lesser Fill: Wick down to 14.0 (20% fill). Should NOT reduce max_fill.
        indicator.update(candle(5, "2026-05-24T10:04:00Z", 18., 18., 14.0, 17.));

        let gap = &indicator.active_gaps[0];
        assert_eq!(gap.state().touch_count(), 2);
        assert_f64_eq(gap.state().max_fill_percentage(), 0.5); // Retains 50% max
    }

    #[test]
    fn full_fill_migrates_gap_to_historical() {
        let mut indicator = StreamingFairValueGap::default().with_min_gap_size(1.0);

        // 1. Create Bearish Gap: Top=20.0, Bottom=15.0, Size=5.0
        indicator.update(candle(1, "2026-05-24T10:00:00Z", 20., 25., 20., 22.)); // C1 Low=20
        indicator.update(candle(2, "2026-05-24T10:01:00Z", 18., 22., 15., 16.)); // C2
        indicator.update(candle(3, "2026-05-24T10:02:00Z", 12., 15., 10., 11.)); // C3 High=15

        assert_eq!(indicator.active_gaps.len(), 1);
        assert_eq!(indicator.closed_gaps.len(), 0);

        // 2. Miss (Price drops further away from the gap)
        indicator.update(candle(4, "2026-05-24T10:03:00Z", 10., 12., 5., 8.));
        assert_eq!(indicator.active_gaps[0].state().touch_count(), 0);

        // 3. Full Fill (Price violently rallies through Top of 20.0)
        indicator.update(candle(5, "2026-05-24T10:04:00Z", 12., 21., 12., 21.)); // High = 21 >= 20

        // Assert Migration
        assert_eq!(
            indicator.active_gaps.len(),
            0,
            "Gap should be removed from active pool"
        );
        assert_eq!(
            indicator.closed_gaps.len(),
            1,
            "Gap should be migrated to history"
        );

        let closed = &indicator.closed_gaps[0];
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

        // Send a candle that wicks to EXACTLY 15.0
        // Because process_candle uses `candle.low < self.top`, this evaluates to false.
        // It is mathematically defined as a Miss, NOT a touch/partial fill.
        indicator.update(candle(4, "2026-05-26T10:04:00Z", 20., 20., 15.0, 20.));

        let gap = &indicator.active_gaps()[0];
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
        indicator.update(candle(2, "2026-05-26T10:02:00Z", 10., 12., 8., 11.));
        indicator.update(candle(3, "2026-05-26T10:03:00Z", 15., 20., 15., 18.));

        // 2. Create Bullish Gap B (25 -> 30) further up the trend
        indicator.update(candle(4, "2026-05-26T10:04:00Z", 25., 25., 20., 22.));
        indicator.update(candle(5, "2026-05-26T10:05:00Z", 25., 28., 22., 26.));
        indicator.update(candle(6, "2026-05-26T10:06:00Z", 30., 35., 30., 32.));

        assert_eq!(indicator.active_gaps().len(), 2);

        // 3. Price drops to 20. This completely fills Gap B (25->30), but only misses Gap A (10->15)
        indicator.update(candle(7, "2026-05-26T10:07:00Z", 30., 30., 20., 25.));

        assert_eq!(indicator.active_gaps().len(), 1, "Gap B should be closed");
        assert_eq!(
            indicator.historical_gaps().len(),
            1,
            "Gap B should be in history"
        );

        // Verify Gap A is still active and untouched
        let active_gap = &indicator.active_gaps()[0];
        assert_eq!(active_gap.bottom().0, 10.0);
        assert_eq!(active_gap.state().touch_count(), 0);

        // Verify Gap B is closed
        let closed_gap = &indicator.historical_gaps()[0];
        assert_eq!(closed_gap.bottom().0, 25.0);
        assert_eq!(closed_gap.state().touch_count(), 1);
        assert_f64_eq(closed_gap.state().max_fill_percentage(), 1.0);
    }

    // ==========================================
    // === 5. Time-To-Live (TTL) Expiration ===
    // ==========================================

    #[test]
    fn ttl_expires_after_n_bars() {
        // Expire if 2 or more bars have closed since creation
        let mut indicator = StreamingFairValueGap::default().with_min_gap_size(1.0).with_ttl_policy(TtlPolicy::Bars(2));

        // 1. Create Bullish Gap: Top=15.0, Bottom=10.0
        // C3 is the RHS candle, so creation_index = 3
        indicator.update(candle(1, "2026-05-26T10:01:00Z", 10., 10., 5., 8.));
        indicator.update(candle(2, "2026-05-26T10:02:00Z", 10., 12., 8., 11.));
        indicator.update(candle(3, "2026-05-26T10:03:00Z", 15., 20., 15., 18.));

        assert_eq!(indicator.active_gaps().len(), 1);
        assert_eq!(indicator.expired_gaps().len(), 0);

        // 2. Candle 4 (Index 4). Diff = 4 - 3 = 1 bar.
        // 1 < 2, so the gap remains active.
        indicator.update(candle(4, "2026-05-26T10:04:00Z", 20., 25., 20., 22.));

        assert_eq!(indicator.active_gaps().len(), 1);
        assert_eq!(indicator.expired_gaps().len(), 0);

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

        let expired = &indicator.expired_gaps()[0];
        assert_eq!(expired.creation_index(), 3);
        assert_eq!(expired.state().expired_time(), ts("2026-05-26T10:05:00Z"));
    }

    #[test]
    fn ttl_expires_after_time_duration() {
        // Expire if 5 minutes have passed since creation
        let mut indicator =
            StreamingFairValueGap::default().with_min_gap_size(1.0).with_ttl_policy(TtlPolicy::Time(Duration::minutes(5)));

        // 1. Create Bullish Gap: Top=15.0, Bottom=10.0
        // C3 close_timestamp = "10:03:00Z"
        indicator.update(candle(1, "2026-05-26T10:01:00Z", 10., 10., 5., 8.));
        indicator.update(candle(2, "2026-05-26T10:02:00Z", 10., 12., 8., 11.));
        indicator.update(candle(3, "2026-05-26T10:03:00Z", 15., 20., 15., 18.));

        // 2. Candle at 10:07:00Z. Diff = 4 mins.
        // 4 mins < 5 mins, so it remains active.
        indicator.update(candle(4, "2026-05-26T10:07:00Z", 20., 25., 20., 22.));
        assert_eq!(indicator.active_gaps().len(), 1);

        // 3. Candle at 10:08:00Z. Diff = 5 mins.
        // 5 mins >= 5 mins, gap expires.
        indicator.update(candle(5, "2026-05-26T10:08:00Z", 20., 25., 20., 22.));
        assert_eq!(indicator.active_gaps().len(), 0);
        assert_eq!(indicator.expired_gaps().len(), 1);
    }

    #[test]
    fn expired_state_preserves_partial_fill_history() {
        let mut indicator = StreamingFairValueGap::default().with_min_gap_size(1.0).with_ttl_policy(TtlPolicy::Bars(2));

        // 1. Create Bullish Gap: Top=15.0, Bottom=10.0, Size=5.0
        indicator.update(candle(1, "2026-05-26T10:01:00Z", 10., 10., 5., 8.));
        indicator.update(candle(2, "2026-05-26T10:02:00Z", 10., 12., 8., 11.));
        indicator.update(candle(3, "2026-05-26T10:03:00Z", 15., 20., 15., 18.));

        // 2. Partial Fill: Wick down to 12.5 (50% fill) on the very next bar
        // This is 1 bar after creation, so it does NOT expire yet.
        indicator.update(candle(4, "2026-05-26T10:04:00Z", 18., 18., 12.5, 17.));

        // 3. Expiration: Next bar runs away but triggers the 2-bar expiration limit.
        indicator.update(candle(5, "2026-05-26T10:05:00Z", 20., 25., 20., 22.));

        assert_eq!(indicator.active_gaps().len(), 0);
        assert_eq!(indicator.expired_gaps().len(), 1);

        // Verify that the ExpiredState successfully inherited the fill data from OpenState
        let expired = &indicator.expired_gaps()[0];
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
        let mut indicator = StreamingFairValueGap::default().with_min_gap_size(1.0).with_ttl_policy(TtlPolicy::Filled);

        // 1. Bullish Gap erstellen: Top=15.0, Bottom=10.0 (Creation Index = 3)
        indicator.update(candle(1, "2026-05-26T10:01:00Z", 10., 10., 5., 8.));
        indicator.update(candle(2, "2026-05-26T10:02:00Z", 10., 12., 8., 11.));
        indicator.update(candle(3, "2026-05-26T10:03:00Z", 15., 20., 15., 18.));

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

    #[test]
    fn expiration_takes_precedence_over_fill() {
        // Dieser Test dokumentiert die Invariante, dass das TTL geprüft wird,
        // BEVOR die Preis-Action der Kerze gegen die Lücke evaluiert wird.
        let mut indicator = StreamingFairValueGap::default().with_min_gap_size(1.0).with_ttl_policy(TtlPolicy::Bars(2));

        // 1. Bullish Gap erstellen: Top=15.0, Bottom=10.0 (Creation Index = 3)
        indicator.update(candle(1, "2026-05-26T10:01:00Z", 10., 10., 5., 8.));
        indicator.update(candle(2, "2026-05-26T10:02:00Z", 10., 12., 8., 11.));
        indicator.update(candle(3, "2026-05-26T10:03:00Z", 15., 20., 15., 18.));

        // 2. Index 5 ist exakt 2 Bars nach Index 3. Das löst das Expiration-Limit aus.
        // Obwohl diese Kerze massiv fällt (Low=5.0) und die Lücke eigentlich komplett schließen würde,
        // muss sie als 'Expired' (und nicht als 'Closed') markiert werden, da das TTL Vorrang hat.
        indicator.update(candle(5, "2026-05-26T10:05:00Z", 20., 20., 5., 8.));

        assert_eq!(indicator.active_gaps().len(), 0);
        assert_eq!(
            indicator.historical_gaps().len(),
            0,
            "Gap should NOT be marked as closed"
        );
        assert_eq!(
            indicator.expired_gaps().len(),
            1,
            "Gap MUST be marked as expired due to operation order"
        );

        let expired = &indicator.expired_gaps()[0];
        // Da die Lücke verfiel, bevor der Drop ausgewertet wurde, bleibt der Fill-Status bei 0.0
        assert_f64_eq(expired.state().final_fill_percentage(), 0.0);
    }
}
