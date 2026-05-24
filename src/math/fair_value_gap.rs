use std::{collections::VecDeque, fmt::Debug};

use chrono::{DateTime, Utc};

use crate::{
    data::{
        domain::Price,
        event::{MarketEvent, Ohlcv},
    },
    math::StreamingIndicator,
};

const LHS: usize = 0;
const RHS: usize = 2;
const PATTERN_LENGTH: usize = 3;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FairValueGapDirection {
    Bullish,
    Bearish,
}

#[derive(Debug, Clone, Copy)]
pub struct FairValueGap<S: FairValueGapState> {
    direction: FairValueGapDirection,
    creation_time: DateTime<Utc>,
    top: Price,
    bottom: Price,
    state: S,
}

#[derive(Debug, Clone, Copy)]
pub enum FairValueGapStatus {
    Open(FairValueGap<OpenState>),
    Closed(FairValueGap<ClosedState>),
}

impl MarketEvent for FairValueGapStatus {
    fn point_in_time(&self) -> DateTime<Utc> {
        match self {
            FairValueGapStatus::Open(gap) => gap.point_in_time(),
            FairValueGapStatus::Closed(gap) => gap.point_in_time(),
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

    pub fn top(&self) -> Price {
        self.top
    }

    pub fn bottom(&self) -> Price {
        self.bottom
    }

    pub fn state(&self) -> &S {
        &self.state
    }

    pub fn gap_size(&self) -> Price {
        self.top - self.bottom
    }

    pub fn map<NewState: FairValueGapState, F>(self, f: F) -> FairValueGap<NewState>
    where
        F: FnOnce(S) -> NewState,
    {
        FairValueGap {
            direction: self.direction,
            creation_time: self.creation_time,
            top: self.top,
            bottom: self.bottom,
            state: f(self.state),
        }
    }
}

impl FairValueGap<OpenState> {
    /// Evaluates the incoming candle against the open gap.
    pub fn process_candle(self, candle: Ohlcv) -> FairValueGapStatus {
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

        let fill_pct = match self.direction {
            FairValueGapDirection::Bullish => (self.top - candle.low) / gap_size,
            FairValueGapDirection::Bearish => (candle.high - self.bottom) / gap_size,
        }
        .0;

        FairValueGapStatus::Open(self.with_partial_fill(fill_pct))
    }

    /// Updates the open state with a new fill percentage, incrementing the touch count.
    fn with_partial_fill(self, cur_fill_pct: f64) -> Self {
        let max_fill_percentage = self
            .state
            .max_fill_percentage
            .max(cur_fill_pct.clamp(0.0, 1.0));
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
}

#[derive(Debug, Clone)]
pub struct StreamingFairValueGap {
    min_gap_size: f64,
    buffer: VecDeque<Ohlcv>,
    active_gaps: Vec<FairValueGap<OpenState>>,
    historical_gaps: Vec<FairValueGap<ClosedState>>,
}

impl StreamingFairValueGap {
    /// Creates a new `StreamingFairValueGap` indicator.
    ///
    /// # Arguments
    /// * `min_gap_size` - The minimum absolute price difference required to register a Fair Value Gap.
    ///                    This acts as a filter to reject microscopic imbalances. Must be > 0.0.
    ///
    /// # Panics
    /// Panics if `min_gap_size` <= 0.0.
    pub fn new(min_gap_size: f64) -> Self {
        assert!(
            min_gap_size > 0.0,
            "min_gap_size must be strictly positive (got {min_gap_size} which is <= 0.0)"
        );
        Self {
            min_gap_size,
            buffer: VecDeque::with_capacity(PATTERN_LENGTH),
            active_gaps: Vec::new(),
            historical_gaps: Vec::new(),
        }
    }

    pub fn active_gaps(&self) -> &[FairValueGap<OpenState>] {
        &self.active_gaps
    }

    pub fn historical_gaps(&self) -> &[FairValueGap<ClosedState>] {
        &self.historical_gaps
    }
}

impl StreamingFairValueGap {
    /// Function to detect a new gap.
    fn detect_gap(&self) -> Option<FairValueGap<OpenState>> {
        if self.buffer.len() < PATTERN_LENGTH {
            return None;
        }

        let lhs = self.buffer[LHS];
        let rhs = self.buffer[RHS];

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
            top,
            bottom,
            state: OpenState::default(),
        })
    }
}

impl StreamingIndicator for StreamingFairValueGap {
    type Input = Ohlcv;
    type Output<'a> = &'a [FairValueGap<OpenState>];

    fn update(&mut self, candle: Self::Input) -> Self::Output<'_> {
        let mut i = 0;
        while i < self.active_gaps.len() {
            let gap = self.active_gaps[i];
            match gap.process_candle(candle) {
                FairValueGapStatus::Open(updated_gap) => {
                    self.active_gaps[i] = updated_gap;
                    i += 1;
                }
                FairValueGapStatus::Closed(closed_gap) => {
                    self.historical_gaps.push(closed_gap);
                    self.active_gaps.remove(i);
                }
            }
        }

        if self.buffer.len() >= PATTERN_LENGTH {
            self.buffer.pop_front();
        }
        self.buffer.push_back(candle);

        if let Some(new_gap) = self.detect_gap() {
            self.active_gaps.push(new_gap);
        }

        self.active_gaps.as_slice()
    }

    fn reset(&mut self) {
        self.buffer.clear();
        self.active_gaps.clear();
        self.historical_gaps.clear();
    }
}

/*
 *
Implemenation Plan Batch Indicator:

### Part 2: Fair Value Gaps (FVG)

FVG boundary logic is purely geometric and trivial for Polars, but tracking partial fills and touches is path-dependent.

#### Step 1: Polars Transformation (Creation)

We use Polars `shift` to instantly find all 3-bar imbalances. No lookahead bias exists here because an FVG is inherently confirmed upon the close of Candle 3 (the current row).

```python
df = df.with_columns([
    # Bullish FVG
    (pl.col("high").shift(2) < pl.col("low")).alias("is_bull_fvg_created"),
    pl.col("low").alias("bull_fvg_top"),
    pl.col("high").shift(2).alias("bull_fvg_bottom"),

    # Bearish FVG
    (pl.col("low").shift(2) > pl.col("high")).alias("is_bear_fvg_created"),
    pl.col("low").shift(2).alias("bear_fvg_top"),
    pl.col("high").alias("bear_fvg_bottom"),
])

```

#### Step 2: Rust AoS Pre-pass (The Lifecycle Simulation)

Because RL agents need to know the exact state of all open gaps at time `t`, we must pre-simulate the mitigations. We will create a `Vec<Vec<Fvg<OpenState>>>` sidecar array, where the outer vector maps 1:1 to your Gym's `t` index.

Iterate through your AoS from `t = 0` to `end`:

1. **Clone Previous State:** Copy the active gaps from `t - 1` into the current step `t`.
2. **Apply Price Action to Existing Gaps:**
* Loop through the active gaps.
* If `current_candle.low` enters a Bullish gap: increment `touch_count`.
* Calculate the penetration percentage. If it exceeds `max_fill_percentage`, update it.
* If `max_fill_percentage >= 1.0`, the gap is fully mitigated. Use your `map` endofunctor to transition it to `ClosedState`, and remove it from the active list for step `t`.


3. **Add New Gaps:** Check the Polars boolean flags for row `t`. If `is_bull_fvg_created == true`, push a pristine `Fvg<OpenState>` into the active list.
4. **Store:** Save the active list to `precomputed_fvgs[t]`.

**O(1) Lookup:** During `env.step(t)`, your Gym observation space simply clones `precomputed_fvgs[t]`. The RL agent instantly receives the array of all valid, open gaps, complete with their current `max_fill_percentage` and `touch_count`, with zero runtime cost.
 */

#[cfg(test)]
mod tests {
    use super::*;
    use std::f64::EPSILON;
    // Assuming Quantity is your domain type for volume based on the HHLL tests
    use crate::data::domain::Quantity;

    // ==========================================
    // === 1. Mocks & Helpers ===
    // ==========================================

    /// Parse RFC3339 timestamp string to DateTime<Utc>.
    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    /// A rapid builder for OHLCV candles to keep our test trajectories readable.
    fn candle(time: &str, open: f64, high: f64, low: f64, close: f64) -> Ohlcv {
        assert!(high >= low, "Invalid mock candle: high {high} < low {low}");
        Ohlcv {
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
        // If both gaps existed: C3.low > C1.high >= C1.low > C3.high
        // This transitively means C3.low > C3.high, which is physically impossible.

        let mut fvg = StreamingFairValueGap::new(0.1);

        // Feed an erratic sequence to ensure the math holds and the debug_assert never fires
        let trajectory = vec![
            candle("2026-05-24T10:00:00Z", 50., 100., 10., 50.), // Massive range
            candle("2026-05-24T10:01:00Z", 50., 50., 50., 50.),  // Inside doji
            candle("2026-05-24T10:02:00Z", 10., 10., 10., 10.),  // Exact bottom touch
        ];

        for c in trajectory {
            let _ = fvg.update(c);
        }

        // The debug_assert! in detect_gap mathematically guarantees this state.
        assert_eq!(fvg.active_gaps.len(), 0);
    }

    // ==========================================
    // === 3. Detection & Noise Filtering ===
    // ==========================================

    #[test]
    fn filters_noise_below_min_gap_size() {
        let mut indicator = StreamingFairValueGap::new(2.0);

        // Gap size will be 11.0 - 10.0 = 1.0.
        // Since 1.0 < min_gap_size (2.0), it must be rejected as noise.
        indicator.update(candle("2026-05-24T10:00:00Z", 10., 10., 5., 8.)); // C1 High = 10
        indicator.update(candle("2026-05-24T10:01:00Z", 10., 12., 8., 11.)); // C2
        indicator.update(candle("2026-05-24T10:02:00Z", 12., 15., 11., 14.)); // C3 Low = 11

        assert!(indicator.active_gaps.is_empty());
    }

    #[test]
    fn detects_bullish_and_bearish_fvgs() {
        let mut indicator = StreamingFairValueGap::new(1.0);

        // --- Bullish Sequence ---
        indicator.update(candle("2026-05-24T10:00:00Z", 10., 10., 5., 8.)); // C1 High = 10
        indicator.update(candle("2026-05-24T10:01:00Z", 10., 12., 8., 11.)); // C2
        indicator.update(candle("2026-05-24T10:02:00Z", 15., 20., 15., 18.)); // C3 Low = 15

        assert_eq!(indicator.active_gaps.len(), 1);
        let gap = &indicator.active_gaps[0];
        assert_eq!(gap.direction(), FairValueGapDirection::Bullish);
        assert_eq!(gap.bottom().0, 10.0);
        assert_eq!(gap.top().0, 15.0);
        assert_f64_eq(gap.gap_size().0, 5.0);

        indicator.reset();

        // --- Bearish Sequence ---
        indicator.update(candle("2026-05-24T10:00:00Z", 20., 25., 20., 22.)); // C1 Low = 20
        indicator.update(candle("2026-05-24T10:01:00Z", 18., 22., 15., 16.)); // C2
        indicator.update(candle("2026-05-24T10:02:00Z", 12., 15., 10., 11.)); // C3 High = 15

        assert_eq!(indicator.active_gaps.len(), 1);
        let gap = &indicator.active_gaps[0];
        assert_eq!(gap.direction(), FairValueGapDirection::Bearish);
        assert_eq!(gap.top().0, 20.0);
        assert_eq!(gap.bottom().0, 15.0);
        assert_f64_eq(gap.gap_size().0, 5.0);
    }

    // ==========================================
    // === 4. State Management (Active/Hist) ===
    // ==========================================

    #[test]
    fn partial_fill_updates_active_state_and_clamps() {
        let mut indicator = StreamingFairValueGap::new(1.0);

        // 1. Create Bullish Gap: Top=15.0, Bottom=10.0, Size=5.0
        indicator.update(candle("2026-05-24T10:00:00Z", 10., 10., 5., 8.));
        indicator.update(candle("2026-05-24T10:01:00Z", 10., 12., 8., 11.));
        indicator.update(candle("2026-05-24T10:02:00Z", 15., 20., 15., 18.));

        // 2. Partial Fill: Wick down to 12.5 (50% fill)
        indicator.update(candle("2026-05-24T10:03:00Z", 18., 18., 12.5, 17.));

        assert_eq!(indicator.active_gaps.len(), 1);
        assert_eq!(indicator.historical_gaps.len(), 0); // Still active

        let gap = &indicator.active_gaps[0];
        assert_eq!(gap.state().touch_count(), 1);
        assert_f64_eq(gap.state().max_fill_percentage(), 0.5); // (15 - 12.5) / 5

        // 3. Lesser Fill: Wick down to 14.0 (20% fill). Should NOT reduce max_fill.
        indicator.update(candle("2026-05-24T10:04:00Z", 18., 18., 14.0, 17.));

        let gap = &indicator.active_gaps[0];
        assert_eq!(gap.state().touch_count(), 2);
        assert_f64_eq(gap.state().max_fill_percentage(), 0.5); // Retains 50% max
    }

    #[test]
    fn full_fill_migrates_gap_to_historical() {
        let mut indicator = StreamingFairValueGap::new(1.0);

        // 1. Create Bearish Gap: Top=20.0, Bottom=15.0, Size=5.0
        indicator.update(candle("2026-05-24T10:00:00Z", 20., 25., 20., 22.)); // C1 Low=20
        indicator.update(candle("2026-05-24T10:01:00Z", 18., 22., 15., 16.)); // C2
        indicator.update(candle("2026-05-24T10:02:00Z", 12., 15., 10., 11.)); // C3 High=15

        assert_eq!(indicator.active_gaps.len(), 1);
        assert_eq!(indicator.historical_gaps.len(), 0);

        // 2. Miss (Price drops further away from the gap)
        indicator.update(candle("2026-05-24T10:03:00Z", 10., 12., 5., 8.));
        assert_eq!(indicator.active_gaps[0].state().touch_count(), 0);

        // 3. Full Fill (Price violently rallies through Top of 20.0)
        indicator.update(candle("2026-05-24T10:04:00Z", 12., 21., 12., 21.)); // High = 21 >= 20

        // Assert Migration
        assert_eq!(
            indicator.active_gaps.len(),
            0,
            "Gap should be removed from active pool"
        );
        assert_eq!(
            indicator.historical_gaps.len(),
            1,
            "Gap should be migrated to history"
        );

        let closed = &indicator.historical_gaps[0];
        assert_eq!(closed.direction(), FairValueGapDirection::Bearish);
        assert_f64_eq(closed.state().max_fill_percentage(), 1.0); // Full fill is exactly 1.0
        assert_eq!(closed.state().touch_count(), 1); // Only took 1 touch to close
        assert_eq!(closed.state().closed_time(), ts("2026-05-24T10:04:00Z")); // Time of the violating candle
    }

    #[test]
    fn boundary_exact_tick_is_a_miss() {
        let mut indicator = StreamingFairValueGap::new(1.0);

        // Create Bullish Gap: Top=15.0, Bottom=10.0
        indicator.update(candle("1", 10., 10., 5., 8.));
        indicator.update(candle("2", 10., 12., 8., 11.));
        indicator.update(candle("3", 15., 20., 15., 18.));

        // Send a candle that wicks to EXACTLY 15.0
        // Because process_candle uses `candle.low < self.top`, this evaluates to false.
        // It is mathematically defined as a Miss, NOT a touch/partial fill.
        indicator.update(candle("4", 20., 20., 15.0, 20.));

        let gap = &indicator.active_gaps[0];
        assert_eq!(
            gap.state().touch_count(),
            0,
            "Exact tick overlap should not increment touches"
        );
        assert_f64_eq(gap.state().max_fill_percentage(), 0.0);
    }

    #[test]
    fn multiple_gaps_tracked_and_filled_independently() {
        let mut indicator = StreamingFairValueGap::new(1.0);

        // 1. Create Bullish Gap A (10 -> 15)
        indicator.update(candle("1", 10., 10., 5., 8.));
        indicator.update(candle("2", 10., 12., 8., 11.));
        indicator.update(candle("3", 15., 20., 15., 18.));

        // 2. Create Bullish Gap B (25 -> 30) further up the trend
        indicator.update(candle("4", 25., 25., 20., 22.));
        indicator.update(candle("5", 25., 28., 22., 26.));
        indicator.update(candle("6", 30., 35., 30., 32.));

        assert_eq!(indicator.active_gaps.len(), 2);

        // 3. Price drops to 20. This completely fills Gap B (25->30), but only misses Gap A (10->15)
        indicator.update(candle("7", 30., 30., 20., 25.));

        assert_eq!(indicator.active_gaps.len(), 1, "Gap B should be closed");
        assert_eq!(
            indicator.historical_gaps.len(),
            1,
            "Gap B should be in history"
        );

        // Verify Gap A is still active and untouched
        let active_gap = &indicator.active_gaps[0];
        assert_eq!(active_gap.bottom().0, 10.0);
        assert_eq!(active_gap.state().touch_count(), 0);

        // Verify Gap B is closed
        let closed_gap = &indicator.historical_gaps[0];
        assert_eq!(closed_gap.bottom().0, 25.0);
        assert_eq!(closed_gap.state().touch_count(), 1);
        assert_f64_eq(closed_gap.state().max_fill_percentage(), 1.0);
    }
}
