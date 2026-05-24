use std::{collections::VecDeque, fmt::Debug};

use chrono::{DateTime, Utc};

use crate::{
    data::{
        domain::Price,
        event::{MarketEvent, Ohlcv},
    },
    math::StreamingIndicator,
};

const PATTERN_LENGTH: usize = 3;
pub trait FairValueGapState: Debug + Clone + Send + Sync + 'static {}

#[derive(Debug, Clone, Copy, PartialEq)]
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
    /// Returns `true` if the gap is fully filled and should be closed.
    fn process_candle(self, candle: Ohlcv) -> FairValueGapStatus {
        let top_f = self.top;
        let bot_f = self.bottom;
        let gap_size = top_f - bot_f;

        match self.direction {
            FairValueGapDirection::Bullish => {
                if candle.low >= self.top {
                    return FairValueGapStatus::Open(self);
                }

                let is_fully_filled = candle.low <= self.bottom;
                if is_fully_filled {
                    let closed_fvg = self.into_closed(candle.point_in_time());
                    return FairValueGapStatus::Closed(closed_fvg);
                }

                let cur_fill_pct = ((top_f - candle.low) / gap_size).0.clamp(0.0, 1.0);
                let new_gap = self.with_cur_fill_percentage(cur_fill_pct);
                FairValueGapStatus::Open(new_gap)
            }
            FairValueGapDirection::Bearish => {
                // Highly predictable early exit: Price didn't rally into the gap.
                if candle.high <= self.bottom {
                    return FairValueGapStatus::Open(self);
                }

                let is_fully_filled = candle.high >= self.top;
                if is_fully_filled {
                    let closed_fvg = self.into_closed(candle.point_in_time());
                    return FairValueGapStatus::Closed(closed_fvg);
                }

                // Use math instead of branches to bound the state
                let cur_fill_pct = ((candle.high - bot_f) / gap_size).0.clamp(0.0, 1.0);
                let new_gap = self.with_cur_fill_percentage(cur_fill_pct);
                FairValueGapStatus::Open(new_gap)
            }
        }
    }

    fn with_cur_fill_percentage(self, cur_fill_percentage: f64) -> Self {
        let max_fill_percentage = self.state.max_fill_percentage.max(cur_fill_percentage);
        self.map(|s| OpenState {
            max_fill_percentage,
            touch_count: s.touch_count + 1,
        })
    }

    /// Consumes the open gap and returns a closed gap state.
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
}

impl StreamingFairValueGap {
    /// Function to detect a new gap.
    fn detect_gap(&self) -> Option<FairValueGap<OpenState>> {
        if self.buffer.len() < PATTERN_LENGTH {
            return None;
        }

        let lhs = self.buffer[0];
        let rhs = self.buffer[2];

        // Calculate both potential gaps. Only one can physically be >= 0 at a time.
        let gap_up = rhs.low.0 - lhs.high.0;
        let gap_down = lhs.low.0 - rhs.high.0;

        if gap_up >= self.min_gap_size {
            Some(FairValueGap {
                direction: FairValueGapDirection::Bullish,
                creation_time: rhs.close_timestamp,
                top: rhs.low,
                bottom: lhs.high,
                state: OpenState {
                    max_fill_percentage: 0.0,
                    touch_count: 0,
                },
            })
        } else if gap_down >= self.min_gap_size {
            Some(FairValueGap {
                direction: FairValueGapDirection::Bearish,
                creation_time: rhs.close_timestamp,
                top: lhs.low,
                bottom: rhs.high,
                state: OpenState {
                    max_fill_percentage: 0.0,
                    touch_count: 0,
                },
            })
        } else {
            None
        }
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
