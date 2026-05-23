// === 1. Typestate Definitions ===

use crate::data::event::MarketEvent;

pub trait FvgState {}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OpenState {
    pub max_fill_percentage: f64,
    pub touch_count: u32,
}
impl FvgState for OpenState {}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ClosedState {
    pub closed_time: DateTime<Utc>,
    pub max_fill_percentage: f64, // Usually 1.0, but preserved for analytics
    pub touch_count: u32,
}
impl FvgState for ClosedState {}

// === 2. The Core Struct & Endofunctor ===

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FvgDirection {
    Bullish,
    Bearish,
}

#[derive(Debug, Clone)]
pub struct FairValueGap<S: FvgState> {
    pub direction: FvgDirection,
    pub creation_time: DateTime<Utc>,
    // Boundaries are completely immutable
    pub top: Price,
    pub bottom: Price,
    pub state: S,
}

impl<S: FvgState> MarketEvent for FairValueGap<S> {
    fn point_in_time(&self) -> DateTime<Utc> {
        self.creation_time
    }
}

impl<S: FvgState> FairValueGap<S> {
    /// Endofunctor map: Transitions the FVG from one state to another.
    pub fn map<NewState: FvgState, F>(self, f: F) -> FairValueGap<NewState>
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

// === 3. FVG Wrapper Enum for the Gym State ===

#[derive(Debug, Clone)]
pub enum FvgStatus {
    Open(FairValueGap<OpenState>),
    Closed(FairValueGap<ClosedState>),
}

// === 4. The Streaming Indicator ===

#[derive(Debug, Clone)]
pub struct StreamingFvg {
    buffer: VecDeque<Ohlcv>,
    pub active_gaps: Vec<FairValueGap<OpenState>>,
    pub historical_gaps: Vec<FairValueGap<ClosedState>>, // Optional: store closed ones
}

impl StreamingFvg {
    pub fn new() -> Self {
        Self {
            buffer: VecDeque::with_capacity(3),
            active_gaps: Vec::new(),
            historical_gaps: Vec::new(),
        }
    }

    // Assumes Price -> f64 conversion for percentage math.
    // In production, implement a method on `Price` for this.
    fn to_f64(p: Price) -> f64 {
        0.0 /* p.into() or p.0 */
    }
}

impl StreamingIndicator for StreamingFvg {
    type Input = Ohlcv;
    type Output = Vec<FairValueGap<OpenState>>; // Gym only needs currently active gaps

    fn update(&mut self, candle: Ohlcv) -> Self::Output {
        // --- 1. State Maintenance (Partial Fills & Closures) ---

        let mut i = 0;
        while i < self.active_gaps.len() {
            let gap = &mut self.active_gaps[i];
            let top_f = Self::to_f64(gap.top);
            let bot_f = Self::to_f64(gap.bottom);
            let gap_size = top_f - bot_f;

            let mut is_filled = false;

            if gap.direction == FvgDirection::Bullish && candle.low < gap.top {
                gap.state.touch_count += 1;
                let penetration = (top_f - Self::to_f64(candle.low)) / gap_size;

                if penetration > gap.state.max_fill_percentage {
                    gap.state.max_fill_percentage = penetration;
                }

                if candle.low <= gap.bottom {
                    is_filled = true;
                }
            }
            // ... (Bearish Logic Mirrored) ...

            // Transition State via the Endofunctor map!
            if is_filled {
                let open_gap = self.active_gaps.remove(i);
                let closed_gap = open_gap.map(|s| ClosedState {
                    closed_time: candle.close_timestamp,
                    max_fill_percentage: 1.0,
                    touch_count: s.touch_count,
                });
                self.historical_gaps.push(closed_gap);
            } else {
                i += 1; // Only advance if we didn't remove an item
            }
        }

        // --- 2. FVG Creation (3-Bar Pattern) ---

        self.buffer.push_back(candle);
        if self.buffer.len() > 3 {
            self.buffer.pop_front();
        }

        if self.buffer.len() == 3 {
            let c1 = self.buffer[0];
            let c3 = self.buffer[2]; // current candle

            // Bullish Imbalance: C1 High < C3 Low
            if c1.high < c3.low {
                self.active_gaps.push(FairValueGap {
                    direction: FvgDirection::Bullish,
                    creation_time: c3.close_timestamp,
                    top: c3.low,
                    bottom: c1.high,
                    state: OpenState {
                        max_fill_percentage: 0.0,
                        touch_count: 0,
                    },
                });
            }
            // Bearish Imbalance: C1 Low > C3 High
            else if c1.low > c3.high {
                self.active_gaps.push(FairValueGap {
                    direction: FvgDirection::Bearish,
                    creation_time: c3.close_timestamp,
                    top: c1.low,     // C1 Low is the top of the gap
                    bottom: c3.high, // C3 High is the bottom
                    state: OpenState {
                        max_fill_percentage: 0.0,
                        touch_count: 0,
                    },
                });
            }
        }

        self.active_gaps.clone()
    }

    fn reset(&mut self) {
        self.buffer.clear();
        self.active_gaps.clear();
        self.historical_gaps.clear();
    }
}
