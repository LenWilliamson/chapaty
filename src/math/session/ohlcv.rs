use std::fmt::Debug;

use crate::{
    data::{
        domain::{Price, Volume},
        event::{MarketEvent, Ohlcv},
    },
    math::{
        StreamingIndicator,
        volatility::{StreamingOhlcvVwap, VwapPriceSource},
    },
};

use super::{Awaiting, Building, Closed, RangeState, SessionDate, SessionWindow, WindowPosition};

// ================================================================================================
// OHLCV Session Range
// ================================================================================================

/// Frozen overnight/session range built from [`Ohlcv`] bars.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OvernightRangeOhlcvData {
    session: SessionDate,
    high: Price,
    low: Price,
    highest_close: Price,
    lowest_close: Price,
    volume: Volume,
    vwap: Option<Price>,
}

impl OvernightRangeOhlcvData {
    /// Date anchoring this session.
    pub fn session(&self) -> SessionDate {
        self.session
    }

    /// Highest bar high over the window.
    pub fn high(&self) -> Price {
        self.high
    }

    /// Lowest bar low over the window.
    pub fn low(&self) -> Price {
        self.low
    }

    /// Highest bar *close* over the window.
    pub fn highest_close(&self) -> Price {
        self.highest_close
    }

    /// Lowest bar *close* over the window.
    pub fn lowest_close(&self) -> Price {
        self.lowest_close
    }

    /// Total traded volume over the window.
    pub fn volume(&self) -> Volume {
        self.volume
    }

    /// Session VWAP at the last update.
    pub fn vwap(&self) -> Option<Price> {
        self.vwap
    }
}

impl OvernightRangeOhlcvData {
    fn fold(self, mut vwap_acc: StreamingOhlcvVwap, bar: Ohlcv) -> (Self, StreamingOhlcvVwap) {
        let vwap = vwap_acc.update(bar).map(Price);
        let new_self = Self {
            session: self.session,
            high: self.high.max(bar.high),
            low: self.low.min(bar.low),
            highest_close: self.highest_close.max(bar.close),
            lowest_close: self.lowest_close.min(bar.close),
            volume: self.volume + bar.volume.0,
            vwap,
        };
        (new_self, vwap_acc)
    }
}

/// Represents the deterministic outcome of processing an OHLCV bar.
enum TransitionOutcome {
    Progress(OvernightOhlcvRangeStatus),
    Completed {
        next_status: OvernightOhlcvRangeStatus,
        completed_range: OvernightRangeOhlcvData,
    },
}

/// Streaming overnight/session range over [`Ohlcv`] bars.
#[derive(Debug, Clone, Copy)]
struct OvernightOhlcvRange<S: RangeState> {
    window: SessionWindow,
    vwap_source: VwapPriceSource,
    state: S,
}

impl<S: RangeState> OvernightOhlcvRange<S> {
    fn map<NewState: RangeState, F>(self, f: F) -> OvernightOhlcvRange<NewState>
    where
        F: FnOnce(S) -> NewState,
    {
        OvernightOhlcvRange {
            window: self.window,
            vwap_source: self.vwap_source,
            state: f(self.state),
        }
    }

    fn reset(self) -> OvernightOhlcvRange<Awaiting> {
        self.map(|_| Awaiting)
    }

    fn into_building(
        self,
        session: SessionDate,
        bar: Ohlcv,
    ) -> OvernightOhlcvRange<Building<OvernightRangeOhlcvData, StreamingOhlcvVwap>> {
        self.map(|_| {
            let mut vwap_acc = StreamingOhlcvVwap::new(self.vwap_source);
            let vwap = vwap_acc.update(bar).map(Price);
            Building {
                range: OvernightRangeOhlcvData {
                    session,
                    high: bar.high,
                    low: bar.low,
                    highest_close: bar.close,
                    lowest_close: bar.close,
                    volume: bar.volume,
                    vwap,
                },
                accumulator: vwap_acc,
            }
        })
    }
}

impl OvernightOhlcvRange<Awaiting> {
    fn new(window: SessionWindow, vwap_source: VwapPriceSource) -> Self {
        Self {
            window,
            vwap_source,
            state: Awaiting,
        }
    }

    fn update(self, ohlcv: Ohlcv) -> TransitionOutcome {
        match self.window.classify(ohlcv.point_in_time()) {
            WindowPosition::Within(session) => TransitionOutcome::Progress(
                OvernightOhlcvRangeStatus::Building(self.into_building(session, ohlcv)),
            ),
            WindowPosition::Outside => {
                TransitionOutcome::Progress(OvernightOhlcvRangeStatus::Awaiting(self))
            }
        }
    }
}

impl OvernightOhlcvRange<Closed<OvernightRangeOhlcvData>> {
    fn update(self, ohlcv: Ohlcv) -> TransitionOutcome {
        match self.window.classify(ohlcv.point_in_time()) {
            WindowPosition::Within(session) => TransitionOutcome::Progress(
                OvernightOhlcvRangeStatus::Building(self.into_building(session, ohlcv)),
            ),
            WindowPosition::Outside => {
                TransitionOutcome::Progress(OvernightOhlcvRangeStatus::Closed(self))
            }
        }
    }
}

impl OvernightOhlcvRange<Building<OvernightRangeOhlcvData, StreamingOhlcvVwap>> {
    fn close(self) -> OvernightOhlcvRange<Closed<OvernightRangeOhlcvData>> {
        self.map(|s| Closed { range: s.range })
    }

    fn update(self, ohlcv: Ohlcv) -> TransitionOutcome {
        match self.window.classify(ohlcv.point_in_time()) {
            WindowPosition::Within(current_session) => {
                if current_session == self.state.range.session {
                    // Same session -> Fold
                    let (new_range, new_vwap_acc) =
                        self.state.range.fold(self.state.accumulator, ohlcv);
                    let new_state = self.map(|_| Building {
                        range: new_range,
                        accumulator: new_vwap_acc,
                    });
                    TransitionOutcome::Progress(OvernightOhlcvRangeStatus::Building(new_state))
                } else {
                    // Back-to-back sessions (no 'Outside' gap) -> Hard reset to new session
                    let completed_range = self.state.range;
                    let new_state = self.into_building(current_session, ohlcv);
                    TransitionOutcome::Completed {
                        next_status: OvernightOhlcvRangeStatus::Building(new_state),
                        completed_range,
                    }
                }
            }
            WindowPosition::Outside => {
                let closed_state = self.close();
                let completed_range = closed_state.state.range;
                TransitionOutcome::Completed {
                    next_status: OvernightOhlcvRangeStatus::Closed(closed_state),
                    completed_range,
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum OvernightOhlcvRangeStatus {
    Awaiting(OvernightOhlcvRange<Awaiting>),
    Building(OvernightOhlcvRange<Building<OvernightRangeOhlcvData, StreamingOhlcvVwap>>),
    Closed(OvernightOhlcvRange<Closed<OvernightRangeOhlcvData>>),
}

impl OvernightOhlcvRangeStatus {
    fn update(self, ohlcv: Ohlcv) -> TransitionOutcome {
        match self {
            OvernightOhlcvRangeStatus::Awaiting(r) => r.update(ohlcv),
            OvernightOhlcvRangeStatus::Building(r) => r.update(ohlcv),
            OvernightOhlcvRangeStatus::Closed(r) => r.update(ohlcv),
        }
    }

    fn reset(self) -> OvernightOhlcvRangeStatus {
        match self {
            OvernightOhlcvRangeStatus::Awaiting(r) => {
                OvernightOhlcvRangeStatus::Awaiting(r.reset())
            }
            OvernightOhlcvRangeStatus::Building(r) => {
                OvernightOhlcvRangeStatus::Awaiting(r.reset())
            }
            OvernightOhlcvRangeStatus::Closed(r) => OvernightOhlcvRangeStatus::Awaiting(r.reset()),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StreamingOvernightOhlcvRange {
    status: OvernightOhlcvRangeStatus,
    /// Caches the most recently completed session so it isn't lost in 24/7 markets
    last_completed_session: Option<OvernightRangeOhlcvData>,
}

impl StreamingOvernightOhlcvRange {
    pub fn new(window: SessionWindow, vwap_source: VwapPriceSource) -> Self {
        Self {
            status: OvernightOhlcvRangeStatus::Awaiting(OvernightOhlcvRange::new(
                window,
                vwap_source,
            )),
            last_completed_session: None,
        }
    }
}

impl StreamingIndicator for StreamingOvernightOhlcvRange {
    type Input = Ohlcv;
    type Output<'a> = Option<OvernightRangeOhlcvData>;

    fn update(&mut self, ohlcv: Ohlcv) -> Self::Output<'_> {
        match self.status.update(ohlcv) {
            TransitionOutcome::Progress(new_status) => {
                self.status = new_status;
            }
            TransitionOutcome::Completed {
                next_status,
                completed_range,
            } => {
                self.status = next_status;
                self.last_completed_session = Some(completed_range);
            }
        }
        self.last_completed_session
    }

    fn reset(&mut self) {
        self.status = self.status.reset();
        self.last_completed_session = None;
    }
}
