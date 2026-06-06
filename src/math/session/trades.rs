use std::fmt::Debug;

use crate::{
    data::event::{MarketEvent, TradeEvent},
    math::{StreamingIndicator, volatility::StreamingTradesVwap},
};

use super::{Awaiting, Building, Closed, RangeState, SessionDate, SessionWindow, WindowPosition};

// ================================================================================================
// Trades Session Range
// ================================================================================================

/// Frozen overnight/session range built from [`TradeEvent`]s.
///
/// Trades are point data, so there is no high-vs-close distinction: `high`/`low`
/// are the price envelope and there are no separate close extremes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OvernightRangeTradesData {
    session: SessionDate,
    /// Highest execution price over the window.
    pub high: f64,
    /// Lowest execution price over the window.
    pub low: f64,
    /// Total traded quantity over the window.
    pub volume: f64,
    /// Session VWAP at the last update.
    pub vwap: f64,
}

impl OvernightRangeTradesData {
    /// Date anchoring this session.
    pub fn session(&self) -> SessionDate {
        self.session
    }
}

impl OvernightRangeTradesData {
    fn fold(
        self,
        mut vwap_acc: StreamingTradesVwap,
        trade: TradeEvent,
    ) -> (Self, StreamingTradesVwap) {
        let snapshot = vwap_acc.update(trade).unwrap_or(trade.price.0);
        let new_self = Self {
            session: self.session,
            high: self.high.max(trade.price.0),
            low: self.low.min(trade.price.0),
            volume: self.volume + trade.quantity.0,
            vwap: snapshot,
        };
        (new_self, vwap_acc)
    }
}

/// Represents the deterministic outcome of processing a TradeEvent.
enum TransitionOutcome {
    Progress(OvernightTradesRangeStatus),
    Completed {
        next_status: OvernightTradesRangeStatus,
        completed_range: OvernightRangeTradesData,
    },
}

/// Streaming overnight/session range over [`TradeEvent`]s.
#[derive(Debug, Clone, Copy)]
struct OvernightTradesRange<S: RangeState> {
    window: SessionWindow,
    state: S,
}

impl<S: RangeState> OvernightTradesRange<S> {
    fn map<NewState: RangeState, F>(self, f: F) -> OvernightTradesRange<NewState>
    where
        F: FnOnce(S) -> NewState,
    {
        OvernightTradesRange {
            window: self.window,
            state: f(self.state),
        }
    }

    fn reset(self) -> OvernightTradesRange<Awaiting> {
        self.map(|_| Awaiting)
    }

    fn into_building(
        self,
        session: SessionDate,
        trade: TradeEvent,
    ) -> OvernightTradesRange<Building<OvernightRangeTradesData, StreamingTradesVwap>> {
        self.map(|_| {
            let mut vwap_acc = StreamingTradesVwap::new();
            let vwap = vwap_acc.update(trade).unwrap_or(trade.price.0);
            Building {
                range: OvernightRangeTradesData {
                    session,
                    high: trade.price.0,
                    low: trade.price.0,
                    volume: trade.quantity.0,
                    vwap,
                },
                accumulator: vwap_acc,
            }
        })
    }
}

impl OvernightTradesRange<Awaiting> {
    fn new(window: SessionWindow) -> Self {
        Self {
            window,
            state: Awaiting,
        }
    }

    fn update(self, trade: TradeEvent) -> TransitionOutcome {
        match self.window.classify(trade.point_in_time()) {
            WindowPosition::Within(session) => TransitionOutcome::Progress(
                OvernightTradesRangeStatus::Building(self.into_building(session, trade)),
            ),
            WindowPosition::Outside => {
                TransitionOutcome::Progress(OvernightTradesRangeStatus::Awaiting(self))
            }
        }
    }
}

impl OvernightTradesRange<Closed<OvernightRangeTradesData>> {
    fn update(self, trade: TradeEvent) -> TransitionOutcome {
        match self.window.classify(trade.point_in_time()) {
            WindowPosition::Within(session) => TransitionOutcome::Progress(
                OvernightTradesRangeStatus::Building(self.into_building(session, trade)),
            ),
            WindowPosition::Outside => {
                TransitionOutcome::Progress(OvernightTradesRangeStatus::Closed(self))
            }
        }
    }
}

impl OvernightTradesRange<Building<OvernightRangeTradesData, StreamingTradesVwap>> {
    fn close(self) -> OvernightTradesRange<Closed<OvernightRangeTradesData>> {
        self.map(|s| Closed { range: s.range })
    }

    fn update(self, trade: TradeEvent) -> TransitionOutcome {
        match self.window.classify(trade.point_in_time()) {
            WindowPosition::Within(current_session) => {
                if current_session == self.state.range.session {
                    let (new_range, new_vwap_acc) =
                        self.state.range.fold(self.state.accumulator, trade);
                    let new_state = self.map(|_| Building {
                        range: new_range,
                        accumulator: new_vwap_acc,
                    });
                    TransitionOutcome::Progress(OvernightTradesRangeStatus::Building(new_state))
                } else {
                    let completed_range = self.state.range;
                    let new_state = self.into_building(current_session, trade);
                    TransitionOutcome::Completed {
                        next_status: OvernightTradesRangeStatus::Building(new_state),
                        completed_range,
                    }
                }
            }
            WindowPosition::Outside => {
                let closed_state = self.close();
                let completed_range = closed_state.state.range;
                TransitionOutcome::Completed {
                    next_status: OvernightTradesRangeStatus::Closed(closed_state),
                    completed_range,
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum OvernightTradesRangeStatus {
    Awaiting(OvernightTradesRange<Awaiting>),
    Building(OvernightTradesRange<Building<OvernightRangeTradesData, StreamingTradesVwap>>),
    Closed(OvernightTradesRange<Closed<OvernightRangeTradesData>>),
}

impl OvernightTradesRangeStatus {
    fn update(self, trade: TradeEvent) -> TransitionOutcome {
        match self {
            OvernightTradesRangeStatus::Awaiting(r) => r.update(trade),
            OvernightTradesRangeStatus::Building(r) => r.update(trade),
            OvernightTradesRangeStatus::Closed(r) => r.update(trade),
        }
    }

    fn reset(self) -> OvernightTradesRangeStatus {
        match self {
            OvernightTradesRangeStatus::Awaiting(r) => {
                OvernightTradesRangeStatus::Awaiting(r.reset())
            }
            OvernightTradesRangeStatus::Building(r) => {
                OvernightTradesRangeStatus::Awaiting(r.reset())
            }
            OvernightTradesRangeStatus::Closed(r) => {
                OvernightTradesRangeStatus::Awaiting(r.reset())
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StreamingOvernightTradesRange {
    status: OvernightTradesRangeStatus,
    last_completed_session: Option<OvernightRangeTradesData>,
}

impl StreamingOvernightTradesRange {
    pub fn new(window: SessionWindow) -> Self {
        Self {
            status: OvernightTradesRangeStatus::Awaiting(OvernightTradesRange::new(window)),
            last_completed_session: None,
        }
    }
}

impl StreamingIndicator for StreamingOvernightTradesRange {
    type Input = TradeEvent;
    type Output<'a> = Option<OvernightRangeTradesData>;

    fn update(&mut self, trade: TradeEvent) -> Self::Output<'_> {
        match self.status.update(trade) {
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
