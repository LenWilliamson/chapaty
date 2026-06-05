use std::cmp::Ordering;

use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use chrono_tz::{America::New_York, Asia::Tokyo, Europe::London, Tz};
use serde::{Deserialize, Serialize};

use crate::{
    data::event::{MarketEvent, Ohlcv, TradeEvent},
    math::{
        StreamingIndicator,
        volatility::{StreamingOhlcvVwap, StreamingTradesVwap, VwapPriceSource},
    },
};

// `MarketEvent`, `StreamingIndicator`, `Ohlcv`, `TradeEvent`, `VwapPriceSource`,
// `StreamingOhlcvVwap`, `StreamingTradesVwap` come from the surrounding crate.

// ================================================================================================
// Session window (shared timing core)
// ================================================================================================

/// Where an event falls relative to an accumulation window.
///
/// Replaces a bare `bool`: an in-window event additionally carries the
/// [`SessionDate`] it belongs to, which is everything the state machine needs
/// to decide between continuing a build and opening a fresh session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WindowPosition {
    /// Inside `[start, end)`, belonging to the given session.
    Within(SessionDate),
    /// Outside the window.
    Outside,
}

/// Identifies one accumulation session by its anchor date.
///
/// For an intraday window this is the event's calendar date. For an overnight
/// window (e.g. 18:00 → 09:30) the evening leg and the following morning leg
/// share a single anchor: the date on which the window opened.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct SessionDate(NaiveDate);

/// A timezone-aware accumulation window defined by a local start and end
/// time-of-day. Holds no running state, so both range indicators share one.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionWindow {
    timezone: Tz,
    start: NaiveTime,
    end: NaiveTime,
}

impl SessionWindow {
    pub fn new(timezone: Tz, start: NaiveTime, end: NaiveTime) -> Self {
        Self {
            timezone,
            start,
            end,
        }
    }

    /// US overnight / pre-market: futures open (18:00 ET) through the equities
    /// open (09:30 ET).
    pub fn us_pre_market() -> Self {
        Self::new(New_York, hm(18, 0), hm(9, 30))
    }

    /// Tokyo cash session (09:00 → 15:00 JST).
    pub fn tokyo_session() -> Self {
        Self::new(Tokyo, hm(9, 0), hm(15, 0))
    }

    /// London morning range (08:00 → 12:00 London time).
    pub fn london_morning() -> Self {
        Self::new(London, hm(8, 0), hm(12, 0))
    }

    /// Classifies an instant against the window, resolving the session it
    /// belongs to when inside.
    fn classify(&self, instant: DateTime<Utc>) -> WindowPosition {
        let local = instant.with_timezone(&self.timezone);
        let date = local.date_naive();
        let now = local.time();

        match self.start.cmp(&self.end) {
            // Intraday window: one calendar day, `[start, end)`.
            Ordering::Less => {
                if (self.start..self.end).contains(&now) {
                    WindowPosition::Within(SessionDate(date))
                } else {
                    WindowPosition::Outside
                }
            }
            // Overnight window wrapping midnight: `[start, 24:00) ∪ [00:00, end)`.
            // The morning leg is anchored on the *previous* calendar day.
            // `start == end` degenerates to a 24h session with its boundary at
            // `start`.
            Ordering::Greater | Ordering::Equal => {
                if now >= self.start {
                    WindowPosition::Within(SessionDate(date))
                } else if now < self.end {
                    WindowPosition::Within(SessionDate(date.pred_opt().unwrap_or(date)))
                } else {
                    WindowPosition::Outside
                }
            }
        }
    }
}

/// Convenience for the preset constructors; the hour/minute pairs are static.
fn hm(h: u32, m: u32) -> NaiveTime {
    NaiveTime::from_hms_opt(h, m, 0).expect("static time-of-day is valid")
}

// ================================================================================================
// OHLCV session range
// ================================================================================================

/// Frozen overnight/session range built from [`Ohlcv`] bars.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct OvernightRangeOhlcvData {
    /// Highest bar high over the window.
    pub high: f64,
    /// Lowest bar low over the window.
    pub low: f64,
    /// Highest bar *close* over the window.
    pub highest_close: f64,
    /// Lowest bar *close* over the window.
    pub lowest_close: f64,
    /// Total traded volume over the window.
    pub volume: f64,
    /// Session VWAP at the last update.
    pub vwap: f64,
}

impl OvernightRangeOhlcvData {
    /// Folds one in-window bar into the running range and advances the VWAP.
    /// High/low/close envelopes and volume update for every bar; the VWAP
    /// itself skips non-positive volume internally.
    fn fold(&mut self, vwap: &mut StreamingOhlcvVwap, bar: Ohlcv) {
        let snapshot = vwap.update(bar).unwrap_or(bar.close.0);
        self.high = self.high.max(bar.high.0);
        self.low = self.low.min(bar.low.0);
        self.highest_close = self.highest_close.max(bar.close.0);
        self.lowest_close = self.lowest_close.min(bar.close.0);
        self.volume += bar.volume.0;
        self.vwap = snapshot;
    }
}

/// Lifecycle of the OHLCV range across one session boundary.
#[derive(Debug, Clone, Default)]
enum OhlcvRangeState {
    /// No session tracked yet — awaiting the first in-window bar.
    #[default]
    Awaiting
    /// Inside the window, folding bars into `range`.
    Building {
        session: SessionDate,
        range: OvernightRangeOhlcvData,
        vwap: StreamingOhlcvVwap,
    },
    /// Window closed: `range` is frozen and re-emitted until the next session.
    Closed {
        session: SessionDate,
        range: OvernightRangeOhlcvData,
    },
}

/// Streaming overnight/session range over [`Ohlcv`] bars.
///
/// Emits `None` while the window is open; once it closes, emits the frozen
/// [`OvernightRangeOhlcvData`] on every subsequent event until the next session
/// opens. The anchor is re-armed automatically whenever the [`SessionDate`]
/// changes (and via [`reset`](StreamingIndicator::reset)).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingOvernightOhlcvRange {
    window: SessionWindow,
    vwap_source: VwapPriceSource,
    #[serde(skip)]
    state: OhlcvRangeState,
}

impl StreamingOvernightOhlcvRange {
    pub fn new(window: SessionWindow, vwap_source: VwapPriceSource) -> Self {
        Self {
            window,
            vwap_source,
            state: OhlcvRangeState::Awaiting,
        }
    }

    /// The frozen range, available once the session window has closed.
    pub fn value(&self) -> Option<OvernightRangeOhlcvData> {
        match &self.state {
            OhlcvRangeState::Closed { range, .. } => Some(*range),
            OhlcvRangeState::Awaiting | OhlcvRangeState::Building { .. } => None,
        }
    }
}

/// Opens a fresh OHLCV session from its first in-window bar.
fn open_ohlcv(session: SessionDate, bar: Ohlcv, source: VwapPriceSource) -> OhlcvRangeState {
    let mut vwap = StreamingOhlcvVwap::new(source);
    let snapshot = vwap.update(bar).unwrap_or(bar.close.0);
    OhlcvRangeState::Building {
        session,
        range: OvernightRangeOhlcvData {
            high: bar.high.0,
            low: bar.low.0,
            highest_close: bar.close.0,
            lowest_close: bar.close.0,
            volume: bar.volume.0,
            vwap: snapshot,
        },
        vwap,
    }
}

impl StreamingIndicator for StreamingOvernightOhlcvRange {
    type Input = Ohlcv;
    type Output<'a>
        = Option<OvernightRangeOhlcvData>
    where
        Self::Input: 'a;

    fn update(&mut self, bar: Ohlcv) -> Self::Output<'_> {
        match self.window.classify(bar.point_in_time()) {
            WindowPosition::Within(session) => {
                match &mut self.state {
                    // Same session in progress → extend it.
                    OhlcvRangeState::Building {
                        session: active,
                        range,
                        vwap,
                    } if *active == session => {
                        range.fold(vwap, bar);
                    }
                    // First bar, a new session after close, or a session-id
                    // change → (re)open. Disjoint field read of `vwap_source`.
                    state => *state = open_ohlcv(session, bar, self.vwap_source),
                }
                None
            }
            WindowPosition::Outside => {
                // The first out-of-window bar after building freezes the range;
                // the live VWAP is no longer needed once frozen.
                let frozen = match &self.state {
                    OhlcvRangeState::Building { session, range, .. } => {
                        Some(OhlcvRangeState::Closed {
                            session: *session,
                            range: *range,
                        })
                    }
                    OhlcvRangeState::Awaiting | OhlcvRangeState::Closed { .. } => None,
                };
                if let Some(state) = frozen {
                    self.state = state;
                }
                self.value()
            }
        }
    }

    fn reset(&mut self) {
        self.state = OhlcvRangeState::Awaiting;
    }
}

// ================================================================================================
// Trades session range
// ================================================================================================

/// Frozen overnight/session range built from [`TradeEvent`]s.
///
/// Trades are point data, so there is no high-vs-close distinction: `high`/`low`
/// are the price envelope and there are no separate close extremes.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct OvernightRangeTradesData {
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
    /// Folds one in-window trade into the running range and advances the VWAP.
    fn fold(&mut self, vwap: &mut StreamingTradesVwap, trade: TradeEvent) {
        let snapshot = vwap.update(trade).unwrap_or(trade.price.0);
        self.high = self.high.max(trade.price.0);
        self.low = self.low.min(trade.price.0);
        self.volume += trade.quantity.0;
        self.vwap = snapshot;
    }
}

/// Lifecycle of the trades range across one session boundary.
#[derive(Debug, Clone, Default)]
enum TradesRangeState {
    /// No session tracked yet — awaiting the first in-window trade.
    #[default]
    Awaiting,
    /// Inside the window, folding trades into `range`.
    Building {
        session: SessionDate,
        range: OvernightRangeTradesData,
        vwap: StreamingTradesVwap,
    },
    /// Window closed: `range` is frozen and re-emitted until the next session.
    Closed {
        session: SessionDate,
        range: OvernightRangeTradesData,
    },
}

/// Streaming overnight/session range over [`TradeEvent`]s.
///
/// Same lifecycle and emission contract as [`StreamingOvernightOhlcvRange`], but
/// a trade carries a single price, so there is no [`VwapPriceSource`] to choose.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingOvernightTradesRange {
    window: SessionWindow,
    #[serde(skip)]
    state: TradesRangeState,
}

impl StreamingOvernightTradesRange {
    pub fn new(window: SessionWindow) -> Self {
        Self {
            window,
            state: TradesRangeState::Awaiting,
        }
    }

    /// The frozen range, available once the session window has closed.
    pub fn value(&self) -> Option<OvernightRangeTradesData> {
        match &self.state {
            TradesRangeState::Closed { range, .. } => Some(*range),
            TradesRangeState::Awaiting | TradesRangeState::Building { .. } => None,
        }
    }
}

/// Opens a fresh trades session from its first in-window trade.
fn open_trades(session: SessionDate, trade: TradeEvent) -> TradesRangeState {
    let mut vwap = StreamingTradesVwap::new();
    let snapshot = vwap.update(trade).unwrap_or(trade.price.0);
    TradesRangeState::Building {
        session,
        range: OvernightRangeTradesData {
            high: trade.price.0,
            low: trade.price.0,
            volume: trade.quantity.0,
            vwap: snapshot,
        },
        vwap,
    }
}

impl StreamingIndicator for StreamingOvernightTradesRange {
    type Input = TradeEvent;
    type Output<'a>
        = Option<OvernightRangeTradesData>
    where
        Self::Input: 'a;

    fn update(&mut self, trade: TradeEvent) -> Self::Output<'_> {
        match self.window.classify(trade.point_in_time()) {
            WindowPosition::Within(session) => {
                match &mut self.state {
                    TradesRangeState::Building {
                        session: active,
                        range,
                        vwap,
                    } if *active == session => {
                        range.fold(vwap, trade);
                    }
                    state => *state = open_trades(session, trade),
                }
                None
            }
            WindowPosition::Outside => {
                let frozen = match &self.state {
                    TradesRangeState::Building { session, range, .. } => {
                        Some(TradesRangeState::Closed {
                            session: *session,
                            range: *range,
                        })
                    }
                    TradesRangeState::Awaiting | TradesRangeState::Closed { .. } => None,
                };
                if let Some(state) = frozen {
                    self.state = state;
                }
                self.value()
            }
        }
    }

    fn reset(&mut self) {
        self.state = TradesRangeState::Awaiting;
    }
}
