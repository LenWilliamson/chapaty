use std::{cmp::Ordering, fmt::Debug};

use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use chrono_tz::Tz;

use crate::{
    data::{
        domain::{Price, Volume},
        event::{MarketEvent, Ohlcv, TradeEvent},
    },
    math::{
        StreamingIndicator,
        volatility::{StreamingOhlcvVwap, StreamingTradesVwap, VwapPriceSource},
    },
};

// ================================================================================================
// Session window (shared timing core)
// ================================================================================================

/// Where an event falls relative to an accumulation window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WindowPosition {
    /// Inside `[start, end)`, belonging to the given session.
    Within(SessionDate),
    /// Outside the window.
    Outside,
}

/// The chronological shape of the session window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WindowKind {
    /// Intraday window: contained within one calendar day, `[start, end)`.
    Intraday,
    /// Overnight window: crosses midnight, wrapping to the next day.
    Overnight,
}

/// Identifies one accumulation session by its anchor date.
///
/// For an intraday window, this is the event's exact calendar date. For an overnight
/// window (e.g., 18:00 -> 09:30), the evening leg and the following morning leg
/// share a single anchor: the date on which the window opened.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SessionDate(pub NaiveDate);

/// A timezone-aware accumulation window defined by a local start and end time-of-day.
#[derive(Debug, Clone, Copy)]
pub struct SessionWindow {
    timezone: Tz,
    start: NaiveTime,
    end: NaiveTime,
}

impl SessionWindow {
    /// Creates a new custom session window.
    pub fn new(timezone: Tz, start: NaiveTime, end: NaiveTime) -> Self {
        Self {
            timezone,
            start,
            end,
        }
    }

    /// US Regular Trading Hours (RTH) Core Session (09:30 -> 16:00 ET).
    pub fn us_core_session() -> Self {
        SessionWindow::new(Tz::America__New_York, hm(9, 30), hm(16, 0))
    }

    /// London Core European Session (08:00 -> 16:30 London time).
    pub fn london_core_session() -> Self {
        SessionWindow::new(Tz::Europe__London, hm(8, 0), hm(16, 30))
    }

    /// US/Europe Overlap (13:00 -> 17:00 London time / 08:00 -> 12:00 ET).
    pub fn us_europe_overlap() -> Self {
        SessionWindow::new(Tz::Europe__London, hm(13, 0), hm(17, 0))
    }

    /// Singapore Core Session (09:00 -> 17:00 SGT).
    pub fn singapore_core_session() -> Self {
        SessionWindow::new(Tz::Asia__Singapore, hm(9, 0), hm(17, 0))
    }

    /// Sydney Core Session (10:00 -> 16:00 AEST).
    pub fn sydney_core_session() -> Self {
        SessionWindow::new(Tz::Australia__Sydney, hm(10, 0), hm(16, 0))
    }

    /// US Overnight Globex / Extended Session (18:00 -> 09:30 ET).
    pub fn us_extended_overnight() -> Self {
        SessionWindow::new(Tz::America__New_York, hm(18, 0), hm(9, 30))
    }

    /// Tokyo (TSE) Core Session (09:00 -> 15:00 JST).
    pub fn tokyo_core_session() -> Self {
        SessionWindow::new(Tz::Asia__Tokyo, hm(9, 0), hm(15, 0))
    }

    /// Asia-Pacific Institutional Core (09:00 -> 17:00 SGT).
    pub fn asia_institutional_core() -> Self {
        SessionWindow::new(Tz::Asia__Singapore, hm(9, 0), hm(17, 0))
    }

    /// Hong Kong (HKEX) Core Session (09:30 -> 16:00 HKT).
    pub fn hong_kong_core_session() -> Self {
        SessionWindow::new(Tz::Asia__Hong_Kong, hm(9, 30), hm(16, 0))
    }

    /// Asia-Pacific Overnight / Off-Hours Block (17:00 -> 08:00 SGT).
    pub fn apac_overnight() -> Self {
        SessionWindow::new(Tz::Asia__Singapore, hm(17, 0), hm(8, 0))
    }
}

impl SessionWindow {
    /// Determines the shape of the window based on its chronological bounds.
    fn window_kind(&self) -> WindowKind {
        match self.start.cmp(&self.end) {
            Ordering::Less => WindowKind::Intraday,
            // Overnight window wrapping midnight: `[start, 24:00) u [00:00, end)`.
            // The morning leg is anchored on the previous calendar day.
            // If `start == end`, this implies a 24h session with its boundary at `start`.
            Ordering::Equal | Ordering::Greater => WindowKind::Overnight,
        }
    }

    /// Classifies a UTC timestamp against the window, resolving the session it belongs to when inside.
    fn classify(&self, utc_ts: DateTime<Utc>) -> WindowPosition {
        let local = utc_ts.with_timezone(&self.timezone);
        let date = local.date_naive();
        let now = local.time();

        match self.window_kind() {
            WindowKind::Intraday => {
                if (self.start..self.end).contains(&now) {
                    WindowPosition::Within(SessionDate(date))
                } else {
                    WindowPosition::Outside
                }
            }
            WindowKind::Overnight => {
                if now >= self.start {
                    WindowPosition::Within(SessionDate(date))
                } else if now < self.end {
                    let anchor_date = date.pred_opt().expect(
                        "Market data timestamp violates Chrono's minimum representable date",
                    );
                    WindowPosition::Within(SessionDate(anchor_date))
                } else {
                    WindowPosition::Outside
                }
            }
        }
    }
}

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

trait OhlcvRangeState: Debug + Clone + Copy + Send + Sync + 'static {}

#[derive(Debug, Clone, Copy)]
struct Awaiting;

impl OhlcvRangeState for Awaiting {}

#[derive(Debug, Clone, Copy)]
struct Building {
    range: OvernightRangeOhlcvData,
    streaming_ohlcv_vwap: StreamingOhlcvVwap,
}

impl OhlcvRangeState for Building {}

#[derive(Debug, Clone, Copy)]
struct Closed {
    range: OvernightRangeOhlcvData,
}

impl OhlcvRangeState for Closed {}

/// Streaming overnight/session range over [`Ohlcv`] bars.
///
/// Emits `None` while the window is open. Once it closes, emits the frozen
/// [`OvernightRangeOhlcvData`] on every subsequent event until the next session
/// opens.
#[derive(Debug, Clone, Copy)]
struct OvernightOhlcvRange<S: OhlcvRangeState> {
    window: SessionWindow,
    vwap_source: VwapPriceSource,
    state: S,
}

impl<S: OhlcvRangeState> OvernightOhlcvRange<S> {
    fn map<NewState: OhlcvRangeState, F>(self, f: F) -> OvernightOhlcvRange<NewState>
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

    fn into_building(self, session: SessionDate, bar: Ohlcv) -> OvernightOhlcvRange<Building> {
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
                streaming_ohlcv_vwap: vwap_acc,
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

    fn update(self, ohlcv: Ohlcv) -> OvernightOhlcvRangeStatus {
        match self.window.classify(ohlcv.point_in_time()) {
            WindowPosition::Within(session) => {
                OvernightOhlcvRangeStatus::Building(self.into_building(session, ohlcv))
            }
            WindowPosition::Outside => OvernightOhlcvRangeStatus::Awaiting(self),
        }
    }
}

impl OvernightOhlcvRange<Closed> {
    fn update(self, ohlcv: Ohlcv) -> OvernightOhlcvRangeStatus {
        match self.window.classify(ohlcv.point_in_time()) {
            WindowPosition::Within(session) => {
                OvernightOhlcvRangeStatus::Building(self.into_building(session, ohlcv))
            }
            WindowPosition::Outside => OvernightOhlcvRangeStatus::Closed(self),
        }
    }
}

impl OvernightOhlcvRange<Building> {
    fn close(self) -> OvernightOhlcvRange<Closed> {
        self.map(|s| Closed { range: s.range })
    }

    fn update(self, ohlcv: Ohlcv) -> OvernightOhlcvRangeStatus {
        match self.window.classify(ohlcv.point_in_time()) {
            WindowPosition::Within(current_session) => {
                if current_session == self.state.range.session {
                    // Same session -> Fold
                    let (new_range, new_vwap_acc) = self
                        .state
                        .range
                        .fold(self.state.streaming_ohlcv_vwap, ohlcv);
                    let new_state = self.map(|_| Building {
                        range: new_range,
                        streaming_ohlcv_vwap: new_vwap_acc,
                    });
                    OvernightOhlcvRangeStatus::Building(new_state)
                } else {
                    // Back-to-back sessions (no 'Outside' gap) -> Hard reset to new session
                    OvernightOhlcvRangeStatus::Building(self.into_building(current_session, ohlcv))
                }
            }
            WindowPosition::Outside => OvernightOhlcvRangeStatus::Closed(self.close()),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum OvernightOhlcvRangeStatus {
    /// No session tracked yet. Awaiting the first in-window bar.
    Awaiting(OvernightOhlcvRange<Awaiting>),
    /// Inside the window, actively accumulating bars into `range`.
    Building(OvernightOhlcvRange<Building>),
    /// Window closed: `range` is frozen and re-emitted until the next session opens.
    Closed(OvernightOhlcvRange<Closed>),
}

impl OvernightOhlcvRangeStatus {
    /// The frozen range, available once the session window has closed.
    fn value(&self) -> Option<OvernightRangeOhlcvData> {
        use OvernightOhlcvRangeStatus::*;
        match self {
            Closed(r) => Some(r.state.range),
            Awaiting(_) | Building(_) => None,
        }
    }

    fn update(self, ohlcv: Ohlcv) -> OvernightOhlcvRangeStatus {
        use OvernightOhlcvRangeStatus::*;
        match self {
            Awaiting(r) => r.update(ohlcv),
            Building(r) => r.update(ohlcv),
            Closed(r) => r.update(ohlcv),
        }
    }

    fn reset(self) -> OvernightOhlcvRangeStatus {
        use OvernightOhlcvRangeStatus::*;
        match self {
            Awaiting(r) => Awaiting(r.reset()),
            Building(r) => Awaiting(r.reset()),
            Closed(r) => Awaiting(r.reset()),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StreamingOvernightOhlcvRange {
    status: OvernightOhlcvRangeStatus,
}

impl StreamingOvernightOhlcvRange {
    pub fn new(window: SessionWindow, vwap_source: VwapPriceSource) -> Self {
        Self {
            status: OvernightOhlcvRangeStatus::Awaiting(OvernightOhlcvRange::new(
                window,
                vwap_source,
            )),
        }
    }
}

impl StreamingIndicator for StreamingOvernightOhlcvRange {
    type Input = Ohlcv;
    type Output<'a> = Option<OvernightRangeOhlcvData>;

    fn update(&mut self, ohlcv: Ohlcv) -> Self::Output<'_> {
        self.status = self.status.update(ohlcv);
        self.status.value()
    }

    fn reset(&mut self) {
        self.status = self.status.reset();
    }
}

// ================================================================================================
// Trades Session Range
// ================================================================================================

/// Frozen overnight/session range built from [`TradeEvent`]s.
///
/// Trades are point data, so there is no high-vs-close distinction: `high`/`low`
/// are the price envelope and there are no separate close extremes.
#[derive(Debug, Clone, Copy, PartialEq)]
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
#[derive(Debug, Clone)]
pub struct StreamingOvernightTradesRange {
    window: SessionWindow,
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
    type Output<'a> = Option<OvernightRangeTradesData>;

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

// ================================================================================================
// Helper Functions
// ================================================================================================

fn hm(h: u32, m: u32) -> NaiveTime {
    NaiveTime::from_hms_opt(h, m, 0).expect("invalid hour or minute")
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_session_hours() {
        let us = SessionWindow::us_core_session();
        assert_eq!(us.timezone, Tz::America__New_York);
        assert_eq!(us.start, hm(9, 30));
        assert_eq!(us.end, hm(16, 0));

        let london = SessionWindow::london_core_session();
        assert_eq!(london.timezone, Tz::Europe__London);
        assert_eq!(london.start, hm(8, 0));
        assert_eq!(london.end, hm(16, 30));

        let overlap = SessionWindow::us_europe_overlap();
        assert_eq!(overlap.timezone, Tz::Europe__London);
        assert_eq!(overlap.start, hm(13, 0));
        assert_eq!(overlap.end, hm(17, 0));

        let sg = SessionWindow::singapore_core_session();
        assert_eq!(sg.timezone, Tz::Asia__Singapore);
        assert_eq!(sg.start, hm(9, 0));
        assert_eq!(sg.end, hm(17, 0));

        let sydney = SessionWindow::sydney_core_session();
        assert_eq!(sydney.timezone, Tz::Australia__Sydney);
        assert_eq!(sydney.start, hm(10, 0));
        assert_eq!(sydney.end, hm(16, 0));

        let us_overnight = SessionWindow::us_extended_overnight();
        assert_eq!(us_overnight.timezone, Tz::America__New_York);
        assert_eq!(us_overnight.start, hm(18, 0));
        assert_eq!(us_overnight.end, hm(9, 30));

        let tokyo = SessionWindow::tokyo_core_session();
        assert_eq!(tokyo.timezone, Tz::Asia__Tokyo);
        assert_eq!(tokyo.start, hm(9, 0));
        assert_eq!(tokyo.end, hm(15, 0));

        let asia = SessionWindow::asia_institutional_core();
        assert_eq!(asia.timezone, Tz::Asia__Singapore);
        assert_eq!(asia.start, hm(9, 0));
        assert_eq!(asia.end, hm(17, 0));

        let hk = SessionWindow::hong_kong_core_session();
        assert_eq!(hk.timezone, Tz::Asia__Hong_Kong);
        assert_eq!(hk.start, hm(9, 30));
        assert_eq!(hk.end, hm(16, 0));

        let apac = SessionWindow::apac_overnight();
        assert_eq!(apac.timezone, Tz::Asia__Singapore);
        assert_eq!(apac.start, hm(17, 0));
        assert_eq!(apac.end, hm(8, 0));
    }
}
