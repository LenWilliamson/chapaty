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
        volatility::{StreamingOhlcvVwap, StreamingTradesVwap},
    },
};

// ================================================================================================
// OHLCV Session
// ================================================================================================

pub type StreamingOvernightOhlcvRange =
    StreamingOvernightRange<OhlcvSessionData, StreamingOhlcvVwap>;

impl StreamingOvernightOhlcvRange {
    pub fn new(window: SessionWindow, indicator: StreamingOhlcvVwap) -> Self {
        Self {
            status: OvernightRangeStatus::Awaiting(OvernightRange::new(window, indicator)),
            last_completed_session: None,
        }
    }
}

/// Frozen overnight/session range built from [`Ohlcv`] bars.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OhlcvSessionData {
    session: SessionDate,
    high: Price,
    low: Price,
    highest_close: Price,
    lowest_close: Price,
    volume: Volume,
    vwap: Option<Price>,
}

impl Range for OhlcvSessionData {
    type Indicator = StreamingOhlcvVwap;
    type Event = Ohlcv;

    fn session_date(&self) -> SessionDate {
        self.session
    }

    fn open(
        session: SessionDate,
        mut indicator: Self::Indicator,
        event: Self::Event,
    ) -> (Self::Indicator, Self) {
        indicator.reset();
        let vwap = indicator.update(event).map(Price);
        let new_self = Self {
            session,
            high: event.high,
            low: event.low,
            highest_close: event.close,
            lowest_close: event.close,
            volume: event.volume,
            vwap,
        };
        (indicator, new_self)
    }

    fn fold(self, mut indicator: Self::Indicator, event: Self::Event) -> (Self::Indicator, Self) {
        let vwap = indicator.update(event).map(Price);
        let new_self = Self {
            session: self.session,
            high: self.high.max(event.high),
            low: self.low.min(event.low),
            highest_close: self.highest_close.max(event.close),
            lowest_close: self.lowest_close.min(event.close),
            volume: self.volume + event.volume,
            vwap,
        };
        (indicator, new_self)
    }
}

impl OhlcvSessionData {
    /// Highest ohlcv bar high over the window.
    pub fn high(&self) -> Price {
        self.high
    }

    /// Lowest ohlcv bar low over the window.
    pub fn low(&self) -> Price {
        self.low
    }

    /// Highest ohlcv bar close over the window.
    pub fn highest_close(&self) -> Price {
        self.highest_close
    }

    /// Lowest ohlcv bar close over the window.
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

// ================================================================================================
// Trades Session
// ================================================================================================

pub type StreamingOvernightTradesRange =
    StreamingOvernightRange<TradesSessionData, StreamingTradesVwap>;

/// Frozen overnight/session range built from [`TradeEvent`]s.
///
/// Trades are point data, so there is no high-vs-close distinction: `high`/`low`
/// are the price envelope and there are no separate close extremes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TradesSessionData {
    session: SessionDate,
    high: Price,
    low: Price,
    volume: Volume,
    vwap: Option<Price>,
}

impl Range for TradesSessionData {
    type Indicator = StreamingTradesVwap;
    type Event = TradeEvent;

    fn session_date(&self) -> SessionDate {
        self.session
    }

    fn open(
        session: SessionDate,
        mut indicator: Self::Indicator,
        event: Self::Event,
    ) -> (Self::Indicator, Self) {
        let vwap = indicator.update(event).map(Price);
        let new_self = Self {
            session,
            high: event.price,
            low: event.price,
            volume: event.quantity,
            vwap,
        };
        (indicator, new_self)
    }

    fn fold(self, mut indicator: Self::Indicator, event: Self::Event) -> (Self::Indicator, Self) {
        let vwap = indicator.update(event).map(Price);
        let new_self = Self {
            session: self.session,
            high: self.high.max(event.price),
            low: self.low.min(event.price),
            volume: self.volume + event.quantity,
            vwap,
        };
        (indicator, new_self)
    }
}

impl TradesSessionData {
    /// Highest ohlcv bar high over the window.
    pub fn high(&self) -> Price {
        self.high
    }

    /// Lowest ohlcv bar low over the window.
    pub fn low(&self) -> Price {
        self.low
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

// ================================================================================================
// Session Window
// ================================================================================================

/// Where an event falls relative to an accumulation window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowPosition {
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
    pub timezone: Tz,
    pub start: NaiveTime,
    pub end: NaiveTime,
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
}

impl SessionWindow {
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
// Shared Typestate Definitions
// ================================================================================================

trait RangeState {}

#[derive(Debug, Clone, Copy)]
struct Awaiting;

impl RangeState for Awaiting {}

#[derive(Debug, Clone, Copy)]
struct Building<SessionRangeData, Indicator> {
    indicator: Indicator,
    range: SessionRangeData,
}

impl<SessionRangeData, Indicator> RangeState for Building<SessionRangeData, Indicator>
where
    SessionRangeData: Range,
    Indicator: StreamingIndicator,
{
}

#[derive(Debug, Clone, Copy)]
struct Closed<SessionRangeData> {
    range: SessionRangeData,
}

impl<SessionRangeData> RangeState for Closed<SessionRangeData> where SessionRangeData: Range {}

/// Represents the deterministic outcome of processing data.
enum TransitionOutcome<Status, SessionRangeData> {
    Progress(Status),
    Completed {
        next_status: Status,
        completed_range: SessionRangeData,
    },
}

trait Range: Debug + Send + Sync {
    type Indicator: StreamingIndicator;
    type Event: MarketEvent;

    fn session_date(&self) -> SessionDate;
    fn open(session: SessionDate, event: Self::Event) -> (Self::Indicator, Self);
    fn fold(self, indicator: Self::Indicator, event: Self::Event) -> (Self::Indicator, Self);
}

struct OvernightRange<R: Range, S: RangeState> {
    window: SessionWindow,
    indicator: R::Indicator,
    state: S,
}

impl<R, S> OvernightRange<R, S>
where
    R: Range,
    S: RangeState,
{
    fn map<F, NewState>(self, f: F) -> OvernightRange<R, NewState>
    where
        F: FnOnce(S) -> NewState,
        NewState: RangeState,
    {
        OvernightRange {
            window: self.window,
            indicator: self.indicator,
            state: f(self.state),
        }
    }

    fn reset(self) -> OvernightRange<R, Awaiting> {
        self.map(|_| Awaiting)
    }

    fn into_building(
        self,
        session: SessionDate,
        event: R::Event,
    ) -> OvernightRange<R, Building<R, R::Indicator>> {
        self.map(|_| {
            let (indicator, range) = R::open(session, event);
            Building { indicator, range }
        })
    }
}

impl<R: Range> OvernightRange<R, Awaiting> {
    fn new(window: SessionWindow, mut indicator: R::Indicator) -> Self {
        indicator.reset();
        Self {
            window,
            indicator,
            state: Awaiting,
        }
    }

    fn update(self, event: R::Event) -> TransitionOutcome<OvernightRangeStatus<R>, R> {
        match self.window.classify(event.point_in_time()) {
            WindowPosition::Within(session) => TransitionOutcome::Progress(
                OvernightRangeStatus::Building(self.into_building(session, event)),
            ),
            WindowPosition::Outside => {
                TransitionOutcome::Progress(OvernightRangeStatus::Awaiting(self))
            }
        }
    }
}

impl<R: Range> OvernightRange<R, Closed<R>> {
    fn update(self, event: R::Event) -> TransitionOutcome<OvernightRangeStatus<R>, R> {
        match self.window.classify(event.point_in_time()) {
            WindowPosition::Within(session) => TransitionOutcome::Progress(
                OvernightRangeStatus::Building(self.into_building(session, event)),
            ),
            WindowPosition::Outside => {
                TransitionOutcome::Progress(OvernightRangeStatus::Closed(self))
            }
        }
    }
}

impl<R: Range> OvernightRange<R, Building<R, R::Indicator>> {
    fn close(self) -> OvernightRange<R, Closed<R>> {
        self.map(|s| Closed { range: s.range })
    }

    fn update(self, event: R::Event) -> TransitionOutcome<OvernightRangeStatus<R>, R> {
        match self.window.classify(event.point_in_time()) {
            WindowPosition::Within(current_session) => {
                if current_session == self.state.range.session_date() {
                    let (new_indicator, new_range) =
                        self.state.range.fold(self.state.indicator, event);
                    let new_state = self.map(|_| Building {
                        indicator: new_indicator,
                        range: new_range,
                    });
                    TransitionOutcome::Progress(OvernightRangeStatus::Building(new_state))
                } else {
                    let completed_range = self.state.range;
                    let new_state = self.into_building(current_session, event);
                    TransitionOutcome::Completed {
                        next_status: OvernightRangeStatus::Building(new_state),
                        completed_range,
                    }
                }
            }
            WindowPosition::Outside => {
                let closed_state = self.close();
                let completed_range = closed_state.state.range;
                TransitionOutcome::Completed {
                    next_status: OvernightRangeStatus::Closed(closed_state),
                    completed_range,
                }
            }
        }
    }
}

enum OvernightRangeStatus<R: Range> {
    Awaiting(OvernightRange<R, Awaiting>),
    Building(OvernightRange<R, Building<R, R::Indicator>>),
    Closed(OvernightRange<R, Closed<R>>),
}

#[derive(Debug)]
struct StreamingOvernightRange<R: Range> {
    status: OvernightRangeStatus<R>,
    last_completed_session: Option<R>,
}

impl<R: Range> StreamingIndicator for StreamingOvernightRange<R> {
    type Input = R::Event;
    type Output<'a> = Option<R> where R: 'a;

    fn update(&mut self, event: Self::Input) -> Self::Output<'_> {
        match self.status.update(event) {
            TransitionOutcome::Progress(s) => self.status = s,
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

// ================================================================================================
// Session State Machine
// ================================================================================================

fn hm(h: u32, m: u32) -> NaiveTime {
    NaiveTime::from_hms_opt(h, m, 0).expect("invalid hour or minute")
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, TimeZone, Utc};
    use chrono_tz::America::New_York;

    /// Helper to easily construct a UTC timestamp from a local timezone date and time.
    fn local_to_utc(
        tz: Tz,
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        min: u32,
        sec: u32,
    ) -> DateTime<Utc> {
        tz.with_ymd_and_hms(year, month, day, hour, min, sec)
            .unwrap()
            .with_timezone(&Utc)
    }

    /// Helper to easily create a NaiveDate
    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).unwrap()
    }
    #[test]
    fn test_intraday_classify() {
        let session = SessionWindow::us_core_session(); // 09:30 -> 16:00 ET

        let test_date = date(2026, 6, 10);
        let expected_session = WindowPosition::Within(SessionDate(test_date));

        // 1. Before session starts (09:29:59) -> Outside
        let before = local_to_utc(New_York, 2026, 6, 10, 9, 29, 59);
        assert_eq!(session.classify(before), WindowPosition::Outside);

        // 2. Exactly at start (09:30:00) -> [inclusive boundary] -> Within
        let start = local_to_utc(New_York, 2026, 6, 10, 9, 30, 0);
        assert_eq!(session.classify(start), expected_session);

        // 3. Inside session (12:00:00) -> Within
        let mid = local_to_utc(New_York, 2026, 6, 10, 12, 0, 0);
        assert_eq!(session.classify(mid), expected_session);

        // 4. Exactly at end (16:00:00) -> [exclusive boundary] -> Outside
        let end = local_to_utc(New_York, 2026, 6, 10, 16, 0, 0);
        assert_eq!(session.classify(end), WindowPosition::Outside);

        // 5. After session ends (16:00:01) -> Outside
        let after = local_to_utc(New_York, 2026, 6, 10, 16, 0, 1);
        assert_eq!(session.classify(after), WindowPosition::Outside);
    }

    #[test]
    fn test_overnight_classify() {
        let session = SessionWindow::us_extended_overnight(); // 18:00 -> 09:30 ET

        let anchor = date(2026, 6, 10); // The evening the session opened
        let expected_session = WindowPosition::Within(SessionDate(anchor));

        // 1. Just before the evening leg opens (17:59:59) -> Outside
        let before_eve = local_to_utc(New_York, 2026, 6, 10, 17, 59, 59);
        assert_eq!(session.classify(before_eve), WindowPosition::Outside);

        // 2. Evening leg opens (18:00:00 on June 10) -> Within(June 10)
        let eve_start = local_to_utc(New_York, 2026, 6, 10, 18, 0, 0);
        assert_eq!(session.classify(eve_start), expected_session);

        // 3. Evening leg middle (23:00:00 on June 10) -> Within(June 10)
        let eve_mid = local_to_utc(New_York, 2026, 6, 10, 23, 0, 0);
        assert_eq!(session.classify(eve_mid), expected_session);

        // 4. Cross midnight / Morning leg begins (00:00:00 on June 11) -> Within(June 10)
        let morn_start = local_to_utc(New_York, 2026, 6, 11, 0, 0, 0);
        assert_eq!(session.classify(morn_start), expected_session);

        // 5. Morning leg middle (08:00:00 on June 11) -> Within(June 10)
        let morn_mid = local_to_utc(New_York, 2026, 6, 11, 8, 0, 0);
        assert_eq!(session.classify(morn_mid), expected_session);

        // 6. Morning leg ends (09:30:00 on June 11) -> [exclusive boundary] -> Outside
        let morn_end = local_to_utc(New_York, 2026, 6, 11, 9, 30, 0);
        assert_eq!(session.classify(morn_end), WindowPosition::Outside);

        // 7. Middle of the daytime (12:00:00 on June 11) -> Outside
        let day_mid = local_to_utc(New_York, 2026, 6, 11, 12, 0, 0);
        assert_eq!(session.classify(day_mid), WindowPosition::Outside);
    }

    #[test]
    fn test_24_hour_classify() {
        // A 24-hour window wrapping at 17:00 (start == end implies Overnight logic)
        let session = SessionWindow::new(New_York, hm(17, 0), hm(17, 0));

        let anchor = date(2026, 6, 10);
        let expected_session = WindowPosition::Within(SessionDate(anchor));

        // 1. Right at start (17:00 on June 10) -> Anchored on June 10
        let start = local_to_utc(New_York, 2026, 6, 10, 17, 0, 0);
        assert_eq!(session.classify(start), expected_session);

        // 2. Late evening (23:59 on June 10) -> Anchored on June 10
        let eve = local_to_utc(New_York, 2026, 6, 10, 23, 59, 0);
        assert_eq!(session.classify(eve), expected_session);

        // 3. Next morning (08:00 on June 11) -> Anchored on June 10
        let morn = local_to_utc(New_York, 2026, 6, 11, 8, 0, 0);
        assert_eq!(session.classify(morn), expected_session);

        // 4. Right before wrap (16:59:59 on June 11) -> Anchored on June 10
        let before_wrap = local_to_utc(New_York, 2026, 6, 11, 16, 59, 59);
        assert_eq!(session.classify(before_wrap), expected_session);

        // 5. Window wraps/restarts (17:00:00 on June 11) -> Now anchored on June 11
        let next_anchor = date(2026, 6, 11);
        let next_expected_session = WindowPosition::Within(SessionDate(next_anchor));
        let restart = local_to_utc(New_York, 2026, 6, 11, 17, 0, 0);
        assert_eq!(session.classify(restart), next_expected_session);
    }
}
