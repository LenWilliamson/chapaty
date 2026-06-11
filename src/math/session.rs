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
    Intraday,
    Overnight,
}

/// Identifies one accumulation session by its anchor date.
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
    pub fn new(timezone: Tz, start: NaiveTime, end: NaiveTime) -> Self {
        Self {
            timezone,
            start,
            end,
        }
    }

    pub fn us_core_session() -> Self {
        SessionWindow::new(Tz::America__New_York, hm(9, 30), hm(16, 0))
    }
    pub fn london_core_session() -> Self {
        SessionWindow::new(Tz::Europe__London, hm(8, 0), hm(16, 30))
    }
    pub fn us_europe_overlap() -> Self {
        SessionWindow::new(Tz::Europe__London, hm(13, 0), hm(17, 0))
    }
    pub fn singapore_core_session() -> Self {
        SessionWindow::new(Tz::Asia__Singapore, hm(9, 0), hm(17, 0))
    }
    pub fn sydney_core_session() -> Self {
        SessionWindow::new(Tz::Australia__Sydney, hm(10, 0), hm(16, 0))
    }
    pub fn us_extended_overnight() -> Self {
        SessionWindow::new(Tz::America__New_York, hm(18, 0), hm(9, 30))
    }
    pub fn tokyo_core_session() -> Self {
        SessionWindow::new(Tz::Asia__Tokyo, hm(9, 0), hm(15, 0))
    }
    pub fn asia_institutional_core() -> Self {
        SessionWindow::new(Tz::Asia__Singapore, hm(9, 0), hm(17, 0))
    }
    pub fn hong_kong_core_session() -> Self {
        SessionWindow::new(Tz::Asia__Hong_Kong, hm(9, 30), hm(16, 0))
    }
    pub fn apac_overnight() -> Self {
        SessionWindow::new(Tz::Asia__Singapore, hm(17, 0), hm(8, 0))
    }

    fn window_kind(&self) -> WindowKind {
        match self.start.cmp(&self.end) {
            Ordering::Less => WindowKind::Intraday,
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

fn hm(h: u32, m: u32) -> NaiveTime {
    NaiveTime::from_hms_opt(h, m, 0).expect("invalid hour or minute")
}

// ================================================================================================
// Typestate
// ================================================================================================

trait RangeState {}

#[derive(Debug, Clone, Copy)]
struct Awaiting;
impl RangeState for Awaiting {}

#[derive(Debug, Clone, Copy)]
struct Building<R> {
    range: R,
}
impl<R: Range> RangeState for Building<R> {}

#[derive(Debug, Clone, Copy)]
struct Closed<R> {
    range: R,
}
impl<R: Range> RangeState for Closed<R> {}

/// Deterministic outcome of processing one event.
enum TransitionOutcome<Status, R> {
    Progress(Status),
    Completed {
        next_status: Status,
        completed_range: R,
    },
}

/// The one thing each event family must supply: how to open a session from its
/// first event and how to fold subsequent in-session events in. Everything else
/// (timing, transitions, completion, caching, reset) is the shared.
trait Range: Debug + Copy + Send + Sync {
    type Indicator: StreamingIndicator;
    type Event: MarketEvent;

    fn session_date(&self) -> SessionDate;

    fn open(
        session: SessionDate,
        indicator: Self::Indicator,
        event: Self::Event,
    ) -> (Self::Indicator, Self);

    fn fold(self, indicator: Self::Indicator, event: Self::Event) -> (Self::Indicator, Self);
}

// ================================================================================================
// Generic Finite State Machine
// ================================================================================================

#[derive(Debug)]
struct OvernightRange<R: Range, S: RangeState> {
    window: SessionWindow,
    indicator: R::Indicator,
    state: S,
}

impl<R: Range, S: RangeState> OvernightRange<R, S> {
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

    fn reset(mut self) -> OvernightRange<R, Awaiting> {
        self.indicator.reset();
        self.map(|_| Awaiting)
    }

    fn into_building(
        self,
        session: SessionDate,
        event: R::Event,
    ) -> OvernightRange<R, Building<R>> {
        let OvernightRange {
            window,
            mut indicator,
            ..
        } = self;
        indicator.reset();
        let (indicator, range) = R::open(session, indicator, event);
        OvernightRange {
            window,
            indicator,
            state: Building { range },
        }
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

impl<R: Range> OvernightRange<R, Building<R>> {
    fn close(self) -> OvernightRange<R, Closed<R>> {
        self.map(|s| Closed { range: s.range })
    }

    fn update(self, event: R::Event) -> TransitionOutcome<OvernightRangeStatus<R>, R> {
        match self.window.classify(event.point_in_time()) {
            WindowPosition::Within(current_session) => {
                if current_session == self.state.range.session_date() {
                    let OvernightRange {
                        window,
                        indicator,
                        state,
                    } = self;
                    let (indicator, range) = state.range.fold(indicator, event);
                    let next = OvernightRange {
                        window,
                        indicator,
                        state: Building { range },
                    };
                    TransitionOutcome::Progress(OvernightRangeStatus::Building(next))
                } else {
                    let completed_range = self.state.range;
                    let next = self.into_building(current_session, event);
                    TransitionOutcome::Completed {
                        next_status: OvernightRangeStatus::Building(next),
                        completed_range,
                    }
                }
            }
            WindowPosition::Outside => {
                let completed_range = self.state.range;
                let closed = self.close();
                TransitionOutcome::Completed {
                    next_status: OvernightRangeStatus::Closed(closed),
                    completed_range,
                }
            }
        }
    }
}

#[derive(Debug)]
enum OvernightRangeStatus<R: Range> {
    Awaiting(OvernightRange<R, Awaiting>),
    Building(OvernightRange<R, Building<R>>),
    Closed(OvernightRange<R, Closed<R>>),
}

impl<R: Range> OvernightRangeStatus<R> {
    fn update(self, event: R::Event) -> TransitionOutcome<Self, R> {
        match self {
            OvernightRangeStatus::Awaiting(r) => r.update(event),
            OvernightRangeStatus::Building(r) => r.update(event),
            OvernightRangeStatus::Closed(r) => r.update(event),
        }
    }

    fn reset(self) -> Self {
        match self {
            OvernightRangeStatus::Awaiting(r) => OvernightRangeStatus::Awaiting(r.reset()),
            OvernightRangeStatus::Building(r) => OvernightRangeStatus::Awaiting(r.reset()),
            OvernightRangeStatus::Closed(r) => OvernightRangeStatus::Awaiting(r.reset()),
        }
    }
}

#[derive(Debug)]
struct StreamingOvernightRange<R: Range> {
    /// Always `Some` between calls. Wrapped in `Option` so the by-value typestate
    /// transition can be moved out from behind `&mut self` via `take()` — this is
    /// what replaces the old reliance on `Copy` now that the indicator may not be.
    status: Option<OvernightRangeStatus<R>>,
    /// Caches the most recently completed session so it isn't lost in 24/7 markets.
    last_completed_session: Option<R>,
}

impl<R: Range> StreamingOvernightRange<R> {
    pub fn last_completed_session(&self) -> Option<R> {
        self.last_completed_session
    }
}

impl<R: Range> StreamingIndicator for StreamingOvernightRange<R> {
    type Input = R::Event;
    type Output<'a>
        = Option<R>
    where
        R: 'a;

    fn update(&mut self, event: Self::Input) -> Self::Output<'_> {
        let status = self
            .status
            .take()
            .expect("status is always present between update calls");

        match status.update(event) {
            TransitionOutcome::Progress(next) => self.status = Some(next),
            TransitionOutcome::Completed {
                next_status,
                completed_range,
            } => {
                self.status = Some(next_status);
                self.last_completed_session = Some(completed_range);
            }
        }
        self.last_completed_session
    }

    fn reset(&mut self) {
        let status = self
            .status
            .take()
            .expect("status is always present between update calls");
        self.status = Some(status.reset());
        self.last_completed_session = None;
    }
}

// ================================================================================================
// OHLCV session
// ================================================================================================

pub type StreamingOvernightOhlcvRange = StreamingOvernightRange<OhlcvSessionData>;

impl StreamingOvernightOhlcvRange {
    pub fn new(window: SessionWindow, indicator: StreamingOhlcvVwap) -> Self {
        Self {
            status: Some(OvernightRangeStatus::Awaiting(OvernightRange::new(
                window, indicator,
            ))),
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
    /// Session date of the data.
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
    /// Highest bar close over the window.
    pub fn highest_close(&self) -> Price {
        self.highest_close
    }
    /// Lowest bar close over the window.
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
// Trades session
// ================================================================================================

pub type StreamingOvernightTradesRange = StreamingOvernightRange<TradesSessionData>;

impl StreamingOvernightTradesRange {
    pub fn new(window: SessionWindow, indicator: StreamingTradesVwap) -> Self {
        Self {
            status: Some(OvernightRangeStatus::Awaiting(OvernightRange::new(
                window, indicator,
            ))),
            last_completed_session: None,
        }
    }
}

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
    /// Session date of the data.
    pub fn session(&self) -> SessionDate {
        self.session
    }
    /// Highest trade price over the window.
    pub fn high(&self) -> Price {
        self.high
    }
    /// Lowest trade price over the window.
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
