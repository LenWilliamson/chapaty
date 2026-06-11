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
    type Indicator: StreamingIndicator + Clone;
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

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
struct StreamingOvernightRange<R: Range> {
    status: OvernightRangeStatus<R>,
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
        match self.status.clone().update(event) {
            TransitionOutcome::Progress(next) => self.status = next,
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
        self.status = self.status.clone().reset();
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
            status: OvernightRangeStatus::Awaiting(OvernightRange::new(window, indicator)),
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

    // ============================================================================================
    // Finite State Machine Test Harness
    // ============================================================================================

    /// Counts how many times it was updated / reset.
    #[derive(Debug, Clone, Default)]
    struct CountingIndicator {
        updates: u32,
        resets: u32,
        seen: Vec<i64>,
    }

    impl StreamingIndicator for CountingIndicator {
        type Input = FakeEvent;
        type Output<'a> = Option<u32>;

        fn update(&mut self, input: Self::Input) -> Self::Output<'_> {
            self.updates += 1;
            self.seen.push(input.value);
            Some(self.updates)
        }

        fn reset(&mut self) {
            self.updates = 0;
            self.resets += 1;
            self.seen.clear();
        }
    }

    /// Smallest possible event: a timestamp + an integer payload.
    #[derive(Debug, Clone, Copy)]
    struct FakeEvent {
        ts: DateTime<Utc>,
        value: i64,
    }

    impl MarketEvent for FakeEvent {
        fn point_in_time(&self) -> DateTime<Utc> {
            self.ts
        }
    }

    /// Whether the indicator handed to [`Range::open`] had been reset before use.
    ///
    /// The machine's contract is that `open` always receives a fresh indicator.
    /// [`OvernightRange::into_building`] calls `reset` immediately before delegating
    /// to `open`. This enum lets the mock range record which case it actually saw,
    /// so tests can assert the machine upholds that contract rather than assuming it.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum IndicatorFreshness {
        /// The indicator had zero accumulated updates: the machine reset it first.
        Fresh,
        /// The indicator still carried state from a prior session: reset was skipped.
        Carried,
    }

    impl IndicatorFreshness {
        /// Derives freshness from the number of updates an indicator has accumulated.
        fn from_update_count(updates: u32) -> Self {
            if updates == 0 {
                Self::Fresh
            } else {
                Self::Carried
            }
        }

        /// True if the indicator was reset before `open` (zero accumulated updates).
        fn is_fresh(&self) -> bool {
            matches!(self, Self::Fresh)
        }

        /// True if the indicator still carried prior-session state into `open`.
        fn is_carried(&self) -> bool {
            matches!(self, Self::Carried)
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq)]
    struct FakeRange {
        session: SessionDate,
        sum: i64,
        folds: u32,
        /// Records the freshness of the indicator that [`Range::open`] received,
        /// so tests can verify the machine reset it before opening a session.
        opened: IndicatorFreshness,
    }

    impl Range for FakeRange {
        type Indicator = CountingIndicator;
        type Event = FakeEvent;

        fn session_date(&self) -> SessionDate {
            self.session
        }

        fn open(
            session: SessionDate,
            mut indicator: Self::Indicator,
            event: Self::Event,
        ) -> (Self::Indicator, Self) {
            // The machine should have reset the indicator before handing it over.
            let opened = IndicatorFreshness::from_update_count(indicator.updates);
            indicator.update(event);
            let range = FakeRange {
                session,
                sum: event.value,
                folds: 0,
                opened,
            };
            (indicator, range)
        }

        fn fold(
            self,
            mut indicator: Self::Indicator,
            event: Self::Event,
        ) -> (Self::Indicator, Self) {
            indicator.update(event);
            let range = FakeRange {
                session: self.session,
                sum: self.sum + event.value,
                folds: self.folds + 1,
                opened: self.opened,
            };
            (indicator, range)
        }
    }

    /// Build a finite state machine over the mock range with a fresh counting indicator.
    fn fsm(window: SessionWindow) -> StreamingOvernightRange<FakeRange> {
        StreamingOvernightRange {
            status: OvernightRangeStatus::Awaiting(OvernightRange::new(
                window,
                CountingIndicator::default(),
            )),
            last_completed_session: None,
        }
    }

    /// 24h window (start == end)is never `Outside`, so the ONLY way a range
    /// completes is a session rollover.
    fn fsm_24h() -> StreamingOvernightRange<FakeRange> {
        fsm(SessionWindow::new(New_York, hm(17, 0), hm(17, 0)))
    }

    /// Year is fixed to 2026.
    fn ev(month: u32, day: u32, hour: u32, min: u32, value: i64) -> FakeEvent {
        FakeEvent {
            ts: local_to_utc(New_York, 2026, month, day, hour, min, 0),
            value,
        }
    }

    fn session(month: u32, day: u32) -> SessionDate {
        SessionDate(date(2026, month, day))
    }

    fn status_name(m: &StreamingOvernightRange<FakeRange>) -> &'static str {
        match m.status {
            OvernightRangeStatus::Awaiting(_) => "awaiting",
            OvernightRangeStatus::Building(_) => "building",
            OvernightRangeStatus::Closed(_) => "closed",
        }
    }

    /// Peek the live, in-progress range without completing it.
    fn building_range(m: &StreamingOvernightRange<FakeRange>) -> FakeRange {
        match m.status.clone() {
            OvernightRangeStatus::Building(r) => r.state.range,
            other => panic!("expected Building, got {other:?}"),
        }
    }

    #[test]
    fn open_reports_carried_when_indicator_was_not_reset() {
        // Check that open() reports Carried when given an indicator that still has
        // state. This confirms is_fresh()/is_carried() actually distinguish the two
        // cases, so the Fresh checks in the other tests aren't passing for free.
        let mut dirty = CountingIndicator::default();
        dirty.update(ev(6, 10, 9, 0, 1)); // give it some state, don't reset it

        let (_indicator, range) = FakeRange::open(session(6, 10), dirty, ev(6, 10, 10, 0, 5));

        assert!(
            range.opened.is_carried(),
            "open must report Carried when handed an un-reset indicator"
        );
    }

    // ============================================================================================
    // Finite State Machine Transition Test
    // ============================================================================================

    #[test]
    fn awaiting_ignores_events_before_any_session_opens() {
        let mut m = fsm(SessionWindow::us_core_session()); // 09:30–16:00 ET

        // 08:00 and 09:00 are before the window: stay Awaiting, complete nothing.
        assert_eq!(m.update(ev(6, 10, 8, 0, 1)), None);
        assert_eq!(status_name(&m), "awaiting");
        assert_eq!(m.update(ev(6, 10, 9, 0, 1)), None);
        assert_eq!(status_name(&m), "awaiting");
    }

    #[test]
    fn opens_on_first_in_session_event_then_folds() {
        let mut m = fsm(SessionWindow::us_core_session());

        // First in-window event opens the session; nothing completed yet.
        assert_eq!(m.update(ev(6, 10, 10, 0, 5)), None);
        assert_eq!(status_name(&m), "building");
        let r = building_range(&m);
        assert_eq!(r.session, session(6, 10));
        assert_eq!(r.sum, 5);
        assert_eq!(r.folds, 0);
        assert!(
            r.opened.is_fresh(),
            "indicator should be reset when the first session opens"
        );

        // Same session: fold in place.
        assert_eq!(m.update(ev(6, 10, 11, 0, 3)), None);
        assert_eq!(status_name(&m), "building");
        let r = building_range(&m);
        assert_eq!(r.sum, 8);
        assert_eq!(r.folds, 1);
        assert!(
            r.opened.is_fresh(),
            "freshness is set once at open and must persist across folds"
        );
    }

    #[test]
    fn completes_when_leaving_the_window_and_caches_result() {
        let mut m = fsm(SessionWindow::us_core_session());

        m.update(ev(6, 10, 10, 0, 5));
        m.update(ev(6, 10, 11, 0, 3)); // sum = 8, folds = 1

        // Leaving the window completes the range and returns the frozen copy.
        let completed = m
            .update(ev(6, 10, 17, 0, 99))
            .expect("should complete on exit");
        assert_eq!(
            completed,
            FakeRange {
                session: session(6, 10),
                sum: 8,
                folds: 1,
                opened: IndicatorFreshness::Fresh,
            }
        );
        assert_eq!(status_name(&m), "closed");

        // Still outside: the cached range is returned verbatim — nothing re-opened,
        // re-folded, or mutated. Assert the whole value, not just one field.
        let again = m
            .update(ev(6, 10, 18, 0, 99))
            .expect("cache should persist");
        assert_eq!(
            again,
            FakeRange {
                session: session(6, 10),
                sum: 8,
                folds: 1,
                opened: IndicatorFreshness::Fresh,
            }
        );
        assert_eq!(status_name(&m), "closed");
    }

    #[test]
    fn closed_reopens_a_new_session_next_day_without_losing_the_cache() {
        let mut m = fsm(SessionWindow::us_core_session());

        m.update(ev(6, 10, 10, 0, 5));
        m.update(ev(6, 10, 17, 0, 99)); // complete 6/10, now Closed, cache = sum 5
        assert_eq!(status_name(&m), "closed");

        // Next day, in-window: opening a new session is Progress, not Completed,
        // so the returned cache is still the previous completed session (6/10),
        // returned verbatim — assert the whole frozen value, not just its date.
        let out = m
            .update(ev(6, 11, 10, 0, 7))
            .expect("cache survives reopen");
        assert_eq!(
            out,
            FakeRange {
                session: session(6, 10),
                sum: 5,
                folds: 0,
                opened: IndicatorFreshness::Fresh,
            }
        );
        assert_eq!(status_name(&m), "building");

        // The newly opened session is fresh and independent of the cached one.
        let r = building_range(&m);
        assert_eq!(
            r,
            FakeRange {
                session: session(6, 11),
                sum: 7,
                folds: 0,
                opened: IndicatorFreshness::Fresh,
            }
        );
        assert!(
            r.opened.is_fresh(),
            "indicator must be reset when reopening into a new session"
        );
    }

    #[test]
    fn rolls_straight_into_next_session_on_a_24h_window() {
        // No `Outside` ever fires here, so a session change is the only completer.
        let mut m = fsm_24h();

        m.update(ev(6, 10, 18, 0, 5)); // opens anchor 6/10 (18:00 >= 17:00)
        // 16:00 next day is still anchor 6/10 (morning leg, < 17:00 end).
        assert_eq!(m.update(ev(6, 11, 16, 0, 3)), None);
        assert_eq!(status_name(&m), "building");

        // 17:00 the next day rolls the anchor to 6/11: completes 6/10, opens 6/11.
        let completed = m
            .update(ev(6, 11, 17, 0, 9))
            .expect("rollover completes prior session");
        assert_eq!(
            completed,
            FakeRange {
                session: session(6, 10),
                sum: 8,
                folds: 1,
                opened: IndicatorFreshness::Fresh,
            }
        );

        // The next session is already building (no Awaiting gap), and the rollover
        // event (9) opened it fresh. It must NOT carry the completed session's state.
        assert_eq!(status_name(&m), "building");
        let next = building_range(&m);
        assert_eq!(
            next,
            FakeRange {
                session: session(6, 11),
                sum: 9,
                folds: 0,
                opened: IndicatorFreshness::Fresh,
            }
        );
        assert!(
            next.opened.is_fresh(),
            "the session opened by a rollover must start from a reset indicator"
        );
    }

    #[test]
    fn indicator_is_reset_between_sessions() {
        // Build up several updates in session A, then confirm session B opens fresh.
        let mut m = fsm_24h();

        m.update(ev(6, 10, 18, 0, 5)); // open A
        m.update(ev(6, 10, 20, 0, 7)); // fold A
        m.update(ev(6, 11, 10, 0, 9)); // fold A (still 6/10 anchor) -> 3 indicator updates

        let a = m
            .update(ev(6, 11, 17, 0, 1))
            .expect("A completes when B opens");
        assert_eq!(
            a,
            FakeRange {
                session: session(6, 10),
                sum: 21, // 5 + 7 + 9
                folds: 2,
                opened: IndicatorFreshness::Fresh,
            }
        );

        // If the machine had failed to reset the indicator, B would have inherited
        // A's 3 updates and would be Carried, not Fresh.
        let b = building_range(&m);
        assert_eq!(
            b,
            FakeRange {
                session: session(6, 11),
                sum: 1, // only the rollover event (value 1)
                folds: 0,
                opened: IndicatorFreshness::Fresh,
            }
        );
        assert!(
            b.opened.is_fresh(),
            "a new session must start from a reset indicator"
        );
    }

    #[test]
    fn reset_clears_both_status_and_cache() {
        let mut m = fsm(SessionWindow::us_core_session());

        m.update(ev(6, 10, 10, 0, 5));
        m.update(ev(6, 10, 17, 0, 99)); // complete one -> Closed, cache populated

        m.reset();
        assert_eq!(status_name(&m), "awaiting");

        // Cache is gone: an out-of-window event returns None, not the stale range.
        assert_eq!(m.update(ev(6, 10, 8, 0, 1)), None);
    }
}
