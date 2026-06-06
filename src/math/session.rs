pub mod ohlcv;
pub mod trades;

use std::{cmp::Ordering, fmt::Debug};

use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use chrono_tz::Tz;

// ================================================================================================
// Session window (shared timing core)
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

trait RangeState: Debug + Clone + Copy + Send + Sync + 'static {}

#[derive(Debug, Clone, Copy)]
struct Awaiting;

impl RangeState for Awaiting {}

#[derive(Debug, Clone, Copy)]
struct Building<SessionRangeData, VwapAccumulator> {
    range: SessionRangeData,
    accumulator: VwapAccumulator,
}

impl<SessionRangeData, VwapAccumulator> RangeState for Building<SessionRangeData, VwapAccumulator>
where
    SessionRangeData: Debug + Clone + Copy + Send + Sync + 'static,
    VwapAccumulator: Debug + Clone + Copy + Send + Sync + 'static,
{
}

#[derive(Debug, Clone, Copy)]
struct Closed<SessionRangeData> {
    range: SessionRangeData,
}

impl<SessionRangeData> RangeState for Closed<SessionRangeData> where
    SessionRangeData: Debug + Clone + Copy + Send + Sync + 'static
{
}

// ================================================================================================
// Helper Functions
// ================================================================================================

fn hm(h: u32, m: u32) -> NaiveTime {
    NaiveTime::from_hms_opt(h, m, 0).expect("invalid hour or minute")
}
