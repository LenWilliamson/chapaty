use chrono::{DateTime, Datelike, Duration, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use strum::Display;

use crate::{
    error::{ChapatyError, ChapatyResult, SystemError},
    impl_add_sub_mul_div_primitive, impl_from_primitive,
};

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct EpisodeId(pub usize);
impl_from_primitive!(EpisodeId, usize);
impl_add_sub_mul_div_primitive!(EpisodeId, usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Episode {
    id: EpisodeId,
    length: EpisodeLength,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
}

impl Default for Episode {
    fn default() -> Self {
        Self {
            id: EpisodeId::default(),
            length: EpisodeLength::default(),
            start: DateTime::<Utc>::MIN_UTC,
            end: DateTime::<Utc>::MAX_UTC,
        }
    }
}

impl Episode {
    pub fn is_episode_end(&self, current_ts: DateTime<Utc>) -> bool {
        if self.length.is_infinite() {
            return false;
        }
        current_ts >= self.end
    }

    pub fn id(&self) -> EpisodeId {
        self.id
    }

    pub fn length(&self) -> EpisodeLength {
        self.length
    }

    pub fn start(&self) -> DateTime<Utc> {
        self.start
    }

    pub fn end(&self) -> DateTime<Utc> {
        self.end
    }
}

impl Episode {
    pub(crate) fn next(self, start: DateTime<Utc>) -> Episode {
        let length = self.length();
        Self {
            id: EpisodeId(self.id().0 + 1),
            length,
            start,
            end: length.calculate_end(start),
        }
    }
}

pub(crate) struct EpisodeBuilder {
    id: EpisodeId,
    length: Option<EpisodeLength>,
    start: Option<DateTime<Utc>>,
}

impl EpisodeBuilder {
    pub fn new() -> Self {
        Self {
            id: EpisodeId(0),
            length: None,
            start: None,
        }
    }

    pub(crate) fn with_length(self, length: EpisodeLength) -> Self {
        Self {
            length: Some(length),
            ..self
        }
    }

    pub(crate) fn with_start(self, start: DateTime<Utc>) -> Self {
        Self {
            start: Some(start),
            ..self
        }
    }

    pub(crate) fn build(self) -> ChapatyResult<Episode> {
        let length = self.length.ok_or_else(|| episode_build_err("length"))?;
        let start = self.start.ok_or_else(|| episode_build_err("start"))?;
        let end = length.calculate_end(start);
        Ok(Episode {
            id: self.id,
            length,
            start,
            end,
        })
    }
}

fn episode_build_err(s: &str) -> ChapatyError {
    ChapatyError::System(SystemError::MissingField(format!(
        "Field `{s}` is required to build `Episode`"
    )))
}

/// Controls how long trades may remain open before being force-closed.
/// Trades may close early based on policy logic, but are always reset
/// at the end of the episode.
#[derive(
    Copy,
    Clone,
    Debug,
    Hash,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Display,
    Default,
    PartialOrd,
    Ord,
)]
pub enum EpisodeLength {
    /// Trades reset at the end of each day (UTC).
    #[default]
    Day = 1,

    /// Trades reset at the end of each week (UTC Sunday night).
    Week = 2,

    /// Trades reset at the end of each calendar month.
    Month = 3,

    /// Trades reset every three months (quarter-end).
    Quarter = 4,

    /// Trades reset every six months (semiannual period).
    SemiAnnual = 5,

    /// Trades reset at the end of the calendar year.
    Annual = 6,

    /// Trades are not reset at all.
    Infinite = 7,
}

impl EpisodeLength {
    pub fn is_infinite(&self) -> bool {
        matches!(self, EpisodeLength::Infinite)
    }

    pub fn is_day(&self) -> bool {
        matches!(self, EpisodeLength::Day)
    }

    pub fn is_week(&self) -> bool {
        matches!(self, EpisodeLength::Week)
    }

    pub fn is_month(&self) -> bool {
        matches!(self, EpisodeLength::Month)
    }

    pub fn is_quarter(&self) -> bool {
        matches!(self, EpisodeLength::Quarter)
    }

    pub fn is_semi_annual(&self) -> bool {
        matches!(self, EpisodeLength::SemiAnnual)
    }

    pub fn is_annual(&self) -> bool {
        matches!(self, EpisodeLength::Annual)
    }

    pub fn max_episodes(&self) -> usize {
        use EpisodeLength::*;

        match self {
            Day => 366,
            Week => 52,
            Month => 12,
            Quarter => 4,
            SemiAnnual => 2,
            Annual | Infinite => 1,
        }
    }
}

impl EpisodeLength {
    /// Calculates the exclusive end time of an episode given its start time.
    ///
    /// The episode is treated as the time interval `[start, end)`, meaning the
    /// episode includes the `start` time but ends just before the `end` time.
    ///
    /// # Arguments
    ///
    /// * `start` - The `DateTime<Utc>` when the episode begins.
    ///
    /// # Returns
    ///
    /// The `DateTime<Utc>` marking the beginning of the next period, which is the
    /// exclusive end of the current episode. For `Infinite` length, it returns `DateTime::MAX_UTC`.
    fn calculate_end(&self, start: DateTime<Utc>) -> DateTime<Utc> {
        use EpisodeLength::*;
        match self {
            Infinite => DateTime::<Utc>::MAX_UTC,
            Day => {
                let start_of_next_day = (start.date_naive() + Duration::days(1))
                    .and_hms_opt(0, 0, 0)
                    .unwrap();
                DateTime::from_naive_utc_and_offset(start_of_next_day, Utc)
            }
            Week => {
                // Calculates the start of the next week (Monday).
                let days_to_next_monday = 7 - start.weekday().num_days_from_monday();
                let start_of_next_week = (start.date_naive()
                    + Duration::days(days_to_next_monday as i64))
                .and_hms_opt(0, 0, 0)
                .unwrap();
                DateTime::from_naive_utc_and_offset(start_of_next_week, Utc)
            }
            Month => {
                let (year, month) = (start.year(), start.month());
                let (next_month_year, next_month) = if month == 12 {
                    (year + 1, 1)
                } else {
                    (year, month + 1)
                };
                let start_of_next_month = NaiveDate::from_ymd_opt(next_month_year, next_month, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap();
                DateTime::from_naive_utc_and_offset(start_of_next_month, Utc)
            }
            Quarter => {
                let year = start.year();
                let month = start.month();
                let (next_quarter_start_year, next_quarter_start_month) = match month {
                    1..=3 => (year, 4),
                    4..=6 => (year, 7),
                    7..=9 => (year, 10),
                    10..=12 => (year + 1, 1),
                    _ => unreachable!(),
                };
                let start_of_next_quarter =
                    NaiveDate::from_ymd_opt(next_quarter_start_year, next_quarter_start_month, 1)
                        .unwrap()
                        .and_hms_opt(0, 0, 0)
                        .unwrap();
                DateTime::from_naive_utc_and_offset(start_of_next_quarter, Utc)
            }
            SemiAnnual => {
                let year = start.year();
                let month = start.month();
                let (next_period_start_year, next_period_start_month) =
                    if month <= 6 { (year, 7) } else { (year + 1, 1) };
                let start_of_next_period =
                    NaiveDate::from_ymd_opt(next_period_start_year, next_period_start_month, 1)
                        .unwrap()
                        .and_hms_opt(0, 0, 0)
                        .unwrap();
                DateTime::from_naive_utc_and_offset(start_of_next_period, Utc)
            }
            Annual => {
                let start_of_next_year = NaiveDate::from_ymd_opt(start.year() + 1, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap();
                DateTime::from_naive_utc_and_offset(start_of_next_year, Utc)
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    // ============================================================================================
    // Helper Functions
    // ============================================================================================

    fn utc(year: i32, month: u32, day: u32, hour: u32, min: u32, sec: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, hour, min, sec)
            .unwrap()
    }

    fn midnight(year: i32, month: u32, day: u32) -> DateTime<Utc> {
        utc(year, month, day, 0, 0, 0)
    }

    fn episode(length: EpisodeLength, start: DateTime<Utc>) -> Episode {
        EpisodeBuilder::new()
            .with_length(length)
            .with_start(start)
            .build()
            .unwrap()
    }

    // ============================================================================================
    // EpisodeLength::calculate_end Tests
    // ============================================================================================

    #[test]
    fn day_ends_at_next_midnight() {
        let start = utc(2025, 6, 15, 14, 30, 0);
        let end = EpisodeLength::Day.calculate_end(start);
        assert_eq!(end, midnight(2025, 6, 16));
    }

    #[test]
    fn day_at_midnight_ends_next_day() {
        let start = midnight(2025, 6, 15);
        let end = EpisodeLength::Day.calculate_end(start);
        assert_eq!(end, midnight(2025, 6, 16));
    }

    #[test]
    fn day_handles_month_boundary() {
        let start = utc(2025, 1, 31, 23, 59, 59);
        let end = EpisodeLength::Day.calculate_end(start);
        assert_eq!(end, midnight(2025, 2, 1));
    }

    #[test]
    fn day_handles_year_boundary() {
        let start = utc(2025, 12, 31, 12, 0, 0);
        let end = EpisodeLength::Day.calculate_end(start);
        assert_eq!(end, midnight(2026, 1, 1));
    }

    #[test]
    fn week_ends_at_next_monday() {
        // Wednesday June 18, 2025 -> Monday June 23, 2025
        let start = utc(2025, 6, 18, 10, 0, 0);
        let end = EpisodeLength::Week.calculate_end(start);
        assert_eq!(end, midnight(2025, 6, 23));
    }

    #[test]
    fn week_on_monday_ends_next_monday() {
        // Monday June 16, 2025 -> Monday June 23, 2025
        let start = utc(2025, 6, 16, 0, 0, 0);
        let end = EpisodeLength::Week.calculate_end(start);
        assert_eq!(end, midnight(2025, 6, 23));
    }

    #[test]
    fn week_on_sunday_ends_next_day() {
        // Sunday June 22, 2025 -> Monday June 23, 2025
        let start = utc(2025, 6, 22, 23, 0, 0);
        let end = EpisodeLength::Week.calculate_end(start);
        assert_eq!(end, midnight(2025, 6, 23));
    }

    #[test]
    fn week_handles_year_boundary() {
        // Wednesday Dec 31, 2025 -> Monday Jan 5, 2026
        let start = utc(2025, 12, 31, 12, 0, 0);
        let end = EpisodeLength::Week.calculate_end(start);
        assert_eq!(end, midnight(2026, 1, 5));
    }

    #[test]
    fn month_ends_at_first_of_next_month() {
        let start = utc(2025, 6, 15, 8, 0, 0);
        let end = EpisodeLength::Month.calculate_end(start);
        assert_eq!(end, midnight(2025, 7, 1));
    }

    #[test]
    fn month_december_ends_at_january() {
        let start = utc(2025, 12, 25, 12, 0, 0);
        let end = EpisodeLength::Month.calculate_end(start);
        assert_eq!(end, midnight(2026, 1, 1));
    }

    #[test]
    fn month_first_day_ends_next_month() {
        let start = midnight(2025, 3, 1);
        let end = EpisodeLength::Month.calculate_end(start);
        assert_eq!(end, midnight(2025, 4, 1));
    }

    #[test]
    fn quarter_q1_ends_at_april() {
        let start = utc(2025, 2, 15, 0, 0, 0);
        let end = EpisodeLength::Quarter.calculate_end(start);
        assert_eq!(end, midnight(2025, 4, 1));
    }

    #[test]
    fn quarter_q2_ends_at_july() {
        let start = utc(2025, 5, 1, 0, 0, 0);
        let end = EpisodeLength::Quarter.calculate_end(start);
        assert_eq!(end, midnight(2025, 7, 1));
    }

    #[test]
    fn quarter_q3_ends_at_october() {
        let start = utc(2025, 9, 30, 23, 59, 59);
        let end = EpisodeLength::Quarter.calculate_end(start);
        assert_eq!(end, midnight(2025, 10, 1));
    }

    #[test]
    fn quarter_q4_ends_at_next_year() {
        let start = utc(2025, 11, 15, 0, 0, 0);
        let end = EpisodeLength::Quarter.calculate_end(start);
        assert_eq!(end, midnight(2026, 1, 1));
    }

    #[test]
    fn semi_annual_h1_ends_at_july() {
        let start = utc(2025, 3, 15, 0, 0, 0);
        let end = EpisodeLength::SemiAnnual.calculate_end(start);
        assert_eq!(end, midnight(2025, 7, 1));
    }

    #[test]
    fn semi_annual_h2_ends_at_next_year() {
        let start = utc(2025, 10, 1, 0, 0, 0);
        let end = EpisodeLength::SemiAnnual.calculate_end(start);
        assert_eq!(end, midnight(2026, 1, 1));
    }

    #[test]
    fn semi_annual_june_boundary() {
        let start = utc(2025, 6, 30, 23, 59, 59);
        let end = EpisodeLength::SemiAnnual.calculate_end(start);
        assert_eq!(end, midnight(2025, 7, 1));
    }

    #[test]
    fn annual_ends_at_next_year() {
        let start = utc(2025, 7, 4, 12, 0, 0);
        let end = EpisodeLength::Annual.calculate_end(start);
        assert_eq!(end, midnight(2026, 1, 1));
    }

    #[test]
    fn annual_first_day_ends_next_year() {
        let start = midnight(2025, 1, 1);
        let end = EpisodeLength::Annual.calculate_end(start);
        assert_eq!(end, midnight(2026, 1, 1));
    }

    #[test]
    fn infinite_returns_max_utc() {
        let start = utc(2025, 6, 15, 0, 0, 0);
        let end = EpisodeLength::Infinite.calculate_end(start);
        assert_eq!(end, DateTime::<Utc>::MAX_UTC);
    }

    // ============================================================================================
    // Episode::is_episode_end Tests
    // ============================================================================================

    #[test]
    fn before_end_returns_false() {
        let ep = episode(EpisodeLength::Day, midnight(2025, 6, 15));
        let current = utc(2025, 6, 15, 23, 59, 59);
        assert!(!ep.is_episode_end(current));
    }

    #[test]
    fn at_end_returns_true() {
        let ep = episode(EpisodeLength::Day, midnight(2025, 6, 15));
        let current = midnight(2025, 6, 16); // exactly at end
        assert!(ep.is_episode_end(current));
    }

    #[test]
    fn after_end_returns_true() {
        let ep = episode(EpisodeLength::Day, midnight(2025, 6, 15));
        let current = utc(2025, 6, 16, 0, 0, 1);
        assert!(ep.is_episode_end(current));
    }

    #[test]
    fn infinite_never_ends() {
        let ep = episode(EpisodeLength::Infinite, midnight(2025, 1, 1));
        let far_future = utc(2099, 12, 31, 23, 59, 59);
        assert!(!ep.is_episode_end(far_future));
    }

    #[test]
    fn week_boundary() {
        let ep = episode(EpisodeLength::Week, utc(2025, 6, 16, 10, 0, 0)); // Monday
        // Before Monday midnight
        assert!(!ep.is_episode_end(utc(2025, 6, 22, 23, 59, 59)));
        // At Monday midnight
        assert!(ep.is_episode_end(midnight(2025, 6, 23)));
    }

    // ============================================================================================
    // Episode::next Tests
    // ============================================================================================

    #[test]
    fn increments_id() {
        let ep = EpisodeBuilder::new()
            .with_length(EpisodeLength::Day)
            .with_start(midnight(2025, 6, 15))
            .build()
            .unwrap();

        let next_ep = ep.next(midnight(2025, 6, 16));
        assert_eq!(next_ep.id().0, 1);
    }

    #[test]
    fn preserves_length() {
        let ep = EpisodeBuilder::new()
            .with_length(EpisodeLength::Week)
            .with_start(midnight(2025, 6, 16))
            .build()
            .unwrap();

        let next_ep = ep.next(midnight(2025, 6, 23));
        assert_eq!(next_ep.length(), EpisodeLength::Week);
    }

    #[test]
    fn calculates_new_boundaries() {
        let ep = EpisodeBuilder::new()
            .with_length(EpisodeLength::Month)
            .with_start(midnight(2025, 6, 1))
            .build()
            .unwrap();

        let next_ep = ep.next(midnight(2025, 7, 1));
        assert_eq!(next_ep.start(), midnight(2025, 7, 1));
        assert_eq!(next_ep.end(), midnight(2025, 8, 1));
    }

    #[test]
    fn chained_episodes() {
        let ep0 = EpisodeBuilder::new()
            .with_length(EpisodeLength::Day)
            .with_start(midnight(2025, 6, 15))
            .build()
            .unwrap();

        let ep1 = ep0.next(midnight(2025, 6, 16));
        let ep2 = ep1.next(midnight(2025, 6, 17));
        let ep3 = ep2.next(midnight(2025, 6, 18));

        assert_eq!(ep3.id().0, 3);
        assert_eq!(ep3.start(), midnight(2025, 6, 18));
        assert_eq!(ep3.end(), midnight(2025, 6, 19));
    }

    // ============================================================================================
    // EpisodeBuilder Tests
    // ============================================================================================

    #[test]
    fn builds_valid_episode() {
        let ep = EpisodeBuilder::new()
            .with_length(EpisodeLength::Day)
            .with_start(midnight(2025, 6, 15))
            .build()
            .unwrap();

        assert_eq!(ep.id(), EpisodeId(0));
        assert_eq!(ep.length(), EpisodeLength::Day);
        assert_eq!(ep.start(), midnight(2025, 6, 15));
        assert_eq!(ep.end(), midnight(2025, 6, 16));
    }

    #[test]
    fn missing_length_fails() {
        let result = EpisodeBuilder::new()
            .with_start(midnight(2025, 6, 15))
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn missing_start_fails() {
        let result = EpisodeBuilder::new()
            .with_length(EpisodeLength::Day)
            .build();

        assert!(result.is_err());
    }

    // ============================================================================================
    // EpisodeLength Helpers Tests
    // ============================================================================================

    #[test]
    fn max_episodes_values() {
        assert_eq!(EpisodeLength::Day.max_episodes(), 366);
        assert_eq!(EpisodeLength::Week.max_episodes(), 52);
        assert_eq!(EpisodeLength::Month.max_episodes(), 12);
        assert_eq!(EpisodeLength::Quarter.max_episodes(), 4);
        assert_eq!(EpisodeLength::SemiAnnual.max_episodes(), 2);
        assert_eq!(EpisodeLength::Annual.max_episodes(), 1);
        assert_eq!(EpisodeLength::Infinite.max_episodes(), 1);
    }

    #[test]
    fn is_predicates() {
        assert!(EpisodeLength::Day.is_day());
        assert!(EpisodeLength::Week.is_week());
        assert!(EpisodeLength::Month.is_month());
        assert!(EpisodeLength::Quarter.is_quarter());
        assert!(EpisodeLength::SemiAnnual.is_semi_annual());
        assert!(EpisodeLength::Annual.is_annual());
        assert!(EpisodeLength::Infinite.is_infinite());
    }

    #[test]
    fn default_is_day() {
        assert_eq!(EpisodeLength::default(), EpisodeLength::Day);
    }
}
