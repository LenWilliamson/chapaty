use std::fmt;

use crate::enums::markets::TimeFrame;

use chrono::{Datelike, NaiveDateTime, Timelike};
use polars::prelude::{BooleanChunked, IntoSeries, Series};

pub trait InInterval {
    /// This function determines if a **UTC timestamp in milliseconds** is inside the
    /// half-open interval [start_day:start_h, end_day:end_h)
    ///
    /// # Note
    /// * For the PPP strategy, any trade happening during the weekend is disabled by default.
    /// * The timestamp is in **UTC** time
    /// * The timestamp is in **milliseconds**
    ///
    /// # Arguments
    /// * `ts_in_milliseconds` - **UTC** timestamp in **milliseconds**
    /// * `granularity` - we can filter `in_weekly_time_interval` or `in_daily_time_interval`
    fn in_time_interval(&self, val: &Series, granularity: &TimeFrame) -> Series;
    // fn in_weekly_time_interval(&self, utc_ts_in_milliseconds: i64) -> bool;
    // fn in_daily_time_interval(&self, utc_ts_in_milliseconds: i64) -> bool;
}

/// This struct defines the observation period of one calendar week. We align with the rule
/// of half-open intervals. That is [start_day:start_h, end_day:end_h)
///
/// # Note
///
/// For the PPP strategy, any trade happening during the weekend is disabled by default.
///
/// # Attributes
///
/// * `start_day` - weekday we want to enter our trade
/// * `start_h` - hour we want to enter our trade
/// * `end_day` - weekday we want to exit our trade
/// * `end_h` - hour we want to exit our trade
///
/// # Example
///
/// Let us choose the assumption that we only want to have trades from Monday 01:00UTC until
/// Friday 23:00UTC. Then we have to set the parameters as follows:
/// ```
/// let time_interval = TimeInterval {
///     start_day: chrono::Weekday::Mon,
///     start_h: 1,
///     end_day: chrono::Weekday::Fri,
///     end_h: 23,
/// }
/// ```
#[derive(Debug, Clone, Copy)]
pub struct TimeInterval {
    pub start_day: chrono::Weekday,
    pub start_h: u32,
    pub end_day: chrono::Weekday,
    pub end_h: u32,
}

impl InInterval for TimeInterval {
    fn in_time_interval(&self, val: &Series, time_frame: &TimeFrame) -> Series {
        val.i64()
            .unwrap()
            .into_iter()
            .map(|o: Option<i64>| {
                o.map(|ts: i64| match time_frame {
                    TimeFrame::Weekly => self.in_weekly_time_interval(ts),
                    TimeFrame::Daily => self.in_daily_time_interval(ts),
                })
            })
            .collect::<BooleanChunked>()
            .into_series()
    }
}

impl TimeInterval {
    fn in_weekly_time_interval(&self, utc_ts_in_milliseconds: i64) -> bool {
        let ts = NaiveDateTime::from_timestamp_opt(utc_ts_in_milliseconds / 1000, 0).unwrap();
        let weekend = ts.weekday() == chrono::Weekday::Sat || ts.weekday() == chrono::Weekday::Sun;
        let too_early = ts.hour() < self.start_h
            && ts.weekday().number_from_monday() <= self.start_day.number_from_monday();
        let too_late = ts.hour() >= self.end_h
            && ts.weekday().number_from_monday() >= self.end_day.number_from_monday();
        !(weekend || too_early || too_late)
    }

    fn in_daily_time_interval(&self, utc_ts_in_milliseconds: i64) -> bool {
        let ts = NaiveDateTime::from_timestamp_opt(utc_ts_in_milliseconds / 1000, 0).unwrap();
        let weekend = ts.weekday() == chrono::Weekday::Sat || ts.weekday() == chrono::Weekday::Sun;
        let too_early = ts.hour() < self.start_h;
        let too_late = ts.hour() >= self.end_h;
        !(weekend || too_early || too_late)
    }
}

impl fmt::Display for TimeInterval {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:?}{}h{}m-{:?}{}h{}m",
            self.start_day, self.start_h, 0, self.end_day, self.end_h, 0
        )
    }
}

pub fn timestamp_in_milli_to_string(ts: i64) -> String {
    NaiveDateTime::from_timestamp_opt(ts / 1000, 0)
        .unwrap()
        .format("%Y-%m-%d %H:%M:%S")
        .to_string()
}

#[cfg(test)]
mod tests {
    use crate::lazy_frame_operations::closures::{get_cw_from_ts, get_weekday_from_ts};

    use super::*;
    use polars::prelude::{df, DataFrame, NamedFrom};

    /// This unit test checks for the DataFrame
    ///
    /// ```
    /// atid    ,px      , qx    ,ftid   ,ltid   ,ts             ,bm     ,btpm   ,cw
    /// 0       ,10.00   ,1.00   ,0      ,0      ,1645300600000  ,false  ,true   ,1645300600000
    /// 1       ,20.00   ,1.00   ,0      ,0      ,1645400600000  ,true   ,true   ,1645400600000
    /// ```
    ///
    /// If a call of `get_cw_from_ts` on the `cw` column yields the following DataFrame
    ///
    /// ```
    /// atid    ,px      , qx    ,ftid   ,ltid   ,ts             ,bm     ,btpm   ,cw
    /// 0       ,10.00   ,1.00   ,0      ,0      ,1645300600000  ,false  ,true   ,7
    /// 1       ,20.00   ,1.00   ,0      ,0      ,1645400600000  ,true   ,true   ,7
    /// ```
    #[tokio::test]
    async fn test_get_cw_from_ts() {
        let df: polars::prelude::PolarsResult<DataFrame> = df!(
            "atid" => &[0, 1],
            "px" => &[10.00, 20.00],
            "qx" => &[1.00, 1.00],
            "ftid" => &[0, 0],
            "ltid" => &[0, 0],
            "ts" => &[1645300600000_i64, 1645400600000],
            "bm" => &[false, true],
            "btpm" => &[true, true],
            "cw" => &[1645300600000_i64, 1645400600000],
        );
        let target_df: polars::prelude::PolarsResult<DataFrame> = df!(
            "atid" => &[0, 1],
            "px" => &[10.00, 20.00],
            "qx" => &[1.00, 1.00],
            "ftid" => &[0, 0],
            "ltid" => &[0, 0],
            "ts" => &[1645300600000_i64, 1645400600000],
            "bm" => &[false, true],
            "btpm" => &[true, true],
            "cw" => &[7_i64, 7],
        );
        assert_eq!(
            df.unwrap()
                .apply("cw", get_cw_from_ts)
                .unwrap()
                .frame_equal(&target_df.unwrap()),
            true
        );
    }

    /// This unit test checks for the DataFrame
    ///
    /// ```
    /// atid    ,px      , qx    ,ftid   ,ltid   ,ts             ,bm     ,btpm   ,weekday
    /// 0       ,10.00   ,1.00   ,0      ,0      ,1645300600000  ,false  ,true   ,1645300600000
    /// 1       ,20.00   ,1.00   ,0      ,0      ,1645400600000  ,true   ,true   ,1645400600000
    /// ```
    ///
    /// If a call of `test_get_weekday_from_ts` on the `weekday` column yields the following DataFrame
    ///
    /// ```
    /// atid    ,px      , qx    ,ftid   ,ltid   ,ts             ,bm     ,btpm   ,weekday
    /// 0       ,10.00   ,1.00   ,0      ,0      ,1645300600000  ,false  ,true   ,6
    /// 1       ,20.00   ,1.00   ,0      ,0      ,1645400600000  ,true   ,true   ,7
    /// ```
    #[tokio::test]
    async fn test_get_weekday_from_ts() {
        let df: polars::prelude::PolarsResult<DataFrame> = df!(
            "atid" => &[0, 1],
            "px" => &[10.00, 20.00],
            "qx" => &[1.00, 1.00],
            "ftid" => &[0, 0],
            "ltid" => &[0, 0],
            "ts" => &[1645300600000_i64, 1645400600000],
            "bm" => &[false, true],
            "btpm" => &[true, true],
            "weekday" => &[1645300600000_i64, 1645400600000],
        );
        let target_df: polars::prelude::PolarsResult<DataFrame> = df!(
            "atid" => &[0, 1],
            "px" => &[10.00, 20.00],
            "qx" => &[1.00, 1.00],
            "ftid" => &[0, 0],
            "ltid" => &[0, 0],
            "ts" => &[1645300600000_i64, 1645400600000],
            "bm" => &[false, true],
            "btpm" => &[true, true],
            "weekday" => &[6_i64, 7],
        );
        assert_eq!(
            df.unwrap()
                .apply("weekday", get_weekday_from_ts)
                .unwrap()
                .frame_equal(&target_df.unwrap()),
            true
        );
    }

    /// This unit test checks if the function `in_time_interval` returns the expected values. We
    /// check especially for the boundary cases. The `TimeInterval` configuration for this test is
    /// ```
    /// let time_interval_config = TimeInterval {
    ///     start_day: chrono::Weekday::Mon,
    ///     start_h: 1,
    ///     end_day: chrono::Weekday::Fri,
    ///     end_h: 23,
    /// }
    /// ```
    ///
    /// # Note
    ///
    /// * We check for UTC timestamps
    /// * To double check the conversion from the timestamp in milliseconds to a human readable date time format, please refer to: https://currentmillis.com/
    ///
    #[test]
    fn test_in_time_interval() {
        let time_interval_config = TimeInterval {
            start_day: chrono::Weekday::Mon,
            start_h: 1,
            end_day: chrono::Weekday::Fri,
            end_h: 23,
        };

        // Monday
        // UTC: 2022-08-22 00:30:00 Monday
        assert_eq!(
            time_interval_config.in_weekly_time_interval(1661128200000),
            false
        );

        // UTC: 2022-08-22 00:59:59 Monday
        assert_eq!(
            time_interval_config.in_weekly_time_interval(1661129999000),
            false
        );

        // UTC: 2022-08-22 01:00:00 Monday
        assert_eq!(
            time_interval_config.in_weekly_time_interval(1661130000000),
            true
        );

        // UTC: 2022-08-22 12:00:00 Monday
        assert_eq!(
            time_interval_config.in_weekly_time_interval(1661169600000),
            true
        );

        // UTC: 2022-08-22 22:59:00 Monday
        assert_eq!(
            time_interval_config.in_weekly_time_interval(1661209140000),
            true
        );

        // UTC: 2022-08-22 23:00:00 Monday
        assert_eq!(
            time_interval_config.in_weekly_time_interval(1661209200000),
            true
        );

        // Wednesday
        // UTC: 2022-08-24 00:30:00 Wednesday
        assert_eq!(
            time_interval_config.in_weekly_time_interval(1661301000000),
            true
        );

        // UTC: 2022-08-24 00:59:59 Wednesday
        assert_eq!(
            time_interval_config.in_weekly_time_interval(1661302799000),
            true
        );

        // UTC: 2022-08-24 01:00:00 Wednesday
        assert_eq!(
            time_interval_config.in_weekly_time_interval(1661302800000),
            true
        );

        // UTC: 2022-08-24 12:00:00 Wednesday
        assert_eq!(
            time_interval_config.in_weekly_time_interval(1661342400000),
            true
        );

        // UTC: 2022-08-24 22:59:00 Wednesday
        assert_eq!(
            time_interval_config.in_weekly_time_interval(1661381999000),
            true
        );

        // UTC: 2022-08-24 23:00:00 Wednesday
        assert_eq!(
            time_interval_config.in_weekly_time_interval(1661382000000),
            true
        );

        // Friday
        // UTC: 2022-08-26 00:30:00 Friday
        assert_eq!(
            time_interval_config.in_weekly_time_interval(1661473800000),
            true
        );

        // UTC: 2022-08-26 00:59:59 Friday
        assert_eq!(
            time_interval_config.in_weekly_time_interval(1661475599000),
            true
        );

        // UTC: 2022-08-26 01:00:00 Friday
        assert_eq!(
            time_interval_config.in_weekly_time_interval(1661475600000),
            true
        );

        // UTC: 2022-08-26 12:00:00 Friday
        assert_eq!(
            time_interval_config.in_weekly_time_interval(1661515200000),
            true
        );

        // UTC: 2022-08-26 22:59:00 Friday
        assert_eq!(
            time_interval_config.in_weekly_time_interval(1661554799000),
            true
        );

        // UTC: 2022-08-26 23:00:00 Friday
        assert_eq!(
            time_interval_config.in_weekly_time_interval(1661554800000),
            false
        );

        // Saturday
        // UTC: 2022-08-27 12:00:00 Saturday
        assert_eq!(
            time_interval_config.in_weekly_time_interval(1661601600000),
            false
        );

        // Sunday
        // UTC: 2022-08-28 12:00:00 Sunday
        assert_eq!(
            time_interval_config.in_weekly_time_interval(1661688000000),
            false
        );
    }
}
