use chrono::{Datelike, NaiveDateTime, Duration};
use polars::{
    prelude::{Float64Chunked, Int64Chunked},
    series::{IntoSeries, Series},
};

pub fn round(val: &Series) -> Series {
    val.f64()
        .unwrap()
        .into_iter()
        .map(|o: Option<f64>| o.map(|px: f64| px.round()))
        .collect::<Float64Chunked>()
        .into_series()
}

/// This function computes the calender week (cw) from given timestamp in the `cw` Series of our DataFrame,
/// which is itself a contiguous growable collection of Series that have the same length.
///
/// # Arguments
/// * `val` - The `cw` Series of our DataFrame that contains timestamps in milliseconds
///
/// # Example
/// Suppose we have the following DataFrame
/// ```
/// // Some DataFrame as a .csv file
///
/// // atid     ,px      , qx    ,ftid   ,ltid   ,ts             ,bm     ,btpm   ,cw
/// // 0        ,10.00   ,1.00   ,0      ,0      ,1645300600000  ,false  ,true   ,1645300600000
/// // 1        ,20.00   ,1.00   ,0      ,0      ,1645400600000  ,true   ,true   ,1645400600000
///
/// let mut df: Result<DataFrame> = df!(
///     "atid" => &[0, 1],
///     "px" => &[10.00, 20.00],
///     "qx" => &[1.00, 1.00],
///     "ftid" => &[0, 0],
///     "ltid" => &[0, 0],
///     "ts" => &[1645300600000, 1645400600000],
///     "bm" => &[false, true],
///     "btpm" => &[true, true],
///     "cw" => &[1645300600000, 1645400600000],
/// );
///
/// // Calling our function on the "cw" column of our DataFrame yields a new DataFrame
/// // where we computed from the timestamp in the "cw" column the actual calender week
///
/// df.unwrap().apply("cw", get_cw_from_ts).unwrap();
///
/// // This call affects the "cw" Series and yields for this example the new DataFrame
///
/// // atid     ,px      , qx    ,ftid   ,ltid   ,ts             ,bm     ,btpm   ,cw
/// // 0        ,10.00   ,1.00   ,0      ,0      ,1645300600000  ,false  ,true   ,7
/// // 1        ,20.00   ,1.00   ,0      ,0      ,1645400600000  ,true   ,true   ,7
/// ```
pub fn get_cw_from_ts(val: &Series) -> Series {
    val.i64()
        .unwrap()
        .into_iter()
        .map(|o: Option<i64>| {
            o.map(|ts: i64| {
                i64::try_from(
                    NaiveDateTime::from_timestamp_opt(ts / 1000, 0)
                        .unwrap()
                        .iso_week()
                        .week(),
                )
                .unwrap()
            })
        })
        .collect::<Int64Chunked>()
        .into_series()
}

/// This function computes the weekday (wd) from given timestamp in the `weekday` Series of our DataFrame,
/// which is itself a contiguous growable collection of Series that have the same length.
///
/// # Arguments
/// * `val` - The `weekday` Series of our DataFrame that contains timestamps in milliseconds
///
/// # Example
/// Suppose we have the following DataFrame
/// ```
/// // Some DataFrame as a .csv file
///
/// // atid     ,px      , qx    ,ftid   ,ltid   ,ts             ,bm     ,btpm   ,weekday
/// // 0        ,10.00   ,1.00   ,0      ,0      ,1645300600000  ,false  ,true   ,1645300600000
/// // 1        ,20.00   ,1.00   ,0      ,0      ,1645400600000  ,true   ,true   ,1645400600000
///
/// let mut df: Result<DataFrame> = df!(
///     "atid" => &[0, 1],
///     "px" => &[10.00, 20.00],
///     "qx" => &[1.00, 1.00],
///     "ftid" => &[0, 0],
///     "ltid" => &[0, 0],
///     "ts" => &[1645300600000, 1645400600000],
///     "bm" => &[false, true],
///     "btpm" => &[true, true],
///     "weekday" => &[1645300600000, 1645400600000],
/// );
///
/// // Calling our function on the "weekday" column of our DataFrame yields a new DataFrame
/// // where we computed from the timestamp in the "weekday" column the actual calender week
///
/// df.unwrap().apply("weekday", get_weekday_from_ts).unwrap();
///
/// // This call affects the "weekday" Series and yields for this example the new DataFrame
///
/// // atid     ,px      , qx    ,ftid   ,ltid   ,ts             ,bm     ,btpm   ,weekday
/// // 0        ,10.00   ,1.00   ,0      ,0      ,1645300600000  ,false  ,true   ,6
/// // 1        ,20.00   ,1.00   ,0      ,0      ,1645400600000  ,true   ,true   ,7
/// ```
pub fn get_weekday_from_ts(val: &Series) -> Series {
    val.i64()
        .unwrap()
        .into_iter()
        .map(|o: Option<i64>| {
            o.map(|utc_ts_in_milliseconds: i64| {
                let ts =
                    NaiveDateTime::from_timestamp_opt(utc_ts_in_milliseconds / 1000, 0).unwrap();

                match ts.weekday() {
                    chrono::Weekday::Mon => {
                        i64::try_from(ts.weekday().number_from_monday()).unwrap()
                    }
                    chrono::Weekday::Tue => {
                        i64::try_from(ts.weekday().number_from_monday()).unwrap()
                    }
                    chrono::Weekday::Wed => {
                        i64::try_from(ts.weekday().number_from_monday()).unwrap()
                    }
                    chrono::Weekday::Thu => {
                        i64::try_from(ts.weekday().number_from_monday()).unwrap()
                    }
                    chrono::Weekday::Fri => {
                        i64::try_from(ts.weekday().number_from_monday()).unwrap()
                    }
                    chrono::Weekday::Sat => {
                        i64::try_from(ts.weekday().number_from_monday()).unwrap()
                    }
                    chrono::Weekday::Sun => {
                        i64::try_from(ts.weekday().number_from_monday()).unwrap()
                    }
                }
            })
        })
        .collect::<Int64Chunked>()
        .into_series()
}

pub fn comma_separated_string_to_f64(val: Series) -> Series {
    val.utf8()
        .unwrap()
        .into_iter()
        .map(|o| o.map(|x| x.replace(",", ".").parse::<f64>().unwrap()))
        .collect::<Float64Chunked>()
        .into_series()
}

pub fn sub_time(val: Series, duration: Duration) -> Series {
    val.utf8()
        .unwrap()
        .into_iter()
        .map(|o| {
            o.map(|ts| {
                NaiveDateTime::parse_from_str(ts, "%d.%m.%Y %H:%M:%S")
                    .unwrap()
                    .checked_sub_signed(duration)
                    .unwrap()
                    .timestamp_millis()
            })
        })
        .collect::<Int64Chunked>()
        .into_series()
}


#[cfg(test)]
mod tests {
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
}
