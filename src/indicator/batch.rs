use polars::{
    datatypes::{TimeUnit, TimeZone},
    prelude::{DataType, Expr, LazyFrame, NULL, SortMultipleOptions, col, lit, when},
};

use crate::{
    data::domain::{SessionWindow, WindowKind},
    error::{ChapatyError, ChapatyResult, DataError},
    transport::schema::CanonicalCol,
};

pub mod event;
pub mod ohlcv;
pub mod trades;

/// A trait enabling builder-pattern injection of batch indicators.
pub trait WithBatchIndicators: Sized {
    type BatchIndicator: Clone;

    fn with_indicator(self, kind: Self::BatchIndicator) -> Self;

    fn with_indicators(self, kinds: &[Self::BatchIndicator]) -> Self {
        kinds
            .iter()
            .fold(self, |acc, kind| acc.with_indicator(kind.clone()))
    }
}

pub(crate) trait BatchCompute {
    fn pre_compute(&self, lf: LazyFrame) -> ChapatyResult<LazyFrame>;
}

trait IndicatorExprExt {
    /// Computes a continuous, running VWAP.
    ///
    /// **Shape:** Yields a Vector (Series) of the same length as the input.
    /// **Usage:** Use this strictly inside projection contexts like `.with_columns()`
    /// or `.select()` when you need the running history of the VWAP at every tick.
    fn vwap_with_volume(self, volume: Expr) -> Expr;

    /// Computes the final, static VWAP for a grouped aggregation.
    ///
    /// **Shape:** Yields a Scalar (`f64`).
    /// **Usage:** Use this strictly inside `.group_by().agg()` blocks.
    /// Using the standard [`IndicatorExprExt::vwap`] method in an aggregation context will incorrectly
    /// yield a `List<f64>` instead of a singular value.
    fn agg_vwap_with_volume(self, volume: Expr) -> Expr;

    /// Classifies timestamps into session dates based on a `SessionWindow`.
    /// Handles timezone conversions and overnight session wrapping.
    fn session_date(self, session: SessionWindow) -> Expr;

    /// Detects the precision of an integer epoch timestamp (Seconds, Millis, Micros, or Nanos)
    /// based on its magnitude and normalizes it to a UTC Datetime in Microseconds.
    ///
    /// **Thresholds:**
    /// * Seconds (10 digits): `< 10^11`
    /// * Millis (13 digits): `< 10^14`
    /// * Micros (16 digits): `< 10^17`
    /// * Nanos (19 digits): `> 10^17`
    fn ts_as_microseconds(self) -> Expr;
}

impl IndicatorExprExt for Expr {
    fn vwap_with_volume(self, volume: Expr) -> Expr {
        let (price_x_volume, vol) = prepare_vwap_components(self, volume);

        let cumulative_v = vol.cum_sum(false);

        when(cumulative_v.clone().gt(lit(0.0)))
            .then(price_x_volume.cum_sum(false) / cumulative_v)
            .otherwise(lit(NULL))
    }

    fn agg_vwap_with_volume(self, volume: Expr) -> Expr {
        let (price_x_volume, vol) = prepare_vwap_components(self, volume);

        let sum_pv = price_x_volume.sum();
        let sum_v = vol.sum();

        when(sum_v.clone().gt(lit(0.0)))
            .then(sum_pv / sum_v)
            .otherwise(lit(NULL))
    }

    fn session_date(self, session: SessionWindow) -> Expr {
        let local_ts = self.dt().convert_time_zone(session.pl_time_zone());
        let local_time = local_ts.clone().dt().time();
        let local_date = local_ts.clone().dt().date();

        let start_time = lit(session.start_nanos_since_midnight());
        let end_time = lit(session.end_nanos_since_midnight());

        match session.window_kind() {
            WindowKind::Intraday => when(
                local_time
                    .clone()
                    .gt_eq(start_time)
                    .and(local_time.lt(end_time)),
            )
            .then(local_date)
            .otherwise(lit(NULL)),
            WindowKind::Overnight => when(local_time.clone().gt_eq(start_time))
                .then(local_date.clone())
                .when(local_time.lt(end_time))
                .then(local_date - lit(chrono::Duration::days(1)))
                .otherwise(lit(NULL)),
        }
    }

    fn ts_as_microseconds(self) -> Expr {
        let ts = self.cast(DataType::Int64);

        when(ts.clone().lt(lit(100_000_000_000i64))) // < 10^11 -> Seconds
            .then(ts.clone() * lit(1_000_000))
            .when(ts.clone().lt(lit(100_000_000_000_000i64))) // < 10^14 -> Millis
            .then(ts.clone() * lit(1_000))
            .when(ts.clone().gt(lit(100_000_000_000_000_000i64))) // > 10^17 -> Nanos
            .then(ts.clone() / lit(1000)) // Drop nano remainder by integer division
            .otherwise(ts) // Already Micros
            .cast(DataType::Datetime(
                TimeUnit::Microseconds,
                Some(TimeZone::UTC),
            ))
    }
}

/// Returns a tuple of `(price_x_volume, valid_volume)` where any rows with
/// zero or negative volume are masked out.
fn prepare_vwap_components(price: Expr, volume: Expr) -> (Expr, Expr) {
    let has_volume = volume.clone().gt(lit(0.0));

    let price_x_volume = when(has_volume.clone())
        .then(price * volume.clone())
        .otherwise(lit(0.0));

    let valid_vol = when(has_volume).then(volume).otherwise(lit(0.0));

    (price_x_volume, valid_vol)
}

trait LazyFrameIndicatorExt {
    /// Projects a single-value indicator into the canonical `[Timestamp, Price]` shape.
    fn into_price_timeseries(self, value: Expr) -> Self;
}

impl LazyFrameIndicatorExt for LazyFrame {
    fn into_price_timeseries(self, value: Expr) -> Self {
        self.sort(
            [CanonicalCol::PointInTime],
            SortMultipleOptions::default().with_maintain_order(false),
        )
        .select([
            col(CanonicalCol::PointInTime),
            value.alias(CanonicalCol::Price),
        ])
        .filter(col(CanonicalCol::Price).is_not_null())
    }
}

fn convert_err(e: polars::error::PolarsError) -> ChapatyError {
    ChapatyError::Data(DataError::DataFrame(format!(
        "Error while building batch indicator: {e}"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::schema::CanonicalCol;
    use chrono::{TimeZone, Utc};
    use polars::prelude::{IntoLazy, NamedFrom, Series};

    fn utc_micros(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> i64 {
        Utc.with_ymd_and_hms(y, mo, d, h, mi, 0)
            .unwrap()
            .timestamp_micros()
    }

    /// Runs `IndicatorExprExt::session_date` over a column of UTC timestamps and
    /// returns the resulting session dates as ISO strings (`None` when outside the window).
    fn session_dates(window: SessionWindow, ts_utc: &[i64]) -> Vec<Option<String>> {
        let series = Series::new(CanonicalCol::PointInTime.as_str().into(), ts_utc.to_vec())
            .cast(&DataType::Datetime(
                TimeUnit::Microseconds,
                Some(polars::prelude::TimeZone::UTC),
            ))
            .expect("cast to datetime");

        let df = series
            .into_frame()
            .lazy()
            .select([col(CanonicalCol::PointInTime)
                .session_date(window)
                .alias("session_date")])
            .collect()
            .expect("collect");

        let as_str = df
            .column("session_date")
            .unwrap()
            .cast(&DataType::String)
            .unwrap();
        let series = as_str.as_materialized_series();
        let ca = series.str().unwrap();
        (0..ca.len())
            .map(|i| ca.get(i).map(str::to_string))
            .collect()
    }

    fn some(date: &str) -> Option<String> {
        Some(date.to_string())
    }

    /// Intraday window (`start_mins < end_mins`): US core session 09:30–16:00 ET.
    /// The start is inclusive and the end exclusive; everything outside is null.
    /// June keeps ET at a fixed UTC−4 offset (no DST ambiguity).
    #[test]
    fn session_date_intraday_us_core_session() {
        let window = SessionWindow::us_core_session();
        let timestamps = [
            utc_micros(2026, 6, 13, 13, 29), // 09:29 ET — before open
            utc_micros(2026, 6, 13, 13, 30), // 09:30 ET — open (inclusive)
            utc_micros(2026, 6, 13, 16, 0),  // 12:00 ET — mid-session
            utc_micros(2026, 6, 13, 19, 59), // 15:59 ET — still inside
            utc_micros(2026, 6, 13, 20, 0),  // 16:00 ET — close (exclusive)
            utc_micros(2026, 6, 13, 22, 0),  // 18:00 ET — after close
        ];

        assert_eq!(
            session_dates(window, &timestamps),
            vec![
                None,
                some("2026-06-13"),
                some("2026-06-13"),
                some("2026-06-13"),
                None,
                None,
            ]
        );
    }

    /// Overnight window (`start_mins >= end_mins`): US extended overnight
    /// 18:00–09:30 ET. The evening leg keeps its calendar date, while the
    /// post-midnight morning leg is pulled back to the previous day's session.
    #[test]
    fn session_date_overnight_pulls_morning_leg_to_previous_day() {
        let window = SessionWindow::us_extended_overnight();
        let timestamps = [
            utc_micros(2026, 6, 13, 21, 59), // 17:59 ET Jun 13 — before evening open
            utc_micros(2026, 6, 13, 22, 0),  // 18:00 ET Jun 13 — evening open
            utc_micros(2026, 6, 14, 3, 0),   // 23:00 ET Jun 13 — late evening
            utc_micros(2026, 6, 14, 6, 0),   // 02:00 ET Jun 14 — morning leg → Jun 13
            utc_micros(2026, 6, 14, 13, 29), // 09:29 ET Jun 14 — morning leg → Jun 13
            utc_micros(2026, 6, 14, 13, 30), // 09:30 ET Jun 14 — end (exclusive)
            utc_micros(2026, 6, 14, 16, 0),  // 12:00 ET Jun 14 — daytime gap
        ];

        assert_eq!(
            session_dates(window, &timestamps),
            vec![
                None,
                some("2026-06-13"),
                some("2026-06-13"),
                some("2026-06-13"),
                some("2026-06-13"),
                None,
                None,
            ]
        );
    }
}
