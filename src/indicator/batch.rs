use chrono::Timelike;
use polars::prelude::{Expr, LazyFrame, NULL, SortMultipleOptions, col, lit, when};

use crate::{
    data::domain::SessionWindow,
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
    fn vwap(self, volume: Expr) -> Expr;

    /// Computes the final, static VWAP for a grouped aggregation.
    ///
    /// **Shape:** Yields a Scalar (`f64`).
    /// **Usage:** Use this strictly inside `.group_by().agg()` blocks.
    /// Using the standard [`IndicatorExprExt::vwap`] method in an aggregation context will incorrectly
    /// yield a `List<f64>` instead of a singular value.
    fn agg_vwap(self, volume: Expr) -> Expr;

    /// Classifies timestamps into session dates based on a `SessionWindow`.
    /// Handles timezone conversions and overnight session wrapping.
    fn session_date(self, session: SessionWindow) -> Expr;
}

impl IndicatorExprExt for Expr {
    fn vwap(self, volume: Expr) -> Expr {
        let (price_x_volume, vol) = prepare_vwap_components(self, volume);

        // .cum_sum() maintains the Vector shape for running histories
        let cumulative_v = vol.cum_sum(false);

        when(cumulative_v.clone().gt(lit(0.0)))
            .then(price_x_volume.cum_sum(false) / cumulative_v)
            .otherwise(lit(NULL))
    }

    fn agg_vwap(self, volume: Expr) -> Expr {
        let (price_x_volume, vol) = prepare_vwap_components(self, volume);

        // .sum() collapses the group into a single Scalar
        let sum_pv = price_x_volume.sum();
        let sum_v = vol.sum();

        when(sum_v.clone().gt(lit(0.0)))
            .then(sum_pv / sum_v)
            .otherwise(lit(NULL))
    }

    fn session_date(self, session: SessionWindow) -> Expr {
        let start_mins = session.start.hour() * 60 + session.start.minute();
        let end_mins = session.end.hour() * 60 + session.end.minute();
        let is_intraday = start_mins < end_mins;

        // 1. Convert to local timezone and extract raw time components
        let local_ts = self.dt().convert_time_zone(session.pl_time_zone());
        let time_mins = local_ts.clone().dt().hour() * lit(60u32) + local_ts.clone().dt().minute();
        let local_date = local_ts.clone().dt().date();

        // 2. Classify the Session Date
        if is_intraday {
            when(
                time_mins
                    .clone()
                    .gt_eq(lit(start_mins))
                    .and(time_mins.lt(lit(end_mins))),
            )
            .then(local_date)
            .otherwise(lit(NULL))
        } else {
            when(time_mins.clone().gt_eq(lit(start_mins)))
                .then(local_date.clone())
                .when(time_mins.lt(lit(end_mins)))
                // Pull the morning leg back into the previous evening's session date
                .then(local_date - lit(chrono::Duration::days(1)))
                .otherwise(lit(NULL))
        }
    }
}

/// Helper function to encapsulate the shared data-cleaning logic for VWAP calculations.
///
/// Returns a tuple of `(price_x_volume, valid_volume)` where any rows with
/// zero or negative volume are masked out to prevent skewing.
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
    fn finalize_scalar(self, value: Expr) -> Self;
}

impl LazyFrameIndicatorExt for LazyFrame {
    fn finalize_scalar(self, value: Expr) -> Self {
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
