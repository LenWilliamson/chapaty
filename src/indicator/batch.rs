use polars::prelude::{Expr, LazyFrame, SortMultipleOptions, TimeZone, col};

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



trait BatchCompute {
    fn pre_compute(&self, lf: LazyFrame) -> ChapatyResult<LazyFrame>;
}

/// Projects a single-value indicator into the canonical `[Timestamp, Price]` shape
/// consumed by the technical-indicator pipeline.
///
/// The frame is sorted by timestamp (so windowed/recursive expressions evaluate in
/// order), the indicator value is placed in `Price`, and warm-up rows where the
/// value is still null are dropped. This mirrors the `{ timestamp, price }` output
/// structs of the streaming indicators (`Ema`, `Sma`, `Rsi`, ...).
fn finalize_scalar(lf: LazyFrame, value: Expr) -> LazyFrame {
    lf.sort(
        [CanonicalCol::Timestamp],
        SortMultipleOptions::default().with_maintain_order(false),
    )
    .select([
        col(CanonicalCol::Timestamp),
        value.alias(CanonicalCol::Price),
    ])
    .filter(col(CanonicalCol::Price).is_not_null())
}

fn get_polars_tz(window: &SessionWindow) -> ChapatyResult<TimeZone> {
    TimeZone::from_chrono(&window.timezone)
        .map_err(convert_err)?
        .ok_or_else(|| {
            ChapatyError::Data(DataError::DataFrame(format!(
                "Failed to resolve timezone for session batch indicator"
            )))
        })
}

fn convert_err(e: polars::error::PolarsError) -> ChapatyError {
    ChapatyError::Data(DataError::DataFrame(format!(
        "Error while building batch indicator: {e}"
    )))
}
