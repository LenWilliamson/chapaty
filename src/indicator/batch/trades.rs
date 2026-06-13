use polars::prelude::{LazyFrame, SortMultipleOptions, col};
use serde::{Deserialize, Serialize};

use crate::{
    data::domain::SessionWindow,
    error::ChapatyResult,
    indicator::batch::{BatchCompute, IndicatorExprExt, LazyFrameIndicatorExt},
    transport::schema::CanonicalCol,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BatchTradesIndicator {
    Vwap,
    OvernightRange(SessionWindow),
}

impl BatchCompute for BatchTradesIndicator {
    fn pre_compute(&self, lf: LazyFrame) -> ChapatyResult<LazyFrame> {
        match self {
            BatchTradesIndicator::Vwap => pre_compute_trades_vwap(lf),
            BatchTradesIndicator::OvernightRange(session) => {
                pre_compute_overnight_range(*session, lf)
            }
        }
    }
}

// ================================================================================================
// LazyFrame Pre-Computations
// ================================================================================================

fn pre_compute_trades_vwap(lf: LazyFrame) -> ChapatyResult<LazyFrame> {
    Ok(lf.finalize_scalar(col(CanonicalCol::Price).vwap(col(CanonicalCol::Volume))))
}

fn pre_compute_overnight_range(session: SessionWindow, lf: LazyFrame) -> ChapatyResult<LazyFrame> {
    let session_date_col = col(CanonicalCol::PointInTime).session_date(session);

    let out_lf = lf
        .with_column(session_date_col.alias(CanonicalCol::Date))
        .filter(col(CanonicalCol::Date).is_not_null())
        .group_by([col(CanonicalCol::Date)])
        .agg([
            // --- Time Boundaries ---
            col(CanonicalCol::OpenTimestamp)
                .first()
                .alias("OpenTimestamp"),
            col(CanonicalCol::PointInTime)
                .last()
                .alias(CanonicalCol::PointInTime),
            // --- Trade Extremes (Derived from Price) ---
            col(CanonicalCol::Price)
                .max()
                .alias(CanonicalCol::SessionHigh),
            col(CanonicalCol::Price)
                .min()
                .alias(CanonicalCol::SessionLow),
            col(CanonicalCol::Volume)
                .sum()
                .alias(CanonicalCol::SessionVolume),
            // --- Session VWAP ---
            col(CanonicalCol::Price)
                .agg_vwap(col(CanonicalCol::Volume))
                .alias(CanonicalCol::SessionVwap),
        ])
        // Ensure chronological order
        .sort([CanonicalCol::PointInTime], SortMultipleOptions::default());

    Ok(out_lf)
}
