use chrono::Duration;
use polars::prelude::{LazyFrame, NULL, SortMultipleOptions, col, lit, when};
use serde::{Deserialize, Serialize};

use crate::{
    data::batch_indicator::{convert_err, finalize_scalar},
    error::ChapatyResult,
    transport::schema::CanonicalCol,
};

use super::config::SessionConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BatchTradesIndicator {
    Vwap,
    OvernightRange(SessionConfig),
}

impl BatchTradesIndicator {
    pub(crate) fn pre_compute(&self, lf: LazyFrame) -> ChapatyResult<LazyFrame> {
        match self {
            BatchTradesIndicator::Vwap => pre_compute_trades_vwap(lf),
            BatchTradesIndicator::OvernightRange(session) => session.pre_compute_trades_session(lf),
        }
    }
}

// === Implementations ===

fn pre_compute_trades_vwap(lf: LazyFrame) -> ChapatyResult<LazyFrame> {
    // Pure execution price VWAP (no HLC3 approximation needed for trades).
    // Trade quantity is carried in the canonical `Volume` column.
    //
    // Non-positive quantity contributes nothing, mirroring the streaming
    // accumulator. A conditional (rather than `filter`) keeps the series the
    // same length as the frame so it stays alignable with the timestamp.
    let has_qty = col(CanonicalCol::Volume).gt(lit(0.0));
    let price_x_qty = when(has_qty.clone())
        .then(col(CanonicalCol::Price) * col(CanonicalCol::Volume))
        .otherwise(lit(0.0));
    let quantity = when(has_qty)
        .then(col(CanonicalCol::Volume))
        .otherwise(lit(0.0));

    let cumulative_q = quantity.cum_sum(false);
    // VWAP is undefined until the first positive-quantity trade arrives.
    let vwap_expr = when(cumulative_q.clone().gt(lit(0.0)))
        .then(price_x_qty.cum_sum(false) / cumulative_q)
        .otherwise(lit(NULL));

    Ok(finalize_scalar(lf, vwap_expr))
}

impl SessionConfig {
    fn pre_compute_trades_session(&self, lf: LazyFrame) -> ChapatyResult<LazyFrame> {
        let start_mins = (self.start_h as u32) * 60 + (self.start_m as u32);
        let end_mins = (self.end_h as u32) * 60 + (self.end_m as u32);
        let is_intraday = start_mins < end_mins;

        let local_ts = col(CanonicalCol::Timestamp)
            .dt()
            .convert_time_zone(self.polars_tz()?);
        let time_mins = local_ts.clone().dt().hour() * lit(60u32) + local_ts.clone().dt().minute();
        let local_date = local_ts.clone().dt().date();

        let session_date_expr = if is_intraday {
            when(
                time_mins
                    .clone()
                    .gt_eq(lit(start_mins))
                    .and(time_mins.clone().lt(lit(end_mins))),
            )
            .then(local_date.clone())
            .otherwise(lit(NULL))
        } else {
            when(time_mins.clone().gt_eq(lit(start_mins)))
                .then(local_date.clone())
                .when(time_mins.lt(lit(end_mins)))
                .then(local_date - lit(Duration::days(1)))
                .otherwise(lit(NULL))
        };

        // For trades, VWAP is exactly price * quantity (quantity carried in `Volume`).
        let pv = col(CanonicalCol::Price) * col(CanonicalCol::Volume);

        // Session VWAP partitioned by session; `over` is fallible so divide last.
        let session_pv_cum = pv
            .cum_sum(false)
            .over([col(CanonicalCol::SessionDate)])
            .map_err(convert_err)?;
        let session_volume_cum = col(CanonicalCol::Volume)
            .cum_sum(false)
            .over([col(CanonicalCol::SessionDate)])
            .map_err(convert_err)?;
        let session_vwap = session_pv_cum / session_volume_cum;

        let out_lf = lf
            .sort(
                [CanonicalCol::Timestamp],
                SortMultipleOptions::default().with_maintain_order(false),
            )
            .with_column(session_date_expr.alias(CanonicalCol::SessionDate))
            .with_columns([
                // Trades do not have inherent High/Low columns, so we calculate the
                // extremes dynamically based on execution Price.
                col(CanonicalCol::Price)
                    .cum_max(false)
                    .over([col(CanonicalCol::SessionDate)])
                    .map_err(convert_err)?
                    .alias(CanonicalCol::SessionHigh),
                col(CanonicalCol::Price)
                    .cum_min(false)
                    .over([col(CanonicalCol::SessionDate)])
                    .map_err(convert_err)?
                    .alias(CanonicalCol::SessionLow),
                col(CanonicalCol::Volume)
                    .cum_sum(false)
                    .over([col(CanonicalCol::SessionDate)])
                    .map_err(convert_err)?
                    .alias(CanonicalCol::SessionVolume),
                session_vwap.alias(CanonicalCol::SessionVwap),
            ])
            // Project to the columns of the `TradesSessionData` struct (keyed by
            // timestamp) and keep only rows that belong to a session.
            .select([
                col(CanonicalCol::Timestamp),
                col(CanonicalCol::SessionDate),
                col(CanonicalCol::SessionHigh),
                col(CanonicalCol::SessionLow),
                col(CanonicalCol::SessionVolume),
                col(CanonicalCol::SessionVwap),
            ])
            .filter(col(CanonicalCol::SessionDate).is_not_null());

        Ok(out_lf)
    }
}
