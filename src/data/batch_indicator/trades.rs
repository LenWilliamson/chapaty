use chrono::Duration;
use polars::prelude::{Expr, LazyFrame, SortMultipleOptions, col, lit, when};
use serde::{Deserialize, Serialize};

use crate::{error::ChapatyResult, transport::schema::CanonicalCol};

use super::{SessionConfig, VwapConfig};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BatchTradesIndicator {
    Vwap(VwapConfig),
    Session(SessionConfig),
}

impl BatchTradesIndicator {
    pub fn pre_compute(&self, lf: LazyFrame) -> ChapatyResult<LazyFrame> {
        match self {
            BatchTradesIndicator::Vwap(vwap) => vwap.pre_compute_vwap(lf),
            BatchTradesIndicator::Session(session) => session.pre_compute_session(lf),
        }
    }
}

// === Implementations ===

impl VwapConfig {
    fn pre_compute_vwap(&self, lf: LazyFrame) -> ChapatyResult<LazyFrame> {
        // Pure execution price VWAP (No HLC3 approximation needed for trades)
        let price_x_qty = col(CanonicalCol::Price) * col(CanonicalCol::Quantity);

        let valid_qty_mask = col(CanonicalCol::Quantity).gt(lit(0.0));

        let cumulative_pv = price_x_qty.filter(valid_qty_mask.clone()).cum_sum(false);
        let cumulative_q = col(CanonicalCol::Quantity)
            .filter(valid_qty_mask)
            .cum_sum(false);

        let vwap_expr = cumulative_pv / cumulative_q;

        Ok(lf
            .sort(
                [CanonicalCol::Timestamp],
                SortMultipleOptions::default().with_maintain_order(false),
            )
            .with_column(vwap_expr.alias(CanonicalCol::Vwap)))
    }
}

impl SessionConfig {
    fn pre_compute_session(&self, lf: LazyFrame) -> ChapatyResult<LazyFrame> {
        let start_mins = (self.start_h as u32) * 60 + (self.start_m as u32);
        let end_mins = (self.end_h as u32) * 60 + (self.end_m as u32);
        let is_intraday = start_mins < end_mins;

        let local_ts = col(CanonicalCol::Timestamp)
            .dt()
            .convert_time_zone(lit(self.timezone.clone()));
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
            .otherwise(lit(Expr::Literal(polars::prelude::LiteralValue::Null)))
        } else {
            when(time_mins.clone().gt_eq(lit(start_mins)))
                .then(local_date.clone())
                .when(time_mins.lt(lit(end_mins)))
                .then(local_date - lit(Duration::days(1)))
                .otherwise(lit(Expr::Literal(polars::prelude::LiteralValue::Null)))
        };

        // For Trades, VWAP is exactly Price * Quantity
        let pv = col(CanonicalCol::Price) * col(CanonicalCol::Quantity);

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
                    .over([CanonicalCol::SessionDate])
                    .map_err(convert_err)?
                    .alias(CanonicalCol::SessionHigh),
                col(CanonicalCol::Price)
                    .cum_min(false)
                    .over([CanonicalCol::SessionDate])
                    .map_err(convert_err)?
                    .alias(CanonicalCol::SessionLow),
                col(CanonicalCol::Quantity)
                    .cum_sum(false)
                    .over([CanonicalCol::SessionDate])
                    .map_err(convert_err)?
                    .alias(CanonicalCol::SessionVolume),
                (pv.cum_sum(false).over([CanonicalCol::SessionDate])
                    / col(CanonicalCol::Quantity)
                        .cum_sum(false)
                        .over([CanonicalCol::SessionDate]))
                .map_err(convert_err)?
                .alias(CanonicalCol::SessionVwap),
            ]);

        Ok(out_lf)
    }
}
