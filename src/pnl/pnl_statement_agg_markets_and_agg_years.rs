use polars::prelude::DataFrame;
use serde::{Deserialize, Serialize};

use crate::{
    converter::pnl_to_report::{as_equity_curve, PnLToReportRequestBuilder},
    data_frame_operations::io_operations::save_df_as_csv,
    MarketKind,
};

use super::pnl_statement_agg_markets::PnLStatementAggMarkets;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PnLStatementAggMarketsAggYears {
    pub strategy_name: String,
    pub markets: Vec<MarketKind>,
    pub years: Vec<u32>,
    pub pnl: DataFrame,
}

impl PnLStatementAggMarketsAggYears {
    pub fn save_as_csv(&self, file_name: &str) {
        save_df_as_csv(
            &mut self.pnl.clone(),
            &format!("{file_name}_all_markets_all_years_pnl"),
        )
    }
}

impl From<PnLStatementAggMarkets> for PnLStatementAggMarketsAggYears {
    fn from(value: PnLStatementAggMarkets) -> Self {
        Self {
            strategy_name: value.strategy_name.clone(),
            markets: value.markets.clone(),
            years: value.years.clone(),
            pnl: value.agg_year().with_row_count("id", Some(1)).unwrap(),
        }
    }
}

impl PnLStatementAggMarketsAggYears {
    pub fn compute_trade_breakdown_report(&self) -> DataFrame {
        PnLToReportRequestBuilder::new()
            .is_agg_markets(true)
            .is_agg_years(true)
            .with_pnl(self.pnl.clone())
            .with_strategy(self.strategy_name.clone())
            .build()
            .as_trade_breakdown_df()
            .with_row_count("id", Some(1))
            .unwrap()
    }

    pub fn compute_performance_report(&self) -> DataFrame {
        PnLToReportRequestBuilder::new()
            .is_agg_markets(true)
            .is_agg_years(true)
            .with_pnl(self.pnl.clone())
            .with_strategy(self.strategy_name.clone())
            .build()
            .as_performance_report_df()
    }

    pub fn compute_equity_curve(&self) -> Vec<f64> {
        as_equity_curve(&self.pnl, true)
    }
}
