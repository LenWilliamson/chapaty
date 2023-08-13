use polars::prelude::DataFrame;
use serde::{Serialize, Deserialize};

use crate::{MarketKind, pnl::pnl_statement_agg_markets::{PnLSnapshotAggMarkets, PnLStatementAggMarkets}, data_frame_operations::io_operations::save_df_as_csv};


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PerformanceReportAggMarket {
    pub markets: Vec<MarketKind>,
    pub report: DataFrame,
}

impl From<PnLStatementAggMarkets> for PerformanceReportAggMarket {
    fn from(value: PnLStatementAggMarkets) -> Self {
        PnLSnapshotAggMarkets {
            pnl_reports: value.pnl_data,
            strategy_name: value.strategy_name.clone(),
        }
        .into()
    }
}

impl PerformanceReportAggMarket {
    pub fn save_as_csv(&self, file_name: &str) {
        save_df_as_csv(&mut self.report.clone(), &format!("{file_name}_all_markets_performance_report"))
    }
}