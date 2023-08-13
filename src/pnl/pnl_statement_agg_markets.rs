use std::collections::HashMap;

use polars::prelude::{DataFrame, IntoLazy, LazyFrame};
use serde::{Deserialize, Serialize};

use crate::{
    equity_curve::{self},
    lazy_frame_operations::trait_extensions::MyLazyFrameVecOperations,
    performance_report, trade_breakdown_report, MarketKind,
};

use super::{pnl_report_agg_markets::PnLReportsAggMarkets, pnl_statement::PnLStatement};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PnLStatementAggMarkets {
    pub strategy_name: String,
    pub markets: Vec<MarketKind>,
    pub pnl_data: PnLReportsAggMarkets,
}

impl PnLStatementAggMarkets {
    pub fn save_as_csv(&self, file_name: &str) {
        self.pnl_data.save_as_csv(file_name);
    }
}

impl From<PnLStatement> for PnLStatementAggMarkets {
    fn from(value: PnLStatement) -> Self {
        Self {
            strategy_name: value.strategy_name,
            markets: value.markets,
            pnl_data: value.pnl_data.into(),
        }
    }
}

pub struct PnLSnapshotAggMarkets {
    pub pnl_reports: PnLReportsAggMarkets,
    pub strategy_name: String,
}

impl From<PnLSnapshotAggMarkets>
    for trade_breakdown_report::agg_markets_and_year::TradeBreakDownReportAggMarket
{
    fn from(value: PnLSnapshotAggMarkets) -> Self {
        Self {
            markets: value.pnl_reports.markets.clone(),
            report: value.compute_trade_breakdown_report(),
        }
    }
}

impl From<PnLSnapshotAggMarkets>
    for performance_report::agg_markets_and_year::PerformanceReportAggMarket
{
    fn from(value: PnLSnapshotAggMarkets) -> Self {
        Self {
            markets: value.pnl_reports.markets.clone(),
            report: value.compute_performance_report(),
        }
    }
}

impl From<PnLSnapshotAggMarkets> for equity_curve::agg_markets_and_year::EquityCurvesAggMarkets {
    fn from(value: PnLSnapshotAggMarkets) -> Self {
        Self {
            markets: value.pnl_reports.markets.clone(),
            years: value.pnl_reports.years.clone(),
            curves: value.compute_equity_curves(),
        }
    }
}

impl PnLSnapshotAggMarkets {
    fn compute_equity_curves(self) -> HashMap<u32, Vec<f64>> {
        self.pnl_reports
            .reports
            .into_iter()
            .map(|(_, pnl_report)| pnl_report.as_equity_curve())
            .collect()
    }

    fn compute_trade_breakdown_report(self) -> DataFrame {
        let ldfs: Vec<LazyFrame> = self
            .pnl_reports
            .reports
            .into_iter()
            .map(|(_, pnl_report)| pnl_report.as_trade_breakdown_df())
            .map(|df| df.lazy())
            .collect();

        ldfs.concatenate_to_data_frame()
            .with_row_count("#", Some(1))
            .unwrap()
    }

    pub fn compute_performance_report(self) -> DataFrame {
        let ldfs: Vec<LazyFrame> = self
            .pnl_reports
            .reports
            .into_iter()
            .map(|(_, pnl_report)| pnl_report.as_performance_report_df())
            .map(|df| df.lazy())
            .collect();

        ldfs.concatenate_to_data_frame()
            .with_row_count("#", Some(1))
            .unwrap()
    }
}
