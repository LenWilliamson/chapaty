use std::collections::HashMap;

use polars::prelude::DataFrame;
use serde::{Deserialize, Serialize};

use crate::{equity_curve, performance_report, trade_breakdown_report, MarketKind};

use super::{pnl_report_agg_years::PnLReportAggYears, pnl_statement::PnLStatement};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PnLStatementAggYears {
    pub strategy_name: String,
    pub markets: Vec<MarketKind>,
    pub pnl_data: HashMap<MarketKind, PnLReportAggYears>,
}

impl PnLStatementAggYears {
    pub fn save_as_csv(&self, file_name: &str) {
        self.pnl_data
            .iter()
            .for_each(|(_, data)| data.save_as_csv(file_name))
    }
}

impl From<PnLStatement> for PnLStatementAggYears {
    fn from(value: PnLStatement) -> Self {
        let pnl_data = value
            .pnl_data
            .into_iter()
            .map(|(market, pnl_reports)| (market, pnl_reports.into()))
            .collect();
        Self {
            strategy_name: value.strategy_name,
            markets: value.markets,
            pnl_data,
        }
    }
}

pub struct PnLSnapshotAggYears {
    pub pnl_report: PnLReportAggYears,
    pub strategy_name: String,
}

impl From<PnLSnapshotAggYears>
    for trade_breakdown_report::market_and_agg_years::TradeBreakDownReportAggYears
{
    fn from(value: PnLSnapshotAggYears) -> Self {
        Self {
            market: value.pnl_report.market,
            report: value.compute_trade_breakdown_report(),
        }
    }
}

impl From<PnLSnapshotAggYears>
    for performance_report::market_and_agg_years::PerformanceReportAggYears
{
    fn from(value: PnLSnapshotAggYears) -> Self {
        Self {
            market: value.pnl_report.market,
            report: value.compute_performance_report(),
        }
    }
}

impl From<PnLSnapshotAggYears> for equity_curve::market_and_agg_years::EquityCurveAggYears {
    fn from(value: PnLSnapshotAggYears) -> Self {
        Self {
            market: value.pnl_report.market,
            years: value.pnl_report.years.clone(),
            curve: value.compute_equity_curves(),
        }
    }
}

impl PnLSnapshotAggYears {
    fn compute_equity_curves(self) -> Vec<f64> {
        self.pnl_report.as_equity_curve()
    }

    fn compute_trade_breakdown_report(self) -> DataFrame {
        self.pnl_report.as_trade_breakdown_df()
    }

    pub fn compute_performance_report(self) -> DataFrame {
        self.pnl_report.as_performance_report_df()
    }
}
