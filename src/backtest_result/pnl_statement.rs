use super::{
    equity_curves::{EquityCurve, EquityCurves},
    performance_report::PerformanceReport,
    pnl_report::PnLReports,
    trade_break_down_report::TradeBreakDownReport,
};
use crate::{
    enums::markets::MarketKind, lazy_frame_operations::trait_extensions::MyLazyFrameVecOperations,
};
use polars::prelude::{DataFrame, IntoLazy, LazyFrame};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PnLStatement {
    pub strategy_name: String,
    pub markets: Vec<MarketKind>,
    pub pnl_data: HashMap<MarketKind, PnLReports>,
}

impl PnLStatement {
    pub fn save_as_csv(&self, file_name: &str) {
        self.pnl_data
            .iter()
            .for_each(|(_, data)| data.save_as_csv(file_name))
    }
}

pub struct PnLSnapshot {
    pub pnl_reports: PnLReports,
    pub strategy_name: String,
}

impl From<PnLSnapshot> for TradeBreakDownReport {
    fn from(value: PnLSnapshot) -> Self {
        Self {
            market: value.pnl_reports.market,
            report: value.compute_trade_breakdown_report(),
        }
    }
}

impl From<PnLSnapshot> for PerformanceReport {
    fn from(value: PnLSnapshot) -> Self {
        Self {
            market: value.pnl_reports.market,
            report: value.compute_performance_report(),
        }
    }
}

impl From<PnLSnapshot> for EquityCurves {
    fn from(value: PnLSnapshot) -> Self {
        Self {
            market: value.pnl_reports.market,
            years: value.pnl_reports.years.clone(),
            curves: value.compute_equity_curves(),
        }
    }
}

impl PnLSnapshot {
    fn compute_equity_curves(self) -> HashMap<u32, EquityCurve> {
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
