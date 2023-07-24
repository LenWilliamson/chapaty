use crate::data_frame_operations::save_df_as_csv;
use crate::enums::bot::StrategyKind;
use crate::lazy_frame_operations::trait_extensions::MyLazyFrameVecOperations;
use std::collections::HashMap;

use crate::enums::markets::MarketKind;
use crate::lazy_frame_operations::trait_extensions::MyLazyFrameOperations;
use polars::prelude::DataFrame;
use polars::prelude::IntoLazy;
use polars::prelude::LazyFrame;

use super::equity_curves::{EquityCurve, EquityCurves};
use super::performance_report::PerformanceReport;
use super::{pnl_report::PnLReports, pnl_statement::PnLStatement};

#[derive(Debug)]

pub struct TradeBreakDownReports {
    pub markets: Vec<MarketKind>,
    pub reports: HashMap<MarketKind, TradeBreakDownReport>,
}

impl TradeBreakDownReports {
    pub fn save_as_csv(&self, file_name: &str) {
        self.reports
            .iter()
            .for_each(|(market, trade_break_down_report)| {
                save_df_as_csv(
                    &mut trade_break_down_report.report.clone(),
                    &format!("{file_name}_{market}_trade_break_down_report"),
                )
            })
    }
}

#[derive(Debug)]
pub struct TradeBreakDownReport {
    pub market: MarketKind,
    pub report: DataFrame,
}

impl From<PnLStatement> for TradeBreakDownReports {
    fn from(value: PnLStatement) -> Self {
        value
            .pnl_data
            .into_iter()
            .map(|(_, pnl_reports)| PnLSnapshot {
                pnl_reports,
                bot: value.strategy,
            })
            .map(|pnl_snapshot| pnl_snapshot.into())
            .collect()
    }
}

struct TradeBreakDownReportsBuilder {
    markets: Vec<MarketKind>,
    reports: HashMap<MarketKind, TradeBreakDownReport>,
}

impl TradeBreakDownReportsBuilder {
    fn new() -> Self {
        Self {
            markets: Vec::new(),
            reports: HashMap::new(),
        }
    }

    fn append(self, report: TradeBreakDownReport) -> Self {
        let market = report.market;
        let mut markets = self.markets;
        markets.push(market);

        let mut reports = self.reports;
        reports.insert(market, report);

        Self { markets, reports }
    }

    fn build(self) -> TradeBreakDownReports {
        TradeBreakDownReports {
            markets: self.markets,
            reports: self.reports,
        }
    }
}

impl FromIterator<TradeBreakDownReport> for TradeBreakDownReports {
    fn from_iter<T: IntoIterator<Item = TradeBreakDownReport>>(iter: T) -> Self {
        iter.into_iter()
            .fold(TradeBreakDownReportsBuilder::new(), |builder, i| {
                builder.append(i)
            })
            .build()
    }
}

pub struct PnLSnapshot {
    pub pnl_reports: PnLReports,
    pub bot: StrategyKind,
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
            .map(|df| df.lazy().append_strategy_col(self.bot))
            .collect();

        ldfs.concatenate_to_data_frame()
    }

    pub fn compute_performance_report(self) -> DataFrame {
        let ldfs: Vec<LazyFrame> = self
            .pnl_reports
            .reports
            .into_iter()
            .map(|(_, pnl_report)| pnl_report.as_performance_report_df())
            .map(|df| df.lazy().append_strategy_col(self.bot))
            .collect();

        ldfs.concatenate_to_data_frame()
    }
}
