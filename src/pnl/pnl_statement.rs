use crate::{
    converter::pnl_to_report::{as_equity_curve, PnLToReportRequestBuilder},
    enums::markets::MarketKind,
    equity_curve::market_and_year::{EquityCurves, EquityCurvesReport},
    lazy_frame_operations::trait_extensions::MyLazyFrameVecOperations,
    performance_report::market_and_year::PerformanceReports,
    trade_breakdown_report::market_and_year::TradeBreakdownReports,
};
use polars::prelude::{DataFrame, IntoLazy, LazyFrame};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::pnl_report::PnLReports;

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

    pub fn compute_trade_breakdown_report(&self) -> TradeBreakdownReports {
        let request_builder = PnLToReportRequestBuilder::new()
            .is_agg_markets(false)
            .is_agg_years(false);
        let trade_breakdown_reports: HashMap<MarketKind, DataFrame> = self
            .pnl_data
            .iter()
            .map(|(market, pnl_reports)| {
                (
                    *market,
                    pnl_reports
                        .reports
                        .iter()
                        .map(|(year, pnl_report)| {
                            request_builder.clone()
                                .with_pnl(pnl_report.pnl.clone())
                                .with_market(pnl_report.market)
                                .with_strategy(pnl_report.strategy.clone())
                                .with_year(*year)
                                .build()
                                .as_trade_breakdown_df()
                        })
                        .map(|df| df.lazy())
                        .collect::<Vec<LazyFrame>>()
                        .concatenate_to_data_frame()
                        .with_row_count("#", Some(1))
                        .unwrap(),
                )
            })
            .collect();

        TradeBreakdownReports {
            markets: self.markets.clone(),
            reports: trade_breakdown_reports,
        }
    }
    pub fn compute_performance_report(&self) -> PerformanceReports {
        let request_builder = PnLToReportRequestBuilder::new()
            .is_agg_markets(false)
            .is_agg_years(false);
        let trade_breakdown_reports: HashMap<MarketKind, DataFrame> = self
            .pnl_data
            .iter()
            .map(|(market, pnl_reports)| {
                (
                    *market,
                    pnl_reports
                        .reports
                        .iter()
                        .map(|(year, pnl_report)| {
                            request_builder.clone()
                                .with_pnl(pnl_report.pnl.clone())
                                .with_market(pnl_report.market)
                                .with_strategy(pnl_report.strategy.clone())
                                .with_year(*year)
                                .build()
                                .as_performance_report_df()
                        })
                        .map(|df| df.lazy())
                        .collect::<Vec<LazyFrame>>()
                        .concatenate_to_data_frame()
                        .with_row_count("#", Some(1))
                        .unwrap(),
                )
            })
            .collect();

        PerformanceReports {
            markets: self.markets.clone(),
            reports: trade_breakdown_reports,
        }
    }

    pub fn compute_equity_curves(&self) -> EquityCurvesReport {
        let equity_curves = self
            .pnl_data
            .iter()
            .map(|(market, pnl_reports)| {
                let curves = pnl_reports
                    .reports
                    .iter()
                    .map(|(year, pnl_report)| (*year, as_equity_curve(&pnl_report.pnl, false)))
                    .collect();
                (
                    *market,
                    EquityCurves {
                        market: *market,
                        years: pnl_reports.years.clone(),
                        curves,
                    },
                )
            })
            .collect();

        EquityCurvesReport {
            markets: self.markets.clone(),
            curves: equity_curves,
        }
    }
}
