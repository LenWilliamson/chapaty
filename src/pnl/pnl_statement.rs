use crate::{
    converter::pnl_to_report::{as_equity_curve, PnLToReportRequestBuilder},
    enums::markets::MarketKind,
    equity_curve::{EquityCurves, EquityCurvesReport},
    lazy_frame_operations::trait_extensions::{MyLazyFrameOperations, MyLazyFrameVecOperations},
    performance_report::PerformanceReports,
    trade_breakdown_report::TradeBreakdownReports, PnLReportColumnKind,
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

    pub fn agg_markets(&self) -> HashMap<u32, DataFrame> {
        let years: &Vec<u32> = self.pnl_data[&self.markets[0]].years.as_ref();
        years
            .iter()
            .map(|year| (*year, self.get_agg_pnl_report_in_year(year)))
            .collect()
    }

    fn get_agg_pnl_report_in_year(&self, year: &u32) -> DataFrame {
        self.pnl_data
            .values()
            .map(|pnl_reports| pnl_reports.reports.get(year).unwrap().clone().lazy())
            .collect::<Vec<LazyFrame>>()
            .concatenate_to_lazy_frame()
            .sort_by_date_and_market()
            .drop_columns(vec![&PnLReportColumnKind::Id.to_string()])
            .collect()
            .unwrap()
            .with_row_count(&PnLReportColumnKind::Id.to_string(), Some(1))
            .unwrap()
    }

    pub fn get_years(&self) -> Vec<u32> {
        self.pnl_data[&self.markets[0]].years.clone()
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
                            request_builder
                                .clone()
                                .with_pnl(pnl_report.clone())
                                .with_market(*market)
                                .with_strategy(self.strategy_name.clone())
                                .with_year(*year)
                                .build()
                                .as_trade_breakdown_df()
                        })
                        .map(|df| df.lazy())
                        .collect::<Vec<LazyFrame>>()
                        .concatenate_to_data_frame()
                        .with_row_count(&PnLReportColumnKind::Id.to_string(), Some(1))
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
                            request_builder
                                .clone()
                                .with_pnl(pnl_report.clone())
                                .with_market(*market)
                                .with_strategy(self.strategy_name.clone())
                                .with_year(*year)
                                .build()
                                .as_performance_report_df()
                        })
                        .map(|df| df.lazy())
                        .collect::<Vec<LazyFrame>>()
                        .concatenate_to_data_frame()
                        .with_row_count(&PnLReportColumnKind::Id.to_string(), Some(1))
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
                    .map(|(year, pnl_report)| (*year, as_equity_curve(&pnl_report, false)))
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
