use std::collections::HashMap;

use polars::prelude::DataFrame;
use serde::{Deserialize, Serialize};

use crate::{
    converter::pnl_to_report::{as_equity_curve, PnLToReportRequestBuilder},
    data_frame_operations::io_operations::save_df_as_csv,
    equity_curve::EquityCurvesAggYears,
    performance_report::PerformanceReportsAggYears,
    trade_breakdown_report::TradeBreakDownReportsAggYears,
    MarketKind,
};

use super::pnl_statement::PnLStatement;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PnLStatementAggYears {
    pub strategy_name: String,
    pub markets: Vec<MarketKind>,
    pub years: Vec<u32>,
    pub pnl_data: HashMap<MarketKind, DataFrame>,
}

impl PnLStatementAggYears {
    pub fn save_as_csv(&self, file_name: &str) {
        self.pnl_data.iter().for_each(|(market, data)| {
            save_df_as_csv(
                &mut data.clone(),
                &format!("{file_name}_{}_all_years_pnl", market),
            )
        })
    }

    pub fn compute_trade_breakdown_reports(&self) -> TradeBreakDownReportsAggYears {
        let request_builder = PnLToReportRequestBuilder::new()
            .is_agg_markets(false)
            .is_agg_years(true);
        let trade_breakdown_reports = self
            .pnl_data
            .iter()
            .map(|(market, pnl)| {
                (
                    *market,
                    request_builder
                        .clone()
                        .with_pnl(pnl.clone())
                        .with_market(*market)
                        .with_strategy(self.strategy_name.clone())
                        .build()
                        .as_trade_breakdown_df()
                        .with_row_count("id", Some(1))
                        .unwrap(),
                )
            })
            .collect();

        TradeBreakDownReportsAggYears {
            markets: self.markets.clone(),
            reports: trade_breakdown_reports,
        }
    }

    pub fn compute_performance_reports(&self) -> PerformanceReportsAggYears {
        let request_builder = PnLToReportRequestBuilder::new()
            .is_agg_markets(false)
            .is_agg_years(true);
        let performance_reports = self
            .pnl_data
            .iter()
            .map(|(market, pnl)| {
                (
                    *market,
                    request_builder
                        .clone()
                        .with_pnl(pnl.clone())
                        .with_market(*market)
                        .with_strategy(self.strategy_name.clone())
                        .build()
                        .as_performance_report_df()
                        .with_row_count("id", Some(1))
                        .unwrap(),
                )
            })
            .collect();

        PerformanceReportsAggYears {
            markets: self.markets.clone(),
            reports: performance_reports,
        }
    }

    pub fn compute_equity_curves(&self) -> EquityCurvesAggYears {
        let equity_curves = self
            .pnl_data
            .iter()
            .map(|(market, pnl)| (*market, as_equity_curve(pnl, false)))
            .collect();

        EquityCurvesAggYears {
            markets: self.markets.clone(),
            years: self.years.clone(),
            curves: equity_curves,
        }
    }
}

impl From<PnLStatement> for PnLStatementAggYears {
    fn from(value: PnLStatement) -> Self {
        let pnl_data = value
            .pnl_data
            .iter()
            .map(|(market, pnl_reports)| {
                (
                    *market,
                    pnl_reports.agg_year().with_row_count("id", Some(1)).unwrap(),
                )
            })
            .collect();
        let years = value.pnl_data[&value.markets[0]].years.clone();
        Self {
            strategy_name: value.strategy_name,
            markets: value.markets,
            years,
            pnl_data,
        }
    }
}
