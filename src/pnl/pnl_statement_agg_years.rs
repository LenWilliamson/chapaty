use std::collections::HashMap;

use polars::prelude::DataFrame;
use serde::{Deserialize, Serialize};

use crate::{
    converter::pnl_to_report::{as_equity_curve, as_performance_report_df, as_trade_breakdown_df},
    data_frame_operations::io_operations::save_df_as_csv,
    equity_curve::market_and_agg_years::EquityCurvesAggYears,
    performance_report::market_and_agg_years::PerformanceReportsAggYears,
    trade_breakdown_report::market_and_agg_years::TradeBreakDownReportsAggYears,
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
        let trade_breakdown_reports = self
            .pnl_data
            .iter()
            .map(|(market, pnl)| {
                (
                    *market,
                    as_trade_breakdown_df(pnl, market, &self.strategy_name),
                )
            })
            .collect();

        TradeBreakDownReportsAggYears {
            markets: self.markets.clone(),
            reports: trade_breakdown_reports,
        }
    }

    pub fn compute_performance_reports(&self) -> PerformanceReportsAggYears {
        let performance_reports = self
            .pnl_data
            .iter()
            .map(|(market, pnl)| {
                (
                    *market,
                    as_performance_report_df(pnl, market, &self.strategy_name),
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
            .map(|(market, pnl)| (*market, as_equity_curve(pnl)))
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
            .map(|(market, pnl_reports)| (*market, pnl_reports.agg_year()))
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
