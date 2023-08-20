use polars::prelude::{IntoLazy, LazyFrame};
use serde::{Deserialize, Serialize};

use crate::{
    converter::pnl_to_report::{as_equity_curve, PnLToReportRequestBuilder},
    equity_curve::EquityCurvesAggMarket,
    lazy_frame_operations::trait_extensions::MyLazyFrameVecOperations,
    performance_report::PerformanceReportAggMarket,
    trade_breakdown_report::TradeBreakDownReportAggMarket,
    MarketKind,
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
    pub fn compute_trade_breakdown_report(&self) -> TradeBreakDownReportAggMarket {
        let request_builder = PnLToReportRequestBuilder::new()
            .is_agg_markets(true)
            .is_agg_years(false);
        let trade_breakdown_reports = self
            .pnl_data
            .reports
            .iter()
            .map(|(year, pnl_report)| {
                request_builder
                    .clone()
                    .with_pnl(pnl_report.clone())
                    .with_strategy(self.strategy_name.clone())
                    .with_year(*year)
                    .build()
                    .as_trade_breakdown_df()
            })
            .map(|df| df.lazy())
            .collect::<Vec<LazyFrame>>()
            .concatenate_to_data_frame()
            .with_row_count("#", Some(1))
            .unwrap();

        TradeBreakDownReportAggMarket {
            markets: self.markets.clone(),
            report: trade_breakdown_reports,
        }
    }
    pub fn compute_performance_report(&self) -> PerformanceReportAggMarket {
        let request_builder = PnLToReportRequestBuilder::new()
            .is_agg_markets(true)
            .is_agg_years(false);
        let trade_breakdown_reports = self
            .pnl_data
            .reports
            .iter()
            .map(|(year, pnl_report)| {
                request_builder
                    .clone()
                    .with_pnl(pnl_report.clone())
                    .with_strategy(self.strategy_name.clone())
                    .with_year(*year)
                    .build()
                    .as_performance_report_df()
            })
            .map(|df| df.lazy())
            .collect::<Vec<LazyFrame>>()
            .concatenate_to_data_frame()
            .with_row_count("#", Some(1))
            .unwrap();

        PerformanceReportAggMarket {
            markets: self.markets.clone(),
            report: trade_breakdown_reports,
        }
    }

    pub fn compute_equity_curves(&self) -> EquityCurvesAggMarket {
        let equity_curves = self
            .pnl_data
            .reports
            .iter()
            .map(|(year, pnl_report)| (*year, as_equity_curve(&pnl_report, false)))
            .collect();

        EquityCurvesAggMarket {
            markets: self.markets.clone(),
            years: self.pnl_data.years.clone(),
            curves: equity_curves,
        }
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
