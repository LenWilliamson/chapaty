use std::collections::HashMap;

use polars::prelude::{DataFrame, IntoLazy, LazyFrame};
use serde::{Deserialize, Serialize};

use crate::{
    converter::pnl_to_report::{as_equity_curve, PnLToReportRequestBuilder},
    data_frame_operations::io_operations::save_df_as_csv,
    equity_curve::EquityCurvesAggMarkets,
    lazy_frame_operations::trait_extensions::{MyLazyFrameVecOperations, MyLazyFrameOperations},
    performance_report::PerformanceReportAggMarkets,
    trade_breakdown_report::TradeBreakDownReportAggMarkets,
    MarketKind,
};

use super::pnl_statement::PnLStatement;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PnLStatementAggMarkets {
    pub strategy_name: String,
    pub markets: Vec<MarketKind>,
    pub years: Vec<u32>,
    pub pnl_data: HashMap<u32, DataFrame>,
}

impl PnLStatementAggMarkets {
    pub fn save_as_csv(&self, file_name: &str) {
        self.pnl_data.iter().for_each(|(year, pnl)| {
            save_df_as_csv(
                &mut pnl.clone(),
                &format!("{file_name}_all_markets_{year}_pnl"),
            )
        })
    }

    pub fn agg_year(&self) -> DataFrame {
        let ldfs = self.years.iter().fold(Vec::new(), |mut acc, year| {
            acc.push(self.pnl_data.get(year).unwrap().clone().lazy());
            acc
        });
        ldfs.concatenate_to_lazy_frame()
            .sort_by_date()
            .drop_columns(vec!["id"])
            .collect()
            .unwrap()
    }

    pub fn compute_trade_breakdown_report(&self) -> TradeBreakDownReportAggMarkets {
        let request_builder = PnLToReportRequestBuilder::new()
            .is_agg_markets(true)
            .is_agg_years(false);
        let trade_breakdown_reports = self
            .pnl_data
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
            .with_row_count("id", Some(1))
            .unwrap();

        TradeBreakDownReportAggMarkets {
            markets: self.markets.clone(),
            report: trade_breakdown_reports,
        }
    }
    pub fn compute_performance_report(&self) -> PerformanceReportAggMarkets {
        let request_builder = PnLToReportRequestBuilder::new()
            .is_agg_markets(true)
            .is_agg_years(false);
        let trade_breakdown_reports = self
            .pnl_data
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
            .with_row_count("id", Some(1))
            .unwrap();

        PerformanceReportAggMarkets {
            markets: self.markets.clone(),
            report: trade_breakdown_reports,
        }
    }

    pub fn compute_equity_curves(&self) -> EquityCurvesAggMarkets {
        let equity_curves = self
            .pnl_data
            .iter()
            .map(|(year, pnl_report)| (*year, as_equity_curve(&pnl_report, false)))
            .collect();

        EquityCurvesAggMarkets {
            markets: self.markets.clone(),
            years: self.years.clone(),
            curves: equity_curves,
        }
    }
}

impl From<PnLStatement> for PnLStatementAggMarkets {
    fn from(value: PnLStatement) -> Self {
        Self {
            strategy_name: value.strategy_name.clone(),
            markets: value.markets.clone(),
            years: value.get_years(),
            pnl_data: value.agg_markets(),
        }
    }
}
