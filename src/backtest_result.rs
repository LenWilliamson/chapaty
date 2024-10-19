use crate::{
    data_frame_operations::io_operations::save_df_as_csv,
    equity_curve::{EquityCurvesAggMarkets, EquityCurvesAggYears, EquityCurvesReport},
    performance_report::{
        PerformanceReportAggMarkets, PerformanceReports, PerformanceReportsAggYears,
    },
    pnl::{
        pnl_statement::PnLStatement, pnl_statement_agg_markets::PnLStatementAggMarkets,
        pnl_statement_agg_markets_and_agg_years::PnLStatementAggMarketsAggYears,
        pnl_statement_agg_years::PnLStatementAggYears,
    },
    trade_breakdown_report::{
        TradeBreakDownReportAggMarkets, TradeBreakDownReportsAggYears, TradeBreakdownReports,
    },
};

use polars::prelude::DataFrame;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct BacktestResult {
    pub market_and_year: MarketAndYearBacktestResult,
    pub agg_market_and_year: AggMarketsAndYearBacktestResult,
    pub market_and_agg_year: MarketAndAggYearsBacktestResult,
    pub agg_market_and_agg_year: AggMarketsAndAggYearsBacktestResult,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MarketAndYearBacktestResult {
    pub pnl_statement: PnLStatement,
    pub performance_reports: PerformanceReports,
    pub trade_breakdown_reports: TradeBreakdownReports,
    pub equity_curves: EquityCurvesReport,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AggMarketsAndYearBacktestResult {
    pub pnl_statement: PnLStatementAggMarkets,
    pub performance_report: PerformanceReportAggMarkets,
    pub trade_breakdown_report: TradeBreakDownReportAggMarkets,
    pub equity_curves: EquityCurvesAggMarkets,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MarketAndAggYearsBacktestResult {
    pub pnl_statement: PnLStatementAggYears,
    pub performance_report: PerformanceReportsAggYears,
    pub trade_breakdown_report: TradeBreakDownReportsAggYears,
    pub equity_curves: EquityCurvesAggYears,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AggMarketsAndAggYearsBacktestResult {
    pub pnl_statement: PnLStatementAggMarketsAggYears,
    pub performance_report: DataFrame,
    pub trade_breakdown_report: DataFrame,
    pub equity_curve: Vec<f64>,
}

impl From<MarketAndYearBacktestResult> for BacktestResult {
    fn from(value: MarketAndYearBacktestResult) -> Self {
        let agg_market_and_year: AggMarketsAndYearBacktestResult = value.clone().into();
        Self {
            market_and_year: value.clone(),
            agg_market_and_year: agg_market_and_year.clone(),
            market_and_agg_year: value.clone().into(),
            agg_market_and_agg_year: agg_market_and_year.into(),
        }
    }
}

impl From<MarketAndYearBacktestResult> for AggMarketsAndYearBacktestResult {
    fn from(value: MarketAndYearBacktestResult) -> Self {
        let pnl_statement: PnLStatementAggMarkets = value.pnl_statement.clone().into();
        Self {
            pnl_statement: pnl_statement.clone(),
            performance_report: pnl_statement.compute_performance_report(),
            trade_breakdown_report: pnl_statement.compute_trade_breakdown_report(),
            equity_curves: pnl_statement.compute_equity_curves(),
        }
    }
}

impl From<MarketAndYearBacktestResult> for MarketAndAggYearsBacktestResult {
    fn from(value: MarketAndYearBacktestResult) -> Self {
        let pnl_statement: PnLStatementAggYears = value.pnl_statement.clone().into();
        Self {
            pnl_statement: pnl_statement.clone(),
            performance_report: pnl_statement.compute_performance_reports(),
            trade_breakdown_report: pnl_statement.compute_trade_breakdown_reports(),
            equity_curves: pnl_statement.compute_equity_curves(),
        }
    }
}

impl From<AggMarketsAndYearBacktestResult> for AggMarketsAndAggYearsBacktestResult {
    fn from(value: AggMarketsAndYearBacktestResult) -> Self {
        let pnl_statement: PnLStatementAggMarketsAggYears = value.pnl_statement.into();
        Self {
            pnl_statement: pnl_statement.clone(),
            performance_report: pnl_statement.compute_performance_report(),
            trade_breakdown_report: pnl_statement.compute_trade_breakdown_report(),
            equity_curve: pnl_statement.compute_equity_curve(),
        }
    }
}

impl BacktestResult {
    pub fn save_as_csv(&self, file_name: &str) {
        self.market_and_year.save_as_csv(file_name);
        self.agg_market_and_year.save_as_csv(file_name);
        self.market_and_agg_year.save_as_csv(file_name);
        self.agg_market_and_agg_year.save_as_csv(file_name);
    }
}

impl MarketAndYearBacktestResult {
    pub fn save_as_csv(&self, file_name: &str) {
        self.pnl_statement.save_as_csv(file_name);
        self.performance_reports.save_as_csv(file_name);
        self.trade_breakdown_reports.save_as_csv(file_name);
    }
}

impl AggMarketsAndYearBacktestResult {
    pub fn save_as_csv(&self, file_name: &str) {
        self.pnl_statement.save_as_csv(file_name);
        self.performance_report.save_as_csv(file_name);
        self.trade_breakdown_report.save_as_csv(file_name);
    }
}
impl MarketAndAggYearsBacktestResult {
    pub fn save_as_csv(&self, file_name: &str) {
        self.pnl_statement.save_as_csv(file_name);
        self.performance_report.save_as_csv(file_name);
        self.trade_breakdown_report.save_as_csv(file_name);
    }
}
impl AggMarketsAndAggYearsBacktestResult {
    pub fn save_as_csv(&self, file_name: &str) {
        self.pnl_statement.save_as_csv(file_name);
        let name = format!("{file_name}_all_markets_all_years_performance_report");
        save_df_as_csv(&mut self.performance_report.clone(), &name);
        let name = format!("{file_name}_all_markets_all_years_trade_breakdown_report");
        save_df_as_csv(&mut self.trade_breakdown_report.clone(), &name);
    }
}
