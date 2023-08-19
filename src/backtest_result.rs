use crate::{
    data_frame_operations::io_operations::save_df_as_csv,
    equity_curve::{
        agg_markets_and_year::EquityCurvesAggMarket, market_and_agg_years::EquityCurvesAggYears,
        market_and_year::EquityCurvesReport,
    },
    performance_report::{
        agg_markets_and_year::PerformanceReportAggMarket,
        market_and_agg_years::PerformanceReportsAggYears, market_and_year::PerformanceReports,
    },
    pnl::{
        pnl_statement::PnLStatement, pnl_statement_agg_markets::PnLStatementAggMarkets,
        pnl_statement_agg_markets_and_agg_years::PnLStatementAggMarketsAggYears,
        pnl_statement_agg_years::PnLStatementAggYears,
    },
    trade_breakdown_report::{
        agg_markets_and_year::TradeBreakDownReportAggMarket,
        market_and_agg_years::TradeBreakDownReportsAggYears,
        market_and_year::TradeBreakdownReports,
    },
};

use polars::prelude::DataFrame;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct BacktestResult {
    market_and_year: MarketAndYearBacktestResult,
    agg_market_and_year: AggMarketAndYearBacktestResult,
    market_and_agg_year: MarketAndAggYearBacktestResult,
    agg_market_and_agg_year: AggMarketAndAggYearBacktestResult,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MarketAndYearBacktestResult {
    pub pnl_statement: PnLStatement,
    pub performance_reports: PerformanceReports,
    pub trade_breakdown_reports: TradeBreakdownReports,
    pub equity_curves: EquityCurvesReport,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AggMarketAndYearBacktestResult {
    pub pnl_statement: PnLStatementAggMarkets,
    pub performance_report: PerformanceReportAggMarket,
    pub trade_breakdown_report: TradeBreakDownReportAggMarket,
    pub equity_curves: EquityCurvesAggMarket,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MarketAndAggYearBacktestResult {
    pub pnl_statement: PnLStatementAggYears,
    pub performance_report: PerformanceReportsAggYears,
    pub trade_breakdown_report: TradeBreakDownReportsAggYears,
    pub equity_curves: EquityCurvesAggYears,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AggMarketAndAggYearBacktestResult {
    pub pnl_statement: PnLStatementAggMarketsAggYears,
    pub performance_report: DataFrame,
    pub trade_breakdown_report: DataFrame,
    pub equity_curve: Vec<f64>,
}

impl From<MarketAndYearBacktestResult> for BacktestResult {
    fn from(value: MarketAndYearBacktestResult) -> Self {
        let agg_market_and_year: AggMarketAndYearBacktestResult = value.clone().into();
        Self {
            market_and_year: value.clone(),
            agg_market_and_year: agg_market_and_year.clone(),
            market_and_agg_year: value.clone().into(),
            agg_market_and_agg_year: agg_market_and_year.into(),
        }
    }
}

impl From<MarketAndYearBacktestResult> for AggMarketAndYearBacktestResult {
    fn from(value: MarketAndYearBacktestResult) -> Self {
        let pnl_statement: PnLStatementAggMarkets = value.pnl_statement.clone().into();
        Self {
            pnl_statement: pnl_statement.clone(),
            performance_report: pnl_statement.clone().into(),
            trade_breakdown_report: pnl_statement.clone().into(),
            equity_curves: pnl_statement.clone().into(),
        }
    }
}

impl From<MarketAndYearBacktestResult> for MarketAndAggYearBacktestResult {
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

impl From<AggMarketAndYearBacktestResult> for AggMarketAndAggYearBacktestResult {
    fn from(value: AggMarketAndYearBacktestResult) -> Self {
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

impl AggMarketAndYearBacktestResult {
    pub fn save_as_csv(&self, file_name: &str) {
        self.pnl_statement.save_as_csv(file_name);
        self.performance_report.save_as_csv(file_name);
        self.trade_breakdown_report.save_as_csv(file_name);
    }
}
impl MarketAndAggYearBacktestResult {
    pub fn save_as_csv(&self, file_name: &str) {
        self.pnl_statement.save_as_csv(file_name);
        self.performance_report.save_as_csv(file_name);
        self.trade_breakdown_report.save_as_csv(file_name);
    }
}
impl AggMarketAndAggYearBacktestResult {
    pub fn save_as_csv(&self, file_name: &str) {
        self.pnl_statement.save_as_csv(file_name);
        let name = format!("{file_name}_all_markets_all_years_performance_report");
        save_df_as_csv(&mut self.performance_report.clone(), &name);
        let name = format!("{file_name}_all_markets_all_years_trade_breakdown_report");
        save_df_as_csv(&mut self.trade_breakdown_report.clone(), &name);
    }
}
