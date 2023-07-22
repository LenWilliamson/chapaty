pub mod equity_curves;
pub mod metrics;
pub mod performance_report;
pub mod pnl_report;
pub mod pnl_statement;
pub mod trade_breakdown_report;
use self::{
    equity_curves::EquityCurvesReport, performance_report::PerformanceReports,
    pnl_statement::PnLStatement, trade_breakdown_report::TradeBreakDownReports,
};

#[derive(Debug)]
pub struct BacktestResult {
    pub pnl_statement: PnLStatement,
    pub performance_report: PerformanceReports,
    pub trade_breakdown_report: TradeBreakDownReports,
    pub equity_curves: EquityCurvesReport,
}
