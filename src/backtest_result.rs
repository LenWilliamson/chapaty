pub mod equity_curves;
pub mod metrics;
pub mod performance_report;
pub mod pnl_report;
pub mod pnl_statement;
pub mod trade_break_down_report;
use self::{
    equity_curves::EquityCurvesReport, performance_report::PerformanceReports,
    pnl_statement::PnLStatement, trade_break_down_report::TradeBreakDownReports,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct BacktestResult {
    pub pnl_statement: PnLStatement,
    pub performance_reports: PerformanceReports,
    pub trade_breakdown_reports: TradeBreakDownReports,
    pub equity_curves: EquityCurvesReport,
}

impl BacktestResult {
    pub fn save_as_csv(&self, file_name: &str) {
        self.pnl_statement.save_as_csv(file_name);
        self.performance_reports.save_as_csv(file_name);
        self.trade_breakdown_reports.save_as_csv(file_name);
    }
}
