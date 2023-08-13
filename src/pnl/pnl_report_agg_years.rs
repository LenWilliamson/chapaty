use polars::prelude::{df, DataFrame, IntoLazy, NamedFrom};
use serde::{Deserialize, Serialize};

use super::{
    metrics::{
        accumulated_profit, avg_loss, avg_trade, avg_win, avg_win_by_avg_loose, max_draw_down_abs,
        max_draw_down_rel, net_profit, number_loser_trades, number_no_entry,
        number_timeout_loser_trades, number_timeout_trades, number_timeout_winner_trades,
        number_winner_trades, percent_profitability, profit_factor, timeout_loss, timeout_win,
        total_loss, total_number_loser_trades, total_number_trades, total_number_winner_trades,
        total_win,
    },
    pnl_report::PnLReports,
};
use crate::{
    enums::column_names::{PerformanceReportColumnKind, TradeBreakDownReportColumnKind},
    lazy_frame_operations::trait_extensions::{MyLazyFrameOperations, MyLazyFrameVecOperations},
    MarketKind, data_frame_operations::io_operations::save_df_as_csv,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PnLReportAggYears {
    pub market: MarketKind,
    pub years: Vec<u32>,
    pub strategy: String,
    pub pnl: DataFrame,
}

impl PnLReportAggYears {
    pub fn save_as_csv(&self, file_name: &str) {
        save_df_as_csv(
            &mut self.pnl.clone(),
            &format!("{file_name}_{}_all_years_pnl", self.market),
        )
    }
}

impl From<PnLReports> for PnLReportAggYears {
    fn from(value: PnLReports) -> Self {
        let strategy = value.reports.get(&value.years[0]).unwrap().strategy.clone();
        let ldfs = value.years.iter().fold(Vec::new(), |mut acc, year| {
            acc.push(value.reports.get(year).unwrap().pnl.clone().lazy());
            acc
        });
        let pnl = ldfs
            .concatenate_to_lazy_frame()
            .sort_by_date()
            .collect()
            .unwrap();

        Self {
            market: value.market,
            years: value.years,
            strategy,
            pnl,
        }
    }
}

impl PnLReportAggYears {
    pub fn as_trade_breakdown_df(self) -> DataFrame {
        let pl = self.pnl;

        let total_number_of_trades = total_number_trades(pl.clone());
        let total_number_winner = total_number_winner_trades(pl.clone());
        let total_win = total_win(pl.clone());
        let total_loss = total_loss(pl.clone());
        let timeout_win = timeout_win(pl.clone());
        let timeout_loss = timeout_loss(pl.clone());
        let clean_win = total_win - timeout_win;
        let clean_loss = total_loss - timeout_loss;

        df!(
            &TradeBreakDownReportColumnKind::Year.to_string() => &vec!["All Years".to_string()],
            &TradeBreakDownReportColumnKind::Market.to_string() => &vec![self.market.to_string()],
            &TradeBreakDownReportColumnKind::Strategy.to_string() => &vec![self.strategy.to_string()],
            &TradeBreakDownReportColumnKind::TotalWin.to_string() => &vec![total_win],
            &TradeBreakDownReportColumnKind::TotalLoss.to_string() => &vec![total_loss],
            &TradeBreakDownReportColumnKind::CleanWin.to_string() => &vec![clean_win],
            &TradeBreakDownReportColumnKind::TimeoutWin.to_string() => &vec![timeout_win],
            &TradeBreakDownReportColumnKind::CleanLoss.to_string() => &vec![clean_loss],
            &TradeBreakDownReportColumnKind::TimeoutLoss.to_string() => &vec![timeout_loss],
            &TradeBreakDownReportColumnKind::TotalNumberWinnerTrades.to_string() => &vec![total_number_winner],
            &TradeBreakDownReportColumnKind::TotalNumberLoserTrades.to_string() => &vec![total_number_loser_trades(pl.clone())],
            &TradeBreakDownReportColumnKind::TotalNumberTrades.to_string() => &vec![total_number_of_trades],
            &TradeBreakDownReportColumnKind::NumberWinnerTrades.to_string() => &vec![number_winner_trades(pl.clone())],
            &TradeBreakDownReportColumnKind::NumberLoserTrades.to_string() => &vec![number_loser_trades(pl.clone())],
            &TradeBreakDownReportColumnKind::NumberTimeoutWinnerTrades.to_string() => &vec![number_timeout_winner_trades(pl.clone())],
            &TradeBreakDownReportColumnKind::NumberTimeoutLoserTrades.to_string() => &vec![number_timeout_loser_trades(pl.clone())],
            &TradeBreakDownReportColumnKind::NumberTimeoutTrades.to_string() => &vec![number_timeout_trades(pl.clone())],
            &TradeBreakDownReportColumnKind::NumberNoEntry.to_string() => &vec![number_no_entry(pl.clone())],
        ).unwrap()
    }

    pub fn as_equity_curve(self) -> Vec<f64> {
        accumulated_profit(self.pnl, 0.0)
    }

    pub fn as_performance_report_df(self) -> DataFrame {
        let pl = self.pnl;
        let net_profit = net_profit(pl.clone());
        let total_number_of_trades = total_number_trades(pl.clone());
        let accumulated_profit = accumulated_profit(pl.clone(), 0.0);
        let total_number_winner = total_number_winner_trades(pl.clone());
        let avg_win = avg_win(pl.clone());
        let avg_loss = avg_loss(pl.clone());
        let total_win = total_win(pl.clone());
        let total_loss = total_loss(pl.clone());

        df!(
            &PerformanceReportColumnKind::Year.to_string() => &vec!["All Years".to_string()],
            &PerformanceReportColumnKind::Market.to_string() => &vec![self.market.to_string()],
            &PerformanceReportColumnKind::Strategy.to_string() => &vec![self.strategy.to_string()],
            &PerformanceReportColumnKind::NetProfit.to_string() => &vec![net_profit],
            &PerformanceReportColumnKind::AvgWinnByTrade.to_string() => &vec![avg_trade(net_profit, total_number_of_trades)],
            &PerformanceReportColumnKind::MaxDrawDownAbs.to_string() => &vec![max_draw_down_abs(&accumulated_profit)],
            &PerformanceReportColumnKind::MaxDrawDownRel.to_string() => &vec![max_draw_down_rel(&accumulated_profit)],
            &PerformanceReportColumnKind::PercentageProfitability.to_string() => &vec![percent_profitability(total_number_winner, total_number_of_trades)],
            &PerformanceReportColumnKind::RatioAvgWinByAvgLoss.to_string() => &vec![avg_win_by_avg_loose(avg_win, avg_loss)],
            &PerformanceReportColumnKind::AvgWin.to_string() => &vec![avg_win],
            &PerformanceReportColumnKind::AvgLoss.to_string() => &vec![avg_loss],
            &PerformanceReportColumnKind::ProfitFactor.to_string() => &vec![profit_factor(total_win, total_loss)],
        ).unwrap()
    }
}
