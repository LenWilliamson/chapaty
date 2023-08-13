use polars::prelude::{col, df, DataFrame, IntoLazy, NamedFrom};
use serde::{Deserialize, Serialize};

use crate::{
    enums::column_names::{
        PerformanceReportColumnKind, PnLReportColumnKind, TradeBreakDownReportColumnKind,
    },
    lazy_frame_operations::trait_extensions::{MyLazyFrameOperations, MyLazyFrameVecOperations},
    MarketKind, data_frame_operations::io_operations::save_df_as_csv,
};

use super::{
    metrics::{
        accumulated_profit, avg_loss, avg_trade, avg_win, avg_win_by_avg_loose, max_draw_down_abs,
        max_draw_down_rel, net_profit, number_loser_trades, number_no_entry,
        number_timeout_loser_trades, number_timeout_trades, number_timeout_winner_trades,
        number_winner_trades, percent_profitability, profit_factor, timeout_loss, timeout_win,
        total_loss, total_number_loser_trades, total_number_trades, total_number_winner_trades,
        total_win,
    },
    pnl_statement_agg_markets::PnLStatementAggMarkets,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PnLStatementAggMarketsAggYears {
    pub strategy_name: String,
    pub markets: Vec<MarketKind>,
    pub years: Vec<u32>,
    pub pnl: DataFrame,
}

impl PnLStatementAggMarketsAggYears {
    pub fn save_as_csv(&self, file_name: &str) {
        save_df_as_csv(
            &mut self.pnl.clone(),
            &format!("{file_name}_all_markets_all_years_pnl"),
        )
    }
}

impl From<PnLStatementAggMarkets> for PnLStatementAggMarketsAggYears {
    fn from(value: PnLStatementAggMarkets) -> Self {
        let ldfs = value
            .pnl_data
            .years
            .iter()
            .fold(Vec::new(), |mut acc, year| {
                acc.push(value.pnl_data.reports.get(year).unwrap().pnl.clone().lazy());
                acc
            });
        let pnl = ldfs
            .concatenate_to_lazy_frame()
            .sort_by_date()
            .collect()
            .unwrap();
        Self {
            strategy_name: value.strategy_name,
            markets: value.markets,
            years: value.pnl_data.years,
            pnl,
        }
    }
}

impl PnLStatementAggMarketsAggYears {
    pub fn compute_performance_report(&self) -> DataFrame {
        let pl = self.pnl.clone();

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
            &TradeBreakDownReportColumnKind::Market.to_string() => &vec!["All Markets".to_string()],
            &TradeBreakDownReportColumnKind::Strategy.to_string() => &vec![self.strategy_name.to_string()],
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

    pub fn compute_trade_breakdown_report(&self) -> DataFrame {
        let date = PnLReportColumnKind::Date.to_string();
        let pl_dollar = PnLReportColumnKind::PlDollar.to_string();
        let pl = self.pnl.clone();
        let pl_agg = pl.clone()
            .lazy()
            .groupby([col(&date)])
            .agg([col(&pl_dollar).sum()])
            .collect()
            .unwrap();
        let net_profit = net_profit(pl.clone());
        let total_number_of_trades = total_number_trades(pl.clone());
        let accumulated_profit = accumulated_profit(pl_agg, 0.0);
        let total_number_winner = total_number_winner_trades(pl.clone());
        let avg_win = avg_win(pl.clone());
        let avg_loss = avg_loss(pl.clone());
        let total_win = total_win(pl.clone());
        let total_loss = total_loss(pl.clone());

        df!(
            &PerformanceReportColumnKind::Year.to_string() => &vec!["All Years".to_string()],
            &PerformanceReportColumnKind::Market.to_string() => &vec!["All Markets".to_string()],
            &PerformanceReportColumnKind::Strategy.to_string() => &vec![self.strategy_name.to_string()],
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

    pub fn compute_equity_curve(&self) -> Vec<f64> {
        let date = PnLReportColumnKind::Date.to_string();
        let pl_dollar = PnLReportColumnKind::PlDollar.to_string();
        let agg_pnl = self
            .pnl.clone()
            .lazy()
            .groupby([col(&date)])
            .agg([col(&pl_dollar).sum()])
            .collect()
            .unwrap();

        accumulated_profit(agg_pnl, 0.0)
    }
}
