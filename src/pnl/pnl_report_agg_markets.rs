use std::collections::HashMap;

use polars::prelude::{col, df, DataFrame, IntoLazy, NamedFrom};
use serde::{Deserialize, Serialize};

use crate::{
    data_frame_operations::io_operations::save_df_as_csv,
    enums::column_names::{
        PerformanceReportColumnKind, PnLReportColumnKind, TradeBreakDownReportColumnKind,
    },
    lazy_frame_operations::trait_extensions::{MyLazyFrameOperations, MyLazyFrameVecOperations},
    MarketKind,
};

use super::metrics::{
    accumulated_profit, avg_loss, avg_trade, avg_win, avg_win_by_avg_loose, max_draw_down_abs,
    max_draw_down_rel, net_profit, number_loser_trades, number_no_entry,
    number_timeout_loser_trades, number_timeout_trades, number_timeout_winner_trades,
    number_winner_trades, percent_profitability, profit_factor, timeout_loss, timeout_win,
    total_loss, total_number_loser_trades, total_number_trades, total_number_winner_trades,
    total_win,
};

use super::pnl_report::{PnLReport, PnLReports};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PnLReportsAggMarkets {
    pub markets: Vec<MarketKind>,
    pub years: Vec<u32>,
    pub reports: HashMap<u32, PnLReportAggMarkets>,
}

impl PnLReportsAggMarkets {
    pub fn save_as_csv(&self, file_name: &str) {
        self.reports.iter().for_each(|(year, data)| {
            save_df_as_csv(
                &mut data.pnl.clone(),
                &format!("{file_name}_all_markets_{year}_pnl"),
            )
        })
    }
}

pub struct PnLReportsAggMarketsBuilder {
    pub markets: Option<Vec<MarketKind>>,
    pub years: Option<Vec<u32>>,
    pub pnl_data: Option<HashMap<MarketKind, PnLReports>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PnLReportAggMarkets {
    pub markets: Vec<MarketKind>,
    pub year: u32,
    pub strategy: String,
    pub pnl: DataFrame,
}

impl PnLReportsAggMarketsBuilder {
    fn new() -> Self {
        Self {
            markets: None,
            years: None,
            pnl_data: None,
        }
    }

    fn with_markets(self, markets: Vec<MarketKind>) -> Self {
        Self {
            markets: Some(markets),
            ..self
        }
    }

    fn with_years(self, years: Vec<u32>) -> Self {
        Self {
            years: Some(years),
            ..self
        }
    }

    fn with_pnl_data_to_be_aggregated(self, pnl_data: HashMap<MarketKind, PnLReports>) -> Self {
        Self {
            pnl_data: Some(pnl_data),
            ..self
        }
    }

    fn build(self) -> PnLReportsAggMarkets {
        let reports = self.aggregate_pnl_data_by_markets();
        PnLReportsAggMarkets {
            markets: self.markets.unwrap(),
            years: self.years.unwrap(),
            reports,
        }
    }

    fn aggregate_pnl_data_by_markets(&self) -> HashMap<u32, PnLReportAggMarkets> {
        self.years
            .as_ref()
            .unwrap()
            .iter()
            .map(|year| (*year, self.get_agg_pnl_report_in_year(year)))
            .collect()
    }

    fn get_agg_pnl_report_in_year(&self, year: &u32) -> PnLReportAggMarkets {
        let pnl_data = self.pnl_data.as_ref().unwrap();
        pnl_data
            .values()
            .map(|pnl_reports| pnl_reports.reports.get(year).unwrap().clone())
            .collect()
    }
}

impl From<HashMap<MarketKind, PnLReports>> for PnLReportsAggMarkets {
    fn from(value: HashMap<MarketKind, PnLReports>) -> Self {
        let markets: Vec<MarketKind> = value.keys().map(|k| *k).collect();
        PnLReportsAggMarketsBuilder::new()
            .with_markets(markets.clone())
            .with_years(value.get(&markets[0]).unwrap().years.clone())
            .with_pnl_data_to_be_aggregated(value)
            .build()
    }
}

impl FromIterator<PnLReport> for PnLReportAggMarkets {
    fn from_iter<T: IntoIterator<Item = PnLReport>>(iter: T) -> Self {
        let markets = Vec::new();
        let (year, strategy, ldfs) = iter.into_iter().fold(
            (0, String::default(), Vec::new()),
            |mut df_acc, pnl_report| {
                df_acc.0 = pnl_report.year;
                df_acc.1 = pnl_report.strategy;
                df_acc.2.push(pnl_report.pnl.lazy());
                df_acc
            },
        );
        let pnl = ldfs
            .concatenate_to_lazy_frame()
            .sort_by_date()
            .collect()
            .unwrap();
        Self {
            markets,
            year,
            strategy,
            pnl,
        }
    }
}

impl PnLReportAggMarkets {
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
            &TradeBreakDownReportColumnKind::Year.to_string() => &vec![self.year],
            &TradeBreakDownReportColumnKind::Market.to_string() => &vec!["All Markets".to_string()],
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

    pub fn as_equity_curve(self) -> (u32, Vec<f64>) {
        let year = self.year;
        let date = PnLReportColumnKind::Date.to_string();
        let pl_dollar = PnLReportColumnKind::PlDollar.to_string();
        let agg_pnl = self
            .pnl
            .lazy()
            .groupby([col(&date)])
            .agg([col(&pl_dollar).sum()])
            .collect()
            .unwrap();

        (year, accumulated_profit(agg_pnl, 0.0))
    }

    pub fn as_performance_report_df(self) -> DataFrame {
        let date = PnLReportColumnKind::Date.to_string();
        let pl_dollar = PnLReportColumnKind::PlDollar.to_string();
        let pl = self.pnl;
        let pl_agg = pl
            .clone()
            .lazy()
            .groupby([col(&date)])
            .agg([col(&pl_dollar).sum()])
            .sort_by_date()
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
            &PerformanceReportColumnKind::Year.to_string() => &vec![self.year],
            &PerformanceReportColumnKind::Market.to_string() => &vec!["All Markets".to_string()],
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
