use crate::{
    enums::column_names::{
        PerformanceReportColumnKind, PnLReportColumnKind, TradeBreakDownReportColumnKind,
    },
    lazy_frame_operations::trait_extensions::MyLazyFrameOperations,
    pnl::metrics::{
        accumulated_profit, avg_loss, avg_trade, avg_win, avg_win_by_avg_loose, max_draw_down_abs,
        max_draw_down_rel, net_profit, number_loser_trades, number_no_entry,
        number_timeout_loser_trades, number_timeout_trades, number_timeout_winner_trades,
        number_winner_trades, percent_profitability, profit_factor, timeout_loss, timeout_win,
        total_loss, total_number_loser_trades, total_number_trades, total_number_winner_trades,
        total_win,
    },
    MarketKind,
};
use polars::prelude::{col, df, DataFrame, IntoLazy};

pub struct PnLToReportRequest {
    pnl: DataFrame,
    year: Option<u32>,
    market: Option<MarketKind>,
    strategy: String,
    agg_years: bool,
    agg_markets: bool,
}

impl PnLToReportRequest {
    pub fn as_trade_breakdown_df(&self) -> DataFrame {
        let pl = self.pnl.clone();

        let total_number_of_trades = total_number_trades(pl.clone());
        let total_number_winner = total_number_winner_trades(pl.clone());
        let total_win = total_win(pl.clone());
        let total_loss = total_loss(pl.clone());
        let timeout_win = timeout_win(pl.clone());
        let timeout_loss = timeout_loss(pl.clone());
        let clean_win = total_win - timeout_win;
        let clean_loss = total_loss - timeout_loss;

        let year = if self.agg_years {
            "All Years".to_string()
        } else {
            self.year.unwrap().to_string()
        };

        let market = if self.agg_markets {
            "All Markets".to_string()
        } else {
            self.market.unwrap().to_string()
        };

        df!(
            &TradeBreakDownReportColumnKind::Year.to_string() => &vec![year],
            &TradeBreakDownReportColumnKind::Market.to_string() => &vec![market],
            &TradeBreakDownReportColumnKind::Strategy.to_string() => &vec![self.strategy.clone()],
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

    pub fn as_performance_report_df(&self) -> DataFrame {
        let pl = self.pnl.clone();
        let net_profit = net_profit(pl.clone());
        let total_number_of_trades = total_number_trades(pl.clone());
        let accumulated_profit = if self.agg_markets {
            let agg_pnl = pnl_aggregated_by_date(&pl);
            accumulated_profit(agg_pnl, 0.0)
        } else {
            accumulated_profit(pl.clone(), 0.0)
        };
        let total_number_winner = total_number_winner_trades(pl.clone());
        let avg_win = avg_win(pl.clone());
        let avg_loss = avg_loss(pl.clone());
        let total_win = total_win(pl.clone());
        let total_loss = total_loss(pl.clone());

        let year = if self.agg_years {
            "All Years".to_string()
        } else {
            self.year.unwrap().to_string()
        };

        let market = if self.agg_markets {
            "All Markets".to_string()
        } else {
            self.market.unwrap().to_string()
        };

        df!(
            &PerformanceReportColumnKind::Year.to_string() => &vec![year],
            &PerformanceReportColumnKind::Market.to_string() => &vec![market.to_string()],
            &PerformanceReportColumnKind::Strategy.to_string() => &vec![self.strategy.clone()],
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

pub fn as_equity_curve(pnl: &DataFrame, agg_markets: bool) -> Vec<f64> {
    if agg_markets {
        let agg_pnl = pnl_aggregated_by_date(pnl);
        accumulated_profit(agg_pnl, 0.0)
    } else {
        accumulated_profit(pnl.clone(), 0.0)
    }
}

fn pnl_aggregated_by_date(pnl: &DataFrame) -> DataFrame {
    let date = PnLReportColumnKind::Date.to_string();
    let pl_dollar = PnLReportColumnKind::PlDollar.to_string();
    pnl.clone()
        .lazy()
        .group_by([col(&date)])
        .agg([col(&pl_dollar).sum()])
        .sort_by_date()
        .collect()
        .unwrap()
}

#[derive(Clone)]
pub struct PnLToReportRequestBuilder {
    pnl: Option<DataFrame>,
    year: Option<u32>,
    market: Option<MarketKind>,
    strategy: Option<String>,
    agg_years: Option<bool>,
    agg_markets: Option<bool>,
}

impl PnLToReportRequestBuilder {
    pub fn new() -> Self {
        Self {
            year: None,
            pnl: None,
            market: None,
            strategy: None,
            agg_years: None,
            agg_markets: None,
        }
    }
    pub fn with_pnl(self, pnl: DataFrame) -> Self {
        Self {
            pnl: Some(pnl),
            ..self
        }
    }

    pub fn with_year(self, year: u32) -> Self {
        Self {
            year: Some(year),
            ..self
        }
    }

    pub fn with_market(self, market: MarketKind) -> Self {
        Self {
            market: Some(market),
            ..self
        }
    }

    pub fn with_strategy(self, strategy: String) -> Self {
        Self {
            strategy: Some(strategy),
            ..self
        }
    }

    pub fn is_agg_years(self, is_agg_years: bool) -> Self {
        Self {
            agg_years: Some(is_agg_years),
            ..self
        }
    }

    pub fn is_agg_markets(self, is_agg_markets: bool) -> Self {
        Self {
            agg_markets: Some(is_agg_markets),
            ..self
        }
    }

    pub fn build(self) -> PnLToReportRequest {
        PnLToReportRequest {
            pnl: self.pnl.unwrap(),
            year: self.year,
            market: self.market,
            strategy: self.strategy.unwrap(),
            agg_years: self.agg_years.unwrap(),
            agg_markets: self.agg_markets.unwrap(),
        }
    }
}
