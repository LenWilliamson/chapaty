use crate::{
    bot::time_interval::timestamp_in_milli_to_string,
    calculator::pnl_report_data_row_calculator::PnLReportDataRow,
    converter::market_decimal_places::MyDecimalPlaces,
    data_frame_operations::io_operations::save_df_as_csv,
    enums::{
        bot::StrategyKind,
        column_names::{self, PerformanceReportColumnKind, TradeBreakDownReportColumnKind},
    },
};
use std::{convert::identity, collections::HashMap};

use super::metrics::{
    accumulated_profit, avg_loss, avg_trade, avg_win, avg_win_by_avg_loose, max_draw_down_abs,
    max_draw_down_rel, net_profit, number_loser_trades, number_no_entry,
    number_timeout_loser_trades, number_timeout_trades, number_timeout_winner_trades,
    number_winner_trades, percent_profitability, profit_factor, timeout_loss, timeout_win,
    total_loss, total_number_loser_trades, total_number_trades, total_number_winner_trades,
    total_win,
};
use crate::enums::markets::MarketKind;
use crate::lazy_frame_operations::trait_extensions::MyLazyFrameVecOperations;
use chrono::NaiveDate;
use polars::df;
use polars::prelude::NamedFrom;
use polars::prelude::{DataFrame, IntoLazy, LazyFrame};

use super::equity_curves::EquityCurve;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PnLReports {
    pub market: MarketKind,
    pub years: Vec<u32>,
    pub reports: HashMap<u32, PnLReport>,
}

impl PnLReports {
    pub fn save_as_csv(&self, file_name: &str) {
        self.reports.iter().for_each(|(year, data)| {
            save_df_as_csv(
                &mut data.pnl.clone(),
                &format!("{file_name}_{}_{year}_pnl", self.market),
            )
        })
    }
}

impl FromIterator<PnLReport> for PnLReports {
    fn from_iter<T: IntoIterator<Item = PnLReport>>(iter: T) -> Self {
        iter.into_iter()
            .fold(PnLReportsBuilder::new(), |builder, i| builder.append(i))
            .build()
    }
}

struct PnLReportsBuilder {
    market: Option<MarketKind>,
    years: Vec<u32>,
    reports: HashMap<u32, PnLReport>,
}

impl PnLReportsBuilder {
    pub fn new() -> Self {
        Self {
            market: None,
            years: Vec::new(),
            reports: HashMap::new(),
        }
    }

    pub fn append(self, pnl_report: PnLReport) -> Self {
        let market = pnl_report.market;
        let year = pnl_report.year;
        let mut years = self.years;
        years.push(year);

        let mut reports = self.reports;
        reports.insert(year, pnl_report);

        Self {
            market: Some(market),
            years,
            reports,
        }
    }

    pub fn build(self) -> PnLReports {
        PnLReports {
            market: self.market.unwrap(),
            years: self.years,
            reports: self.reports,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PnLReport {
    pub market: MarketKind,
    pub year: u32,
    pub strategy: StrategyKind,
    pub pnl: DataFrame,
}

impl PnLReport {
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

    pub fn as_equity_curve(self) -> (u32, EquityCurve) {
        let year = self.year;
        let equity_curve = EquityCurve {
            market: self.market,
            year,
            curve: accumulated_profit(self.pnl, 0.0),
        };

        (year, equity_curve)
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
            &PerformanceReportColumnKind::Year.to_string() => &vec![self.year],
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

impl PnLReportDataRow {
    fn report_with_trade(self) -> DataFrame {
        let tick_factor = self.get_tick_factor();
        let tick_to_dollar = self.get_tick_to_dollar_conversion_factor();
        let trade_pnl = self.trade_pnl.clone().unwrap();

        let cw = self.time_frame_snapshot.get_calendar_week_as_int();
        let date = self.get_date();
        let strategy = self.strategy.to_string().to_uppercase();
        let market = self.market.to_string();
        let trade_direction = self.trade.trade_kind.to_string();
        let entry_price = self.trade.entry_price;
        let take_profit = self.trade.take_profit;
        let stop_loss = self.trade.stop_loss;
        let expected_win_tick = self.expected_win_in_tick(tick_factor);
        let expected_loss_tick = self.expected_loss_in_tick(tick_factor);
        let expected_win_dollar = expected_win_tick * tick_to_dollar;
        let expected_loss_dollar = expected_loss_tick * tick_to_dollar;
        let crv = (expected_win_tick / expected_loss_tick).abs();
        let entry_ts = self.get_entry_ts();
        let take_profit_ts = self.get_take_profit_ts();
        let stop_loss_ts = self.get_stop_loss_ts();
        let exit_price = trade_pnl.exit_price();
        let status = trade_pnl.trade_outcome();
        let pl_tick = trade_pnl.profit() / tick_factor;
        let pl_dollar = pl_tick * tick_to_dollar;

        let n = self.get_decimal_places();

        df!(
            &column_names::PnLReportColumnKind::CalendarWeek.to_string() =>vec![cw],
            &column_names::PnLReportColumnKind::Date.to_string() =>vec![date],
            &column_names::PnLReportColumnKind::Strategy.to_string() =>vec![strategy],
            &column_names::PnLReportColumnKind::Market.to_string() =>vec![market],
            &column_names::PnLReportColumnKind::TradeDirection.to_string() =>vec![trade_direction],
            &column_names::PnLReportColumnKind::Entry.to_string() =>vec![entry_price.round_to_n_decimal_places(n)],
            &column_names::PnLReportColumnKind::TakeProfit.to_string() =>vec![take_profit.round_to_n_decimal_places(n)],
            &column_names::PnLReportColumnKind::StopLoss.to_string() =>vec![stop_loss.round_to_n_decimal_places(n)],
            &column_names::PnLReportColumnKind::ExpectedWinTick.to_string() =>vec![expected_win_tick],
            &column_names::PnLReportColumnKind::ExpectedLossTick.to_string() =>vec![expected_loss_tick],
            &column_names::PnLReportColumnKind::ExpectedWinDollar.to_string() =>vec![expected_win_dollar.round_to_dollar_cents()],
            &column_names::PnLReportColumnKind::ExpectedLossDollar.to_string() =>vec![expected_loss_dollar.round_to_dollar_cents()],
            &column_names::PnLReportColumnKind::Crv.to_string() =>vec![crv.round_to_n_decimal_places(3)],
            &column_names::PnLReportColumnKind::EntryTimestamp.to_string() =>vec![entry_ts],
            &column_names::PnLReportColumnKind::TakeProfitTimestamp.to_string() =>vec![take_profit_ts],
            &column_names::PnLReportColumnKind::StopLossTimestamp.to_string() =>vec![stop_loss_ts],
            &column_names::PnLReportColumnKind::ExitPrice.to_string() =>vec![exit_price.round_to_n_decimal_places(n)],
            &column_names::PnLReportColumnKind::Status.to_string() =>vec![status],
            &column_names::PnLReportColumnKind::PlTick.to_string() =>vec![pl_tick.round_to_n_decimal_places(n)],
            &column_names::PnLReportColumnKind::PlDollar.to_string() =>vec![pl_dollar.round_to_dollar_cents()],
        ).unwrap()
    }

    fn report_without_trade(self) -> DataFrame {
        let tick_factor = self.get_tick_factor();
        let tick_to_dollar = self.get_tick_to_dollar_conversion_factor();

        let cw = self.time_frame_snapshot.get_calendar_week_as_int();
        let date = self.get_date();
        let strategy = self.strategy.to_string().to_uppercase();
        let market = self.market.to_string();
        let trade_direction = self.trade.trade_kind.to_string();
        let entry_price = self.trade.entry_price;
        let take_profit = self.trade.take_profit;
        let stop_loss = self.trade.stop_loss;
        let expected_win_tick = self.expected_win_in_tick(tick_factor);
        let expected_loss_tick = self.expected_loss_in_tick(tick_factor);
        let expected_win_dollar = expected_win_tick * tick_to_dollar;
        let expected_loss_dollar = expected_loss_tick * tick_to_dollar;
        let crv = (expected_win_tick / expected_loss_tick).abs();

        let n = self.get_decimal_places();

        df!(
            &column_names::PnLReportColumnKind::CalendarWeek.to_string() =>vec![cw],
            &column_names::PnLReportColumnKind::Date.to_string() =>vec![date],
            &column_names::PnLReportColumnKind::Strategy.to_string() =>vec![strategy],
            &column_names::PnLReportColumnKind::Market.to_string() =>vec![market],
            &column_names::PnLReportColumnKind::TradeDirection.to_string() =>vec![trade_direction],
            &column_names::PnLReportColumnKind::Entry.to_string() =>vec![entry_price.round_to_n_decimal_places(n)],
            &column_names::PnLReportColumnKind::TakeProfit.to_string() =>vec![take_profit.round_to_n_decimal_places(n)],
            &column_names::PnLReportColumnKind::StopLoss.to_string() =>vec![stop_loss.round_to_n_decimal_places(n)],
            &column_names::PnLReportColumnKind::ExpectedWinTick.to_string() =>vec![expected_win_tick],
            &column_names::PnLReportColumnKind::ExpectedLossTick.to_string() =>vec![expected_loss_tick],
            &column_names::PnLReportColumnKind::ExpectedWinDollar.to_string() =>vec![expected_win_dollar.round_to_dollar_cents()],
            &column_names::PnLReportColumnKind::ExpectedLossDollar.to_string() =>vec![expected_loss_dollar.round_to_dollar_cents()],
            &column_names::PnLReportColumnKind::Crv.to_string() =>vec![crv.round_to_n_decimal_places(3)],
            &column_names::PnLReportColumnKind::EntryTimestamp.to_string() => &["NoEntry".to_string()],
            &column_names::PnLReportColumnKind::TakeProfitTimestamp.to_string() => &["NoEntry".to_string()],
            &column_names::PnLReportColumnKind::StopLossTimestamp.to_string() => &["NoEntry".to_string()],
            &column_names::PnLReportColumnKind::ExitPrice.to_string() => &[0.0],
            &column_names::PnLReportColumnKind::Status.to_string() => &["NoEntry".to_string()],
            &column_names::PnLReportColumnKind::PlTick.to_string() => &[0.0],
            &column_names::PnLReportColumnKind::PlDollar.to_string() => &[0.0],
        )
        .unwrap()
    }

    fn get_date(&self) -> String {
        let cw = self.time_frame_snapshot.get_calendar_week_as_int();
        let day = self.time_frame_snapshot.get_weekday();
        NaiveDate::from_isoywd_opt(
            i32::try_from(self.year).unwrap(),
            u32::try_from(cw).unwrap(),
            day,
        )
        .unwrap()
        .format("%Y-%m-%d")
        .to_string()
    }

    fn get_decimal_places(&self) -> i32 {
        self.market.decimal_places()
    }

    fn get_tick_factor(&self) -> f64 {
        self.market.tick_step_size().map_or_else(|| 1.0, identity)
    }

    fn get_tick_to_dollar_conversion_factor(&self) -> f64 {
        self.market
            .tik_to_dollar_conversion_factor()
            .map_or_else(|| 1.0, identity)
    }

    fn get_entry_ts(&self) -> String {
        let trade_pnl = self.trade_pnl.clone().unwrap();
        timestamp_in_milli_to_string(trade_pnl.trade_entry_ts)
    }

    fn get_take_profit_ts(&self) -> String {
        let trade_pnl = self.trade_pnl.clone().unwrap();
        trade_pnl.clone().take_profit.map_or_else(
            || "Timeout".to_string(),
            |pnl| timestamp_in_milli_to_string(pnl.ts.unwrap()),
        )
    }

    fn get_stop_loss_ts(&self) -> String {
        let trade_pnl = self.trade_pnl.clone().unwrap();
        trade_pnl.clone().stop_loss.map_or_else(
            || "Timeout".to_string(),
            |pnl| timestamp_in_milli_to_string(pnl.ts.unwrap()),
        )
    }

    fn expected_win_in_tick(&self, tick_factor: f64) -> f64 {
        (self.trade.profit(self.trade.take_profit) / tick_factor).round()
    }

    fn expected_loss_in_tick(&self, tick_factor: f64) -> f64 {
        (self.trade.profit(self.trade.stop_loss) / tick_factor).round()
    }
}

pub struct PnLReportBuilder {
    market: Option<MarketKind>,
    year: Option<u32>,
    strategy: Option<StrategyKind>,
    data_rows: Option<Vec<LazyFrame>>,
}

impl From<PnLReportDataRow> for DataFrame {
    fn from(value: PnLReportDataRow) -> Self {
        match value.trade_pnl {
            Some(_) => value.report_with_trade(),
            None => value.report_without_trade(),
        }
    }
}

impl FromIterator<PnLReportDataRow> for PnLReport {
    fn from_iter<T: IntoIterator<Item = PnLReportDataRow>>(iter: T) -> Self {
        iter.into_iter()
            .fold(PnLReportBuilder::new(), |builder, i| builder.append(i))
            .build()
    }
}

impl PnLReportBuilder {
    pub fn new() -> Self {
        Self {
            market: None,
            year: None,
            strategy: None,
            data_rows: None,
        }
    }

    pub fn append(self, row: PnLReportDataRow) -> Self {
        let row_as_df: DataFrame = row.clone().into();
        let data_rows = match self.data_rows {
            Some(mut v) => {
                v.push(row_as_df.lazy());
                v
            }
            None => vec![row_as_df.lazy()],
        };
        Self {
            market: Some(row.market),
            year: Some(row.year),
            strategy: Some(row.strategy),
            data_rows: Some(data_rows),
        }
    }

    pub fn build(self) -> PnLReport {
        PnLReport {
            market: self.market.unwrap(),
            strategy: self.strategy.unwrap(),
            year: self.year.unwrap(),
            pnl: self
                .data_rows
                .unwrap()
                .concatenate_to_data_frame()
                .with_row_count("#", Some(1))
                .unwrap(),
        }
    }
}
