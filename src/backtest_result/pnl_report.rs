use std::collections::HashMap;
use std::convert::identity;

use crate::bot::time_frame_snapshot::TimeFrameSnapshot;
use crate::bot::time_interval::timestamp_in_milli_to_string;
use crate::bot::trade::Trade;
use crate::calculator::trade_pnl_calculator::TradePnL;
use crate::converter::market_decimal_places::MyDecimalPlaces;
use crate::data_frame_operations::save_df_as_csv;
use crate::enums::bots::StrategyKind;

use super::metrics::{
    accumulated_profit, avg_loss, avg_trade, avg_win, avg_win_by_avg_loose, max_draw_down_abs,
    max_draw_down_rel, net_profit, number_loser_trades, number_no_entry,
    number_timeout_loser_trades, number_timeout_trades, number_timeout_winner_trades,
    number_winner_trades, percent_profitability, profit_factor, timeout_loss, timeout_win,
    total_loss, total_number_loser_trades, total_number_trades, total_number_winner_trades,
    total_win,
};
use crate::enums::columns::{PerformanceStatisticColumnNames, ProfitAndLossColumnNames};
use crate::enums::markets::MarketKind;
use crate::lazy_frame_operations::trait_extensions::MyLazyFrameVecOperations;
use chrono::NaiveDate;
use polars::df;
use polars::prelude::NamedFrom;
use polars::prelude::{DataFrame, IntoLazy, LazyFrame};

use super::equity_curves::EquityCurve;

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
pub struct PnLReport {
    pub market: MarketKind,
    pub year: u32,
    pub pnl: DataFrame,
}

impl PnLReport {
    pub fn as_trade_breakdown_df(self) -> DataFrame {
        let pl = self.pnl;
        let year = self.year;
        let market = self.market;

        let total_number_of_trades = total_number_trades(pl.clone());
        let total_number_winner = total_number_winner_trades(pl.clone());
        let total_win = total_win(pl.clone());
        let total_loss = total_loss(pl.clone());
        let timeout_win = timeout_win(pl.clone());
        let timeout_loss = timeout_loss(pl.clone());
        let clean_win = total_win - timeout_win;
        let clean_loss = total_loss - timeout_loss;

        df!(
            &PerformanceStatisticColumnNames::Year.to_string() => &vec![year],
            &PerformanceStatisticColumnNames::Market.to_string() => &vec![market.to_string()],
            &PerformanceStatisticColumnNames::TotalWin.to_string() => &vec![total_win],
            &PerformanceStatisticColumnNames::TotalLoss.to_string() => &vec![total_loss],
            &PerformanceStatisticColumnNames::CleanWin.to_string() => &vec![clean_win],
            &PerformanceStatisticColumnNames::TimeoutWin.to_string() => &vec![timeout_win],
            &PerformanceStatisticColumnNames::CleanLoss.to_string() => &vec![clean_loss],
            &PerformanceStatisticColumnNames::TimeoutLoss.to_string() => &vec![timeout_loss],
            &PerformanceStatisticColumnNames::TotalNumberWinnerTrades.to_string() => &vec![total_number_winner],
            &PerformanceStatisticColumnNames::TotalNumberLoserTrades.to_string() => &vec![total_number_loser_trades(pl.clone())],
            &PerformanceStatisticColumnNames::TotalNumberTrades.to_string() => &vec![total_number_of_trades],
            &PerformanceStatisticColumnNames::NumberWinnerTrades.to_string() => &vec![number_winner_trades(pl.clone())],
            &PerformanceStatisticColumnNames::NumberLoserTrades.to_string() => &vec![number_loser_trades(pl.clone())],
            &PerformanceStatisticColumnNames::NumberTimeoutWinnerTrades.to_string() => &vec![number_timeout_winner_trades(pl.clone())],
            &PerformanceStatisticColumnNames::NumberTimeoutLoserTrades.to_string() => &vec![number_timeout_loser_trades(pl.clone())],
            &PerformanceStatisticColumnNames::NumberTimeoutTrades.to_string() => &vec![number_timeout_trades(pl.clone())],
            &PerformanceStatisticColumnNames::NumberNoEntry.to_string() => &vec![number_no_entry(pl.clone())],
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
        let year = self.year;
        let market = self.market;

        let net_profit = net_profit(pl.clone());
        let total_number_of_trades = total_number_trades(pl.clone());
        let accumulated_profit = accumulated_profit(pl.clone(), 0.0);
        let total_number_winner = total_number_winner_trades(pl.clone());
        let avg_win = avg_win(pl.clone());
        let avg_loss = avg_loss(pl.clone());
        let total_win = total_win(pl.clone());
        let total_loss = total_loss(pl.clone());

        df!(
        &PerformanceStatisticColumnNames::Year.to_string() => &vec![year],
        &PerformanceStatisticColumnNames::Market.to_string() => &vec![market.to_string()],
        &PerformanceStatisticColumnNames::NetProfit.to_string() => &vec![net_profit],
        &PerformanceStatisticColumnNames::AvgWinnByTrade.to_string() => &vec![avg_trade(net_profit, total_number_of_trades)],
        &PerformanceStatisticColumnNames::MaxDrawDownAbs.to_string() => &vec![max_draw_down_abs(&accumulated_profit)],
        &PerformanceStatisticColumnNames::MaxDrawDownRel.to_string() => &vec![max_draw_down_rel(&accumulated_profit)],
        &PerformanceStatisticColumnNames::PercentageProfitability.to_string() => &vec![percent_profitability(total_number_winner, total_number_of_trades)],
        &PerformanceStatisticColumnNames::RatioAvgWinByAvgLoss.to_string() => &vec![avg_win_by_avg_loose(avg_win, avg_loss)],
        &PerformanceStatisticColumnNames::AvgWin.to_string() => &vec![avg_win],
        &PerformanceStatisticColumnNames::AvgLoss.to_string() => &vec![avg_loss],
        &PerformanceStatisticColumnNames::ProfitFactor.to_string() => &vec![profit_factor(total_win, total_loss)],
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
        let take_profit = self.trade.take_prift;
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
            &ProfitAndLossColumnNames::CalendarWeek.to_string() =>vec![cw],
            &ProfitAndLossColumnNames::Date.to_string() =>vec![date],
            &ProfitAndLossColumnNames::Strategy.to_string() =>vec![strategy],
            &ProfitAndLossColumnNames::Market.to_string() =>vec![market],
            &ProfitAndLossColumnNames::TradeDirection.to_string() =>vec![trade_direction],
            &ProfitAndLossColumnNames::Entry.to_string() =>vec![entry_price.round_to_n_decimal_places(n)],
            &ProfitAndLossColumnNames::TakeProfit.to_string() =>vec![take_profit.round_to_n_decimal_places(n)],
            &ProfitAndLossColumnNames::StopLoss.to_string() =>vec![stop_loss.round_to_n_decimal_places(n)],
            &ProfitAndLossColumnNames::ExpectedWinTick.to_string() =>vec![expected_win_tick],
            &ProfitAndLossColumnNames::ExpectedLossTick.to_string() =>vec![expected_loss_tick],
            &ProfitAndLossColumnNames::ExpectedWinDollar.to_string() =>vec![expected_win_dollar.round_to_dollar_cents()],
            &ProfitAndLossColumnNames::ExpectedLossDollar.to_string() =>vec![expected_loss_dollar.round_to_dollar_cents()],
            &ProfitAndLossColumnNames::Crv.to_string() =>vec![crv.round_to_n_decimal_places(3)],
            &ProfitAndLossColumnNames::EntryTimestamp.to_string() =>vec![entry_ts],
            &ProfitAndLossColumnNames::TakeProfitTimestamp.to_string() =>vec![take_profit_ts],
            &ProfitAndLossColumnNames::StopLossTimestamp.to_string() =>vec![stop_loss_ts],
            &ProfitAndLossColumnNames::ExitPrice.to_string() =>vec![exit_price.round_to_n_decimal_places(n)],
            &ProfitAndLossColumnNames::Status.to_string() =>vec![status],
            &ProfitAndLossColumnNames::PlTick.to_string() =>vec![pl_tick],
            &ProfitAndLossColumnNames::PlDollar.to_string() =>vec![pl_dollar.round_to_dollar_cents()],
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
        let take_profit = self.trade.take_prift;
        let stop_loss = self.trade.stop_loss;
        let expected_win_tick = self.expected_win_in_tick(tick_factor);
        let expected_loss_tick = self.expected_loss_in_tick(tick_factor);
        let expected_win_dollar = expected_win_tick * tick_to_dollar;
        let expected_loss_dollar = expected_loss_tick * tick_to_dollar;
        let crv = (expected_win_tick / expected_loss_tick).abs();

        let n = self.get_decimal_places();

        df!(
            &ProfitAndLossColumnNames::CalendarWeek.to_string() =>vec![cw],
            &ProfitAndLossColumnNames::Date.to_string() =>vec![date],
            &ProfitAndLossColumnNames::Strategy.to_string() =>vec![strategy],
            &ProfitAndLossColumnNames::Market.to_string() =>vec![market],
            &ProfitAndLossColumnNames::TradeDirection.to_string() =>vec![trade_direction],
            &ProfitAndLossColumnNames::Entry.to_string() =>vec![entry_price.round_to_n_decimal_places(n)],
            &ProfitAndLossColumnNames::TakeProfit.to_string() =>vec![take_profit.round_to_n_decimal_places(n)],
            &ProfitAndLossColumnNames::StopLoss.to_string() =>vec![stop_loss.round_to_n_decimal_places(n)],
            &ProfitAndLossColumnNames::ExpectedWinTick.to_string() =>vec![expected_win_tick],
            &ProfitAndLossColumnNames::ExpectedLossTick.to_string() =>vec![expected_loss_tick],
            &ProfitAndLossColumnNames::ExpectedWinDollar.to_string() =>vec![expected_win_dollar.round_to_dollar_cents()],
            &ProfitAndLossColumnNames::ExpectedLossDollar.to_string() =>vec![expected_loss_dollar.round_to_dollar_cents()],
            &ProfitAndLossColumnNames::Crv.to_string() =>vec![crv.round_to_n_decimal_places(3)],
            &ProfitAndLossColumnNames::EntryTimestamp.to_string() => &["NoEntry".to_string()],
            &ProfitAndLossColumnNames::TakeProfitTimestamp.to_string() => &["NoEntry".to_string()],
            &ProfitAndLossColumnNames::StopLossTimestamp.to_string() => &["NoEntry".to_string()],
            &ProfitAndLossColumnNames::ExitPrice.to_string() => &[0.0],
            &ProfitAndLossColumnNames::Status.to_string() => &["NoEntry".to_string()],
            &ProfitAndLossColumnNames::PlTick.to_string() => &[0.0],
            &ProfitAndLossColumnNames::PlDollar.to_string() => &[0.0],
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
        (self.trade.profit(self.trade.take_prift) / tick_factor).round()
    }

    fn expected_loss_in_tick(&self, tick_factor: f64) -> f64 {
        (self.trade.profit(self.trade.stop_loss) / tick_factor).round()
    }
}

pub struct PnLReportBuilder {
    market: Option<MarketKind>,
    year: Option<u32>,
    data_rows: Option<Vec<LazyFrame>>,
}

#[derive(Debug, Clone)]
pub struct PnLReportDataRow {
    pub market: MarketKind,
    pub year: u32,
    pub strategy: StrategyKind,
    pub time_frame_snapshot: TimeFrameSnapshot,
    pub trade: Trade,
    pub trade_pnl: Option<TradePnL>,
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
            data_rows: Some(data_rows),
        }
    }

    pub fn build(self) -> PnLReport {
        PnLReport {
            market: self.market.unwrap(),
            year: self.year.unwrap(),
            pnl: self.data_rows.unwrap().concatenate_to_data_frame(),
        }
    }
}
