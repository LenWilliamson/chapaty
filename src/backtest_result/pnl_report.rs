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

        let p_and_l = df!(
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
        );
        p_and_l.unwrap()
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

// pub fn schema() -> polars::prelude::Schema {
//     polars::prelude::Schema::from_iter(
//         vec![
//             Field::new(
//                 &ProfitAndLossColumnNames::CalendarWeek.to_string(),
//                 polars::prelude::DataType::Int64,
//             ),
//             Field::new(
//                 &ProfitAndLossColumnNames::Date.to_string(),
//                 polars::prelude::DataType::Utf8,
//             ),
//             Field::new(
//                 &ProfitAndLossColumnNames::Strategy.to_string(),
//                 polars::prelude::DataType::Utf8,
//             ),
//             Field::new(
//                 &ProfitAndLossColumnNames::Market.to_string(),
//                 polars::prelude::DataType::Utf8,
//             ),
//             Field::new(
//                 &ProfitAndLossColumnNames::TradeDirection.to_string(),
//                 polars::prelude::DataType::Utf8,
//             ),
//             Field::new(
//                 &ProfitAndLossColumnNames::Entry.to_string(),
//                 polars::prelude::DataType::Float64,
//             ),
//             Field::new(
//                 &ProfitAndLossColumnNames::TakeProfit.to_string(),
//                 polars::prelude::DataType::Float64,
//             ),
//             Field::new(
//                 &ProfitAndLossColumnNames::StopLoss.to_string(),
//                 polars::prelude::DataType::Float64,
//             ),
//             Field::new(
//                 &ProfitAndLossColumnNames::ExpectedWinTik.to_string(),
//                 polars::prelude::DataType::Float64,
//             ),
//             Field::new(
//                 &ProfitAndLossColumnNames::ExpectedLossTik.to_string(),
//                 polars::prelude::DataType::Float64,
//             ),
//             Field::new(
//                 &ProfitAndLossColumnNames::ExpectedWinDollar.to_string(),
//                 polars::prelude::DataType::Float64,
//             ),
//             Field::new(
//                 &ProfitAndLossColumnNames::ExpectedLossDollar.to_string(),
//                 polars::prelude::DataType::Float64,
//             ),
//             Field::new(
//                 &ProfitAndLossColumnNames::Crv.to_string(),
//                 polars::prelude::DataType::Float64,
//             ),
//             Field::new(
//                 &ProfitAndLossColumnNames::EntryTimestamp.to_string(),
//                 polars::prelude::DataType::Utf8,
//             ),
//             Field::new(
//                 &ProfitAndLossColumnNames::TargetTimestamp.to_string(),
//                 polars::prelude::DataType::Utf8,
//             ),
//             Field::new(
//                 &ProfitAndLossColumnNames::StopLossTimestamp.to_string(),
//                 polars::prelude::DataType::Utf8,
//             ),
//             Field::new(
//                 &ProfitAndLossColumnNames::ExitPrice.to_string(),
//                 polars::prelude::DataType::Float64,
//             ),
//             Field::new(
//                 &ProfitAndLossColumnNames::Status.to_string(),
//                 polars::prelude::DataType::Utf8,
//             ),
//             Field::new(
//                 &ProfitAndLossColumnNames::PlTik.to_string(),
//                 polars::prelude::DataType::Float64,
//             ),
//             Field::new(
//                 &ProfitAndLossColumnNames::PlDollar.to_string(),
//                 polars::prelude::DataType::Float64,
//             ),
//         ]
//         .into_iter(),
//     )
// }

// /// This function generates a `.csv` report with the current trade statistics. It calls the respective subroutines,
// /// whether a trade happend or not.
// ///
// /// # Arguments
// /// * `cw` - current calender week
// /// * `entry_price` - buy price, i.e. poc
// /// * `trade_kind` - Either `Long`, `Short` or `None`
// /// * `profit_and_loss` - Either `None` or `Some(ProfitAndLoss)`
// ///
// /// # Example
// /// TODO
// pub fn generate_profit_and_loss_report(
//     time_frame_snapshot: &TimeFrameSnapshot,
//     trading_session: &TradingSession,
//     entry_price: f64,
//     entry_ts: &std::option::Option<i64>,
//     trade_kind: &TradeKind,
//     profit_and_loss: &std::option::Option<ProfitAndLoss>,
// ) -> DataFrame {
//     let cw = time_frame_snapshot.get_calendar_week_as_int();
//     let day = time_frame_snapshot.get_weekday();
//     let market = trading_session.market_kind;
//     let year = trading_session.year;
//     // let mut records = Vec::<data_model::profit_and_loss::ProfitAndLossCsvRecord>::new();
//     // _report_with_trade(cw, entry_price, entry_ts.unwrap(), &trade_kind, &pl) das mit entry_ts.unwrap() ist unsauber. aber der
//     // Wert ist immer valid, da pl valid ist.
//     match profit_and_loss {
//         Some(pl) => report_with_trade(
//             cw,
//             day,
//             entry_price,
//             entry_ts.unwrap(),
//             &trade_kind,
//             &pl,
//             market,
//             year,
//         ),
//         None => report_without_trade(cw, day, entry_price, &trade_kind, market, year),
//     }
// }

// fn report_with_trade(
//     cw: i64,
//     day: Weekday,
//     entry_price: f64,
//     entry_timestamp: i64,
//     trade_kind: &TradeKind,
//     profit_and_loss: &ProfitAndLoss,
//     market: MarketKind,
//     year: u32,
// ) -> DataFrame {
//     // Determine conversion factors
//     let to_tik = if let Some(val) = market.tik_step() {
//         val
//     } else {
//         1.0
//     };
//     let tik_to_dollar = if let Some(val) = market.tik_to_dollar_conversion_factor() {
//         val
//     } else {
//         1.0
//     };

//     let calender_week = vec![cw];
//     let date = vec![NaiveDate::from_isoywd_opt(
//         i32::try_from(year).unwrap(),
//         u32::try_from(cw).unwrap(),
//         day,
//     )
//     .unwrap()
//     .format("%Y-%m-%d")
//     .to_string()];
//     let strategy = vec!["PPP"];
//     let market = vec![market.to_string()];
//     let trade_direction = vec![trade_kind.to_string()];
//     let entry = vec![entry_price];
//     let target = vec![profit_and_loss.take_profit.condition];
//     let stop_loss = vec![profit_and_loss.stop_loss.condition];
//     let expected_win_tik = vec![
//         compute_profit(
//             profit_and_loss.take_profit.condition,
//             trade_kind,
//             entry_price,
//         ) / to_tik,
//     ];
//     let expected_loss_tik =
//         vec![compute_profit(profit_and_loss.stop_loss.condition, trade_kind, entry_price) / to_tik];
//     let expected_win_dollar = vec![expected_win_tik[0] * tik_to_dollar];
//     let expected_loss_dollar = vec![expected_loss_tik[0] * tik_to_dollar];
//     let crv = vec![(expected_win_tik[0] / expected_loss_tik[0]).abs()];
//     let entry_ts = vec![NaiveDateTime::from_timestamp_opt(entry_timestamp / 1000, 0)
//         .unwrap()
//         .format("%Y-%m-%d %H:%M:%S")
//         .to_string()];
//     let target_ts = match profit_and_loss.take_profit.entry_time_stamp {
//         Some(ts) => vec![NaiveDateTime::from_timestamp_opt(ts / 1000, 0)
//             .unwrap()
//             .format("%Y-%m-%d %H:%M:%S")
//             .to_string()],
//         None => vec!["Timeout")],
//     };
//     let stop_loss_ts = match profit_and_loss.stop_loss.entry_time_stamp {
//         Some(ts) => vec![NaiveDateTime::from_timestamp_opt(ts / 1000, 0)
//             .unwrap()
//             .format("%Y-%m-%d %H:%M:%S")
//             .to_string()],
//         None => vec!["Timeout")],
//     };

//     let status = vec![determine_status2(profit_and_loss)];

//     // let exit_price = match profit_and_loss.take_profit.triggered {
//     //     // We entered a trade so the tp condition was hit
//     //     true => vec![profit_and_loss.take_profit.condition],

//     //     // We ran into a timeout so the last trade price is the exit
//     //     false => {
//     //         let x = profit_and_loss.take_profit.timeout.as_ref().unwrap();
//     //         vec![x.condition]
//     //     }
//     // };

//     // TODO we set exit price below when computing pl_tik
//     let mut exit_price = vec![0.0];

//     // If sl timestamp < tp timestamp => sl price
//     let sl_timestamp = profit_and_loss.stop_loss.entry_time_stamp;
//     let tp_timestamp = profit_and_loss.take_profit.entry_time_stamp;
//     let pl_tik = if sl_timestamp.is_none() && tp_timestamp.is_none() {
//         // We ran into a timeout so the last trade price is the exit
//         let x = profit_and_loss.take_profit.timeout.as_ref().unwrap();
//         exit_price = vec![x.condition];
//         // Timeout
//         match &profit_and_loss.take_profit.timeout {
//             Some(to) => vec![to.profit / to_tik],
//             None => panic!("timeout is None but trade ran into a timeout"),
//         }
//     } else {
//         if sl_timestamp.is_none() {
//             exit_price = vec![profit_and_loss.take_profit.condition];
//             // Only tp_timestamp triggered
//             vec![profit_and_loss.take_profit.profit.unwrap() / to_tik]
//         } else if tp_timestamp.is_none() {
//             exit_price = vec![profit_and_loss.stop_loss.condition];
//             // Only sl_timestamp triggerd
//             vec![profit_and_loss.stop_loss.profit.unwrap() / to_tik]
//         } else {
//             // tp_ts and sl_timestamp tiggered
//             let tp_ts = tp_timestamp.unwrap();
//             let sl_ts = sl_timestamp.unwrap();
//             if tp_ts < sl_ts {
//                 exit_price = vec![profit_and_loss.take_profit.condition];
//                 vec![profit_and_loss.take_profit.profit.unwrap() / to_tik]
//             } else if sl_ts < tp_ts {
//                 exit_price = vec![profit_and_loss.stop_loss.condition];
//                 vec![profit_and_loss.stop_loss.profit.unwrap() / to_tik]
//             } else {
//                 // TODO panic!("take profit and stop loss timestamp triggered at the same time")
//                 // We cannot say if it was a winner or loser
//                 vec![0.0]
//             }
//         }
//     };
//     // let pl_tik = match profit_and_loss.take_profit.profit {
//     //     Some(p) => vec![p / to_tik],
//     //     None => match &profit_and_loss.take_profit.timeout {
//     //         Some(to) => vec![to.profit / to_tik],
//     //         None => panic!("timeout is None but trade ran into a timeout"),
//     //     },
//     // };
//     let pl_dollar = vec![pl_tik[0] * tik_to_dollar];

//     let p_and_l = df!(
//         &ProfitAndLossColumnNames::CalendarWeek.to_string() => &calender_week,
//         &ProfitAndLossColumnNames::Date.to_string() => &date,
//         &ProfitAndLossColumnNames::Strategy.to_string() => &strategy,
//         &ProfitAndLossColumnNames::Market.to_string() => &market,
//         &ProfitAndLossColumnNames::TradeDirection.to_string() => &trade_direction,
//         &ProfitAndLossColumnNames::Entry.to_string() => &entry,
//         &ProfitAndLossColumnNames::TakeProfit.to_string() => &target,
//         &ProfitAndLossColumnNames::StopLoss.to_string() => &stop_loss,
//         &ProfitAndLossColumnNames::ExpectedWinTik.to_string() => &expected_win_tik,
//         &ProfitAndLossColumnNames::ExpectedLossTik.to_string() => &expected_loss_tik,
//         &ProfitAndLossColumnNames::ExpectedWinDollar.to_string() => &expected_win_dollar,
//         &ProfitAndLossColumnNames::ExpectedLossDollar.to_string() => &expected_loss_dollar,
//         &ProfitAndLossColumnNames::Crv.to_string() => &crv,
//         &ProfitAndLossColumnNames::EntryTimestamp.to_string() => &entry_ts,
//         &ProfitAndLossColumnNames::TargetTimestamp.to_string() => &target_ts,
//         &ProfitAndLossColumnNames::StopLossTimestamp.to_string() => &stop_loss_ts,
//         &ProfitAndLossColumnNames::ExitPrice.to_string() => &exit_price,
//         &ProfitAndLossColumnNames::Status.to_string() => &status,
//         &ProfitAndLossColumnNames::PlTik.to_string() => &pl_tik,
//         &ProfitAndLossColumnNames::PlDollar.to_string() => &pl_dollar,
//     );
//     p_and_l.unwrap()
//     // let out_dir = gcs::Strategy::find(finder, &InDataKind::ProfitAndLoss);
//     // dbg!(&out_dir);
//     // save_file(
//     //     self.dp.get_client(),
//     //     p_and_l.unwrap(),
//     //     &out_dir,
//     //     "pl.csv",
//     // )
//     // .await; // &format!("{}.csv", cw)
// }

// fn report_without_trade(
//     cw: i64,
//     day: Weekday,
//     entry_price: f64,
//     trade_kind: &TradeKind,
//     market: MarketKind,
//     year: u32,
// ) -> DataFrame {
//     let calender_week = vec![cw];
//     let date = vec![NaiveDate::from_isoywd_opt(
//         i32::try_from(year).unwrap(),
//         u32::try_from(cw).unwrap(),
//         day,
//     )
//     .unwrap()
//     .format("%Y-%m-%d")
//     .to_string()];
//     let strategy = vec!["PPP"];
//     let market = vec![market.to_string()];
//     let trade_direction = vec![trade_kind.to_string()];
//     let entry = vec![entry_price];
//     // let target = vec![profit_and_loss.take_profit.condition];
//     // let stop_loss = vec![profit_and_loss.stop_loss.condition];
//     // let expected_win_tik = vec![compute_profit(
//     //     profit_and_loss.take_profit.condition,
//     //     trade_kind,
//     //     entry_price,
//     // )];
//     // let expected_loss_tik = vec![compute_profit(
//     //     profit_and_loss.stop_loss.condition,
//     //     trade_kind,
//     //     entry_price,
//     // )];
//     // let expected_win_dollar = vec![compute_profit(
//     //     profit_and_loss.take_profit.condition,
//     //     trade_kind,
//     //     entry_price,
//     // )];
//     // let expected_loss_dollar = vec![compute_profit(
//     //     profit_and_loss.stop_loss.condition,
//     //     trade_kind,
//     //     entry_price,
//     // )];
//     // let crv = vec![(expected_win_tik[0] / expected_loss_tik[0]).abs()];

//     let p_and_l = df!(
//         &ProfitAndLossColumnNames::CalendarWeek.to_string() => &calender_week,
//         &ProfitAndLossColumnNames::Date.to_string() => &date,
//         &ProfitAndLossColumnNames::Strategy.to_string() => &strategy,
//         &ProfitAndLossColumnNames::Market.to_string() => &market,
//         &ProfitAndLossColumnNames::TradeDirection.to_string() => &trade_direction,
//         &ProfitAndLossColumnNames::Entry.to_string() => &entry,
//         &ProfitAndLossColumnNames::TakeProfit.to_string() => vec![0.0], // &target,
//         &ProfitAndLossColumnNames::StopLoss.to_string() => vec![0.0], // &stop_loss,
//         &ProfitAndLossColumnNames::ExpectedWinTik.to_string() => vec![0.0], // &expected_win_tik,
//         &ProfitAndLossColumnNames::ExpectedLossTik.to_string() => vec![0.0], // &expected_loss_tik,
//         &ProfitAndLossColumnNames::ExpectedWinDollar.to_string() => vec![0.0], // &expected_win_dollar,
//         &ProfitAndLossColumnNames::ExpectedLossDollar.to_string() => vec![0.0], // &expected_loss_dollar,
//         &ProfitAndLossColumnNames::Crv.to_string() => vec![0.0], // &crv,
//         &ProfitAndLossColumnNames::EntryTimestamp.to_string() => &["NoEntry")],
//         &ProfitAndLossColumnNames::TargetTimestamp.to_string() => &["NoEntry")],
//         &ProfitAndLossColumnNames::StopLossTimestamp.to_string() => &["NoEntry")],
//         &ProfitAndLossColumnNames::ExitPrice.to_string() => &[0.0],
//         &ProfitAndLossColumnNames::Status.to_string() => &["NoEntry")],
//         &ProfitAndLossColumnNames::PlTik.to_string() => &[0.0],
//         &ProfitAndLossColumnNames::PlDollar.to_string() => &[0.0],
//     );

//     p_and_l.unwrap()
//     // let out_dir = gcs::Strategy::find(finder, &InDataKind::ProfitAndLoss);
//     // save_file(
//     //     self.dp.get_client(),
//     //     p_and_l.unwrap(),
//     //     &out_dir,
//     //     "pl.csv",
//     // )
//     // .await; // &format!("{}.csv", cw)
// }

// /// This function returns the tik step size for a market that uses tiks as units. Otherwise we return `None`.
// ///
// /// # Arguments
// /// * `market` - we want to get tik step size
// ///
// /// # Examples
// /// ```
// /// // BtcUsdt does not use tiks as unit
// /// assert_eq!(tik_step(MarketKind::BtcUsdt).is_some(), false)
// /// // EurUsd uses tiks as unit
// /// assert_eq!(tik_step(MarketKind::EurUsd).is_some(), true)
// /// ```
// pub fn tik_step(market: &MarketKind) -> Option<f64> {
//     match market {
//         MarketKind::BtcUsdt => None,
//         MarketKind::EurUsd => Some(0.00005),
//     }
// }

// /// This function returns the tik to dollar conversion factor for a market that uses tiks as units. Otherwise we return `None`.
// ///
// /// # Arguments
// /// * `market` - we want to get tik step size
// ///
// /// # Examples
// /// ```
// /// // BtcUsdt does not use tiks as unit
// /// assert_eq!(tik_to_dollar_conversion_factor(MarketKind::BtcUsdt).is_some(), false)
// /// // EurUsd uses tiks as unit
// /// assert_eq!(tik_to_dollar_conversion_factor(MarketKind::EurUsd).is_some(), true)
// /// ```
// pub fn tik_to_dollar_conversion_factor(market: &MarketKind) -> Option<f64> {
//     match market {
//         MarketKind::BtcUsdt => None,
//         MarketKind::EurUsd => Some(6.25),
//     }
// }

// #[cfg(test)]
// mod test {
//     use super::*;
//     use std::sync::Arc;

//     use crate::{
//         common::finder::Finder,
//         config::GCS_DATA_BUCKET,
//         enums::{
//             bots::StrategyKind,
//             data::{LeafDir, RootDir},
//             markets::{TimeFrame, MarketKind},
//             trades::TradeKind,
//         },
//         producers::{test::Test, DataProvider},
//         streams::tests::load_csv,
//     };
//     use google_cloud_default::WithAuthExt;
//     use google_cloud_storage::client::{Client, ClientConfig};

//     // TODO: Muss hier überhaupt was getestet werden?
//     #[tokio::test]
//     async fn test_generate_report() {}

//     #[tokio::test]
//     async fn test_report_with_trade() {
//         let config = ClientConfig::default().with_auth().await.unwrap();
//         let dp: Arc<dyn DataProvider + Send + Sync> =
//             Arc::new(Test::new(std::path::PathBuf::from(GCS_DATA_BUCKET)));

//         let finder: Finder = Finder::new(
//             dp.get_bucket_name(),
//             dp.get_data_producer_kind(),
//             MarketKind::BtcUsdt,
//             2022,
//             StrategyKind::Ppp,
//             TimeFrame::Weekly,
//         )
//         .await;
//         let cw = 10;
//         let entry_price = 42_100.0;
//         let entry_ts = 1646812800000_i64;
//         let trade_kind = TradeKind::Short;
//         let profit_and_loss_1 = ProfitAndLoss {
//             stop_loss: StopLossTrade {
//                 condition: 42_594.06,
//                 entry_time_stamp: Some(1646838000000_i64),
//                 profit: Some(-494.06),
//             },
//             take_profit: TakeProfitTrade {
//                 condition: 29_004.73,
//                 triggered: false,
//                 entry_time_stamp: None,
//                 profit: None,
//                 timeout: Some(Timeout {
//                     condition: 39_385.01,
//                     profit: 2714.99,
//                 }),
//             },
//         };
//         let profit_and_loss_2 = ProfitAndLoss {
//             stop_loss: StopLossTrade {
//                 condition: 44_601.12,
//                 entry_time_stamp: None,
//                 profit: None,
//             },
//             take_profit: TakeProfitTrade {
//                 condition: 39_104.73,
//                 triggered: true,
//                 entry_time_stamp: Some(1646888400000_i64),
//                 profit: Some(2_995.27),
//                 timeout: None,
//             },
//         };

//         let target_dir = finder._path_to_target(&RootDir::Strategy, &LeafDir::ProfitAndLoss);
//         // let out_dir = finder.find(&RootDir::Strategy, &InDataKind::ProfitAndLoss);

//         finder
//             .delete_files(
//                 finder.get_client_clone(),
//                 RootDir::Strategy,
//                 LeafDir::ProfitAndLoss,
//             )
//             .await;
//         let result = report_with_trade(
//             &finder,
//             cw,
//             Weekday::Mon,
//             entry_price,
//             entry_ts,
//             &trade_kind,
//             &profit_and_loss_1,
//         );
//         // let result = load_csv(&out_dir, "pl.csv").await.unwrap();
//         let target_pl1 = load_csv(&target_dir, "10_pl1.csv").await.unwrap();
//         assert_eq!(dbg!(result).frame_equal(dbg!(&target_pl1)), true);

//         finder
//             .delete_files(
//                 finder.get_client_clone(),
//                 RootDir::Strategy,
//                 LeafDir::ProfitAndLoss,
//             )
//             .await;
//         let result = report_with_trade(
//             &finder,
//             cw,
//             Weekday::Mon,
//             entry_price,
//             entry_ts,
//             &trade_kind,
//             &profit_and_loss_2,
//         );
//         // let result = load_csv(&out_dir, "pl.csv").await.unwrap();
//         let target_pl2 = load_csv(&target_dir, "10_pl2.csv").await.unwrap();
//         assert_eq!(result.frame_equal(&target_pl2), true);
//     }

//     #[tokio::test]
//     async fn test_report_without_trade() {
//         let config = ClientConfig::default().with_auth().await.unwrap();
//         let dp: Arc<dyn DataProvider + Send + Sync> =
//             Arc::new(Test::new(std::path::PathBuf::from(GCS_DATA_BUCKET)));

//         let finder: Finder = Finder::new(
//             dp.get_bucket_name(),
//             dp.get_data_producer_kind(),
//             MarketKind::BtcUsdt,
//             2022,
//             StrategyKind::Ppp,
//             TimeFrame::Weekly,
//         )
//         .await;

//         let cw = 10;
//         let entry_price = 42_100.0;
//         let trade_kind = TradeKind::Short;

//         let target_dir = finder._path_to_target(&RootDir::Strategy, &LeafDir::ProfitAndLoss);
//         // let out_dir = finder.find(&RootDir::Strategy, &InDataKind::ProfitAndLoss);
//         finder
//             .delete_files(
//                 finder.get_client_clone(),
//                 RootDir::Strategy,
//                 LeafDir::ProfitAndLoss,
//             )
//             .await;

//         let result = report_without_trade(&finder, cw, Weekday::Mon, entry_price, &trade_kind);
//         // let result = load_csv(&out_dir, "pl.csv").await.unwrap();
//         let target_pl1 = load_csv(&target_dir, "no_trade_10.csv").await.unwrap();
//         assert_eq!(result.frame_equal(&target_pl1), true);
//     }
// }
