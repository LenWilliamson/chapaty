use crate::{
    bot::time_interval::timestamp_in_milli_to_string,
    calculator::pnl_report_data_row_calculator::PnLReportDataRow,
    converter::market_decimal_places::MyDecimalPlaces,
    data_frame_operations::io_operations::save_df_as_csv,
    enums::markets::MarketKind,
    enums::{column_names, trade_and_pre_trade::TradeDirectionKind},
    lazy_frame_operations::trait_extensions::{MyLazyFrameOperations, MyLazyFrameVecOperations},
};
use chrono::NaiveDate;
use polars::df;
use polars::prelude::NamedFrom;
use polars::prelude::{DataFrame, IntoLazy};
use std::{collections::HashMap, convert::identity};

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PnLReports {
    pub market: MarketKind,
    pub years: Vec<u32>,
    pub strategy: String,
    pub reports: HashMap<u32, DataFrame>,
}

impl PnLReports {
    pub fn save_as_csv(&self, file_name: &str) {
        self.reports.iter().for_each(|(year, data)| {
            save_df_as_csv(
                &mut data.clone(),
                &format!("{file_name}_{}_{year}_pnl", self.market),
            )
        })
    }

    pub fn agg_year(&self) -> DataFrame {
        let ldfs = self.years.iter().fold(Vec::new(), |mut acc, year| {
            acc.push(self.reports.get(year).unwrap().clone().lazy());
            acc
        });
        ldfs.concatenate_to_lazy_frame()
            .sort_by_date()
            .collect()
            .unwrap()
    }
}

impl PnLReportDataRow {
    fn report_with_trade(self) -> DataFrame {
        let tick_factor = self.get_tick_factor();
        let tick_to_dollar = self.get_tick_to_dollar_conversion_factor();
        let trade_pnl = self.trade_pnl.clone().unwrap();

        let cw = self.time_frame_snapshot.get_calendar_week_as_int();
        let date = self.get_date();
        let strategy = self.strategy_name.to_string().to_uppercase();
        let market = self.market.to_string();
        let trade_direction = self.trade.trade_kind.to_string();
        let entry_price = self.trade.entry_price;
        let take_profit = self.trade.take_profit.map_or_else(|| 0.0, identity);
        let stop_loss = self.trade.stop_loss.map_or_else(|| 0.0, identity);
        let expected_win_tick = self.expected_win_in_tick(tick_factor);
        let expected_loss_tick = self.expected_loss_in_tick(tick_factor);
        let expected_win_dollar = expected_win_tick * tick_to_dollar;
        let expected_loss_dollar = expected_loss_tick * tick_to_dollar;
        let crv = match self.trade.trade_kind {
            TradeDirectionKind::None => 0.0,
            _ => compute_crv(expected_win_tick, expected_loss_tick),
        };
        let entry_ts = self.get_entry_ts();
        let take_profit_ts = self.get_take_profit_ts();
        let stop_loss_ts = self.get_stop_loss_ts();
        let exit_price = match self.trade.trade_kind {
            TradeDirectionKind::None => 0.0,
            _ => trade_pnl.exit_price(),
        };
        let pl_tick = match self.trade.trade_kind {
            TradeDirectionKind::None => 0.0,
            _ => trade_pnl.profit() / tick_factor,
        };
        let pl_dollar = pl_tick * tick_to_dollar;
        let status = match self.trade.trade_kind {
            TradeDirectionKind::None => "No Trade".to_string(),
            _ => determine_status(pl_dollar),
        };

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
        let strategy = self.strategy_name.to_string().to_uppercase();
        let market = self.market.to_string();
        let trade_direction = self.trade.trade_kind.to_string();
        let entry_price = self.trade.entry_price;
        let take_profit = self.trade.take_profit.map_or_else(|| 0.0, identity);
        let stop_loss = self.trade.stop_loss.map_or_else(|| 0.0, identity);
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
        let profit = self
            .trade
            .profit(self.trade.take_profit.map_or_else(|| 0.0, identity));
        (profit / tick_factor).round()
    }

    fn expected_loss_in_tick(&self, tick_factor: f64) -> f64 {
        let loss = self
            .trade
            .profit(self.trade.stop_loss.map_or_else(|| 0.0, identity));
        (loss / tick_factor).round()
    }
}

fn determine_status(profit: f64) -> String {
    if profit > 0.0 {
        "Winner".to_string()
    } else {
        "Loser".to_string()
    }
}

fn compute_crv(win: f64, loss: f64) -> f64 {
    if loss == 0.0 {
        f64::INFINITY
    } else {
        (win / loss).abs()
    }
}

impl From<PnLReportDataRow> for DataFrame {
    fn from(value: PnLReportDataRow) -> Self {
        match value.trade_pnl {
            Some(_) => value.report_with_trade(),
            None => value.report_without_trade(),
        }
    }
}

impl FromIterator<PnLReportDataRow> for DataFrame {
    fn from_iter<T: IntoIterator<Item = PnLReportDataRow>>(iter: T) -> Self {
        iter.into_iter()
            .fold(Vec::new(), |mut ldfs, pnl_report_data_row| {
                let df: DataFrame = pnl_report_data_row.into();
                ldfs.push(df.lazy());
                ldfs
            })
            .concatenate_to_lazy_frame()
            .sort_by_date()
            .collect()
            .unwrap()
            .with_row_count("#", Some(1))
            .unwrap()
    }
}

pub struct PnLReport {
    pub market: MarketKind,
    pub year: u32,
    pub strategy: String,
    pub pnl: DataFrame,
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
    strategy_name: Option<String>,
    years: Vec<u32>,
    reports: HashMap<u32, DataFrame>,
}

impl PnLReportsBuilder {
    pub fn new() -> Self {
        Self {
            market: None,
            strategy_name: None,
            years: Vec::new(),
            reports: HashMap::new(),
        }
    }

    pub fn append(self, pnl_report: PnLReport) -> Self {
        let market = pnl_report.market;
        let strategy_name = pnl_report.strategy.clone();
        let year = pnl_report.year;
        let mut years = self.years;
        years.push(year);

        let mut reports = self.reports;
        reports.insert(year, pnl_report.pnl);

        Self {
            market: Some(market),
            strategy_name: Some(strategy_name),
            years,
            reports,
        }
    }

    pub fn build(self) -> PnLReports {
        PnLReports {
            market: self.market.unwrap(),
            strategy: self.strategy_name.unwrap(),
            years: self.years,
            reports: self.reports,
        }
    }
}
