use crate::{
    calculator::pnl_report_data_row_calculator::PnLReportDataRow,
    converter::market_decimal_places::MyDecimalPlaces,
    data_frame_operations::io_operations::save_df_as_csv,
    enums::{column_names, markets::MarketKind, trade_and_pre_trade::TradeDirectionKind},
    lazy_frame_operations::trait_extensions::{MyLazyFrameOperations, MyLazyFrameVecOperations},
    PnLReportColumnKind,
};
use chrono::NaiveDate;
use polars::df;
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
            .drop(vec![
                PnLReportColumnKind::Uid.to_string(),
                PnLReportColumnKind::Id.to_string(),
            ])
            .collect()
            .unwrap()
    }
}

impl PnLReportDataRow {
    fn report_with_trade(&self) -> DataFrame {
        let tick_factor = self.get_tick_factor();
        let tick_to_dollar = self.get_tick_to_dollar_conversion_factor();
        let trade_pnl = self.trade_pnl.clone().unwrap();
        let trade = &self.trade;

        let cw = self.time_frame_snapshot.get_calendar_week_as_int();
        let date = self.get_date();
        let strategy = self.strategy_name.to_string().to_uppercase();
        let market = self.market.to_string();
        let trade_direction = trade.trade_direction_kind.unwrap().to_string();
        let entry_price = trade.entry_price.unwrap();
        let take_profit = trade.take_profit.unwrap_or(entry_price);
        let stop_loss = trade.stop_loss.unwrap_or(entry_price);
        let expected_win_tick = trade.expected_win_in_tick(tick_factor).unwrap();
        let expected_loss_tick = trade.expected_loss_in_tick(tick_factor).unwrap();
        let expected_win_dollar = expected_win_tick * tick_to_dollar;
        let expected_loss_dollar = expected_loss_tick * tick_to_dollar;
        let crv = trade.compute_risk_reward_ratio(tick_factor).unwrap();
        let entry_ts = trade_pnl.get_entry_ts();
        let take_profit_ts = trade_pnl.get_take_profit_ts();
        let stop_loss_ts = trade_pnl.get_stop_loss_ts();
        let exit_price = match trade.trade_direction_kind.unwrap() {
            TradeDirectionKind::None => entry_price,
            _ => trade_pnl.exit_price(),
        };
        let pl_tick = match trade.trade_direction_kind.unwrap() {
            TradeDirectionKind::None => entry_price,
            _ => trade_pnl.profit() / tick_factor,
        };
        let pl_dollar = pl_tick * tick_to_dollar;
        let status = match trade.trade_direction_kind.unwrap() {
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
            &column_names::PnLReportColumnKind::ExpectedWinTick.to_string() =>vec![expected_win_tick.round_to_n_decimal_places(0)],
            &column_names::PnLReportColumnKind::ExpectedLossTick.to_string() =>vec![expected_loss_tick.round_to_n_decimal_places(0)],
            &column_names::PnLReportColumnKind::ExpectedWinDollar.to_string() =>vec![expected_win_dollar.round_to_n_decimal_places(2)],
            &column_names::PnLReportColumnKind::ExpectedLossDollar.to_string() =>vec![expected_loss_dollar.round_to_n_decimal_places(2)],
            &column_names::PnLReportColumnKind::Crv.to_string() =>vec![crv.round_to_n_decimal_places(3)],
            &column_names::PnLReportColumnKind::EntryTimestamp.to_string() =>vec![entry_ts],
            &column_names::PnLReportColumnKind::TakeProfitTimestamp.to_string() =>vec![take_profit_ts],
            &column_names::PnLReportColumnKind::StopLossTimestamp.to_string() =>vec![stop_loss_ts],
            &column_names::PnLReportColumnKind::ExitPrice.to_string() =>vec![exit_price.round_to_n_decimal_places(n)],
            &column_names::PnLReportColumnKind::Status.to_string() =>vec![status],
            &column_names::PnLReportColumnKind::PlTick.to_string() =>vec![pl_tick.round_to_n_decimal_places(0)],
            &column_names::PnLReportColumnKind::PlDollar.to_string() =>vec![pl_dollar.round_to_n_decimal_places(2)],
        ).unwrap()
    }

    // fn report_without_trade(&self) -> DataFrame {
    //     let tick_factor = self.get_tick_factor();
    //     let tick_to_dollar = self.get_tick_to_dollar_conversion_factor();
    //     let trade = &self.trade;

    //     let cw = self.time_frame_snapshot.get_calendar_week_as_int();
    //     let date = self.get_date();
    //     let strategy = self.strategy_name.to_string().to_uppercase();
    //     let market = self.market.to_string();
    //     let trade_direction = trade.trade_kind.to_string();
    //     let entry_price = trade.entry_price;
    //     let take_profit = trade.take_profit.map_or(entry_price, identity);
    //     let stop_loss = trade.stop_loss.map_or(entry_price, identity);
    //     let expected_win_tick = trade.expected_win_in_tick(tick_factor);
    //     let expected_loss_tick = trade.expected_loss_in_tick(tick_factor);
    //     let expected_win_dollar = expected_win_tick * tick_to_dollar;
    //     let expected_loss_dollar = expected_loss_tick * tick_to_dollar;
    //     let crv = compute_crv(expected_win_tick, expected_loss_tick);

    //     let n = self.get_decimal_places();

    //     df!(
    //         &column_names::PnLReportColumnKind::CalendarWeek.to_string() =>vec![cw],
    //         &column_names::PnLReportColumnKind::Date.to_string() =>vec![date],
    //         &column_names::PnLReportColumnKind::Strategy.to_string() =>vec![strategy],
    //         &column_names::PnLReportColumnKind::Market.to_string() =>vec![market],
    //         &column_names::PnLReportColumnKind::TradeDirection.to_string() =>vec![trade_direction],
    //         &column_names::PnLReportColumnKind::Entry.to_string() =>vec![entry_price.round_to_n_decimal_places(n)],
    //         &column_names::PnLReportColumnKind::TakeProfit.to_string() =>vec![take_profit.round_to_n_decimal_places(n)],
    //         &column_names::PnLReportColumnKind::StopLoss.to_string() =>vec![stop_loss.round_to_n_decimal_places(n)],
    //         &column_names::PnLReportColumnKind::ExpectedWinTick.to_string() =>vec![expected_win_tick.round_to_n_decimal_places(0)],
    //         &column_names::PnLReportColumnKind::ExpectedLossTick.to_string() =>vec![expected_loss_tick.round_to_n_decimal_places(0)],
    //         &column_names::PnLReportColumnKind::ExpectedWinDollar.to_string() =>vec![expected_win_dollar.round_to_n_decimal_places(2)],
    //         &column_names::PnLReportColumnKind::ExpectedLossDollar.to_string() =>vec![expected_loss_dollar.round_to_n_decimal_places(2)],
    //         &column_names::PnLReportColumnKind::Crv.to_string() =>vec![crv.round_to_n_decimal_places(3)],
    //         &column_names::PnLReportColumnKind::EntryTimestamp.to_string() => &["NoEntry".to_string()],
    //         &column_names::PnLReportColumnKind::TakeProfitTimestamp.to_string() => &["NoEntry".to_string()],
    //         &column_names::PnLReportColumnKind::StopLossTimestamp.to_string() => &["NoEntry".to_string()],
    //         &column_names::PnLReportColumnKind::ExitPrice.to_string() => &[0.0],
    //         &column_names::PnLReportColumnKind::Status.to_string() => &["NoEntry".to_string()],
    //         &column_names::PnLReportColumnKind::PlTick.to_string() => &[0.0],
    //         &column_names::PnLReportColumnKind::PlDollar.to_string() => &[0.0],
    //     )
    //     .unwrap()
    // }

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
        self.market.tick_step_size().map_or(1.0, identity)
    }

    fn get_tick_to_dollar_conversion_factor(&self) -> f64 {
        self.market
            .tik_to_dollar_conversion_factor()
            .map_or(1.0, identity)
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
        0.0
    } else {
        (win / loss).abs()
    }
}

impl From<PnLReportDataRow> for DataFrame {
    fn from(value: PnLReportDataRow) -> Self {
        // match (&value.trade_pnl, value.trade.is_valid) {
        //     (None, _) | (_, false) => value.report_without_trade(),
        //     _ => value.report_with_trade(),
        // }
        value.report_with_trade()
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
            // TODO sort by entry timestamp
            .sort_by_date()
            .collect()
            .unwrap()
            .with_row_index(PnLReportColumnKind::Id.to_string().into(), Some(1))
            .unwrap()
            .with_row_index(PnLReportColumnKind::Uid.to_string().into(), Some(1))
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
