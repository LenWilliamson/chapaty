use std::str::FromStr;

use crate::common::data_engine::{compute_profit, determine_status};
use crate::enums::strategies::{StopLossKind, TakeProfitKind};
use crate::{common::finder::Finder, enums::markets::MarketKind};
use crate::enums;

// pub struct ProfitAndLosses {
//     pub stop_loss: StopLossTrade,
//     pub take_profit: Vec<TakeProfitTrade>,
//     // timeout: Vec<Option<Timeout>>,
// }

#[derive(Debug)]
pub struct ProfitAndLoss {
    pub stop_loss: StopLossTrade,
    pub take_profit: TakeProfitTrade,
    // timeout: Vec<Option<Timeout>>,
}

#[derive(Debug)]
pub struct TakeProfitTrade {
    pub condition: f64,
    pub triggered: bool, // if true timeout == None
    pub entry_time_stamp: Option<i64>,
    pub profit: Option<f64>,
    pub timeout: Option<Timeout>,
}

#[derive(Debug, Copy, Clone)]
pub struct Timeout {
    pub condition: f64, // used to be timestamp -> Now last traded price
    pub profit: f64,
}

#[derive(Debug)]
pub struct StopLossTrade {
    pub condition: f64,
    pub entry_time_stamp: Option<i64>,
    pub profit: Option<f64>, // profit < 0 is a loss
}

#[derive(Clone, Copy)]
pub struct StopLoss {
    pub condition: enums::strategies::StopLossKind,
    pub offset: f64,
}


#[derive(Clone, Copy)]
pub struct TakeProfit {
    pub condition: enums::strategies::TakeProfitKind,
    pub offset: f64,
}

use crate::enums::columns::ProfitAndLossColumnNames;
use crate::enums::trades::TradeKind;
use chrono::{NaiveDate, NaiveDateTime, Weekday};
use polars::df;
use polars::prelude::NamedFrom;
use polars::prelude::{DataFrame, Field};
pub fn schema() -> polars::prelude::Schema {
    polars::prelude::Schema::from_iter(
        vec![
            Field::new(
                &ProfitAndLossColumnNames::CalendarWeek.to_string(),
                polars::prelude::DataType::Int64,
            ),
            Field::new(
                &ProfitAndLossColumnNames::Date.to_string(),
                polars::prelude::DataType::Utf8,
            ),
            Field::new(
                &ProfitAndLossColumnNames::Strategy.to_string(),
                polars::prelude::DataType::Utf8,
            ),
            Field::new(
                &ProfitAndLossColumnNames::Market.to_string(),
                polars::prelude::DataType::Utf8,
            ),
            Field::new(
                &ProfitAndLossColumnNames::TradeDirection.to_string(),
                polars::prelude::DataType::Utf8,
            ),
            Field::new(
                &ProfitAndLossColumnNames::Entry.to_string(),
                polars::prelude::DataType::Float64,
            ),
            Field::new(
                &ProfitAndLossColumnNames::TakeProfit.to_string(),
                polars::prelude::DataType::Float64,
            ),
            Field::new(
                &ProfitAndLossColumnNames::StopLoss.to_string(),
                polars::prelude::DataType::Float64,
            ),
            Field::new(
                &ProfitAndLossColumnNames::ExpectedWinTik.to_string(),
                polars::prelude::DataType::Float64,
            ),
            Field::new(
                &ProfitAndLossColumnNames::ExpectedLossTik.to_string(),
                polars::prelude::DataType::Float64,
            ),
            Field::new(
                &ProfitAndLossColumnNames::ExpectedWinDollar.to_string(),
                polars::prelude::DataType::Float64,
            ),
            Field::new(
                &ProfitAndLossColumnNames::ExpectedLossDollar.to_string(),
                polars::prelude::DataType::Float64,
            ),
            Field::new(
                &ProfitAndLossColumnNames::Crv.to_string(),
                polars::prelude::DataType::Float64,
            ),
            Field::new(
                &ProfitAndLossColumnNames::EntryTimestamp.to_string(),
                polars::prelude::DataType::Utf8,
            ),
            Field::new(
                &ProfitAndLossColumnNames::TargetTimestamp.to_string(),
                polars::prelude::DataType::Utf8,
            ),
            Field::new(
                &ProfitAndLossColumnNames::StopLossTimestamp.to_string(),
                polars::prelude::DataType::Utf8,
            ),
            Field::new(
                &ProfitAndLossColumnNames::ExitPrice.to_string(),
                polars::prelude::DataType::Float64,
            ),
            Field::new(
                &ProfitAndLossColumnNames::Status.to_string(),
                polars::prelude::DataType::Utf8,
            ),
            Field::new(
                &ProfitAndLossColumnNames::PlTik.to_string(),
                polars::prelude::DataType::Float64,
            ),
            Field::new(
                &ProfitAndLossColumnNames::PlDollar.to_string(),
                polars::prelude::DataType::Float64,
            ),
        ]
        .into_iter(),
    )
}

/// This function generates a `.csv` report with the current trade statistics. It calls the respective subroutines,
/// whether a trade happend or not.
///
/// # Arguments
/// * `cw` - current calender week
/// * `entry_price` - buy price, i.e. poc
/// * `trade_kind` - Either `Long`, `Short` or `None`
/// * `profit_and_loss` - Either `None` or `Some(ProfitAndLoss)`
///
/// # Example
/// TODO
pub fn generate_profit_loss_report(
    finder: &Finder,
    cw: i64,
    day: Weekday,
    entry_price: f64,
    entry_ts: &std::option::Option<i64>,
    trade_kind: &TradeKind,
    profit_and_loss: &std::option::Option<ProfitAndLoss>,
) -> DataFrame {
    // let mut records = Vec::<data_model::profit_and_loss::ProfitAndLossCsvRecord>::new();
    // _report_with_trade(cw, entry_price, entry_ts.unwrap(), &trade_kind, &pl) das mit entry_ts.unwrap() ist unsauber. aber der
    // Wert ist immer valid, da pl valid ist.
    match profit_and_loss {
        Some(pl) => report_with_trade(
            &finder,
            cw,
            day,
            entry_price,
            entry_ts.unwrap(),
            &trade_kind,
            &pl,
        ),
        None => report_without_trade(&finder, cw, day, entry_price, &trade_kind),
    }
}

fn report_with_trade(
    finder: &Finder,
    cw: i64,
    day: Weekday,
    entry_price: f64,
    entry_timestamp: i64,
    trade_kind: &TradeKind,
    profit_and_loss: &ProfitAndLoss,
) -> DataFrame {
    // Determine conversion factors
    let to_tik = if let Some(val) = finder.get_market().tik_step() {
        val
    } else {
        1.0
    };
    let tik_to_dollar = if let Some(val) = finder.get_market().tik_to_dollar_conversion_factor() {
        val
    } else {
        1.0
    };

    let calender_week = vec![cw];
    let date = vec![NaiveDate::from_isoywd_opt(
        i32::try_from(finder.get_year()).unwrap(),
        u32::try_from(cw).unwrap(),
        day,
    )
    .unwrap()
    .format("%Y-%m-%d")
    .to_string()];
    let strategy = vec!["PPP"];
    let market = vec![finder.get_market().to_string()];
    let trade_direction = vec![trade_kind.to_string()];
    let entry = vec![entry_price];
    let target = vec![profit_and_loss.take_profit.condition];
    let stop_loss = vec![profit_and_loss.stop_loss.condition];
    let expected_win_tik = vec![
        compute_profit(
            profit_and_loss.take_profit.condition,
            trade_kind,
            entry_price,
        ) / to_tik,
    ];
    let expected_loss_tik =
        vec![compute_profit(profit_and_loss.stop_loss.condition, trade_kind, entry_price) / to_tik];
    let expected_win_dollar = vec![expected_win_tik[0] * tik_to_dollar];
    let expected_loss_dollar = vec![expected_loss_tik[0] * tik_to_dollar];
    let crv = vec![(expected_win_tik[0] / expected_loss_tik[0]).abs()];
    let entry_ts = vec![NaiveDateTime::from_timestamp_opt(entry_timestamp / 1000, 0)
        .unwrap()
        .format("%Y-%m-%d %H:%M:%S")
        .to_string()];
    let target_ts = match profit_and_loss.take_profit.entry_time_stamp {
        Some(ts) => vec![NaiveDateTime::from_timestamp_opt(ts / 1000, 0)
            .unwrap()
            .format("%Y-%m-%d %H:%M:%S")
            .to_string()],
        None => vec![String::from("Timeout")],
    };
    let stop_loss_ts = match profit_and_loss.stop_loss.entry_time_stamp {
        Some(ts) => vec![NaiveDateTime::from_timestamp_opt(ts / 1000, 0)
            .unwrap()
            .format("%Y-%m-%d %H:%M:%S")
            .to_string()],
        None => vec![String::from("Timeout")],
    };

    let status = vec![determine_status(profit_and_loss)];

    // let exit_price = match profit_and_loss.take_profit.triggered {
    //     // We entered a trade so the tp condition was hit
    //     true => vec![profit_and_loss.take_profit.condition],

    //     // We ran into a timeout so the last trade price is the exit
    //     false => {
    //         let x = profit_and_loss.take_profit.timeout.as_ref().unwrap();
    //         vec![x.condition]
    //     }
    // };

    // TODO we set exit price below when computing pl_tik
    let mut exit_price = vec![0.0];

    // If sl timestamp < tp timestamp => sl price
    let sl_timestamp = profit_and_loss.stop_loss.entry_time_stamp;
    let tp_timestamp = profit_and_loss.take_profit.entry_time_stamp;
    let pl_tik = if sl_timestamp.is_none() && tp_timestamp.is_none() {
        // We ran into a timeout so the last trade price is the exit
        let x = profit_and_loss.take_profit.timeout.as_ref().unwrap();
        exit_price = vec![x.condition];
        // Timeout
        match &profit_and_loss.take_profit.timeout {
            Some(to) => vec![to.profit / to_tik],
            None => panic!("timeout is None but trade ran into a timeout"),
        }
    } else {
        if sl_timestamp.is_none() {
            exit_price = vec![profit_and_loss.take_profit.condition];
            // Only tp_timestamp triggered
            vec![profit_and_loss.take_profit.profit.unwrap() / to_tik]
        } else if tp_timestamp.is_none() {
            exit_price = vec![profit_and_loss.stop_loss.condition];
            // Only sl_timestamp triggerd
            vec![profit_and_loss.stop_loss.profit.unwrap() / to_tik]
        } else {
            // tp_ts and sl_timestamp tiggered
            let tp_ts = tp_timestamp.unwrap();
            let sl_ts = sl_timestamp.unwrap();
            if tp_ts < sl_ts {
                exit_price = vec![profit_and_loss.take_profit.condition];
                vec![profit_and_loss.take_profit.profit.unwrap() / to_tik]
            } else if sl_ts < tp_ts {
                exit_price = vec![profit_and_loss.stop_loss.condition];
                vec![profit_and_loss.stop_loss.profit.unwrap() / to_tik]
            } else {
                // TODO panic!("take profit and stop loss timestamp triggered at the same time")
                // We cannot say if it was a winner or loser
                vec![0.0]
            }
        }
    };
    // let pl_tik = match profit_and_loss.take_profit.profit {
    //     Some(p) => vec![p / to_tik],
    //     None => match &profit_and_loss.take_profit.timeout {
    //         Some(to) => vec![to.profit / to_tik],
    //         None => panic!("timeout is None but trade ran into a timeout"),
    //     },
    // };
    let pl_dollar = vec![pl_tik[0] * tik_to_dollar];

    let p_and_l = df!(
        &ProfitAndLossColumnNames::CalendarWeek.to_string() => &calender_week,
        &ProfitAndLossColumnNames::Date.to_string() => &date,
        &ProfitAndLossColumnNames::Strategy.to_string() => &strategy,
        &ProfitAndLossColumnNames::Market.to_string() => &market,
        &ProfitAndLossColumnNames::TradeDirection.to_string() => &trade_direction,
        &ProfitAndLossColumnNames::Entry.to_string() => &entry,
        &ProfitAndLossColumnNames::TakeProfit.to_string() => &target,
        &ProfitAndLossColumnNames::StopLoss.to_string() => &stop_loss,
        &ProfitAndLossColumnNames::ExpectedWinTik.to_string() => &expected_win_tik,
        &ProfitAndLossColumnNames::ExpectedLossTik.to_string() => &expected_loss_tik,
        &ProfitAndLossColumnNames::ExpectedWinDollar.to_string() => &expected_win_dollar,
        &ProfitAndLossColumnNames::ExpectedLossDollar.to_string() => &expected_loss_dollar,
        &ProfitAndLossColumnNames::Crv.to_string() => &crv,
        &ProfitAndLossColumnNames::EntryTimestamp.to_string() => &entry_ts,
        &ProfitAndLossColumnNames::TargetTimestamp.to_string() => &target_ts,
        &ProfitAndLossColumnNames::StopLossTimestamp.to_string() => &stop_loss_ts,
        &ProfitAndLossColumnNames::ExitPrice.to_string() => &exit_price,
        &ProfitAndLossColumnNames::Status.to_string() => &status,
        &ProfitAndLossColumnNames::PlTik.to_string() => &pl_tik,
        &ProfitAndLossColumnNames::PlDollar.to_string() => &pl_dollar,
    );
    p_and_l.unwrap()
    // let out_dir = gcs::Strategy::find(finder, &InDataKind::ProfitAndLoss);
    // dbg!(&out_dir);
    // save_file(
    //     self.dp.get_client(),
    //     p_and_l.unwrap(),
    //     &out_dir,
    //     "pl.csv",
    // )
    // .await; // &format!("{}.csv", cw)
}

fn report_without_trade(
    finder: &Finder,
    cw: i64,
    day: Weekday,
    entry_price: f64,
    trade_kind: &TradeKind,
) -> DataFrame {
    let calender_week = vec![cw];
    let date = vec![NaiveDate::from_isoywd_opt(
        i32::try_from(finder.get_year()).unwrap(),
        u32::try_from(cw).unwrap(),
        day,
    )
    .unwrap()
    .format("%Y-%m-%d")
    .to_string()];
    let strategy = vec!["PPP"];
    let market = vec![finder.get_market().to_string()];
    let trade_direction = vec![trade_kind.to_string()];
    let entry = vec![entry_price];
    // let target = vec![profit_and_loss.take_profit.condition];
    // let stop_loss = vec![profit_and_loss.stop_loss.condition];
    // let expected_win_tik = vec![compute_profit(
    //     profit_and_loss.take_profit.condition,
    //     trade_kind,
    //     entry_price,
    // )];
    // let expected_loss_tik = vec![compute_profit(
    //     profit_and_loss.stop_loss.condition,
    //     trade_kind,
    //     entry_price,
    // )];
    // let expected_win_dollar = vec![compute_profit(
    //     profit_and_loss.take_profit.condition,
    //     trade_kind,
    //     entry_price,
    // )];
    // let expected_loss_dollar = vec![compute_profit(
    //     profit_and_loss.stop_loss.condition,
    //     trade_kind,
    //     entry_price,
    // )];
    // let crv = vec![(expected_win_tik[0] / expected_loss_tik[0]).abs()];

    let p_and_l = df!(
        &ProfitAndLossColumnNames::CalendarWeek.to_string() => &calender_week,
        &ProfitAndLossColumnNames::Date.to_string() => &date,
        &ProfitAndLossColumnNames::Strategy.to_string() => &strategy,
        &ProfitAndLossColumnNames::Market.to_string() => &market,
        &ProfitAndLossColumnNames::TradeDirection.to_string() => &trade_direction,
        &ProfitAndLossColumnNames::Entry.to_string() => &entry,
        &ProfitAndLossColumnNames::TakeProfit.to_string() => vec![0.0], // &target,
        &ProfitAndLossColumnNames::StopLoss.to_string() => vec![0.0], // &stop_loss,
        &ProfitAndLossColumnNames::ExpectedWinTik.to_string() => vec![0.0], // &expected_win_tik,
        &ProfitAndLossColumnNames::ExpectedLossTik.to_string() => vec![0.0], // &expected_loss_tik,
        &ProfitAndLossColumnNames::ExpectedWinDollar.to_string() => vec![0.0], // &expected_win_dollar,
        &ProfitAndLossColumnNames::ExpectedLossDollar.to_string() => vec![0.0], // &expected_loss_dollar,
        &ProfitAndLossColumnNames::Crv.to_string() => vec![0.0], // &crv,
        &ProfitAndLossColumnNames::EntryTimestamp.to_string() => &[String::from("NoEntry")],
        &ProfitAndLossColumnNames::TargetTimestamp.to_string() => &[String::from("NoEntry")],
        &ProfitAndLossColumnNames::StopLossTimestamp.to_string() => &[String::from("NoEntry")],
        &ProfitAndLossColumnNames::ExitPrice.to_string() => &[0.0],
        &ProfitAndLossColumnNames::Status.to_string() => &[String::from("NoEntry")],
        &ProfitAndLossColumnNames::PlTik.to_string() => &[0.0],
        &ProfitAndLossColumnNames::PlDollar.to_string() => &[0.0],
    );

    p_and_l.unwrap()
    // let out_dir = gcs::Strategy::find(finder, &InDataKind::ProfitAndLoss);
    // save_file(
    //     self.dp.get_client(),
    //     p_and_l.unwrap(),
    //     &out_dir,
    //     "pl.csv",
    // )
    // .await; // &format!("{}.csv", cw)
}

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

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::Arc;

    use crate::{
        common::finder::Finder,
        config::GCS_DATA_BUCKET,
        enums::{
            bots::BotKind,
            data::{LeafDir, RootDir},
            markets::{GranularityKind, MarketKind},
            trades::TradeKind,
        },
        producers::{test::Test, DataProducer},
        streams::tests::load_csv,
    };
    use google_cloud_default::WithAuthExt;
    use google_cloud_storage::client::{Client, ClientConfig};

    // TODO: Muss hier überhaupt was getestet werden?
    #[tokio::test]
    async fn test_generate_report() {}

    #[tokio::test]
    async fn test_report_with_trade() {
        let config = ClientConfig::default().with_auth().await.unwrap();
        let dp: Arc<dyn DataProducer + Send + Sync> =
            Arc::new(Test::new(std::path::PathBuf::from(GCS_DATA_BUCKET)));

        let finder: Finder = Finder::new(
            dp.get_bucket_name(),
            dp.get_data_producer_kind(),
            MarketKind::BtcUsdt,
            2022,
            BotKind::Ppp,
            GranularityKind::Weekly,
        )
        .await;
        let cw = 10;
        let entry_price = 42_100.0;
        let entry_ts = 1646812800000_i64;
        let trade_kind = TradeKind::Short;
        let profit_and_loss_1 = ProfitAndLoss {
            stop_loss: StopLossTrade {
                condition: 42_594.06,
                entry_time_stamp: Some(1646838000000_i64),
                profit: Some(-494.06),
            },
            take_profit: TakeProfitTrade {
                condition: 29_004.73,
                triggered: false,
                entry_time_stamp: None,
                profit: None,
                timeout: Some(Timeout {
                    condition: 39_385.01,
                    profit: 2714.99,
                }),
            },
        };
        let profit_and_loss_2 = ProfitAndLoss {
            stop_loss: StopLossTrade {
                condition: 44_601.12,
                entry_time_stamp: None,
                profit: None,
            },
            take_profit: TakeProfitTrade {
                condition: 39_104.73,
                triggered: true,
                entry_time_stamp: Some(1646888400000_i64),
                profit: Some(2_995.27),
                timeout: None,
            },
        };

        let target_dir = finder._path_to_target(&RootDir::Strategy, &LeafDir::ProfitAndLoss);
        // let out_dir = finder.find(&RootDir::Strategy, &InDataKind::ProfitAndLoss);

        finder
            .delete_files(
                finder.get_client_clone(),
                RootDir::Strategy,
                LeafDir::ProfitAndLoss,
            )
            .await;
        let result = report_with_trade(
            &finder,
            cw,
            Weekday::Mon,
            entry_price,
            entry_ts,
            &trade_kind,
            &profit_and_loss_1,
        );
        // let result = load_csv(&out_dir, "pl.csv").await.unwrap();
        let target_pl1 = load_csv(&target_dir, "10_pl1.csv").await.unwrap();
        assert_eq!(dbg!(result).frame_equal(dbg!(&target_pl1)), true);

        finder
            .delete_files(
                finder.get_client_clone(),
                RootDir::Strategy,
                LeafDir::ProfitAndLoss,
            )
            .await;
        let result = report_with_trade(
            &finder,
            cw,
            Weekday::Mon,
            entry_price,
            entry_ts,
            &trade_kind,
            &profit_and_loss_2,
        );
        // let result = load_csv(&out_dir, "pl.csv").await.unwrap();
        let target_pl2 = load_csv(&target_dir, "10_pl2.csv").await.unwrap();
        assert_eq!(result.frame_equal(&target_pl2), true);
    }

    #[tokio::test]
    async fn test_report_without_trade() {
        let config = ClientConfig::default().with_auth().await.unwrap();
        let dp: Arc<dyn DataProducer + Send + Sync> =
            Arc::new(Test::new(std::path::PathBuf::from(GCS_DATA_BUCKET)));

        let finder: Finder = Finder::new(
            dp.get_bucket_name(),
            dp.get_data_producer_kind(),
            MarketKind::BtcUsdt,
            2022,
            BotKind::Ppp,
            GranularityKind::Weekly,
        )
        .await;

        let cw = 10;
        let entry_price = 42_100.0;
        let trade_kind = TradeKind::Short;

        let target_dir = finder._path_to_target(&RootDir::Strategy, &LeafDir::ProfitAndLoss);
        // let out_dir = finder.find(&RootDir::Strategy, &InDataKind::ProfitAndLoss);
        finder
            .delete_files(
                finder.get_client_clone(),
                RootDir::Strategy,
                LeafDir::ProfitAndLoss,
            )
            .await;

        let result = report_without_trade(&finder, cw, Weekday::Mon, entry_price, &trade_kind);
        // let result = load_csv(&out_dir, "pl.csv").await.unwrap();
        let target_pl1 = load_csv(&target_dir, "no_trade_10.csv").await.unwrap();
        assert_eq!(result.frame_equal(&target_pl1), true);
    }
}
