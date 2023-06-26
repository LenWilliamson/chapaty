use std::sync::Arc;

use enum_map::EnumMap;
use polars::prelude::{col, lit, AnyValue, DataFrame, IntoLazy};

use crate::{
    bots::Bot,
    enums::{
        bots::{PreTradeDataKind, TradeDataKind},
        columns::{Columns, OhlcColumnNames, VolumeProfileColumnNames},
        trades::TradeKind,
    },
    producers::{
        profit_loss_report::{ProfitAndLoss, StopLossTrade, TakeProfitTrade, Timeout},
        DataProducer,
    },
    // utils::trade::{PreTradeData, TradeData},
};

use super::wrappers::{unwrap_float64, unwrap_int64};

/// This function computes the `PreTradeData` consiting of
/// * `last_trade_price` - closing price at `TimeInterval::{end_day, end_h}`,
/// * `poc` - price we are willing to enter,
/// * `lowest_trade_price` - lowest traded price in this week,
/// * `highest_trade_price` - highest traded price in this week,
/// * `trade` - if `last_trade_price` {< | = | >} `poc` => {short | none | long},
///
/// that is needed to evaluate our strategy in the subsequent week.
///
/// # Assumptions
/// * OHLC data is sorted by time
///
/// # Arguments
/// * `df_ohlc` - OHLC data
/// * `poc` - point of control
pub fn compute_pre_trade_data(
    dp: Arc<dyn DataProducer + Send + Sync>,
    df_ohlc: DataFrame,
    df_vol: DataFrame,
    ptd: Vec<PreTradeDataKind>,
) -> EnumMap<PreTradeDataKind, f64> {
    let mut pre_trade_data_map = EnumMap::default();
    for request in ptd {
        match request {
            PreTradeDataKind::Poc => {
                let res = compute_poc(dp.clone(), df_vol.clone());
                pre_trade_data_map[PreTradeDataKind::Poc] = res;
            }
            PreTradeDataKind::LastTradePrice => {
                let res = compute_last_trade_price(dp.clone(), df_ohlc.clone());
                pre_trade_data_map[PreTradeDataKind::LastTradePrice] = res;
            }
            PreTradeDataKind::LowestTradePrice => {
                let res = compute_lowest_trade_price(dp.clone(), df_ohlc.clone());
                pre_trade_data_map[PreTradeDataKind::LowestTradePrice] = res;
            }
            PreTradeDataKind::HighestTradePrice => {
                let res = compute_highest_trade_price(dp.clone(), df_ohlc.clone());
                pre_trade_data_map[PreTradeDataKind::HighestTradePrice] = res;
            }
            _ => panic!("Not yet implemented!"),
        }
    }

    pre_trade_data_map
}

/// This function computes the `Option<TradeData>` consisting of
/// * `entry_ts` - entry time stamp where the market hits the `poc`
/// * `last_trade_price` - closing price of this week
/// * `lowest_price_since_entry` - lowest traded price since we entered our trade
/// * `highest_price_since_entry` - highest traded price since we entered our trade
/// * `lowest_price_since_entry_ts` - time stamp of lowest traded price since we entered our trade
/// * `highest_price_since_entry_ts` - time stamp of highest traded price since we entered our trade
///
/// We determine in this function if we enter a trade or not. We return
/// * `None` - if we don't enter a trade
/// * `Some(TradeData)` - if we enter a trade
///
/// # Note
///
/// A Trade has two phases which are each in two separates but consecutive calendar weeks. The first
/// phase is the pre trade phase (`PreTradeData`). The second phase is the trade phase (`TradeData`). In each phase we collect data.
/// If we don't enter the trade in the second week, the `trade_data` object is `None`.
///
/// Collects the data if a trade occurs in the current week
///
/// TODO: Funktionen schreiben für die Berechnungen und diese in `compute_pre_trade_data` wiederverwenden
pub fn compute_trade_data<'a>(
    dp: Arc<dyn DataProducer + Send + Sync>,
    df_ohlc: DataFrame,
    entry_price: f64,
) -> Option<EnumMap<TradeDataKind, AnyValue<'a>>> {
    if let Some(ts) = time_when_price_taken(&dp, df_ohlc.clone(), entry_price) {
        let mut trade_data_map = EnumMap::default();

        // BEGIN
        let ots = dp.column_name_as_str(&Columns::Ohlc(OhlcColumnNames::OpenTime));
        let high = dp.column_name_as_str(&Columns::Ohlc(OhlcColumnNames::High));
        let low = dp.column_name_as_str(&Columns::Ohlc(OhlcColumnNames::Low));
        let close = dp.column_name_as_str(&Columns::Ohlc(OhlcColumnNames::Close));
        let filt = df_ohlc
            .lazy()
            .filter(col(&ots).gt_eq(lit(ts)))
            .select([
                col(&close).last(),
                col(&low).min(),
                col(&high).max(),
                col(&ots)
                    .filter(col(&high).eq(col(&high).max()))
                    .first()
                    .alias("high_ts"),
                col(&ots)
                    .filter(col(&low).eq(col(&low).min()))
                    .first()
                    .alias("low_ts"),
            ])
            .collect()
            .unwrap();

        // DONE

        let v = filt.get(0).unwrap();
        let lst_tp = unwrap_float64(&v[0]);
        let low = unwrap_float64(&v[1]);
        let high = unwrap_float64(&v[2]);
        let high_ots = unwrap_int64(&v[3]);
        let low_ots = unwrap_int64(&v[4]);

        trade_data_map[TradeDataKind::EntryPrice] = AnyValue::Float64(entry_price);
        trade_data_map[TradeDataKind::EntryTimestamp] = AnyValue::Int64(ts);
        trade_data_map[TradeDataKind::LastTradePrice] = AnyValue::Float64(lst_tp);
        trade_data_map[TradeDataKind::LowestTradePriceSinceEntry] = AnyValue::Float64(low);
        trade_data_map[TradeDataKind::LowestTradePriceSinceEntryTimestamp] =
            AnyValue::Int64(low_ots);
        trade_data_map[TradeDataKind::HighestTradePriceSinceEntry] = AnyValue::Float64(high);
        trade_data_map[TradeDataKind::HighestTradePriceSinceEntryTimestamp] =
            AnyValue::Int64(high_ots);

        Some(trade_data_map)
    } else {
        None
    }
}

/// This function computes the `ProfitAndLoss` consiting of
/// * `stop_loss: StopLossTrade`,
///   * `condition` - the stop loss condition
///   * `entry_time_stamp` - when the condition is taken, otherwise `None`
///   * `profit` - the loss, i.e. `profit < 0` we made when the condition is taken, otherwise `None`
/// * `take_profit: TakeProfitTrade`,
///   * `condition` - the stop loss condition
///   * `triggered` - `true` if the `condition` is triggered, else `false`
///   * `entry_time_stamp` - when the condition is taken, otherwise `None`
///   * `profit` - the profit > 0 we made when the condition is taken, otherwise `None`
///   * `timeout: Timeout` - neither stop loss, nor take profit is triggered. Then we run in a timeout at the end of the week
///     * `condition` - last traded price in that week
///     * `profit` - profit or loss we face, when running into a timeout
///
/// that is generated by our strategy in the current week.
///
/// # Assumptions
/// * OHLC data is sorted by time
///
/// # Arguments
/// * `df_ohlc` - OHLC data
/// * `pre_trade_data` - trade data of the previous week to evaluate the P&L for the PPP
/// * `trade_data` - trade data of the current week to evaluate the P&L for the PPP
/// * `sl` - stop loss condition
/// * `tp` - take profit condition
pub fn compute_profit_and_loss(
    dp: Arc<dyn DataProducer + Send + Sync>,
    bot: Arc<dyn Bot + Send + Sync>,
    df_ohlc: DataFrame,
    pre_trade_data_map: &EnumMap<PreTradeDataKind, f64>,
    trade_data_map: &EnumMap<TradeDataKind, AnyValue>,
) -> ProfitAndLoss {
    let sl_price = bot.get_sl_price(&pre_trade_data_map);
    // Compute df_ohlc_filtered for the right time statistics.
    // Otherwise we can get an timestamp earlier our entry timestamp, because the market hits the exit price before we enter the trade
    // TODO avoid clone
    let df_filtered = df_ohlc
        .clone()
        .lazy()
        .filter(
            col(&dp.column_name_as_str(&Columns::Ohlc(OhlcColumnNames::OpenTime))).gt_eq(lit(
                unwrap_int64(&trade_data_map[TradeDataKind::EntryTimestamp]),
            )),
        )
        .collect()
        .unwrap();
    // Retrieve entry timestamp when stop-loss condition is taken
    let sl_entry_timestamp = time_when_price_taken(&dp, df_filtered.clone(), sl_price);

    // Compute loss (i.e. profit < 0)
    let profit = match sl_entry_timestamp {
        // Some loss, ass entry timestamp was taken
        Some(_) => Some(compute_profit(
            sl_price,
            &bot.get_trade_kind(&pre_trade_data_map).unwrap(),
            bot.get_entry_price(&pre_trade_data_map),
        )),
        // Entry timestamp not taken. Hence, no loss
        None => None,
    };

    // Populate StopLossTrade
    let stop_loss = StopLossTrade {
        condition: sl_price,
        entry_time_stamp: sl_entry_timestamp,
        profit,
    };

    let tp_price = bot.get_tp_price(&pre_trade_data_map);
    // Retrieve entry timestamp when take-profit condition is taken
    let tp_entry_timestamp = time_when_price_taken(&dp, df_filtered, tp_price);
    let profit = match tp_entry_timestamp {
        Some(_) => Some(compute_profit(
            tp_price,
            &bot.get_trade_kind(&pre_trade_data_map).unwrap(),
            bot.get_entry_price(&pre_trade_data_map),
        )),
        None => None,
    };

    let triggered = if tp_entry_timestamp.is_some() {
        true
    } else {
        false
    };

    // Retrieve last trade price
    let last_trade_price = &trade_data_map[TradeDataKind::LastTradePrice];
    let last_trade_price = unwrap_float64(last_trade_price);

    // If triggered false we run into a timeout, because we entered a trade
    let timeout = if triggered {
        None
    } else {
        Some(Timeout {
            condition: last_trade_price,
            profit: match bot.get_trade_kind(&pre_trade_data_map).unwrap() {
                TradeKind::Short => bot.get_entry_price(&pre_trade_data_map) - last_trade_price,
                TradeKind::Long => last_trade_price - bot.get_entry_price(&pre_trade_data_map),
                TradeKind::None => {
                    panic!("Cannot compute profit for TradeKind::None")
                }
            },
        })
    };

    // Populate TakeProfitTrade
    let take_profit = TakeProfitTrade {
        condition: tp_price,
        triggered,
        entry_time_stamp: tp_entry_timestamp,
        profit,
        timeout,
    };

    ProfitAndLoss {
        stop_loss,
        take_profit,
    }
}

/// This function computes the POC for the given volume profile. The POC is the point of control. Hence,
/// the price where the highest volume for a given time interval was traded.
///
/// # Arguments
/// * `df_vol` - volume profile
fn compute_poc(dp: Arc<dyn DataProducer + Send + Sync>, df: DataFrame) -> f64 {
    let qx = &dp.column_name_as_str(&Columns::Vol(VolumeProfileColumnNames::Quantity));
    let px = &dp.column_name_as_str(&Columns::Vol(VolumeProfileColumnNames::Price));

    let sorted = df
        .lazy()
        .select([col(&px).filter(col(&qx).eq(col(&qx).max()))])
        .collect()
        .unwrap();

    let poc = unwrap_float64(&sorted.get(0).unwrap()[0]);
    poc
}

/// This function returns the first `Option<i64>` timestamp, when the price is met by the OHLC chart.
///
/// # Arguments
/// * `df_ohlc` - OHLC data
/// * `px` - price
fn time_when_price_taken(
    dp: &Arc<dyn DataProducer + Send + Sync>,
    df_ohlc: DataFrame,
    px: f64,
) -> Option<i64> {
    let high = dp.column_name_as_str(&Columns::Ohlc(OhlcColumnNames::High));
    let low = dp.column_name_as_str(&Columns::Ohlc(OhlcColumnNames::Low));
    let ots = dp.column_name_as_str(&Columns::Ohlc(OhlcColumnNames::OpenTime));
    let filt = df_ohlc
        .clone()
        .lazy()
        .select([col(&ots).filter(col(&low).lt_eq(lit(px)).and(col(&high).gt_eq(lit(px))))])
        .first()
        .collect()
        .unwrap();

    if 0 == filt.shape().0 {
        // Now rows in filtered data frame means there is no data
        None
    } else {
        Some(unwrap_int64(&filt.get(0).unwrap()[0]))
    }
}

/// This function computes the profit of a trade depending on its `TradeKind`
/// * `TradeKind::Short` - `entry_price - exit_px`
/// * `TradeKind::Long` - `exit_px - entry_price`
/// * `TradeKind::None` - `panic!("Cannot compute profit for TradeKind::None")`
pub fn compute_profit(
    exit_px: f64, // entry_ts: i64
    trade_kind: &TradeKind,
    entry_price: f64,
) -> f64 {
    match trade_kind {
        TradeKind::Short => entry_price - exit_px,
        TradeKind::Long => exit_px - entry_price,
        TradeKind::None => {
            panic!("Cannot compute profit for TradeKind::None")
        }
    }
}

/// This function determines the `ProfitAndLossColumnsNames::Status` for a `data_model::profit_and_loss::ProfitAndLoss`
/// # Arguments
/// * `p_and_l`- Profit and loss statement we want to determine a trade
pub fn determine_status(p_and_l: &ProfitAndLoss) -> String {

    if p_and_l.stop_loss.entry_time_stamp.is_none() && p_and_l.take_profit.entry_time_stamp.is_none() {
        let res = if p_and_l.take_profit.timeout.unwrap().profit < 0.0 {
            String::from("Loser")
        } else {
            String::from("Winner")
        };

        return res;
    }

    let sl_entry = match p_and_l.stop_loss.entry_time_stamp {
        Some(ts) => ts,
        None => i64::MAX,
    };

    let tp_entry = match p_and_l.take_profit.entry_time_stamp {
        Some(ts) => ts,
        None => i64::MAX,
    };

    if sl_entry < tp_entry {
        String::from("Loser")
    } else if sl_entry > tp_entry {
        String::from("Winner")
    } else {
        String::from("Not Clear")
    }
}

fn compute_last_trade_price(dp: Arc<dyn DataProducer + Send + Sync>, df: DataFrame) -> f64 {
    let close = dp.column_name_as_str(&Columns::Ohlc(OhlcColumnNames::Close));
    let filt = df.lazy().select([col(&close).last()]).collect().unwrap();

    let v = filt.get(0).unwrap();
    let last_trade_price = unwrap_float64(&v[0]);
    last_trade_price
}
fn compute_lowest_trade_price(dp: Arc<dyn DataProducer + Send + Sync>, df: DataFrame) -> f64 {
    let low = dp.column_name_as_str(&Columns::Ohlc(OhlcColumnNames::Low));
    let filt = df.lazy().select([col(&low).min()]).collect().unwrap();

    let v = filt.get(0).unwrap();
    let lowest_trade_price = unwrap_float64(&v[0]);
    lowest_trade_price
}
fn compute_highest_trade_price(dp: Arc<dyn DataProducer + Send + Sync>, df: DataFrame) -> f64 {
    let high = dp.column_name_as_str(&Columns::Ohlc(OhlcColumnNames::High));
    let filt = df.lazy().select([col(&high).max()]).collect().unwrap();

    let v = filt.get(0).unwrap();
    let highest_trade_price = unwrap_float64(&v[0]);
    highest_trade_price
}

#[cfg(test)]
mod tests {
    use google_cloud_default::WithAuthExt;
    use google_cloud_storage::client::{Client, ClientConfig};
    use polars::{df, prelude::NamedFrom};

    use crate::{
        bots::ppp::Ppp,
        common::functions::df_from_file,
        config::GCS_DATA_BUCKET,
        enums::strategies::{StopLossKind, TakeProfitKind},
        producers::{
            profit_loss_report::{StopLoss, TakeProfit},
            test::Test,
        },
    };

    use super::*;

    #[tokio::test]
    async fn test_compute_poc() {
        let config = ClientConfig::default().with_auth().await.unwrap();
        let dp: Arc<dyn DataProducer + Send + Sync> = Arc::new(Test::new(
            std::path::PathBuf::from(GCS_DATA_BUCKET),
        ));

        let px = dp.column_name_as_str(&Columns::Vol(VolumeProfileColumnNames::Price));
        let qx = dp.column_name_as_str(&Columns::Vol(VolumeProfileColumnNames::Quantity));
        let df = df!(
            &px => &[ 83_200.0, 38_100.0, 38_000.0, 1.0],
            &qx => &[100.0, 300.0, 150.0, 300.0],
        );
        let poc = 38_100.0;
        assert_eq!(poc, compute_poc(dp, df.unwrap()));

        // TODO Aktuell ist der POC der kleinste Preis, wenn es mehrere gibt
    }

    #[tokio::test]
    async fn test_compute_last_trade_price() {
        let config = ClientConfig::default().with_auth().await.unwrap();
        let dp: Arc<dyn DataProducer + Send + Sync> = Arc::new(Test::new(
            std::path::PathBuf::from(GCS_DATA_BUCKET),
        ));

        let out_dir = std::path::PathBuf::from("data/test/other/ppp");
        let file = out_dir.join("pre_trade_data.csv");

        // Load test data
        let df = df_from_file(&file, None, None)
            .await
            .unwrap();

        // Define target values from test data
        let last_trade_price = 43_578.87;

        let res = compute_last_trade_price(dp, df);
        assert_eq!(last_trade_price, res);
    }

    #[tokio::test]
    async fn test_compute_lowest_trade_price() {
        let config = ClientConfig::default().with_auth().await.unwrap();
        let dp: Arc<dyn DataProducer + Send + Sync> = Arc::new(Test::new(
            std::path::PathBuf::from(GCS_DATA_BUCKET),
        ));

        let out_dir = std::path::PathBuf::from("data/test/other/ppp");
        let file = out_dir.join("pre_trade_data.csv");

        // Load test data
        let df = df_from_file( &file, None, None)
            .await
            .unwrap();

        // Define target values from test data
        let lowest_trade_price = 37_934.89;

        let res = compute_lowest_trade_price(dp, df);
        assert_eq!(lowest_trade_price, res);
    }

    #[tokio::test]
    async fn test_compute_highest_trade_price() {
        let config = ClientConfig::default().with_auth().await.unwrap();
        let dp: Arc<dyn DataProducer + Send + Sync> = Arc::new(Test::new(
            std::path::PathBuf::from(GCS_DATA_BUCKET),
        ));

        let out_dir = std::path::PathBuf::from("data/test/other/ppp");
        let file = out_dir.join("pre_trade_data.csv");

        // Load test data
        let df = df_from_file( &file, None, None)
            .await
            .unwrap();

        // Define target values from test data
        let highest_trade_price = 44_225.84;

        let res = compute_highest_trade_price(dp, df);
        assert_eq!(highest_trade_price, res);
    }

    /// This unit test computes the `PreTradeData` consiting of
    /// * `last_trade_price` - closing price at `TimeInterval::{end_day, end_h}`,
    /// * `poc` - price we are willing to enter,
    /// * `lowest_trade_price` - lowest traded price in this week,
    /// * `highest_trade_price` - highest traded price in this week,
    /// * `trade` - if `last_trade_price` {< | = | >} `poc` => {short | none | long},
    ///
    /// that is needed to evaluate our strategy in the subsequent week.
    ///
    /// # Set up
    /// * The OHLC data is sorted in `{EDS}/test/other/ppp/pre_trade_data.csv`
    /// * We test against all cases `{short | none | long}`
    #[tokio::test]
    async fn test_compute_pre_trade_data() {}

    /// This unit test computes the `TradeData` consiting of
    /// * `entry_ts` - entry time stamp where the market hits the `poc`
    /// * `last_trade_price` - closing price of this week
    /// * `lowest_price_since_entry` - lowest traded price since we entered our trade
    /// * `highest_price_since_entry` - highest traded price since we entered our trade
    /// * `lowest_price_since_entry_ts` - time stamp of lowest traded price since we entered our trade
    /// * `highest_price_since_entry_ts` - time stamp of highest traded price since we entered our trade
    ///
    /// that is needed to evaluate our strategy in the subsequent week. Or returns `None` if no traded occured.
    ///
    /// # Set up
    /// * The OHLC data is sorted in `{EDS}/test/other/ppp/pre_trade_data.csv`
    /// * We test against all cases `{Some(TradeData) | None}`
    #[tokio::test]
    async fn test_compute_trade_data() {let config = ClientConfig::default().with_auth().await.unwrap();
        let dp: Arc<dyn DataProducer + Send + Sync> = Arc::new(Test::new(
            std::path::PathBuf::from(GCS_DATA_BUCKET),
        ));

        let out_dir = std::path::PathBuf::from("data/test/other/ppp");
        let file = out_dir.join("pre_trade_data.csv");

        // Load test data
        let df = df_from_file(&file, None, None)
            .await
            .unwrap();

        // Define target values from test data
        let entry_price = 42_000.0;
        let ts = 1646085600000_i64;
        let lst_tp = 43_578.87;
        let low = 41628.99;
        let high = 44_225.84;
        let low_ots = 1646085600000_i64;
        let high_ots = 1646085600000_i64;
        let mut target_trade = EnumMap::default();

        target_trade[TradeDataKind::EntryPrice] = AnyValue::Float64(entry_price);
        target_trade[TradeDataKind::EntryTimestamp] = AnyValue::Int64(ts);
        target_trade[TradeDataKind::LastTradePrice] = AnyValue::Float64(lst_tp);
        target_trade[TradeDataKind::LowestTradePriceSinceEntry] = AnyValue::Float64(low);
        target_trade[TradeDataKind::LowestTradePriceSinceEntryTimestamp] = AnyValue::Int64(low_ots);
        target_trade[TradeDataKind::HighestTradePriceSinceEntry] = AnyValue::Float64(high);
        target_trade[TradeDataKind::HighestTradePriceSinceEntryTimestamp] =
            AnyValue::Int64(high_ots);

        // Test if compute_pre_trade_data returns the correct output
        // BEGIN: Some(TradeData)
        match compute_trade_data(dp.clone(), df.clone(), entry_price) {
            Some(trade) => assert_eq!(target_trade, trade),
            None => assert!(false),
        }
        // END: Some(TradeData)

        // BEGIN: None (choose poc = 0.0)
        match compute_trade_data(dp, df.clone(), 0.0) {
            Some(_) => assert!(false),
            None => assert!(true),
        }
        // END: None
    }

    #[test]
    fn test_test_compute_profit() {
        let entry_price = 100.0;
        assert_eq!(-1.0, compute_profit(101.0, &TradeKind::Short, entry_price));
        assert_eq!(0.0, compute_profit(100.0, &TradeKind::Short, entry_price));
        assert_eq!(1.0, compute_profit(99.0, &TradeKind::Short, entry_price));

        assert_eq!(1.0, compute_profit(101.0, &TradeKind::Long, entry_price));
        assert_eq!(0.0, compute_profit(100.0, &TradeKind::Long, entry_price));
        assert_eq!(-1.0, compute_profit(99.0, &TradeKind::Long, entry_price));
    }

    // #[test]
    // #[should_panic(expected = "Cannot compute profit for TradeKind::None")]
    // fn test_compute_profit_should_panic() {
    //     compute_profit(0.0, &TradeKind::None, 0.0);
    // }

    /// This unit test checks if we compute the right timestamp if a price is taken by the OHLC chart.
    ///
    /// # Set up
    /// * The OHLC data is sorted in `{EDS}/test/other/ppp/pre_trade_data.csv`
    /// * We test against all cases `{Some(x) | None}`
    #[tokio::test]
    async fn test_test_time_when_price_taken() {
        let config = ClientConfig::default().with_auth().await.unwrap();
        let dp: Arc<dyn DataProducer + Send + Sync> = Arc::new(Test::new(
            std::path::PathBuf::from(GCS_DATA_BUCKET),
        ));

        let target_ts_taken = 1646085600000_i64;
        let px_taken = 42_000.0;
        let px_not_taken = 0.0;

        let out_dir = std::path::PathBuf::from("data/test/other/ppp");
        let file = out_dir.join("pre_trade_data.csv");

        // Load test data
        let df = df_from_file( &file, None, None)
            .await
            .unwrap();

        // let df = GcpCsvReader::new(dp.get_client(), String::from("trust-data"))
        //     .from_path(file)
        //     .has_header(true)
        //     .finish()
        //     .await
        //     .unwrap();

        match time_when_price_taken(&dp, df.clone(), px_taken) {
            Some(result_taken) => assert_eq!(result_taken, target_ts_taken),
            None => assert!(false),
        };

        match time_when_price_taken(&dp, df.clone(), px_not_taken) {
            Some(_) => assert!(false),
            None => assert!(true),
        };
    }

    /// This unit test computes the `ProfitAndLoss` consiting of
    /// * `stop_loss: StopLossTrade`,
    ///   * `condition` - the stop loss condition
    ///   * `entry_time_stamp` - when the condition is taken, otherwise `None`
    ///   * `profit` - the loss, i.e. `profit < 0` we made when the condition is taken, otherwise `None`
    /// * `take_profit: TakeProfitTrade`,
    ///   * `condition` - the stop loss condition
    ///   * `triggered` - `true` if the `condition` is triggered, else `false`
    ///   * `entry_time_stamp` - when the condition is taken, otherwise `None`
    ///   * `profit` - the profit > 0 we made when the condition is taken, otherwise `None`
    ///   * `timeout: Timeout` - neither stop loss, nor take profit is triggered. Then we run in a timeout at the end of the week
    ///     * `condition` - last traded price in that week
    ///     * `profit` - profit or loss we face, when running into a timeout
    ///
    /// # Test data
    /// The test data files are stored inside `{EDS}/test/other/ppp/`
    /// * `8_vol.csv` contains the volume profile for `9_long.csv`
    /// * `9_vol.csv` contains the volume profile for `10_short.csv`
    ///
    /// # TODO
    /// * Choosing `PrevLow` or `PrevHigh` does not matter for Long or Short trades
    /// * We fix this and introduce a `enum` type `PrevExtrem` or similar
    /// * TP Long muss oberhalb POC sein (äquivalent für SL)
    /// * TP Short muss unterhalb POC sein (äquivalent für SL)
    /// * Test case for `panic!(...)`
    ///
    /// # Cases
    ///
    /// We test against all possible cases at least once. If a `TakeProfit` trade is not triggered, we run into a `Timeout`.
    ///
    /// |   |SL |TP |TO |
    /// |---|---|---|---|
    /// |1) | T | T | - |
    /// |2) | T | F | T |
    /// |3) | F | T | - |
    /// |4) | F | F | T |
    #[tokio::test]
    async fn test_test_compute_profit_and_loss() {
        let config = ClientConfig::default().with_auth().await.unwrap();
        let dp: Arc<dyn DataProducer + Send + Sync> = Arc::new(Test::new(
            std::path::PathBuf::from(GCS_DATA_BUCKET),
        ));

        // ################ LONG #################
        // BEGIN: Trade Data Long
        let mut pre_trade_data_map = EnumMap::default();
        pre_trade_data_map[PreTradeDataKind::Poc] = 38_100.0;
        pre_trade_data_map[PreTradeDataKind::LastTradePrice] = 39_424.14;
        pre_trade_data_map[PreTradeDataKind::LowestTradePrice] = 36_220.54;
        pre_trade_data_map[PreTradeDataKind::HighestTradePrice] = 39_843.0;

        let mut trade_data_map = EnumMap::default();
        trade_data_map[TradeDataKind::EntryTimestamp] = AnyValue::Int64(1646010000000_i64);
        trade_data_map[TradeDataKind::LastTradePrice] = AnyValue::Float64(43_160.0);
        trade_data_map[TradeDataKind::LowestTradePriceSinceEntry] = AnyValue::Float64(37_451.56);
        trade_data_map[TradeDataKind::HighestTradePriceSinceEntry] = AnyValue::Float64(44_225.84);
        trade_data_map[TradeDataKind::LowestTradePriceSinceEntryTimestamp] =
            AnyValue::Int64(1646028000000_i64);
        trade_data_map[TradeDataKind::HighestTradePriceSinceEntryTimestamp] =
            AnyValue::Int64(1646085600000_i64);

        let pre_trade_data_long = pre_trade_data_map.clone();
        let trade_data_long = trade_data_map.clone();

        let mut in_dir = std::path::PathBuf::from("data/test/other/ppp");
        let mut file = in_dir.join("9_long_curr.csv");

        // Load test data
        let df_long = df_from_file( &file, None, None)
            .await
            .unwrap();
        // END: Trade Data Long

        // BEGIN: Case 1)
        let sl_long_1 = StopLoss {
            condition: StopLossKind::PrevPoc,
            offset: 0.0, // triggered
        };

        let tp_long_1 = TakeProfit {
            condition: TakeProfitKind::PrevClose,
            offset: 0.0, // triggered
        };

        let target_long_1 = ProfitAndLoss {
            stop_loss: StopLossTrade {
                condition: 38_100.0,
                entry_time_stamp: Some(1646010000000_i64),
                profit: Some(0.0),
            },
            take_profit: TakeProfitTrade {
                condition: 39_424.14,
                triggered: true,
                entry_time_stamp: Some(1646056800000_i64),
                profit: Some(1_324.1399999999994),
                timeout: None,
            },
        };

        let mut bot = Ppp::new();
        bot.set_stop_loss(sl_long_1);
        bot.set_take_profit(tp_long_1);

        let mut result = compute_profit_and_loss(
            dp.clone(),
            Arc::new(bot),
            df_long.clone(),
            &pre_trade_data_long,
            &trade_data_long,
            // &sl_long_1,
            // &tp_long_1,
        );
        // Test for equality in stop_loss
        assert_eq!(
            target_long_1.stop_loss.condition,
            result.stop_loss.condition
        );
        assert!(matches!(
            result.stop_loss.entry_time_stamp,
            Some(1646010000000_i64)
        ));
        assert_eq!(
            target_long_1.stop_loss.profit.unwrap(),
            result.stop_loss.profit.unwrap()
        );

        // Test for equality in take_profit
        assert_eq!(
            target_long_1.take_profit.condition,
            result.take_profit.condition
        );
        assert_eq!(
            target_long_1.take_profit.triggered,
            result.take_profit.triggered
        );
        assert!(matches!(
            result.take_profit.entry_time_stamp,
            Some(1646056800000_i64)
        ));
        assert_eq!(
            target_long_1.take_profit.profit.unwrap(),
            result.take_profit.profit.unwrap()
        );
        assert!(matches!(result.take_profit.timeout, None));
        // END: Case 1)

        // BEGIN: Case 1)
        let sl_long_2 = StopLoss {
            condition: StopLossKind::PrevPoc,
            offset: 648.44, // triggered
        };

        let tp_long_2 = TakeProfit {
            condition: TakeProfitKind::PrevClose,
            offset: -100.0, // triggered
        };

        let target_long_2 = ProfitAndLoss {
            stop_loss: StopLossTrade {
                condition: 37_451.56,
                entry_time_stamp: Some(1646028000000_i64),
                profit: Some(-648.44),
            },
            take_profit: TakeProfitTrade {
                condition: 39_324.14,
                triggered: true,
                entry_time_stamp: Some(1646056800000_i64),
                profit: Some(1_224.14),
                timeout: None,
            },
        };

        let mut bot = Ppp::new();
        bot.set_stop_loss(sl_long_2);
        bot.set_take_profit(tp_long_2);

        result = compute_profit_and_loss(
            dp.clone(),
            Arc::new(bot),
            df_long.clone(),
            &pre_trade_data_long,
            &trade_data_long,
            // &sl_long_2,
            // &tp_long_2,
        );
        // Test for equality in stop_loss
        assert_eq!(
            target_long_2.stop_loss.condition,
            result.stop_loss.condition
        );
        assert!(matches!(
            result.stop_loss.entry_time_stamp,
            Some(1646028000000_i64)
        ));
        assert_eq!(
            (target_long_2.stop_loss.profit.unwrap().floor()),
            result.stop_loss.profit.unwrap().floor()
        );

        // Test for equality in take_profit
        assert_eq!(
            target_long_2.take_profit.condition,
            result.take_profit.condition
        );
        assert_eq!(
            target_long_2.take_profit.triggered,
            result.take_profit.triggered
        );
        assert!(matches!(
            result.take_profit.entry_time_stamp,
            Some(1646056800000_i64)
        ));
        assert_eq!(
            target_long_2.take_profit.profit.unwrap().floor(),
            result.take_profit.profit.unwrap().floor()
        );
        assert!(matches!(result.take_profit.timeout, None));
        // END: Case 1)

        // BEGIN: Case 3)
        let sl_long_3 = StopLoss {
            condition: StopLossKind::PrevPoc,
            offset: 648.45, // not triggered => Timeout
        };

        let tp_long_3 = TakeProfit {
            condition: TakeProfitKind::PrevClose,
            offset: 100.0, // triggered
        };

        let target_long_3 = ProfitAndLoss {
            stop_loss: StopLossTrade {
                condition: 37_451.55,
                entry_time_stamp: None,
                profit: None,
            },
            take_profit: TakeProfitTrade {
                condition: 39_524.14,
                triggered: true,
                entry_time_stamp: Some(1646056800000_i64),
                profit: Some(1_424.14),
                timeout: None,
            },
        };

        let mut bot = Ppp::new();
        bot.set_stop_loss(sl_long_3);
        bot.set_take_profit(tp_long_3);

        result = compute_profit_and_loss(
            dp.clone(),
            Arc::new(bot),
            df_long.clone(),
            &pre_trade_data_long,
            &trade_data_long,
            // &sl_long_3,
            // &tp_long_3,
        );
        // Test for equality in stop_loss
        assert_eq!(
            target_long_3.stop_loss.condition,
            result.stop_loss.condition
        );
        assert!(matches!(result.stop_loss.entry_time_stamp, None));
        assert!(matches!(result.stop_loss.profit, None));

        // Test for equality in take_profit
        assert_eq!(
            target_long_3.take_profit.condition,
            result.take_profit.condition
        );
        assert_eq!(
            target_long_3.take_profit.triggered,
            result.take_profit.triggered
        );
        assert!(matches!(
            result.take_profit.entry_time_stamp,
            Some(1646056800000_i64)
        ));
        assert_eq!(
            target_long_3.take_profit.profit.unwrap().floor(),
            result.take_profit.profit.unwrap().floor()
        );
        assert!(matches!(result.take_profit.timeout, None));
        // END: Case 3)

        // BEGIN: Case 4)
        let sl_long_4 = StopLoss {
            condition: StopLossKind::PrevLow,
            offset: 0.0, // not triggered => Timeout
        };

        let tp_long_4 = TakeProfit {
            condition: TakeProfitKind::PrevClose,
            offset: 10_000.0, // Timeout
        };

        let target_long_4 = ProfitAndLoss {
            stop_loss: StopLossTrade {
                condition: 36_220.54,
                entry_time_stamp: None,
                profit: None,
            },
            take_profit: TakeProfitTrade {
                condition: 49_424.14,
                triggered: false,
                entry_time_stamp: None,
                profit: None,
                timeout: Some(Timeout {
                    condition: 43_160.0,
                    profit: 5060.0,
                }),
            },
        };

        let mut bot = Ppp::new();
        bot.set_stop_loss(sl_long_4);
        bot.set_take_profit(tp_long_4);

        result = compute_profit_and_loss(
            dp.clone(),
            Arc::new(bot),
            df_long.clone(),
            &pre_trade_data_long,
            &trade_data_long,
            // &sl_long_4,
            // &tp_long_4,
        );
        // Test for equality in stop_loss
        assert_eq!(
            target_long_4.stop_loss.condition,
            result.stop_loss.condition
        );
        assert!(matches!(result.stop_loss.entry_time_stamp, None));
        assert!(matches!(result.stop_loss.profit, None));

        // Test for equality in take_profit
        assert_eq!(
            target_long_4.take_profit.condition,
            result.take_profit.condition
        );
        assert_eq!(
            target_long_4.take_profit.triggered,
            result.take_profit.triggered
        );
        assert!(matches!(result.take_profit.entry_time_stamp, None));
        assert!(matches!(result.take_profit.profit, None));

        // Test for timout in take_prift
        assert_eq!(
            target_long_4
                .take_profit
                .timeout
                .as_ref()
                .unwrap()
                .condition,
            result.take_profit.timeout.as_ref().unwrap().condition
        );
        assert_eq!(
            target_long_4.take_profit.timeout.unwrap().profit.floor(),
            result.take_profit.timeout.unwrap().profit.floor()
        );
        // END: Case 4)

        // BEGIN: Case 2)
        // Does not change if we use PrevHigh (we fix the code to make it more intutive). See TODO above
        let sl_long_5 = StopLoss {
            condition: StopLossKind::PrevHigh,
            offset: -1_231.02, // triggered
        };

        let tp_long_5 = TakeProfit {
            condition: TakeProfitKind::PrevClose,
            offset: 10_000.0, // Timeout
        };

        let target_long_5 = ProfitAndLoss {
            stop_loss: StopLossTrade {
                condition: 37_451.56,
                entry_time_stamp: Some(1646028000000_i64),
                profit: Some(-648.44),
            },
            take_profit: TakeProfitTrade {
                condition: 49_424.14,
                triggered: false,
                entry_time_stamp: None,
                profit: None,
                timeout: Some(Timeout {
                    condition: 43_160.0,
                    profit: 5060.0,
                }),
            },
        };

        let mut bot = Ppp::new();
        bot.set_stop_loss(sl_long_5);
        bot.set_take_profit(tp_long_5);

        result = compute_profit_and_loss(
            dp.clone(),
            Arc::new(bot),
            df_long.clone(),
            &pre_trade_data_long,
            &trade_data_long,
            // &sl_long_5,
            // &tp_long_5,
        );
        // Test for equality in stop_loss
        assert_eq!(
            target_long_5.stop_loss.condition,
            result.stop_loss.condition
        );
        assert!(matches!(
            result.stop_loss.entry_time_stamp,
            Some(1646028000000_i64)
        ));
        assert_eq!(
            target_long_5.stop_loss.profit.unwrap().floor(),
            result.stop_loss.profit.unwrap().floor()
        );

        // Test for equality in take_profit
        assert_eq!(
            target_long_5.take_profit.condition,
            result.take_profit.condition
        );
        assert_eq!(
            target_long_5.take_profit.triggered,
            result.take_profit.triggered
        );
        assert!(matches!(result.take_profit.entry_time_stamp, None));
        assert!(matches!(result.take_profit.profit, None));
        // Test for timout in take_prift
        assert_eq!(
            target_long_5
                .take_profit
                .timeout
                .as_ref()
                .unwrap()
                .condition,
            result.take_profit.timeout.as_ref().unwrap().condition
        );
        assert_eq!(
            target_long_5.take_profit.timeout.unwrap().profit.floor(),
            result.take_profit.timeout.unwrap().profit.floor()
        );
        // END: Case 2)

        // TODO -> BEGIN: Edge Case
        // let sl_long_6 = StopLoss {
        //     condition: StopLossKind::PrevHigh,
        //     offset: -10_000.00, // TODO SL darf nicht über POC liegen
        // };

        // let tp_long_6 = TakeProfit {
        //     condition: TakeProfitKind::PrevClose,
        //     offset: -10_000.0, // TODO TP unterhalb POC
        // };

        // let target_long_6 = ProfitAndLoss {
        //     stop_loss: StopLossTrade {
        //         condition: 0.0,
        //         entry_time_stamp: None,
        //         profit: None,
        //     },
        //     take_profit: TakeProfitTrade {
        //         condition: 0.0,
        //         triggered: false,
        //         entry_time_stamp: None,
        //         profit: None,
        //         timeout: None,
        //     },
        // };
        // END: Edge Case
        // ################ LONG #################

        // ---------------------------------------
        // #######################################
        // ---------------------------------------

        // ################ SHORT ################
        // BEGIN: Trade Data Short

        pre_trade_data_map = EnumMap::default();
        pre_trade_data_map[PreTradeDataKind::Poc] = 42_100.0;
        pre_trade_data_map[PreTradeDataKind::LastTradePrice] = 39_004.73;
        pre_trade_data_map[PreTradeDataKind::LowestTradePrice] = 38_550.0;
        pre_trade_data_map[PreTradeDataKind::HighestTradePrice] = 44_101.12;

        trade_data_map = EnumMap::default();
        trade_data_map[TradeDataKind::EntryTimestamp] = AnyValue::Int64(1646812800000_i64);
        trade_data_map[TradeDataKind::LastTradePrice] = AnyValue::Float64(39_385.01);
        trade_data_map[TradeDataKind::LowestTradePriceSinceEntry] = AnyValue::Float64(38_848.48);
        trade_data_map[TradeDataKind::HighestTradePriceSinceEntry] = AnyValue::Float64(42_594.06);
        trade_data_map[TradeDataKind::LowestTradePriceSinceEntryTimestamp] =
            AnyValue::Int64(1646888400000_i64);
        trade_data_map[TradeDataKind::HighestTradePriceSinceEntryTimestamp] =
            AnyValue::Int64(1646838000000_i64);

        let pre_trade_data_short = pre_trade_data_map.clone();
        let trade_data_short = trade_data_map.clone();

        in_dir = std::path::PathBuf::from("data/test/other/ppp");
        file = in_dir.join("10_short_curr.csv");

        // Load test data
        let df_short = df_from_file( &file, None, None)
            .await
            .unwrap();
        // END: Trade Data Short

        // BEGIN: Case 1)
        let sl_short_1 = StopLoss {
            condition: StopLossKind::PrevPoc,
            offset: 0.0, // triggered
        };

        let tp_short_1 = TakeProfit {
            condition: TakeProfitKind::PrevClose,
            offset: 0.0, // triggered
        };

        let target_short_1 = ProfitAndLoss {
            stop_loss: StopLossTrade {
                condition: 42_100.0,
                entry_time_stamp: Some(1646812800000_i64),
                profit: Some(0.0),
            },
            take_profit: TakeProfitTrade {
                condition: 39_004.73,
                triggered: true,
                entry_time_stamp: Some(1646888400000_i64),
                profit: Some(3_095.27),
                timeout: None,
            },
        };

        let mut bot = Ppp::new();
        bot.set_stop_loss(sl_short_1);
        bot.set_take_profit(tp_short_1);

        result = compute_profit_and_loss(
            dp.clone(),
            Arc::new(bot),
            df_short.clone(),
            &pre_trade_data_short,
            &trade_data_short,
            // &sl_short_1,
            // &tp_short_1,
        );
        // Test for equality in stop_loss
        assert_eq!(
            target_short_1.stop_loss.condition,
            result.stop_loss.condition
        );
        assert!(matches!(
            result.stop_loss.entry_time_stamp,
            Some(1646812800000_i64)
        ));
        assert_eq!(
            target_short_1.stop_loss.profit.unwrap().floor(),
            result.stop_loss.profit.unwrap().floor()
        );

        // Test for equality in take_profit
        assert_eq!(
            target_short_1.take_profit.condition,
            result.take_profit.condition
        );
        assert_eq!(
            target_short_1.take_profit.triggered,
            result.take_profit.triggered
        );
        assert!(matches!(
            result.take_profit.entry_time_stamp,
            Some(1646888400000_i64)
        ));
        assert_eq!(
            target_short_1.take_profit.profit.unwrap().floor(),
            result.take_profit.profit.unwrap().floor()
        );
        assert!(matches!(result.take_profit.timeout, None));
        // END: Case 1)

        // BEGIN: Case 1)
        let sl_short_2 = StopLoss {
            condition: StopLossKind::PrevPoc,
            offset: 494.06, // triggered
        };

        let tp_short_2 = TakeProfit {
            condition: TakeProfitKind::PrevClose,
            offset: -100.0, // triggered
        };

        let target_short_2 = ProfitAndLoss {
            stop_loss: StopLossTrade {
                condition: 42_594.06,
                entry_time_stamp: Some(1646838000000_i64),
                profit: Some(-494.06),
            },
            take_profit: TakeProfitTrade {
                condition: 39_104.73,
                triggered: true,
                entry_time_stamp: Some(1646888400000_i64),
                profit: Some(2_995.27),
                timeout: None,
            },
        };

        let mut bot = Ppp::new();
        bot.set_stop_loss(sl_short_2);
        bot.set_take_profit(tp_short_2);

        result = compute_profit_and_loss(
            dp.clone(),
            Arc::new(bot),
            df_short.clone(),
            &pre_trade_data_short,
            &trade_data_short,
            // &sl_short_2,
            // &tp_short_2,
        );
        // Test for equality in stop_loss
        assert_eq!(
            target_short_2.stop_loss.condition,
            result.stop_loss.condition
        );
        assert!(matches!(
            result.stop_loss.entry_time_stamp,
            Some(1646838000000_i64)
        ));
        assert_eq!(
            target_short_2.stop_loss.profit.unwrap().floor(),
            result.stop_loss.profit.unwrap().floor()
        );

        // Test for equality in take_profit
        assert_eq!(
            target_short_2.take_profit.condition,
            result.take_profit.condition
        );
        assert_eq!(
            target_short_2.take_profit.triggered,
            result.take_profit.triggered
        );
        assert!(matches!(
            result.take_profit.entry_time_stamp,
            Some(1646888400000_i64)
        ));
        assert_eq!(
            target_short_2.take_profit.profit.unwrap().floor(),
            result.take_profit.profit.unwrap().floor()
        );
        assert!(matches!(result.take_profit.timeout, None));
        // END: Case 1)

        // BEGIN: Case 4)
        let sl_short_3 = StopLoss {
            condition: StopLossKind::PrevPoc,
            offset: 494.07, // not triggered => Timeout
        };

        let tp_short_3 = TakeProfit {
            condition: TakeProfitKind::PrevClose,
            offset: 156.26, // not triggered => Timeout
        };

        let target_short_3 = ProfitAndLoss {
            stop_loss: StopLossTrade {
                condition: 42_594.07,
                entry_time_stamp: None,
                profit: None,
            },
            take_profit: TakeProfitTrade {
                condition: 38_848.47,
                triggered: false,
                entry_time_stamp: None,
                profit: None,
                timeout: Some(Timeout {
                    condition: 39_385.01,
                    profit: 2714.09,
                }),
            },
        };

        let mut bot = Ppp::new();
        bot.set_stop_loss(sl_short_3);
        bot.set_take_profit(tp_short_3);

        result = compute_profit_and_loss(
            dp.clone(),
            Arc::new(bot),
            df_short.clone(),
            &pre_trade_data_short,
            &trade_data_short,
            // &sl_short_3,
            // &tp_short_3,
        );
        // Test for equality in stop_loss
        assert_eq!(
            target_short_3.stop_loss.condition,
            result.stop_loss.condition
        );
        assert!(matches!(result.stop_loss.entry_time_stamp, None));
        assert!(matches!(result.stop_loss.profit, None));

        // Test for equality in take_profit
        assert_eq!(
            target_short_3.take_profit.condition,
            result.take_profit.condition
        );
        assert_eq!(
            target_short_3.take_profit.triggered,
            result.take_profit.triggered
        );
        assert!(matches!(result.take_profit.entry_time_stamp, None));
        assert!(matches!(result.take_profit.profit, None));

        // Test for timout in take_prift
        assert_eq!(
            target_short_3
                .take_profit
                .timeout
                .as_ref()
                .unwrap()
                .condition,
            result.take_profit.timeout.as_ref().unwrap().condition
        );
        assert_eq!(
            target_short_3.take_profit.timeout.unwrap().profit.floor(),
            result.take_profit.timeout.unwrap().profit.floor()
        );
        // END: Case 4)

        // BEGIN: Case 3)
        // Does not change if we use PrevHigh (we fix the code to make it more intutive). See TODO above
        let sl_short_4 = StopLoss {
            condition: StopLossKind::PrevLow,
            offset: 500.0, // not triggered => Timeout
        };

        let tp_short_4 = TakeProfit {
            condition: TakeProfitKind::PrevClose,
            offset: -100.0, // triggered
        };

        let target_short_4 = ProfitAndLoss {
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

        let mut bot = Ppp::new();
        bot.set_stop_loss(sl_short_4);
        bot.set_take_profit(tp_short_4);

        result = compute_profit_and_loss(
            dp.clone(),
            Arc::new(bot),
            df_short.clone(),
            &pre_trade_data_short,
            &trade_data_short,
            // &sl_short_4,
            // &tp_short_4,
        );
        // Test for equality in stop_loss
        assert_eq!(
            target_short_4.stop_loss.condition,
            result.stop_loss.condition
        );
        assert!(matches!(result.stop_loss.entry_time_stamp, None));
        assert!(matches!(result.stop_loss.profit, None));

        // Test for equality in take_profit
        assert_eq!(
            target_short_4.take_profit.condition,
            result.take_profit.condition
        );
        assert_eq!(
            target_short_4.take_profit.triggered,
            result.take_profit.triggered
        );
        assert!(matches!(
            result.take_profit.entry_time_stamp,
            Some(1646888400000_i64)
        ));
        assert_eq!(
            target_short_4.take_profit.profit.unwrap().floor(),
            result.take_profit.profit.unwrap().floor()
        );
        assert!(matches!(result.take_profit.timeout, None));
        // END: Case 3)

        // BEGIN: Case 2)
        let sl_short_5 = StopLoss {
            condition: StopLossKind::PrevHigh,
            offset: -1_507.06, // triggered
        };

        let tp_short_5 = TakeProfit {
            condition: TakeProfitKind::PrevClose,
            offset: 10_000.0, // Timeout
        };

        let target_short_5 = ProfitAndLoss {
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

        let mut bot = Ppp::new();
        bot.set_stop_loss(sl_short_5);
        bot.set_take_profit(tp_short_5);

        result = compute_profit_and_loss(
            dp.clone(),
            Arc::new(bot),
            df_short.clone(),
            &pre_trade_data_short,
            &trade_data_short,
            // &sl_short_5,
            // &tp_short_5,
        );
        
        // Test for equality in stop_loss
        assert_eq!(
            target_short_5.stop_loss.condition.floor(),
            result.stop_loss.condition.floor()
        );
        // TODO aufgrund des Rundungsfehlres wird kein trade ausgelöst 42594.060000000005 > 42594.06
        // assert!(matches!(
        //     result.stop_loss.entry_time_stamp,
        //     Some(1646838000000_i64)
        // ));
        // assert_eq!(
        //     target_short_5.stop_loss.profit.unwrap().floor(),
        //     result.stop_loss.profit.unwrap().floor()
        // );

        // Test for equality in take_profit
        // TODO warum hier .floor() und beim long trade nicht?
        assert_eq!(
            target_short_5.take_profit.condition.floor(),
            result.take_profit.condition.floor()
        );
        assert_eq!(
            target_short_5.take_profit.triggered,
            result.take_profit.triggered
        );
        assert!(matches!(result.take_profit.entry_time_stamp, None));
        assert!(matches!(result.take_profit.profit, None));

        // Test for timout in take_prift
        assert_eq!(
            target_short_5
                .take_profit
                .timeout
                .as_ref()
                .unwrap()
                .condition,
            result.take_profit.timeout.as_ref().unwrap().condition
        );
        assert_eq!(
            target_short_5.take_profit.timeout.unwrap().profit.floor(),
            result.take_profit.timeout.unwrap().profit.floor()
        );
        // END: Case 2)

        // TODO BEGIN: Edge Case
        // let sl_short_6 = StopLoss {
        //     condition: StopLossKind::PrevHigh,
        //     offset: -10_000.00, // TODO SL darf nicht unter POC liegen
        // };

        // let tp_short_6 = TakeProfit {
        //     condition: TakeProfitKind::PrevClose,
        //     offset: -10_000.0, // TODO TP oberhalb POC
        // };

        // let target_short_6 = ProfitAndLoss {
        //     stop_loss: StopLossTrade {
        //         condition: 0.0,
        //         entry_time_stamp: None,
        //         profit: None,
        //     },
        //     take_profit: TakeProfitTrade {
        //         condition: 0.0,
        //         triggered: false,
        //         entry_time_stamp: None,
        //         profit: None,
        //         timeout: None,
        //     },
        // };
        // END: Edge Case

        // ################ SHORT ################
    }
}
