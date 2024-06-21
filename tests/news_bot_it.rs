// TODO Fix Path finder so I can handle multiple files in Test Case. Currently this file https://storage.cloud.google.com/chapaty-ai-hdb-test/cme/ohlc/ohlc-1m-2024.csv
// Contains testdata for the News strategy

// TODO Fix Segfault when chapaty::bot::trading_session::TradingSession fn run_backtesting_daily(&self) -> DataFrame fails due to empty pnl_report_data_rows
mod common;
use chapaty::{
    config::{self},
    BotBuilder, MarketKind, MarketSimulationDataKind, TimeFrameKind,
};
use std::time::Instant;

#[tokio::test]
async fn it_test() {
    let start = Instant::now();

    // If news strategy, limit the possible range of ohlc time frames
    // If news are at 11:45 -> only 1m, 5m, 15m
    // If news are at 12:30 -> only 1m, 5m, 15m, 30m
    // If news are at 15:00 -> only 1m, 5m, 15m, 30m, 1h
    let strategy = common::setup_news_strategy();
    // let strategy = common::setup_ppp_strategy();
    let data_provider = common::setup_data_provider();
    // let years = vec![2022, 2021, 2020, 2019, 2018, 2017];
    let years = vec![2022];
    let markets = vec![
        // MarketKind::AudUsdFuture,
        // MarketKind::CadUsdFuture,
        // MarketKind::GbpUsdFuture,
        MarketKind::EurUsdFuture,
        // MarketKind::YenUsdFuture,
        // MarketKind::NzdUsdFuture,
        // MarketKind::BtcUsdFuture,
    ];
    let market_simulation_data = MarketSimulationDataKind::Ohlc5m;
    let time_interval = common::setup_time_interval();
    let time_frame = TimeFrameKind::Daily;
    let client = config::get_google_cloud_storage_client().await;
    let bucket = config::GoogleCloudBucket {
        historical_market_data_bucket_name: "chapaty-ai-hdb-test".to_string(),
        cached_bot_data_bucket_name: "chapaty-ai-test".to_string(),
    };
    let bot = BotBuilder::new(strategy, data_provider)
        .with_years(years)
        .with_markets(markets)
        .with_market_simulation_data(market_simulation_data)
        // .with_time_interval(time_interval)
        .with_time_frame(time_frame)
        .with_google_cloud_storage_client(client)
        .with_google_cloud_bucket(bucket)
        .with_save_result_as_csv(true)
        .with_cache_computations(false)
        .build()
        .unwrap();

    let _ = bot.backtest().await;

    let duration = start.elapsed();
    println!("Time elapsed is: {duration:?}");

    assert_eq!(0, 0);
}
