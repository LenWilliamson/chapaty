mod common;
use chapaty::{
    config::{self},
    BotBuilder, MarketKind, MarketSimulationDataKind, TimeFrameKind,
};
use std::time::Instant;

#[tokio::test]
async fn it_test() {
    let start = Instant::now();

    let strategy = common::setup_strategy();
    let data_provider = common::setup_data_provider();
    // let years = vec![2022, 2021, 2020, 2019, 2018, 2017];
    let years = vec![2022];
    let market_simulation_data = MarketSimulationDataKind::Ohlc1m;
    let markets = vec![
        // MarketKind::AudUsdFuture,
        MarketKind::EurUsdFuture,
        // MarketKind::GbpUsdFuture,
        // MarketKind::CadUsdFuture,
        // MarketKind::YenUsdFuture,
        // MarketKind::NzdUsdFuture,
        // MarketKind::BtcUsdFuture,
    ];
    let time_interval = common::setup_time_interval();
    let time_frame = TimeFrameKind::Daily;
    let client = config::get_google_cloud_storage_client().await;
    let bucket = config::GoogleCloudBucket {
        historical_market_data_bucket_name: "chapaty-ai-hdb-int".to_string(),
        cached_bot_data_bucket_name: "chapaty-ai-int".to_string(),
    };
    let bot = BotBuilder::new(strategy, data_provider)
        .with_years(years)
        .with_markets(markets)
        .with_market_simulation_data(market_simulation_data)
        .with_time_interval(time_interval)
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
