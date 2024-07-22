mod strategy_configurations;

use chapaty::{
    config::{self},
    BotBuilder, MarketKind, MarketSimulationDataKind, TimeFrameKind,
};
use polars::{io::SerReader, prelude::CsvReadOptions};
use std::time::Instant;

#[tokio::test]
async fn it_test() {
    let start = Instant::now();

    let strategy = strategy_configurations::setup_news_strategy();
    let data_provider = strategy_configurations::setup_data_provider();
    let years = vec![2024];
    let markets = vec![MarketKind::EurUsdFuture];
    let market_simulation_data = MarketSimulationDataKind::Ohlc1m;
    let time_frame = TimeFrameKind::Daily;
    let client = config::get_google_cloud_storage_client().await;
    let bucket = config::GoogleCloudBucket {
        historical_market_data_bucket_name: "chapaty-ai-hdb-test".to_string(),
        cached_bot_data_bucket_name: "chapaty-ai-test".to_string(),
    };

    let bot = BotBuilder::new(strategy, data_provider)
        .with_years(years.clone())
        .with_markets(markets.clone())
        .with_market_simulation_data(market_simulation_data)
        .with_time_frame(time_frame)
        .with_google_cloud_storage_client(client)
        .with_google_cloud_bucket(bucket)
        .with_save_result_as_csv(false)
        .with_cache_computations(false)
        .build()
        .unwrap();

    let res = bot.backtest().await;
    let pnl = res
        .market_and_year
        .pnl_statement
        .pnl_data
        .get(&markets[0])
        .unwrap()
        .reports
        .get(&years[0])
        .unwrap();

    let target = CsvReadOptions::default()
        .with_has_header(true)
        .try_into_reader_with_file_path(Some(
            "tests/expected_results/expecteted_chapaty_6e_2024_pnl.csv".into(),
        ))
        .unwrap()
        .finish();

    assert_eq!(target.unwrap().equals(&pnl), true);

    let duration = start.elapsed();
    println!("Time elapsed is: {duration:?}");
}
