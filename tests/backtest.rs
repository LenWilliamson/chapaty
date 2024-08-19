pub mod test_configurations;

use chapaty::{
    config::{self},
    data_provider::cme::Cme,
    strategy::{news::NewsBuilder, StopLoss, Strategy, TakeProfit},
    BotBuilder, MarketKind, MarketSimulationDataKind, NewsKind, StopLossKind, TakeProfitKind,
    TimeFrameKind,
};
use std::{sync::Arc, time::Instant};

/// Example integration test for configuring and running a backtest using the `chapaty` API.
/// 
/// This test demonstrates how to set up a backtest with a custom strategy and market conditions 
/// using the `chapaty` library. It is intended as a template for quickly testing and configuring 
/// custom strategies.
/// 
/// Marked as `#[ignore]` since it's primarily for demonstration and quick validation of strategies.
#[ignore]
#[tokio::test]
async fn backtest() {
    let start = Instant::now();

    let strategy = setup_strategy();
    let data_provider = Arc::new(Cme);
    // let years = vec![2008, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024];
    let years = vec![2009];

    let markets = vec![
        // MarketKind::AudUsdFuture,
        // MarketKind::CadUsdFuture,
        // MarketKind::GbpUsdFuture,
        MarketKind::EurUsdFuture,
        // MarketKind::YenUsdFuture,
        // MarketKind::NzdUsdFuture,
        // MarketKind::BtcUsdFuture,
    ];
    let market_simulation_data = MarketSimulationDataKind::Ohlc1m;
    // let time_interval = strategy_configurations::setup_time_interval();
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

fn setup_strategy() -> Arc<dyn Strategy + Send + Sync> {
    let news_builder = NewsBuilder::new();
    let sl = StopLoss {
        kind: StopLossKind::PriceUponTradeEntry,
        offset: 0.3,
    };
    let tp = TakeProfit {
        kind: TakeProfitKind::PriceUponTradeEntry,
        offset: 0.9,
    };

    let strategy = news_builder
        .with_stop_loss(sl)
        .with_take_profit(tp)
        .with_news_kind(NewsKind::UsaNFP)
        .with_is_counter_trade(true)
        .with_number_candles_to_wait(5)
        .with_loss_to_win_ratio(2.0)
        .build();

    Arc::new(strategy)
}
