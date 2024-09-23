pub mod test_configurations;

use chapaty::{
    config::{self},
    data_provider::cme::Cme,
    strategy::{
        news_counter::NewsCounterBuilder, news_rassler::NewsRasslerBuilder,
        news_rassler_with_confirmation::NewsRasslerWithConfirmationBuilder, StopLoss, Strategy,
        TakeProfit,
    },
    BotBuilder, ExecutionData, MarketKind, MarketSimulationDataKind, NewsKind, StopLossKind,
    TakeProfitKind, TimeFrameKind,
};

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Instant,
};

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

    let strategy = setup_news_rassler_with_confirmation_strategy();
    // let strategy = setup_news_counter_strategy();
    let data_provider = Arc::new(Cme);
    // let years = vec![2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024];
    let years = (2006..=2024).collect();

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
        .with_cache_computations(true)
        .build()
        .unwrap();

    let (_, session_cache) = bot.backtest().await;

    let duration = start.elapsed();
    println!("Time elapsed is: {duration:?}");
    // backtest_with_session_cache(session_cache).await;

    assert_eq!(0, 0);
}

async fn backtest_with_session_cache(
    session_cache: Arc<Mutex<HashMap<MarketKind, HashMap<u32, ExecutionData>>>>,
) {
    let start = Instant::now();

    let strategy = setup_news_counter_strategy();
    let data_provider = Arc::new(Cme);
    // let years = vec![2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024];
    let years = (2006..=2024).collect();

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
    // let client = config::get_google_cloud_storage_client().await;
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
        // .with_google_cloud_storage_client(client)
        .with_google_cloud_bucket(bucket)
        .with_save_result_as_csv(true)
        .with_session_cache_computations(session_cache)
        .with_cache_computations(false)
        .build()
        .unwrap();

    let _ = bot.backtest().await;

    let duration = start.elapsed();
    println!("Time elapsed with cached execution data is: {duration:?}");

    assert_eq!(0, 0);
}

fn setup_news_counter_strategy() -> Arc<dyn Strategy + Send + Sync> {
    let news_builder = NewsCounterBuilder::new();
    let tp = TakeProfit {
        kind: TakeProfitKind::PriceUponTradeEntry,
        offset: 1.45,
    };

    let strategy = news_builder
        .with_stop_loss_kind(StopLossKind::PriceUponTradeEntry)
        .with_take_profit(tp)
        .with_news_kind(NewsKind::UsaCPI)
        .with_number_candles_to_wait(11)
        .with_loss_to_win_ratio(1.05)
        .build();

    Arc::new(strategy)
}

fn setup_news_rassler_strategy() -> Arc<dyn Strategy + Send + Sync> {
    let news_builder = NewsRasslerBuilder::new();
    let sl = StopLoss {
        kind: StopLossKind::PriceUponTradeEntry,
        offset: 0.3,
    };

    let strategy = news_builder
        .with_take_profit_kind(TakeProfitKind::PriceUponTradeEntry)
        .with_stop_loss(sl)
        .with_news_kind(NewsKind::UsaCPI)
        .with_number_candles_to_wait(3)
        .with_loss_to_win_ratio(1.0)
        .build();

    Arc::new(strategy)
}

fn setup_news_rassler_with_confirmation_strategy() -> Arc<dyn Strategy + Send + Sync> {
    let news_builder = NewsRasslerWithConfirmationBuilder::new();
    let sl = StopLoss {
        kind: StopLossKind::PriceUponTradeEntry,
        offset: 1.3,
    };

    let strategy = news_builder
        .with_take_profit_kind(TakeProfitKind::PriceUponTradeEntry)
        .with_stop_loss(sl)
        .with_news_kind(NewsKind::UsaCPI)
        .with_number_candles_to_wait(11)
        .with_loss_to_win_ratio(2.9)
        .build();

    Arc::new(strategy)
}

/*
Counter                 NFP 1m-Chart 2006 - 20020: number_candles_to_wait = 8, loss_to_win_ratio = 2.8, offset = 1.25, total_profit = 12125
Counter                 CPI 1m-Chart 2006 - 20020: number_candles_to_wait = 11, loss_to_win_ratio = 1.05, offset = 1.45, total_profit = 8593.75

Rassler                 NFP 1m-Chart 2006 - 20020: number_candles_to_wait = 3, loss_to_win_ratio = 0.65, offset = 0.55, total_profit = 9012.5
Rassler                 CPI 1m-Chart 2006 - 20020: number_candles_to_wait = 10, loss_to_win_ratio = 0.6, offset = 0.5, total_profit = 5093.75

Rassler Confirmation    NFP 5m-Chart 2006 - 20020: number_candles_to_wait = 10, loss_to_win_ratio = 1, offset = 1.3, total_profit = 9800, treffer_quote = 0.5982905982905983
Rassler Confirmation    CPI 5m-Chart 2006 - 20020: number_candles_to_wait = 11, loss_to_win_ratio = 2.9, offset = 1.3, total_profit = 13093.75, treffer_quote = 0.589041095890411
*/
