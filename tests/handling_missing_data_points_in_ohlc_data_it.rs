mod test_configurations;

use chapaty::{
    config::{self},
    data_provider::cme::Cme,
    strategy::{news_counter::NewsCounterBuilder, Strategy, TakeProfit},
    MarketKind, MarketSimulationDataKind, NewsKind, StopLossKind, TakeProfitKind, TimeFrameKind,
};

use std::{sync::Arc, time::Instant};
use test_configurations::{
    bot_config::BotConfig,
    get_expected_result,
    test_runner::{self, TestRunner},
};

/// Integration test for handling missing OHLC data in backtesting.
/// 
/// This test is directly related to [Design Decision 2: Handling Missing Data Points in OHLC Data].
/// During backtesting, missing OHLC data points led to `None` values in `RequiredPreTradeValuesWithData`,
/// causing panics during the runtime. This update introduces a fallback mechanism in the `news_candle` function,
/// which searches for the most recent valid data when the expected data is missing, ensuring continuity 
/// and preventing crashes.
/// 
/// ### Test Data
/// 
/// The test uses historical market data for the `6E JUN24` contract, provided via the 
/// NinjaTrader CME Marketdata Level 1.
#[tokio::test]
async fn handling_missing_data_points_in_ohlc_data_it() {
    let start = Instant::now();
    let bucket = config::GoogleCloudBucket {
        historical_market_data_bucket_name: "chapaty-ai-hdb-test".to_string(),
        cached_bot_data_bucket_name: "chapaty-ai-test".to_string(),
    };
    let bot_config = BotConfig {
        client: config::get_google_cloud_storage_client().await,
        bucket,
        strategy: setup_strategy(),
        data_provider: Arc::new(Cme),
        market: MarketKind::EurUsdFuture,
        year: 2015,
        market_simulation_data: MarketSimulationDataKind::Ohlc1m,
        time_interval: None,
        time_frame: TimeFrameKind::Daily,
    };

    let tr = TestRunner::new(bot_config);
    let bot = tr.setup().unwrap();
    let test_result = tr.run(bot).await;
    let file_name = "tests/expected_results/expected_handling_missing_data_points_in_ohlc_data.csv";
    test_runner::assert(test_result, get_expected_result(&file_name));

    let duration = start.elapsed();
    println!("Time elapsed is: {duration:?} for handling_missing_data_points_in_ohlc_data_it().");
}

fn setup_strategy() -> Arc<dyn Strategy + Send + Sync> {
    let news_builder = NewsCounterBuilder::new();
    let tp = TakeProfit {
        kind: TakeProfitKind::PriceUponTradeEntry,
        offset: 0.9,
    };

    let strategy = news_builder
        .with_stop_loss_kind(StopLossKind::PriceUponTradeEntry)
        .with_take_profit(tp)
        .with_news_kind(NewsKind::UsaNFP)
        .with_number_candles_to_wait(5)
        .with_loss_to_win_ratio(2.0)
        .build();

    Arc::new(strategy)
}