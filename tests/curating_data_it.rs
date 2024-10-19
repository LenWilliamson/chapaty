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

/// Integration test for verifying the precision curation logic in `chapaty`.
///
/// This test is directly related to [Design Decision 1: Curating Data]
/// in the project documentation. It validates the correctness of the rounding mechanism applied
/// to the symbols we can trade, ensuring that floating-point precision is
/// handled according to the tick sizes specified in the market contract specifications.
///
/// ### Test Data
///
/// The test uses historical market data for the `6E JUN24` contract, provided via the
/// NinjaTrader CME Marketdata Level 1.
#[tokio::test]
async fn curating_data_it() {
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
        year: 2024,
        market_simulation_data: MarketSimulationDataKind::Ohlc1m,
        time_interval: None,
        time_frame: TimeFrameKind::Daily,
    };

    let tr = TestRunner::new(bot_config);
    let bot = tr.setup().unwrap();
    let test_result = tr.run(bot).await;
    let file_name = "tests/expected_results/expecteted_curating_data.csv";
    test_runner::assert(test_result, get_expected_result(&file_name));

    let duration = start.elapsed();
    println!("Time elapsed is: {duration:?} for curating_data_it().");
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
