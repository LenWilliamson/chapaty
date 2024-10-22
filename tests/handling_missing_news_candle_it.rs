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

/// Integration test for managing missing OHLC data during backtesting.
///
/// This test addresses [Design Decision 4: Handling Missing News Candle in OHLC Data].
/// During backtesting, missing news candles in the OHLC data result in `None` values
/// within `RequiredPreTradeValuesWithData`, leading to runtime panics when attempting to
/// compute the entry timestamp. To resolve this, the `Strategy` trait was updated to return
/// an additional boolean flag alongside the entry timestamp, indicating whether the computation
/// should proceed.
///
/// ```rust
/// pub trait Strategy {
///     fn get_entry_ts(&self, pre_trade_values: &RequiredPreTradeValuesWithData) -> (Option<i64>, bool);
///     // Additional trait definitions...
/// }
/// ```
///
/// Additionally, the type of the `market_values` field in `RequiredPreTradeValuesWithData`
/// was modified to enhance robustness. The new fallback mechanisms ensure that missing news
/// candles are gracefully skipped, allowing the backtesting process to continue smoothly
/// without triggering crashes.
///
/// ### Test Data
///
/// The test uses historical market data for the 6E JUN24 contract, provided via the
/// NinjaTrader CME Marketdata Level 1.
#[tokio::test]
async fn handling_missing_news_candle_it() {
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
        year: 2009,
        time_interval: None,
        time_frame: TimeFrameKind::Daily,
    };

    let tr = TestRunner::new(bot_config);
    let bot = tr.setup().unwrap();
    let test_result = tr.run(bot).await;
    let file_name = "tests/expected_results/expected_handling_missing_news_candle.csv";
    test_runner::assert(test_result, get_expected_result(&file_name));

    let duration = start.elapsed();
    println!("Time elapsed is: {duration:?} for handling_missing_news_candle_it().");
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
        .with_market_simulation_data_kind(MarketSimulationDataKind::Ohlc1m)
        .with_number_candles_to_wait(5)
        .with_loss_to_win_ratio(2.0)
        .build();

    Arc::new(strategy)
}
