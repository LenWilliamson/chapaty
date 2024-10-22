pub mod test_configurations;

use chapaty::{
    config::{self},
    data_provider::cme::Cme,
    strategy::{news_rassler_conf::NewsRasslerConfBuilder, StopLoss, Strategy},
    MarketKind, MarketSimulationDataKind, NewsKind, StopLossKind, TakeProfitKind, TimeFrameKind,
};
use std::{sync::Arc, time::Instant};
use test_configurations::{
    bot_config::BotConfig,
    get_expected_result,
    test_runner::{self, TestRunner},
};

#[ignore]
#[tokio::test]
async fn news_rassler_with_confirmation_bot_strategy_1_it() {
    let start = Instant::now();
    let expected_result = "";

    news_rassler_with_confirmation_bot_it(setup_strategy(0.3), &expected_result).await;

    let duration = start.elapsed();
    println!(
        "Time elapsed is: {duration:?} for news_rassler_with_confirmation_bot_strategy_1_it()."
    );
}

#[ignore]
#[tokio::test]
async fn news_rassler_with_confirmation_bot_strategy_2_it() {
    let start = Instant::now();
    let expected_result = "";

    news_rassler_with_confirmation_bot_it(setup_strategy(0.3), &expected_result).await;

    let duration = start.elapsed();
    println!(
        "Time elapsed is: {duration:?} for news_rassler_with_confirmation_bot_strategy_2_it()."
    );
}

async fn news_rassler_with_confirmation_bot_it(
    strategy: Arc<dyn Strategy + Send + Sync>,
    expected_result: &str,
) {
    let bucket = config::GoogleCloudBucket {
        historical_market_data_bucket_name: "chapaty-ai-hdb-int".to_string(),
        cached_bot_data_bucket_name: "chapaty-ai-int".to_string(),
    };
    let bot_config = BotConfig {
        client: config::get_google_cloud_storage_client().await,
        bucket,
        strategy,
        data_provider: Arc::new(Cme),
        market: MarketKind::EurUsdFuture,
        year: 2021,
        time_interval: None,
        time_frame: TimeFrameKind::Daily,
    };

    let tr = TestRunner::new(bot_config);
    let bot = tr.setup().unwrap();
    let test_result = tr.run(bot).await;
    test_runner::assert(test_result, get_expected_result(expected_result));
}

fn setup_strategy(offset: f64) -> Arc<dyn Strategy + Send + Sync> {
    let news_builder = NewsRasslerConfBuilder::new();
    let sl = StopLoss {
        kind: StopLossKind::PriceUponTradeEntry,
        offset,
    };

    let strategy = news_builder
        .with_stop_loss(sl)
        .with_take_profit_kind(TakeProfitKind::PriceUponTradeEntry)
        .with_news_kind(NewsKind::UsaNFP)
        .with_market_simulation_data_kind(MarketSimulationDataKind::Ohlc5m)
        .with_number_candles_to_wait(5)
        .with_loss_to_win_ratio(2.0)
        .build();

    Arc::new(strategy)
}
