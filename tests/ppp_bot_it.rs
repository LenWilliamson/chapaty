mod test_configurations;
use chapaty::{
    config::{self},
    data_provider::cme::Cme,
    strategy::{ppp::PppBuilder, StopLoss, Strategy, TakeProfit},
    MarketKind, MarketSimulationDataKind, PriceHistogramKind, StopLossKind, TakeProfitKind,
    TimeFrameKind, TimeInterval, TradingIndicatorKind,
};
use std::{sync::Arc, time::Instant};
use test_configurations::{
    bot_config::BotConfig,
    get_expected_result,
    test_runner::{self, TestRunner},
};

#[ignore]
#[tokio::test]
async fn ppp_bot_strategy_1_it() {
    let start = Instant::now();
    let expected_result = "";

    ppp_bot_it(setup_strategy_1(), &expected_result).await;

    let duration = start.elapsed();
    println!("Time elapsed is: {duration:?} for ppp_bot_strategy_1_it().");
}

#[ignore]
#[tokio::test]
async fn ppp_bot_strategy_2_it() {
    let start = Instant::now();
    let expected_result = "";

    ppp_bot_it(setup_strategy_2(), &expected_result).await;

    let duration = start.elapsed();
    println!("Time elapsed is: {duration:?} for ppp_bot_strategy_2_it().");
}

async fn ppp_bot_it(strategy: Arc<dyn Strategy + Send + Sync>, expected_result: &str) {
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
        year: 2022,
        time_interval: Some(setup_time_interval()),
        time_frame: TimeFrameKind::Daily,
    };

    let tr = TestRunner::new(bot_config);
    let bot = tr.setup().unwrap();
    let test_result = tr.run(bot).await;
    test_runner::assert(test_result, get_expected_result(expected_result));
}

pub fn setup_strategy_1() -> Arc<dyn Strategy + Send + Sync> {
    let ppp_builder = PppBuilder::new();
    let sl = StopLoss {
        kind: StopLossKind::PrevHighOrLow,
        offset: 0.0,
    };
    let tp = TakeProfit {
        kind: TakeProfitKind::PrevClose,
        offset: 0.0,
    };

    let strategy = ppp_builder
        .with_stop_loss(sl)
        .with_take_profit(tp)
        .with_entry(TradingIndicatorKind::Poc(PriceHistogramKind::Tpo1m))
        .build();
    Arc::new(strategy)
}

pub fn setup_strategy_2() -> Arc<dyn Strategy + Send + Sync> {
    let ppp_builder = PppBuilder::new();
    let sl = StopLoss {
        kind: StopLossKind::PrevHighOrLow,
        offset: 125_000.0,
    };
    let tp = TakeProfit {
        kind: TakeProfitKind::PrevClose,
        offset: 125.0,
    };

    let strategy = ppp_builder
        .with_stop_loss(sl)
        .with_take_profit(tp)
        .with_entry(TradingIndicatorKind::Poc(PriceHistogramKind::Tpo1m))
        .with_market_simulation_data_kind(MarketSimulationDataKind::Ohlc1m)
        .build();
    Arc::new(strategy)
}

fn setup_time_interval() -> TimeInterval {
    TimeInterval {
        start_day: chrono::Weekday::Mon,
        start_h: 1,
        end_day: chrono::Weekday::Fri,
        end_h: 23,
    }
}
