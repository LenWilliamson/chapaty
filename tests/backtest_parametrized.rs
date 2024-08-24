pub mod test_configurations;

use chapaty::{
    config,
    converter::any_value::AnyValueConverter,
    data_provider::cme::Cme,
    strategy::{news_counter::NewsCounterBuilder, Strategy, TakeProfit},
    BotBuilder, MarketKind, MarketSimulationDataKind, NewsKind, StopLossKind, TakeProfitKind,
    TimeFrameKind,
};
use std::{sync::Arc, time::Instant};

/// Example integration test for configuring and running a backtest using the `chapaty` API.
///
/// This test serves as a template for setting up and running a backtest with a custom, parameterized strategy
/// under specific market conditions using the `chapaty` library. It demonstrates how to configure various
/// strategy parameters and assess their performance by iterating over a range of values.
///
/// This approach, while straightforward, may not be the most efficient for finding the optimal strategy
/// parameters, as it involves a naive brute-force search over the parameter space. Future enhancements
/// will incorporate more sophisticated techniques, such as gradient descent, to more effectively
/// optimize strategy parameters.
///
/// This test is marked with `#[ignore]` because it is intended primarily for demonstration purposes
/// and quick validation of custom strategies, rather than as a routine test case.
#[ignore]
#[tokio::test]
async fn backtest_parametrized() {
    let start = Instant::now();

    let strategy_parameters: Vec<(i32, f64, f64)> = (3..=15)
        .flat_map(|number_candles_to_wait| {
            range(0.5, 3.0, 0.1, 10.0)
                .into_iter()
                .flat_map(move |loss_to_win_ratio| {
                    range(0.3, 1.5, 0.05, 100.0)
                        .into_iter()
                        .map(move |offset| (number_candles_to_wait, loss_to_win_ratio, offset))
                })
        })
        .collect();

    // let tasks: Vec<_> = strategy_parameters
    //     .into_iter()
    //     .map(|(number_candles_to_wait, loss_to_win_ratio, offset)| {
    //         tokio::spawn(async move {
    //             let data_provider = Arc::new(Cme);
    //             // let years = (2006..=2020).collect();
    //             let years = vec![2024];

    //             let markets = vec![
    //                 // MarketKind::AudUsdFuture,
    //                 // MarketKind::CadUsdFuture,
    //                 // MarketKind::GbpUsdFuture,
    //                 MarketKind::EurUsdFuture,
    //                 // MarketKind::YenUsdFuture,
    //                 // MarketKind::NzdUsdFuture,
    //                 // MarketKind::BtcUsdFuture,
    //             ];
    //             let market_simulation_data = MarketSimulationDataKind::Ohlc1m;
    //             // let time_interval = strategy_configurations::setup_time_interval();
    //             let time_frame = TimeFrameKind::Daily;
    //             let client = config::get_google_cloud_storage_client().await;
    //             let bucket = config::GoogleCloudBucket {
    //                 historical_market_data_bucket_name: "chapaty-ai-hdb-int".to_string(),
    //                 cached_bot_data_bucket_name: "chapaty-ai-int".to_string(),
    //             };

    //             let strategy =
    //                 setup_strategy_parametrized(offset, number_candles_to_wait, loss_to_win_ratio);
    //             let bot = BotBuilder::new(strategy, data_provider)
    //                 .with_years(years)
    //                 .with_markets(markets)
    //                 .with_market_simulation_data(market_simulation_data)
    //                 // .with_time_interval(time_interval)
    //                 .with_time_frame(time_frame)
    //                 .with_google_cloud_storage_client(client)
    //                 .with_google_cloud_bucket(bucket)
    //                 .with_save_result_as_csv(false)
    //                 .with_cache_computations(false)
    //                 .build()
    //                 .unwrap();
    //             (
    //                 number_candles_to_wait,
    //                 loss_to_win_ratio,
    //                 offset,
    //                 bot.backtest().await,
    //             )
    //         })
    //     })
    //     .collect();

    // let res = futures::future::join_all(tasks)
    //     .await
    //     .into_iter()
    //     .map(Result::unwrap)
    //     .map(
    //         |(number_candles_to_wait, loss_to_win_ratio, offset, result)| {
    //             let total_profit = result
    //                 .agg_market_and_agg_year
    //                 .performance_report
    //                 .get(0)
    //                 .unwrap()[4]
    //                 .unwrap_float64();
    //             (
    //                 number_candles_to_wait,
    //                 loss_to_win_ratio,
    //                 offset,
    //                 total_profit,
    //             )
    //         },
    //     )
    //     .collect::<Vec<_>>();

    let mut res = Vec::new();

    for (number_candles_to_wait, loss_to_win_ratio, offset) in strategy_parameters.iter() {
        let data_provider = Arc::new(Cme);
        let years: Vec<_> = (2006..=2020).collect();
        let markets = vec![MarketKind::EurUsdFuture];
        let market_simulation_data = MarketSimulationDataKind::Ohlc1m;
        let time_frame = TimeFrameKind::Daily;
        let client = config::get_google_cloud_storage_client().await;
        let bucket = config::GoogleCloudBucket {
            historical_market_data_bucket_name: "chapaty-ai-hdb-int".to_string(),
            cached_bot_data_bucket_name: "chapaty-ai-int".to_string(),
        };

        let strategy =
            setup_strategy_parametrized(*offset, *number_candles_to_wait, *loss_to_win_ratio);
        let bot = BotBuilder::new(strategy, data_provider.clone())
            .with_years(years.clone())
            .with_markets(markets.clone())
            .with_market_simulation_data(market_simulation_data)
            .with_time_frame(time_frame)
            .with_google_cloud_storage_client(client.clone())
            .with_google_cloud_bucket(bucket.clone())
            .with_save_result_as_csv(false)
            .with_cache_computations(false)
            .build()
            .unwrap();

        let result = bot.backtest().await;
        let total_profit = result
            .agg_market_and_agg_year
            .performance_report
            .get(0)
            .unwrap()[4]
            .unwrap_float64();
        res.push((
            number_candles_to_wait,
            loss_to_win_ratio,
            offset,
            total_profit,
        ));
    }

    let max_profit = res
        .iter()
        .map(|&(_, _, _, total_profit)| total_profit)
        .max_by(|a, b| a.partial_cmp(b).unwrap())
        .unwrap();

    let max_rows: Vec<_> = res.into_iter()
    .inspect(|(number_candles_to_wait, loss_to_win_ratio, offset, total_profit)| {
        println!(
            "number_candles_to_wait = {number_candles_to_wait}, loss_to_win_ratio = {loss_to_win_ratio}, offset = {offset}, total_profit = {total_profit}",
        );
    })
    .filter(|&(_, _, _, total_profit)| total_profit >= max_profit)
    .collect();

    if !max_rows.is_empty() {
        for (number_candles_to_wait, loss_to_win_ratio, offset, total_profit) in max_rows {
            println!(
            "Max profit row: number_candles_to_wait = {number_candles_to_wait}, loss_to_win_ratio = {loss_to_win_ratio}, offset = {offset}, total_profit = {total_profit}",
        );
        }
    }

    let duration = start.elapsed();
    println!(
        "Total time elapsed testing {} combinations is: {duration:?}",
        strategy_parameters.len()
    );

    assert_eq!(0, 0);
}

fn setup_strategy_parametrized(
    offset: f64,
    number_candles_to_wait: i32,
    loss_to_win_ration: f64,
) -> Arc<dyn Strategy + Send + Sync> {
    let news_builder = NewsCounterBuilder::new();
    let tp = TakeProfit {
        kind: TakeProfitKind::PriceUponTradeEntry,
        offset,
    };

    let strategy = news_builder
        .with_stop_loss_kind(StopLossKind::PriceUponTradeEntry)
        .with_take_profit(tp)
        .with_news_kind(NewsKind::UsaNFP)
        .with_number_candles_to_wait(number_candles_to_wait)
        .with_loss_to_win_ratio(loss_to_win_ration)
        .build();

    Arc::new(strategy)
}

fn range(start: f64, end: f64, step: f64, setp_precision: f64) -> Vec<f64> {
    let mut vec = Vec::new();
    let start_scaled = (start * setp_precision).round() as i32;
    let end_scaled = (end * setp_precision).round() as i32;
    let step_scaled = (step * setp_precision).round() as usize;

    for i in (start_scaled..=end_scaled).step_by(step_scaled) {
        let value = i as f64 / setp_precision;
        vec.push(value);
    }

    vec
}
