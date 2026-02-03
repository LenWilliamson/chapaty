use std::{collections::BTreeSet, path::Path, time::Instant};

use anyhow::{Context, Result};
use chapaty::{
    agent::news::breakout::{NewsBreakout, NewsBreakoutGrid},
    data::{
        config::{EconomicCalendarConfig, OhlcvFutureConfig},
        filter::EconomicCalendarPolicy,
    },
    prelude::*,
};

use polars::io::cloud::CloudOptions;
use rayon::iter::{ParallelBridge, ParallelIterator};

// === BEGIN JEMALLOC CONFIG ===
#[cfg(target_os = "linux")]
use tikv_jemallocator::Jemalloc;

#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;
// === END JEMALLOC CONFIG ===

#[tokio::main]
async fn main() -> Result<()> {
    println!("Starting evaluation process...");

    let build_start = Instant::now();
    let mut env = environment().await?;
    let build_time = build_start.elapsed();

    let (stream_len, agents_iter) = news_breakout_grid();
    let grid_backtest_start = Instant::now();
    let leaderboard = env.evaluate_agents(
        agents_iter.collect::<Vec<_>>().into_iter().par_bridge(),
        100,
        stream_len as u64,
    )?;
    let grid_backtest_time = grid_backtest_start.elapsed();

    let path = Path::new("examples/reports/news_breakout");
    leaderboard.to_csv(path, None, None)?;

    println!("\n--- Evaluation Timings ---");
    println!("1. Environment build time:      {build_time:?}");
    println!("2. Breakout agents run time:    {grid_backtest_time:?}");

    Ok(())
}

// ================================================================================================
// Helper Functions
// ================================================================================================

fn news_breakout_grid() -> (usize, impl ParallelIterator<Item = (usize, NewsBreakout)>) {
    let cal_config = economic_calendar_config()
        .to_id()
        .expect("Failed to create economic calendar ID");
    let ohlcv_config = ohlcv_config().to_id().expect("Failed to create OHLCV ID");
    NewsBreakoutGrid::baseline(cal_config, ohlcv_config)
        .expect("Failed to create baseline grid")
        // Optional: Constrain the grid for a quick demo run
        // .with_stop_loss_risk_factor(GridAxis::new("0.5", "1.5", "0.01").expect("Invalid stop loss axis"))
        // .with_risk_reward_ratio(GridAxis::new("0.5", "1.0", "0.1").expect("Invalid RRR axis"))
        .build()
}

async fn environment() -> Result<Environment> {
    let allowed_years = Some((2024..=2025).collect::<BTreeSet<_>>());
    let filter_config = FilterConfig {
        allowed_years,
        economic_news_policy: Some(EconomicCalendarPolicy::OnlyWithEvents),
        ..Default::default()
    };

    let cfg = EnvConfig::default()
        .add_ohlcv_future(DataSource::Chapaty, ohlcv_config())
        .with_episode_length(EpisodeLength::Day)
        .with_filter_config(filter_config)
        .add_economic_calendar(DataSource::Chapaty, economic_calendar_config())
        .with_trade_hint(2);

    let loc = StorageLocation::Cloud {
        path: "gs://chapaty-cache/examples/breakout",
        options: CloudOptions::default(),
    };

    chapaty::load(cfg, &loc, SerdeFormat::Postcard, 128 * 1024)
        .await
        .context("Failed to load trading environment")
}

fn economic_calendar_config() -> EconomicCalendarConfig {
    EconomicCalendarConfig {
        broker: DataBroker::InvestingCom,
        data_source: None,
        country_code: Some(CountryCode::Us),
        category: Some(EconomicCategory::Employment),
        importance: Some(EconomicEventImpact::High),
        batch_size: 1000,
    }
}

fn ohlcv_config() -> OhlcvFutureConfig {
    OhlcvFutureConfig {
        broker: DataBroker::NinjaTrader,
        symbol: Symbol::Future(FutureContract {
            root: FutureRoot::EurUsd,
            month: ContractMonth::March,
            year: ContractYear::Y6,
        }),
        exchange: Some(Exchange::Cme),
        period: Period::Minute(1),
        batch_size: 1000,
        indicators: vec![],
    }
}
