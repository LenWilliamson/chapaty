use std::{collections::BTreeSet, path::Path, time::Instant};

use anyhow::{Context, Result};
use chapaty::{
    agent::news::breakout::NewsBreakout,
    data::{
        config::{EconomicCalendarConfig, OhlcvFutureConfig},
        filter::EconomicCalendarPolicy,
    },
    prelude::*,
};
use chrono::Duration;
use polars::io::cloud::CloudOptions;

#[tokio::main]
async fn main() -> Result<()> {
    println!("Starting evaluation process...");

    let build_start = Instant::now();
    let mut env = environment().await?;
    let build_time = build_start.elapsed();

    let mut agent = news_breakout()?;
    let breakout_start = Instant::now();
    let journal = env.evaluate_agent(&mut agent)?;
    let breakout_time = breakout_start.elapsed();

    let path = Path::new("examples/reports/news_breakout");
    journal.to_csv(path, None, None)?;
    journal.cumulative_returns()?.to_csv(path, None, None)?;
    journal.equity_curve_fitting()?.to_csv(path, None, None)?;
    journal.portfolio_performance()?.to_csv(path, None, None)?;
    journal.trade_stats()?.to_csv(path, None, None)?;

    println!("\n--- Evaluation Timings ---");
    println!("1. Environment build time:      {build_time:?}");
    println!("2. Breakout agent run time:     {breakout_time:?}");

    Ok(())
}

// ================================================================================================
// Helper Functions
// ================================================================================================

fn news_breakout() -> Result<NewsBreakout> {
    let cal_id = economic_calendar_config().to_id()?;
    let ohlcv_id = ohlcv_config().to_id()?;
    let agent = NewsBreakout::baseline(cal_id, ohlcv_id)
        .with_earliest_entry_candle(Duration::minutes(10))
        .with_latest_entry_candle(Duration::minutes(50))
        .with_stop_loss_risk_factor(1.15)
        .with_risk_reward_ratio(1.0 / 0.7)?;
    Ok(agent)
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
