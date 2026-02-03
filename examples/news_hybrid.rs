use std::{collections::BTreeSet, path::Path, time::Instant};

use anyhow::{Context, Result};
use chapaty::{
    agent::news::{breakout::NewsBreakout, fade::NewsFade, hybrid::NewsHybrid},
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

    let mut agent = news_hybrid()?;
    let decision_start = Instant::now();
    let journal = env.evaluate_agent(&mut agent)?;
    let decision_time = decision_start.elapsed();

    let path = Path::new("examples/reports/news_hybrid");
    journal.to_csv(path, None, None)?;
    journal.cumulative_returns()?.to_csv(path, None, None)?;
    journal.equity_curve_fitting()?.to_csv(path, None, None)?;
    journal.portfolio_performance()?.to_csv(path, None, None)?;
    journal.trade_stats()?.to_csv(path, None, None)?;

    println!("\n--- Evaluation Timings ---");
    println!("1. Environment build time:      {build_time:?}");
    println!("2. Hybrid agent run time:       {decision_time:?}");

    Ok(())
}

// ================================================================================================
// Helper Functions
// ================================================================================================

fn news_hybrid() -> Result<NewsHybrid> {
    let cal_id = economic_calendar_config().to_id()?;
    let ohlcv_1m_id = ohlcv_config(Period::Minute(1)).to_id()?;

    let fade = NewsFade::baseline(cal_id, ohlcv_1m_id)
        .with_candles_after_news(Duration::minutes(7))
        .with_take_profit_risk_factor(1.27)
        .with_risk_reward_ratio(0.276)?;

    let ohlcv_5m_id = ohlcv_config(Period::Minute(5)).to_id()?;
    let breakout = NewsBreakout::baseline(cal_id, ohlcv_5m_id)
        .with_earliest_entry_candle(Duration::minutes(8))
        .with_latest_entry_candle(Duration::minutes(50))
        .with_stop_loss_risk_factor(0.89)
        .with_risk_reward_ratio(0.726)?;

    Ok(NewsHybrid { breakout, fade })
}

async fn environment() -> Result<Environment> {
    let allowed_years = Some((2024..=2025).collect::<BTreeSet<_>>());
    let filter_config = FilterConfig {
        allowed_years,
        economic_news_policy: Some(EconomicCalendarPolicy::OnlyWithEvents),
        ..Default::default()
    };

    let cfg = EnvConfig::default()
        .add_ohlcv_future(DataSource::Chapaty, ohlcv_config(Period::Minute(1)))
        .add_ohlcv_future(DataSource::Chapaty, ohlcv_config(Period::Minute(5)))
        .with_episode_length(EpisodeLength::Day)
        .with_filter_config(filter_config)
        .add_economic_calendar(DataSource::Chapaty, economic_calendar_config())
        .with_trade_hint(4);

    let loc = StorageLocation::Cloud {
        path: "gs://chapaty-cache/examples/hybrid",
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

fn ohlcv_config(period: Period) -> OhlcvFutureConfig {
    OhlcvFutureConfig {
        broker: DataBroker::NinjaTrader,
        symbol: Symbol::Future(FutureContract {
            root: FutureRoot::EurUsd,
            month: ContractMonth::March,
            year: ContractYear::Y6,
        }),
        exchange: Some(Exchange::Cme),
        period,
        batch_size: 1000,
        indicators: vec![],
    }
}
