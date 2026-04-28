use std::{path::Path, time::Instant};

use anyhow::{Context, Result};
use chapaty::{
    gym::trading::agent::news::{breakout::NewsBreakout, fade::NewsFade, hybrid::NewsHybrid},
    prelude::*,
};
use chrono::Duration;

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
    let file_cfg = FileConfig::default().with_dir(path);
    journal.to_file_sync(&file_cfg)?;
    journal.cumulative_returns()?.to_file_sync(&file_cfg)?;
    journal.portfolio_performance()?.to_file_sync(&file_cfg)?;
    journal.trade_stats()?.to_file_sync(&file_cfg)?;
    env.equity_curve_report()?
        .into_eod()?
        .to_file_sync(&file_cfg)?;

    println!("\n--- Evaluation Timings ---");
    println!("1. Environment build time:      {build_time:?}");
    println!("2. Hybrid agent run time:       {decision_time:?}");

    Ok(())
}

// ================================================================================================
// Helper Functions
// ================================================================================================

fn news_hybrid() -> Result<NewsHybrid> {
    let cal_id = economic_calendar_id();

    let fade = NewsFade::baseline(cal_id, ohlcv_id(Period::Minute(1)))
        .with_candles_after_news(Duration::minutes(7))
        .with_take_profit_risk_factor(1.27)
        .with_risk_reward_ratio(0.276)?;

    let breakout = NewsBreakout::baseline(cal_id, ohlcv_id(Period::Minute(5)))
        .with_earliest_entry_candle(Duration::minutes(8))
        .with_latest_entry_candle(Duration::minutes(50))
        .with_stop_loss_risk_factor(0.89)
        .with_risk_reward_ratio(0.726)?;

    Ok(NewsHybrid { breakout, fade })
}

async fn environment() -> Result<Environment> {
    let preset = EnvPreset::NinjaTraderCme6eh61m5mUsEmpHighEventsOnly;
    let file_stem = preset.to_string();
    let loc = StorageLocation::HuggingFace { version: None };
    let cfg = IoConfig::new(loc).with_file_stem(&file_stem);

    chapaty::load(preset, &cfg)
        .await
        .context("Failed to load trading environment")
}

fn economic_calendar_id() -> EconomicCalendarId {
    EconomicCalendarId {
        broker: DataBroker::InvestingCom,
        data_source: None,
        country_code: Some(CountryCode::Us),
        category: Some(EconomicCategory::Employment),
        importance: Some(EconomicEventImpact::High),
    }
}

fn ohlcv_id(period: Period) -> OhlcvId {
    OhlcvId {
        broker: DataBroker::NinjaTrader,
        exchange: Exchange::Cme,
        symbol: Symbol::Future(FutureContract {
            root: FutureRoot::EurUsd,
            month: ContractMonth::March,
            year: ContractYear::Y6,
        }),
        period,
    }
}
