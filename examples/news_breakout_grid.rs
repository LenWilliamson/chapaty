use std::{path::Path, time::Instant};

use anyhow::{Context, Result};
use chapaty::{
    gym::trading::agent::news::breakout::{NewsBreakout, NewsBreakoutGrid},
    prelude::*,
};
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
    leaderboard.to_file_sync(&FileConfig::default().with_dir(path))?;

    println!("\n--- Evaluation Timings ---");
    println!("1. Environment build time:      {build_time:?}");
    println!("2. Breakout agents run time:    {grid_backtest_time:?}");

    Ok(())
}

// ================================================================================================
// Helper Functions
// ================================================================================================

fn news_breakout_grid() -> (usize, impl ParallelIterator<Item = (usize, NewsBreakout)>) {
    NewsBreakoutGrid::baseline(economic_calendar_id(), ohlcv_id())
        .expect("Failed to create baseline grid")
        // Optional: Constrain the grid for a quick demo run
        // .with_stop_loss_risk_factor(GridAxis::new("0.5", "1.5", "0.01").expect("Invalid stop loss axis"))
        // .with_risk_reward_ratio(GridAxis::new("0.5", "1.0", "0.1").expect("Invalid RRR axis"))
        .build()
}

async fn environment() -> Result<Environment> {
    let preset = EnvPreset::NinjaTraderCme6eh61mUsEmpHighEventsOnly;
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

fn ohlcv_id() -> OhlcvId {
    OhlcvId {
        broker: DataBroker::NinjaTrader,
        exchange: Exchange::Cme,
        symbol: Symbol::Future(FutureContract {
            root: FutureRoot::EurUsd,
            month: ContractMonth::March,
            year: ContractYear::Y6,
        }),
        period: Period::Minute(1),
    }
}
