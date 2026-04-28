use anyhow::{Context, Result};
use chapaty::{
    gym::trading::agent::crossover::{PrecomputedCrossover, StreamingCrossover},
    prelude::*,
};
use std::path::Path;

#[tokio::main]
async fn main() -> Result<()> {
    let ohlcv_id = ohlcv_id();
    let fast_sma = SmaWindow(20);
    let slow_sma = SmaWindow(50);
    let fast_sma_id = SmaId {
        parent: ohlcv_id,
        length: fast_sma,
    };
    let slow_sma_id = SmaId {
        parent: ohlcv_id,
        length: slow_sma,
    };

    let mut env = environment().await?;

    println!("Running Streaming Crossover Agent...");
    let mut streaming_agent = StreamingCrossover::new(ohlcv_id, fast_sma.0, slow_sma.0);
    let journal_stream = env.evaluate_agent(&mut streaming_agent)?;
    let file_cfg = FileConfig::default().with_dir(Path::new("examples/reports/streaming_cross"));
    journal_stream.to_file_sync(&file_cfg)?;
    env.equity_curve_report()?
        .into_eod()?
        .to_file_sync(&file_cfg)?;

    println!("Running Precomputed Crossover Agent...");
    let mut env_agent = PrecomputedCrossover::new(ohlcv_id, fast_sma_id, slow_sma_id);
    let journal_env = env.evaluate_agent(&mut env_agent)?;
    let file_cfg = FileConfig::default().with_dir(Path::new("examples/reports/precomputed_cross"));
    journal_env.to_file_sync(&file_cfg)?;
    env.equity_curve_report()?
        .into_eod()?
        .to_file_sync(&file_cfg)?;

    Ok(())
}

async fn environment() -> Result<Environment> {
    let preset = EnvPreset::BinanceBtcUsdt1dSma20Sma50;
    let file_stem = preset.to_string();
    let loc = StorageLocation::HuggingFace { version: None };
    let cfg = IoConfig::new(loc).with_file_stem(&file_stem);

    chapaty::load(preset, &cfg)
        .await
        .context("Failed to load trading environment")
}

fn ohlcv_id() -> OhlcvId {
    OhlcvId {
        broker: DataBroker::Binance,
        exchange: Exchange::Binance,
        symbol: Symbol::Spot(SpotPair::BtcUsdt),
        period: Period::Day(1),
    }
}
