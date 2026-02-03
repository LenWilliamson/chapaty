use anyhow::{Context, Result};
use chapaty::{
    agent::crossover::{PrecomputedCrossover, StreamingCrossover},
    data::{config::OhlcvSpotConfig, event::SmaId},
    prelude::*,
};
use polars::io::cloud::CloudOptions;
use std::{collections::BTreeSet, path::Path};

#[tokio::main]
async fn main() -> Result<()> {
    let fast_window = 20;
    let slow_window = 50;
    let ohlcv_cfg = ohlcv_config(fast_window, slow_window);

    let ohlcv_id = ohlcv_cfg.to_id()?;
    let fast_sma_id = SmaId {
        parent: ohlcv_id,
        length: SmaWindow(fast_window as u16),
    };
    let slow_sma_id = SmaId {
        parent: ohlcv_id,
        length: SmaWindow(slow_window as u16),
    };

    let mut env = environment(ohlcv_cfg).await?;

    println!("Running Streaming Crossover Agent...");
    let mut streaming_agent = StreamingCrossover::new(ohlcv_id, fast_window, slow_window);
    let journal_stream = env.evaluate_agent(&mut streaming_agent)?;
    journal_stream.to_csv(Path::new("examples/reports/streaming_cross"), None, None)?;

    println!("Running Precomputed Crossover Agent...");
    let mut env_agent = PrecomputedCrossover::new(ohlcv_id, fast_sma_id, slow_sma_id);
    let journal_env = env.evaluate_agent(&mut env_agent)?;
    journal_env.to_csv(Path::new("examples/reports/precomputed_cross"), None, None)?;

    Ok(())
}

async fn environment(ohlcv_cfg: OhlcvSpotConfig) -> Result<Environment> {
    let filter_cfg = FilterConfig {
        allowed_years: Some((2024..=2025).collect::<BTreeSet<_>>()),
        ..FilterConfig::default()
    };
    let cfg = EnvConfig::default()
        .add_ohlcv_spot(DataSource::Chapaty, ohlcv_cfg)
        .with_episode_length(EpisodeLength::Infinite)
        .with_filter_config(filter_cfg);

    let loc = StorageLocation::Cloud {
        path: "gs://chapaty-cache/examples/crossover",
        options: CloudOptions::default(),
    };

    chapaty::load(cfg, &loc, SerdeFormat::Postcard, 128 * 1024)
        .await
        .context("Failed to load trading environment")
}

fn ohlcv_config(fast_window: u16, slow_window: u16) -> OhlcvSpotConfig {
    OhlcvSpotConfig {
        broker: DataBroker::Binance,
        symbol: Symbol::Spot(SpotPair::BtcUsdt),
        exchange: Some(Exchange::Binance),
        period: Period::Day(1),
        batch_size: 1000,
        indicators: vec![
            TechnicalIndicator::Sma(SmaWindow(fast_window)),
            TechnicalIndicator::Sma(SmaWindow(slow_window)),
        ],
    }
}
