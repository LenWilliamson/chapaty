use anyhow::{Context, Result};
use chapaty::prelude::*;
use serde::Serialize;
use std::path::Path;
use std::sync::Arc;

#[derive(Clone, Serialize)]
struct NoOpAgent;

impl Agent for NoOpAgent {
    fn identifier(&self) -> AgentIdentifier {
        AgentIdentifier::Named(Arc::new("NoOpAgent".to_string()))
    }

    fn reset(&mut self) {}

    fn act(&mut self, _obs: Observation) -> ChapatyResult<Actions> {
        // Return no actions, guaranteeing 0 trades
        Ok(Actions::no_op())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut env = environment().await?;
    let num_agents = 5;
    let agents: Vec<(usize, NoOpAgent)> = (0..num_agents).map(|uid| (uid, NoOpAgent)).collect();

    let leaderboard = env.evaluate_agents(agents, 10)?;

    println!(
        "Evaluation complete. Leaderboard size: {}",
        leaderboard.as_df().height()
    );

    let export_dir = Path::new("examples/reports/noop_grid");
    let file_cfg = FileConfig::default().with_dir(export_dir);
    leaderboard.to_file_sync(&file_cfg)?;

    println!("Saved leaderboard to {}", export_dir.display());

    Ok(())
}

async fn environment() -> Result<Environment> {
    let preset = EnvPreset::BinanceBtcUsdt1d;
    let file_stem = preset.to_string();
    let loc = StorageLocation::HuggingFace { version: None };
    let cfg = IoConfig::new(loc).with_file_stem(&file_stem);

    chapaty::load(preset, &cfg)
        .await
        .context("Failed to load trading environment")
}

#[allow(dead_code)] // Provided for completeness
fn ohlcv_id() -> OhlcvId {
    OhlcvId {
        broker: DataBroker::Binance,
        exchange: Exchange::Binance,
        symbol: Symbol::Spot(SpotPair::BtcUsdt),
        period: Period::Day(1),
    }
}
