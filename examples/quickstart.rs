use anyhow::{Context, Result};
use chapaty::prelude::*;
use serde::Serialize;
use std::{env, fs, path::Path, sync::Arc, time::Instant};
use time::macros::format_description;
use tracing::{debug, info};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;

const LEADERBOARD_TOP_K: usize = 10;
const GRID_SIZE: usize = 400;
const REPORTS_SUBDIR: &str = "examples/reports/quickstart";

// ================================================================================================
// No-Op Agent
//
// A placeholder agent that never trades. It exists only to demonstrate the evaluation API
// (single-agent journals + parallel leaderboards) and the logging setup, without bundling any
// real strategy logic into the core crate.
//
// For real, ready-to-run strategies, see chapaty-zoo:
// https://github.com/LenWilliamson/chapaty-zoo
// ================================================================================================

#[derive(Clone, Serialize)]
struct NoOpAgent {
    #[serde(skip)]
    agent_id: AgentIdentifier,
}

impl Default for NoOpAgent {
    fn default() -> Self {
        Self {
            agent_id: AgentIdentifier::Named(Arc::new("NoOpAgent".to_string())),
        }
    }
}

impl Agent for NoOpAgent {
    fn identifier(&self) -> AgentIdentifier {
        self.agent_id.clone()
    }

    fn reset(&mut self) {}

    // `act` is called millions of times.
    // Keep logging here at `debug` so it stays silent under the default `info` filter.
    #[tracing::instrument(skip_all)]
    fn act(&mut self, _obs: Observation) -> ChapatyResult<Actions> {
        debug!("Returning no actions, guaranteeing 0 trades");
        Ok(Actions::no_op())
    }
}

// ================================================================================================
// Main
// ================================================================================================

#[tokio::main]
async fn main() -> Result<()> {
    let _guard = init_tracing()?;
    info!("Starting evaluation example...");

    let build_start = Instant::now();
    let mut env = environment().await?;
    info!(build_time = ?build_start.elapsed(), "Environment ready");

    let reports_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join(REPORTS_SUBDIR);
    let file_cfg = FileConfig::default().with_dir(&reports_dir);

    // === 1. Single-agent baseline: full journal + reports ===
    let mut baseline = NoOpAgent::default();
    let label = baseline.identifier();

    let baseline_start = Instant::now();
    info!(%label, "Running baseline backtest...");

    let journal = env.evaluate_agent(&mut baseline)?;
    journal.to_file_sync(&file_cfg)?;
    journal.cumulative_returns()?.to_file_sync(&file_cfg)?;
    journal.portfolio_performance()?.to_file_sync(&file_cfg)?;
    journal.trade_stats()?.to_file_sync(&file_cfg)?;
    env.equity_curve_report()?
        .into_eod()?
        .to_file_sync(&file_cfg)?;

    info!(%label, elapsed = ?baseline_start.elapsed(), "Baseline backtest complete");

    // === 2. Parallel grid: ranked leaderboard ===
    let agents = (0..GRID_SIZE)
        .map(|uid| (uid, NoOpAgent::default()))
        .collect::<Vec<_>>();

    let grid_start = Instant::now();
    info!(grid_size = GRID_SIZE, "Evaluating agents in parallel...");

    let leaderboard = env.evaluate_agents(agents, LEADERBOARD_TOP_K)?;
    leaderboard.to_file_sync(&file_cfg)?;

    info!(
        elapsed = ?grid_start.elapsed(),
        rows = leaderboard.as_df().height(),
        dir = %file_cfg.dir.display(),
        "Grid evaluation complete; leaderboard saved"
    );

    // The WorkerGuard ensures all buffered logs are flushed when dropped.
    drop(_guard);
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

// ================================================================================================
// Tracing Configuration
//
// JSON to stdout in containers, or to a timestamped file under the OS state dir locally.
// ================================================================================================

fn init_tracing() -> Result<Option<WorkerGuard>> {
    let app_name = "chapaty";

    let in_container = env::var("CONTAINER").is_ok() || Path::new("/.dockerenv").exists();

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    if in_container {
        // Container mode: log to stdout
        tracing_subscriber::fmt()
            .json()
            .with_env_filter(env_filter)
            .with_span_events(tracing_subscriber::fmt::format::FmtSpan::NONE)
            .with_current_span(true)
            .with_thread_ids(true)
            .with_thread_names(true)
            .with_timer(tracing_subscriber::fmt::time::UtcTime::rfc_3339())
            .init();

        info!("Logging to stdout (container mode)");
        Ok(None)
    } else {
        // Local mode: log to file
        let log_dir = dirs::state_dir()
            .map(|mut p| {
                p.push(app_name);
                p.push("logs");
                p
            })
            .unwrap_or_else(|| {
                let mut home = dirs::home_dir().expect("Failed to find home directory");
                home.push(format!(".local/state/{app_name}/logs"));
                home
            });
        fs::create_dir_all(&log_dir)?;

        let timestamp = time::OffsetDateTime::now_utc()
            .format(&format_description!(
                "[year][month][day]-[hour][minute][second]"
            ))
            .context("Failed to format timestamp")?;
        let file_name = format!("{app_name}-{timestamp}.log");

        let file_appender = tracing_appender::rolling::never(&log_dir, &file_name);
        let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

        tracing_subscriber::fmt()
            .json()
            .with_env_filter(env_filter)
            .with_writer(non_blocking)
            .with_span_events(tracing_subscriber::fmt::format::FmtSpan::NONE)
            .with_current_span(true)
            .with_thread_ids(true)
            .with_thread_names(true)
            .with_timer(tracing_subscriber::fmt::time::UtcTime::rfc_3339())
            .init();

        info!(log_file = %log_dir.join(&file_name).display(), "Logging to file (local mode)");
        Ok(Some(guard))
    }
}
