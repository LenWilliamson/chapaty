use anyhow::{Context, Result};
use chapaty::prelude::*;
use serde::Serialize;
use std::{env, fs, sync::Arc};
use time::macros::format_description;
use tracing::{debug, info};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;

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

    #[tracing::instrument(skip_all)]
    fn act(&mut self, _obs: Observation) -> ChapatyResult<Actions> {
        debug!("Return no actions, guaranteeing 0 trades");
        Ok(Actions::no_op())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Create simple logging subscriber
    let _guard = init_tracing()?;

    let mut baseline = NoOpAgent::default();
    let label = baseline.identifier();
    info!("Starting NoOpGrid example with label: {label}");

    let mut env = environment().await?;
    let journal = env.evaluate_agent(&mut baseline)?;

    let file_cfg = FileConfig::default();
    journal.to_file_sync(&file_cfg)?;
    journal.cumulative_returns()?.to_file_sync(&file_cfg)?;
    journal.portfolio_performance()?.to_file_sync(&file_cfg)?;
    journal.trade_stats()?.to_file_sync(&file_cfg)?;
    env.equity_curve_report()?
        .into_eod()?
        .to_file_sync(&file_cfg)?;
    info!("{label} baseline backtest complete");

    let num_agents = 1_000_000;
    let agents = (0..num_agents)
        .map(|uid| (uid, NoOpAgent::default()))
        .collect::<Vec<_>>();

    info!("Evaluating agents in parallel...");
    let leaderboard = env.evaluate_agents(agents, 100)?;

    info!(
        "{label} grid evaluation complete. Leaderboard size: {}",
        leaderboard.as_df().height()
    );

    leaderboard.to_file_sync(&file_cfg)?;
    info!("Saved leaderboard to {}", file_cfg.dir.display());

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
// ================================================================================================

fn init_tracing() -> Result<Option<WorkerGuard>> {
    let app_name = "chapaty";

    // Detect if running in container
    let in_container =
        env::var("CONTAINER").is_ok() || std::path::Path::new("/.dockerenv").exists();

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
        let file_path = log_dir.join(file_name);

        let file_appender =
            tracing_appender::rolling::never(log_dir.clone(), file_path.file_name().unwrap());
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

        info!(log_file = %file_path.display(), "Logging to file (local mode)");
        Ok(Some(guard))
    }
}
