use std::{env, fs, path::Path, time::Instant};

use anyhow::{Context, Result};
use chapaty::{gym::trading::agent::news::fade::NewsFade, prelude::*};
use chrono::Duration;
use time::macros::format_description;
use tracing::info;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    // Create simple logging subscriber
    let _guard = init_tracing()?;

    println!("Starting evaluation process...");

    let build_start = Instant::now();
    let mut env = environment().await?;
    let build_time = build_start.elapsed();

    let mut agent = news_fade()?;
    let fade_start = Instant::now();
    let journal = env.evaluate_agent(&mut agent)?;
    let fade_time = fade_start.elapsed();

    let path = Path::new("examples/reports/news_fade");
    journal.to_file_sync(&FileConfig::default().with_dir(path))?;

    println!("\n--- Evaluation Timings ---");
    println!("1. Environment build time:      {build_time:?}");
    println!("2. Fade agent run time:         {fade_time:?}");

    // The WorkerGuard ensures all buffered logs are flushed when dropped.
    drop(_guard);

    Ok(())
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

// ================================================================================================
// Helper Functions
// ================================================================================================

fn news_fade() -> Result<NewsFade> {
    let agent = NewsFade::baseline(economic_calendar_id(), ohlcv_id())
        .with_candles_after_news(Duration::minutes(14))
        .with_take_profit_risk_factor(0.0)
        .with_risk_reward_ratio(0.1)?;
    Ok(agent)
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
            month: ContractMonth::June,
            year: ContractYear::Y6,
        }),
        period: Period::Minute(1),
    }
}
