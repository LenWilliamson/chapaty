use crate::{
    data::{
        common::ProfileAggregation,
        config::ConfigId,
        domain::{
            Count, CountryCode, EconomicEventImpact, EconomicValue, ExecutionDepth, LiquiditySide,
            Price, Quantity, TradeId,
        },
        episode::{EpisodeBuilder, EpisodeLength},
        event::{
            EconomicCalendarId, EconomicEvent, Ema, EmaId, Ohlcv, OhlcvId, Rsi, RsiId, Sma, SmaId,
            StreamId, Tpo, TpoBin, TpoId, Trade, TradesId, VolumeProfile, VolumeProfileBin,
            VolumeProfileId,
        },
        filter::{EconomicCalendarPolicy, TradingWindow, Weekday},
        indicator::{EmaWindow, RsiWindow, SmaWindow, TechnicalIndicator},
    },
    error::{ChapatyError, ChapatyResult, DataError, EnvError},
    gym::trading::{
        config::{EnvConfig, ExecutionBias},
        env::Environment,
        ledger::Ledger,
        state::States,
    },
    io::{SerdeFormat, StorageLocation},
    math::market_profile::compute_profile_stats,
    sim::{
        cursor_group::CursorGroup,
        data::{SimulationData, SimulationDataBuilder},
    },
    sorted_vec_map::SortedVecMap,
    transport::{
        fetcher::Fetchable, loader::load_batch, schema::CanonicalCol, source::SourceGroup,
    },
};

use chrono::{DateTime, Utc};
use itertools::izip;
use polars::{
    frame::{DataFrame, UniqueKeepStrategy},
    prelude::{
        BooleanType, ChunkedArray, DataType, DatetimeType, Float64Type, Int64Type, JoinArgs,
        JoinType, LazyFrame, Logical, PlSmallStr, Schema, SchemaRef, Selector, SortMultipleOptions,
        StringType, TimeUnit, UnionArgs, col, lit,
    },
};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use std::{
    collections::{BTreeMap, HashMap},
    hash::Hash,
    pin::Pin,
    sync::Arc,
};
use tracing::{debug, info, warn};

/// Builds a trading environment from this configuration.
#[tracing::instrument(skip(cfg), fields(hash = tracing::field::Empty))]
pub async fn make(cfg: impl Into<EnvConfig>) -> ChapatyResult<Environment> {
    let env_cfg = cfg.into();
    if !env_cfg.is_valid() {
        return Err(EnvError::InvalidConfig("No market data configured".to_string()).into());
    }

    if let Ok(hash) = env_cfg.hash() {
        tracing::Span::current().record("hash", &hash);
    }

    let mut ctx = BuildCtx {
        env_cfg,
        ..Default::default()
    };

    ctx.run().await?;
    ctx.final_env.ok_or(EnvError::NotBuilt.into())
}

/// Loads a pre-built environment from storage, or builds a new one on cache miss.
///
/// # Arguments
///
/// * `env_cfg` - The environment configuration (used to derive the filename hash)
/// * `location` - The storage location to read from
/// * `format` - The serialization format to use (must match the format used to write)
/// * `buffer_size` - Size of the internal read buffer, in bytes. A good default is: `128 * 1024` (128 KiB).
#[tracing::instrument(skip(cfg), fields(hash = tracing::field::Empty))]
pub async fn load<'a>(
    cfg: impl Into<EnvConfig>,
    location: &StorageLocation<'a>,
    format: SerdeFormat,
    buffer_size: usize,
) -> ChapatyResult<Environment> {
    let env_cfg: EnvConfig = cfg.into();

    let sim_data = match SimulationData::read(&env_cfg, location, format, buffer_size).await {
        Ok(data) => {
            tracing::info!("Cache hit: Initializing environment from loaded data.");
            Arc::new(data)
        }
        Err(_) => {
            tracing::info!("Cache miss: Building new environment.");
            return make(env_cfg).await;
        }
    };

    let trade_hint = env_cfg.trade_hint();
    let initial_states = States::with_capacity(&sim_data.market_ids(), trade_hint);

    let ep_len = env_cfg.episode_length();
    let start_time = sim_data.global_open_start();

    let initial_ep = EpisodeBuilder::new()
        .with_start(start_time)
        .with_length(ep_len)
        .build()?;

    let estimated_capacity = env_cfg.max_episode_capacity();
    let ledger = Ledger::with_capacity(estimated_capacity, initial_states);

    let cursor = CursorGroup::new(&sim_data)?;

    tracing::debug!("Hydrating environment from cached SimulationData");

    Ok(Environment::new(cursor, sim_data, ledger, initial_ep)
        .with_invalid_action_penalty(env_cfg.invalid_action_penalty())
        .with_execution_bias(ExecutionBias::default())
        .with_risk_metrics_cfg(env_cfg.risk_metrics_cfg()))
}

// ================================================================================================
// Finite State Machine
// ================================================================================================

#[derive(Default)]
struct BuildCtx {
    env_cfg: EnvConfig,
    final_env: Option<Environment>,

    ohlcv_spot_map: Option<HashMap<OhlcvId, (SchemaRef, LazyFrame)>>,
    ohlcv_future_map: Option<HashMap<OhlcvId, (SchemaRef, LazyFrame)>>,
    trade_spot_map: Option<HashMap<TradesId, (SchemaRef, LazyFrame)>>,
    tpo_spot_map: Option<HashMap<TpoId, (SchemaRef, LazyFrame)>>,
    tpo_future_map: Option<HashMap<TpoId, (SchemaRef, LazyFrame)>>,
    vp_spot_map: Option<HashMap<VolumeProfileId, (SchemaRef, LazyFrame)>>,
    economic_calendar_map: Option<HashMap<EconomicCalendarId, (SchemaRef, LazyFrame)>>,

    // === Technical Indicators ===
    ema_map: Option<HashMap<EmaId, (SchemaRef, LazyFrame)>>,
    rsi_map: Option<HashMap<RsiId, (SchemaRef, LazyFrame)>>,
    sma_map: Option<HashMap<SmaId, (SchemaRef, LazyFrame)>>,
}

impl BuildCtx {
    #[tracing::instrument(skip(self), name = "env_build")]
    async fn run(&mut self) -> ChapatyResult<()> {
        let mut state = Self::start()?;
        loop {
            match state {
                StateFn::Next(next_sync) => state = next_sync(self)?,
                StateFn::NextAsync(next_async) => state = next_async(self).await?,
                StateFn::Done => break,
            }
        }
        Ok(())
    }
}

// ================================================================================================
// State Functions / Methods
// ================================================================================================

impl BuildCtx {
    #[tracing::instrument]
    fn start<'a>() -> NextState<'a, Self> {
        info!("Start building trade environent");
        next_async_fn(|ctx| Box::pin(async move { ctx.fetch_data().await }))
    }

    #[tracing::instrument(skip_all)]
    async fn fetch_data<'a>(&mut self) -> NextState<'a, Self> {
        tracing::info!("Starting parallel data ingestion");

        let years = self.env_cfg.allowed_years();
        let (ohlcv_spot, ohlcv_future, trade_spot, tpo_spot, tpo_future, vp_spot, news) = tokio::try_join!(
            fetch_groups(self.env_cfg.ohlcv_spot(), years.clone()),
            fetch_groups(self.env_cfg.ohlcv_future(), years.clone()),
            fetch_groups(self.env_cfg.trade_spot(), years.clone()),
            fetch_groups(self.env_cfg.tpo_spot(), years.clone()),
            fetch_groups(self.env_cfg.tpo_future(), years.clone()),
            fetch_groups(self.env_cfg.volume_profile_spot(), years.clone()),
            fetch_groups(self.env_cfg.economic_calendar(), years),
        )?;

        tracing::info!("Data ingestion complete. Storing raw data.");

        self.ohlcv_spot_map = Some(ohlcv_spot);
        self.ohlcv_future_map = Some(ohlcv_future);
        self.trade_spot_map = Some(trade_spot);
        self.tpo_spot_map = Some(tpo_spot);
        self.tpo_future_map = Some(tpo_future);
        self.vp_spot_map = Some(vp_spot);
        self.economic_calendar_map = Some(news);

        Ok(StateFn::Next(|ctx| ctx.compute_indicators()))
    }

    #[tracing::instrument(skip_all)]
    fn compute_indicators<'a>(&mut self) -> NextState<'a, Self> {
        tracing::info!("Computing derived technical indicators");

        // 1. Initialize Indicator Maps
        let mut ema_map = HashMap::new();
        let mut sma_map = HashMap::new();
        let mut rsi_map = HashMap::new();

        let s = Schema::from_iter(vec![
            CanonicalCol::Timestamp.field(),
            CanonicalCol::Price.field(),
        ]);
        let schema = Arc::new(s);

        // 2. Define a helper to process indicators for a specific parent LazyFrame
        let mut process_indicators = |parent_id: OhlcvId,
                                      source_lf: LazyFrame,
                                      indicators: &[TechnicalIndicator]|
         -> ChapatyResult<()> {
            for &ind in indicators {
                let lf_result = compute_indicator(source_lf.clone(), ind)?;

                match ind {
                    TechnicalIndicator::Ema(EmaWindow(w)) => {
                        let id = EmaId {
                            parent: parent_id,
                            length: EmaWindow(w),
                        };
                        ema_map.insert(id, (schema.clone(), lf_result));
                    }
                    TechnicalIndicator::Sma(SmaWindow(w)) => {
                        let id = SmaId {
                            parent: parent_id,
                            length: SmaWindow(w),
                        };
                        sma_map.insert(id, (schema.clone(), lf_result));
                    }
                    TechnicalIndicator::Rsi(RsiWindow(w)) => {
                        let id = RsiId {
                            parent: parent_id,
                            length: RsiWindow(w),
                        };
                        rsi_map.insert(id, (schema.clone(), lf_result));
                    }
                }
            }
            Ok(())
        };

        // 3. Process Spot Markets
        if let Some(spot_map) = &self.ohlcv_spot_map {
            for group in self.env_cfg.ohlcv_spot() {
                for config in &group.items {
                    if config.indicators.is_empty() {
                        continue;
                    }

                    let parent_id = config.to_id()?;
                    if let Some((_, lf)) = spot_map.get(&parent_id) {
                        process_indicators(parent_id, lf.clone(), &config.indicators)?;
                    }
                }
            }
        }

        // 4. Process Futures Markets
        if let Some(future_map) = &self.ohlcv_future_map {
            for group in self.env_cfg.ohlcv_future() {
                for config in &group.items {
                    if config.indicators.is_empty() {
                        continue;
                    }

                    let parent_id = config.to_id()?;
                    if let Some((_, lf)) = future_map.get(&parent_id) {
                        process_indicators(parent_id, lf.clone(), &config.indicators)?;
                    }
                }
            }
        }

        // 5. Store Results in Context
        self.ema_map = Some(ema_map);
        self.sma_map = Some(sma_map);
        self.rsi_map = Some(rsi_map);

        Ok(StateFn::Next(|ctx: &mut BuildCtx| {
            ctx.overlay_economic_calendar_policy()
        }))
    }

    #[tracing::instrument(skip_all)]
    fn overlay_economic_calendar_policy<'a>(&mut self) -> NextState<'a, Self> {
        tracing::info!("Applying economic calendar policy to market data");

        let active_policy = self
            .env_cfg
            .filter_config()
            .as_ref()
            .and_then(|cfg| cfg.economic_news_policy)
            .filter(|p| !p.is_unrestricted());

        let Some(policy) = active_policy else {
            tracing::info!(
                "Policy is Unrestricted or undefined. Skipping economic calendar overlay."
            );
            return Ok(StateFn::Next(|ctx| ctx.filter_markets_by_trading_window()));
        };

        // Handle Edge Case: No Calendar Data
        // If the policy excludes events (ExcludeEvents) and we have none, we keep all data.
        // If the policy requires events (OnlyWithEvents) but we have none, we must clear all data.
        let is_empty = self
            .economic_calendar_map
            .as_ref()
            .is_none_or(|m| m.is_empty());
        if is_empty {
            if policy.is_only_with_events() {
                tracing::warn!(
                    "Policy is OnlyWithEvents but no economic data found. Clearing all market data."
                );
                self.ohlcv_spot_map = None;
                self.ohlcv_future_map = None;
                self.trade_spot_map = None;
                self.tpo_spot_map = None;
                self.tpo_future_map = None;
                self.vp_spot_map = None;
            }
            return Ok(StateFn::Next(|ctx| ctx.filter_markets_by_trading_window()));
        }

        tracing::info!("Applying economic calendar policy: {:?}", policy);

        // Create Master Calendar: Union of all events, projected to minimum schema (Timestamp, Category)
        let master_calendar_lf = {
            let map = self.economic_calendar_map.as_ref().unwrap();

            let lfs = map
                .values()
                .map(|(_, lf)| {
                    // Strictly select only what the overlay logic needs
                    lf.clone()
                        .select([col(CanonicalCol::Timestamp), col(CanonicalCol::Category)])
                })
                .collect::<Vec<LazyFrame>>();

            polars::prelude::concat(
                lfs,
                UnionArgs {
                    parallel: true,
                    rechunk: true,
                    ..Default::default()
                },
            )
            .map_err(|e| DataError::DataFrame(format!("Failed to concat calendars: {e}")))?
            .unique(None, UniqueKeepStrategy::default())
        };

        let sim_timeframe = self.env_cfg.episode_length();

        if let Some(map) = self.ohlcv_spot_map.as_mut() {
            apply_overlay(map, &master_calendar_lf, sim_timeframe, policy);
        }
        if let Some(map) = self.ohlcv_future_map.as_mut() {
            apply_overlay(map, &master_calendar_lf, sim_timeframe, policy);
        }
        if let Some(map) = self.trade_spot_map.as_mut() {
            apply_overlay(map, &master_calendar_lf, sim_timeframe, policy);
        }
        if let Some(map) = self.tpo_spot_map.as_mut() {
            apply_overlay(map, &master_calendar_lf, sim_timeframe, policy);
        }
        if let Some(map) = self.tpo_future_map.as_mut() {
            apply_overlay(map, &master_calendar_lf, sim_timeframe, policy);
        }
        if let Some(map) = self.vp_spot_map.as_mut() {
            apply_overlay(map, &master_calendar_lf, sim_timeframe, policy);
        }

        tracing::info!("Economic calendar policy applied successfully");

        Ok(StateFn::Next(|ctx| ctx.filter_markets_by_trading_window()))
    }

    #[tracing::instrument(skip_all)]
    fn filter_markets_by_trading_window<'a>(&mut self) -> NextState<'a, Self> {
        let Some(allowed_hours_map) = self
            .env_cfg
            .filter_config()
            .as_ref()
            .and_then(|cfg| cfg.allowed_trading_hours.as_ref())
        else {
            tracing::info!("No trading hour restrictions defined. Skipping filter.");
            return Ok(StateFn::Next(|ctx| ctx.sort_all_data()));
        };

        if allowed_hours_map.is_empty() {
            tracing::warn!(
                "Trading hours filter is active but empty: ALL data will be filtered out."
            );
        } else {
            tracing::info!("Filtering markets by allowed trading hours");
        }

        tracing::info!("Filtering markets by allowed trading hours");

        if let Some(map) = self.ohlcv_spot_map.as_mut() {
            apply_filter(map, allowed_hours_map);
        }
        if let Some(map) = self.ohlcv_future_map.as_mut() {
            apply_filter(map, allowed_hours_map);
        }
        if let Some(map) = self.trade_spot_map.as_mut() {
            apply_filter(map, allowed_hours_map);
        }
        if let Some(map) = self.tpo_spot_map.as_mut() {
            apply_filter(map, allowed_hours_map);
        }
        if let Some(map) = self.tpo_future_map.as_mut() {
            apply_filter(map, allowed_hours_map);
        }
        if let Some(map) = self.vp_spot_map.as_mut() {
            apply_filter(map, allowed_hours_map);
        }

        tracing::info!("Trading hours filter applied successfully");
        Ok(StateFn::Next(|ctx| ctx.sort_all_data()))
    }

    #[tracing::instrument(skip_all)]
    fn sort_all_data<'a>(&mut self) -> NextState<'a, Self> {
        tracing::info!("Finalizing data order: Sorting all datasets by timestamp");

        if let Some(map) = self.ohlcv_spot_map.as_mut() {
            apply_sort(map)?;
        }
        if let Some(map) = self.ohlcv_future_map.as_mut() {
            apply_sort(map)?;
        }
        if let Some(map) = self.trade_spot_map.as_mut() {
            apply_sort(map)?;
        }
        if let Some(map) = self.tpo_spot_map.as_mut() {
            apply_sort(map)?;
        }
        if let Some(map) = self.tpo_future_map.as_mut() {
            apply_sort(map)?;
        }
        if let Some(map) = self.vp_spot_map.as_mut() {
            apply_sort(map)?;
        }

        if let Some(map) = self.economic_calendar_map.as_mut() {
            apply_sort(map)?;
        }

        tracing::info!("Sorting applied successfully");
        Ok(StateFn::Next(|ctx| ctx.finish()))
    }

    #[tracing::instrument(skip_all)]
    fn finish<'a>(&mut self) -> NextState<'a, Self> {
        info!("Starting environment finalization");

        let mut ohlcv_res: ChapatyResult<SortedVecMap<OhlcvId, Box<[Ohlcv]>>> =
            Ok(SortedVecMap::new());
        let mut trade_res: ChapatyResult<SortedVecMap<TradesId, Box<[Trade]>>> =
            Ok(SortedVecMap::new());
        let mut tpo_res: ChapatyResult<SortedVecMap<TpoId, Box<[Tpo]>>> = Ok(SortedVecMap::new());
        let mut vp_res: ChapatyResult<SortedVecMap<VolumeProfileId, Box<[VolumeProfile]>>> =
            Ok(SortedVecMap::new());
        let mut cal_res: ChapatyResult<SortedVecMap<EconomicCalendarId, Box<[EconomicEvent]>>> =
            Ok(SortedVecMap::new());
        let mut ema_res: ChapatyResult<SortedVecMap<EmaId, Box<[Ema]>>> = Ok(SortedVecMap::new());
        let mut rsi_res: ChapatyResult<SortedVecMap<RsiId, Box<[Rsi]>>> = Ok(SortedVecMap::new());
        let mut sma_res: ChapatyResult<SortedVecMap<SmaId, Box<[Sma]>>> = Ok(SortedVecMap::new());

        debug!("Spawning parallel data extraction tasks");
        rayon::scope(|s| {
            // === OHLCV (Merge Spot + Future) ===
            s.spawn(|_| {
                debug!("Processing OHLCV data (spot + future)");
                let spot = process_map(self.ohlcv_spot_map.as_ref(), |df, _id| extract_ohlcv(df));
                let fut = process_map(self.ohlcv_future_map.as_ref(), |df, _id| extract_ohlcv(df));
                ohlcv_res = match (spot, fut) {
                    (Ok(s), Ok(f)) => {
                        let merged = s.merge(f);
                        info!("OHLCV: extracted {} streams", merged.len());
                        Ok(merged)
                    }
                    (Err(e), _) | (_, Err(e)) => Err(e),
                };
            });

            // === TPO (Merge Spot + Future) ===
            s.spawn(|_| {
                debug!("Processing TPO data (spot + future)");
                let spot = process_map(self.tpo_spot_map.as_ref(), |df, id| {
                    extract_tpo(df, &id.aggregation)
                });
                let fut = process_map(self.tpo_future_map.as_ref(), |df, id| {
                    extract_tpo(df, &id.aggregation)
                });
                tpo_res = match (spot, fut) {
                    (Ok(s), Ok(f)) => {
                        let merged = s.merge(f);
                        info!("TPO: extracted {} streams", merged.len());
                        Ok(merged)
                    }
                    (Err(e), _) | (_, Err(e)) => Err(e),
                };
            });

            // === Trades ===
            s.spawn(|_| {
                debug!("Processing Trade data");
                trade_res = process_map(self.trade_spot_map.as_ref(), |df, _id| extract_trade(df));
                if let Ok(ref map) = trade_res {
                    info!("Trade: extracted {} streams", map.len());
                }
            });

            // === Volume Profile ===
            s.spawn(|_| {
                debug!("Processing Volume Profile data");
                vp_res = process_map(self.vp_spot_map.as_ref(), |df, id| {
                    extract_vp(df, &id.aggregation)
                });
                if let Ok(ref map) = vp_res {
                    info!("Volume Profile: extracted {} streams", map.len());
                }
            });

            // === Economic Calendar ===
            s.spawn(|_| {
                debug!("Processing Economic Calendar data");
                cal_res = process_map(self.economic_calendar_map.as_ref(), |df, _id| {
                    extract_economic(df)
                });
                if let Ok(ref map) = cal_res {
                    info!("Economic: extracted {} streams", map.len());
                }
            });

            // === EMA ===
            s.spawn(|_| {
                debug!("Processing EMA indicators");
                ema_res = process_map(self.ema_map.as_ref(), |df, _id| extract_ema(df));
                if let Ok(ref map) = ema_res {
                    info!("EMA: extracted {} streams", map.len());
                }
            });

            // === RSI ===
            s.spawn(|_| {
                debug!("Processing RSI indicators");
                rsi_res = process_map(self.rsi_map.as_ref(), |df, _id| extract_rsi(df));
                if let Ok(ref map) = rsi_res {
                    info!("RSI: extracted {} streams", map.len());
                }
            });

            // === SMA ===
            s.spawn(|_| {
                debug!("Processing SMA indicators");
                sma_res = process_map(self.sma_map.as_ref(), |df, _id| extract_sma(df));
                if let Ok(ref map) = sma_res {
                    info!("SMA: extracted {} streams", map.len());
                }
            });
        });

        debug!("All extraction tasks completed, building SimulationData");

        // Construct Environment
        let ohlcv = ohlcv_res?;
        let trade = trade_res?;
        let trade_hint = self.env_cfg.trade_hint();
        let sim_data = Arc::new(
            SimulationDataBuilder::new()
                .with_ohlcv(ohlcv)
                .with_trade(trade)
                .with_economic_news(cal_res?)
                .with_volume_profile(vp_res?)
                .with_tpo(tpo_res?)
                .with_ema(ema_res?)
                .with_rsi(rsi_res?)
                .with_sma(sma_res?)
                .build(self.env_cfg.clone())?,
        );
        let initial_states = States::with_capacity(&sim_data.market_ids(), trade_hint);

        info!("SimulationData built successfully");

        let ep_capacity = self.env_cfg.max_episode_capacity();
        let ep_log = Ledger::with_capacity(ep_capacity, initial_states);

        let ep_len = self.env_cfg.episode_length();
        let episode = EpisodeBuilder::new()
            .with_start(sim_data.global_open_start())
            .with_length(ep_len)
            .build()?;

        debug!("Building final Environment");
        let env = Environment::new(CursorGroup::new(&sim_data)?, sim_data, ep_log, episode)
            .with_invalid_action_penalty(self.env_cfg.invalid_action_penalty())
            .with_execution_bias(ExecutionBias::default())
            .with_risk_metrics_cfg(self.env_cfg.risk_metrics_cfg());

        self.final_env = Some(env);
        info!("Environment finalization complete");
        Ok(StateFn::Done)
    }
}

// ================================================================================================
// Helper Functions
// ================================================================================================
fn next_async_fn<'a, F>(f: F) -> ChapatyResult<StateFn<'a, BuildCtx>>
where
    F: for<'ctx> FnOnce(
            &'ctx mut BuildCtx,
        ) -> Pin<
            Box<dyn Future<Output = ChapatyResult<StateFn<'a, BuildCtx>>> + Send + 'ctx>,
        > + Send
        + 'a,
{
    Ok(StateFn::NextAsync(Box::new(f)))
}

/// Generic helper to fetch data from a list of SourceGroups.
async fn fetch_groups<T: Fetchable>(
    groups: &[SourceGroup<T>],
    years: Vec<u16>,
) -> ChapatyResult<HashMap<T::Id, (SchemaRef, LazyFrame)>> {
    let total_items: usize = groups.iter().map(|g| g.items.len()).sum();
    let mut aggregated_map = HashMap::with_capacity(total_items);

    for group in groups {
        let mut client = group.source.connect().await?;
        let batch_map = load_batch(&mut client, group.items.clone(), years.clone()).await?;
        aggregated_map.extend(batch_map);
    }

    Ok(aggregated_map)
}

fn compute_indicator(lf: LazyFrame, indicator: TechnicalIndicator) -> ChapatyResult<LazyFrame> {
    match indicator {
        TechnicalIndicator::Ema(w) => w.pre_compute_ema(lf),
        TechnicalIndicator::Sma(w) => w.pre_compute_sma(lf),
        TechnicalIndicator::Rsi(w) => w.pre_compute_rsi(lf),
    }
}

fn apply_overlay<T>(
    map: &mut HashMap<T, (SchemaRef, LazyFrame)>,
    news_lf: &LazyFrame,
    sim_timeframe: EpisodeLength,
    policy: EconomicCalendarPolicy,
) {
    for (_id, (_schema, lf)) in map.iter_mut() {
        let new_lf =
            lf.clone()
                .join_with_economic_calendar_overlay(news_lf.clone(), sim_timeframe, policy);
        *lf = new_lf;
    }
}

fn apply_filter<T>(
    map: &mut HashMap<T, (SchemaRef, LazyFrame)>,
    allowed_windows: &BTreeMap<Weekday, Vec<TradingWindow>>,
) {
    if map.is_empty() {
        return;
    }

    // Build predicate once inside this function
    let predicate = {
        let ts_col = col(CanonicalCol::Timestamp);
        let wd = ts_col.clone().dt().weekday();
        let hr = ts_col.dt().hour();

        let mut conditions = Vec::with_capacity(allowed_windows.len());

        for (day, windows) in allowed_windows {
            let weekday: chrono::Weekday = (*day).into();
            let day_num = weekday.number_from_monday();

            for window in windows {
                // Condition: (Weekday == Day) AND (Start <= Hour < End)
                let cond = wd
                    .clone()
                    .eq(lit(day_num))
                    .and(hr.clone().gt_eq(lit(window.start())))
                    .and(hr.clone().lt(lit(window.end())));

                conditions.push(cond);
            }
        }

        // Combine with OR: (Win1) OR (Win2)...
        // If 'conditions' is empty (empty map), this results in 'lit(false)', filtering all rows.
        conditions
            .into_iter()
            .reduce(|acc, expr| acc.or(expr))
            .unwrap_or(lit(false))
    };

    // Apply filter to all LazyFrames
    for (_id, (_schema, lf)) in map.iter_mut() {
        *lf = lf.clone().filter(predicate.clone());
    }
}

fn apply_sort<T>(map: &mut HashMap<T, (SchemaRef, LazyFrame)>) -> ChapatyResult<()> {
    for (_id, (_schema, lf)) in map.iter_mut() {
        *lf = lf.clone().sort(
            [CanonicalCol::Timestamp],
            SortMultipleOptions::default().with_maintain_order(false),
        );
    }
    Ok(())
}

/// Generic helper to materialize and transform a map of LazyFrames.
///
/// Short-circuits early if the map is `None` or empty.
#[tracing::instrument(skip_all)]
fn process_map<Id, Event, F>(
    map: Option<&HashMap<Id, (SchemaRef, LazyFrame)>>,
    extractor: F,
) -> ChapatyResult<SortedVecMap<Id, Box<[Event]>>>
where
    Id: StreamId + Hash + Send + Sync,
    Event: Send,
    F: Fn(DataFrame, &Id) -> ChapatyResult<Box<[Event]>> + Sync + Send,
{
    // Early return: No data to process
    let Some(map) = map else {
        debug!("No data map provided, returning empty");
        return Ok(SortedVecMap::new());
    };

    // Early return: Empty map
    if map.is_empty() {
        debug!("Empty data map, returning empty");
        return Ok(SortedVecMap::new());
    }

    debug!("Processing {} dataframes in parallel", map.len());

    let data = map
        .par_iter()
        .map(|(id, (_schema, lf))| {
            let df = lf.clone().collect().map_err(|e| {
                warn!("Failed to collect dataframe for {:?}: {}", id, e);
                DataError::DataFrame(format!("Failed to collect {id:?}: {e}"))
            })?;
            debug!("Collected dataframe for {:?}: {} rows", id, df.height());

            let events = extractor(df, id)?;
            debug!("Extracted {} events for {:?}", events.len(), id);

            Ok((*id, events))
        })
        .collect::<ChapatyResult<HashMap<Id, Box<[Event]>>>>()?;

    // SortedVecMap handles sorting internally
    Ok(data.into())
}

// ================================================================================================
// Extractor Functions
// ================================================================================================

fn extract_ohlcv(df: DataFrame) -> ChapatyResult<Box<[Ohlcv]>> {
    let len = df.height();
    if len == 0 {
        return Ok(Box::new([]));
    }

    // Required fields
    let open_dt_logical = df.dt_logical(CanonicalCol::OpenTimestamp)?;
    let open_ts_ca = open_dt_logical.physical();
    let ts_dt_locial = df.dt_logical(CanonicalCol::Timestamp)?;
    let ts_ca = ts_dt_locial.physical();
    let open_ca = df.f64_ca(CanonicalCol::Open)?;
    let high_ca = df.f64_ca(CanonicalCol::High)?;
    let low_ca = df.f64_ca(CanonicalCol::Low)?;
    let close_ca = df.f64_ca(CanonicalCol::Close)?;
    let vol_ca = df.f64_ca(CanonicalCol::Volume)?;

    // Optional numeric fields with iterators
    let qav_iter: Box<dyn Iterator<Item = Option<f64>>> = df
        .f64_ca(CanonicalCol::QuoteAssetVolume)
        .map(|ca| Box::new(ca.into_iter()) as Box<dyn Iterator<Item = Option<f64>>>)
        .unwrap_or_else(|_| Box::new(std::iter::repeat_n(None, len)));

    let nt_iter: Box<dyn Iterator<Item = Option<i64>>> = df
        .i64_ca(CanonicalCol::NumberOfTrades)
        .map(|ca| Box::new(ca.into_iter()) as Box<dyn Iterator<Item = Option<i64>>>)
        .unwrap_or_else(|_| Box::new(std::iter::repeat_n(None, len)));

    let tbbav_iter: Box<dyn Iterator<Item = Option<f64>>> = df
        .f64_ca(CanonicalCol::TakerBuyBaseAssetVolume)
        .map(|ca| Box::new(ca.into_iter()) as Box<dyn Iterator<Item = Option<f64>>>)
        .unwrap_or_else(|_| Box::new(std::iter::repeat_n(None, len)));

    let tbqav_iter: Box<dyn Iterator<Item = Option<f64>>> = df
        .f64_ca(CanonicalCol::TakerBuyQuoteAssetVolume)
        .map(|ca| Box::new(ca.into_iter()) as Box<dyn Iterator<Item = Option<f64>>>)
        .unwrap_or_else(|_| Box::new(std::iter::repeat_n(None, len)));

    let mut events = Vec::with_capacity(len);

    for (o_ts, ts, o, h, l, c, v, qav, nt, tbbav, tbqav) in izip!(
        open_ts_ca.into_iter(),
        ts_ca.into_iter(),
        open_ca.into_iter(),
        high_ca.into_iter(),
        low_ca.into_iter(),
        close_ca.into_iter(),
        vol_ca.into_iter(),
        qav_iter,
        nt_iter,
        tbbav_iter,
        tbqav_iter
    ) {
        let open_ts_val = o_ts.ok_or(DataError::DataFrame("Missing OpenTimestamp".into()))?;
        let open_timestamp = DateTime::<Utc>::from_timestamp_micros(open_ts_val).ok_or_else(|| {
            DataError::TimestampConversion(format!(
                "Failed to convert OpenTimestamp ({open_ts_val}) from microseconds to UTC DateTime"
            ))
        })?;
        let ts_val = ts.ok_or(DataError::DataFrame("Missing Timestamp".into()))?;
        let timestamp = DateTime::<Utc>::from_timestamp_micros(ts_val).ok_or_else(|| {
            DataError::TimestampConversion(format!(
                "Failed to convert Timestamp ({ts_val}) from microseconds to UTC DateTime"
            ))
        })?;
        let open_val = o.ok_or(DataError::DataFrame("Missing Open".into()))?;
        let high_val = h.ok_or(DataError::DataFrame("Missing High".into()))?;
        let low_val = l.ok_or(DataError::DataFrame("Missing Low".into()))?;
        let close_val = c.ok_or(DataError::DataFrame("Missing Close".into()))?;
        let vol_val = v.ok_or(DataError::DataFrame("Missing Volume".into()))?;

        events.push(Ohlcv {
            // === Required ===
            open_timestamp,
            close_timestamp: timestamp,
            open: Price(open_val),
            high: Price(high_val),
            low: Price(low_val),
            close: Price(close_val),
            volume: Quantity(vol_val),

            // === Optionals ===
            quote_asset_volume: qav.map(Quantity),
            number_of_trades: nt.map(Count),
            taker_buy_base_asset_volume: tbbav.map(Quantity),
            taker_buy_quote_asset_volume: tbqav.map(Quantity),
        });
    }

    Ok(events.into_boxed_slice())
}

fn extract_trade(df: DataFrame) -> ChapatyResult<Box<[Trade]>> {
    let len = df.height();
    if len == 0 {
        return Ok(Box::new([]));
    }

    // Required fields
    let dt_logical = df.dt_logical(CanonicalCol::Timestamp)?;
    let ts_ca = dt_logical.physical();
    let price_ca = df.f64_ca(CanonicalCol::Price)?;
    let vol_ca = df.f64_ca(CanonicalCol::Volume)?;

    // Optional numeric fields with iterators
    let trade_id_iter: Box<dyn Iterator<Item = Option<i64>>> = df
        .i64_ca(CanonicalCol::TradeId)
        .map(|ca| Box::new(ca.into_iter()) as Box<dyn Iterator<Item = Option<i64>>>)
        .unwrap_or_else(|_| Box::new(std::iter::repeat_n(None, len)));

    let quote_vol_iter: Box<dyn Iterator<Item = Option<f64>>> = df
        .f64_ca(CanonicalCol::QuoteAssetVolume)
        .map(|ca| Box::new(ca.into_iter()) as Box<dyn Iterator<Item = Option<f64>>>)
        .unwrap_or_else(|_| Box::new(std::iter::repeat_n(None, len)));

    let is_maker_iter: Box<dyn Iterator<Item = Option<bool>>> = df
        .bool_ca(CanonicalCol::IsBuyerMaker)
        .map(|ca| Box::new(ca.into_iter()) as Box<dyn Iterator<Item = Option<bool>>>)
        .unwrap_or_else(|_| Box::new(std::iter::repeat_n(None, len)));

    let is_best_iter: Box<dyn Iterator<Item = Option<bool>>> = df
        .bool_ca(CanonicalCol::IsBestMatch)
        .map(|ca| Box::new(ca.into_iter()) as Box<dyn Iterator<Item = Option<bool>>>)
        .unwrap_or_else(|_| Box::new(std::iter::repeat_n(None, len)));

    let mut events = Vec::with_capacity(len);

    for (ts, price, vol, trade_id, quote_vol, is_maker, is_best) in izip!(
        ts_ca.into_iter(),
        price_ca.into_iter(),
        vol_ca.into_iter(),
        trade_id_iter,
        quote_vol_iter,
        is_maker_iter,
        is_best_iter
    ) {
        let ts_val = ts.ok_or(DataError::DataFrame("Missing Timestamp".into()))?;
        let timestamp = DateTime::<Utc>::from_timestamp_micros(ts_val).ok_or_else(|| {
            DataError::TimestampConversion(format!(
                "Failed to convert Timestamp ({ts_val}) from microseconds to UTC DateTime"
            ))
        })?;
        let price_val = price.ok_or(DataError::DataFrame("Missing Price".into()))?;
        let vol_val = vol.ok_or(DataError::DataFrame("Missing Volume".into()))?;

        events.push(Trade {
            timestamp,
            price: Price(price_val),
            quantity: Quantity(vol_val),
            trade_id: trade_id.map(TradeId),
            quote_asset_volume: quote_vol.map(Quantity),
            is_buyer_maker: is_maker.map(LiquiditySide::from),
            is_best_match: is_best.map(ExecutionDepth::from),
        });
    }

    Ok(events.into_boxed_slice())
}

fn extract_economic(df: DataFrame) -> ChapatyResult<Box<[EconomicEvent]>> {
    let len = df.height();
    if len == 0 {
        return Ok(Box::new([]));
    }

    // Required fields
    let dt_logical = df.dt_logical(CanonicalCol::Timestamp)?;
    let ts_ca = dt_logical.physical();
    let source_ca = df.str_ca(CanonicalCol::DataSource)?;
    let cat_ca = df.str_ca(CanonicalCol::Category)?;
    let name_ca = df.str_ca(CanonicalCol::NewsName)?;
    let country_ca = df.str_ca(CanonicalCol::CountryCode)?;
    let currency_ca = df.str_ca(CanonicalCol::CurrencyCode)?;
    let impact_ca = df.i64_ca(CanonicalCol::EconomicImpact)?;

    // Optional string fields with iterators
    let news_type_iter: Box<dyn Iterator<Item = Option<&str>>> = df
        .str_ca(CanonicalCol::NewsType)
        .map(|ca| Box::new(ca.into_iter()) as Box<dyn Iterator<Item = Option<&str>>>)
        .unwrap_or_else(|_| Box::new(std::iter::repeat_n(None, len)));

    let news_src_iter: Box<dyn Iterator<Item = Option<&str>>> = df
        .str_ca(CanonicalCol::NewsTypeSource)
        .map(|ca| Box::new(ca.into_iter()) as Box<dyn Iterator<Item = Option<&str>>>)
        .unwrap_or_else(|_| Box::new(std::iter::repeat_n(None, len)));

    let period_iter: Box<dyn Iterator<Item = Option<&str>>> = df
        .str_ca(CanonicalCol::Period)
        .map(|ca| Box::new(ca.into_iter()) as Box<dyn Iterator<Item = Option<&str>>>)
        .unwrap_or_else(|_| Box::new(std::iter::repeat_n(None, len)));

    // Optional numeric fields with iterators
    let conf_iter: Box<dyn Iterator<Item = Option<f64>>> = df
        .f64_ca(CanonicalCol::NewsTypeConfidence)
        .map(|ca| Box::new(ca.into_iter()) as Box<dyn Iterator<Item = Option<f64>>>)
        .unwrap_or_else(|_| Box::new(std::iter::repeat_n(None, len)));

    let actual_iter: Box<dyn Iterator<Item = Option<f64>>> = df
        .f64_ca(CanonicalCol::Actual)
        .map(|ca| Box::new(ca.into_iter()) as Box<dyn Iterator<Item = Option<f64>>>)
        .unwrap_or_else(|_| Box::new(std::iter::repeat_n(None, len)));

    let forecast_iter: Box<dyn Iterator<Item = Option<f64>>> = df
        .f64_ca(CanonicalCol::Forecast)
        .map(|ca| Box::new(ca.into_iter()) as Box<dyn Iterator<Item = Option<f64>>>)
        .unwrap_or_else(|_| Box::new(std::iter::repeat_n(None, len)));

    let prev_iter: Box<dyn Iterator<Item = Option<f64>>> = df
        .f64_ca(CanonicalCol::Previous)
        .map(|ca| Box::new(ca.into_iter()) as Box<dyn Iterator<Item = Option<f64>>>)
        .unwrap_or_else(|_| Box::new(std::iter::repeat_n(None, len)));

    let mut events = Vec::with_capacity(len);

    for (
        ts,
        source,
        cat,
        name,
        country,
        currency,
        impact,
        news_type,
        news_src,
        period,
        conf,
        actual,
        forecast,
        prev,
    ) in izip!(
        ts_ca.into_iter(),
        source_ca.into_iter(),
        cat_ca.into_iter(),
        name_ca.into_iter(),
        country_ca.into_iter(),
        currency_ca.into_iter(),
        impact_ca.into_iter(),
        news_type_iter,
        news_src_iter,
        period_iter,
        conf_iter,
        actual_iter,
        forecast_iter,
        prev_iter
    ) {
        let ts_val = ts.ok_or(DataError::DataFrame("Missing Timestamp".into()))?;
        let timestamp = DateTime::<Utc>::from_timestamp_micros(ts_val).ok_or_else(|| {
            DataError::TimestampConversion(format!(
                "Failed to convert Timestamp ({ts_val}) from microseconds to UTC DateTime"
            ))
        })?;

        let source_val = source.ok_or(DataError::DataFrame("Missing DataSource".into()))?;
        let cat_val = cat.ok_or(DataError::DataFrame("Missing Category".into()))?;
        let name_val = name.ok_or(DataError::DataFrame("Missing NewsName".into()))?;
        let country_val = country.ok_or(DataError::DataFrame("Missing CountryCode".into()))?;
        let currency_val = currency.ok_or(DataError::DataFrame("Missing CurrencyCode".into()))?;
        let impact_val = impact.ok_or(DataError::DataFrame("Missing EconomicImpact".into()))?;

        let country_code = std::str::FromStr::from_str(country_val).unwrap_or(CountryCode::Us);
        let economic_impact = match impact_val {
            3 => EconomicEventImpact::High,
            2 => EconomicEventImpact::Medium,
            _ => EconomicEventImpact::Low,
        };

        events.push(EconomicEvent {
            timestamp,
            data_source: source_val.to_string(),
            category: cat_val.to_string(),
            news_name: name_val.to_string(),
            country_code,
            currency_code: currency_val.to_string(),
            economic_impact,
            news_type: news_type.map(|s| s.to_string()),
            news_type_source: news_src.map(|s| s.to_string()),
            period: period.map(|s| s.to_string()),
            news_type_confidence: conf,
            actual: actual.map(EconomicValue),
            forecast: forecast.map(EconomicValue),
            previous: prev.map(EconomicValue),
        });
    }

    Ok(events.into_boxed_slice())
}

fn extract_tpo(df: DataFrame, cfg: &ProfileAggregation) -> ChapatyResult<Box<[Tpo]>> {
    let len = df.height();
    if len == 0 {
        return Ok(Box::new([]));
    }

    let sorted_df = df
        .sort(
            [
                CanonicalCol::OpenTimestamp.as_str(), // 1. Group by Window
                CanonicalCol::PriceBinStart.as_str(), // 2. Order bins Low to High
            ],
            SortMultipleOptions::default(),
        )
        .map_err(|e| ChapatyError::Data(DataError::DataFrame(e.to_string())))?;

    let open_dt_logical = sorted_df.dt_logical(CanonicalCol::OpenTimestamp)?;
    let ts_open_ca = open_dt_logical.physical();
    let ts_dt_logical = sorted_df.dt_logical(CanonicalCol::Timestamp)?;
    let ts_close_ca = ts_dt_logical.physical();
    let p_start_ca = sorted_df.f64_ca(CanonicalCol::PriceBinStart)?;
    let p_end_ca = sorted_df.f64_ca(CanonicalCol::PriceBinEnd)?;
    let count_ca = sorted_df.i64_ca(CanonicalCol::TimeSlotCount)?;

    let mut profiles = Vec::new();
    let mut current_bins = Vec::new();
    let mut current_window_start: Option<i64> = None;
    let mut current_window_end: Option<i64> = None;

    let va_pct = cfg.value_area_pct();
    let poc_rule = cfg.poc_rule.unwrap_or_default();
    let va_rule = cfg.value_area_rule.unwrap_or_default();

    for (ts_open, ts_close, p_start, p_end, count) in izip!(
        ts_open_ca.into_iter(),
        ts_close_ca.into_iter(),
        p_start_ca.into_iter(),
        p_end_ca.into_iter(),
        count_ca.into_iter()
    ) {
        let ts_open_val =
            ts_open.ok_or(DataError::DataFrame("Missing TPO Open Timestamp".into()))?;
        let ts_val = ts_close.ok_or(DataError::DataFrame("Missing TPO Timestamp".into()))?;

        if Some(ts_open_val) != current_window_start {
            if !current_bins.is_empty()
                && let (Some(start), Some(end)) = (current_window_start, current_window_end)
            {
                let stats = compute_profile_stats(&current_bins, va_pct, poc_rule, va_rule)?;

                profiles.push(Tpo {
                    open_timestamp: DateTime::from_timestamp_micros(start).unwrap(),
                    close_timestamp: DateTime::from_timestamp_micros(end).unwrap(),
                    poc: stats.poc,
                    value_area_high: stats.value_area_high,
                    value_area_low: stats.value_area_low,
                    bins: current_bins.into_boxed_slice(),
                });
            }
            current_window_start = Some(ts_open_val);
            current_window_end = Some(ts_val);
            current_bins = Vec::new();
        }

        let count_val = count.ok_or(DataError::DataFrame("Missing TPO Count".into()))?;
        current_bins.push(TpoBin {
            price_bin_start: Price(
                p_start.ok_or(DataError::DataFrame("Missing TPO Price Start".into()))?,
            ),
            price_bin_end: Price(
                p_end.ok_or(DataError::DataFrame("Missing TPO Price End".into()))?,
            ),
            time_slot_count: Count(count_val),
        });
    }

    if !current_bins.is_empty()
        && let (Some(start), Some(end)) = (current_window_start, current_window_end)
    {
        let stats = compute_profile_stats(&current_bins, va_pct, poc_rule, va_rule)?;
        profiles.push(Tpo {
            open_timestamp: DateTime::from_timestamp_micros(start).unwrap(),
            close_timestamp: DateTime::from_timestamp_micros(end).unwrap(),
            poc: stats.poc,
            value_area_high: stats.value_area_high,
            value_area_low: stats.value_area_low,
            bins: current_bins.into_boxed_slice(),
        });
    }

    Ok(profiles.into_boxed_slice())
}

fn extract_vp(df: DataFrame, cfg: &ProfileAggregation) -> ChapatyResult<Box<[VolumeProfile]>> {
    let len = df.height();
    if len == 0 {
        return Ok(Box::new([]));
    }

    let sorted_df = df
        .sort(
            [
                CanonicalCol::OpenTimestamp.as_str(), // 1. Group by Window
                CanonicalCol::PriceBinStart.as_str(), // 2. Order bins Low to High
            ],
            SortMultipleOptions::default(),
        )
        .map_err(|e| ChapatyError::Data(DataError::DataFrame(e.to_string())))?;

    let open_dt_logical = sorted_df.dt_logical(CanonicalCol::OpenTimestamp)?;
    let ts_open_ca = open_dt_logical.physical();
    let close_dt_logical = sorted_df.dt_logical(CanonicalCol::Timestamp)?;
    let ts_close_ca = close_dt_logical.physical();
    let p_start_ca = sorted_df.f64_ca(CanonicalCol::PriceBinStart)?;
    let p_end_ca = sorted_df.f64_ca(CanonicalCol::PriceBinEnd)?;
    let vol_ca = sorted_df.f64_ca(CanonicalCol::Volume)?;

    let get_opt_iter = |col: CanonicalCol| -> Box<dyn Iterator<Item = Option<f64>>> {
        if let Ok(ca) = sorted_df.f64_ca(col) {
            Box::new(ca.into_iter())
        } else {
            Box::new(std::iter::repeat_n(None, len))
        }
    };

    let get_cnt_iter = |col: CanonicalCol| -> Box<dyn Iterator<Item = Option<i64>>> {
        if let Ok(ca) = sorted_df.i64_ca(col) {
            Box::new(ca.into_iter())
        } else {
            Box::new(std::iter::repeat_n(None, len))
        }
    };

    let tb_base_iter = get_opt_iter(CanonicalCol::TakerBuyBaseAssetVolume);
    let ts_base_iter = get_opt_iter(CanonicalCol::TakerSellBaseAssetVolume);
    let q_vol_iter = get_opt_iter(CanonicalCol::QuoteAssetVolume);
    let tb_quote_iter = get_opt_iter(CanonicalCol::TakerBuyQuoteAssetVolume);
    let ts_quote_iter = get_opt_iter(CanonicalCol::TakerSellQuoteAssetVolume);
    let n_trd_iter = get_cnt_iter(CanonicalCol::NumberOfTrades);
    let n_buy_iter = get_cnt_iter(CanonicalCol::NumberOfBuyTrades);
    let n_sell_iter = get_cnt_iter(CanonicalCol::NumberOfSellTrades);

    let mut profiles = Vec::new();
    let mut current_bins = Vec::new();

    let mut current_window_start: Option<i64> = None;
    let mut current_window_end: Option<i64> = None;

    let va_pct = cfg.value_area_pct();
    let poc_rule = cfg.poc_rule.unwrap_or_default();
    let va_rule = cfg.value_area_rule.unwrap_or_default();

    for (
        ts_open,
        ts_close,
        p_start,
        p_end,
        vol,
        tb_base,
        ts_base,
        q_vol,
        tb_quote,
        ts_quote,
        n_trd,
        n_buy,
        n_sell,
    ) in izip!(
        ts_open_ca.into_iter(),
        ts_close_ca.into_iter(),
        p_start_ca.into_iter(),
        p_end_ca.into_iter(),
        vol_ca.into_iter(),
        tb_base_iter,
        ts_base_iter,
        q_vol_iter,
        tb_quote_iter,
        ts_quote_iter,
        n_trd_iter,
        n_buy_iter,
        n_sell_iter
    ) {
        let ts_open_val =
            ts_open.ok_or(DataError::DataFrame("Missing VP Open Timestamp".into()))?;
        let ts_val = ts_close.ok_or(DataError::DataFrame("Missing VP Timestamp".into()))?;

        if Some(ts_open_val) != current_window_start {
            if !current_bins.is_empty()
                && let (Some(start), Some(end)) = (current_window_start, current_window_end)
            {
                let stats = compute_profile_stats(&current_bins, va_pct, poc_rule, va_rule)?;
                profiles.push(VolumeProfile {
                    open_timestamp: DateTime::from_timestamp_micros(start).unwrap(),
                    close_timestamp: DateTime::from_timestamp_micros(end).unwrap(),
                    poc: stats.poc,
                    value_area_high: stats.value_area_high,
                    value_area_low: stats.value_area_low,
                    bins: current_bins.into_boxed_slice(),
                });
            }
            current_window_start = Some(ts_open_val);
            current_window_end = Some(ts_val);
            current_bins = Vec::new();
        }

        current_bins.push(VolumeProfileBin {
            price_bin_start: Price(
                p_start.ok_or(DataError::DataFrame("Missing VP Price Start".into()))?,
            ),
            price_bin_end: Price(p_end.ok_or(DataError::DataFrame("Missing VP Price End".into()))?),
            volume: vol
                .ok_or(DataError::DataFrame("Missing VP Volume".into()))
                .map(Quantity)?,
            taker_buy_base_asset_volume: tb_base.map(Quantity),
            taker_sell_base_asset_volume: ts_base.map(Quantity),
            quote_asset_volume: q_vol.map(Quantity),
            taker_buy_quote_asset_volume: tb_quote.map(Quantity),
            taker_sell_quote_asset_volume: ts_quote.map(Quantity),
            number_of_trades: n_trd.map(Count),
            number_of_buy_trades: n_buy.map(Count),
            number_of_sell_trades: n_sell.map(Count),
        });
    }

    if !current_bins.is_empty()
        && let (Some(start), Some(end)) = (current_window_start, current_window_end)
    {
        let stats = compute_profile_stats(&current_bins, va_pct, poc_rule, va_rule)?;
        profiles.push(VolumeProfile {
            open_timestamp: DateTime::from_timestamp_micros(start).unwrap(),
            close_timestamp: DateTime::from_timestamp_micros(end).unwrap(),
            poc: stats.poc,
            value_area_high: stats.value_area_high,
            value_area_low: stats.value_area_low,
            bins: current_bins.into_boxed_slice(),
        });
    }

    Ok(profiles.into_boxed_slice())
}

fn extract_ema(df: DataFrame) -> ChapatyResult<Box<[Ema]>> {
    extract_technical_indicator(df, |timestamp, price| Ema { timestamp, price })
}

fn extract_rsi(df: DataFrame) -> ChapatyResult<Box<[Rsi]>> {
    extract_technical_indicator(df, |timestamp, price| Rsi { timestamp, price })
}

fn extract_sma(df: DataFrame) -> ChapatyResult<Box<[Sma]>> {
    extract_technical_indicator(df, |timestamp, price| Sma { timestamp, price })
}

fn extract_technical_indicator<T, F>(df: DataFrame, constructor: F) -> ChapatyResult<Box<[T]>>
where
    F: Fn(DateTime<Utc>, Price) -> T,
{
    let len = df.height();
    if len == 0 {
        return Ok(Box::new([]));
    }

    let ts_dt_logical = df.dt_logical(CanonicalCol::Timestamp)?;
    let ts_ca = ts_dt_logical.physical();
    let price_ca = df.f64_ca(CanonicalCol::Price)?;

    let mut events = Vec::with_capacity(len);

    for (ts_opt, price_opt) in izip!(ts_ca.into_iter(), price_ca.into_iter()) {
        let ts_val = ts_opt.ok_or(DataError::DataFrame("Missing Timestamp".into()))?;
        let price_val = match price_opt {
            Some(v) => v,
            None => continue,
        };

        let timestamp = DateTime::<Utc>::from_timestamp_micros(ts_val).ok_or_else(|| {
            DataError::TimestampConversion(format!(
                "Failed to convert Timestamp ({ts_val}) from microseconds to UTC DateTime"
            ))
        })?;

        events.push(constructor(timestamp, Price(price_val)));
    }

    Ok(events.into_boxed_slice())
}

// ================================================================================================
// Extension Trait for Polars Logic
// ================================================================================================

trait LazyFrameCalendarExt {
    /// Enriches market data with economic calendar overlays and handles calendar-based filtering.
    ///
    /// This method joins market data with an economic calendar at minute resolution, propagates the
    /// context (`__is_on_calendar_event`) across the simulation timeframe (e.g., Day), and
    /// optionally filters the data based on the provided [`EconomicCalendarPolicy`].
    ///
    /// # Join Strategy: "Window-Based Semi-Join"
    ///
    /// 1. **Align:** Left-joins Market and Calendar on 1-minute buckets.
    /// 2. **Propagate:** If *any* minute in a Simulation Window (e.g., Day) has an economic calendar event,
    ///    marks the *entire* window as `__is_on_calendar_event = true`.
    /// 3. **Filter (Optional):**
    ///    - If `policy == OnlyWithEvents`, drops windows where `__is_on_calendar_event` is false (Inner Join behavior).
    ///    - If `policy == ExcludeEvents`, drops windows where `__is_on_calendar_event` is true (Anti Join behavior).
    ///
    /// # Returns
    /// `LazyFrame` with potentially filtered rows based on the policy.
    fn join_with_economic_calendar_overlay(
        self,
        calendar_lf: LazyFrame,
        sim_timeframe: EpisodeLength,
        policy: EconomicCalendarPolicy,
    ) -> LazyFrame;

    /// Adds a temporary grouping key based on the simulation episode length (e.g., Day, Week).
    fn with_simulation_window_key(
        self,
        ts_col: CanonicalCol,
        timeframe: EpisodeLength,
        alias: &str,
    ) -> LazyFrame;
}

impl LazyFrameCalendarExt for LazyFrame {
    fn join_with_economic_calendar_overlay(
        self,
        calendar_lf: LazyFrame,
        sim_timeframe: EpisodeLength,
        policy: EconomicCalendarPolicy,
    ) -> LazyFrame {
        // Defines the common key we will join on (e.g., the specific Day or Week)
        let join_key = "__sim_window_key";
        let is_on_calendar_event = "__is_on_calendar_event";

        // 1. Prepare Market: Key by Simulation Window
        let market_w_key =
            self.with_simulation_window_key(CanonicalCol::OpenTimestamp, sim_timeframe, join_key);

        // 2. Prepare Calendar: Key by Simulation Window + deduplicate
        let news_w_key = calendar_lf
            .with_simulation_window_key(CanonicalCol::Timestamp, sim_timeframe, join_key)
            .select([col(join_key)])
            .unique(None, UniqueKeepStrategy::Any) // prevent row multiplication
            .with_column(lit(true).alias(is_on_calendar_event));

        // 3. Left Join
        let joined = market_w_key.join(
            news_w_key,
            [col(join_key)],
            [col(join_key)],
            JoinArgs {
                how: JoinType::Left,
                ..Default::default()
            },
        );

        // 4. Apply Policy Filter
        let filtered = match policy {
            EconomicCalendarPolicy::OnlyWithEvents => {
                // Keep only if we found a match in the calendar
                joined.filter(col(is_on_calendar_event).is_not_null())
            }
            EconomicCalendarPolicy::ExcludeEvents => {
                // Keep only if we did NOT find a match
                joined.filter(col(is_on_calendar_event).is_null())
            }
            EconomicCalendarPolicy::Unrestricted => joined,
        };

        // 5. Cleanup
        filtered.drop(Selector::ByName {
            names: Arc::from([
                PlSmallStr::from(join_key),
                PlSmallStr::from(is_on_calendar_event),
                CanonicalCol::Category.into(),
            ]),
            strict: false,
        })
    }

    fn with_simulation_window_key(
        self,
        ts_col: CanonicalCol,
        timeframe: EpisodeLength,
        alias: &str,
    ) -> LazyFrame {
        let ts = col(ts_col);

        let window_expr = match timeframe {
            EpisodeLength::Day => ts.dt().truncate(lit("1d")),
            EpisodeLength::Week => ts.dt().truncate(lit("1w")),
            EpisodeLength::Month => ts.dt().truncate(lit("1mo")),
            EpisodeLength::Quarter => ts.dt().truncate(lit("3mo")),
            EpisodeLength::SemiAnnual => ts.dt().truncate(lit("6mo")),
            EpisodeLength::Annual => ts.dt().truncate(lit("1y")),

            // "Infinite": Constant grouping key puts all rows in one group
            EpisodeLength::Infinite => lit(1),
        };

        self.with_column(window_expr.alias(alias))
    }
}

trait DataFrameExt {
    fn dt_logical(&self, col: CanonicalCol) -> ChapatyResult<Logical<DatetimeType, Int64Type>>;
    fn f64_ca(&self, col: CanonicalCol) -> ChapatyResult<&ChunkedArray<Float64Type>>;
    fn i64_ca(&self, col: CanonicalCol) -> ChapatyResult<&ChunkedArray<Int64Type>>;
    fn str_ca(&self, col: CanonicalCol) -> ChapatyResult<&ChunkedArray<StringType>>;
    fn bool_ca(&self, col: CanonicalCol) -> ChapatyResult<&ChunkedArray<BooleanType>>;
}

impl DataFrameExt for DataFrame {
    fn dt_logical(&self, col: CanonicalCol) -> ChapatyResult<Logical<DatetimeType, Int64Type>> {
        let s = self
            .column(col.as_str())
            .map_err(|_| DataError::DataFrame(format!("Failed to get column {col:?}")))?;

        if matches!(s.dtype(), DataType::Datetime(TimeUnit::Microseconds, _)) {
            return s.datetime().cloned().map_err(|_| {
                DataError::DataFrame(format!("Column {col:?} is not Datetime")).into()
            });
        }

        let casted = s
            .cast(&DataType::Datetime(TimeUnit::Microseconds, None))
            .map_err(|e| {
                DataError::DataFrame(format!("Failed to cast {col:?} to Microseconds: {e}"))
            })?;

        casted.datetime().cloned().map_err(|_| {
            DataError::DataFrame(format!("Cast produced invalid Datetime for {col:?}")).into()
        })
    }

    fn f64_ca(&self, col: CanonicalCol) -> ChapatyResult<&ChunkedArray<Float64Type>> {
        self.column(col.as_str())
            .map_err(|_| DataError::DataFrame(format!("Failed to get column {col:?}")).into())
            .and_then(|s| {
                s.f64().map_err(|_| {
                    DataError::DataFrame(format!("Column {col:?} is not Float64")).into()
                })
            })
    }

    fn i64_ca(&self, col: CanonicalCol) -> ChapatyResult<&ChunkedArray<Int64Type>> {
        self.column(col.as_str())
            .map_err(|_| DataError::DataFrame(format!("Failed to get column {col:?}")).into())
            .and_then(|s| {
                s.i64().map_err(|_| {
                    DataError::DataFrame(format!("Column {col:?} is not Int64")).into()
                })
            })
    }
    fn str_ca(&self, col: CanonicalCol) -> ChapatyResult<&ChunkedArray<StringType>> {
        self.column(col.as_str())
            .map_err(|_| DataError::DataFrame(format!("Failed to get column {col:?}")).into())
            .and_then(|s| {
                s.str().map_err(|_| {
                    DataError::DataFrame(format!("Column {col:?} is not String")).into()
                })
            })
    }

    fn bool_ca(&self, col: CanonicalCol) -> ChapatyResult<&ChunkedArray<BooleanType>> {
        self.column(col.as_str())
            .map_err(|_| DataError::DataFrame(format!("Failed to get column {col:?}")).into())
            .and_then(|s| {
                s.bool().map_err(|_| {
                    DataError::DataFrame(format!("Column {col:?} is not Boolean")).into()
                })
            })
    }
}

// ================================================================================================
// StateFn
// ================================================================================================

type NextState<'a, Ctx> = ChapatyResult<StateFn<'a, Ctx>>;

#[allow(clippy::type_complexity)]
enum StateFn<'a, Ctx> {
    Next(fn(&mut Ctx) -> NextState<'a, Ctx>),
    NextAsync(
        Box<
            dyn for<'ctx> FnOnce(
                    &'ctx mut Ctx,
                )
                    -> Pin<Box<dyn Future<Output = NextState<'a, Ctx>> + Send + 'ctx>>
                + Send
                + 'a,
        >,
    ),
    Done,
}

// ================================================================================================
// Tests
// ================================================================================================

#[cfg(test)]
mod test {
    use super::*;
    use chrono::{TimeZone, Timelike};
    use polars::{
        df,
        prelude::{DataType, IntoLazy, LazyCsvReader, LazyFileListReader, PlPath, TimeUnit},
    };
    use std::path::PathBuf;

    // ============================================================================
    // Test Fixtures & Helpers
    // ============================================================================

    /// Returns the absolute path to the test fixtures directory.
    fn fixtures_path() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/gym")
    }

    /// Loads an OHLCV CSV fixture and maps column names to the canonical schema.
    fn load_ohlcv_fixture(filename: &str) -> LazyFrame {
        let path = fixtures_path().join("input").join(filename);

        LazyCsvReader::new(PlPath::new(path.as_os_str().to_str().expect("filepath")))
            .with_has_header(true)
            .finish()
            .expect("Failed to parse fixture CSV")
            .select([
                col("open_timestamp")
                    .cast(DataType::Datetime(
                        TimeUnit::Microseconds,
                        Some(polars::prelude::TimeZone::UTC),
                    ))
                    .alias(CanonicalCol::OpenTimestamp.as_str()),
                col("close_timestamp")
                    .cast(DataType::Datetime(
                        TimeUnit::Microseconds,
                        Some(polars::prelude::TimeZone::UTC),
                    ))
                    .alias(CanonicalCol::Timestamp.as_str()),
                // Rename the metrics (Implicitly drops 'exchange', 'symbol', etc.)
                col("open").alias(CanonicalCol::Open.as_str()),
                col("high").alias(CanonicalCol::High.as_str()),
                col("low").alias(CanonicalCol::Low.as_str()),
                col("close").alias(CanonicalCol::Close.as_str()),
                col("volume").alias(CanonicalCol::Volume.as_str()),
                col("quote_asset_volume").alias(CanonicalCol::QuoteAssetVolume.as_str()),
                col("number_of_trades").alias(CanonicalCol::NumberOfTrades.as_str()),
                col("taker_buy_base_asset_volume")
                    .alias(CanonicalCol::TakerBuyBaseAssetVolume.as_str()),
                col("taker_buy_quote_asset_volume")
                    .alias(CanonicalCol::TakerBuyQuoteAssetVolume.as_str()),
            ])
    }

    /// Loads an economic calendar CSV fixture and maps to the canonical schema.
    fn load_calendar_fixture(filename: &str) -> LazyFrame {
        let path = fixtures_path().join("input").join(filename);

        LazyCsvReader::new(PlPath::new(path.as_os_str().to_str().expect("filepath")))
            .with_has_header(true)
            .finish()
            .expect("Failed to parse calendar CSV")
            .select([
                col("event_timestamp")
                    .cast(DataType::Datetime(
                        TimeUnit::Microseconds,
                        Some(polars::prelude::TimeZone::UTC),
                    ))
                    .alias(CanonicalCol::Timestamp.as_str()),
                col("category").alias(CanonicalCol::Category.as_str()),
            ])
    }

    // ============================================================================
    // 1. INDICATOR COMPUTATION TESTS
    // ============================================================================

    struct IndicatorTestCase {
        name: &'static str,
        indicator: TechnicalIndicator,
        expected_file: &'static str,
    }

    /// **REGRESSION TEST**
    ///
    /// Checks that the current indicator logic produces the same output as
    /// previously recorded snapshots.
    ///
    /// **NOTE:** The `expected/*.csv` files were generated by this library itself.
    /// They serve to catch accidental changes in logic (regressions), but they
    /// do NOT guarantee mathematical correctness against an external standard
    /// (like TA-Lib or TradingView).
    #[test]
    fn test_indicators_regression_consistency() {
        let test_cases = vec![
            IndicatorTestCase {
                name: "EMA-20",
                indicator: TechnicalIndicator::Ema(EmaWindow(20)),
                expected_file: "ema_20_daily.csv",
            },
            IndicatorTestCase {
                name: "SMA-14",
                indicator: TechnicalIndicator::Sma(SmaWindow(14)),
                expected_file: "sma_14_daily.csv",
            },
            IndicatorTestCase {
                name: "RSI-14",
                indicator: TechnicalIndicator::Rsi(RsiWindow(14)),
                expected_file: "rsi_14_daily.csv",
            },
        ];

        for case in test_cases {
            println!("Running indicator test: {}", case.name);

            // 1. Load Input
            let input_lf = load_ohlcv_fixture("binance-btc-usdt-8h.csv");

            // 2. Compute (Simulating internal build step)
            let result_lf = compute_indicator(input_lf, case.indicator)
                .expect(&format!("Failed to compute {}", case.name));

            // 3. Assert
            let result_df = result_lf.collect().unwrap();

            let expected_file = fixtures_path().join("expected").join(case.expected_file);
            let expected_df = LazyCsvReader::new(PlPath::new(
                expected_file.as_os_str().to_str().expect("filepath"),
            ))
            .with_has_header(true)
            .finish()
            .unwrap()
            .with_column(col("timestamp").cast(DataType::Datetime(
                TimeUnit::Microseconds,
                Some(polars::prelude::TimeZone::UTC),
            )))
            .collect()
            .unwrap();

            assert_eq!(
                result_df, expected_df,
                "DataFrame mismatch for test case: {}",
                case.name
            );
        }
    }

    // ============================================================================
    // 2. ECONOMIC CALENDAR OVERLAY TESTS
    // ============================================================================

    struct OverlayTestCase {
        name: &'static str,
        policy: EconomicCalendarPolicy,
        episode_length: EpisodeLength,
        expected_file: &'static str,
    }

    #[test]
    fn test_overlay_economic_calendar_policy() {
        let test_cases = vec![
            OverlayTestCase {
                name: "OnlyWithEvents-Daily",
                policy: EconomicCalendarPolicy::OnlyWithEvents,
                episode_length: EpisodeLength::Day,
                expected_file: "overlay_only_events_daily.csv",
            },
            OverlayTestCase {
                name: "OnlyWithEvents-Weekly",
                policy: EconomicCalendarPolicy::OnlyWithEvents,
                episode_length: EpisodeLength::Week,
                expected_file: "overlay_only_events_weekly.csv",
            },
            OverlayTestCase {
                name: "ExcludeEvents-Daily",
                policy: EconomicCalendarPolicy::ExcludeEvents,
                episode_length: EpisodeLength::Day,
                expected_file: "overlay_exclude_events_daily.csv",
            },
            OverlayTestCase {
                name: "ExcludeEvents-Weekly",
                policy: EconomicCalendarPolicy::ExcludeEvents,
                episode_length: EpisodeLength::Week,
                expected_file: "overlay_exclude_events_weekly.csv",
            },
            OverlayTestCase {
                name: "Unrestricted",
                policy: EconomicCalendarPolicy::Unrestricted,
                episode_length: EpisodeLength::Day,
                expected_file: "overlay_unrestricted.csv",
            },
        ];

        for case in test_cases {
            println!("Running overlay test: {}", case.name);

            // 1. Load Inputs
            let market_lf = load_ohlcv_fixture("binance-btc-usdt-8h.csv");
            let inflation_cal = load_calendar_fixture("investingcom-ez-inflation.csv");
            let employment_cal = load_calendar_fixture("investingcom-us-employment.csv");

            // 2. Prepare Master Calendar (Union of sources)
            let cols = [
                col(CanonicalCol::Timestamp.as_str()),
                col(CanonicalCol::Category.as_str()),
            ];
            let master_calendar = polars::prelude::concat(
                vec![
                    inflation_cal.select(cols.clone()),
                    employment_cal.select(cols),
                ],
                UnionArgs {
                    parallel: true,
                    rechunk: true,
                    ..Default::default()
                },
            )
            .unwrap()
            .unique(None, UniqueKeepStrategy::First);

            // 3. Apply Overlay
            let result_lf = market_lf.join_with_economic_calendar_overlay(
                master_calendar,
                case.episode_length,
                case.policy,
            );

            // 4. Assert
            let result_df = result_lf.collect().unwrap();

            let expected_file = fixtures_path().join("expected").join(case.expected_file);
            let expected_df = LazyCsvReader::new(PlPath::new(
                expected_file.as_os_str().to_str().expect("filepath"),
            ))
            .with_has_header(true)
            .finish()
            .unwrap()
            .with_columns([
                col(CanonicalCol::OpenTimestamp).cast(DataType::Datetime(
                    TimeUnit::Microseconds,
                    Some(polars::prelude::TimeZone::UTC),
                )),
                col(CanonicalCol::Timestamp).cast(DataType::Datetime(
                    TimeUnit::Microseconds,
                    Some(polars::prelude::TimeZone::UTC),
                )),
            ])
            .collect()
            .unwrap();

            assert_eq!(
                result_df, expected_df,
                "DataFrame mismatch for test case: {}",
                case.name
            );
        }
    }

    #[test]
    fn test_overlay_empty_calendar_edge_case() {
        println!("Running edge case: OnlyWithEvents with empty calendar");

        let market_lf = load_ohlcv_fixture("binance-btc-usdt-8h.csv");
        let empty_calendar =
            load_calendar_fixture("investingcom-ez-inflation.csv").filter(lit(false));

        let result_lf = market_lf.join_with_economic_calendar_overlay(
            empty_calendar,
            EpisodeLength::Day,
            EconomicCalendarPolicy::OnlyWithEvents,
        );

        let result_df = result_lf.collect().unwrap();

        assert_eq!(
            result_df.height(),
            0,
            "Expected 0 rows when filtering OnlyWithEvents against an empty calendar"
        );
    }

    // ============================================================================
    // 3. TRADING WINDOW FILTER TESTS
    // ============================================================================

    // Helper to wrap a LazyFrame into the specific map structure your functions expect.
    // We use "BTC" as a dummy key.
    const KEY: &str = "__key";
    fn wrap_in_map(mut lf: LazyFrame) -> HashMap<String, (SchemaRef, LazyFrame)> {
        let schema = lf.collect_schema().unwrap();
        let mut map = HashMap::new();
        map.insert(KEY.to_string(), (schema, lf));
        map
    }

    // Helper to extract the result back out of the map
    fn unwrap_map(mut map: HashMap<String, (SchemaRef, LazyFrame)>) -> DataFrame {
        map.remove(KEY).unwrap().1.collect().unwrap()
    }

    #[test]
    fn test_filter_trading_windows_logic() {
        // Data:
        // Mon 08:00 (Fail start)
        // Mon 10:00 (Pass)
        // Mon 17:00 (Fail end - exclusive)
        // Tue 10:00 (Fail day)
        // Wed 15:00 (Pass Wed window)
        let df = df!(
            "timestamp" => &[
                ts_micros("2026-01-05T08:00:00Z"), // Mon - Too early
                ts_micros("2026-01-05T10:00:00Z"), // Mon - OK
                ts_micros("2026-01-05T17:00:00Z"), // Mon - Too late (if 9-17)
                ts_micros("2026-01-06T10:00:00Z"), // Tue - Wrong day
                ts_micros("2026-01-07T15:00:00Z"), // Wed - OK (if 14-20)
            ],
            "open" => &[100.0, 101.0, 102.0, 103.0, 104.0]
        )
        .unwrap();
        let mut lf_map = wrap_in_map(with_ts_cols(df, &["timestamp"]).lazy());

        // Define Rules
        let mut allowed = BTreeMap::new();
        // Mon: 09:00 - 17:00 (Hours 9, 10... 16). 17 is excluded.
        allowed.insert(Weekday::Monday, vec![TradingWindow::new(9, 17).unwrap()]);
        // Wed: 14:00 - 20:00
        allowed.insert(
            Weekday::Wednesday,
            vec![TradingWindow::new(14, 20).unwrap()],
        );

        // Action
        apply_filter(&mut lf_map, &allowed);
        let result = unwrap_map(lf_map);

        // Assertions
        assert_eq!(result.height(), 2);

        let valid_ts = result
            .column(CanonicalCol::Timestamp.as_str())
            .unwrap()
            .datetime()
            .unwrap()
            .as_datetime_iter()
            .map(|opt| opt.unwrap().and_utc().timestamp_micros())
            .collect::<Vec<_>>();

        // Expect Mon 10:00 and Wed 15:00
        assert!(valid_ts.contains(&ts_micros("2026-01-05T10:00:00Z")));
        assert!(valid_ts.contains(&ts_micros("2026-01-07T15:00:00Z")));

        // Verify Boundary Exclusions
        assert!(
            !valid_ts.contains(&ts_micros("2026-01-05T08:00:00Z")),
            "Start bound failed"
        );
        assert!(
            !valid_ts.contains(&ts_micros("2026-01-05T17:00:00Z")),
            "End bound failed (should be exclusive)"
        );
    }

    #[test]
    fn test_filter_empty_rules_drops_all() {
        let df = df!(
            "timestamp" => &[ts_micros("2026-01-01T10:00:00Z")]
        )
        .unwrap();
        let mut lf_map = wrap_in_map(with_ts_cols(df, &["timestamp"]).lazy());

        let allowed = BTreeMap::new(); // Empty rules

        apply_filter(&mut lf_map, &allowed);
        let result = unwrap_map(lf_map);

        assert_eq!(result.height(), 0, "Empty filter should drop all rows");
    }

    #[test]
    fn test_filter_multi_window_same_day() {
        // Split sessions (e.g., Morning 0-4, Evening 20-24)
        let df = df!(
            "timestamp" => &[
                ts_micros("2026-01-06T02:00:00Z"), // Tue 02:00 (Pass)
                ts_micros("2026-01-06T12:00:00Z"), // Tue 12:00 (Fail - Lunch)
                ts_micros("2026-01-06T21:00:00Z"), // Tue 21:00 (Pass)
            ]
        )
        .unwrap();
        let mut lf_map = wrap_in_map(with_ts_cols(df, &["timestamp"]).lazy());

        let mut allowed = BTreeMap::new();
        allowed.insert(
            Weekday::Tuesday,
            vec![
                TradingWindow::new(0, 4).unwrap(),
                TradingWindow::new(20, 24).unwrap(),
            ],
        );

        apply_filter(&mut lf_map, &allowed);
        let result = unwrap_map(lf_map);

        assert_eq!(result.height(), 2);
        // Verify the 12:00 entry is gone
        let times = result.column("timestamp").unwrap().datetime().unwrap();
        assert!(times.physical().get(0).is_some());
        // We rely on height=2 and inputs to know 12:00 is the one missing
    }

    // ============================================================================
    // 4. DATA SORTING TESTS
    // ============================================================================

    #[test]
    fn test_sort_primary_and_secondary_keys() {
        // SCENARIO: Sorting Logic Verification
        // Primary Sort:   Close Time (timestamp) -> ASC
        // Secondary Sort: REMOVED.
        //
        // NOTE: We previously sorted by Open Time as a secondary index.
        // However, not all data sources (e.g. tick data, economic calendar, etc.) provide an
        // OpenTimestamp. To support generic inputs, we strictly sort by the Canonical
        // Timestamp (Close Time, the time when an event is truly available).
        //
        // IMPLICATION: If two rows have the exact same Canonical Timestamp, their
        // relative order is nondeterministic (unstable sort). In production, this
        // theoretically represents "corrupted" or "duplicate" data, as unique
        // streams should have unique IDs or times. This test ensures the primary
        // sort still functions despite this ambiguity.

        let t_0800 = ts_micros("2026-01-01T08:00:00Z");
        let t_0900 = ts_micros("2026-01-01T09:00:00Z");
        let t_0930 = ts_micros("2026-01-01T09:30:00Z");
        let t_1000 = ts_micros("2026-01-01T10:00:00Z");
        let t_1100 = ts_micros("2026-01-01T11:00:00Z");

        // SETUP: Define rows in a shuffled order.
        // We create a "sandwich" around the collision:
        // - Head: ID 1 (09:00)
        // - Middle: ID 2 & 3 (10:00) -> The Collision
        // - Tail: ID 4 (11:00)
        let df_shuffled = df!(
            "id"             => &[4,      3,      1,      2     ],
            "timestamp"      => &[t_1100, t_1000, t_0900, t_1000], // Close Time
            "open_timestamp" => &[t_1000, t_0930, t_0800, t_0900]  // Open Time (Ignored)
        )
        .unwrap();

        // Wrap for the function under test
        let mut lf_map =
            wrap_in_map(with_ts_cols(df_shuffled, &["timestamp", "open_timestamp"]).lazy());

        // ACTION
        apply_sort(&mut lf_map).expect("failed to apply sort");

        // ASSERTION
        let result = unwrap_map(lf_map);
        let ids = result
            .column("id")
            .unwrap()
            .i32()
            .unwrap()
            .into_no_null_iter()
            .collect::<Vec<_>>();

        let times = result
            .column("timestamp")
            .unwrap()
            .datetime()
            .unwrap()
            .physical()
            .into_no_null_iter()
            .collect::<Vec<_>>();

        // 1. Verify Timestamps are strictly sorted
        assert_eq!(
            times,
            vec![t_0900, t_1000, t_1000, t_1100],
            "Timestamps must be sorted ASC, regardless of ID order"
        );

        // 2. Verify Head (Deterministically ID 1)
        assert_eq!(ids[0], 1, "ID 1 must be first (09:00)");

        // 3. Verify Middle (Nondeterministically ID 2 or 3)
        // We assert set membership because the unstable sort allows [2,3] or [3,2]
        let middle_ids = &ids[1..3];
        assert!(middle_ids.contains(&2));
        assert!(middle_ids.contains(&3));

        // 4. Verify Tail (Deterministically ID 4)
        // This proves the sort didn't break after the collision group
        assert_eq!(ids[3], 4, "ID 4 must be last (11:00)");
    }

    // ============================================================================
    // 5. DATA EXTRACTION TESTS
    // ============================================================================

    /// Helper to create a microsecond timestamp for Polars test data.
    fn ts_micros(dt_str: &str) -> i64 {
        DateTime::parse_from_rfc3339(dt_str)
            .unwrap()
            .with_timezone(&Utc)
            .timestamp_micros()
    }

    /// Helper to cast timestamp columns to the correct Logical Type expected by extractors.
    fn with_ts_cols(df: DataFrame, cols: &[&str]) -> DataFrame {
        let mut lf = df.lazy();
        for &c in cols {
            lf = lf.with_column(col(c).cast(DataType::Datetime(
                TimeUnit::Microseconds,
                Some(polars::prelude::TimeZone::UTC),
            )));
        }
        lf.collect().unwrap()
    }

    // Helper for standard aggregation config
    fn default_agg() -> ProfileAggregation {
        ProfileAggregation::default()
    }

    #[test]
    fn test_extract_technical_indicator() {
        // 1. Setup Data
        let df = df!(
            CanonicalCol::Timestamp.as_str() => &[
                ts_micros("2026-01-01T10:00:00Z"),
                ts_micros("2026-01-01T11:00:00Z"),
            ],
            CanonicalCol::Price.as_str() => &[100.5, 101.0],
        )
        .unwrap();
        let df = with_ts_cols(df, &[CanonicalCol::Timestamp.as_str()]);

        // 2. Extract
        let events = extract_ema(df).expect("failed to extract ema");

        // 3. Verify
        assert_eq!(events.len(), 2);

        // Row 1
        assert_eq!(
            events[0].timestamp,
            Utc.with_ymd_and_hms(2026, 1, 1, 10, 0, 0).unwrap()
        );
        assert_eq!(events[0].price, Price(100.5));

        // Row 2
        assert_eq!(
            events[1].timestamp,
            Utc.with_ymd_and_hms(2026, 1, 1, 11, 0, 0).unwrap()
        );
        assert_eq!(events[1].price, Price(101.0));
    }

    #[test]
    fn test_extract_technical_indicator_skips_warmup_nones() {
        // 1. Setup Data
        // Simulate a "Price" column that is actually an Indicator (e.g., SMA)
        // Row 1: None (Warming up)
        // Row 2: 100.5 (Ready)
        // Row 3: 101.0 (Ready)
        let df = df!(
            CanonicalCol::Timestamp.as_str() => &[
                ts_micros("2026-01-01T10:00:00Z"), // Index 0
                ts_micros("2026-01-01T11:00:00Z"), // Index 1
                ts_micros("2026-01-01T12:00:00Z"), // Index 2
            ],
            CanonicalCol::Price.as_str() => &[
                None::<f64>, // <--- Warmup period (should be skipped)
                Some(100.5),
                Some(101.0)
            ],
        )
        .unwrap();

        // Ensure timestamp is properly cast to Microseconds
        let df = with_ts_cols(df, &[CanonicalCol::Timestamp.as_str()]);

        // 2. Extract
        // We use extract_ema as a proxy for any technical indicator extractor
        let events = extract_ema(df).expect("failed to extract ema");

        // 3. Verify
        // We entered 3 rows, but expect 2 events because the first one was None
        assert_eq!(events.len(), 2);

        // Event 0 should match Row 1 (11:00)
        assert_eq!(
            events[0].timestamp,
            Utc.with_ymd_and_hms(2026, 1, 1, 11, 0, 0).unwrap()
        );
        assert_eq!(events[0].price, Price(100.5));

        // Event 1 should match Row 2 (12:00)
        assert_eq!(
            events[1].timestamp,
            Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap()
        );
        assert_eq!(events[1].price, Price(101.0));
    }

    #[test]
    fn test_extract_ohlcv() {
        let df = df!(
            CanonicalCol::OpenTimestamp.as_str() => &[ts_micros("2026-01-01T09:00:00Z")],
            CanonicalCol::Timestamp.as_str()      => &[ts_micros("2026-01-01T10:00:00Z")],
            CanonicalCol::Open.as_str()           => &[150.0],
            CanonicalCol::High.as_str()           => &[155.0],
            CanonicalCol::Low.as_str()            => &[149.0],
            CanonicalCol::Close.as_str()          => &[152.0],
            CanonicalCol::Volume.as_str()         => &[1000.0],
            // Optionals
            CanonicalCol::QuoteAssetVolume.as_str()           => &[Some(152000.0)],
            CanonicalCol::NumberOfTrades.as_str()             => &[Some(50i64)],
            CanonicalCol::TakerBuyBaseAssetVolume.as_str()  => &[Some(600.0)],
            CanonicalCol::TakerBuyQuoteAssetVolume.as_str() => &[None::<f64>],
        )
        .unwrap();
        let df = with_ts_cols(
            df,
            &[
                CanonicalCol::OpenTimestamp.as_str(),
                CanonicalCol::Timestamp.as_str(),
            ],
        );

        let events = extract_ohlcv(df).expect("failed to extract ohlcv");

        assert_eq!(events.len(), 1);
        let candle = &events[0];

        // Required Fields
        assert_eq!(
            candle.open_timestamp,
            Utc.with_ymd_and_hms(2026, 1, 1, 9, 0, 0).unwrap()
        );
        assert_eq!(
            candle.close_timestamp,
            Utc.with_ymd_and_hms(2026, 1, 1, 10, 0, 0).unwrap()
        );
        assert_eq!(candle.open, Price(150.0));
        assert_eq!(candle.high, Price(155.0));
        assert_eq!(candle.low, Price(149.0));
        assert_eq!(candle.close, Price(152.0));
        assert_eq!(candle.volume, Quantity(1000.0));

        // Optional Fields
        assert_eq!(candle.quote_asset_volume, Some(Quantity(152000.0)));
        assert_eq!(candle.number_of_trades, Some(Count(50)));
        assert_eq!(candle.taker_buy_base_asset_volume, Some(Quantity(600.0)));
        assert_eq!(candle.taker_buy_quote_asset_volume, None);
    }

    #[test]
    fn test_extract_trade() {
        let df = df!(
            CanonicalCol::Timestamp.as_str()          => &[
                ts_micros("2026-01-01T12:00:01Z"),
                ts_micros("2026-01-01T12:00:02Z")
            ],
            CanonicalCol::Price.as_str()              => &[20000.0, 20001.0],
            CanonicalCol::Volume.as_str()             => &[0.5, 1.0], // Maps to quantity
            // Optionals
            CanonicalCol::TradeId.as_str()            => &[Some(12345i64), None],
            CanonicalCol::QuoteAssetVolume.as_str()   => &[Some(10000.0), None],
            CanonicalCol::IsBuyerMaker.as_str()       => &[Some(true), Some(false)],
            CanonicalCol::IsBestMatch.as_str()        => &[Some(true), None],
        )
        .unwrap();
        let df = with_ts_cols(df, &[CanonicalCol::Timestamp.as_str()]);

        let events = extract_trade(df).expect("failed to extract trade");

        assert_eq!(events.len(), 2);

        // === Trade 1 (All fields present) ===
        let trade1 = &events[0];

        // Required Fields
        assert_eq!(
            trade1.timestamp,
            Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 1).unwrap()
        );
        assert_eq!(trade1.price, Price(20000.0));
        assert_eq!(trade1.quantity, Quantity(0.5));

        // Optional Fields
        assert_eq!(trade1.trade_id, Some(TradeId(12345)));
        assert_eq!(trade1.quote_asset_volume, Some(Quantity(10000.0)));
        assert_eq!(trade1.is_buyer_maker, Some(LiquiditySide::Bid));
        assert_eq!(trade1.is_best_match, Some(ExecutionDepth::TopOfBook));

        // === Tradee 2 (Optionals missing) ===
        let trade2 = &events[1];

        // Required Fields
        assert_eq!(
            trade2.timestamp,
            Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 2).unwrap()
        );
        assert_eq!(trade2.price, Price(20001.0));
        assert_eq!(trade2.quantity, Quantity(1.0));

        // Optional Fields
        assert_eq!(trade2.trade_id, None);
        assert_eq!(trade2.quote_asset_volume, None);
        assert_eq!(trade2.is_buyer_maker, Some(LiquiditySide::Ask));
        assert_eq!(trade2.is_best_match, None);
    }

    #[test]
    fn test_extract_economic() {
        let df = df!(
            CanonicalCol::Timestamp.as_str()            => &[
                ts_micros("2026-10-01T08:30:00Z"),
                ts_micros("2026-10-02T09:00:00Z")
            ],
            CanonicalCol::DataSource.as_str()           => &["investingcom", "fred"],
            CanonicalCol::Category.as_str()             => &["employment", "inflation"],
            CanonicalCol::NewsName.as_str()             => &["Non-Farm Payrolls", "Minor Index"],
            CanonicalCol::CountryCode.as_str()          => &["US", "EZ"],
            CanonicalCol::CurrencyCode.as_str()         => &["USD", "EUR"],
            CanonicalCol::EconomicImpact.as_str()       => &[3i64, 1i64], // 3=High, 1=Low

            // Optionals
            CanonicalCol::NewsType.as_str()             => &[Some("NFP"), None],
            CanonicalCol::NewsTypeSource.as_str()       => &[Some("manual"), None],
            CanonicalCol::Period.as_str()               => &[Some("mom"), None],
            CanonicalCol::NewsTypeConfidence.as_str()   => &[Some(1.0), None],
            CanonicalCol::Actual.as_str()               => &[Some(150.0), None],
            CanonicalCol::Forecast.as_str()             => &[Some(170.0), None],
            CanonicalCol::Previous.as_str()             => &[Some(160.0), None],
        )
        .unwrap();
        let df = with_ts_cols(df, &[CanonicalCol::Timestamp.as_str()]);

        let events = extract_economic(df).expect("failed to extract economic");

        assert_eq!(events.len(), 2);

        // === Event 1 (All fields present) ===
        let event1 = &events[0];

        // Required Fields
        assert_eq!(
            event1.timestamp,
            Utc.with_ymd_and_hms(2026, 10, 1, 8, 30, 0).unwrap()
        );
        assert_eq!(event1.data_source, "investingcom");
        assert_eq!(event1.category, "employment");
        assert_eq!(event1.news_name, "Non-Farm Payrolls");
        assert_eq!(event1.country_code, CountryCode::Us);
        assert_eq!(event1.currency_code, "USD");
        assert_eq!(event1.economic_impact, EconomicEventImpact::High);

        // Optional Fields
        assert_eq!(event1.news_type.as_deref(), Some("NFP"));
        assert_eq!(event1.news_type_source.as_deref(), Some("manual"));
        assert_eq!(event1.period.as_deref(), Some("mom"));
        assert_eq!(event1.news_type_confidence, Some(1.0));
        assert_eq!(event1.actual, Some(EconomicValue(150.0)));
        assert_eq!(event1.forecast, Some(EconomicValue(170.0)));
        assert_eq!(event1.previous, Some(EconomicValue(160.0)));

        // === Event 2 (Optionals missing) ===
        let event2 = &events[1];

        // Required Fields
        assert_eq!(
            event2.timestamp,
            Utc.with_ymd_and_hms(2026, 10, 2, 9, 0, 0).unwrap()
        );
        assert_eq!(event2.data_source, "fred");
        assert_eq!(event2.country_code, CountryCode::Ez);
        assert_eq!(event2.currency_code, "EUR");
        assert_eq!(event2.economic_impact, EconomicEventImpact::Low);

        // Optional Fields
        assert_eq!(event2.news_type, None);
        assert_eq!(event2.actual, None);
        assert_eq!(event2.forecast, None);
        assert_eq!(event2.previous, None);
    }

    // Helper to build a TPO dataframe specifically
    fn build_tpo_df(ts_open: i64, ts_close: i64, prices: &[f64], counts: &[i64]) -> DataFrame {
        let n = prices.len();
        let df = df!(
            CanonicalCol::OpenTimestamp.as_str()  => vec![ts_open; n],
            CanonicalCol::Timestamp.as_str()       => vec![ts_close; n],
            CanonicalCol::PriceBinStart.as_str() => prices,
            CanonicalCol::PriceBinEnd.as_str()   => prices.iter().map(|p| p + 1.0).collect::<Vec<_>>(),
            CanonicalCol::TimeSlotCount.as_str() => counts,
        )
        .unwrap();
        with_ts_cols(
            df,
            &[
                CanonicalCol::OpenTimestamp.as_str(),
                CanonicalCol::Timestamp.as_str(),
            ],
        )
    }

    #[test]
    fn test_tpo_full_field_mapping() {
        // SCENARIO:
        // Bin 100.0: 10 TPOs
        // Bin 101.0: 50 TPOs
        // Total: 60. Target VA (70%): 42.
        // POC (101.0) has 50 > 42, so VA is contained entirely within the POC bin.

        let df = build_tpo_df(
            ts_micros("2026-01-01T08:00:00Z"),
            ts_micros("2026-01-01T08:30:00Z"),
            &[100.0, 101.0],
            &[10, 50],
        );

        let profiles = extract_tpo(df, &default_agg()).expect("failed to extract tpo");

        assert_eq!(profiles.len(), 1);
        let tpo = &profiles[0];

        // 1. Header Metadata
        assert_eq!(
            tpo.open_timestamp,
            Utc.with_ymd_and_hms(2026, 1, 1, 8, 0, 0).unwrap()
        );
        assert_eq!(
            tpo.close_timestamp,
            Utc.with_ymd_and_hms(2026, 1, 1, 8, 30, 0).unwrap()
        );

        // 2. Computed Stats (POC & VA)
        // With default aggregation, High Volume VA rule applies.
        assert_eq!(tpo.poc, Price(101.0));
        assert_eq!(tpo.value_area_low, Price(101.0));
        assert_eq!(tpo.value_area_high, Price(101.0));

        // 3. Bins Data
        assert_eq!(tpo.bins.len(), 2);

        // Bin 0
        assert_eq!(tpo.bins[0].price_bin_start, Price(100.0));
        assert_eq!(tpo.bins[0].price_bin_end, Price(101.0)); // Assumes 1.0 step based on input
        assert_eq!(tpo.bins[0].time_slot_count, Count(10));

        // Bin 1
        assert_eq!(tpo.bins[1].price_bin_start, Price(101.0));
        assert_eq!(tpo.bins[1].time_slot_count, Count(50));
    }

    #[test]
    fn test_tpo_multi_window_grouping() {
        // SCENARIO: Explicitly test TPO window flushing.
        // Window A: 08:00 (1 bin, 100.0)
        // Window B: 09:00 (1 bin, 200.0)
        let t1 = ts_micros("2026-01-01T08:00:00Z");
        let t2 = ts_micros("2026-01-01T09:00:00Z");

        let df = df!(
            CanonicalCol::OpenTimestamp.as_str()  => &[t1, t2],
            CanonicalCol::Timestamp.as_str()       => &[t1 + 1000, t2 + 1000],
            CanonicalCol::PriceBinStart.as_str() => &[100.0, 200.0],
            CanonicalCol::PriceBinEnd.as_str()   => &[101.0, 201.0],
            CanonicalCol::TimeSlotCount.as_str() => &[5i64, 10i64],
        )
        .unwrap();
        let df = with_ts_cols(
            df,
            &[
                CanonicalCol::OpenTimestamp.as_str(),
                CanonicalCol::Timestamp.as_str(),
            ],
        );

        let profiles = extract_tpo(df, &default_agg()).expect("failed to extract tpo");

        assert_eq!(profiles.len(), 2, "Failed to flush distinct TPO windows");

        // Profile 1 (08:00)
        assert_eq!(profiles[0].open_timestamp.hour(), 8);
        assert_eq!(profiles[0].poc, Price(100.0));
        assert_eq!(profiles[0].bins[0].time_slot_count, Count(5));

        // Profile 2 (09:00)
        assert_eq!(profiles[1].open_timestamp.hour(), 9);
        assert_eq!(profiles[1].poc, Price(200.0));
        assert_eq!(profiles[1].bins[0].time_slot_count, Count(10));
    }

    #[test]
    fn test_tpo_missing_columns() {
        // SCENARIO: TPO requires 'time_slot_count'. If missing, it should fail nicely.
        let df = df!(
            CanonicalCol::OpenTimestamp.as_str()  => &[ts_micros("2026-01-01T08:00:00Z")],
            CanonicalCol::Timestamp.as_str()       => &[ts_micros("2026-01-01T08:30:00Z")],
            CanonicalCol::PriceBinStart.as_str() => &[100.0],
            CanonicalCol::PriceBinEnd.as_str()   => &[101.0],
            // "time_slot_count" is MISSING
            CanonicalCol::Volume.as_str()          => &[100.0],
        )
        .unwrap();
        let df = with_ts_cols(
            df,
            &[
                CanonicalCol::OpenTimestamp.as_str(),
                CanonicalCol::Timestamp.as_str(),
            ],
        );

        let result = extract_tpo(df, &default_agg());

        assert!(result.is_err());
        match result {
            Err(ChapatyError::Data(DataError::DataFrame(msg))) => {
                assert!(msg.contains("Failed to get column"));
            }
            _ => panic!("Expected DataFrameError regarding missing column"),
        }
    }

    #[test]
    fn test_tpo_zero_counts() {
        // SCENARIO: A bin exists but has 0 count.
        // [100.0 (0), 101.0 (10), 102.0 (0)].
        // POC should be 101.0. VA should be tight around 101.0.
        let df = build_tpo_df(
            ts_micros("2026-01-01T08:00:00Z"),
            ts_micros("2026-01-01T08:30:00Z"),
            &[100.0, 101.0, 102.0],
            &[0, 10, 0],
        );

        let profiles = extract_tpo(df, &default_agg()).expect("failed to extract tpo");
        let tpo = &profiles[0];

        assert_eq!(tpo.poc, Price(101.0));
        assert_eq!(tpo.value_area_low, Price(101.0));
        assert_eq!(tpo.value_area_high, Price(101.0));

        assert_eq!(tpo.bins[0].time_slot_count, Count(0));
        assert_eq!(tpo.bins[1].time_slot_count, Count(10));
    }

    #[test]
    fn test_vp_full_field_mapping() {
        // SCENARIO: Ensure every single column, including Optionals, is mapped correctly.
        // Also checks that 'volume' is converted to Quantity/Volume types correctly.
        let df = df!(
            CanonicalCol::OpenTimestamp.as_str()               => &[ts_micros("2026-01-01T09:00:00Z")],
            CanonicalCol::Timestamp.as_str()                    => &[ts_micros("2026-01-01T10:00:00Z")],
            CanonicalCol::PriceBinStart.as_str()              => &[100.0],
            CanonicalCol::PriceBinEnd.as_str()                => &[101.0],
            CanonicalCol::Volume.as_str()                       => &[1000.0],
            // Full set of optionals
            CanonicalCol::TakerBuyBaseAssetVolume.as_str()  => &[Some(600.0)],
            CanonicalCol::TakerSellBaseAssetVolume.as_str() => &[Some(400.0)],
            CanonicalCol::QuoteAssetVolume.as_str()           => &[Some(100_000.0)],
            CanonicalCol::TakerBuyQuoteAssetVolume.as_str() => &[Some(60_000.0)],
            CanonicalCol::TakerSellQuoteAssetVolume.as_str()=> &[Some(40_000.0)],
            CanonicalCol::NumberOfTrades.as_str()             => &[Some(50i64)],
            CanonicalCol::NumberOfBuyTrades.as_str()         => &[Some(30i64)],
            CanonicalCol::NumberOfSellTrades.as_str()        => &[Some(20i64)],
        )
        .unwrap();
        let df = with_ts_cols(
            df,
            &[
                CanonicalCol::OpenTimestamp.as_str(),
                CanonicalCol::Timestamp.as_str(),
            ],
        );

        let profiles = extract_vp(df, &default_agg()).expect("failed to extract vp");

        assert_eq!(profiles.len(), 1);
        let vp = &profiles[0];

        // 1. Stats Check (Single bin -> POC is that bin)
        assert_eq!(vp.poc, Price(100.0));
        assert_eq!(vp.value_area_low, Price(100.0));
        assert_eq!(vp.value_area_high, Price(100.0));

        // 2. Bin Fields Check
        let bin = &vp.bins[0];

        // Base Volume
        assert_eq!(bin.volume, Quantity(1000.0));
        assert_eq!(bin.taker_buy_base_asset_volume, Some(Quantity(600.0)));
        assert_eq!(bin.taker_sell_base_asset_volume, Some(Quantity(400.0)));

        // Quote Volume
        assert_eq!(bin.quote_asset_volume, Some(Quantity(100_000.0)));
        assert_eq!(bin.taker_buy_quote_asset_volume, Some(Quantity(60_000.0)));
        assert_eq!(bin.taker_sell_quote_asset_volume, Some(Quantity(40_000.0)));

        // Trade Counts
        assert_eq!(bin.number_of_trades, Some(Count(50)));
        assert_eq!(bin.number_of_buy_trades, Some(Count(30)));
        assert_eq!(bin.number_of_sell_trades, Some(Count(20)));
    }

    #[test]
    fn test_vp_partial_data_handling() {
        // SCENARIO: Some optional columns are completely missing (Null/None) in the DF.
        // The extractor should handle iterators yielding None gracefully.
        let df = df!(
            CanonicalCol::OpenTimestamp.as_str()  => &[ts_micros("2026-01-01T09:00:00Z")],
            CanonicalCol::Timestamp.as_str()       => &[ts_micros("2026-01-01T10:00:00Z")],
            CanonicalCol::PriceBinStart.as_str() => &[100.0],
            CanonicalCol::PriceBinEnd.as_str()   => &[101.0],
            CanonicalCol::Volume.as_str()          => &[1000.0],
            // Note: Other columns are totally absent from creation or are explicitly None
            CanonicalCol::NumberOfTrades.as_str()=> &[None::<i64>],
        )
        .unwrap();
        let df = with_ts_cols(
            df,
            &[
                CanonicalCol::OpenTimestamp.as_str(),
                CanonicalCol::Timestamp.as_str(),
            ],
        );

        let profiles = extract_vp(df, &default_agg()).expect("failed to extract vp");
        let bin = &profiles[0].bins[0];

        // Ensure required fields exist
        assert_eq!(bin.volume, Quantity(1000.0));

        // Ensure missing fields are None
        assert_eq!(bin.number_of_trades, None);
        assert_eq!(bin.taker_buy_base_asset_volume, None);
        assert_eq!(bin.quote_asset_volume, None);
        assert_eq!(bin.number_of_buy_trades, None);
    }

    #[test]
    fn test_multi_window_grouping_logic() {
        // SCENARIO: Tests the loop state machine: `if Some(ts_val) != current_window_start`.

        // Window 1: 09:00 (2 bins)
        // Window 2: 10:00 (1 bin)
        let df = df!(
            CanonicalCol::OpenTimestamp.as_str()  => &[
                ts_micros("2026-01-01T09:00:00Z"), ts_micros("2026-01-01T09:00:00Z"),
                ts_micros("2026-01-01T10:00:00Z")
            ],
            CanonicalCol::Timestamp.as_str()       => &[
                ts_micros("2026-01-01T09:30:00Z"), ts_micros("2026-01-01T09:30:00Z"),
                ts_micros("2026-01-01T10:30:00Z")
            ],
            CanonicalCol::PriceBinStart.as_str() => &[100.0, 101.0, 200.0],
            CanonicalCol::PriceBinEnd.as_str()   => &[101.0, 102.0, 201.0],
            CanonicalCol::TimeSlotCount.as_str() => &[10i64, 20, 5],
        )
        .unwrap();
        let df = with_ts_cols(
            df,
            &[
                CanonicalCol::OpenTimestamp.as_str(),
                CanonicalCol::Timestamp.as_str(),
            ],
        );

        let profiles = extract_tpo(df, &default_agg()).expect("failed to extract tpo");

        assert_eq!(profiles.len(), 2, "Should have flushed 2 distinct profiles");

        // Profile 1 Check
        assert_eq!(profiles[0].open_timestamp.hour(), 9);
        assert_eq!(profiles[0].bins.len(), 2);
        // POC should be bin with count 20 (Price 101.0)
        assert_eq!(profiles[0].poc, Price(101.0));

        // Profile 2 Check
        assert_eq!(profiles[1].open_timestamp.hour(), 10);
        assert_eq!(profiles[1].bins.len(), 1);
        assert_eq!(profiles[1].poc, Price(200.0));
    }

    #[test]
    fn test_empty_dataframe_returns_empty_slice() {
        let df = DataFrame::empty();
        let events = extract_economic(df.clone()).expect("failed to extract economic");
        assert!(events.is_empty());
        let events = extract_ema(df.clone()).expect("failed to extract ema");
        assert!(events.is_empty());
        let events = extract_ohlcv(df.clone()).expect("failed to extract ohlcv");
        assert!(events.is_empty());
        let events = extract_rsi(df.clone()).expect("failed to extract rsi");
        assert!(events.is_empty());
        let events = extract_sma(df.clone()).expect("failed to extract sma");
        assert!(events.is_empty());
        let events = extract_trade(df.clone()).expect("failed to extract trade");
        assert!(events.is_empty());
        let events = extract_tpo(df.clone(), &default_agg()).expect("failed to extract tpo");
        assert!(events.is_empty());
        let events = extract_vp(df, &default_agg()).expect("failed to extract vp");
        assert!(events.is_empty());
    }
}
