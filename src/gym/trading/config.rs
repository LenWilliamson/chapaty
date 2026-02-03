// ================================================================================================
// Domain Strong Types (NewTypes)
// ================================================================================================

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use strum::{Display, EnumCount, EnumIter, EnumString, IntoStaticStr};

use crate::{
    data::{
        common::RiskMetricsConfig,
        config::{
            EconomicCalendarConfig, OhlcvFutureConfig, OhlcvSpotConfig, TpoFutureConfig,
            TpoSpotConfig, TradeSpotConfig, VolumeProfileSpotConfig,
        },
        domain::{DataBroker, Exchange, Period, SpotPair, Symbol},
        episode::EpisodeLength,
        filter::FilterConfig,
    },
    error::{ChapatyResult, EnvError},
    gym::Reward,
    transport::source::{DataSource, SourceGroup},
};

/// Configuration parameter for penalizing invalid actions.
///
/// This is a Newtype wrapper around [`Reward`] to distinguish it
/// from standard step rewards and allow for specific default values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct InvalidActionPenalty(pub Reward);

// Defines the sensible default for this specific parameter
impl Default for InvalidActionPenalty {
    fn default() -> Self {
        Self(Reward(0))
    }
}

// Allow seamless conversion to the underlying Reward when doing math
impl From<InvalidActionPenalty> for Reward {
    fn from(penalty: InvalidActionPenalty) -> Self {
        penalty.0
    }
}

// ================================================================================================
// Preset Environment Configurations
// ================================================================================================

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    EnumString,
    Display,
    PartialOrd,
    Ord,
    EnumIter,
    IntoStaticStr,
    EnumCount,
)]
pub enum EnvPreset {
    /// **Bitcoin (BTC/USDT) End-of-Day Strategy**
    ///
    /// A classic daily timeframe environment ideal for trend-following or swing trading strategies.
    ///
    /// # Configuration Details
    /// * **Market:** Spot BTC/USDT.
    /// * **Source:** Binance (via Chapaty).
    /// * **Resolution:** 1 Day OHLCV candles.
    /// * **Range:** 2018 - 2025 (Inclusive).
    /// * **Episode:** Standard Defaults.
    BtcUsdtEod,
}

impl From<EnvPreset> for EnvConfig {
    fn from(preset: EnvPreset) -> Self {
        match preset {
            EnvPreset::BtcUsdtEod => {
                let market_config = OhlcvSpotConfig {
                    broker: DataBroker::Binance,
                    symbol: Symbol::Spot(SpotPair::BtcUsdt),
                    period: Period::Day(1),
                    batch_size: 1000,
                    exchange: Some(Exchange::Binance),
                    indicators: Vec::new(),
                };
                let allowed_years = (2018..=2025).collect::<BTreeSet<_>>();
                let filter = FilterConfig {
                    allowed_years: Some(allowed_years),
                    ..FilterConfig::default()
                };
                EnvConfig::default()
                    .add_ohlcv_spot(DataSource::Chapaty, market_config)
                    .with_filter_config(filter)
            }
        }
    }
}
// ================================================================================================
// Bias
// ================================================================================================

/// Trade outcome evaluation strategy for ambiguous executions.
///
/// In some market scenarios (e.g., large candles, coarse time resolution),
/// it may be unclear which price level was hit first: entry, stop-loss, or take-profit.
/// `ExecutionBias` defines how such ambiguity should be resolved:
///
/// - `Optimistic`: Favors the agent's outcome (e.g., assumes take-profit was hit first).
/// - `Pessimistic`: Favors conservative assumptions (e.g., assumes stop-loss hit or no profit).
///
/// This is particularly relevant in environments where candles can contain multiple trigger prices.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, Default)]
pub enum ExecutionBias {
    /// Choose the most favorable outcome for the agent in ambiguous cases.
    Optimistic,

    /// Choose the least favorable outcome for the agent in ambiguous cases.
    ///
    /// This is the default mode to ensure conservative and risk-aware evaluation.
    #[default]
    Pessimistic,
}

/// Configuration blueprint for building a trading environment.
///
/// Specifies market data streams, technical indicators, economic events,
/// and simulation parameters needed to construct an [`Environment`].
///
/// # Core Components
///
/// **Market Data (with optional indicators):**
/// - `ohlcv_spot`, `ohlcv_future`: Candlestick data with attached technical indicators
/// - `trade_spot`: Trade-level execution data
///
/// **Profile Data (external):**
/// - `tpo_spot`, `tpo_future`: Time Price Opportunity / Market Profile
/// - `volume_profile_spot`: Volume distribution by price level
///
/// **External Data:**
/// - `economic_calendar`: Economic events and news releases
///
/// **Processing Pipelines:**
/// - `filter_config`: Pre-filter raw data
///
/// **Simulation Settings:**
/// - `episode_length`: Maximum trade duration
/// - `trade_hint`: Buffer size optimization
/// - `risk_metrics_cfg`: Sharpe ratio and risk calculations
/// - `invalid_action_penalty`: Penalty for invalid actions
///
/// # Example
///
/// ```no_run
/// # use chapaty::prelude::*;
/// # async fn example() -> ChapatyResult<()> {
/// let market_config = OhlcvSpotConfig {
///     broker: DataBroker::Binance,
///     symbol: Symbol::Spot(SpotPair::BtcUsdt),
///     period: Period::Minute(1),
///     batch_size: 1000,
///     exchange: None,
///     indicators: vec![],
/// }
/// .with_sma(20)
/// .with_rsi(14);
///
/// let env_config = EnvConfig::default()
///     .add_ohlcv_spot(DataSource::Chapaty, market_config)
///     .with_episode_length(EpisodeLength::default());
///
/// // Build the environment using the factory function
/// let env = make(env_config).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Serialize, Deserialize)]
pub struct EnvConfig {
    // ========================================================================
    // Market Data (RPC) + Computed Indicators
    // ========================================================================
    /// OHLCV data from spot markets, optionally with technical indicators.
    ohlcv_spot: Vec<SourceGroup<OhlcvSpotConfig>>,

    /// OHLCV data from futures markets, optionally with technical indicators.
    ohlcv_future: Vec<SourceGroup<OhlcvFutureConfig>>,

    /// Trade-level trade execution data.
    trade_spot: Vec<SourceGroup<TradeSpotConfig>>,

    // ========================================================================
    // Profile Data (External RPC)
    // ========================================================================
    /// Time Price Opportunity (Market Profile) data for spot markets.
    tpo_spot: Vec<SourceGroup<TpoSpotConfig>>,

    /// Time Price Opportunity (Market Profile) data for futures markets.
    tpo_future: Vec<SourceGroup<TpoFutureConfig>>,

    /// Volume Profile data for spot markets.
    volume_profile_spot: Vec<SourceGroup<VolumeProfileSpotConfig>>,

    // ========================================================================
    // External Event Data
    // ========================================================================
    /// Economic calendar events and news releases.
    economic_calendar: Vec<SourceGroup<EconomicCalendarConfig>>,

    // ========================================================================
    // Processing Pipelines
    // ========================================================================
    /// Optional data filtering configuration.
    filter_config: Option<FilterConfig>,

    // ========================================================================
    // Simulation Parameters
    // ========================================================================
    /// Maximum duration trades can remain open before force-close.
    episode_length: EpisodeLength,

    /// Risk metrics calculation settings.
    risk_metrics_cfg: RiskMetricsConfig,

    // === Invariants ===
    /// Expected trades per episode for buffer preallocation (max: 32).
    trade_hint: usize,

    /// Penalty applied for invalid actions (must be <= 0 and defaults to -100.0).
    invalid_action_penalty: InvalidActionPenalty,
}

impl Default for EnvConfig {
    fn default() -> Self {
        Self {
            ohlcv_spot: Vec::new(),
            ohlcv_future: Vec::new(),
            trade_spot: Vec::new(),
            tpo_spot: Vec::new(),
            tpo_future: Vec::new(),
            volume_profile_spot: Vec::new(),
            economic_calendar: Vec::new(),
            filter_config: None,

            episode_length: EpisodeLength::default(),
            risk_metrics_cfg: RiskMetricsConfig::default(),
            trade_hint: 2,
            invalid_action_penalty: InvalidActionPenalty::default(),
        }
    }
}

// ================================================================================================
// Builder Methods - Market Data
// ================================================================================================

impl EnvConfig {
    /// Adds OHLCV spot market data from a specific source.
    pub fn add_ohlcv_spot(self, source: DataSource, config: OhlcvSpotConfig) -> Self {
        Self {
            ohlcv_spot: update_source_group(self.ohlcv_spot, source, config),
            ..self
        }
    }

    /// Adds OHLCV futures market data from a specific source.
    pub fn add_ohlcv_future(self, source: DataSource, config: OhlcvFutureConfig) -> Self {
        Self {
            ohlcv_future: update_source_group(self.ohlcv_future, source, config),
            ..self
        }
    }

    /// Adds trade-level spot market data from a specific source.
    pub fn add_trade_spot(self, source: DataSource, config: TradeSpotConfig) -> Self {
        Self {
            trade_spot: update_source_group(self.trade_spot, source, config),
            ..self
        }
    }

    /// Adds TPO (Market Profile) spot data from a specific source.
    pub fn add_tpo_spot(self, source: DataSource, config: TpoSpotConfig) -> Self {
        Self {
            tpo_spot: update_source_group(self.tpo_spot, source, config),
            ..self
        }
    }

    /// Adds TPO (Market Profile) futures data from a specific source.
    pub fn add_tpo_future(self, source: DataSource, config: TpoFutureConfig) -> Self {
        Self {
            tpo_future: update_source_group(self.tpo_future, source, config),
            ..self
        }
    }

    /// Adds Volume Profile spot data from a specific source.
    pub fn add_volume_profile_spot(
        self,
        source: DataSource,
        config: VolumeProfileSpotConfig,
    ) -> Self {
        Self {
            volume_profile_spot: update_source_group(self.volume_profile_spot, source, config),
            ..self
        }
    }

    /// Adds economic calendar events from a specific source.
    pub fn add_economic_calendar(self, source: DataSource, config: EconomicCalendarConfig) -> Self {
        Self {
            economic_calendar: update_source_group(self.economic_calendar, source, config),
            ..self
        }
    }
}

// ================================================================================================
// Builder Methods - Processing & Simulation
// ================================================================================================

impl EnvConfig {
    /// Sets the data filter configuration.
    pub fn with_filter_config(self, filter_config: FilterConfig) -> Self {
        Self {
            filter_config: Some(filter_config),
            ..self
        }
    }

    /// Sets the maximum trade duration.
    pub fn with_episode_length(self, episode_length: EpisodeLength) -> Self {
        Self {
            episode_length,
            ..self
        }
    }

    /// Sets the risk metrics calculation configuration.
    pub fn with_risk_metrics_cfg(self, risk_metrics_cfg: RiskMetricsConfig) -> Self {
        Self {
            risk_metrics_cfg,
            ..self
        }
    }

    /// Sets the expected trades per episode.
    ///
    /// # Behavior
    /// Automatically clamps the value to a maximum of 32 to prevent excessive
    /// buffer pre-allocation.
    pub fn with_trade_hint(self, trade_hint: u32) -> Self {
        Self {
            trade_hint: trade_hint.min(32) as usize,
            ..self
        }
    }

    /// Sets the penalty for invalid actions.
    ///
    /// # Panics
    /// Panics if the penalty is positive (> 0).
    pub fn with_invalid_action_penalty(self, penalty: InvalidActionPenalty) -> Self {
        assert!(
            penalty.0.0 <= 0,
            "Invalid action penalty must be <= 0, got {}",
            penalty.0.0
        );
        Self {
            invalid_action_penalty: penalty,
            ..self
        }
    }
}

// ================================================================================================
// Accessor Methods
// ================================================================================================

impl EnvConfig {
    pub fn ohlcv_spot(&self) -> &[SourceGroup<OhlcvSpotConfig>] {
        &self.ohlcv_spot
    }

    pub fn ohlcv_future(&self) -> &[SourceGroup<OhlcvFutureConfig>] {
        &self.ohlcv_future
    }

    pub fn trade_spot(&self) -> &[SourceGroup<TradeSpotConfig>] {
        &self.trade_spot
    }

    pub fn tpo_spot(&self) -> &[SourceGroup<TpoSpotConfig>] {
        &self.tpo_spot
    }

    pub fn tpo_future(&self) -> &[SourceGroup<TpoFutureConfig>] {
        &self.tpo_future
    }

    pub fn volume_profile_spot(&self) -> &[SourceGroup<VolumeProfileSpotConfig>] {
        &self.volume_profile_spot
    }

    pub fn economic_calendar(&self) -> &[SourceGroup<EconomicCalendarConfig>] {
        &self.economic_calendar
    }

    pub fn filter_config(&self) -> Option<&FilterConfig> {
        self.filter_config.as_ref()
    }

    pub fn episode_length(&self) -> EpisodeLength {
        self.episode_length
    }

    pub fn risk_metrics_cfg(&self) -> RiskMetricsConfig {
        self.risk_metrics_cfg
    }

    pub fn trade_hint(&self) -> usize {
        self.trade_hint
    }

    pub fn invalid_action_penalty(&self) -> InvalidActionPenalty {
        self.invalid_action_penalty
    }

    /// Resolves the effective list of years allowed by this configuration.
    ///
    /// If specific years are configured in the filter, returns that list.
    /// Otherwise, returns the default simulation range (1990..=2040).
    pub fn allowed_years(&self) -> Vec<u16> {
        if let Some(years_set) = self
            .filter_config()
            .as_ref()
            .and_then(|c| c.allowed_years.as_ref())
        {
            let mut y: Vec<u16> = years_set.iter().copied().collect();
            y.sort_unstable();
            return y;
        }

        // Default broad range if unrestricted
        (1990..=2040).collect()
    }

    /// Calculates the maximum expected capacity (e.g., total episodes or steps)
    /// based on the configured episode length and allowed years.
    ///
    /// Used to pre-allocate the Ledger.
    pub fn max_episode_capacity(&self) -> usize {
        let max_episodes_per_year = self.episode_length().max_episodes();
        let number_of_years = self.allowed_years().len();

        // Ensure we allocate at least 1 to avoid zero-capacity edge cases
        (max_episodes_per_year * number_of_years).max(1)
    }
}

// ================================================================================================
// Internal Helper
// ================================================================================================

/// Update a list of source groups.
///
/// Takes ownership of the vector, modifies it in place, and returns it.
fn update_source_group<T>(
    mut groups: Vec<SourceGroup<T>>,
    source: DataSource,
    config: T,
) -> Vec<SourceGroup<T>> {
    if let Some(group) = groups.iter_mut().find(|g| g.source == source) {
        group.items.push(config);
    } else {
        groups.push(SourceGroup {
            source,
            items: vec![config],
        });
    }
    groups
}

// ================================================================================================
// Environment Construction
// ================================================================================================

impl EnvConfig {
    /// Computes a deterministic hash of this configuration.
    ///
    /// Used for caching and versioning environment configs.
    pub fn hash(&self) -> ChapatyResult<String> {
        let mut hasher = blake3::Hasher::new();
        let bytes = postcard::to_stdvec(self).map_err(EnvError::Encoding)?;
        hasher.update(&bytes);
        Ok(format!("{}", hasher.finalize()))
    }

    /// Validates that at least one market data source is configured.
    pub fn is_valid(&self) -> bool {
        !self.ohlcv_spot.is_empty() || !self.ohlcv_future.is_empty() || !self.trade_spot.is_empty()
    }
}
