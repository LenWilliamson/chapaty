// ================================================================================================
// Domain Strong Types (NewTypes)
// ================================================================================================

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use strum::{Display, EnumCount, EnumIter, EnumString, IntoStaticStr};

use crate::{
    ApiKey, EndpointUrl, SelfHostedApi,
    data::{
        common::{ProfileAggregation, RiskMetricsConfig},
        config::{
            EconomicCalendarConfig, OhlcvFutureConfig, OhlcvSpotConfig, TpoFutureConfig,
            TpoSpotConfig, TradeSpotConfig, VolumeProfileSpotConfig,
        },
        domain::{
            ContractMonth, ContractYear, CountryCode, DataBroker, EconomicCategory,
            EconomicEventImpact, Exchange, FutureContract, FutureRoot, Period, SpotPair, Symbol,
        },
        episode::EpisodeLength,
        filter::{EconomicCalendarPolicy, FilterConfig},
        indicator::{SmaWindow, TechnicalIndicator},
    },
    error::{ChapatyResult, EnvError},
    gym::InvalidActionPenalty,
    transport::source::{DataSource, SourceGroup},
};

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

// ================================================================================================
// Preset Environment Configurations
// ================================================================================================

/// Ready-made environment configurations for common trading setups.
///
/// Each variant encodes the exact data source IDs (broker, symbol, period, etc.) required to
/// reproduce the environment. The underlying datasets are publicly available on
/// [Hugging Face](https://huggingface.co/datasets/chapaty/environments) and are strictly
/// tied to your current `chapaty` crate version.
///
/// # Loading from Hugging Face
///
/// Every preset is pre-compiled and available to download directly from the Hugging Face Hub.
/// To load a preset, configure your I/O settings to use `StorageLocation::HuggingFace`.
///
/// Because the dataset files on Hugging Face are named using the snake-case representation
/// of the preset variants, you can conveniently pass `preset.to_string()` as the filename.
///
/// ```rust,no_run
/// use anyhow::{Context, Result};
/// use chapaty::prelude::*;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     let preset = EnvPreset::BinanceBtcUsdt1dSma20Sma50;
///
///     // The file on HF is named exactly after the preset (e.g., "binance_btc_usdt1d_sma20_sma50")
///     let file_stem = preset.to_string();
///
///     // Specify Hugging Face as the storage location.
///     // Setting version to `None` automatically binds to your current crate version.
///     let loc = StorageLocation::HuggingFace { version: None };
///     let cfg = IoConfig::new(loc).with_file_stem(&file_stem);
///
///     // Load the environment (downloads and caches locally on the first run)
///     let mut env = chapaty::load(preset, &cfg)
///         .await
///         .context("Failed to load trading environment")?;
///
///     println!("Successfully loaded environment for {}!", preset);
///
///     // env.evaluate_agent(&mut agent)?;
///
///     Ok(())
/// }
/// ```
///
/// # Starter Configurations & Customization
///
/// Presets also serve as excellent baseline configurations. If you want to customize a preset
/// (e.g., modifying the episode length or adding a new risk metric), you can convert it into
/// an [`EnvConfig`] using `.into()` and tweak it to your liking:
///
/// ```rust,ignore
/// let mut config: EnvConfig = EnvPreset::BinanceBtcUsdt1d.into();
/// // Modify the config as needed
/// ```
///
/// **Future Roadmap:** Currently, building a customized `EnvConfig` from scratch via
/// `chapaty::make()` requires you to host your own Chapaty gRPC server for the raw historical data.
/// Once the managed Chapaty API is publicly available, `chapaty::make()` will work out-of-the-box
/// for customized presets without requiring local infrastructure.
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
#[strum(serialize_all = "snake_case")]
pub enum EnvPreset {
    /// **BTC/USDT Daily Spot (Binance)**
    ///
    /// A classic daily timeframe environment ideal for trend-following or swing trading
    /// strategies on Bitcoin spot markets.
    ///
    /// # Episode Length
    ///
    /// [`EpisodeLength::Infinite`]
    ///
    /// # Available IDs
    ///
    /// ```rust
    /// # use chapaty::prelude::*;
    /// let ohlcv_id = OhlcvId {
    ///     broker: DataBroker::Binance,
    ///     exchange: Exchange::Binance,
    ///     symbol: Symbol::Spot(SpotPair::BtcUsdt),
    ///     period: Period::Day(1),
    /// };
    /// ```
    BinanceBtcUsdt1d,

    /// **BTC/USDT 1-Minute Spot (Binance)**
    ///
    /// A high-frequency intraday environment for scalping or short-term momentum strategies
    /// on Bitcoin spot markets. Each episode covers a single trading day.
    ///
    /// # Episode Length
    ///
    /// [`EpisodeLength::Infinite`]
    ///
    /// # Available IDs
    ///
    /// ```rust
    /// # use chapaty::prelude::*;
    /// let ohlcv_id = OhlcvId {
    ///     broker: DataBroker::Binance,
    ///     exchange: Exchange::Binance,
    ///     symbol: Symbol::Spot(SpotPair::BtcUsdt),
    ///     period: Period::Minute(1),
    /// };
    /// ```
    BinanceBtcUsdt1m,

    /// **BTC/USDT 1-Minute + 15-Minute Spot (Binance)**
    ///
    /// A multi-resolution intraday environment combining 1-minute and 15-minute BTC/USDT
    /// OHLCV data. The 15-minute timeframe provides trend context while the 1-minute
    /// timeframe is used for precise entry and exit timing. Each episode covers a single
    /// trading day.
    ///
    /// # Episode Length
    ///
    /// [`EpisodeLength::Infinite`]
    ///
    /// # Available IDs
    ///
    /// ```rust
    /// # use chapaty::prelude::*;
    /// let ohlcv_1m_id = OhlcvId {
    ///     broker: DataBroker::Binance,
    ///     exchange: Exchange::Binance,
    ///     symbol: Symbol::Spot(SpotPair::BtcUsdt),
    ///     period: Period::Minute(1),
    /// };
    ///
    /// let ohlcv_15m_id = OhlcvId {
    ///     broker: DataBroker::Binance,
    ///     exchange: Exchange::Binance,
    ///     symbol: Symbol::Spot(SpotPair::BtcUsdt),
    ///     period: Period::Minute(15),
    /// };
    /// ```
    BinanceBtcUsdt1m15m,

    /// **EUR/USD 1-Minute + 5-Minute Futures with US Employment News — Unrestricted (NinjaTrader, CME 6eh6)**
    ///
    /// A multi-resolution intraday environment with 1-minute and 5-minute EUR/USD futures
    /// and US high-impact employment calendar data. The economic calendar filter policy is
    /// [`EconomicCalendarPolicy::Unrestricted`], meaning **all trading days are included**
    /// regardless of whether an event occurs. The calendar data is still available to the
    /// agent for decision-making.
    ///
    /// # Episode Length
    ///
    /// [`EpisodeLength::Day`] (Default)
    ///
    /// # Available IDs
    ///
    /// ```rust
    /// # use chapaty::prelude::*;
    /// let ohlcv_1m_id = OhlcvId {
    ///     broker: DataBroker::NinjaTrader,
    ///     exchange: Exchange::Cme,
    ///     symbol: Symbol::Future(FutureContract {
    ///         root: FutureRoot::EurUsd,
    ///         month: ContractMonth::June,
    ///         year: ContractYear::Y6,
    ///     }),
    ///     period: Period::Minute(1),
    /// };
    ///
    /// let ohlcv_5m_id = OhlcvId {
    ///     broker: DataBroker::NinjaTrader,
    ///     exchange: Exchange::Cme,
    ///     symbol: Symbol::Future(FutureContract {
    ///         root: FutureRoot::EurUsd,
    ///         month: ContractMonth::June,
    ///         year: ContractYear::Y6,
    ///     }),
    ///     period: Period::Minute(5),
    /// };
    ///
    /// let cal_id = EconomicCalendarId {
    ///     broker: DataBroker::InvestingCom,
    ///     data_source: None,
    ///     country_code: Some(CountryCode::Us),
    ///     category: Some(EconomicCategory::Employment),
    ///     importance: Some(EconomicEventImpact::High),
    /// };
    /// ```
    ///
    /// # Filter Policy
    ///
    /// [`EconomicCalendarPolicy::Unrestricted`] — no day-level filtering. All days in
    /// `2008..=2026` are eligible for simulation. The economic calendar serves as
    /// contextual data only.
    NinjaTraderCme6eh61m5mUsEmpHigh,

    /// **EUR/USD 1-Minute Futures with US Employment News — Events Only (NinjaTrader, CME 6eh6)**
    ///
    /// A high-frequency intraday environment for news-driven strategies on EUR/USD futures,
    /// such as breakout or fade entries around scheduled US employment releases.
    /// The economic calendar filter policy is [`EconomicCalendarPolicy::OnlyWithEvents`],
    /// meaning **only days that contain a matching economic event are simulated**.
    ///
    /// # Episode Length
    ///
    /// [`EpisodeLength::Day`] (Default)
    ///
    /// # Available IDs
    ///
    /// ```rust
    /// # use chapaty::prelude::*;
    /// let ohlcv_id = OhlcvId {
    ///     broker: DataBroker::NinjaTrader,
    ///     exchange: Exchange::Cme,
    ///     symbol: Symbol::Future(FutureContract {
    ///         root: FutureRoot::EurUsd,
    ///         month: ContractMonth::June,
    ///         year: ContractYear::Y6,
    ///     }),
    ///     period: Period::Minute(1),
    /// };
    ///
    /// let cal_id = EconomicCalendarId {
    ///     broker: DataBroker::InvestingCom,
    ///     data_source: None,
    ///     country_code: Some(CountryCode::Us),
    ///     category: Some(EconomicCategory::Employment),
    ///     importance: Some(EconomicEventImpact::High),
    /// };
    /// ```
    ///
    /// # Filter Policy
    ///
    /// [`EconomicCalendarPolicy::OnlyWithEvents`] — days without a matching US Employment
    /// (High impact) event are excluded from simulation.
    NinjaTraderCme6eh61mUsEmpHighEventsOnly,

    /// **EUR/USD 1-Minute + 5-Minute Futures with US Employment News — Events Only (NinjaTrader, CME 6eh6)**
    ///
    /// A multi-resolution intraday environment combining 1-minute and 5-minute futures data
    /// for hybrid news strategies that use different timeframes for entry and confirmation.
    /// The economic calendar filter policy is [`EconomicCalendarPolicy::OnlyWithEvents`],
    /// meaning **only days that contain a matching economic event are simulated**.
    ///
    /// # Episode Length
    ///
    /// [`EpisodeLength::Day`] (Default)
    ///
    /// # Available IDs
    ///
    /// ```rust
    /// # use chapaty::prelude::*;
    /// let ohlcv_1m_id = OhlcvId {
    ///     broker: DataBroker::NinjaTrader,
    ///     exchange: Exchange::Cme,
    ///     symbol: Symbol::Future(FutureContract {
    ///         root: FutureRoot::EurUsd,
    ///         month: ContractMonth::June,
    ///         year: ContractYear::Y6,
    ///     }),
    ///     period: Period::Minute(1),
    /// };
    ///
    /// let ohlcv_5m_id = OhlcvId {
    ///     broker: DataBroker::NinjaTrader,
    ///     exchange: Exchange::Cme,
    ///     symbol: Symbol::Future(FutureContract {
    ///         root: FutureRoot::EurUsd,
    ///         month: ContractMonth::June,
    ///         year: ContractYear::Y6,
    ///     }),
    ///     period: Period::Minute(5),
    /// };
    ///
    /// let cal_id = EconomicCalendarId {
    ///     broker: DataBroker::InvestingCom,
    ///     data_source: None,
    ///     country_code: Some(CountryCode::Us),
    ///     category: Some(EconomicCategory::Employment),
    ///     importance: Some(EconomicEventImpact::High),
    /// };
    /// ```
    ///
    /// # Filter Policy
    ///
    /// [`EconomicCalendarPolicy::OnlyWithEvents`] — days without a matching US Employment
    /// (High impact) event are excluded from simulation.
    NinjaTraderCme6eh61m5mUsEmpHighEventsOnly,

    /// **BTC/USDT Daily Spot with SMA Crossover (Binance)**
    ///
    /// A daily timeframe environment pre-configured with SMA(20) and SMA(50) indicators,
    /// tailored for moving-average crossover strategies.
    ///
    /// # Episode Length
    ///
    /// [`EpisodeLength::Infinite`]
    ///
    /// # Available IDs
    ///
    /// ```rust
    /// # use chapaty::prelude::*;
    /// let ohlcv_id = OhlcvId {
    ///     broker: DataBroker::Binance,
    ///     exchange: Exchange::Binance,
    ///     symbol: Symbol::Spot(SpotPair::BtcUsdt),
    ///     period: Period::Day(1),
    /// };
    ///
    /// let fast_sma_id = SmaId {
    ///     parent: ohlcv_id,
    ///     length: SmaWindow(20),
    /// };
    ///
    /// let slow_sma_id = SmaId {
    ///     parent: ohlcv_id,
    ///     length: SmaWindow(50),
    /// };
    /// ```
    BinanceBtcUsdt1dSma20Sma50,

    /// **BTC/USDT 1-Hour + 1-Minute Spot with Daily Volume Profile, 100 USDT bins (Binance)**
    ///
    /// A multi-resolution spot environment combining 1-hour and 1-minute BTC/USDT OHLCV
    /// data with a daily-aggregated Volume Profile using 100 USDT bin size
    /// (10,000 ticks × $0.01 tick size).
    ///
    /// # Episode Length
    ///
    /// [`EpisodeLength::Day`] (Default)
    ///
    /// # Available IDs
    ///
    /// ```rust
    /// # use chapaty::prelude::*;
    /// let ohlcv_1h_id = OhlcvId {
    ///     broker: DataBroker::Binance,
    ///     exchange: Exchange::Binance,
    ///     symbol: Symbol::Spot(SpotPair::BtcUsdt),
    ///     period: Period::Hour(1),
    /// };
    ///
    /// let ohlcv_1m_id = OhlcvId {
    ///     broker: DataBroker::Binance,
    ///     exchange: Exchange::Binance,
    ///     symbol: Symbol::Spot(SpotPair::BtcUsdt),
    ///     period: Period::Minute(1),
    /// };
    ///
    /// let vp_id = VolumeProfileId {
    ///     broker: DataBroker::Binance,
    ///     exchange: Exchange::Binance,
    ///     symbol: Symbol::Spot(SpotPair::BtcUsdt),
    ///     aggregation: ProfileAggregation {
    ///         time_frame: Some(Period::Day(1)),
    ///         ticks_per_bin: Some(10_000), // 10,000 ticks × $0.01 = 100 USDT
    ///         ..ProfileAggregation::default()
    ///     },
    /// };
    /// ```
    BinanceBtcUsdt1h1mVolumeProfile1d100Usdt,

    /// **BTC/USDT 1-Hour + 1-Minute Spot with Daily TPO Profile, 1 USDT bins (Binance)**
    ///
    /// A multi-resolution spot environment combining 1-hour and 1-minute BTC/USDT OHLCV
    /// data with a daily-aggregated TPO (Market Profile) using 1 USDT bin size
    /// (100 ticks × $0.01 tick size).
    ///
    /// # Episode Length
    ///
    /// [`EpisodeLength::Day`] (Default)
    ///
    /// # Available IDs
    ///
    /// ```rust
    /// # use chapaty::prelude::*;
    /// let ohlcv_1h_id = OhlcvId {
    ///     broker: DataBroker::Binance,
    ///     exchange: Exchange::Binance,
    ///     symbol: Symbol::Spot(SpotPair::BtcUsdt),
    ///     period: Period::Hour(1),
    /// };
    ///
    /// let ohlcv_1m_id = OhlcvId {
    ///     broker: DataBroker::Binance,
    ///     exchange: Exchange::Binance,
    ///     symbol: Symbol::Spot(SpotPair::BtcUsdt),
    ///     period: Period::Minute(1),
    /// };
    ///
    /// let tpo_id = TpoId {
    ///     broker: DataBroker::Binance,
    ///     exchange: Exchange::Binance,
    ///     symbol: Symbol::Spot(SpotPair::BtcUsdt),
    ///     aggregation: ProfileAggregation {
    ///         time_frame: Some(Period::Day(1)),
    ///         ticks_per_bin: Some(100), // 100 ticks × $0.01 = 1 USDT
    ///         ..ProfileAggregation::default()
    ///     },
    /// };
    /// ```
    BinanceBtcUsdt1h1mTpo1d1Usdt,

    /// **EUR/USD 1-Minute Futures with Daily TPO Profile (NinjaTrader, CME 6eh6)**
    ///
    /// An intraday futures environment with 1-minute EUR/USD OHLCV data and a
    /// daily-aggregated TPO (Market Profile) using tick-level bin size
    /// (1 tick = 0.00005).
    ///
    /// # Episode Length
    ///
    /// [`EpisodeLength::Day`] (Default)
    ///
    /// # Available IDs
    ///
    /// ```rust
    /// # use chapaty::prelude::*;
    /// let ohlcv_id = OhlcvId {
    ///     broker: DataBroker::NinjaTrader,
    ///     exchange: Exchange::Cme,
    ///     symbol: Symbol::Future(FutureContract {
    ///         root: FutureRoot::EurUsd,
    ///         month: ContractMonth::June,
    ///         year: ContractYear::Y6,
    ///     }),
    ///     period: Period::Minute(1),
    /// };
    ///
    /// let tpo_id = TpoId {
    ///     broker: DataBroker::NinjaTrader,
    ///     exchange: Exchange::Cme,
    ///     symbol: Symbol::Future(FutureContract {
    ///         root: FutureRoot::EurUsd,
    ///         month: ContractMonth::June,
    ///         year: ContractYear::Y6,
    ///     }),
    ///     aggregation: ProfileAggregation {
    ///         time_frame: Some(Period::Day(1)),
    ///         ..ProfileAggregation::default()
    ///     },
    /// };
    /// ```
    NinjaTraderCme6eh61mTpo1d,
}

fn self_hosted_source() -> DataSource {
    let api_key = std::env::var("CHAPATY_API_KEY").ok().map(ApiKey);
    DataSource::SelfHosted(SelfHostedApi {
        endpoint: EndpointUrl("http://[::1]:50051".to_string()),
        api_key,
    })
}

impl From<EnvPreset> for EnvConfig {
    fn from(preset: EnvPreset) -> Self {
        let source = self_hosted_source();
        match preset {
            EnvPreset::BinanceBtcUsdt1d => {
                let market_config = OhlcvSpotConfig {
                    broker: DataBroker::Binance,
                    symbol: Symbol::Spot(SpotPair::BtcUsdt),
                    period: Period::Day(1),
                    batch_size: 1000,
                    exchange: Some(Exchange::Binance),
                    indicators: Vec::new(),
                };
                let allowed_years = (2017..=2026).collect::<BTreeSet<_>>();
                let filter = FilterConfig {
                    allowed_years: Some(allowed_years),
                    ..FilterConfig::default()
                };
                EnvConfig::default()
                    .add_ohlcv_spot(source.clone(), market_config)
                    .with_episode_length(EpisodeLength::Infinite)
                    .with_filter_config(filter)
            }
            EnvPreset::BinanceBtcUsdt1m => {
                let market_config = OhlcvSpotConfig {
                    broker: DataBroker::Binance,
                    symbol: Symbol::Spot(SpotPair::BtcUsdt),
                    period: Period::Minute(1),
                    batch_size: 1000,
                    exchange: Some(Exchange::Binance),
                    indicators: Vec::new(),
                };
                let filter = FilterConfig {
                    allowed_years: Some((2017..=2026).collect::<BTreeSet<_>>()),
                    ..FilterConfig::default()
                };
                EnvConfig::default()
                    .add_ohlcv_spot(source.clone(), market_config)
                    .with_episode_length(EpisodeLength::Infinite)
                    .with_filter_config(filter)
            }
            EnvPreset::BinanceBtcUsdt1m15m => {
                let ohlcv_1m = OhlcvSpotConfig {
                    broker: DataBroker::Binance,
                    symbol: Symbol::Spot(SpotPair::BtcUsdt),
                    exchange: Some(Exchange::Binance),
                    period: Period::Minute(1),
                    batch_size: 1000,
                    indicators: Vec::new(),
                };
                let ohlcv_15m = OhlcvSpotConfig {
                    broker: DataBroker::Binance,
                    symbol: Symbol::Spot(SpotPair::BtcUsdt),
                    exchange: Some(Exchange::Binance),
                    period: Period::Minute(15),
                    batch_size: 1000,
                    indicators: Vec::new(),
                };
                let filter = FilterConfig {
                    allowed_years: Some((2017..=2026).collect::<BTreeSet<_>>()),
                    ..FilterConfig::default()
                };
                EnvConfig::default()
                    .add_ohlcv_spot(source.clone(), ohlcv_1m)
                    .add_ohlcv_spot(source.clone(), ohlcv_15m)
                    .with_episode_length(EpisodeLength::Infinite)
                    .with_filter_config(filter)
            }
            EnvPreset::NinjaTraderCme6eh61m5mUsEmpHigh => {
                let ohlcv_1m = OhlcvFutureConfig {
                    broker: DataBroker::NinjaTrader,
                    symbol: Symbol::Future(FutureContract {
                        root: FutureRoot::EurUsd,
                        month: ContractMonth::June,
                        year: ContractYear::Y6,
                    }),
                    exchange: Some(Exchange::Cme),
                    period: Period::Minute(1),
                    batch_size: 1000,
                    indicators: vec![],
                };
                let ohlcv_5m = OhlcvFutureConfig {
                    broker: DataBroker::NinjaTrader,
                    symbol: Symbol::Future(FutureContract {
                        root: FutureRoot::EurUsd,
                        month: ContractMonth::June,
                        year: ContractYear::Y6,
                    }),
                    exchange: Some(Exchange::Cme),
                    period: Period::Minute(5),
                    batch_size: 1000,
                    indicators: vec![],
                };
                let calendar = EconomicCalendarConfig {
                    broker: DataBroker::InvestingCom,
                    data_source: None,
                    country_code: Some(CountryCode::Us),
                    category: Some(EconomicCategory::Employment),
                    importance: Some(EconomicEventImpact::High),
                    batch_size: 1000,
                };
                let filter = FilterConfig {
                    allowed_years: Some((2008..=2026).collect::<BTreeSet<_>>()),
                    ..FilterConfig::default()
                };
                EnvConfig::default()
                    .add_ohlcv_future(source.clone(), ohlcv_1m)
                    .add_ohlcv_future(source.clone(), ohlcv_5m)
                    .with_episode_length(EpisodeLength::Day)
                    .with_filter_config(filter)
                    .add_economic_calendar(source.clone(), calendar)
                    .with_trade_hint(4)
            }
            EnvPreset::NinjaTraderCme6eh61mUsEmpHighEventsOnly => {
                let ohlcv = OhlcvFutureConfig {
                    broker: DataBroker::NinjaTrader,
                    symbol: Symbol::Future(FutureContract {
                        root: FutureRoot::EurUsd,
                        month: ContractMonth::June,
                        year: ContractYear::Y6,
                    }),
                    exchange: Some(Exchange::Cme),
                    period: Period::Minute(1),
                    batch_size: 1000,
                    indicators: vec![],
                };
                let calendar = EconomicCalendarConfig {
                    broker: DataBroker::InvestingCom,
                    data_source: None,
                    country_code: Some(CountryCode::Us),
                    category: Some(EconomicCategory::Employment),
                    importance: Some(EconomicEventImpact::High),
                    batch_size: 1000,
                };
                let filter = FilterConfig {
                    allowed_years: Some((2008..=2026).collect::<BTreeSet<_>>()),
                    economic_news_policy: Some(EconomicCalendarPolicy::OnlyWithEvents),
                    ..FilterConfig::default()
                };
                EnvConfig::default()
                    .add_ohlcv_future(source.clone(), ohlcv)
                    .with_episode_length(EpisodeLength::Day)
                    .with_filter_config(filter)
                    .add_economic_calendar(source.clone(), calendar)
                    .with_trade_hint(2)
            }
            EnvPreset::NinjaTraderCme6eh61m5mUsEmpHighEventsOnly => {
                let ohlcv_1m = OhlcvFutureConfig {
                    broker: DataBroker::NinjaTrader,
                    symbol: Symbol::Future(FutureContract {
                        root: FutureRoot::EurUsd,
                        month: ContractMonth::June,
                        year: ContractYear::Y6,
                    }),
                    exchange: Some(Exchange::Cme),
                    period: Period::Minute(1),
                    batch_size: 1000,
                    indicators: vec![],
                };
                let ohlcv_5m = OhlcvFutureConfig {
                    broker: DataBroker::NinjaTrader,
                    symbol: Symbol::Future(FutureContract {
                        root: FutureRoot::EurUsd,
                        month: ContractMonth::June,
                        year: ContractYear::Y6,
                    }),
                    exchange: Some(Exchange::Cme),
                    period: Period::Minute(5),
                    batch_size: 1000,
                    indicators: vec![],
                };
                let calendar = EconomicCalendarConfig {
                    broker: DataBroker::InvestingCom,
                    data_source: None,
                    country_code: Some(CountryCode::Us),
                    category: Some(EconomicCategory::Employment),
                    importance: Some(EconomicEventImpact::High),
                    batch_size: 1000,
                };
                let filter = FilterConfig {
                    allowed_years: Some((2008..=2026).collect::<BTreeSet<_>>()),
                    economic_news_policy: Some(EconomicCalendarPolicy::OnlyWithEvents),
                    ..FilterConfig::default()
                };
                EnvConfig::default()
                    .add_ohlcv_future(source.clone(), ohlcv_1m)
                    .add_ohlcv_future(source.clone(), ohlcv_5m)
                    .with_episode_length(EpisodeLength::Day)
                    .with_filter_config(filter)
                    .add_economic_calendar(source.clone(), calendar)
                    .with_trade_hint(4)
            }
            EnvPreset::BinanceBtcUsdt1dSma20Sma50 => {
                let market_config = OhlcvSpotConfig {
                    broker: DataBroker::Binance,
                    symbol: Symbol::Spot(SpotPair::BtcUsdt),
                    exchange: Some(Exchange::Binance),
                    period: Period::Day(1),
                    batch_size: 1000,
                    indicators: vec![
                        TechnicalIndicator::Sma(SmaWindow(20)),
                        TechnicalIndicator::Sma(SmaWindow(50)),
                    ],
                };
                let filter = FilterConfig {
                    allowed_years: Some((2017..=2026).collect::<BTreeSet<_>>()),
                    ..FilterConfig::default()
                };
                EnvConfig::default()
                    .add_ohlcv_spot(source.clone(), market_config)
                    .with_episode_length(EpisodeLength::Infinite)
                    .with_filter_config(filter)
            }
            EnvPreset::BinanceBtcUsdt1h1mVolumeProfile1d100Usdt => {
                let ohlcv_1h = OhlcvSpotConfig {
                    broker: DataBroker::Binance,
                    symbol: Symbol::Spot(SpotPair::BtcUsdt),
                    exchange: Some(Exchange::Binance),
                    period: Period::Hour(1),
                    batch_size: 1000,
                    indicators: vec![],
                };
                let ohlcv_1m = OhlcvSpotConfig {
                    broker: DataBroker::Binance,
                    symbol: Symbol::Spot(SpotPair::BtcUsdt),
                    exchange: Some(Exchange::Binance),
                    period: Period::Minute(1),
                    batch_size: 1000,
                    indicators: vec![],
                };
                let vp = VolumeProfileSpotConfig {
                    broker: DataBroker::Binance,
                    symbol: Symbol::Spot(SpotPair::BtcUsdt),
                    exchange: Some(Exchange::Binance),
                    aggregation: Some(ProfileAggregation {
                        time_frame: Some(Period::Day(1)),
                        ticks_per_bin: Some(10_000),
                        ..ProfileAggregation::default()
                    }),
                    batch_size: 1000,
                };
                let filter = FilterConfig {
                    allowed_years: Some((2017..=2026).collect::<BTreeSet<_>>()),
                    ..FilterConfig::default()
                };
                EnvConfig::default()
                    .add_ohlcv_spot(source.clone(), ohlcv_1h)
                    .add_ohlcv_spot(source.clone(), ohlcv_1m)
                    .add_volume_profile_spot(source.clone(), vp)
                    .with_episode_length(EpisodeLength::Day)
                    .with_filter_config(filter)
            }
            EnvPreset::BinanceBtcUsdt1h1mTpo1d1Usdt => {
                let ohlcv_1h = OhlcvSpotConfig {
                    broker: DataBroker::Binance,
                    symbol: Symbol::Spot(SpotPair::BtcUsdt),
                    exchange: Some(Exchange::Binance),
                    period: Period::Hour(1),
                    batch_size: 1000,
                    indicators: vec![],
                };
                let ohlcv_1m = OhlcvSpotConfig {
                    broker: DataBroker::Binance,
                    symbol: Symbol::Spot(SpotPair::BtcUsdt),
                    exchange: Some(Exchange::Binance),
                    period: Period::Minute(1),
                    batch_size: 1000,
                    indicators: vec![],
                };
                let tpo = TpoSpotConfig {
                    broker: DataBroker::Binance,
                    symbol: Symbol::Spot(SpotPair::BtcUsdt),
                    exchange: Some(Exchange::Binance),
                    aggregation: Some(ProfileAggregation {
                        time_frame: Some(Period::Day(1)),
                        ticks_per_bin: Some(100),
                        ..ProfileAggregation::default()
                    }),
                    batch_size: 1000,
                };
                let filter = FilterConfig {
                    allowed_years: Some((2017..=2026).collect::<BTreeSet<_>>()),
                    ..FilterConfig::default()
                };
                EnvConfig::default()
                    .add_ohlcv_spot(source.clone(), ohlcv_1h)
                    .add_ohlcv_spot(source.clone(), ohlcv_1m)
                    .add_tpo_spot(source.clone(), tpo)
                    .with_episode_length(EpisodeLength::Day)
                    .with_filter_config(filter)
            }
            EnvPreset::NinjaTraderCme6eh61mTpo1d => {
                let ohlcv = OhlcvFutureConfig {
                    broker: DataBroker::NinjaTrader,
                    symbol: Symbol::Future(FutureContract {
                        root: FutureRoot::EurUsd,
                        month: ContractMonth::June,
                        year: ContractYear::Y6,
                    }),
                    exchange: Some(Exchange::Cme),
                    period: Period::Minute(1),
                    batch_size: 1000,
                    indicators: vec![],
                };
                let tpo = TpoFutureConfig {
                    broker: DataBroker::NinjaTrader,
                    symbol: Symbol::Future(FutureContract {
                        root: FutureRoot::EurUsd,
                        month: ContractMonth::June,
                        year: ContractYear::Y6,
                    }),
                    exchange: Some(Exchange::Cme),
                    aggregation: Some(ProfileAggregation {
                        time_frame: Some(Period::Day(1)),
                        ..ProfileAggregation::default()
                    }),
                    batch_size: 1000,
                };
                let filter = FilterConfig {
                    allowed_years: Some((2006..=2026).collect::<BTreeSet<_>>()),
                    ..FilterConfig::default()
                };
                EnvConfig::default()
                    .add_ohlcv_future(source.clone(), ohlcv)
                    .add_tpo_future(source.clone(), tpo)
                    .with_episode_length(EpisodeLength::Day)
                    .with_filter_config(filter)
            }
        }
    }
}

/// Configuration blueprint for building a trading environment.
///
/// Specifies market data streams, technical indicators, economic events,
/// and simulation parameters needed to construct an Environment.
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
