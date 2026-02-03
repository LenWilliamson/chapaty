use std::{fmt::Debug, hash::Hash};

use serde::{Deserialize, Serialize};

use crate::{
    data::{
        common::ProfileAggregation,
        domain::{
            CountryCode, DataBroker, EconomicCategory, EconomicDataSource, EconomicEventImpact,
            Exchange, Period, Symbol,
        },
        event::{EconomicCalendarId, OhlcvId, TpoId, TradesId, VolumeProfileId},
        indicator::{EmaWindow, RsiWindow, SmaWindow, TechnicalIndicator},
    },
    error::ChapatyResult,
};

// ================================================================================================
// OHLCV Configurations
// ================================================================================================

/// Configuration for retrieving OHLCV (Open, High, Low, Close, Volume) data from spot markets.
///
/// OHLCV data represents aggregated price and volume information over specified time periods,
/// commonly used for candlestick charts and technical analysis.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OhlcvSpotConfig {
    /// The data broker to query from.
    pub broker: DataBroker,

    /// The trading pair symbol (e.g., "btc-usdt", "eth-usdt").
    pub symbol: Symbol,

    /// Optional exchange name. If `None`, defaults to the broker's primary exchange.
    pub exchange: Option<Exchange>,

    /// The timeframe for each OHLCV candle (e.g., "1m", "5m", "1h", "1d").
    pub period: Period,

    /// Number of records to stream per batch.
    ///
    /// Valid range: 100-10000. Defaults to 1000 if not specified.
    pub batch_size: i32,

    // Data configurations that support derived technical analysis.
    pub indicators: Vec<TechnicalIndicator>,
}

/// Configuration for retrieving OHLCV (Open, High, Low, Close, Volume) data from futures markets.
///
/// Similar to spot OHLCV data, but specifically for futures contracts which include
/// additional fields like open interest and funding rates.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OhlcvFutureConfig {
    /// The data broker to query from.
    pub broker: DataBroker,

    /// The futures contract symbol. Must be a valid futures symbol format.
    pub symbol: Symbol,

    /// Optional exchange name. If `None`, defaults to the broker's primary exchange.
    pub exchange: Option<Exchange>,

    /// The timeframe for each OHLCV candle (e.g., "1m", "5m", "1h", "1d").
    pub period: Period,

    /// Number of records to stream per batch.
    ///
    /// Valid range: 100-10000. Defaults to 1000 if not specified.
    pub batch_size: i32,

    // Data configurations that support derived technical analysis.
    pub indicators: Vec<TechnicalIndicator>,
}

// ================================================================================================
// Trade Data Configuration
// ================================================================================================

/// Configuration for retrieving trade-level spot market data.
///
/// Trade data represents individual trades or price updates at the finest granularity,
/// capturing every market transaction with microsecond precision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TradeSpotConfig {
    /// The data broker to query from.
    pub broker: DataBroker,

    /// The trading pair symbol (e.g., "btc-usdt", "eth-usdt").
    pub symbol: Symbol,

    /// Optional exchange name. If `None`, defaults to the broker's primary exchange.
    pub exchange: Option<Exchange>,

    /// Number of records to stream per batch.
    ///
    /// Valid range: 100-10000. Defaults to 1000 if not specified.
    /// Consider larger batch sizes for trade data to optimize throughput.
    pub batch_size: i32,
}

// ================================================================================================
// TPO Configurations
// ================================================================================================

/// Configuration for retrieving Time Price Opportunity (TPO) data from spot markets.
///
/// TPO, also known as Market Profile, displays market activity organized by price level
/// and time, showing where trading activity has occurred and helping identify
/// key support/resistance levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TpoSpotConfig {
    /// The data broker to query from.
    pub broker: DataBroker,

    /// The trading pair symbol (e.g., "btc-usdt", "eth-usdt").
    pub symbol: Symbol,

    /// Optional exchange name. If `None`, defaults to the broker's primary exchange.
    pub exchange: Option<Exchange>,

    /// Optional aggregation parameters for profile construction.
    ///
    /// If `None`, uses default aggregation (1m timeframe, finest price granularity).
    pub aggregation: Option<ProfileAggregation>,

    /// Number of records to stream per batch.
    ///
    /// Valid range: 100-10000. Defaults to 1000 if not specified.
    pub batch_size: i32,
}

/// Configuration for retrieving Time Price Opportunity (TPO) data from futures markets.
///
/// TPO data for futures markets, providing Market Profile insights for futures contracts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TpoFutureConfig {
    /// The data broker to query from.
    pub broker: DataBroker,

    /// The futures contract symbol.
    pub symbol: Symbol,

    /// Optional exchange name. If `None`, defaults to the broker's primary exchange.
    pub exchange: Option<Exchange>,

    /// Optional aggregation parameters for profile construction.
    ///
    /// If `None`, uses default aggregation (1m timeframe, finest price granularity).
    pub aggregation: Option<ProfileAggregation>,

    /// Number of records to stream per batch.
    ///
    /// Valid range: 100-10000. Defaults to 1000 if not specified.
    pub batch_size: i32,
}

// ================================================================================================
// Volume Profile Configuration
// ================================================================================================

/// Configuration for retrieving Volume Profile data from spot markets.
///
/// Volume Profile shows the distribution of trading volume across different price levels,
/// helping identify high-volume nodes (HVN) and low-volume nodes (LVN) that often act
/// as support or resistance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct VolumeProfileSpotConfig {
    /// The data broker to query from.
    pub broker: DataBroker,

    /// The trading pair symbol (e.g., "btc-usdt", "eth-usdt").
    pub symbol: Symbol,

    /// Optional exchange name. If `None`, defaults to the broker's primary exchange.
    pub exchange: Option<Exchange>,

    /// Optional aggregation parameters for profile construction.
    ///
    /// If `None`, uses default aggregation (1m timeframe, finest price granularity).
    pub aggregation: Option<ProfileAggregation>,

    /// Number of records to stream per batch.
    ///
    /// Valid range: 100-10000. Defaults to 1000 if not specified.
    pub batch_size: i32,
}

// ================================================================================================
// Economic Calendar Configuration
// ================================================================================================

/// Configuration for retrieving economic calendar events.
///
/// Economic calendar data provides scheduled releases of economic indicators,
/// central bank announcements, and other market-moving events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EconomicCalendarConfig {
    /// The data broker to query from.
    pub broker: DataBroker,

    /// Optional filter by specific data source (e.g., "investingcom", "fred").
    ///
    /// If `None`, retrieves data from all available sources for the broker.
    pub data_source: Option<EconomicDataSource>,

    /// Optional filter by country using ISO 3166-1 alpha-2 code (e.g., "US", "GB", "JP").
    ///
    /// Special code "EZ" represents the Euro Zone. Country codes must be uppercase.
    /// If `None`, retrieves data for all countries.
    pub country_code: Option<CountryCode>,

    /// Optional filter by economic category.
    ///
    /// If `None`, retrieves events across all categories.
    pub category: Option<EconomicCategory>,

    /// Optional filter by economic importance.
    ///
    /// If `None`, retrieves any importance.
    pub importance: Option<EconomicEventImpact>,

    /// Number of records to stream per batch.
    ///
    /// Valid range: 100-10000. Defaults to 1000 if not specified.
    pub batch_size: i32,
}

// ================================================================================================
// Traits
// ================================================================================================

/// A trait for data configurations that support derived technical analysis.
///
/// This trait enables fluent, compile-time checked configuration of indicators
/// on OHLCV data streams.
pub trait TechnicalAnalysis {
    /// Adds a technical indicator to be computed for this data stream.
    fn add_indicator(&mut self, kind: TechnicalIndicator);

    /// Fluent builder version of `add_indicator`.
    ///
    /// # Example
    /// ```
    /// # use chapaty::prelude::*;
    /// let config = OhlcvSpotConfig {
    ///     broker: DataBroker::Binance,
    ///     symbol: Symbol::Spot(SpotPair::BtcUsdt),
    ///     exchange: None,
    ///     period: Period::Minute(1),
    ///     batch_size: 1000,
    ///     indicators: vec![],
    /// }
    /// .with_indicator(TechnicalIndicator::Sma(SmaWindow(20)));
    /// ```
    fn with_indicator(mut self, kind: TechnicalIndicator) -> Self
    where
        Self: Sized,
    {
        self.add_indicator(kind);
        self
    }

    // === Ergonomic Sugar Helpers ===

    /// Adds a Simple Moving Average (SMA) indicator.
    ///
    /// # Example
    /// ```
    /// # use chapaty::prelude::*;
    /// let config = OhlcvSpotConfig {
    ///     broker: DataBroker::Binance,
    ///     symbol: Symbol::Spot(SpotPair::BtcUsdt),
    ///     exchange: None,
    ///     period: Period::Minute(1),
    ///     batch_size: 1000,
    ///     indicators: vec![],
    /// }
    /// .with_sma(20);
    /// ```
    fn with_sma(self, window: u16) -> Self
    where
        Self: Sized,
    {
        self.with_indicator(TechnicalIndicator::Sma(SmaWindow(window)))
    }

    /// Adds an Exponential Moving Average (EMA) indicator.
    ///
    /// # Example
    /// ```
    /// # use chapaty::prelude::*;
    /// let config = OhlcvSpotConfig {
    ///     broker: DataBroker::Binance,
    ///     symbol: Symbol::Spot(SpotPair::BtcUsdt),
    ///     exchange: None,
    ///     period: Period::Minute(1),
    ///     batch_size: 1000,
    ///     indicators: vec![],
    /// }
    /// .with_ema(12);
    /// ```
    fn with_ema(self, window: u16) -> Self
    where
        Self: Sized,
    {
        self.with_indicator(TechnicalIndicator::Ema(EmaWindow(window)))
    }

    /// Adds a Relative Strength Index (RSI) indicator.
    ///
    /// # Example
    /// ```
    /// # use chapaty::prelude::*;
    /// let config = OhlcvSpotConfig {
    ///     broker: DataBroker::Binance,
    ///     symbol: Symbol::Spot(SpotPair::BtcUsdt),
    ///     exchange: None,
    ///     period: Period::Minute(1),
    ///     batch_size: 1000,
    ///     indicators: vec![],
    /// }
    /// .with_rsi(14);
    /// ```
    fn with_rsi(self, window: u16) -> Self
    where
        Self: Sized,
    {
        self.with_indicator(TechnicalIndicator::Rsi(RsiWindow(window)))
    }

    // === Multi-indicator Helpers ===

    /// Adds multiple indicators at once.
    ///
    /// # Example
    /// ```
    /// # use chapaty::prelude::*;
    /// let config = OhlcvSpotConfig {
    ///     broker: DataBroker::Binance,
    ///     symbol: Symbol::Spot(SpotPair::BtcUsdt),
    ///     exchange: None,
    ///     period: Period::Minute(1),
    ///     batch_size: 1000,
    ///     indicators: vec![],
    /// }
    /// .with_indicators(vec![
    ///     TechnicalIndicator::Sma(SmaWindow(20)),
    ///     TechnicalIndicator::Ema(EmaWindow(12)),
    /// ]);
    /// ```
    fn with_indicators(mut self, kinds: Vec<TechnicalIndicator>) -> Self
    where
        Self: Sized,
    {
        for kind in kinds {
            self.add_indicator(kind);
        }
        self
    }

    /// Chainable helper to add multiple SMAs.
    ///
    /// # Example
    /// ```
    /// # use chapaty::prelude::*;
    /// let config = OhlcvSpotConfig {
    ///     broker: DataBroker::Binance,
    ///     symbol: Symbol::Spot(SpotPair::BtcUsdt),
    ///     exchange: None,
    ///     period: Period::Minute(1),
    ///     batch_size: 1000,
    ///     indicators: vec![],
    /// }
    /// .with_smas(&[20, 50, 200]);
    /// ```
    fn with_smas(mut self, windows: &[u16]) -> Self
    where
        Self: Sized,
    {
        for &window in windows {
            self.add_indicator(TechnicalIndicator::Sma(SmaWindow(window)));
        }
        self
    }

    /// Chainable helper to add multiple EMAs.
    fn with_emas(mut self, windows: &[u16]) -> Self
    where
        Self: Sized,
    {
        for &window in windows {
            self.add_indicator(TechnicalIndicator::Ema(EmaWindow(window)));
        }
        self
    }
}

impl TechnicalAnalysis for OhlcvSpotConfig {
    fn add_indicator(&mut self, kind: TechnicalIndicator) {
        self.indicators.push(kind);
    }
}

impl TechnicalAnalysis for OhlcvFutureConfig {
    fn add_indicator(&mut self, kind: TechnicalIndicator) {
        self.indicators.push(kind);
    }
}

/// Maps a configuration type to its corresponding stream identifier.
///
/// This trait enables type-safe conversion from user-facing configuration
/// (which includes wire protocol details like batch_size) to internal
/// domain identifiers used for stream management.
pub trait ConfigId {
    /// The unique identifier type for this configuration's data stream.
    type Id: Copy + PartialEq + Eq + Hash + PartialOrd + Ord + Debug + Send + Sync;

    /// Converts this configuration into its corresponding stream identifier.
    ///
    /// This method extracts only the fields that uniquely identify a data stream,
    /// omitting operational parameters like batch_size or indicators.
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration contains invalid broker/exchange
    /// combinations or if required conversions fail.
    fn to_id(&self) -> ChapatyResult<Self::Id>;
}
impl ConfigId for OhlcvSpotConfig {
    type Id = OhlcvId;

    fn to_id(&self) -> ChapatyResult<Self::Id> {
        let exchange = match self.exchange {
            Some(ex) => ex,
            None => self.broker.try_into()?,
        };

        Ok(OhlcvId {
            broker: self.broker,
            exchange,
            symbol: self.symbol,
            period: self.period,
        })
    }
}

impl ConfigId for OhlcvFutureConfig {
    type Id = OhlcvId;

    fn to_id(&self) -> ChapatyResult<Self::Id> {
        let exchange = match self.exchange {
            Some(ex) => ex,
            None => self.broker.try_into()?,
        };

        Ok(OhlcvId {
            broker: self.broker,
            exchange,
            symbol: self.symbol,
            period: self.period,
        })
    }
}

impl ConfigId for TradeSpotConfig {
    type Id = TradesId;

    fn to_id(&self) -> ChapatyResult<Self::Id> {
        let exchange = match self.exchange {
            Some(ex) => ex,
            None => self.broker.try_into()?,
        };

        Ok(TradesId {
            broker: self.broker,
            exchange,
            symbol: self.symbol,
        })
    }
}

impl ConfigId for TpoSpotConfig {
    type Id = TpoId;

    fn to_id(&self) -> ChapatyResult<Self::Id> {
        let exchange = match self.exchange {
            Some(ex) => ex,
            None => self.broker.try_into()?,
        };

        Ok(TpoId {
            broker: self.broker,
            exchange,
            symbol: self.symbol,
            aggregation: self.aggregation.unwrap_or_default(),
        })
    }
}

impl ConfigId for TpoFutureConfig {
    type Id = TpoId;

    fn to_id(&self) -> ChapatyResult<Self::Id> {
        let exchange = match self.exchange {
            Some(ex) => ex,
            None => self.broker.try_into()?,
        };

        Ok(TpoId {
            broker: self.broker,
            exchange,
            symbol: self.symbol,
            aggregation: self.aggregation.unwrap_or_default(),
        })
    }
}

impl ConfigId for VolumeProfileSpotConfig {
    type Id = VolumeProfileId;

    fn to_id(&self) -> ChapatyResult<Self::Id> {
        let exchange = match self.exchange {
            Some(ex) => ex,
            None => self.broker.try_into()?,
        };

        Ok(VolumeProfileId {
            broker: self.broker,
            exchange,
            symbol: self.symbol,
            aggregation: self.aggregation.unwrap_or_default(),
        })
    }
}

impl ConfigId for EconomicCalendarConfig {
    type Id = EconomicCalendarId;

    fn to_id(&self) -> ChapatyResult<Self::Id> {
        Ok(EconomicCalendarId {
            broker: self.broker,
            data_source: self.data_source,
            country_code: self.country_code,
            category: self.category,
            importance: self.importance,
        })
    }
}
