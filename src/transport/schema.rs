use std::sync::Arc;

use polars::prelude::{DataType, Field, PlSmallStr, Schema, SchemaRef, TimeUnit, TimeZone};
use strum::{Display, EnumString, IntoStaticStr};

/// The standardized vocabulary for all Chapaty market data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display, EnumString, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum CanonicalCol {
    // ========================================================================
    // Identifiers
    // ========================================================================
    /// The eonomic data publisher (e.g., "investingcom", "cftc", "fred").
    DataSource,
    /// Geographic identifier (ISO 3166-1 alpha-2, e.g., "US", "EZ").
    CountryCode,
    /// Currency code (ISO 4217, e.g., "USD").
    CurrencyCode,

    // ========================================================================
    // Time Definitions
    // ========================================================================
    /// The primary index timestamp.
    /// - OHLCV: Close time (When the candle is complete).
    /// - Trade: Trade time.
    /// - TPO/VP: Window End time (When the profile is complete).
    /// - Economic Events: Event Timestamp
    Timestamp,

    /// The start time of an interval.
    /// - OHLCV: Open time.
    /// - TPO/VP: Window Start time.
    OpenTimestamp,

    // ========================================================================
    // Market Data (Price, Volume, Generic)
    // ========================================================================
    /// Temporal granularity.
    /// - Market Data: Candle width ("1m", "1h").
    /// - Econ Calendar: Periodicity ("yoy", "mom", "qoq").
    Period,

    Open,
    High,
    Low,
    Close,
    Price,

    /// Generic Volume.
    /// - OHLCV: Base Volume
    /// - Trade: Quantity
    /// - VP: Base Volume
    Volume,

    // ========================================================================
    // Advanced Spot / Order Flow Metadata
    // ========================================================================
    TradeId,
    QuoteAssetVolume,
    NumberOfTrades,

    // Taker Buy (Aggressor = Buyer)
    TakerBuyBaseAssetVolume,
    TakerBuyQuoteAssetVolume,

    // Taker Sell (Aggressor = Seller) - New for VP
    TakerSellBaseAssetVolume,
    TakerSellQuoteAssetVolume,

    // Trade Counts - New for VP
    NumberOfBuyTrades,
    NumberOfSellTrades,

    // Trade specific flags
    IsBuyerMaker,
    IsBestMatch,

    // ========================================================================
    // Profile Indicators (TPO / Volume Profile)
    // ========================================================================
    /// The lower bound price of the profile bucket.
    PriceBinStart,
    /// The upper bound price of the profile bucket.
    PriceBinEnd,
    /// Number of TPO blocks (time slots) spent at this level.
    TimeSlotCount,

    // ========================================================================
    // Economic Calendar Specific
    // ========================================================================
    /// Broad category (e.g., "inflation", "employment").
    Category,
    /// Specific event type (e.g., "CPI", "NFP").
    NewsType,
    /// Confidence score (0.0 - 1.0) for the inferred NewsType.
    NewsTypeConfidence,
    /// Method used to classify the NewsType (e.g., "ml", "rule").
    NewsTypeSource,
    /// Full display name of the event.
    NewsName,
    /// Economic impact level (1=Low, 2=Medium, 3=High).
    EconomicImpact,

    // === Event Values ===
    /// The actual reported value.
    Actual,
    /// The analyst forecast value.
    Forecast,
    /// The value from the previous period.
    Previous,
}

impl From<CanonicalCol> for PlSmallStr {
    fn from(value: CanonicalCol) -> Self {
        value.as_str().into()
    }
}

impl CanonicalCol {
    pub fn name(&self) -> PlSmallStr {
        (*self).into()
    }

    pub fn as_str(&self) -> &'static str {
        self.into()
    }

    pub fn dtype(&self) -> DataType {
        match self {
            // Strings
            Self::DataSource
            | Self::CountryCode
            | Self::CurrencyCode
            | Self::Category
            | Self::NewsType
            | Self::NewsTypeSource
            | Self::NewsName
            | Self::Period => DataType::String,

            // Integers
            Self::TradeId
            | Self::NumberOfTrades
            | Self::EconomicImpact
            | Self::NumberOfBuyTrades
            | Self::NumberOfSellTrades
            | Self::TimeSlotCount => DataType::Int64,

            // Floats (Prices & Volumes)
            Self::Open
            | Self::High
            | Self::Low
            | Self::Close
            | Self::Price
            | Self::Volume
            | Self::QuoteAssetVolume
            | Self::TakerBuyBaseAssetVolume
            | Self::TakerBuyQuoteAssetVolume
            | Self::TakerSellBaseAssetVolume
            | Self::TakerSellQuoteAssetVolume
            | Self::PriceBinStart
            | Self::PriceBinEnd
            | Self::Actual
            | Self::Forecast
            | Self::Previous
            | Self::NewsTypeConfidence => DataType::Float64,

            // Time
            Self::Timestamp | Self::OpenTimestamp => {
                DataType::Datetime(TimeUnit::Microseconds, Some(TimeZone::UTC))
            }

            // Booleans
            Self::IsBuyerMaker | Self::IsBestMatch => DataType::Boolean,
        }
    }

    pub fn field(&self) -> Field {
        Field::new(self.name(), self.dtype())
    }
}

pub fn ohlcv_future_schema() -> SchemaRef {
    let s = Schema::from_iter([
        CanonicalCol::OpenTimestamp.field(),
        CanonicalCol::Open.field(),
        CanonicalCol::High.field(),
        CanonicalCol::Low.field(),
        CanonicalCol::Close.field(),
        CanonicalCol::Volume.field(),
        CanonicalCol::Timestamp.field(), // CloseTimestamp
    ]);

    Arc::new(s)
}

pub fn ohlcv_spot_schema() -> SchemaRef {
    let s = Schema::from_iter([
        CanonicalCol::OpenTimestamp.field(),
        CanonicalCol::Open.field(),
        CanonicalCol::High.field(),
        CanonicalCol::Low.field(),
        CanonicalCol::Close.field(),
        CanonicalCol::Volume.field(),
        CanonicalCol::Timestamp.field(), // CloseTimestamp
        CanonicalCol::QuoteAssetVolume.field(),
        CanonicalCol::NumberOfTrades.field(),
        CanonicalCol::TakerBuyBaseAssetVolume.field(),
        CanonicalCol::TakerBuyQuoteAssetVolume.field(),
    ]);

    Arc::new(s)
}

pub fn trades_spot_schema() -> SchemaRef {
    let s = Schema::from_iter([
        CanonicalCol::TradeId.field(),
        CanonicalCol::Price.field(),
        CanonicalCol::Volume.field(),           // Maps to 'quantity'
        CanonicalCol::QuoteAssetVolume.field(), // Maps to 'quote_quantity'
        CanonicalCol::Timestamp.field(),        // Maps to 'trade_timestamp'
        CanonicalCol::IsBuyerMaker.field(),
        CanonicalCol::IsBestMatch.field(),
    ]);

    Arc::new(s)
}

pub fn volume_profile_spot_schema() -> SchemaRef {
    let s = Schema::from_iter([
        // Window Metadata
        CanonicalCol::OpenTimestamp.field(), // window_start
        CanonicalCol::Timestamp.field(),     // window_end
        // Profile Bins
        CanonicalCol::PriceBinStart.field(),
        CanonicalCol::PriceBinEnd.field(),
        // Volume Data
        CanonicalCol::Volume.field(), // base_volume
        CanonicalCol::TakerBuyBaseAssetVolume.field(),
        CanonicalCol::TakerSellBaseAssetVolume.field(),
        CanonicalCol::QuoteAssetVolume.field(), // quote_volume
        CanonicalCol::TakerBuyQuoteAssetVolume.field(),
        CanonicalCol::TakerSellQuoteAssetVolume.field(),
        // Trade Counts
        CanonicalCol::NumberOfTrades.field(),
        CanonicalCol::NumberOfBuyTrades.field(),
        CanonicalCol::NumberOfSellTrades.field(),
    ]);

    Arc::new(s)
}

pub fn tpo_spot_schema() -> SchemaRef {
    let s = Schema::from_iter([
        // Window Metadata
        CanonicalCol::OpenTimestamp.field(), // window_start
        CanonicalCol::Timestamp.field(),     // window_end
        // TPO Data
        CanonicalCol::PriceBinStart.field(),
        CanonicalCol::PriceBinEnd.field(),
        CanonicalCol::TimeSlotCount.field(),
    ]);
    Arc::new(s)
}

pub fn tpo_future_schema() -> SchemaRef {
    let s = Schema::from_iter([
        // Window Metadata
        CanonicalCol::OpenTimestamp.field(), // window_start
        CanonicalCol::Timestamp.field(),     // window_end
        // TPO Data
        CanonicalCol::PriceBinStart.field(),
        CanonicalCol::PriceBinEnd.field(),
        CanonicalCol::TimeSlotCount.field(),
    ]);

    Arc::new(s)
}

/// Schema for Investing.com Economic Calendar data.
///
/// Maps specific SQL concepts to Canonical Columns:
/// - `event_timestamp` -> `Timestamp` (The primary index)
/// - `periodicity`     -> `Period` (The frequency, e.g., "yoy")
/// - `data_source`     -> `DataSource`
pub fn economic_calendar_schema() -> SchemaRef {
    let s = Schema::from_iter([
        // Metadata & Identifiers
        CanonicalCol::DataSource.field(),
        CanonicalCol::Category.field(),
        // Primary Time Index (event_timestamp)
        CanonicalCol::Timestamp.field(),
        // Classification Metadata
        CanonicalCol::NewsType.field(),
        CanonicalCol::NewsTypeConfidence.field(), // maps to news_type_confidence
        CanonicalCol::NewsTypeSource.field(),     // maps to news_type_source
        // Event Details
        CanonicalCol::Period.field(), // maps to periodicity
        CanonicalCol::NewsName.field(),
        CanonicalCol::CountryCode.field(),
        CanonicalCol::CurrencyCode.field(),
        CanonicalCol::EconomicImpact.field(),
        // Values
        CanonicalCol::Actual.field(),
        CanonicalCol::Forecast.field(),
        CanonicalCol::Previous.field(),
    ]);

    Arc::new(s)
}
