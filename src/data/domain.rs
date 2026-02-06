use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr, sync::Arc};
use strum::{Display, EnumIter, IntoStaticStr};
use strum_macros::EnumString;

use crate::{
    error::{ChapatyError, DataError, TransportError},
    generated::chapaty::{
        bq_exporter::v1::EconomicCategory as RpcEconomicCategory,
        bq_exporter::v1::EconomicImportance as RpcEconomicImportance,
        data::v1::DataBroker as RpcDataBroker,
    },
    impl_abs_primitive, impl_add_sub_mul_div_primitive, impl_from_primitive, impl_neg_primitive,
};

// ================================================================================================
// Domain Strong Types (NewTypes)
// ================================================================================================

/// Represents a price level in the quote currency.
///
/// Used for: Open, High, Low, Close, Trade Price, Stops, and Take Profits.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Default, Serialize, Deserialize)]
pub struct Price(pub f64);
impl_from_primitive!(Price, f64);
impl_add_sub_mul_div_primitive!(Price, f64);
impl_neg_primitive!(Price, f64);
impl_abs_primitive!(Price, f64);

/// Represents the smallest discrete movement of an asset.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Default, Serialize, Deserialize)]
pub struct Tick(pub i64);
impl_from_primitive!(Tick, i64);
impl_add_sub_mul_div_primitive!(Tick, i64);
impl_neg_primitive!(Tick, i64);
impl_abs_primitive!(Tick, i64);

/// Represents a precise amount of the **Base Asset**.
///
/// This is the fundamental unit for Orders (Quantity), Trades (Size), and
/// Market Data (Volume). It wraps `f64` to support fractional assets while
/// providing strong typing against Price or other metrics.
///
/// # Semantics
/// - **Negative values** are generally not allowed in storage but may appear
///   in delta calculations.
/// - **Precision** is handled via standard `f64` IEEE-754 semantics.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Default, Serialize, Deserialize)]
pub struct Quantity(pub f64);
impl_from_primitive!(Quantity, f64);
impl_add_sub_mul_div_primitive!(Quantity, f64);

/// Semantic alias for `Quantity` when referring to aggregated market activity.
///
/// Use this in contexts like `Ohlcv` or `DailyStats` to indicate the data
/// represents a summation of trades, rather than a single order size.
pub type Volume = Quantity;

/// Represents a generic count (e.g., number of trades, TPO slots).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Serialize, Deserialize)]
pub struct Count(pub i64);
impl_from_primitive!(Count, i64);

/// Represents a unique trade identifier from the exchange.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default, Serialize, Deserialize,
)]
pub struct TradeId(pub i64);
impl_from_primitive!(TradeId, i64);

/// Represents the sequential index of a generic timeframe (e.g., "Week 42").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TimeframeIdx(pub u32);
impl_from_primitive!(TimeframeIdx, u32);

/// Represents a macro-economic indicator value (Actual, Forecast, Previous).
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Default, Serialize, Deserialize)]
pub struct EconomicValue(pub f64);
impl_from_primitive!(EconomicValue, f64);
impl_add_sub_mul_div_primitive!(EconomicValue, f64);

/// Represents the directional outcome of a candlestick,
/// based on the relationship between its open and close prices.
///
/// - [`CandleDirection::Bullish`] — the close is higher than the open.
/// - [`CandleDirection::Bearish`] — the close is lower than the open.
/// - [`CandleDirection::Doji`] — the open and close are equal (indecision).
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    PartialOrd,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    EnumString,
    EnumIter,
)]
#[strum(serialize_all = "lowercase")]
pub enum CandleDirection {
    /// Close > Open (upward / bullish candle)
    Bullish,
    /// Close < Open (downward / bearish candle)
    Bearish,
    /// Close == Open (neutral / indecision candle, often called a "doji")
    Doji,
}

/// Represents the aggressor side of a trade (the "Taker").
///
/// This is the side that "crossed the spread" to make the trade happen.
/// - **Buy:** A market buy order lifted an ask.
/// - **Sell:** A market sell order hit a bid.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    PartialOrd,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    EnumString,
    EnumIter,
)]
#[strum(serialize_all = "lowercase")]
pub enum TradeSide {
    Buy,
    Sell,
}

/// Indicates which side of the Order Book provided the liquidity.
///
/// This replaces the raw `is_buyer_maker` boolean:
/// - `true`  -> `LiquiditySide::Bid` (Maker was Buyer, Aggressor Sold)
/// - `false` -> `LiquiditySide::Ask` (Maker was Seller, Aggressor Bought)
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    PartialOrd,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    EnumString,
    EnumIter,
)]
#[strum(serialize_all = "lowercase")]
pub enum LiquiditySide {
    /// The liquidity was provided by a resting Buy order on the **Bid**.
    /// The Aggressor (Taker) was a Seller.
    Bid,

    /// The liquidity was provided by a resting Sell order on the **Ask**.
    /// The Aggressor (Taker) was a Buyer.
    Ask,
}

impl LiquiditySide {
    /// Returns the side of the aggressor (Taker).
    ///
    /// This is the "Trade Side" usually displayed in UI (Green/Red).
    ///
    /// # Logic
    /// * If Maker = Buyer, then Aggressor = **Sell** (Red).
    /// * If Maker = Seller, then Aggressor = **Buy** (Green).
    pub fn trade_side(&self) -> TradeSide {
        match self {
            LiquiditySide::Bid => TradeSide::Sell,
            LiquiditySide::Ask => TradeSide::Buy,
        }
    }
}

// === Serialization Glue (Bool <-> Enum) ===

impl From<bool> for LiquiditySide {
    fn from(value: bool) -> Self {
        if value {
            LiquiditySide::Bid
        } else {
            LiquiditySide::Ask
        }
    }
}

impl From<LiquiditySide> for bool {
    fn from(value: LiquiditySide) -> Self {
        match value {
            LiquiditySide::Bid => true,
            LiquiditySide::Ask => false,
        }
    }
}

/// Indicates the depth of the Order Book where the trade execution occurred.
///
/// This provides insight into the "aggressiveness" of the trade and the
/// liquidity state at the time of execution.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    PartialOrd,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    EnumString,
    EnumIter,
)]
#[strum(serialize_all = "lowercase")]
pub enum ExecutionDepth {
    /// The trade matched exactly at the Best Bid or Best Ask (BBO).
    ///
    /// This indicates normal liquidity consumption where the resting order
    /// at the top of the book was sufficient to fill this portion of the trade.
    TopOfBook,

    /// The trade swept through the top of the book and matched at a worse price.
    ///
    /// This indicates that the Aggressor's order size exceeded the liquidity
    /// available at the BBO, forcing the engine to match against deeper
    /// levels of the order book (slippage).
    BookSweep,
}

impl From<bool> for ExecutionDepth {
    /// Converts a boolean value to a `TradeMatchQuality`.
    ///
    /// If `true`, it indicates that the trade was filled at the best available price (`BestMatch`).
    /// If `false`, it indicates that the best available quantity was insufficient (`NotBestMatch`).
    fn from(is_best_match: bool) -> Self {
        if is_best_match {
            ExecutionDepth::TopOfBook
        } else {
            ExecutionDepth::BookSweep
        }
    }
}

impl From<ExecutionDepth> for bool {
    /// Converts a `TradeMatchQuality` into a boolean value.
    ///
    /// `TradeMatchQuality::BestMatch` converts to `true`, meaning that the trade was filled entirely
    /// at the best available price.
    /// `TradeMatchQuality::NotBestMatch` converts to `false`, indicating that the best available quantity
    /// was insufficient and additional price levels were used.
    fn from(trade_match_quality: ExecutionDepth) -> Self {
        match trade_match_quality {
            ExecutionDepth::TopOfBook => true,
            ExecutionDepth::BookSweep => false,
        }
    }
}

#[derive(
    Copy,
    Clone,
    Debug,
    EnumString,
    EnumIter,
    Display,
    PartialEq,
    Eq,
    Hash,
    Deserialize,
    Serialize,
    PartialOrd,
    Ord,
)]
#[strum(serialize_all = "lowercase")]
pub enum DataBroker {
    NinjaTrader,
    Binance,
    InvestingCom,
}

impl DataBroker {
    pub fn supports_economic_calendar(&self) -> bool {
        matches!(self, DataBroker::InvestingCom)
    }
}

impl From<DataBroker> for RpcDataBroker {
    fn from(broker: DataBroker) -> Self {
        match broker {
            DataBroker::Binance => RpcDataBroker::Binance,
            DataBroker::NinjaTrader => RpcDataBroker::NinjaTrader,
            DataBroker::InvestingCom => RpcDataBroker::InvestingCom,
        }
    }
}

impl TryFrom<RpcDataBroker> for DataBroker {
    type Error = ChapatyError;

    fn try_from(proto: RpcDataBroker) -> Result<Self, Self::Error> {
        match proto {
            RpcDataBroker::Binance => Ok(DataBroker::Binance),
            RpcDataBroker::NinjaTrader => Ok(DataBroker::NinjaTrader),
            RpcDataBroker::InvestingCom => Ok(DataBroker::InvestingCom),

            // Handle the 0-value case explicitly
            RpcDataBroker::Unspecified => Err(TransportError::RpcTypeNotFound(
                "Broker cannot be unspecified in this context".to_string(),
            )
            .into()),
        }
    }
}

#[derive(
    Copy,
    Clone,
    Debug,
    EnumString,
    Display,
    PartialEq,
    Eq,
    Hash,
    Deserialize,
    Serialize,
    PartialOrd,
    Ord,
)]
#[strum(serialize_all = "lowercase")]
pub enum Exchange {
    Cme,
    Binance,
}

impl TryFrom<DataBroker> for Exchange {
    type Error = ChapatyError;

    fn try_from(broker: DataBroker) -> Result<Self, Self::Error> {
        match broker {
            DataBroker::NinjaTrader => Ok(Exchange::Cme),
            DataBroker::Binance => Ok(Exchange::Binance),
            DataBroker::InvestingCom => Err(DataError::UnexpectedEnumVariant(format!(
                "{} does not map to an exchange",
                broker
            ))
            .into()),
        }
    }
}

// The eonomic data publisher (e.g., "investingcom", "cftc", "fred").
#[derive(
    Copy,
    Clone,
    Debug,
    EnumString,
    Display,
    PartialEq,
    Eq,
    Hash,
    Deserialize,
    Serialize,
    PartialOrd,
    Ord,
)]
#[strum(serialize_all = "lowercase")]
pub enum EconomicDataSource {
    InvestingCom,
}

impl TryFrom<DataBroker> for EconomicDataSource {
    type Error = ChapatyError;

    fn try_from(broker: DataBroker) -> Result<Self, Self::Error> {
        match broker {
            DataBroker::InvestingCom => Ok(EconomicDataSource::InvestingCom),
            DataBroker::NinjaTrader | DataBroker::Binance => Err(DataError::UnexpectedEnumVariant(
                format!("{} does not map to an economic data source", broker),
            )
            .into()),
        }
    }
}

#[derive(
    Copy,
    Clone,
    Debug,
    Hash,
    PartialEq,
    Eq,
    Deserialize,
    Serialize,
    PartialOrd,
    Ord,
    EnumIter,
    EnumString,
    Display,
    IntoStaticStr,
)]
#[strum(serialize_all = "lowercase")]
pub enum Period {
    #[strum(serialize = "{0}h")]
    Hour(u8),
    #[strum(serialize = "{0}m")]
    Minute(u8),
    #[strum(serialize = "{0}d")]
    Day(u8),
    #[strum(serialize = "{0}mo")]
    Month(u8),
    #[strum(serialize = "{0}s")]
    Second(u8),
    #[strum(serialize = "{0}w")]
    Week(u8),
}

/// Represents the classification of a market.
#[derive(
    Copy,
    Clone,
    Debug,
    Hash,
    PartialEq,
    Eq,
    Deserialize,
    Serialize,
    PartialOrd,
    Ord,
    EnumIter,
    EnumString,
    Display,
    IntoStaticStr,
)]
#[strum(serialize_all = "lowercase")]
pub enum MarketType {
    Spot,
    Future,
    FixedIncome,
}

impl From<Symbol> for MarketType {
    fn from(value: Symbol) -> Self {
        match value {
            Symbol::Future(_) => Self::Future,
            Symbol::Spot(_) => Self::Spot,
            Symbol::Bond(_) => Self::FixedIncome,
        }
    }
}

impl From<&Symbol> for MarketType {
    fn from(value: &Symbol) -> Self {
        (*value).into()
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord, IntoStaticStr,
)]
pub enum Symbol {
    Spot(SpotPair),
    Future(FutureContract),
    /// Fixed Income (identified by ISIN)
    Bond(BondSpec), 
}

impl fmt::Display for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Spot(s) => write!(f, "{}", s),
            Self::Future(s) => write!(f, "{}", s),
            Self::Bond(s) => write!(f, "{}", s.0),
        }
    }
}

impl FromStr for Symbol {
    type Err = ChapatyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Try parsing as SpotPair first
        if let Ok(spot) = SpotPair::from_str(s) {
            return Ok(Symbol::Spot(spot));
        }

        // Try parsing as FutureContract
        if let Ok(future) = FutureContract::from_str(s) {
            return Ok(Symbol::Future(future));
        }

        Err(DataError::InvalidSymbol(s.to_string()).into())
    }
}

impl Symbol {
    pub fn market_type(&self) -> MarketType {
        self.into()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, PartialOrd)]
pub struct BondSpec {
    pub isin: Isin, 
    pub currency: Currency,
    pub tick_size: f64, 
    pub face_value: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct Isin(pub Arc<String>);

#[derive(
    Copy,
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    Display,
    EnumString,
    Serialize,
    Deserialize,
    PartialOrd,
    Ord,
    IntoStaticStr,
)]
#[strum(serialize_all = "lowercase")]
pub enum Currency {
    Usd,
    Eur,
    Chf,
}

#[derive(
    Copy,
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    Display,
    EnumString,
    Serialize,
    Deserialize,
    PartialOrd,
    Ord,
    IntoStaticStr,
)]
#[strum(serialize_all = "kebab-case")]
pub enum SpotPair {
    BtcUsdt,
    BnbUsdt,
    EthUsdt,
    SolUsdt,
    XrpUsdt,
    TrxUsdt,
    AdaUsdt,
    XlmUsdt,
}

/// Standard CME/Industry single-letter month codes.
#[derive(
    Clone,
    Copy,
    Debug,
    Display,
    EnumString,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    PartialOrd,
    Ord,
)]
#[strum(serialize_all = "lowercase")]
pub enum ContractMonth {
    #[strum(serialize = "f")]
    January = 1,
    #[strum(serialize = "g")]
    February = 2,
    #[strum(serialize = "h")]
    March = 3,
    #[strum(serialize = "j")]
    April = 4,
    #[strum(serialize = "k")]
    May = 5,
    #[strum(serialize = "m")]
    June = 6,
    #[strum(serialize = "n")]
    July = 7,
    #[strum(serialize = "q")]
    August = 8,
    #[strum(serialize = "u")]
    September = 9,
    #[strum(serialize = "v")]
    October = 10,
    #[strum(serialize = "x")]
    November = 11,
    #[strum(serialize = "z")]
    December = 12,
}

/// Represents the underlying product code (CME "Root").
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Display,
    EnumString,
    Serialize,
    Deserialize,
    PartialOrd,
    Ord,
)]
#[strum(serialize_all = "lowercase")]
pub enum FutureRoot {
    #[strum(serialize = "6a")]
    AudUsd,
    #[strum(serialize = "6b")]
    GbpUsd,
    #[strum(serialize = "6c")]
    CadUsd,
    #[strum(serialize = "6e")]
    EurUsd,
    #[strum(serialize = "6j")]
    JpyUsd,
    #[strum(serialize = "6n")]
    NzdUsd,
    #[strum(serialize = "btc")]
    Btc,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Display,
    EnumString,
    Serialize,
    Deserialize,
    PartialOrd,
    Ord,
)]
#[strum(serialize_all = "lowercase")]
pub enum ContractYear {
    #[strum(serialize = "0")]
    Y0 = 0,
    #[strum(serialize = "1")]
    Y1 = 1,
    #[strum(serialize = "2")]
    Y2 = 2,
    #[strum(serialize = "3")]
    Y3 = 3,
    #[strum(serialize = "4")]
    Y4 = 4,
    #[strum(serialize = "5")]
    Y5 = 5,
    #[strum(serialize = "6")]
    Y6 = 6,
    #[strum(serialize = "7")]
    Y7 = 7,
    #[strum(serialize = "8")]
    Y8 = 8,
    #[strum(serialize = "9")]
    Y9 = 9,
}

/// A concrete Futures contract (e.g., "6ez5").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct FutureContract {
    pub root: FutureRoot,
    pub month: ContractMonth,
    pub year: ContractYear,
}

impl fmt::Display for FutureContract {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}{}", self.root, self.month, self.year)
    }
}

impl FromStr for FutureContract {
    type Err = ChapatyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_lowercase();

        // Expected format: root + month + year
        // e.g., "6ez5" = 6e (EurUsd) + z (December) + 5 (Year 5)

        if s.len() < 3 {
            return Err(DataError::InvalidSymbol(format!(
                "Future contract string too short: {}",
                s
            ))
            .into());
        }

        // Find where the root ends by trying to parse progressively longer prefixes
        let (root, remainder) = if s.len() >= 3 && FutureRoot::from_str(&s[..2]).is_ok() {
            // 2-character root (e.g., "6e")
            (&s[..2], &s[2..])
        } else if s.len() >= 4 && FutureRoot::from_str(&s[..3]).is_ok() {
            // 3-character root (e.g., "btc")
            (&s[..3], &s[3..])
        } else {
            return Err(DataError::InvalidSymbol(format!("Invalid future root in: {}", s)).into());
        };

        if remainder.len() != 2 {
            return Err(
                DataError::InvalidSymbol(format!("Invalid future contract format: {}", s)).into(),
            );
        }

        let root = FutureRoot::from_str(root).map_err(DataError::ParseEnum)?;
        let month = ContractMonth::from_str(&remainder[..1]).map_err(DataError::ParseEnum)?;
        let year = ContractYear::from_str(&remainder[1..]).map_err(DataError::ParseEnum)?;

        Ok(FutureContract { root, month, year })
    }
}

/// Economic impact category for calendar events.
///
/// Categorizes economic indicators by their type, helping filter events
/// based on the area of economic data you're interested in monitoring.
#[derive(
    PartialEq,
    Copy,
    Clone,
    Debug,
    Display,
    EnumString,
    EnumIter,
    Hash,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
)]
#[strum(serialize_all = "camelCase")]
pub enum EconomicCategory {
    /// Employment-related indicators (e.g., Non-Farm Payrolls, Unemployment Rate).
    Employment = 1,

    /// Economic activity indicators (e.g., GDP, PMI, Retail Sales).
    EconomicActivity = 2,

    /// Inflation-related indicators (e.g., CPI, PPI, PCE).
    Inflation = 3,

    /// Credit and lending indicators (e.g., Consumer Credit, Bank Lending).
    Credit = 4,

    /// Central bank policy and actions (e.g., FOMC meetings, Rate Decisions).
    CentralBanks = 5,

    /// Consumer and business confidence indicators (e.g., Consumer Confidence, Business Sentiment).
    ConfidenceIndex = 6,

    /// Balance of payments or trade balance indicators.
    Balance = 7,

    /// Bond market-related indicators (e.g., Treasury Yields, Bond Auctions).
    #[strum(serialize = "Bonds")]
    Bonds = 8,
}

impl From<EconomicCategory> for RpcEconomicCategory {
    fn from(category: EconomicCategory) -> Self {
        match category {
            EconomicCategory::Employment => Self::Employment,
            EconomicCategory::EconomicActivity => Self::EconomicActivity,
            EconomicCategory::Inflation => Self::Inflation,
            EconomicCategory::Credit => Self::Credit,
            EconomicCategory::CentralBanks => Self::CentralBanks,
            EconomicCategory::ConfidenceIndex => Self::ConfidenceIndex,
            EconomicCategory::Balance => Self::Balance,
            EconomicCategory::Bonds => Self::Bonds,
        }
    }
}

impl TryFrom<RpcEconomicCategory> for EconomicCategory {
    type Error = ChapatyError;

    fn try_from(proto: RpcEconomicCategory) -> Result<Self, Self::Error> {
        match proto {
            RpcEconomicCategory::Employment => Ok(Self::Employment),
            RpcEconomicCategory::EconomicActivity => Ok(Self::EconomicActivity),
            RpcEconomicCategory::Inflation => Ok(Self::Inflation),
            RpcEconomicCategory::Credit => Ok(Self::Credit),
            RpcEconomicCategory::CentralBanks => Ok(Self::CentralBanks),
            RpcEconomicCategory::ConfidenceIndex => Ok(Self::ConfidenceIndex),
            RpcEconomicCategory::Balance => Ok(Self::Balance),
            RpcEconomicCategory::Bonds => Ok(Self::Bonds),

            RpcEconomicCategory::Unspecified => Err(TransportError::RpcTypeNotFound(
                "Economic category cannot be unspecified in this context".to_string(),
            )
            .into()),
        }
    }
}

/// Importance level of an economic event as of investing.com (e.g., from 1 to 3 stars).
#[derive(
    Debug,
    Clone,
    Copy,
    Hash,
    PartialEq,
    Eq,
    EnumIter,
    Display,
    Serialize,
    Deserialize,
    PartialOrd,
    Ord,
)]
#[strum(serialize_all = "lowercase")]
pub enum EconomicEventImpact {
    Low = 1,
    Medium = 2,
    High = 3,
}

impl From<EconomicEventImpact> for RpcEconomicImportance {
    fn from(value: EconomicEventImpact) -> Self {
        match value {
            EconomicEventImpact::Low => Self::Low,
            EconomicEventImpact::Medium => Self::Moderate,
            EconomicEventImpact::High => Self::High,
        }
    }
}

impl TryFrom<RpcEconomicImportance> for EconomicEventImpact {
    type Error = ChapatyError;

    fn try_from(proto: RpcEconomicImportance) -> Result<Self, Self::Error> {
        match proto {
            RpcEconomicImportance::Low => Ok(Self::Low),
            RpcEconomicImportance::Moderate => Ok(Self::Medium),
            RpcEconomicImportance::High => Ok(Self::High),
            RpcEconomicImportance::Unspecified => Err(TransportError::RpcTypeNotFound(
                "Economic importance cannot be unspecified in this context".to_string(),
            )
            .into()),
        }
    }
}

/// ISO 3166-1 alpha-2 country or economic region code.
///
/// Identifies the primary economic jurisdiction associated with
/// a macroeconomic calendar event (e.g., "US", "GB", "JP", "EZ").
#[derive(
    PartialEq,
    Copy,
    Clone,
    Debug,
    Display,
    EnumIter,
    EnumString,
    Hash,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
)]
#[strum(serialize_all = "UPPERCASE")]
pub enum CountryCode {
    /// Australia
    Au,
    /// Canada
    Ca,
    /// Euro Zone
    Ez,
    /// United Kingdom
    Gb,
    /// Japan
    Jp,
    /// New Zealand
    Nz,
    /// United States
    Us,
}

// ================================================================================================
// Traits
// ================================================================================================

/// Defines the core financial properties of a tradable instrument.
pub trait Instrument {
    /// The minimum price movement (e.g., 0.00005 for EUR/USD futures).
    fn tick_size(&self) -> f64;

    /// The value of one tick in the quote currency (USD).
    ///
    /// - For Spot (BTC/USDT), if tick is 0.01, tick value is $0.01.
    /// - For Futures (6E/EUR), if tick is 0.00005, tick value is $6.25.
    fn tick_value_usd(&self) -> f64;

    // === CONVERSION LOGIC (Default Implementations) ===

    /// Converts a raw USD PnL target into discrete Tick steps.
    /// Uses `round` to snap to the nearest valid grid point.
    fn usd_to_ticks(&self, usd: f64) -> Tick {
        let ticks = (usd / self.tick_value_usd()).round() as i64;
        Tick(ticks)
    }

    /// Converts discrete Ticks back into a USD value.
    /// This is the safest way to calculate realized PnL.
    fn ticks_to_usd(&self, ticks: Tick) -> f64 {
        ticks.0 as f64 * self.tick_value_usd()
    }

    /// Converts a raw price distance (e.g., target - entry) into Ticks.
    fn price_to_ticks(&self, price_dist: Price) -> Tick {
        let ticks = (price_dist.0 / self.tick_size()).round() as i64;
        Tick(ticks)
    }

    /// Converts Ticks into a valid price distance.
    fn ticks_to_price(&self, ticks: Tick) -> Price {
        Price(ticks.0 as f64 * self.tick_size())
    }

    /// Converts a USD target directly to a Price distance.
    ///
    /// Example: "I want to risk $50. How far away should my stop loss be?"
    fn usd_to_price_dist(&self, usd: f64) -> Price {
        let ticks = self.usd_to_ticks(usd);
        self.ticks_to_price(ticks)
    }

    /// Normalizes a raw price to the nearest valid tick.
    /// Crucial for order entry validation to prevent "Invalid Tick Size" errors.
    fn normalize_price(&self, price: f64) -> f64 {
        let ticks = (price / self.tick_size()).round();
        ticks * self.tick_size()
    }
}

impl Instrument for SpotPair {
    fn tick_size(&self) -> f64 {
        match self {
            SpotPair::BtcUsdt | SpotPair::BnbUsdt | SpotPair::EthUsdt | SpotPair::SolUsdt => 0.01,
            SpotPair::XrpUsdt | SpotPair::TrxUsdt | SpotPair::AdaUsdt | SpotPair::XlmUsdt => 0.0001,
        }
    }

    fn tick_value_usd(&self) -> f64 {
        // For spot, 1 unit of movement = 1 USD per unit held
        self.tick_size()
    }
}

impl Instrument for FutureRoot {
    fn tick_size(&self) -> f64 {
        match self {
            FutureRoot::AudUsd | FutureRoot::CadUsd | FutureRoot::EurUsd | FutureRoot::NzdUsd => {
                0.00005
            }
            FutureRoot::GbpUsd => 0.0001,
            FutureRoot::JpyUsd => 0.0000005,
            FutureRoot::Btc => 5.0,
        }
    }

    fn tick_value_usd(&self) -> f64 {
        match self {
            FutureRoot::EurUsd | FutureRoot::GbpUsd | FutureRoot::JpyUsd => 6.25,
            FutureRoot::AudUsd | FutureRoot::CadUsd | FutureRoot::NzdUsd => 5.0,
            FutureRoot::Btc => 25.0,
        }
    }
}

impl Instrument for BondSpec {
    fn tick_size(&self) -> f64 {
        self.tick_size
    }

    fn tick_value_usd(&self) -> f64 {
        // Die goldene Bond-Math Regel:
        // Da Bonds in % notieren, ist der Tick Value relative zum Nennwert.
        // Wenn Tick = 0.01% und Nominal = 1.0 (Unit Quote), dann:
        
        // ACHTUNG: Hier ist die Falle.
        // Quantity im RFQ ist der Nominalbetrag (z.B. 5.000.000).
        // Der Preis ist in Prozent (z.B. 98.50).
        // PnL = (DiffPrice / 100) * Quantity.
        
        // TickValue ist definiert als: Wertänderung pro 1 Unit Quantity bei 1 Tick Move.
        // TickValue = TickSize / 100.0
        
        self.tick_size / 100.0
    }
}

impl Instrument for Symbol {
    fn tick_size(&self) -> f64 {
        match self {
            Symbol::Spot(spot) => spot.tick_size(),
            Symbol::Future(future) => future.root.tick_size(),
            Symbol::Bond(bond) => bond.tick_size(),
        }
    }

    fn tick_value_usd(&self) -> f64 {
        match self {
            Symbol::Spot(spot) => spot.tick_value_usd(),
            Symbol::Future(future) => future.root.tick_value_usd(),
            Symbol::Bond(bond) => bond.tick_value_usd(),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    // ============================================================================================
    // Symbol String Conversion Tests
    // ============================================================================================

    #[test]
    fn future_contracts_format_as_root_month_year() {
        let cases = [
            (
                FutureRoot::EurUsd,
                ContractMonth::December,
                ContractYear::Y5,
                "6ez5",
            ),
            (
                FutureRoot::GbpUsd,
                ContractMonth::March,
                ContractYear::Y4,
                "6bh4",
            ),
            (
                FutureRoot::AudUsd,
                ContractMonth::June,
                ContractYear::Y3,
                "6am3",
            ),
            (
                FutureRoot::CadUsd,
                ContractMonth::September,
                ContractYear::Y2,
                "6cu2",
            ),
            (
                FutureRoot::JpyUsd,
                ContractMonth::January,
                ContractYear::Y1,
                "6jf1",
            ),
            (
                FutureRoot::NzdUsd,
                ContractMonth::July,
                ContractYear::Y0,
                "6nn0",
            ),
            (
                FutureRoot::Btc,
                ContractMonth::November,
                ContractYear::Y9,
                "btcx9",
            ),
        ];

        for (root, month, year, expected) in cases {
            let contract = FutureContract { root, month, year };
            let symbol = Symbol::Future(contract);
            assert_eq!(symbol.to_string(), expected, "Failed for {:?}", contract);
        }
    }

    #[test]
    fn parses_future_contracts_case_insensitive() {
        let cases = [
            (
                "6ez5",
                FutureRoot::EurUsd,
                ContractMonth::December,
                ContractYear::Y5,
            ),
            (
                "6EZ5",
                FutureRoot::EurUsd,
                ContractMonth::December,
                ContractYear::Y5,
            ),
            (
                "6bh4",
                FutureRoot::GbpUsd,
                ContractMonth::March,
                ContractYear::Y4,
            ),
            (
                "btcx9",
                FutureRoot::Btc,
                ContractMonth::November,
                ContractYear::Y9,
            ),
            (
                "BTCX9",
                FutureRoot::Btc,
                ContractMonth::November,
                ContractYear::Y9,
            ),
        ];

        for (input, root, month, year) in cases {
            let parsed: Symbol = input
                .parse()
                .unwrap_or_else(|_| panic!("Failed to parse '{}'", input));
            let expected = Symbol::Future(FutureContract { root, month, year });
            assert_eq!(parsed, expected, "Mismatch for '{}'", input);
        }
    }

    #[test]
    fn parses_all_future_roots() {
        let cases = [
            ("6az5", FutureRoot::AudUsd),
            ("6bz5", FutureRoot::GbpUsd),
            ("6cz5", FutureRoot::CadUsd),
            ("6ez5", FutureRoot::EurUsd),
            ("6jz5", FutureRoot::JpyUsd),
            ("6nz5", FutureRoot::NzdUsd),
            ("btcz5", FutureRoot::Btc),
        ];

        for (input, expected_root) in cases {
            let parsed: Symbol = input
                .parse()
                .unwrap_or_else(|_| panic!("Failed to parse '{}'", input));
            match parsed {
                Symbol::Future(contract) => assert_eq!(contract.root, expected_root),
                _ => panic!("Expected Future variant for '{}'", input),
            }
        }
    }

    #[test]
    fn rejects_invalid_symbols() {
        let invalid = [
            "", "invalid", "btc",     // missing month/year
            "6e",      // missing month/year
            "6ez",     // missing year
            "btc-",    // incomplete spot
            "-usdt",   // incomplete spot
            "btcusdt", // wrong spot format (missing hyphen)
            "6ez55",   // too long
            "xxz5",    // invalid root
        ];

        for input in invalid {
            let result: Result<Symbol, _> = input.parse();
            assert!(result.is_err(), "Expected '{}' to fail parsing", input);
        }
    }

    #[test]
    fn future_contracts_survive_round_trip() {
        let contracts = [
            FutureContract {
                root: FutureRoot::EurUsd,
                month: ContractMonth::December,
                year: ContractYear::Y5,
            },
            FutureContract {
                root: FutureRoot::Btc,
                month: ContractMonth::March,
                year: ContractYear::Y0,
            },
            FutureContract {
                root: FutureRoot::JpyUsd,
                month: ContractMonth::September,
                year: ContractYear::Y9,
            },
        ];

        for contract in contracts {
            let original = Symbol::Future(contract);
            let serialized = original.to_string();
            let deserialized: Symbol = serialized.parse().unwrap();
            assert_eq!(
                original, deserialized,
                "Round-trip failed for {:?}",
                contract
            );
        }
    }

    #[test]
    fn canonical_strings_parse_back_unchanged() {
        let canonical = ["btc-usdt", "eth-usdt", "6ez5", "6bh4", "btcx9"];

        for input in canonical {
            let parsed: Symbol = input.parse().unwrap();
            let output = parsed.to_string();
            assert_eq!(input, output, "Canonical form changed for '{}'", input);
        }
    }

    #[test]
    fn importance_numeration() {
        assert_eq!(EconomicEventImpact::Low as u8, 1, "Low should be 1");
        assert_eq!(EconomicEventImpact::Medium as u8, 2, "Medium should be 2");
        assert_eq!(EconomicEventImpact::High as u8, 3, "High should be 3");
    }

    /// Helper to create a dummy future for testing logic
    fn future_sym(root: FutureRoot) -> Symbol {
        Symbol::Future(FutureContract {
            root,
            month: ContractMonth::December, // Dummy
            year: ContractYear::Y5,         // Dummy
        })
    }

    #[test]
    fn test_quant_math_eur_usd() {
        let eur = future_sym(FutureRoot::EurUsd);

        // 1. Tick Size & Value Check
        assert_eq!(eur.tick_size(), 0.00005);
        assert_eq!(eur.tick_value_usd(), 6.25);

        // 2. Risk Calculation: "I want to risk $100"
        // $100 / $6.25 = 16 ticks
        // 16 ticks * 0.00005 = 0.0008 price distance
        let risk_dist = eur.usd_to_price_dist(100.0);
        assert!((risk_dist.0 - 0.0008).abs() < f64::EPSILON);

        // 3. Normalization (Rounding)
        // Price 1.00003 is invalid. Should snap to 1.00005 or 1.00000.
        // 1.00003 is closer to 1.00005
        // 0.00003 / 0.00005 = 0.6 -> Rounds to 1.
        let norm = eur.normalize_price(1.00003);
        assert!((norm - 1.00005).abs() < f64::EPSILON);
    }

    #[test]
    fn test_quant_math_btc_future() {
        let btc = future_sym(FutureRoot::Btc);

        // Tick: 5.0, Value: $25.0

        // PnL Check: Price moves from 50,000 to 50,100 (diff 100)
        // 100 / 5.0 = 20 ticks
        // 20 ticks * $25.0 = $500 profit
        let entry = 50_000.0;
        let exit = 50_100.0;
        let ticks = btc.price_to_ticks(Price(exit - entry));
        let pnl = btc.ticks_to_usd(ticks);

        assert_eq!(ticks.0, 20);
        assert_eq!(pnl, 500.0);
    }

    #[test]
    fn handling_floating_point_artifacts() {
        // EUR/USD tick size is 0.00005
        let eur = future_sym(FutureRoot::EurUsd);

        // 1.10000 is a valid grid price (1.10000 / 0.00005 = 22,000 ticks)
        let valid_price = 1.10000;

        // Case 1: Artifact just ABOVE the valid price (e.g. 1.10000001)
        // Should round DOWN to 1.10000
        let dirty_high = valid_price + 0.00000001;
        let norm_high = eur.normalize_price(dirty_high);

        assert!(
            (norm_high - valid_price).abs() < f64::EPSILON,
            "Failed to round down dirty high input: {:.8} -> {:.8}",
            dirty_high,
            norm_high
        );

        // Case 2: Artifact just BELOW the valid price (e.g. 1.09999999)
        // Should round UP to 1.10000
        let dirty_low = valid_price - 0.00000001;
        let norm_low = eur.normalize_price(dirty_low);

        assert!(
            (norm_low - valid_price).abs() < f64::EPSILON,
            "Failed to round up dirty low input: {:.8} -> {:.8}",
            dirty_low,
            norm_low
        );
    }

    #[test]
    fn tick_conversion_ignores_noise() {
        let eur = future_sym(FutureRoot::EurUsd);

        // We expect a price distance of 0.00050 (10 ticks)
        // 0.00050 / 0.00005 = 10.0
        let clean_dist = 0.00050;
        let expected_ticks = 10;

        // Test with positive noise (0.00050001 -> 10.0002 -> round to 10)
        let noisy_dist = Price(clean_dist + 0.00000001);
        let ticks = eur.price_to_ticks(noisy_dist);
        assert_eq!(
            ticks.0, expected_ticks,
            "Positive noise caused tick mismatch"
        );

        // Test with negative noise (0.00049999 -> 9.9998 -> round to 10)
        let noisy_dist_neg = Price(clean_dist - 0.00000001);
        let ticks_neg = eur.price_to_ticks(noisy_dist_neg);
        assert_eq!(
            ticks_neg.0, expected_ticks,
            "Negative noise caused tick mismatch"
        );
    }
}
