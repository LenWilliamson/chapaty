use std::fmt::Debug;

use chrono::{DateTime, Utc};
use polars::{
    df,
    frame::DataFrame,
    prelude::{DataType, IntoLazy, PlSmallStr, TimeUnit, col},
};
use serde::{Deserialize, Serialize};
use strum::{Display, EnumString, IntoStaticStr};

use crate::{
    data::{
        common::{ProfileAggregation, ProfileBinStats},
        domain::{
            CandleDirection, Count, CountryCode, DataBroker, EconomicCategory, EconomicDataSource,
            EconomicEventImpact, EconomicValue, Exchange, ExecutionDepth, LiquiditySide,
            MarketType, Period, Price, Quantity, Symbol, TradeId, Volume,
        },
        indicator::{EmaWindow, RsiWindow, SmaWindow},
    },
    error::{ChapatyError, ChapatyResult, DataError},
};

// ================================================================================================
// Traits
// ================================================================================================

/// Capability to check if a specific price was traded within an event's range.
pub trait PriceReachable {
    /// Returns true if the given `price` was observed by this market event.
    fn price_reached(&self, price: Price) -> bool;
}

/// Capability to provide a "Close" price for resolving market state.
pub trait ClosePriceProvider {
    fn close_price(&self) -> Price;
    fn close_timestamp(&self) -> DateTime<Utc>;
}

/// Defines the temporal properties of any financial event.
pub trait MarketEvent {
    /// The canonical timestamp when the event is finished and the data
    /// becomes immutable and visible to the agent.
    /// (e.g., Candle Close Time, Trade Time, News Release Time)
    fn point_in_time(&self) -> DateTime<Utc>;

    /// The timestamp when the event window began.
    ///
    /// - For Interval events (OHLCV, Profiles): This is the Open Time.
    /// - For Atomic events (Trades, News): This equals `point_in_time()`.
    ///
    /// This is used to determine when a simulation *could* theoretically start,
    /// even if the data isn't fully available yet.
    fn opened_at(&self) -> DateTime<Utc> {
        self.point_in_time()
    }
}

pub trait StreamId: Ord + Copy + Debug {
    type Event: MarketEvent;
}

pub trait SymbolProvider {
    fn symbol(&self) -> &Symbol;
}

// ================================================================================================
// OHLCV
// ================================================================================================

/// Uniquely identifies an OHLCV stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct OhlcvId {
    pub broker: DataBroker,
    pub exchange: Exchange,
    pub symbol: Symbol,
    pub period: Period,
}

/// A standard OHLCV candlestick.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, PartialOrd)]
pub struct Ohlcv {
    /// The open time of the candle.
    pub open_timestamp: DateTime<Utc>,
    /// The close time of the candle.
    pub close_timestamp: DateTime<Utc>,

    /// The opening price.
    pub open: Price,
    /// The highest price reached during the interval.
    pub high: Price,
    /// The lowest price reached during the interval.
    pub low: Price,
    /// The closing price.
    pub close: Price,
    /// The total traded volume of the base asset.
    pub volume: Volume,

    // === Extended Metrics ===
    /// The total traded volume of the quote asset.
    pub quote_asset_volume: Option<Volume>,

    /// The number of trades executed.
    pub number_of_trades: Option<Count>,

    /// The volume of taker buy orders in base asset units.
    pub taker_buy_base_asset_volume: Option<Volume>,

    /// The volume of taker buy orders in quote asset units.
    pub taker_buy_quote_asset_volume: Option<Volume>,
}

impl PriceReachable for Ohlcv {
    fn price_reached(&self, price: Price) -> bool {
        self.low.0 <= price.0 && price.0 <= self.high.0
    }
}

impl ClosePriceProvider for Ohlcv {
    fn close_price(&self) -> Price {
        self.close
    }
    fn close_timestamp(&self) -> DateTime<Utc> {
        self.close_timestamp
    }
}

impl MarketEvent for Ohlcv {
    fn point_in_time(&self) -> DateTime<Utc> {
        self.close_timestamp
    }
    fn opened_at(&self) -> DateTime<Utc> {
        self.open_timestamp
    }
}

impl StreamId for OhlcvId {
    type Event = Ohlcv;
}

impl SymbolProvider for OhlcvId {
    fn symbol(&self) -> &Symbol {
        &self.symbol
    }
}

impl Ohlcv {
    pub fn direction(&self) -> CandleDirection {
        let open = self.open.0;
        let close = self.close.0;

        if close > open {
            CandleDirection::Bullish
        } else if close < open {
            CandleDirection::Bearish
        } else {
            CandleDirection::Doji
        }
    }
}

// ================================================================================================
// Trade
// ================================================================================================

/// Uniquely identifies a Trade stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TradesId {
    pub broker: DataBroker,
    pub exchange: Exchange,
    pub symbol: Symbol,
}

/// A single atomic trade execution.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, PartialOrd)]
pub struct Trade {
    /// Time of trade execution.
    pub timestamp: DateTime<Utc>,

    /// Execution price.
    pub price: Price,

    /// Quantity traded (Base Asset).
    pub quantity: Quantity,

    // === Optional Metadata ===
    /// Unique trade ID.
    pub trade_id: Option<TradeId>,

    /// Notional value (Quote Asset).
    pub quote_asset_volume: Option<Volume>,

    /// Indicates who provided the liquidity.
    ///
    /// Use `.trade_side()` to get the aggressor side (Buy/Sell).
    pub is_buyer_maker: Option<LiquiditySide>,

    /// True if matched at the best available book price.
    pub is_best_match: Option<ExecutionDepth>,
}

impl PriceReachable for Trade {
    fn price_reached(&self, price: Price) -> bool {
        self.price.0 == price.0
    }
}

impl ClosePriceProvider for Trade {
    fn close_price(&self) -> Price {
        self.price
    }
    fn close_timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }
}

impl MarketEvent for Trade {
    fn point_in_time(&self) -> DateTime<Utc> {
        self.timestamp
    }
}

impl SymbolProvider for TradesId {
    fn symbol(&self) -> &Symbol {
        &self.symbol
    }
}

impl StreamId for TradesId {
    type Event = Trade;
}

// ================================================================================================
// Market Profile Properties
// ================================================================================================

/// Standard column names for Profile DataFrames exposed to Agents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display, EnumString, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum ProfileCol {
    // === Metadata ===
    WindowStart, // "open_timestamp"
    WindowEnd,   // "close_timestamp"

    // === Bins ===
    PriceBinStart,
    PriceBinEnd,

    // === Volume ===
    Volume,
    TakerBuyBaseVol,
    TakerSellBaseVol,
    QuoteVol,
    TakerBuyQuoteVol,
    TakerSellQuoteVol,

    // === Counts ===
    NumTrades,
    NumBuyTrades,
    NumSellTrades,

    // === TPO Specific ===
    TimeSlotCount,
}

impl From<ProfileCol> for PlSmallStr {
    fn from(value: ProfileCol) -> Self {
        value.as_str().into()
    }
}

impl ProfileCol {
    pub fn name(&self) -> PlSmallStr {
        (*self).into()
    }

    pub fn as_str(&self) -> &'static str {
        self.into()
    }
}

/// Common behavior for market profile snapshots (Volume or TPO).
pub trait MarketProfile {
    /// The start of the time window this profile covers.
    fn open_timestamp(&self) -> DateTime<Utc>;

    /// The end of the time window.
    fn close_timestamp(&self) -> DateTime<Utc>;

    /// The Point of Control (Price level with highest activity).
    fn poc(&self) -> Price;

    /// The top of the Value Area (e.g. 70%).
    fn value_area_high(&self) -> Price;

    /// The bottom of the Value Area.
    fn value_area_low(&self) -> Price;

    /// Converts the efficient binary snapshot into a Polars DataFrame for complex analysis.
    ///
    /// This unpacks the `Box<[Bin]>` structure into Series.
    fn as_dataframe(&self) -> ChapatyResult<DataFrame>;
}

// ================================================================================================
// Tpo
// ================================================================================================

/// Uniquely identifies a TPO (Time Price Opportunity) stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TpoId {
    // === The market source (Where) ===
    pub broker: DataBroker,
    pub exchange: Exchange,
    pub symbol: Symbol,

    // === The profile configuration (How) ===
    pub aggregation: ProfileAggregation,
}

/// A complete Market Profile (TPO) Snapshot.
///
/// Measures time spent at specific price levels during the window.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tpo {
    // === Metadata ===
    pub open_timestamp: DateTime<Utc>,
    pub close_timestamp: DateTime<Utc>,

    // === Pre-computed Intrinsic Stats ===
    /// The price level with the highest volume (Point of Control).
    pub poc: Price,
    /// The upper bound of the Value Area.
    pub value_area_high: Price,
    /// The lower bound of the Value Area.
    pub value_area_low: Price,

    // === Data ===
    /// TPO bins, sorted by `price_bin_start`.
    pub bins: Box<[TpoBin]>,
}

/// A single price bucket within a TPO snapshot.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, PartialOrd)]
pub struct TpoBin {
    // === Price Range ===
    /// The lower bound of this specific price bucket (inclusive).
    pub price_bin_start: Price,
    /// The upper bound of this specific price bucket (exclusive).
    pub price_bin_end: Price,

    /// Number of TPO blocks (time units) spent in this bin.
    pub time_slot_count: Count,
}

impl SymbolProvider for TpoId {
    fn symbol(&self) -> &Symbol {
        &self.symbol
    }
}

impl MarketEvent for Tpo {
    fn point_in_time(&self) -> DateTime<Utc> {
        self.close_timestamp
    }
    fn opened_at(&self) -> DateTime<Utc> {
        self.open_timestamp
    }
}

impl StreamId for TpoId {
    type Event = Tpo;
}

impl MarketProfile for Tpo {
    fn open_timestamp(&self) -> DateTime<Utc> {
        self.open_timestamp
    }
    fn close_timestamp(&self) -> DateTime<Utc> {
        self.close_timestamp
    }
    fn poc(&self) -> Price {
        self.poc
    }
    fn value_area_high(&self) -> Price {
        self.value_area_high
    }
    fn value_area_low(&self) -> Price {
        self.value_area_low
    }

    fn as_dataframe(&self) -> ChapatyResult<DataFrame> {
        let len = self.bins.len();

        // Pre-allocate vectors
        let window_starts = vec![self.open_timestamp.timestamp_micros(); len];
        let window_ends = vec![self.close_timestamp.timestamp_micros(); len];
        let mut price_starts = Vec::with_capacity(len);
        let mut price_ends = Vec::with_capacity(len);
        let mut counts = Vec::with_capacity(len);

        for bin in self.bins.iter() {
            price_starts.push(bin.price_bin_start.0);
            price_ends.push(bin.price_bin_end.0);
            counts.push(bin.time_slot_count.0); // Assuming Count wraps integer
        }

        let df = df![
            ProfileCol::WindowStart.to_string() => window_starts,
            ProfileCol::WindowEnd.to_string() => window_ends,
            ProfileCol::PriceBinStart.to_string() => price_starts,
            ProfileCol::PriceBinEnd.to_string() => price_ends,
            ProfileCol::TimeSlotCount.to_string() => counts,
        ]
        .map_err(|e| ChapatyError::Data(DataError::DataFrame(e.to_string())))?;

        let lf = df.lazy().with_columns([
            col(ProfileCol::WindowStart).cast(DataType::Datetime(
                TimeUnit::Microseconds,
                Some(polars::prelude::TimeZone::UTC),
            )),
            col(ProfileCol::WindowEnd).cast(DataType::Datetime(
                TimeUnit::Microseconds,
                Some(polars::prelude::TimeZone::UTC),
            )),
        ]);

        lf.collect()
            .map_err(|e| DataError::DataFrame(e.to_string()).into())
    }
}

impl ProfileBinStats for TpoBin {
    fn get_value(&self) -> f64 {
        // TPO Count acts as "Volume" for Market Profile calculations
        self.time_slot_count.0 as f64
    }

    fn get_price(&self) -> Price {
        self.price_bin_start
    }
}

// ================================================================================================
// Volume Profile
// ================================================================================================

/// Uniquely identifies a Volume Profile stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct VolumeProfileId {
    // === The market source (Where) ===
    pub broker: DataBroker,
    pub exchange: Exchange,
    pub symbol: Symbol,

    // === The profile configuration (How) ===
    pub aggregation: ProfileAggregation,
}

/// A complete Volume Profile Snapshot for a specific time window.
///
/// Represents the distribution of trading volume across price levels during the
/// interval `[open_timestamp, close_timestamp)`.
///
/// # Metrics
/// - **POC (Point of Control):** The price level with the highest traded volume.
/// - **VA (Value Area):** The price range containing a specified percentage
///   of total volume.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VolumeProfile {
    // === Temporal Metadata ===
    /// The start time of the profile window.
    pub open_timestamp: DateTime<Utc>,
    /// The end time of the profile window.
    pub close_timestamp: DateTime<Utc>,

    // === Pre-computed Intrinsic Stats ===
    /// The price level with the highest volume (Point of Control).
    pub poc: Price,
    /// The upper bound of the Value Area.
    pub value_area_high: Price,
    /// The lower bound of the Value Area.
    pub value_area_low: Price,

    // === The Data ===
    /// The histogram bins, strictly sorted by `price_bin_start` ascending.
    pub bins: Box<[VolumeProfileBin]>,
}

/// A single price bucket within a Volume Profile snapshot.
///
/// Contains the volume and trade counts for a specific price range.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, PartialOrd)]
pub struct VolumeProfileBin {
    // === Price Range ===
    /// The lower bound of this specific price bucket (inclusive).
    pub price_bin_start: Price,
    /// The upper bound of this specific price bucket (exclusive).
    pub price_bin_end: Price,

    // === Base Asset Volume (The "Size") ===
    /// Total quantity traded in this price bin (Base Asset).
    ///
    /// `Volume = TakerBuyBase + TakerSellBase`
    pub volume: Volume,

    /// The amount of Base Asset bought by aggressors (Market Buys).
    ///
    /// High values relative to `taker_sell_base_asset_volume` indicate strong
    /// buying interest at this level (Absorption or Breakout).
    pub taker_buy_base_asset_volume: Option<Volume>,

    /// The amount of Base Asset sold by aggressors (Market Sells).
    ///
    /// High values indicate strong selling pressure (Supply zone).
    pub taker_sell_base_asset_volume: Option<Volume>,

    // === Quote Asset Volume (The "Money Flow") ===
    /// Total notional value traded in this price bin (Quote Asset).
    ///
    /// Represents the total capital flow: `Price * Volume`.
    pub quote_asset_volume: Option<Volume>,

    /// The notional value of Market Buys (Quote Asset).
    pub taker_buy_quote_asset_volume: Option<Volume>,

    /// The notional value of Market Sells (Quote Asset).
    pub taker_sell_quote_asset_volume: Option<Volume>,

    // === Trade Counts (Activity/Frequency) ===
    /// Total number of individual trades executed in this bin.
    ///
    /// A high trade count with low volume suggests "fighting" (many small retail orders).
    /// A low trade count with high volume suggests "whale" activity (few large institutional orders).
    pub number_of_trades: Option<Count>,

    /// Number of individual trades where the aggressor was a Buyer.
    pub number_of_buy_trades: Option<Count>,

    /// Number of individual trades where the aggressor was a Seller.
    pub number_of_sell_trades: Option<Count>,
}

impl SymbolProvider for VolumeProfileId {
    fn symbol(&self) -> &Symbol {
        &self.symbol
    }
}
impl MarketEvent for VolumeProfile {
    fn point_in_time(&self) -> DateTime<Utc> {
        self.close_timestamp
    }
    fn opened_at(&self) -> DateTime<Utc> {
        self.open_timestamp
    }
}

impl StreamId for VolumeProfileId {
    type Event = VolumeProfile;
}

impl MarketProfile for VolumeProfile {
    fn open_timestamp(&self) -> DateTime<Utc> {
        self.open_timestamp
    }
    fn close_timestamp(&self) -> DateTime<Utc> {
        self.close_timestamp
    }
    fn poc(&self) -> Price {
        self.poc
    }
    fn value_area_high(&self) -> Price {
        self.value_area_high
    }
    fn value_area_low(&self) -> Price {
        self.value_area_low
    }

    fn as_dataframe(&self) -> ChapatyResult<DataFrame> {
        let len = self.bins.len();

        // 1. Metadata columns (Repeated for every row, Polars compresses this well)
        let window_starts = vec![self.open_timestamp.timestamp_micros(); len];
        let window_ends = vec![self.close_timestamp.timestamp_micros(); len];

        // 2. Data columns
        let mut p_starts = Vec::with_capacity(len);
        let mut p_ends = Vec::with_capacity(len);

        // Use Vec<Option<f64>> to handle sparse data correctly in Polars
        let mut vol = Vec::with_capacity(len);
        let mut tb_base = Vec::with_capacity(len);
        let mut ts_base = Vec::with_capacity(len);
        let mut q_vol = Vec::with_capacity(len);
        let mut tb_quote = Vec::with_capacity(len);
        let mut ts_quote = Vec::with_capacity(len);

        // Counts
        let mut n_trades = Vec::with_capacity(len);
        let mut n_buy = Vec::with_capacity(len);
        let mut n_sell = Vec::with_capacity(len);

        for bin in self.bins.iter() {
            p_starts.push(bin.price_bin_start.0);
            p_ends.push(bin.price_bin_end.0);

            vol.push(bin.volume.0);

            // Map Option<Volume> -> Option<f64>
            tb_base.push(bin.taker_buy_base_asset_volume.map(|v| v.0));
            ts_base.push(bin.taker_sell_base_asset_volume.map(|v| v.0));

            q_vol.push(bin.quote_asset_volume.map(|v| v.0));
            tb_quote.push(bin.taker_buy_quote_asset_volume.map(|v| v.0));
            ts_quote.push(bin.taker_sell_quote_asset_volume.map(|v| v.0));

            // Map Option<Count> -> Option<u64>
            n_trades.push(bin.number_of_trades.map(|c| c.0));
            n_buy.push(bin.number_of_buy_trades.map(|c| c.0));
            n_sell.push(bin.number_of_sell_trades.map(|c| c.0));
        }

        let df = df![
            ProfileCol::WindowStart.to_string() => window_starts,
            ProfileCol::WindowEnd.to_string() => window_ends,
            ProfileCol::PriceBinStart.to_string() => p_starts,
            ProfileCol::PriceBinEnd.to_string() => p_ends,

            ProfileCol::Volume.to_string() => vol,
            ProfileCol::TakerBuyBaseVol.to_string() => tb_base,
            ProfileCol::TakerSellBaseVol.to_string() => ts_base,

            ProfileCol::QuoteVol.to_string() => q_vol,
            ProfileCol::TakerBuyQuoteVol.to_string() => tb_quote,
            ProfileCol::TakerSellQuoteVol.to_string() => ts_quote,

            ProfileCol::NumTrades.to_string() => n_trades,
            ProfileCol::NumBuyTrades.to_string() => n_buy,
            ProfileCol::NumSellTrades.to_string() => n_sell,
        ]
        .map_err(|e| ChapatyError::Data(DataError::DataFrame(e.to_string())))?;

        let lf = df.lazy().with_columns([
            col(ProfileCol::WindowStart).cast(DataType::Datetime(
                TimeUnit::Microseconds,
                Some(polars::prelude::TimeZone::UTC),
            )),
            col(ProfileCol::WindowEnd).cast(DataType::Datetime(
                TimeUnit::Microseconds,
                Some(polars::prelude::TimeZone::UTC),
            )),
        ]);

        lf.collect()
            .map_err(|e| DataError::DataFrame(e.to_string()).into())
    }
}

impl ProfileBinStats for VolumeProfileBin {
    fn get_value(&self) -> f64 {
        self.volume.0
    }

    fn get_price(&self) -> Price {
        self.price_bin_start
    }
}

// ================================================================================================
// Technical Indicator
// ================================================================================================

/// Uniquely identifies an Exponential Moving Average (EMA) stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EmaId {
    pub parent: OhlcvId,
    pub length: EmaWindow,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Ema {
    pub timestamp: DateTime<Utc>,
    pub price: Price,
}

impl PriceReachable for Ema {
    fn price_reached(&self, price: Price) -> bool {
        self.price.0 == price.0
    }
}

impl ClosePriceProvider for Ema {
    fn close_price(&self) -> Price {
        self.price
    }
    fn close_timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }
}

impl MarketEvent for Ema {
    fn point_in_time(&self) -> DateTime<Utc> {
        self.timestamp
    }
}

impl StreamId for EmaId {
    type Event = Ema;
}

impl SymbolProvider for EmaId {
    fn symbol(&self) -> &Symbol {
        self.parent.symbol()
    }
}

/// Uniquely identifies a Relative Strength Index (RSI) stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RsiId {
    pub parent: OhlcvId,
    pub length: RsiWindow,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Rsi {
    pub timestamp: DateTime<Utc>,
    pub price: Price,
}

impl PriceReachable for Rsi {
    fn price_reached(&self, price: Price) -> bool {
        self.price.0 == price.0
    }
}

impl ClosePriceProvider for Rsi {
    fn close_price(&self) -> Price {
        self.price
    }
    fn close_timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }
}

impl MarketEvent for Rsi {
    fn point_in_time(&self) -> DateTime<Utc> {
        self.timestamp
    }
}

impl StreamId for RsiId {
    type Event = Rsi;
}

impl SymbolProvider for RsiId {
    fn symbol(&self) -> &Symbol {
        self.parent.symbol()
    }
}
/// Uniquely identifies a Simple Moving Average (SMA) stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SmaId {
    /// The source data stream this indicator is calculated from.
    pub parent: OhlcvId,
    /// The lookback window length (e.g., 14, 200).
    pub length: SmaWindow,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Sma {
    pub timestamp: DateTime<Utc>,
    pub price: Price,
}

impl PriceReachable for Sma {
    fn price_reached(&self, price: Price) -> bool {
        self.price.0 == price.0
    }
}

impl ClosePriceProvider for Sma {
    fn close_price(&self) -> Price {
        self.price
    }
    fn close_timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }
}

impl MarketEvent for Sma {
    fn point_in_time(&self) -> DateTime<Utc> {
        self.timestamp
    }
}

impl StreamId for SmaId {
    type Event = Sma;
}

impl SymbolProvider for SmaId {
    fn symbol(&self) -> &Symbol {
        self.parent.symbol()
    }
}

// ================================================================================================
// Economic Calendar
// ================================================================================================

/// Uniquely identifies an Economic Calendar stream.
///
/// This ID maps 1:1 to the `EconomicCalendarConfig` filters, allowing
/// specific targeting of data subsets (e.g., "US Employment Data from Fred").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EconomicCalendarId {
    /// The primary provider.
    pub broker: DataBroker,

    /// Specific sub-source (e.g., "investingcom", "fred").
    /// If `None`, represents a merged stream of all sources.
    pub data_source: Option<EconomicDataSource>,

    /// Geographic filter.
    /// If `None`, represents a Global stream.
    pub country_code: Option<CountryCode>,

    /// Thematic filter (e.g., "Inflation", "Employment").
    /// If `None`, represents all categories.
    pub category: Option<EconomicCategory>,

    /// Impact filter (e.g., "Low", "Medium", "High").
    /// If `None`, represents all three impact levels.
    pub importance: Option<EconomicEventImpact>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EconomicEvent {
    /// UTC timestamp of the event.
    pub timestamp: DateTime<Utc>,

    /// Data source (e.g., "investingcom", "fred").
    pub data_source: String,

    /// Event category from the data source.
    pub category: String,

    /// Full descriptive name of the event.
    pub news_name: String,

    /// ISO 3166-1 alpha-2 country code (e.g., "US", "GB", "EZ").
    pub country_code: CountryCode,

    /// ISO 4217 currency code (e.g., "USD", "EUR").
    pub currency_code: String,

    /// Importance level of the event (1 = Low, 2 = Medium, 3 = High).
    pub economic_impact: EconomicEventImpact,

    // === Classification (Optional) ===
    /// Classified event type identifier (e.g., "NFP", "CPI", "FOMC").
    pub news_type: Option<String>,

    /// Confidence score for news_type classification (0.0 to 1.0).
    pub news_type_confidence: Option<f64>,

    /// Method used to derive news_type classification.
    pub news_type_source: Option<String>,

    /// Reporting periodicity (e.g., "mom", "qoq", "yoy").
    pub period: Option<String>,

    // === Economic Value (Optional) ===
    /// Actual reported value.
    pub actual: Option<EconomicValue>,

    /// Forecasted value.
    pub forecast: Option<EconomicValue>,

    /// Previously reported value.
    pub previous: Option<EconomicValue>,
}

impl MarketEvent for EconomicEvent {
    fn point_in_time(&self) -> DateTime<Utc> {
        self.timestamp
    }
}

impl StreamId for EconomicCalendarId {
    type Event = EconomicEvent;
}

// ================================================================================================
// Execution Entities
// ================================================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct MarketId {
    pub broker: DataBroker,
    pub exchange: Exchange,
    pub symbol: Symbol,
}

impl SymbolProvider for MarketId {
    fn symbol(&self) -> &Symbol {
        &self.symbol
    }
}

impl MarketId {
    pub fn market_type(&self) -> MarketType {
        self.symbol.into()
    }
}

impl From<OhlcvId> for MarketId {
    fn from(value: OhlcvId) -> Self {
        Self {
            broker: value.broker,
            exchange: value.exchange,
            symbol: value.symbol,
        }
    }
}

impl From<&OhlcvId> for MarketId {
    fn from(value: &OhlcvId) -> Self {
        (*value).into()
    }
}
impl From<TradesId> for MarketId {
    fn from(value: TradesId) -> Self {
        Self {
            broker: value.broker,
            exchange: value.exchange,
            symbol: value.symbol,
        }
    }
}

impl From<&TradesId> for MarketId {
    fn from(value: &TradesId) -> Self {
        (*value).into()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn tpo_as_df() {
        let open_ts = DateTime::parse_from_rfc3339("2023-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let close_ts = DateTime::parse_from_rfc3339("2023-01-01T01:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        let tpo = Tpo {
            open_timestamp: open_ts,
            close_timestamp: close_ts,
            poc: Price(102.0),
            value_area_high: Price(103.0),
            value_area_low: Price(101.0),
            bins: vec![
                TpoBin {
                    price_bin_start: Price(100.0),
                    price_bin_end: Price(101.0),
                    time_slot_count: Count(5),
                },
                TpoBin {
                    price_bin_start: Price(101.0),
                    price_bin_end: Price(102.0),
                    time_slot_count: Count(15),
                },
                TpoBin {
                    price_bin_start: Price(102.0),
                    price_bin_end: Price(103.0),
                    time_slot_count: Count(10),
                },
            ]
            .into_boxed_slice(),
        };

        let df = tpo.as_dataframe().expect("to be df");

        // Verify shape: 3 bins => 3 rows
        assert_eq!(df.shape(), (3, 5));

        // Verify columns existence and types
        let price_start = df.column("price_bin_start").expect("to get column");
        assert_eq!(price_start.dtype(), &DataType::Float64);
        assert_eq!(price_start.f64().expect("to be f64").get(0), Some(100.0));

        let counts = df.column("time_slot_count").expect("to get column");
        assert_eq!(counts.i64().expect("to be i64").get(1), Some(15));

        // Verify Timestamps are correctly expanded
        let starts = df.column("window_start").expect("to get column");
        let _ts_val = starts
            .datetime()
            .expect("to be datetime")
            .physical()
            .get(0)
            .unwrap();
        // Just checking it exists and has length 3
        assert_eq!(starts.len(), 3);
    }

    #[test]
    fn vp_as_df() {
        let open_ts = DateTime::parse_from_rfc3339("2023-01-02T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let close_ts = DateTime::parse_from_rfc3339("2023-01-02T16:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        let vp = VolumeProfile {
            open_timestamp: open_ts,
            close_timestamp: close_ts,
            poc: Price(50.0),
            value_area_high: Price(55.0),
            value_area_low: Price(45.0),
            bins: vec![
                VolumeProfileBin {
                    price_bin_start: Price(40.0),
                    price_bin_end: Price(45.0),
                    volume: Quantity(1000.0),
                    taker_buy_base_asset_volume: Some(Quantity(600.0)),
                    taker_sell_base_asset_volume: Some(Quantity(400.0)),
                    quote_asset_volume: Some(Quantity(50000.0)),
                    taker_buy_quote_asset_volume: None, // Sparse test
                    taker_sell_quote_asset_volume: None, // Sparse test
                    number_of_trades: Some(Count(50)),
                    number_of_buy_trades: Some(Count(30)),
                    number_of_sell_trades: Some(Count(20)),
                },
                VolumeProfileBin {
                    price_bin_start: Price(45.0),
                    price_bin_end: Price(50.0),
                    volume: Quantity(2500.0),
                    taker_buy_base_asset_volume: Some(Quantity(1200.0)),
                    taker_sell_base_asset_volume: Some(Quantity(1300.0)),
                    quote_asset_volume: Some(Quantity(125000.0)),
                    taker_buy_quote_asset_volume: Some(Quantity(60000.0)),
                    taker_sell_quote_asset_volume: Some(Quantity(65000.0)),
                    number_of_trades: Some(Count(120)),
                    number_of_buy_trades: Some(Count(60)),
                    number_of_sell_trades: Some(Count(60)),
                },
            ]
            .into_boxed_slice(),
        };

        let df = vp.as_dataframe().expect("to be df");

        // Verify shape: 2 bins => 2 rows
        // Count columns:
        // Metadata: WindowStart, WindowEnd (2)
        // Bins: PriceBinStart, PriceBinEnd (2)
        // Quantity: Quantity, TakerBuyBase, TakerSellBase (3)
        // Quote: QuoteVol, TakerBuyQuote, TakerSellQuote (3)
        // Counts: NumTrades, NumBuy, NumSell (3)
        // Total = 13 columns
        assert_eq!(df.shape(), (2, 13));

        // Check a dense column
        let vol = df.column("volume").expect("to get vol");
        assert_eq!(vol.f64().expect("to be f64").get(1), Some(2500.0));

        // Check a sparse column (Option::None)
        let tb_quote = df.column("taker_buy_quote_vol").expect("to get tb_quote");
        assert!(tb_quote.f64().expect("to be f64").get(0).is_none()); // First bin was None
        assert_eq!(tb_quote.f64().expect("to be f64").get(1), Some(60000.0)); // Second bin was Some
    }
}
