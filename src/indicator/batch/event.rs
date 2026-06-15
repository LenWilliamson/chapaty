use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    data::{
        domain::{AggregatedPrice, Price, PriceDelta, SessionDate, SessionWindow, Symbol, Volume},
        event::{MarketEvent, OhlcvId, PriceReachable, StreamId, SymbolProvider, TradesId},
    },
    gym::trading::TradeType,
    indicator::{
        batch::ohlcv::SessionCfg,
        config::{AtrConfig, EmaWindow, LookbackWindow, RsiWindow, SmaWindow},
    },
};

// ================================================================================================
// EMA
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
    fn price_reached(&self, target_price: Price, direction: TradeType) -> bool {
        match direction {
            TradeType::Long => self.price.0 <= target_price.0,
            TradeType::Short => self.price.0 >= target_price.0,
        }
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
    fn symbol(&self) -> Symbol {
        self.parent.symbol()
    }
}

// ================================================================================================
// RSI
// ================================================================================================

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
    fn price_reached(&self, target_price: Price, direction: TradeType) -> bool {
        match direction {
            TradeType::Long => self.price.0 <= target_price.0,
            TradeType::Short => self.price.0 >= target_price.0,
        }
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
    fn symbol(&self) -> Symbol {
        self.parent.symbol()
    }
}

// ================================================================================================
// SMA
// ================================================================================================

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
    fn price_reached(&self, target_price: Price, direction: TradeType) -> bool {
        match direction {
            TradeType::Long => self.price.0 <= target_price.0,
            TradeType::Short => self.price.0 >= target_price.0,
        }
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
    fn symbol(&self) -> Symbol {
        self.parent.symbol()
    }
}

// ================================================================================================
// OHLCV VWAP
// ================================================================================================

/// Uniquely identifies a Volume Weighted Average Price (VWAP) stream from OHLCV data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct OhlcvVwapId {
    /// The source data stream this indicator is calculated from.
    pub parent: OhlcvId,
    /// The aggregated price to use for calculations.
    pub price_aggregation: AggregatedPrice,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OhlcvVwap {
    pub timestamp: DateTime<Utc>,
    pub price: Price,
}

impl PriceReachable for OhlcvVwap {
    fn price_reached(&self, target_price: Price, direction: TradeType) -> bool {
        match direction {
            TradeType::Long => self.price.0 <= target_price.0,
            TradeType::Short => self.price.0 >= target_price.0,
        }
    }
}

impl MarketEvent for OhlcvVwap {
    fn point_in_time(&self) -> DateTime<Utc> {
        self.timestamp
    }
}

impl StreamId for OhlcvVwapId {
    type Event = OhlcvVwap;
}

impl SymbolProvider for OhlcvVwapId {
    fn symbol(&self) -> Symbol {
        self.parent.symbol()
    }
}

// ================================================================================================
// Trades VWAP
// ================================================================================================

/// Uniquely identifies a Volume Weighted Average Price (VWAP) stream from Trades data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TradesVwapId {
    /// The source data stream this indicator is calculated from.
    pub parent: TradesId,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TradesVwap {
    pub timestamp: DateTime<Utc>,
    pub price: Price,
}

impl PriceReachable for TradesVwap {
    fn price_reached(&self, target_price: Price, direction: TradeType) -> bool {
        match direction {
            TradeType::Long => self.price.0 <= target_price.0,
            TradeType::Short => self.price.0 >= target_price.0,
        }
    }
}

impl MarketEvent for TradesVwap {
    fn point_in_time(&self) -> DateTime<Utc> {
        self.timestamp
    }
}

impl StreamId for TradesVwapId {
    type Event = TradesVwap;
}

impl SymbolProvider for TradesVwapId {
    fn symbol(&self) -> Symbol {
        self.parent.symbol()
    }
}

// ================================================================================================
// Ohlcv Session
// ================================================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct OhlcvSessionId {
    pub parent: OhlcvId,
    pub cfg: SessionCfg,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct OhlcvSession {
    pub session: SessionDate,
    pub open_timestamp: DateTime<Utc>,
    pub close_timestamp: DateTime<Utc>,
    pub high: Price,
    pub low: Price,
    pub highest_close: Price,
    pub lowest_close: Price,
    pub volume: Volume,
    pub vwap: Price,
}

impl MarketEvent for OhlcvSession {
    fn point_in_time(&self) -> DateTime<Utc> {
        self.close_timestamp
    }
    fn opened_at(&self) -> DateTime<Utc> {
        self.open_timestamp
    }
}

impl StreamId for OhlcvSessionId {
    type Event = OhlcvSession;
}

impl SymbolProvider for OhlcvSessionId {
    fn symbol(&self) -> Symbol {
        self.parent.symbol()
    }
}

// ================================================================================================
// Trades Session
// ================================================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TradesSessionId {
    pub parent: TradesId,
    pub cfg: SessionWindow,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct TradesSession {
    pub session: SessionDate,
    pub open_timestamp: DateTime<Utc>,
    pub close_timestamp: DateTime<Utc>,
    pub high: Price,
    pub low: Price,
    pub volume: Volume,
    pub vwap: Price,
}

impl MarketEvent for TradesSession {
    fn point_in_time(&self) -> DateTime<Utc> {
        self.close_timestamp
    }
    fn opened_at(&self) -> DateTime<Utc> {
        self.open_timestamp
    }
}

impl StreamId for TradesSessionId {
    type Event = TradesSession;
}

impl SymbolProvider for TradesSessionId {
    fn symbol(&self) -> Symbol {
        self.parent.symbol()
    }
}

// ================================================================================================
// ATR
// ================================================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct AtrId {
    pub parent: OhlcvId,
    pub cfg: AtrConfig,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Atr {
    pub timestamp: DateTime<Utc>,
    pub range: PriceDelta,
}

impl MarketEvent for Atr {
    fn point_in_time(&self) -> DateTime<Utc> {
        self.timestamp
    }
}

impl StreamId for AtrId {
    type Event = Atr;
}

impl SymbolProvider for AtrId {
    fn symbol(&self) -> Symbol {
        self.parent.symbol()
    }
}

// ================================================================================================
// Rate Of Change
// ================================================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RocId {
    pub parent: OhlcvId,
    pub lookback: LookbackWindow,
}

/// Represents the Rate of Change (ROC) over a specific lookback window.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Roc {
    /// The point in time when this rate of change was recorded (the end of the window).
    pub timestamp: DateTime<Utc>,

    /// The point in time of the historical reference price (the start of the window).
    pub window_start: DateTime<Utc>,

    /// The raw price difference between the current close and the historical close.
    pub absolute_change: PriceDelta,

    /// The relative rate of change expressed as a ratio.
    pub percentage: f64,
}

impl MarketEvent for Roc {
    fn point_in_time(&self) -> DateTime<Utc> {
        self.timestamp
    }
    fn opened_at(&self) -> DateTime<Utc> {
        self.window_start
    }
}

impl StreamId for RocId {
    type Event = Roc;
}

impl SymbolProvider for RocId {
    fn symbol(&self) -> Symbol {
        self.parent.symbol()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    /// Parse RFC3339 timestamp string to `DateTime`<Utc>.
    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    // ============================================================================
    // Price Reachability Tests
    // ============================================================================

    fn mock_sma(price: f64) -> Sma {
        Sma {
            timestamp: ts("2026-05-01T00:00:00Z"),
            price: Price(price),
        }
    }

    fn mock_ema(price: f64) -> Ema {
        Ema {
            timestamp: ts("2026-05-01T00:00:00Z"),
            price: Price(price),
        }
    }

    fn mock_rsi(value: f64) -> Rsi {
        Rsi {
            timestamp: ts("2026-05-01T00:00:00Z"),
            price: Price(value),
        }
    }

    #[test]
    fn test_sma_long_reachability() {
        // We want to trigger a Long when SMA drops to 50000.0 or below
        let target = Price(50000.0);

        // 1. Undershoot (Miss): SMA is at 50000.1, hasn't dropped enough.
        assert!(!mock_sma(50000.1).price_reached(target, TradeType::Long));

        // 2. Exact Touch: SMA hits exactly 50000.0.
        assert!(mock_sma(50000.0).price_reached(target, TradeType::Long));

        // 3. Overshoot (Gap down): SMA gaps down to 49000.0, completely skipping 50000.0.
        assert!(mock_sma(49000.0).price_reached(target, TradeType::Long));
    }

    #[test]
    fn test_sma_short_reachability() {
        // We want to trigger a Short when SMA rises to 50000.0 or above
        let target = Price(50000.0);

        // 1. Undershoot (Miss): SMA is at 49999.9, hasn't risen enough.
        assert!(!mock_sma(49999.9).price_reached(target, TradeType::Short));

        // 2. Exact Touch: SMA hits exactly 50000.0.
        assert!(mock_sma(50000.0).price_reached(target, TradeType::Short));

        // 3. Overshoot (Gap up): SMA gaps up to 51000.0, completely skipping 50000.0.
        assert!(mock_sma(51000.0).price_reached(target, TradeType::Short));
    }

    #[test]
    fn test_ema_long_reachability() {
        let target = Price(100.5);

        // Test precision boundaries often encountered in floating-point math
        assert!(!mock_ema(100.50000001).price_reached(target, TradeType::Long));
        assert!(mock_ema(100.5).price_reached(target, TradeType::Long));
        assert!(mock_ema(100.49999999).price_reached(target, TradeType::Long));
    }

    #[test]
    fn test_ema_short_reachability() {
        let target = Price(100.5);

        assert!(
            !mock_ema(100.49999999).price_reached(target, TradeType::Short),
            "EMA is just below target"
        );
        assert!(
            mock_ema(100.5).price_reached(target, TradeType::Short),
            "EMA exactly hits target"
        );
        assert!(
            mock_ema(100.50000001).price_reached(target, TradeType::Short),
            "EMA spikes just above target"
        );
    }

    #[test]
    fn test_rsi_oversold_long() {
        // Classic strategy: Buy when RSI drops below 30
        let target = Price(30.0);

        // RSI is 31 (Not oversold enough)
        assert!(!mock_rsi(31.0).price_reached(target, TradeType::Long));

        // RSI is exactly 30 (Trigger)
        assert!(mock_rsi(30.0).price_reached(target, TradeType::Long));

        // RSI plummets to 15 (Trigger)
        assert!(mock_rsi(15.0).price_reached(target, TradeType::Long));
    }

    #[test]
    fn test_rsi_overbought_short() {
        // Classic strategy: Sell when RSI spikes above 70
        let target = Price(70.0);

        // RSI is 69.9 (Not overbought enough)
        assert!(!mock_rsi(69.9).price_reached(target, TradeType::Short));

        // RSI is exactly 70.0 (Trigger)
        assert!(mock_rsi(70.0).price_reached(target, TradeType::Short));

        // RSI rockets to 85.5 (Trigger)
        assert!(mock_rsi(85.5).price_reached(target, TradeType::Short));
    }
}
