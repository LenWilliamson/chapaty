use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    data::{
        domain::{Price, Symbol},
        event::{
            IndicatorValueProvider, MarketEvent, OhlcvId, PriceReachable, StreamId, SymbolProvider,
        },
    },
    gym::trading::TradeType,
    indicator::batch::ohlcv::{EmaWindow, RsiWindow, SmaWindow},
};

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
    fn price_reached(&self, target_price: Price, direction: TradeType) -> bool {
        match direction {
            TradeType::Long => self.price.0 <= target_price.0,
            TradeType::Short => self.price.0 >= target_price.0,
        }
    }
}

impl IndicatorValueProvider for Ema {
    fn value(&self) -> Price {
        self.price
    }
    fn timestamp(&self) -> DateTime<Utc> {
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
    fn symbol(&self) -> Symbol {
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
    fn price_reached(&self, target_price: Price, direction: TradeType) -> bool {
        match direction {
            TradeType::Long => self.price.0 <= target_price.0,
            TradeType::Short => self.price.0 >= target_price.0,
        }
    }
}

impl IndicatorValueProvider for Rsi {
    fn value(&self) -> Price {
        self.price
    }
    fn timestamp(&self) -> DateTime<Utc> {
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
    fn symbol(&self) -> Symbol {
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
    fn price_reached(&self, target_price: Price, direction: TradeType) -> bool {
        match direction {
            TradeType::Long => self.price.0 <= target_price.0,
            TradeType::Short => self.price.0 >= target_price.0,
        }
    }
}

impl IndicatorValueProvider for Sma {
    fn value(&self) -> Price {
        self.price
    }
    fn timestamp(&self) -> DateTime<Utc> {
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
    fn symbol(&self) -> Symbol {
        self.parent.symbol()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    /// Parse RFC3339 timestamp string to DateTime<Utc>.
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
