use serde::{Deserialize, Serialize};

// ============================================================================
// ATR (Average True Range)
// ============================================================================

/// Defines the smoothing algorithm used to average the True Range.
/// Traders often experiment with different smoothing types depending on their
/// responsiveness requirements.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq, Hash)]
pub enum AtrSmoothingType {
    /// J. Welles Wilder's original smoothing method (Running Moving Average / RMA).
    /// Formula: alpha = 1 / window_size
    #[default]
    Wilders,
    /// Simple Moving Average (SMA).
    Sma,
    /// Exponential Moving Average (EMA).
    Ema,
}

/// The blueprint for an Average True Range (ATR) indicator.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct AtrConfig {
    pub window: u16,
    pub smoothing: AtrSmoothingType,
}

// ============================================================================
// Other Blueprints ...
// ============================================================================
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VwapConfig(pub AggregatedPrice);

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EmaWindow(pub u16);

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SmaWindow(pub u16);

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RsiWindow(pub u16);

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RateOfChangeWindow(pub u16);
