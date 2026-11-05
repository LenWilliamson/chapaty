use chrono::Duration;
use polars::prelude::{Expr, NULL, lit};
use serde::{Deserialize, Serialize};

pub use crate::math::deviation::DeviationKind;

/// How an exponentially weighted mean computes its first value.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub enum EwmSeed {
    /// The first output is the plain average of the first `window` inputs
    /// (Wilder's original definition).
    Sma,
    /// Start from the first input. The first output appears after `window`
    /// inputs.
    #[default]
    FirstValue,
}

/// Value of the RSI when the window contains no movement at all, i.e. both
/// the average gain and the average loss are zero.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub enum RsiFlat {
    /// Report the neutral value `50.0`.
    #[default]
    Neutral,
    /// Report `0.0`.
    Zero,
    /// Report `100.0`.
    Max,
    /// Report no value (`None`) while the window is flat.
    Undefined,
}

impl RsiFlat {
    pub(super) const fn value(self) -> Option<f64> {
        match self {
            Self::Neutral => Some(50.0),
            Self::Zero => Some(0.0),
            Self::Max => Some(100.0),
            Self::Undefined => None,
        }
    }

    /// The value as a Polars expression, `null` for [`RsiFlat::Undefined`].
    pub(super) fn to_expr(self) -> Expr {
        self.value().map_or_else(|| lit(NULL), lit)
    }
}

/// Defines the smoothing algorithm used to average the True Range.
/// Traders often experiment with different smoothing types depending on their
/// responsiveness requirements.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum AtrSmoothingType {
    /// J. Welles Wilder's original smoothing method (Running Moving Average /
    /// RMA). Formula: alpha = 1 / `window_size`
    Rma(EwmSeed),
    /// Simple Moving Average (SMA).
    Sma,
    /// Exponential Moving Average (EMA).
    Ema(EwmSeed),
}

impl Default for AtrSmoothingType {
    fn default() -> Self {
        Self::Rma(EwmSeed::default())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct AtrConfig {
    pub(super) window: u16,
    pub(super) smoothing: AtrSmoothingType,
}

impl AtrConfig {
    /// Creates a new `AtrConfig` with the given window size and
    /// [`AtrSmoothingType::default`] smoothing type.
    ///
    /// # Arguments
    ///
    /// * `window` - The window size for the ATR calculation.
    ///
    /// # Panics
    /// Panics if `window == 0`.
    #[must_use]
    pub fn new(window: u16) -> Self {
        assert!(window > 0, "window must be > 0, but got {window} <= 0");
        Self {
            window,
            smoothing: AtrSmoothingType::default(),
        }
    }

    #[must_use]
    pub const fn with_smoothing(self, smoothing: AtrSmoothingType) -> Self {
        Self { smoothing, ..self }
    }

    #[must_use]
    pub const fn window(&self) -> u16 {
        self.window
    }

    #[must_use]
    pub const fn smoothing(&self) -> AtrSmoothingType {
        self.smoothing
    }
}

/// Settings of an Exponential Moving Average (EMA).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EmaConfig {
    /// The number of bars the EMA averages over.
    pub window: EmaWindow,
    /// How the first value is computed.
    pub seed: EwmSeed,
}

impl EmaConfig {
    /// Creates an EMA configuration that starts from the first input
    /// ([`EwmSeed::FirstValue`]).
    #[must_use]
    pub const fn new(window: EmaWindow) -> Self {
        Self {
            window,
            seed: EwmSeed::FirstValue,
        }
    }

    #[must_use]
    pub const fn with_seed(self, seed: EwmSeed) -> Self {
        Self { seed, ..self }
    }
}

/// Settings of a Wilder running moving average (RMA).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RmaConfig {
    /// The number of bars the RMA averages over.
    pub window: RmaWindow,
    /// How the first value is computed.
    pub seed: EwmSeed,
}

impl RmaConfig {
    /// Creates an RMA configuration that starts from the first input
    /// ([`EwmSeed::FirstValue`]). Use [`with_seed`](Self::with_seed) with
    /// [`EwmSeed::Sma`] for Wilder's original definition.
    #[must_use]
    pub const fn new(window: RmaWindow) -> Self {
        Self {
            window,
            seed: EwmSeed::FirstValue,
        }
    }

    #[must_use]
    pub const fn with_seed(self, seed: EwmSeed) -> Self {
        Self { seed, ..self }
    }
}

/// Settings of a Relative Strength Index (RSI).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RsiConfig {
    /// The number of bars the average gain and loss are smoothed over.
    pub window: RsiWindow,
    /// How the first average gain and loss are computed.
    pub seed: EwmSeed,
    /// The value reported when the window contains no movement at all.
    pub flat: RsiFlat,
}

impl RsiConfig {
    /// Creates an RSI configuration that starts from the first input
    /// ([`EwmSeed::FirstValue`]) and reports `50.0` for a flat window
    /// ([`RsiFlat::Neutral`]).
    #[must_use]
    pub const fn new(window: RsiWindow) -> Self {
        Self {
            window,
            seed: EwmSeed::FirstValue,
            flat: RsiFlat::Neutral,
        }
    }

    #[must_use]
    pub const fn with_seed(self, seed: EwmSeed) -> Self {
        Self { seed, ..self }
    }

    #[must_use]
    pub const fn with_flat(self, flat: RsiFlat) -> Self {
        Self { flat, ..self }
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EmaWindow(pub u16);

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SmaWindow(pub u16);

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RsiWindow(pub u16);

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RmaWindow(pub u16);

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct VwapWindow(pub u16);

/// Settings of a rolling Volume-Weighted Average Price (VWAP) with a
/// deviation around it.
///
/// Streaming and batch rolling VWAP take the same settings, so both give the
/// same values for the same data. The per-bar price of the OHLCV version
/// ([`AggregatedPrice`](crate::data::domain::AggregatedPrice)) is not part of
/// it, because trades have no such choice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RollingVwapConfig {
    /// The number of observations (bars or trades) in the window.
    pub window: VwapWindow,
    /// How the spread around the VWAP is measured.
    pub deviation: DeviationKind,
}

impl RollingVwapConfig {
    /// Creates a rolling VWAP configuration that measures the spread as the
    /// mean absolute deviation ([`DeviationKind::MeanAbsolute`]).
    #[must_use]
    pub const fn new(window: VwapWindow) -> Self {
        Self {
            window,
            deviation: DeviationKind::MeanAbsolute,
        }
    }

    #[must_use]
    pub const fn with_deviation(self, deviation: DeviationKind) -> Self {
        Self { deviation, ..self }
    }
}

/// Defines how far back the indicator should look.
/// Gives the trader the degree of freedom to mix time-based and bar-based
/// strategies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum LookbackWindow {
    /// A fixed number of bars/events (e.g., 14 periods).
    Bars(usize),
    /// A fixed time duration.
    Time(Duration),
}

impl LookbackWindow {
    /// Create a time-based window in seconds.
    ///
    /// # Panics
    /// Panics if the duration cannot be represented by `chrono::Duration`.
    #[must_use]
    pub fn seconds(secs: u64) -> Self {
        #[expect(
            clippy::expect_used,
            reason = "Trading lookback durations specified in seconds are highly bounded, \
                      and will never realistically approach Chrono's maximum limit of ~292,000 years."
        )]
        Self::Time(
            Duration::from_std(std::time::Duration::from_secs(secs))
                .expect("Duration exceeds Chrono limit"),
        )
    }

    /// Create a time-based window in minutes.
    ///
    /// # Panics
    /// Panics if the duration cannot be represented by `chrono::Duration`.
    #[must_use]
    pub fn minutes(mins: u64) -> Self {
        #[expect(
            clippy::expect_used,
            reason = "Trading lookback durations specified in minutes are highly bounded, \
                      and will never realistically approach Chrono's maximum limit of ~292,000 years."
        )]
        Self::Time(
            Duration::from_std(std::time::Duration::from_secs(mins * 60))
                .expect("Duration exceeds Chrono limit"),
        )
    }

    /// Create a time-based window in hours.
    ///
    /// # Panics
    /// Panics if the duration cannot be represented by `chrono::Duration`.
    #[must_use]
    pub fn hours(hours: u64) -> Self {
        #[expect(
            clippy::expect_used,
            reason = "Trading lookback durations specified in hours are highly bounded, \
                      and will never realistically approach Chrono's maximum limit of ~292,000 years."
        )]
        Self::Time(
            Duration::from_std(std::time::Duration::from_hours(hours))
                .expect("Duration exceeds Chrono limit"),
        )
    }

    /// Create a time-based window in days.
    ///
    /// # Panics
    /// Panics if the duration cannot be represented by `chrono::Duration`.
    #[must_use]
    pub fn days(days: u64) -> Self {
        #[expect(
            clippy::expect_used,
            reason = "Trading lookback durations specified in days are highly bounded, \
                      and will never realistically approach Chrono's maximum limit of ~292,000 years."
        )]
        Self::Time(
            Duration::from_std(std::time::Duration::from_hours(days * 24))
                .expect("Duration exceeds Chrono limit"),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic(expected = "window must be > 0")]
    fn atr_cfg_new_panics_on_zero_window() {
        let _config = AtrConfig::new(0);
    }
}
