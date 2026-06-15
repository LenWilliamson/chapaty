use chrono::Duration;
use serde::{Deserialize, Serialize};

/// Defines the smoothing algorithm used to average the True Range.
/// Traders often experiment with different smoothing types depending on their
/// responsiveness requirements.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
pub enum AtrSmoothingType {
    /// J. Welles Wilder's original smoothing method (Running Moving Average / RMA).
    /// Formula: alpha = 1 / `window_size`
    #[default]
    Wilders,
    /// Simple Moving Average (SMA).
    Sma,
    /// Exponential Moving Average (EMA).
    Ema,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct AtrConfig {
    pub(super) window: u16,
    pub(super) smoothing: AtrSmoothingType,
}

impl AtrConfig {
    /// Creates a new `AtrConfig` with the given window size and [`AtrSmoothingType::default`] smoothing type.
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

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EmaWindow(pub u16);

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SmaWindow(pub u16);

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RsiWindow(pub u16);

/// Defines how far back the indicator should look.
/// Gives the trader the degree of freedom to mix time-based and bar-based strategies.
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
