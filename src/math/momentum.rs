use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{collections::VecDeque, time::Duration};

use crate::math::StreamingIndicator;

// ================================================================================================
// Inputs & Configuration
// ================================================================================================

/// The required input for time-aware or bar-aware lookback indicators.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MomentumInput {
    pub timestamp: DateTime<Utc>,
    pub value: f64, // Typically the Close price
}

impl From<(DateTime<Utc>, f64)> for MomentumInput {
    fn from((timestamp, value): (DateTime<Utc>, f64)) -> Self {
        Self { timestamp, value }
    }
}

/// Defines how far back the indicator should look.
/// Gives the trader the degree of freedom to mix time-based and bar-based strategies.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum LookbackWindow {
    /// A fixed number of bars/events (e.g., 14 periods).
    Bars(usize),
    /// A fixed time duration using standard library `Duration`.
    Time(Duration),
}

impl LookbackWindow {
    /// Helper to easily create a time-based window in seconds.
    pub fn seconds(secs: u64) -> Self {
        Self::Time(Duration::from_secs(secs))
    }

    /// Helper to easily create a time-based window in minutes.
    pub fn minutes(mins: u64) -> Self {
        Self::Time(Duration::from_secs(mins * 60))
    }

    /// Helper to easily create a time-based window in hours.
    pub fn hours(hours: u64) -> Self {
        Self::Time(Duration::from_secs(hours * 3600))
    }

    /// Helper to easily create a time-based window in days.
    pub fn days(days: u64) -> Self {
        Self::Time(Duration::from_secs(days * 86400))
    }
}

// ================================================================================================
// Core Logic: The Historical Buffer
// ================================================================================================

/// An internal buffer that tracks historical data points and automatically
/// evicts stale data based on the configured `LookbackWindow`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct HistoricalBuffer {
    window: LookbackWindow,
    buffer: VecDeque<MomentumInput>,
}

impl HistoricalBuffer {
    pub(crate) fn new(window: LookbackWindow) -> Self {
        let capacity = match window {
            LookbackWindow::Bars(n) => n + 1,
            LookbackWindow::Time(_) => 60, // Arbitrary starting capacity, will auto-grow
        };

        Self {
            window,
            buffer: VecDeque::with_capacity(capacity),
        }
    }

    /// Pushes the new value into the buffer, drops stale values, and returns the reference value (C_n).
    pub(crate) fn push_and_get_historical(
        &mut self,
        current: MomentumInput,
    ) -> Option<MomentumInput> {
        self.buffer.push_back(current);

        match self.window {
            LookbackWindow::Bars(n) => {
                // We need exactly (n + 1) elements to compare 'current' with 'n' bars ago.
                while self.buffer.len() > n + 1 {
                    self.buffer.pop_front();
                }

                if self.buffer.len() == n + 1 {
                    self.buffer.front().copied()
                } else {
                    None
                }
            }
            LookbackWindow::Time(duration) => {
                // Safely convert std::time::Duration to chrono::Duration
                let time_limit =
                    chrono::Duration::from_std(duration).expect("Duration exceeds Chrono limit");

                // Remove elements that are strictly older than the requested time window.
                while let Some(front) = self.buffer.front() {
                    let diff = current.timestamp.signed_duration_since(front.timestamp);
                    if diff > time_limit {
                        self.buffer.pop_front();
                    } else {
                        break;
                    }
                }

                // To calculate momentum, we need at least an entry and a historical reference
                if self.buffer.len() >= 2 {
                    self.buffer.front().copied()
                } else {
                    None
                }
            }
        }
    }

    pub(crate) fn reset(&mut self) {
        self.buffer.clear();
    }
}

// ================================================================================================
// Indicator: Simple Momentum
// ================================================================================================

/// Momentum Indicator.
/// Measures the absolute change in price over a specific lookback window.
/// Formula: Momentum = Close_{current} - Close_{n}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingMomentum {
    buffer: HistoricalBuffer,
}

impl StreamingMomentum {
    pub fn new(window: LookbackWindow) -> Self {
        Self {
            buffer: HistoricalBuffer::new(window),
        }
    }
}

impl StreamingIndicator for StreamingMomentum {
    type Input = MomentumInput;
    type Output<'a> = Option<f64>;

    fn update(&mut self, current: Self::Input) -> Self::Output<'_> {
        if let Some(historical) = self.buffer.push_and_get_historical(current) {
            // Absolute difference
            Some(current.value - historical.value)
        } else {
            None
        }
    }

    fn reset(&mut self) {
        self.buffer.reset();
    }
}

// ================================================================================================
// Indicator: Rate of Change (ROC)
// ================================================================================================

/// Rate of Change (ROC) Indicator.
/// Measures the percentage change in price over a specific lookback window.
/// Formula: ROC = ((Close_{current} - Close_{n}) / Close_{n}) * 100
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingRoc {
    buffer: HistoricalBuffer,
}

impl StreamingRoc {
    pub fn new(window: LookbackWindow) -> Self {
        Self {
            buffer: HistoricalBuffer::new(window),
        }
    }
}

impl StreamingIndicator for StreamingRoc {
    type Input = MomentumInput;
    type Output<'a> = Option<f64>;

    fn update(&mut self, current: Self::Input) -> Self::Output<'_> {
        if let Some(historical) = self.buffer.push_and_get_historical(current) {
            // Guard against division by zero in extreme/synthetic edge cases
            if historical.value.abs() < f64::EPSILON {
                return None;
            }

            // Percentage difference
            let roc = ((current.value - historical.value) / historical.value) * 100.0;
            Some(roc)
        } else {
            None
        }
    }

    fn reset(&mut self) {
        self.buffer.reset();
    }
}
