use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

use crate::math::StreamingIndicator;

/// The required input for time-aware or bar-aware lookback indicators.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct MomentumInput {
    pub timestamp: DateTime<Utc>,
    pub value: f64,
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
    /// A fixed time duration.
    Time(Duration),
}

impl LookbackWindow {
    /// Create a time-based window in seconds.
    ///
    /// Panics if the duration exceeds the Chrono limit.
    pub fn seconds(secs: u64) -> Self {
        Self::Time(
            Duration::from_std(std::time::Duration::from_secs(secs))
                .expect("Duration exceeds Chrono limit"),
        )
    }

    /// Create a time-based window in minutes.
    ///
    /// Panics if the duration exceeds the Chrono limit.
    pub fn minutes(mins: u64) -> Self {
        Self::Time(
            Duration::from_std(std::time::Duration::from_secs(mins * 60))
                .expect("Duration exceeds Chrono limit"),
        )
    }

    /// Create a time-based window in hours.
    ///
    /// Panics if the duration exceeds the Chrono limit.
    pub fn hours(hours: u64) -> Self {
        Self::Time(
            Duration::from_std(std::time::Duration::from_hours(hours))
                .expect("Duration exceeds Chrono limit"),
        )
    }

    /// Create a time-based window in days.
    ///
    /// This is equivalent to `hours(days * 24)`.
    ///
    /// Panics if the duration exceeds the Chrono limit.
    pub fn days(days: u64) -> Self {
        Self::Time(
            Duration::from_std(std::time::Duration::from_hours(days * 24))
                .expect("Duration exceeds Chrono limit"),
        )
    }
}

/// An internal buffer that tracks historical data points and automatically
/// evicts stale data based on the configured `LookbackWindow`.
#[derive(Debug, Clone)]
struct HistoricalBuffer {
    window: LookbackWindow,
    buffer: VecDeque<MomentumInput>,
}

impl HistoricalBuffer {
    fn new(window: LookbackWindow) -> Self {
        let capacity = match window {
            LookbackWindow::Bars(n) => n + 2,
            // Convert time window to capacity in minutes, rounded up to nearest minute. Worst case for minute based OHLCV data.
            LookbackWindow::Time(d) => ((d.num_seconds() / 60) + 2) as usize,
        };

        Self {
            window,
            buffer: VecDeque::with_capacity(capacity),
        }
    }

    /// Pushes the new value into the buffer, drops stale values, and returns the reference value (C_n).
    fn push(&mut self, current: MomentumInput) -> Option<MomentumInput> {
        self.buffer.push_back(current);

        match self.window {
            LookbackWindow::Bars(n) => {
                while self.buffer.len() > n + 1 {
                    self.buffer.pop_front();
                }

                if self.buffer.len() == n + 1 {
                    self.buffer.front().copied()
                } else {
                    None
                }
            }
            LookbackWindow::Time(time_limit) => {
                while let Some(front) = self.buffer.front() {
                    let diff = current.timestamp.signed_duration_since(front.timestamp);
                    if diff > time_limit {
                        self.buffer.pop_front();
                    } else {
                        break;
                    }
                }

                if self.buffer.len() >= 2 {
                    self.buffer.front().copied()
                } else {
                    None
                }
            }
        }
    }

    fn reset(&mut self) {
        self.buffer.clear();
    }
}

/// Momentum Indicator.
/// Measures the absolute change in price over a specific lookback window.
/// Formula: Momentum = Close_{current} - Close_{n}
#[derive(Debug, Clone)]
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
        if let Some(historical) = self.buffer.push(current) {
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
