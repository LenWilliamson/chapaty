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
    pub fn seconds(secs: u64) -> Self {
        Self::Time(
            Duration::from_std(std::time::Duration::from_secs(secs))
                .expect("Duration exceeds Chrono limit"),
        )
    }

    /// Create a time-based window in minutes.
    pub fn minutes(mins: u64) -> Self {
        Self::Time(
            Duration::from_std(std::time::Duration::from_secs(mins * 60))
                .expect("Duration exceeds Chrono limit"),
        )
    }

    /// Create a time-based window in hours.
    pub fn hours(hours: u64) -> Self {
        Self::Time(
            Duration::from_std(std::time::Duration::from_hours(hours))
                .expect("Duration exceeds Chrono limit"),
        )
    }

    /// Create a time-based window in days.
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
        // Adding +2 prevents reallocation because we push BEFORE we pop in the update loop.
        let capacity = match window {
            LookbackWindow::Bars(n) => n + 2,
            // Convert time window to capacity in minutes, rounded up to nearest minute.
            // Worst case for minute based OHLCV data.
            LookbackWindow::Time(d) => ((d.num_seconds() / 60) + 2) as usize,
        };

        Self {
            window,
            buffer: VecDeque::with_capacity(capacity + 1),
        }
    }

    /// Pushes the new value into the buffer, drops stale values, and returns the reference value (C_n).
    fn update(&mut self, current: MomentumInput) -> Option<MomentumInput> {
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

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct MomentumOutput {
    /// The absolute point change: $Close_{current} - Close_{current - n}$
    pub absolute: f64,
    /// The percentage rate of change: $((Close_{current} - Close_{current - n}) / Close_{current - n}) \times 100$
    pub roc: f64,
}

/// Momentum & Rate of Change (ROC) Indicator.
/// Measures the absolute and percentage change in price over a specific lookback window.
#[derive(Debug, Clone)]
pub struct StreamingRateOfChange {
    buffer: HistoricalBuffer,
}

impl StreamingRateOfChange {
    pub fn new(window: LookbackWindow) -> Self {
        Self {
            buffer: HistoricalBuffer::new(window),
        }
    }
}

impl StreamingIndicator for StreamingRateOfChange {
    type Input = MomentumInput;
    type Output<'a> = Option<MomentumOutput>;

    fn update(&mut self, current: Self::Input) -> Self::Output<'_> {
        if let Some(historical) = self.buffer.update(current) {
            if historical.value.abs() < f64::EPSILON {
                return None;
            }

            let absolute = current.value - historical.value;
            let roc = (absolute / historical.value) * 100.0;

            Some(MomentumOutput { absolute, roc })
        } else {
            None
        }
    }

    fn reset(&mut self) {
        self.buffer.reset();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn ts(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0).single().unwrap()
    }

    fn input(seconds: i64, value: f64) -> MomentumInput {
        MomentumInput {
            timestamp: ts(seconds),
            value,
        }
    }

    #[test]
    fn historical_buffer_bars_capacity_and_off_by_one_edge_cases() {
        // Lookback = 1 means we compare CURRENT against PREVIOUS.
        // Needs exactly 2 elements in the buffer. The +2 capacity guarantees
        // pushing the 3rd element won't trigger a reallocation before the pop.
        let mut buffer = HistoricalBuffer::new(LookbackWindow::Bars(1));

        // T=0: Push 10. Len=1. Returns None.
        assert_eq!(buffer.update(input(0, 10.0)), None);
        assert_eq!(buffer.buffer.len(), 1);

        // T=1: Push 15. Len=2. Returns 10 (the n=1 history).
        assert_eq!(buffer.update(input(1, 15.0)), Some(input(0, 10.0)));
        assert_eq!(buffer.buffer.len(), 2);

        // T=2: Push 20. Queue temporarily holds 3, immediately pops index 0.
        // Returns 15. Proves we strictly bound to n+1 elements long-term.
        assert_eq!(buffer.update(input(2, 20.0)), Some(input(1, 15.0)));
        assert_eq!(buffer.buffer.len(), 2);
    }

    #[test]
    fn historical_buffer_time_strict_boundary_retention() {
        // Lookback = 60s. We MUST retain an element if diff == 60 exactly.
        let mut buffer = HistoricalBuffer::new(LookbackWindow::seconds(60));

        // Start
        buffer.update(input(0, 100.0));

        // 60s Later: diff is exactly 60. Should NOT be popped.
        assert_eq!(buffer.update(input(60, 110.0)), Some(input(0, 100.0)));
        assert_eq!(buffer.buffer.len(), 2);

        // 61s Later: diff is 61. The 0s tick MUST be popped.
        // Returns the 60s tick as the new reference.
        assert_eq!(buffer.update(input(61, 120.0)), Some(input(60, 110.0)));
        assert_eq!(buffer.buffer.len(), 2);
    }

    #[test]
    fn merged_momentum_and_roc_calculates_correctly() {
        let mut momentum = StreamingRateOfChange::new(LookbackWindow::Bars(1));

        assert_eq!(momentum.update(input(0, 50.0)), None);

        // 50 to 75
        // Absolute: 75 - 50 = +25.0
        // ROC: (25 / 50) * 100 = +50.0%
        assert_eq!(
            momentum.update(input(1, 75.0)),
            Some(MomentumOutput {
                absolute: 25.0,
                roc: 50.0
            })
        );

        // 75 to 60
        // Absolute: 60 - 75 = -15.0
        // ROC: (-15 / 75) * 100 = -20.0%
        assert_eq!(
            momentum.update(input(2, 60.0)),
            Some(MomentumOutput {
                absolute: -15.0,
                roc: -20.0
            })
        );
    }

    #[test]
    fn merged_indicator_safely_handles_division_by_zero() {
        let mut momentum = StreamingRateOfChange::new(LookbackWindow::Bars(1));

        // Simulate an asset or synthetic spread priced at exactly 0.0
        assert_eq!(momentum.update(input(0, 0.0)), None);

        // Next input arrives. Math would normally divide by 0.0, but guard catches it.
        // Should return None gracefully instead of panicking or outputting NaN.
        assert_eq!(momentum.update(input(1, 10.0)), None);

        // Next input arrives. Reference point is now 10.0.
        // Absolute = 10.0. ROC = 100%. State machine recovers flawlessly.
        assert_eq!(
            momentum.update(input(2, 20.0)),
            Some(MomentumOutput {
                absolute: 10.0,
                roc: 100.0
            })
        );
    }
}
