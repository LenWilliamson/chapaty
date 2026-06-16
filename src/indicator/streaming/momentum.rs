use std::collections::VecDeque;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    error::{ChapatyResult, DataError},
    indicator::{config::LookbackWindow, streaming::StreamingIndicator},
};

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

/// An internal buffer that tracks historical data points and automatically
/// evicts stale data based on the configured `LookbackWindow`.
#[derive(Debug, Clone)]
struct HistoricalBuffer {
    window: LookbackWindow,
    buffer: VecDeque<MomentumInput>,
}

impl HistoricalBuffer {
    /// Creates a new `HistoricalBuffer`.
    ///
    /// # Errors
    ///
    /// Returns an error if the time window duration is too large to fit in a `usize` capacity.
    fn new(window: LookbackWindow) -> ChapatyResult<Self> {
        // Adding +2 prevents reallocation because we push BEFORE we pop in the update loop.
        let capacity = match window {
            LookbackWindow::Bars(n) => n + 2,
            // Convert time window to capacity in minutes, rounded up to nearest minute.
            // Worst case for minute based OHLCV data.
            LookbackWindow::Time(d) => {
                usize::try_from((d.num_seconds() / 60) + 1 + 2).map_err(DataError::from)?
            }
        };

        Ok(Self {
            window,
            buffer: VecDeque::with_capacity(capacity),
        })
    }

    /// Pushes the new value into the buffer, drops stale values, and returns the reference value (Input_{current - n}).
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

                match self.buffer.front() {
                    Some(front)
                        if current.timestamp.signed_duration_since(front.timestamp)
                            >= time_limit =>
                    {
                        Some(*front)
                    }
                    _ => None,
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
    /// The point in time of the historical reference price (the start of the window).
    pub window_start: DateTime<Utc>,
    /// The absolute point change: $Close_{current} - Close_{current - n}$
    pub absolute: f64,
    /// The percentage rate of change: $((Close_{current} - Close_{current - n}) / Close_{current - n}) \times 100$
    pub roc: f64,
}

/// Streaming Rate of Change (ROC) Indicator.
/// Measures both the absolute and percentage change in price over a specific lookback window.
#[derive(Debug, Clone)]
pub struct StreamingRateOfChange {
    buffer: HistoricalBuffer,
}

impl StreamingRateOfChange {
    /// Creates a new `StreamingRateOfChange`.
    ///
    /// # Errors
    ///
    /// Returns an error if the time window duration is too large to fit in a `usize` capacity.
    pub fn new(window: LookbackWindow) -> ChapatyResult<Self> {
        Ok(Self {
            buffer: HistoricalBuffer::new(window)?,
        })
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

            Some(MomentumOutput {
                window_start: historical.timestamp,
                absolute,
                roc,
            })
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
    #![expect(
        clippy::unwrap_used,
        reason = "tests assert against known-valid fixtures; unwrap surfaces failures as panics that fail the test"
    )]
    use chrono::TimeZone;

    use super::*;

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
        let mut buffer = HistoricalBuffer::new(LookbackWindow::Bars(1)).unwrap();

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
        // Lookback = 60s. A point exactly 60s old is a valid reference and is both
        // retained and returned. A point only 1s old is NOT a 60s lookback, so once
        // the 60s-old point falls out of range the result is None rather than a
        // too-recent (and wrongly labelled) reference.
        let mut buffer = HistoricalBuffer::new(LookbackWindow::seconds(60)).unwrap();

        // Start
        buffer.update(input(0, 100.0));

        // 60s later: diff is exactly 60. Retained and returned.
        assert_eq!(buffer.update(input(60, 110.0)), Some(input(0, 100.0)));
        assert_eq!(buffer.buffer.len(), 2);

        // 61s later: the 0s tick is now 61s old and is evicted. The remaining 60s
        // tick is only 1s back — not a 60s lookback — so the result is None.
        assert_eq!(buffer.update(input(61, 120.0)), None);
    }

    /// Drives one gap-free sequence through a `Bars(n)` buffer and a
    /// `Time(n * period)` buffer and asserts they return the identical reference at
    /// every step. This is the core invariant: on regularly spaced bars a bar-count
    /// window and the equivalent time window must look back to the same point.
    fn assert_bars_matches_time(n: usize, period_secs: u64, points: &[(i64, f64)]) {
        let secs = (n as u64) * period_secs;
        let mut bars = HistoricalBuffer::new(LookbackWindow::Bars(n)).unwrap();
        let mut time = HistoricalBuffer::new(LookbackWindow::seconds(secs)).unwrap();

        for &(t, value) in points {
            let inp = input(t, value);
            assert_eq!(
                bars.update(inp),
                time.update(inp),
                "bars and time disagreed at t={t}"
            );
        }
    }

    #[test]
    fn bars_and_time_match_on_regular_one_minute_bars() {
        // 60s bars: a 1-bar lookback must equal a 60s window at every step.
        assert_bars_matches_time(
            1,
            60,
            &[
                (0, 100.0),
                (60, 110.0),
                (120, 90.0),
                (180, 95.0),
                (240, 105.0),
            ],
        );
    }

    #[test]
    fn bars_and_time_match_for_multi_bar_lookback() {
        // 60s bars, lookback of 3 bars == 180s window. Exercises warmup: both must
        // return None until a point exactly 180s back exists, then agree thereafter.
        assert_bars_matches_time(
            3,
            60,
            &[
                (0, 10.0),
                (60, 11.0),
                (120, 12.0),
                (180, 13.0),
                (240, 14.0),
                (300, 15.0),
            ],
        );
    }

    #[test]
    fn time_window_does_not_emit_during_warmup() {
        // A 120s window on 60s bars must NOT return a reference until a point is
        // actually 120s old. The 60s-old point at t=60 is too recent to be a 120s
        // lookback, so the result is None — matching Bars(2) warmup. This is the
        // exact case the old `len >= 2` guard got wrong (it returned the t=0 point).
        let mut buffer = HistoricalBuffer::new(LookbackWindow::seconds(120)).unwrap();
        assert_eq!(buffer.update(input(0, 100.0)), None);
        assert_eq!(buffer.update(input(60, 110.0)), None);
        assert_eq!(buffer.update(input(120, 120.0)), Some(input(0, 100.0)));
    }

    #[test]
    fn merged_momentum_and_roc_calculates_correctly() {
        let mut momentum = StreamingRateOfChange::new(LookbackWindow::Bars(1)).unwrap();

        assert_eq!(momentum.update(input(0, 50.0)), None);

        // 50 to 75
        // Absolute: 75 - 50 = +25.0
        // ROC: (25 / 50) * 100 = +50.0%
        assert_eq!(
            momentum.update(input(1, 75.0)),
            Some(MomentumOutput {
                window_start: ts(0),
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
                window_start: ts(1),
                absolute: -15.0,
                roc: -20.0
            })
        );
    }

    #[test]
    fn merged_indicator_safely_handles_division_by_zero() {
        let mut momentum = StreamingRateOfChange::new(LookbackWindow::Bars(1)).unwrap();

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
                window_start: ts(1),
                absolute: 10.0,
                roc: 100.0
            })
        );
    }
}
