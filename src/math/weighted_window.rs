use serde::{Deserialize, Serialize};

use crate::{
    math::{accumulators::KahanSum, deviation::DeviationKind},
    ring_buffer::RingBuffer,
};

/// Running weighted mean `Σ w_i*x_i / Σ w_i` with Kahan-compensated sums.
///
/// An observation with a non-finite value, or a non-positive or non-finite
/// weight, is skipped: it contributes nothing, so a zero-weight observation
/// can neither pull the mean nor cause a division by zero when it is the
/// only input.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct WeightedMean {
    sum_weighted: KahanSum,
    sum_weight: KahanSum,
}

impl WeightedMean {
    /// Folds one observation into the running totals.
    #[must_use]
    pub fn add(self, value: f64, weight: f64) -> Self {
        if !value.is_finite() || !weight.is_finite() || weight <= 0.0 {
            return self;
        }
        Self {
            sum_weighted: self.sum_weighted.add(weight * value),
            sum_weight: self.sum_weight.add(weight),
        }
    }

    /// Total accepted weight so far.
    #[must_use]
    pub const fn total_weight(self) -> f64 {
        self.sum_weight.value()
    }

    /// Current mean, or `None` before any positive-weight input has arrived.
    #[must_use]
    pub fn value(self) -> Option<f64> {
        let total = self.total_weight();
        (total > 0.0).then(|| self.sum_weighted.value() / total)
    }

    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

/// Weighted mean and weighted deviation of a window.
///
/// Weights `w_i` (for example traded volumes) turn the plain average into a
/// weighted one: the value `x_i` counts with the share `p_i = w_i / Σw`.
///
/// Example: the values 10 and 20 with the weights 1 and 3 have the shares 0.25
/// and 0.75 and the mean `17.5`. With [`DeviationKind::MeanAbsolute`] the
/// deviation is `0.25*7.5 + 0.75*2.5 = 3.75`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WeightedStats {
    /// `μ = Σ w_i*x_i / Σ w_i`.
    pub mean: f64,
    /// `finalize(Σ p_i*transform(x_i − μ))`, measured as configured by
    /// [`DeviationKind`].
    pub deviation: f64,
}

/// Fixed-size window of `(value, weight)` pairs.
///
/// An observation with a non-finite value, or a non-positive or non-finite
/// weight, occupies a slot but is ignored by [`stats`](Self::stats).
///
/// Each call of `stats` costs _O_(`size`): the deviation depends on the
/// current mean and cannot be updated incrementally.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WeightedWindow {
    window: RingBuffer<(f64, f64)>,
    kind: DeviationKind,
}

impl WeightedWindow {
    /// # Panics
    ///
    /// Panics if `size` is zero.
    #[must_use]
    pub fn new(size: usize, kind: DeviationKind) -> Self {
        assert!(size > 0, "window size must be greater than zero");
        Self {
            window: RingBuffer::new(size),
            kind,
        }
    }

    /// Adds one observation. Once the window is full, the oldest one leaves.
    pub fn push(&mut self, value: f64, weight: f64) {
        self.window.push((value, weight));
    }

    /// Weighted mean and deviation of the window, or `None` while the window
    /// is not full yet and while its total weight is zero.
    #[must_use]
    pub fn stats(&self) -> Option<WeightedStats> {
        if !self.window.is_full() {
            return None;
        }

        let mean = self
            .window
            .iter()
            .fold(WeightedMean::default(), |acc, &(x, w)| acc.add(x, w))
            .value()?;

        let average = self
            .window
            .iter()
            .fold(WeightedMean::default(), |acc, &(x, w)| {
                acc.add(self.kind.transform(x - mean), w)
            })
            .value()?;

        Some(WeightedStats {
            mean,
            deviation: self.kind.finalize(average),
        })
    }

    pub fn clear(&mut self) {
        self.window.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn weighted_mean_skips_invalid_observations() {
        let acc = WeightedMean::default();
        assert_eq!(acc.value(), None);

        let acc = acc
            .add(f64::NAN, 1.0)
            .add(5.0, 0.0)
            .add(5.0, -1.0)
            .add(5.0, f64::INFINITY);
        assert_eq!(acc.value(), None);

        let acc = acc.add(10.0, 1.0).add(20.0, 3.0);
        assert_eq!(acc.value(), Some(17.5));
        assert_eq!(acc.total_weight(), 4.0);
    }

    #[test]
    fn window_is_none_until_full_and_after_clear() {
        let mut window = WeightedWindow::new(2, DeviationKind::MeanAbsolute);
        window.push(10.0, 1.0);
        assert_eq!(window.stats(), None);

        window.push(20.0, 3.0);
        assert!(window.stats().is_some());

        window.clear();
        assert_eq!(window.stats(), None);
    }

    #[test]
    fn window_computes_mean_and_both_deviations() {
        let mut mad = WeightedWindow::new(2, DeviationKind::MeanAbsolute);
        let mut std = WeightedWindow::new(2, DeviationKind::StdDev);
        for window in [&mut mad, &mut std] {
            window.push(10.0, 1.0);
            window.push(20.0, 3.0);
        }

        assert_eq!(
            mad.stats(),
            Some(WeightedStats {
                mean: 17.5,
                deviation: 3.75
            })
        );

        let std = std.stats();
        assert_eq!(std.map(|stats| stats.mean), Some(17.5));
        // A missing value becomes NaN, which fails the comparison.
        assert_f64_eq!(
            std.map_or(f64::NAN, |stats| stats.deviation),
            18.75_f64.sqrt()
        );
    }

    #[test]
    fn window_ignores_invalid_observations_but_keeps_their_slot() {
        let mut window = WeightedWindow::new(3, DeviationKind::MeanAbsolute);
        window.push(10.0, 1.0);
        window.push(f64::NAN, 5.0); // ignored, but fills a slot
        window.push(20.0, 3.0);

        assert_eq!(
            window.stats(),
            Some(WeightedStats {
                mean: 17.5,
                deviation: 3.75
            })
        );
    }

    #[test]
    fn window_is_none_while_total_weight_is_zero() {
        let mut window = WeightedWindow::new(2, DeviationKind::StdDev);
        window.push(10.0, 0.0);
        window.push(20.0, -1.0);
        assert_eq!(window.stats(), None);
    }
}
