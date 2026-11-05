use serde::{Deserialize, Serialize};

/// A precise floating-point accumulator using the [Kahan Summation Algorithm](https://en.wikipedia.org/wiki/Kahan_summation_algorithm).
/// Implemented using move semantics for functional, immutable state
/// transitions.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct KahanSum {
    sum: f64,
    c: f64,
}

impl KahanSum {
    /// Consumes the current state and yields a new [`KahanSum`] containing the
    /// updated running totals.
    pub fn add(self, value: f64) -> Self {
        let y = value - self.c;
        let t = self.sum + y;
        let c = (t - self.sum) - y;

        Self { sum: t, c }
    }

    /// Returns the current mathematically compensated sum.
    pub const fn value(self) -> f64 {
        self.sum
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn kahan_sum_recovers_precision_lost_by_naive_summation() {
        // A leading 1.0 followed by many terms below its ULP: naive addition
        // drops each one, Kahan accumulates them.
        let mut values = vec![1.0];
        values.extend(std::iter::repeat_n(1e-16, 100));
        let truth = 100.0_f64.mul_add(1e-16, 1.0);

        let kahan = values
            .iter()
            .fold(KahanSum::default(), |acc, &v| acc.add(v))
            .value();
        let naive: f64 = values.iter().sum();

        assert_f64_eq!(kahan, truth); // exact
        assert_f64_eq!(naive, 1.0); // every tiny term lost
        assert!((kahan - truth).abs() < (naive - truth).abs());
    }
}
