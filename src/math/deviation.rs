use serde::{Deserialize, Serialize};

/// How far a set of values typically lies from its average.
///
/// Both variants answer the same question ("how far away from the average is
/// a typical value?"), but they count the distances differently.
///
/// # Building blocks
///
/// - `x_i` is the i-th of `n` values.
/// - `μ` is their average.
/// - `d_i = x_i − μ` is the distance of a value from that average. It is
///   negative if the value lies below the average.
///
/// # Calculation
///
/// Every variant computes the deviation in the same two steps, which differ
/// only in the function `h`:
///
/// 1. **Transform and average:** apply `h` to every distance and average the
///    results: `m = (h(d_1) + … + h(d_n)) / n`. The function `h` is
///    [`transform`](Self::transform). The averaging is done by the caller.
/// 2. **Finalize:** bring `m` back to the unit of the values: `deviation =
///    finalize(m)`. See [`finalize`](Self::finalize).
///
/// # Example
///
/// The values 2, 4, 4, 4, 5, 5, 7, 9 have the average `μ = 5` and the
/// distances −3, −1, −1, −1, 0, 0, 2, 4.
#[derive(
    Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
pub enum DeviationKind {
    /// Mean absolute deviation: `h(d) = |d|`, no finalizing needed.
    ///
    /// Example: `(3+1+1+1+0+0+2+4) / 8 = 1.5`.
    ///
    /// Every distance counts exactly as much as it is long, so single
    /// outliers have a limited influence.
    #[default]
    MeanAbsolute,
    /// Population standard deviation: `h(d) = d^2`, finalized with the square
    /// root.
    ///
    /// Example: `√((9+1+1+1+0+0+4+16) / 8) = √4 = 2`.
    ///
    /// Squaring makes all distances positive and lets big distances count much
    /// more. The root at the end turns the result back into the unit of the
    /// values (price instead of price^2). The result is never smaller than
    /// [`MeanAbsolute`](Self::MeanAbsolute).
    ///
    /// There is no Bessel correction (`n − 1`): the values are treated as the
    /// complete data, not as a sample from a larger population.
    StdDev,
}

impl DeviationKind {
    /// Step 1: applies this variant's function `h` to one distance
    /// `d = x − μ` and returns `h(d)`.
    ///
    /// - [`MeanAbsolute`](Self::MeanAbsolute): `|d|`
    /// - [`StdDev`](Self::StdDev): `d^2`
    ///
    /// The caller averages the results over all values.
    #[must_use]
    pub fn transform(self, distance: f64) -> f64 {
        match self {
            Self::MeanAbsolute => distance.abs(),
            Self::StdDev => distance * distance,
        }
    }

    /// Step 2: turns the average `m` of the transformed distances into the
    /// final deviation.
    ///
    /// - [`MeanAbsolute`](Self::MeanAbsolute): `m` unchanged
    /// - [`StdDev`](Self::StdDev): `√m`, which undoes the squaring of
    ///   [`transform`](Self::transform)
    #[must_use]
    pub fn finalize(self, average: f64) -> f64 {
        match self {
            Self::MeanAbsolute => average,
            Self::StdDev => average.sqrt(),
        }
    }
}
