use polars::{
    prelude::{EWMOptions, LazyFrame, RollingOptionsFixedWindow, SortMultipleOptions, col, lit},
    series::ops::NullBehavior,
};
use serde::{Deserialize, Serialize};

use crate::{error::ChapatyResult, transport::schema::CanonicalCol};

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EmaWindow(pub u16);

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SmaWindow(pub u16);

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RsiWindow(pub u16);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TechnicalIndicator {
    Ema(EmaWindow),
    Sma(SmaWindow),
    Rsi(RsiWindow),
}

impl EmaWindow {
    pub fn pre_compute_ema(&self, lf: LazyFrame) -> ChapatyResult<LazyFrame> {
        let window = self.0;

        // Standard EMA formula: alpha = 2 / (span + 1)
        let alpha = 2.0 / (window as f64 + 1.0);
        let options = EWMOptions {
            alpha,
            // Use recursive calculation
            adjust: false,

            // Do not apply statistical sample correction; we want the raw weighted average.
            bias: false,

            // Don't emit values until we have seen 'window' trades.
            // This avoids noisy, highly-volatile values at the start of the stream.
            min_periods: window as usize,

            // If a price is missing, skip the decay step for that row.
            // This ensures the EMA doesn't artificially drop if we have gaps.
            ignore_nulls: true,
        };

        Ok(lf
            .sort(
                [CanonicalCol::Timestamp],
                SortMultipleOptions::default().with_maintain_order(false),
            )
            .select([
                col(CanonicalCol::Timestamp).alias(CanonicalCol::Timestamp),
                col(CanonicalCol::Close)
                    .ewm_mean(options)
                    .alias(CanonicalCol::Price),
            ])
            .drop_nulls(None))
    }
}
impl SmaWindow {
    pub fn pre_compute_sma(&self, lf: LazyFrame) -> ChapatyResult<LazyFrame> {
        let window = self.0;
        let options = RollingOptionsFixedWindow {
            window_size: window as usize,
            min_periods: window as usize, // Strict: Require full window validity
            weights: None,                // Standard SMA is unweighted
            center: false,                // False prevents look-ahead bias
            fn_params: None,
        };

        Ok(lf
            .sort(
                [CanonicalCol::Timestamp],
                SortMultipleOptions::default().with_maintain_order(false),
            )
            .select([
                col(CanonicalCol::Timestamp).alias(CanonicalCol::Timestamp),
                col(CanonicalCol::Close)
                    .rolling_mean(options)
                    .alias(CanonicalCol::Price),
            ])
            .drop_nulls(None))
    }
}

impl RsiWindow {
    pub fn pre_compute_rsi(&self, lf: LazyFrame) -> ChapatyResult<LazyFrame> {
        let window = self.0;
        // Wilder's Smoothing for RSI: alpha = 1 / N
        let alpha = 1.0 / (window as f64);

        // Wilder's Smoothing (effectively an EMA with alpha = 1/window)
        // Note: Some RSI implementations use SMA, but Wilder's is standard.
        let options = EWMOptions {
            alpha,
            // Use recursive calculation
            adjust: false,

            // Do not apply statistical sample correction; we want the raw weighted average.
            bias: false,

            // Don't emit values until we have seen 'window' trades.
            // This avoids noisy, highly-volatile values at the start of the stream.
            min_periods: window as usize,

            // If a price is missing, skip the decay step for that row.
            // This ensures the EMA doesn't artificially drop if we have gaps.
            ignore_nulls: true,
        };

        let rsi_expr = {
            // 1. Calculate the CHANGE (P_t - P_t-1)
            let delta = col(CanonicalCol::Close).diff(lit(1), NullBehavior::Ignore);

            // 2. Separate Gains (Up moves) and Losses (Down moves)
            let gain = delta.clone().clip(lit(0), lit(f64::MAX));
            let loss = delta.clip(lit(f64::MIN), lit(0)).abs();

            // 3. Apply Wilder's Smoothing
            let avg_gain = gain.ewm_mean(options);
            let avg_loss = loss.ewm_mean(options);

            // 4. Calculate Ratio and Normalize to 0-100
            let rs = avg_gain / avg_loss;
            lit(100.0) - (lit(100.0) / (lit(1.0) + rs))
        };

        Ok(lf
            .sort(
                [CanonicalCol::Timestamp],
                SortMultipleOptions::default().with_maintain_order(false),
            )
            .select([
                col(CanonicalCol::Timestamp).alias(CanonicalCol::Timestamp),
                rsi_expr.alias(CanonicalCol::Price),
            ])
            .drop_nulls(None))
    }
}
