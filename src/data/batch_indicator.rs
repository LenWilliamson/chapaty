pub mod ohlcv;
pub mod trades;

use serde::{Deserialize, Serialize};

use crate::error::{ChapatyError, DataError};

/// Stateless cumulative Volume-Weighted Average Price
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct VwapConfig;

/// Configuration for extracting session ranges natively in Polars.
/// Timezone is stored as a String (e.g., "America/New_York") for easy Polars interop.
#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SessionConfig {
    pub timezone: String,
    pub start_h: u8,
    pub start_m: u8,
    pub end_h: u8,
    pub end_m: u8,
}

/// A trait enabling builder-pattern injection of batch indicators.
pub trait WithBatchIndicators: Sized {
    type BatchIndicator: Clone;

    fn with_indicator(self, kind: Self::BatchIndicator) -> Self;

    fn with_indicators(self, kinds: &[Self::BatchIndicator]) -> Self {
        kinds
            .iter()
            .fold(self, |acc, kind| acc.with_indicator(kind.clone()))
    }
}

fn convert_err(e: polars::error::PolarsError) -> ChapatyError {
    ChapatyError::Data(DataError::DataFrame(format!(
        "Error while building batch indicator: {e}"
    )))
}
