use polars::prelude::{LazyFrame, SortMultipleOptions, col};
use serde::{Deserialize, Serialize};

use crate::{
    data::domain::SessionWindow,
    error::ChapatyResult,
    indicator::batch::{BatchCompute, IndicatorExprExt, LazyFrameIndicatorExt},
    transport::schema::CanonicalCol,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BatchTradesIndicator {
    Vwap,
    OvernightRange(SessionWindow),
}

impl BatchCompute for BatchTradesIndicator {
    fn pre_compute(&self, lf: LazyFrame) -> ChapatyResult<LazyFrame> {
        match self {
            BatchTradesIndicator::Vwap => pre_compute_trades_vwap(lf),
            BatchTradesIndicator::OvernightRange(session) => {
                pre_compute_overnight_range(*session, lf)
            }
        }
    }
}

// ================================================================================================
// LazyFrame Pre-Computations
// ================================================================================================

fn pre_compute_trades_vwap(lf: LazyFrame) -> ChapatyResult<LazyFrame> {
    Ok(lf.finalize_scalar(col(CanonicalCol::Price).vwap(col(CanonicalCol::Volume))))
}

fn pre_compute_overnight_range(session: SessionWindow, lf: LazyFrame) -> ChapatyResult<LazyFrame> {
    let session_date_col = col(CanonicalCol::PointInTime).session_date(session);

    let out_lf = lf
        .with_column(session_date_col.alias(CanonicalCol::Date))
        .filter(col(CanonicalCol::Date).is_not_null())
        .group_by([col(CanonicalCol::Date)])
        .agg([
            // --- Time Boundaries ---
            col(CanonicalCol::OpenTimestamp)
                .first()
                .alias("OpenTimestamp"),
            col(CanonicalCol::PointInTime)
                .last()
                .alias(CanonicalCol::PointInTime),
            // --- Trade Extremes (Derived from Price) ---
            col(CanonicalCol::Price)
                .max()
                .alias(CanonicalCol::SessionHigh),
            col(CanonicalCol::Price)
                .min()
                .alias(CanonicalCol::SessionLow),
            col(CanonicalCol::Volume)
                .sum()
                .alias(CanonicalCol::SessionVolume),
            // --- Session VWAP ---
            col(CanonicalCol::Price)
                .agg_vwap(col(CanonicalCol::Volume))
                .alias(CanonicalCol::SessionVwap),
        ])
        // Ensure chronological order
        .sort([CanonicalCol::PointInTime], SortMultipleOptions::default());

    Ok(out_lf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::{DataType, LazyCsvReader, LazyFileListReader, PlRefPath, TimeUnit};
    use std::path::PathBuf;

    // ============================================================================
    // Test Fixtures & Helpers
    // ============================================================================

    /// Returns the absolute path to the test fixtures directory.
    fn fixtures_path() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/gym")
    }

    /// Loads an OHLCV CSV fixture and maps column names to the canonical schema.
    fn load_ohlcv_fixture(filename: &str) -> LazyFrame {
        let path = fixtures_path().join("input").join(filename);

        LazyCsvReader::new(PlRefPath::new(path.as_os_str().to_str().expect("filepath")))
            .with_has_header(true)
            .finish()
            .expect("Failed to parse fixture CSV")
            .select([
                col("open_timestamp")
                    .cast(DataType::Datetime(
                        TimeUnit::Microseconds,
                        Some(polars::prelude::TimeZone::UTC),
                    ))
                    .alias(CanonicalCol::OpenTimestamp.as_str()),
                col("close_timestamp")
                    .cast(DataType::Datetime(
                        TimeUnit::Microseconds,
                        Some(polars::prelude::TimeZone::UTC),
                    ))
                    .alias(CanonicalCol::PointInTime.as_str()),
                // Rename the metrics (Implicitly drops 'exchange', 'symbol', etc.)
                col("open").alias(CanonicalCol::Open.as_str()),
                col("high").alias(CanonicalCol::High.as_str()),
                col("low").alias(CanonicalCol::Low.as_str()),
                col("close").alias(CanonicalCol::Close.as_str()),
                col("volume").alias(CanonicalCol::Volume.as_str()),
                col("quote_asset_volume").alias(CanonicalCol::QuoteAssetVolume.as_str()),
                col("number_of_trades").alias(CanonicalCol::NumberOfTrades.as_str()),
                col("taker_buy_base_asset_volume")
                    .alias(CanonicalCol::TakerBuyBaseAssetVolume.as_str()),
                col("taker_buy_quote_asset_volume")
                    .alias(CanonicalCol::TakerBuyQuoteAssetVolume.as_str()),
            ])
    }

    struct IndicatorTestCase {
        name: &'static str,
        indicator: BatchTradesIndicator,
        expected_file: &'static str,
    }

    // ============================================================================
    // Indicator Regression Tests
    // ============================================================================

    // ============================================================================
    // Indicator Tests
    // ============================================================================
}
