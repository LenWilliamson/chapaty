use std::sync::Arc;

use polars::prelude::{LazyFrame, Schema, SortMultipleOptions, col};
use serde::{Deserialize, Serialize};

use crate::{
    data::domain::SessionWindow,
    error::ChapatyResult,
    indicator::batch::{BatchCompute, IndicatorExprExt, LazyFrameIndicatorExt},
    transport::schema::CanonicalCol,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum BatchTradesIndicator {
    Vwap,
    OvernightRange(SessionWindow),
}

impl BatchCompute for BatchTradesIndicator {
    fn pre_compute(&self, lf: LazyFrame) -> ChapatyResult<LazyFrame> {
        match self {
            Self::Vwap => Ok(pre_compute_trades_vwap(lf)),
            Self::OvernightRange(session) => Ok(pre_compute_overnight_range(*session, lf)),
        }
    }
    fn output_schema(&self) -> Arc<Schema> {
        match self {
            Self::Vwap => Arc::new(Schema::from_iter(vec![
                CanonicalCol::PointInTime.field(),
                CanonicalCol::Price.field(),
            ])),
            Self::OvernightRange(_) => Arc::new(Schema::from_iter(vec![
                CanonicalCol::Date.field(),
                CanonicalCol::OpenTimestamp.field(),
                CanonicalCol::PointInTime.field(),
                CanonicalCol::SessionHigh.field(),
                CanonicalCol::SessionLow.field(),
                CanonicalCol::SessionVolume.field(),
                CanonicalCol::SessionVwap.field(),
            ])),
        }
    }
}

// ================================================================================================
// LazyFrame Pre-Computations
// ================================================================================================

fn pre_compute_trades_vwap(lf: LazyFrame) -> LazyFrame {
    lf.into_price_timeseries(col(CanonicalCol::Price).vwap_with_volume(col(CanonicalCol::Volume)))
}

fn pre_compute_overnight_range(session: SessionWindow, lf: LazyFrame) -> LazyFrame {
    let session_date_col = col(CanonicalCol::PointInTime).session_date(session);

    lf.with_column(session_date_col.alias(CanonicalCol::Date))
        .filter(col(CanonicalCol::Date).is_not_null())
        .group_by([col(CanonicalCol::Date)])
        .agg([
            col(CanonicalCol::PointInTime)
                .min()
                .alias(CanonicalCol::OpenTimestamp),
            col(CanonicalCol::PointInTime).max(),
            col(CanonicalCol::Price)
                .max()
                .alias(CanonicalCol::SessionHigh),
            col(CanonicalCol::Price)
                .min()
                .alias(CanonicalCol::SessionLow),
            col(CanonicalCol::Price)
                .agg_vwap_with_volume(col(CanonicalCol::Volume))
                .alias(CanonicalCol::SessionVwap),
            col(CanonicalCol::Volume)
                .sum()
                .alias(CanonicalCol::SessionVolume),
        ])
        .sort(
            [CanonicalCol::OpenTimestamp],
            SortMultipleOptions::default(),
        )
        .select([
            col(CanonicalCol::Date),
            col(CanonicalCol::OpenTimestamp),
            col(CanonicalCol::PointInTime),
            col(CanonicalCol::SessionHigh),
            col(CanonicalCol::SessionLow),
            col(CanonicalCol::SessionVolume),
            col(CanonicalCol::SessionVwap),
        ])
}

#[cfg(test)]
mod tests {
    #![expect(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::print_stdout,
        clippy::similar_names,
        reason = "tests assert against known-valid fixtures; unwrap and expect surface failures as panics that fail the test"
    )]
    use std::path::PathBuf;

    use polars::prelude::{
        DataType, LazyCsvReader, LazyFileListReader, PlRefPath, SchemaExt, TimeUnit,
    };

    use super::*;

    // ============================================================================
    // Test Fixtures & Helpers
    // ============================================================================

    /// Returns the absolute path to the test fixtures directory.
    fn fixtures_path() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/gym")
    }

    /// Loads an OHLCV CSV fixture and maps column names to the canonical
    /// schema.
    fn load_ohlcv_fixture(filename: &str) -> LazyFrame {
        let path = fixtures_path().join("input").join(filename);

        LazyCsvReader::new(PlRefPath::new(path.as_os_str().to_str().expect("filepath")))
            .with_has_header(true)
            .finish()
            .expect("Failed to parse fixture CSV")
            .select([
                col("trade_id").alias(CanonicalCol::TradeId.as_str()),
                col("price").alias(CanonicalCol::Price.as_str()),
                col("quantity").alias(CanonicalCol::Volume.as_str()),
                col("quote_asset_quantity").alias(CanonicalCol::QuoteAssetVolume.as_str()),
                col("trade_timestamp")
                    .cast(DataType::Datetime(
                        TimeUnit::Microseconds,
                        Some(polars::prelude::TimeZone::UTC),
                    ))
                    .alias(CanonicalCol::PointInTime.as_str()),
                col("is_buyer_maker").alias(CanonicalCol::IsBuyerMaker.as_str()),
                col("is_best_match").alias(CanonicalCol::IsBestMatch.as_str()),
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

    /// **REGRESSION TEST**
    ///
    /// Checks that the current indicator logic produces the same output as
    /// previously recorded snapshots.
    ///
    /// **NOTE:** The `expected/*.csv` files were generated by this library
    /// itself. They serve to catch accidental changes in logic
    /// (regressions), but they do NOT guarantee mathematical correctness
    /// against an external standard (like TA-Lib or `TradingView`).
    #[test]
    fn test_indicators_regression_consistency() {
        let test_cases = vec![
            IndicatorTestCase {
                name: "Trades-VWAP",
                indicator: BatchTradesIndicator::Vwap,
                expected_file: "trades_vwap_daily.csv",
            },
            IndicatorTestCase {
                name: "Overnight-Range-Core",
                indicator: BatchTradesIndicator::OvernightRange(SessionWindow::us_core_session()),
                expected_file: "trades_overnight_range_core_daily.csv",
            },
            IndicatorTestCase {
                name: "Overnight-Range-Night",
                indicator: BatchTradesIndicator::OvernightRange(SessionWindow::us_overnight()),
                expected_file: "trades_overnight_range_night_daily.csv",
            },
        ];

        for case in test_cases {
            println!("Running indicator test: {}", case.name);

            // 1. Load Input
            let input_lf = load_ohlcv_fixture("binance-btc-usdt-trades.csv");

            // 2. Compute (Simulating internal build step)
            let result_lf = case
                .indicator
                .pre_compute(input_lf)
                .unwrap_or_else(|_| panic!("Failed to compute {}", case.name));

            // 3. Assert
            let result_df = result_lf.collect().unwrap();
            let schema = case.indicator.output_schema();

            // Verify that the result matches the promised schema
            assert_eq!(
                result_df.schema().as_ref(),
                schema.as_ref(),
                "Result schema mismatch for test case: {}",
                case.name
            );

            let expected_file = fixtures_path().join("expected").join(case.expected_file);
            let mut expected_lf = LazyCsvReader::new(PlRefPath::new(
                expected_file.as_os_str().to_str().expect("filepath"),
            ))
            .with_has_header(true)
            .finish()
            .unwrap();

            let expected_schema = expected_lf.collect_schema().unwrap();

            // Dynamically align expected DataFrame with the required schema
            for field in schema.iter_fields() {
                let name = field.name();
                let dtype = field.dtype();

                if expected_schema.contains(name.as_str()) {
                    expected_lf = expected_lf.with_column(col(name.as_str()).cast(dtype.clone()));
                } else if name.as_str() == CanonicalCol::PointInTime.as_str()
                    && expected_schema.contains("timestamp")
                {
                    // Handle "timestamp" -> "point_in_time" legacy alias
                    expected_lf = expected_lf
                        .with_column(col("timestamp").cast(dtype.clone()).alias(name.as_str()));
                }
            }

            let expected_df = expected_lf
                .select(
                    result_df
                        .get_column_names()
                        .iter()
                        .map(|n| col(n.as_str()))
                        .collect::<Vec<_>>(),
                )
                .collect()
                .unwrap();

            assert_eq!(
                result_df, expected_df,
                "DataFrame mismatch for test case: {}",
                case.name
            );
        }
    }
}
