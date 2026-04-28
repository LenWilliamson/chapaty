use std::sync::Arc;

use polars::{
    prelude::{
        ClosedWindow, DataFrame, DataType, Duration, DynamicGroupOptions, Field, IntoLazy,
        PlSmallStr, Schema, SchemaRef, SortMultipleOptions, TimeUnit, TimeZone, all, col,
    },
    series::IsSorted,
};
use serde::{Deserialize, Serialize};
use strum::{Display, EnumIter, EnumString, IntoEnumIterator, IntoStaticStr};

use crate::{
    error::{ChapatyError, ChapatyResult, DataError},
    report::io::{Report, ReportName, ToSchema},
};

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    EnumString,
    Display,
    PartialOrd,
    Ord,
    EnumIter,
    IntoStaticStr,
)]
#[strum(serialize_all = "snake_case")]
pub enum EquityCurveCol {
    /// Row identifier for the journal entry (globally unique per row).
    RowId,
    /// Identifier for the episode this trade occurred in.
    EpisodeId,
    /// Timestamp of this cumulative return observation.
    Timestamp,
    /// Portfolio value at this timestamp (Mark to Market).
    PortfolioValue,
}

impl From<EquityCurveCol> for PlSmallStr {
    fn from(value: EquityCurveCol) -> Self {
        value.as_str().into()
    }
}

impl EquityCurveCol {
    pub fn name(&self) -> PlSmallStr {
        (*self).into()
    }

    pub fn as_str(&self) -> &'static str {
        self.into()
    }
}

#[derive(Debug, Clone)]
pub struct EquityCurveReport {
    df: DataFrame,
}

impl EquityCurveReport {
    /// Downsamples the equity curve to End-Of-Day (EOD) resolution.
    ///
    /// This reduces memory and file size by retaining only the final
    /// Mark-to-Market portfolio value for each calendar day across the entire simulation.
    ///
    /// # Time-Series Boundary Edge Cases
    ///
    /// OHLCV market data is defined as a left-inclusive, right-exclusive interval: `[open_ts, close_ts)`.
    ///
    /// To prevent the `T+1 00:00:00` flush into the next calendar day's bucket,
    /// we use Polars' `DynamicGroupOptions` with `ClosedWindow::Right`. This `(start, end]`
    /// inclusivity ensures the midnight tick is strictly evaluated as the terminal state of `T`
    /// without duplicating the row into `T+1`.
    pub fn into_eod(self) -> ChapatyResult<Self> {
        const BUCKET_ALIAS: &str = "_bucket_ts";
        let eod_df = self
            .df
            .lazy()
            .sort(
                [EquityCurveCol::EpisodeId, EquityCurveCol::Timestamp],
                SortMultipleOptions::default(),
            )
            .with_column(col(EquityCurveCol::Timestamp).alias(BUCKET_ALIAS))
            .group_by_dynamic(
                col(BUCKET_ALIAS),
                [],
                DynamicGroupOptions {
                    every: Duration::parse("1d"),
                    period: Duration::parse("1d"),
                    offset: Duration::parse("0s"),
                    // CRITICAL: Right closure (start, end] prevents overlapping buckets
                    // and eliminates duplicate rows for midnight boundary ticks.
                    closed_window: ClosedWindow::Right,
                    ..Default::default()
                },
            )
            .agg([
                col(EquityCurveCol::EpisodeId).last(),
                col(EquityCurveCol::Timestamp).last(),
                col(EquityCurveCol::PortfolioValue).last(),
            ])
            .select([all().exclude_cols([BUCKET_ALIAS]).as_expr()])
            .collect()
            .map_err(|e| ChapatyError::Data(DataError::DataFrame(e.to_string())))?
            .with_row_index(EquityCurveCol::RowId.into(), None)
            .map_err(|e| ChapatyError::Data(DataError::DataFrame(e.to_string())))?;

        Self::new(eod_df)
    }
}

impl ReportName for EquityCurveReport {
    fn base_name(&self) -> String {
        "equity_curve".into()
    }
}

impl Report for EquityCurveReport {
    fn as_df(&self) -> &DataFrame {
        &self.df
    }

    fn as_df_mut(&mut self) -> &mut DataFrame {
        &mut self.df
    }
}

impl EquityCurveReport {
    pub(crate) fn new(df: DataFrame) -> ChapatyResult<Self> {
        let sorted_df = df
            .sort([EquityCurveCol::Timestamp], SortMultipleOptions::default())
            .map_err(|e| ChapatyError::Data(DataError::DataFrame(e.to_string())))?;

        sorted_df
            .column(EquityCurveCol::Timestamp.as_str())
            .ok()
            .map(|s| s.is_sorted_flag() == IsSorted::Ascending)
            .ok_or_else(|| {
                ChapatyError::Data(DataError::DataFrame(
                    "Equity curve report must be sorted by timestamp".to_string(),
                ))
            })?;

        Ok(Self { df: sorted_df })
    }
}

impl Default for EquityCurveReport {
    fn default() -> Self {
        let df = DataFrame::empty_with_schema(&Self::to_schema());
        Self { df }
    }
}

impl ToSchema for EquityCurveReport {
    fn to_schema() -> SchemaRef {
        let fields = EquityCurveCol::iter()
            .map(|col| {
                let dtype = match col {
                    EquityCurveCol::RowId | EquityCurveCol::EpisodeId => DataType::UInt32,
                    EquityCurveCol::Timestamp => {
                        DataType::Datetime(TimeUnit::Microseconds, Some(TimeZone::UTC))
                    }
                    EquityCurveCol::PortfolioValue => DataType::Float64,
                };
                Field::new(col.into(), dtype)
            })
            .collect::<Vec<_>>();
        Arc::new(Schema::from_iter(fields))
    }
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use chrono::{DateTime, Utc};
    use polars::prelude::{LazyCsvReader, LazyFileListReader, PlPath, df};

    use super::*;

    /// Parse RFC3339 timestamp string to DateTime<Utc>.
    fn ts_micros(s: &str) -> i64 {
        DateTime::parse_from_rfc3339(s)
            .unwrap()
            .with_timezone(&Utc)
            .timestamp_micros()
    }

    /// Casts the mock integer timestamp column into the strict Datetime schema.
    fn format_mock_df(df: DataFrame) -> DataFrame {
        df.lazy()
            .with_column(col(EquityCurveCol::Timestamp).cast(DataType::Datetime(
                TimeUnit::Microseconds,
                Some(TimeZone::UTC),
            )))
            .collect()
            .unwrap()
    }

    #[test]
    fn test_journal_creation_and_schema_validation() {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let pb = PathBuf::from(manifest_dir).join("tests/fixtures/report/input/equity_curve.csv");
        let path = PlPath::new(
            pb.as_os_str()
                .to_str()
                .expect("Failed to convert input file path to string"),
        );

        let schema = EquityCurveReport::to_schema();
        let df = LazyCsvReader::new(path)
            .with_has_header(true)
            .with_schema(Some(schema.clone()))
            .with_try_parse_dates(true)
            .finish()
            .expect("Failed to create LazyFrame from CSV")
            .collect()
            .expect("Failed to collect DataFrame from LazyFrame");

        let equity_curve =
            EquityCurveReport::new(df).expect("Failed to create EquityCurveReport from DataFrame");
        let df = &equity_curve.as_df();

        let current_schema = df.schema();
        let expected_schema = EquityCurveReport::to_schema();

        for (name, expected_dtype) in expected_schema.iter() {
            let actual_dtype = current_schema.get(name);
            assert!(
                actual_dtype.is_some(),
                "Missing column in Journal DataFrame: {}",
                name
            );
            assert_eq!(
                actual_dtype.unwrap(),
                expected_dtype,
                "Type mismatch for column '{}'. Expected {:?}, got {:?}",
                name,
                expected_dtype,
                actual_dtype.unwrap()
            );
        }
    }

    #[test]
    fn test_equity_curve_into_eod_empty_dataframe() {
        // 1. Create a perfectly valid but empty report using the Default trait
        let report = EquityCurveReport::default();
        let initial_schema = report.as_df().schema().clone();

        // 2. Apply EOD downsampling
        let eod_report = report.into_eod().expect("Failed to downsample empty DF");
        let eod_df = eod_report.as_df();

        // 3. Verify Results
        // It should gracefully process the empty data and return 0 rows without panicking.
        assert_eq!(eod_df.height(), 0, "Empty input should yield empty output");

        // The schema should remain perfectly perfectly intact
        let final_schema = eod_df.schema().clone();
        assert_eq!(
            initial_schema, final_schema,
            "Schema mutated during empty EOD aggregation"
        );
    }

    #[test]
    fn test_equity_curve_into_eod() {
        let input_df = format_mock_df(
            df![
                EquityCurveCol::RowId => [0u32, 1, 2, 3, 4],
                EquityCurveCol::EpisodeId => [1u32, 1, 1, 1, 1],
                EquityCurveCol::Timestamp => [
                    ts_micros("2026-04-19T00:00:00Z"), // Day 0 Boundary
                    ts_micros("2026-04-19T12:00:00Z"),
                    ts_micros("2026-04-19T23:59:59Z"),
                    ts_micros("2026-04-20T00:00:00Z"), // Day 1 EOD (right exclusive timestamp)
                    ts_micros("2026-04-20T12:53:20Z"), // Day 2 EOD
                ],
                EquityCurveCol::PortfolioValue => [100.0, 150.0, 120.0, 90.0, 200.0]
            ]
            .unwrap(),
        );

        let expected_df = format_mock_df(
            df![
                EquityCurveCol::RowId => [0u32, 1, 2],
                EquityCurveCol::EpisodeId => [1u32, 1, 1],
                EquityCurveCol::Timestamp => [
                    // The Day 0 Boundary (Technically April 18 EOD)
                    ts_micros("2026-04-19T00:00:00Z"),
                    // Day 1 Terminal Flush
                    ts_micros("2026-04-20T00:00:00Z"),
                    // Day 2 EOD
                    ts_micros("2026-04-20T12:53:20Z"),
                ],
                EquityCurveCol::PortfolioValue => [100.0, 90.0, 200.0]
            ]
            .unwrap(),
        );

        let report = EquityCurveReport::new(input_df).expect("Failed to create report");
        let eod_df = report
            .into_eod()
            .expect("Failed to downsample to EOD")
            .as_df()
            .clone();

        assert_eq!(eod_df, expected_df, "Standard EOD aggregation failed");
    }

    #[test]
    fn test_equity_curve_into_eod_episode_boundary() {
        let input_df = format_mock_df(
            df![
                EquityCurveCol::RowId => [0u32, 1, 2, 3, 4, 5],
                EquityCurveCol::EpisodeId => [
                    1u32, // Ep 1
                    1,    // Ep 1
                    2,    // Ep 2 (New Episode starts mid-day!)
                    2,    // Ep 2
                    2,    // Ep 2
                    2     // Ep 2
                ],
                EquityCurveCol::Timestamp => [
                    ts_micros("2026-04-19T00:00:00Z"), // Ep 1 start (Day 0 Boundary)
                    ts_micros("2026-04-19T11:59:00Z"), // Ep 1 ends
                    ts_micros("2026-04-19T12:00:00Z"), // Ep 2 starts
                    ts_micros("2026-04-19T23:59:59Z"), // Ep 2
                    ts_micros("2026-04-20T00:00:00Z"), // Ep 2, Day 1 EOD (right exclusive timestamp)
                    ts_micros("2026-04-20T12:53:20Z"), // Day 2 EOD
                ],
                EquityCurveCol::PortfolioValue => [
                    100.0,
                    120.0,
                    120.0,
                    145.0,
                    150.0, // Terminal flush value for Day 1
                    200.0
                ]
            ]
            .unwrap(),
        );

        let expected_df = format_mock_df(
            df![
                EquityCurveCol::RowId => [0u32, 1, 2],
                EquityCurveCol::EpisodeId => [1u32, 2, 2],
                EquityCurveCol::Timestamp => [
                    ts_micros("2026-04-19T00:00:00Z"), // Day 0 Boundary
                    ts_micros("2026-04-20T00:00:00Z"), // Day 1 EOD
                    ts_micros("2026-04-20T12:53:20Z"), // Day 2 EOD
                ],
                EquityCurveCol::PortfolioValue => [100.0, 150.0, 200.0]
            ]
            .unwrap(),
        );

        let report = EquityCurveReport::new(input_df).expect("Failed to create report");
        let eod_df = report
            .into_eod()
            .expect("Failed to downsample to EOD")
            .as_df()
            .clone();

        assert_eq!(eod_df, expected_df, "Episode boundary stitching failed");
    }

    #[test]
    fn test_equity_curve_into_eod_microsecond_determinism() {
        // This test proves the `ClosedWindow::Both` DDIA interval logic.
        // It proves that exactly 00:00:00.000000 belongs to the previous day's terminal state,
        // but 00:00:00.000001 strictly belongs to the current day.
        let input_df = format_mock_df(
            df![
                EquityCurveCol::RowId => [0u32, 1, 2, 3],
                EquityCurveCol::EpisodeId => [1u32, 1, 1, 1],
                EquityCurveCol::Timestamp => [
                    ts_micros("2026-04-19T23:59:59.999999Z"), // 1µs BEFORE midnight
                    ts_micros("2026-04-20T00:00:00.000000Z"), // EXACTLY midnight (Day 1 Terminal Flush)
                    ts_micros("2026-04-20T00:00:00.000001Z"), // 1µs AFTER midnight (Day 2 first tick)
                    ts_micros("2026-04-20T12:00:00.000000Z"), // Day 2 EOD
                ],
                EquityCurveCol::PortfolioValue => [100.0, 150.0, 160.0, 200.0]
            ]
            .unwrap(),
        );

        let expected_df = format_mock_df(
            df![
                EquityCurveCol::RowId => [0u32, 1],
                EquityCurveCol::EpisodeId => [1u32, 1],
                EquityCurveCol::Timestamp => [
                    // Correctly picks the exact 00:00:00.000000 boundary for Day 1
                    ts_micros("2026-04-20T00:00:00.000000Z"),
                    // Correctly groups the +1µs tick into Day 2, evaluating to the 12:00 EOD
                    ts_micros("2026-04-20T12:00:00.000000Z"),
                ],
                EquityCurveCol::PortfolioValue => [150.0, 200.0]
            ]
            .unwrap(),
        );

        let report = EquityCurveReport::new(input_df).expect("Failed to create report");
        let eod_df = report
            .into_eod()
            .expect("Failed to downsample to EOD")
            .as_df()
            .clone();

        assert_eq!(
            eod_df, expected_df,
            "Microsecond determinism failed at boundary crossover"
        );
    }

    #[test]
    fn test_equity_curve_into_eod_sparse_data() {
        let input_df = format_mock_df(
            df![
                EquityCurveCol::RowId => [0u32, 1, 2, 3],
                EquityCurveCol::EpisodeId => [1u32, 1, 1, 1],
                EquityCurveCol::Timestamp => [
                    ts_micros("2026-04-24T10:00:00Z"), // Friday
                    ts_micros("2026-04-24T15:00:00Z"), // Friday EOD
                    // Missing Saturday (Apr 25)
                    // Missing Sunday (Apr 26)
                    ts_micros("2026-04-27T09:00:00Z"), // Monday
                    ts_micros("2026-04-27T16:00:00Z"), // Monday EOD
                ],
                EquityCurveCol::PortfolioValue => [100.0, 110.0, 110.0, 125.0]
            ]
            .unwrap(),
        );

        let expected_df = format_mock_df(
            df![
                EquityCurveCol::RowId => [0u32, 1],
                EquityCurveCol::EpisodeId => [1u32, 1],
                EquityCurveCol::Timestamp => [
                    ts_micros("2026-04-24T15:00:00Z"),
                    ts_micros("2026-04-27T16:00:00Z"),
                ],
                EquityCurveCol::PortfolioValue => [110.0, 125.0]
            ]
            .unwrap(),
        );

        let report = EquityCurveReport::new(input_df).expect("Failed to create report");
        let eod_df = report
            .into_eod()
            .expect("Failed to downsample to EOD")
            .as_df()
            .clone();

        assert_eq!(eod_df, expected_df, "Sparse data EOD aggregation failed");
    }
}
