use std::{convert::TryFrom, sync::Arc};

use polars::{
    frame::DataFrame,
    prelude::{
        DataType, Expr, Field, FillNullStrategy, IntoLazy, Null, PlSmallStr, Schema, SchemaRef,
        SortMultipleOptions, TimeUnit, TimeZone, UnionArgs, col, lit, when,
    },
};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
use strum::{Display, EnumCount, EnumIter, EnumString, IntoEnumIterator, IntoStaticStr};

use crate::{
    error::{ChapatyError, ChapatyResult, DataError},
    report::{
        grouped::GroupedJournal,
        io::{Report, ReportName, ToSchema, generate_dynamic_base_name},
        journal::{Journal, JournalCol},
        polars_ext::{ExprExt, polars_to_chapaty_error},
    },
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CumulativeReturns {
    df: DataFrame,
}

impl Default for CumulativeReturns {
    fn default() -> Self {
        let df = DataFrame::empty_with_schema(&Self::to_schema());
        Self { df }
    }
}

impl ReportName for CumulativeReturns {
    fn base_name(&self) -> String {
        generate_dynamic_base_name(&self.df, "cumulative_returns")
    }
}

impl Report for CumulativeReturns {
    fn as_df(&self) -> &DataFrame {
        &self.df
    }

    fn as_df_mut(&mut self) -> &mut DataFrame {
        &mut self.df
    }
}

impl ToSchema for CumulativeReturns {
    fn to_schema() -> SchemaRef {
        let fields: Vec<Field> = CumulativeReturnCol::iter()
            .map(|col| {
                let dtype = match col {
                    CumulativeReturnCol::RowId
                    | CumulativeReturnCol::EpisodeId
                    | CumulativeReturnCol::TradeId => DataType::UInt32,

                    CumulativeReturnCol::AgentId
                    | CumulativeReturnCol::DataBroker
                    | CumulativeReturnCol::Symbol
                    | CumulativeReturnCol::Exchange
                    | CumulativeReturnCol::MarketType
                    | CumulativeReturnCol::TradeType
                    | CumulativeReturnCol::ExitReason => DataType::String,

                    CumulativeReturnCol::CumulativeTimestamp
                    | CumulativeReturnCol::LastPeakTimestamp => {
                        DataType::Datetime(TimeUnit::Microseconds, Some(TimeZone::UTC))
                    }

                    CumulativeReturnCol::Quantity
                    | CumulativeReturnCol::PeakCumulativeReturnUsd
                    | CumulativeReturnCol::DrawdownFromPeakUsd
                    | CumulativeReturnCol::DrawdownFromPeakPercentage
                    | CumulativeReturnCol::RollingRecoveryFactor
                    | CumulativeReturnCol::CumulativeRealizedReturnUsd => DataType::Float64,
                };
                Field::new(col.into(), dtype)
            })
            .collect();

        Arc::new(Schema::from_iter(fields))
    }
}

impl TryFrom<&Journal> for CumulativeReturns {
    type Error = ChapatyError;

    fn try_from(j: &Journal) -> ChapatyResult<Self> {
        if j.as_df().is_empty() {
            return Ok(Self::default());
        }

        let init_val = j.risk_metrics_config().initial_portfolio_value();
        let df = j
            .as_df()
            .clone()
            .lazy()
            .select(exprs(init_val))
            .collect()
            .map_err(convert_err)?;

        Ok(Self { df })
    }
}

impl TryFrom<&GroupedJournal<'_>> for CumulativeReturns {
    type Error = ChapatyError;

    fn try_from(gj: &GroupedJournal) -> ChapatyResult<Self> {
        if gj.source().as_df().is_empty() {
            return Ok(Self::default());
        }

        let init_val = gj.source().risk_metrics_config().initial_portfolio_value();
        let (partitions, keys) = gj.to_partitions()?;

        let lazy_computations = partitions
            .into_par_iter()
            .map(|df| {
                let mut selection = Vec::with_capacity(keys.len() + CumulativeReturnCol::COUNT);
                for k in &keys {
                    selection.push(col(k));
                }
                selection.extend(exprs(init_val));

                let lf = df
                    .lazy()
                    .sort(
                        [JournalCol::EntryTimestamp.as_str()],
                        SortMultipleOptions::default(),
                    )
                    .select(selection);
                Ok(lf)
            })
            .collect::<Result<Vec<_>, ChapatyError>>();

        let merged = polars::prelude::concat(
            lazy_computations?,
            UnionArgs {
                parallel: true,
                rechunk: true,
                ..Default::default()
            },
        )
        .map_err(|e| DataError::DataFrame(format!("Merge plan failed: {e}")))?
        .collect()
        .map_err(|e| DataError::DataFrame(format!("Execution failed: {e}")))?;

        Ok(Self { df: merged })
    }
}

fn exprs(init_val: u32) -> Vec<Expr> {
    vec![
        // === Identifiers ===
        col(JournalCol::RowId)
            .alias(CumulativeReturnCol::RowId)
            .cast(DataType::UInt32),
        col(JournalCol::EpisodeId)
            .alias(CumulativeReturnCol::EpisodeId)
            .cast(DataType::UInt32),
        col(JournalCol::TradeId)
            .alias(CumulativeReturnCol::TradeId)
            .cast(DataType::UInt32),
        col(JournalCol::AgentId)
            .alias(CumulativeReturnCol::AgentId)
            .cast(DataType::String),
        // === Market spec ===
        col(JournalCol::DataBroker)
            .alias(CumulativeReturnCol::DataBroker)
            .cast(DataType::String),
        col(JournalCol::Exchange)
            .alias(CumulativeReturnCol::Exchange)
            .cast(DataType::String),
        col(JournalCol::Symbol)
            .alias(CumulativeReturnCol::Symbol)
            .cast(DataType::String),
        col(JournalCol::MarketType)
            .alias(CumulativeReturnCol::MarketType)
            .cast(DataType::String),
        // === Trade configuration ===
        col(JournalCol::TradeType)
            .alias(CumulativeReturnCol::TradeType)
            .cast(DataType::String),
        col(JournalCol::Quantity)
            .alias(CumulativeReturnCol::Quantity)
            .cast(DataType::Float64),
        // === Time ===
        col(JournalCol::ExitTimestamp)
            .alias(CumulativeReturnCol::CumulativeTimestamp)
            .cast(DataType::Datetime(
                TimeUnit::Microseconds,
                Some(TimeZone::UTC),
            )),
        last_peak_timestamp_expr(init_val)
            .alias(CumulativeReturnCol::LastPeakTimestamp)
            .cast(DataType::Datetime(
                TimeUnit::Microseconds,
                Some(TimeZone::UTC),
            )),
        // === Equity curve metrics ===
        peak_cumulative_return_usd_expr(init_val)
            .alias(CumulativeReturnCol::PeakCumulativeReturnUsd)
            .cast(DataType::Float64),
        drawdown_from_peak_usd_expr(init_val)
            .alias(CumulativeReturnCol::DrawdownFromPeakUsd)
            .cast(DataType::Float64),
        drawdown_from_peak_pct_expr(init_val)
            .alias(CumulativeReturnCol::DrawdownFromPeakPercentage)
            .cast(DataType::Float64),
        // === Performance ratio ===
        rolling_recovery_factor_expr(init_val)
            .alias(CumulativeReturnCol::RollingRecoveryFactor)
            .cast(DataType::Float64),
        // === Return outcomes ===
        col(JournalCol::ExitReason)
            .alias(CumulativeReturnCol::ExitReason)
            .cast(DataType::String),
        cumulative_realized_return_usd_expr(init_val)
            .alias(CumulativeReturnCol::CumulativeRealizedReturnUsd)
            .cast(DataType::Float64),
    ]
}

// ================================================================================================
// === Time ===
// ================================================================================================
fn last_peak_timestamp_expr(initial_value: u32) -> Expr {
    let exit_ts = col(JournalCol::ExitTimestamp);
    let cum_ret = cumulative_realized_return_usd_expr(initial_value);
    let peak_ret = peak_cumulative_return_usd_expr(initial_value);

    // Mark the timestamp at each new peak, else null
    let peak_ts = when(cum_ret.clone().eq(peak_ret.clone()))
        .then(exit_ts)
        .otherwise(polars::prelude::lit(Null {}));

    // Forward fill to propagate the last peak timestamp
    peak_ts.fill_null_with_strategy(FillNullStrategy::Forward(None))
}

// ================================================================================================
// === Equity curve metrics ===
// ================================================================================================
pub(super) fn peak_cumulative_return_usd_expr(initial_value: u32) -> Expr {
    cumulative_realized_return_usd_expr(initial_value).cum_max(false)
}

fn drawdown_from_peak_usd_expr(initial_value: u32) -> Expr {
    peak_cumulative_return_usd_expr(initial_value)
        - cumulative_realized_return_usd_expr(initial_value)
}

fn drawdown_from_peak_pct_expr(initial_value: u32) -> Expr {
    let draw_down = drawdown_from_peak_usd_expr(initial_value);
    let peak = peak_cumulative_return_usd_expr(initial_value);
    draw_down.safe_div(peak, Some(0.0))
}

fn rolling_recovery_factor_expr(initial_value: u32) -> Expr {
    let return_col = cumulative_realized_return_usd_expr(initial_value);
    let drawdown_col = drawdown_from_peak_usd_expr(initial_value);
    return_col.safe_div(drawdown_col, None)
}

// ================================================================================================
// === Return outcomes ===
// ================================================================================================
pub(super) fn cumulative_realized_return_usd_expr(initial_value: u32) -> Expr {
    lit(initial_value).cast(DataType::Float64)
        + col(JournalCol::RealizedReturnDollars).cum_sum(false)
}

// ================================================================================================
// Helper Functions
// ================================================================================================
fn convert_err(e: polars::error::PolarsError) -> ChapatyError {
    polars_to_chapaty_error("cumulative return report", e)
}

/// Represents a point in the cumulative return trajectory of a trading strategy.
///
/// Captures the evolution of the strategy’s performance over time, including drawdown
/// and return metrics. This structure is equivalent to an equity curve in traditional finance,
/// or cumulative return in reinforcement learning.
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
    EnumCount,
)]
#[strum(serialize_all = "snake_case")]
pub enum CumulativeReturnCol {
    // === Identifiers ===
    /// Row identifier for the cumulative return entry (globally unique per row).
    RowId,
    /// Identifier for the episode this trade occurred in.
    EpisodeId,
    /// Identifier for the trade that triggered this return.
    TradeId,
    /// Identifier for the agent executing the trade.
    AgentId,

    // === Market spec ===
    /// The market data broker (e.g., `binance`).
    DataBroker,
    /// The exchange of the data broker (e.g., `cme` from data broker `ninjatrader`).
    Exchange,
    /// The trading symbol (e.g., `btc-usdt`).
    Symbol,
    /// The type of instrument (e.g., `spot`, `futures`).
    MarketType,

    // === Trade configuration ===
    /// The type of trade (e.g., `long`, `short`).
    TradeType,
    /// Quantity of the asset involved.
    Quantity,

    // === Time ===
    /// Timestamp of this cumulative return observation.
    CumulativeTimestamp,
    /// Timestamp of the most recent equity peak before this point.
    LastPeakTimestamp,

    // === Equity curve metrics ===
    /// Highest cumulative return observed so far.
    PeakCumulativeReturnUsd,
    /// Drawdown from peak in absolute dollar terms.
    DrawdownFromPeakUsd,
    /// Drawdown from peak as a percentage of the peak.
    DrawdownFromPeakPercentage,

    // === Performance ratio ===
    /// Rolling ratio of total return to maximum drawdown — a measure of recovery strength.
    RollingRecoveryFactor,

    // === Return outcomes ===
    /// The reason the trade was exited (e.g., `take_profit`, `stop_loss`).
    ExitReason,
    /// Cumulative realized return in USD.
    CumulativeRealizedReturnUsd,
}

impl From<CumulativeReturnCol> for PlSmallStr {
    fn from(value: CumulativeReturnCol) -> Self {
        value.as_str().into()
    }
}

impl CumulativeReturnCol {
    pub fn name(&self) -> PlSmallStr {
        (*self).into()
    }

    pub fn as_str(&self) -> &'static str {
        self.into()
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, path::PathBuf};

    use crate::data::common::RiskMetricsConfig;

    use super::*;
    use polars::prelude::{LazyCsvReader, LazyFileListReader, PlPath, SchemaExt};
    use strum::IntoEnumIterator;

    // ========================================================================
    // Helper: Load Journal Fixture
    // ========================================================================

    fn load_journal_fixture() -> Journal {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let fixture_path =
            PathBuf::from(manifest_dir).join("tests/fixtures/report/input/journal.csv");

        assert!(
            fixture_path.exists(),
            "Test fixture missing: {}",
            fixture_path.display()
        );

        let schema = Journal::to_schema();
        let df = LazyCsvReader::new(PlPath::new(
            fixture_path
                .to_str()
                .expect("Invalid UTF-8 in fixture path"),
        ))
        .with_has_header(true)
        .with_schema(Some(schema))
        .with_try_parse_dates(true)
        .finish()
        .expect("Failed to create LazyFrame")
        .collect()
        .expect("Failed to collect DataFrame");

        Journal::new(df, RiskMetricsConfig::default()).expect("Failed to create Journal")
    }

    // ========================================================================
    // Test: Journal to CumulativeReturns Conversion
    // ========================================================================

    #[test]
    fn test_journal_to_cumulative_returns() {
        let journal = load_journal_fixture();
        let result = CumulativeReturns::try_from(&journal);

        assert!(
            result.is_ok(),
            "Failed to convert Journal to CumulativeReturns: {:?}",
            result.err()
        );

        let cum_ret = result.unwrap();
        let df = cum_ret.as_df();

        // Should preserve all 6 rows from fixture
        assert_eq!(
            df.height(),
            6,
            "CumulativeReturns should have 6 rows (one per trade)"
        );
    }

    // ========================================================================
    // Test: All Expected Columns Present
    // ========================================================================

    #[test]
    fn test_all_cumulative_return_fields_present() {
        let journal = load_journal_fixture();
        let cum_ret = CumulativeReturns::try_from(&journal).expect("Conversion failed");
        let df = cum_ret.as_df();

        let expected_columns: Vec<_> = CumulativeReturnCol::iter().collect();

        for col in &expected_columns {
            assert!(
                df.column(col.as_str()).is_ok(),
                "Missing expected column: {}",
                col
            );
        }

        assert_eq!(
            df.schema().len(),
            expected_columns.len(),
            "Column count mismatch. Expected {}, found {}. Details: {:?}",
            expected_columns.len(),
            df.schema().len(),
            {
                let actual = df
                    .get_column_names()
                    .iter()
                    .map(|s| s.to_string())
                    .collect::<HashSet<_>>();
                let expected = expected_columns
                    .iter()
                    .map(|c| c.to_string())
                    .collect::<HashSet<_>>();
                let missing: Vec<_> = expected.difference(&actual).cloned().collect();
                let extra: Vec<_> = actual.difference(&expected).cloned().collect();
                (missing, extra)
            }
        );
    }

    // ========================================================================
    // Test: Data Types Match Schema
    // ========================================================================

    #[test]
    fn test_cumulative_return_data_types() {
        let journal = load_journal_fixture();
        let cum_ret = CumulativeReturns::try_from(&journal).expect("Conversion failed");
        let df = cum_ret.as_df();
        let expected_schema = CumulativeReturns::to_schema();

        for field in expected_schema.iter_fields() {
            let col_name = field.name();
            let expected_dtype = field.dtype();
            let actual_dtype = df
                .column(col_name)
                .unwrap_or_else(|_| panic!("Column '{}' not found", col_name))
                .dtype();

            assert_eq!(
                actual_dtype, expected_dtype,
                "Data type mismatch for '{}': expected {:?}, found {:?}",
                col_name, expected_dtype, actual_dtype
            );
        }
    }

    // ========================================================================
    // Test: Cumulative Return Calculation Logic
    // ========================================================================

    #[test]
    fn test_cumulative_return_calculation() {
        let journal = load_journal_fixture();
        let cum_ret = CumulativeReturns::try_from(&journal).expect("Conversion failed");
        let df = cum_ret.as_df();

        let cum_returns = df
            .column(CumulativeReturnCol::CumulativeRealizedReturnUsd.as_str())
            .expect("Missing cumulative return column")
            .f64()
            .expect("Column is not f64");

        // Fixture returns: -1000, -500, 0, +2000, +500, +1000
        // Initial value: 10000 (default)
        // Cumulative: 9000, 8500, 8500, 10500, 11000, 12000
        let expected = [9000.0, 8500.0, 8500.0, 10500.0, 11000.0, 12000.0];

        for (i, expected_val) in expected.iter().enumerate() {
            let actual = cum_returns.get(i).expect("Missing value at index");
            assert_eq!(
                actual, *expected_val,
                "Cumulative return mismatch at row {}: expected {}, found {}",
                i, expected_val, actual
            );
        }
    }

    // ========================================================================
    // Test: Peak Cumulative Return
    // ========================================================================

    #[test]
    fn test_peak_cumulative_return() {
        let journal = load_journal_fixture();
        let cum_ret = CumulativeReturns::try_from(&journal).expect("Conversion failed");
        let df = cum_ret.as_df();

        let peaks = df
            .column(CumulativeReturnCol::PeakCumulativeReturnUsd.as_str())
            .expect("Missing peak column")
            .f64()
            .expect("Column is not f64");

        // Peaks should be monotonically non-decreasing
        // Expected: 9000, 9000, 9000, 10500, 11000, 12000
        let expected = [9000.0, 9000.0, 9000.0, 10500.0, 11000.0, 12000.0];

        for (i, expected_val) in expected.iter().enumerate() {
            let actual = peaks.get(i).expect("Missing value");
            assert_eq!(
                actual, *expected_val,
                "Peak return mismatch at row {}: expected {}, found {}",
                i, expected_val, actual
            );
        }
    }

    // ========================================================================
    // Test: Drawdown Calculation
    // ========================================================================

    #[test]
    fn test_drawdown_from_peak() {
        let journal = load_journal_fixture();
        let cum_ret = CumulativeReturns::try_from(&journal).expect("Conversion failed");
        let df = cum_ret.as_df();

        let drawdowns = df
            .column(CumulativeReturnCol::DrawdownFromPeakUsd.as_str())
            .expect("Missing drawdown column")
            .f64()
            .expect("Column is not f64");

        // Drawdown = Peak - Current
        // Expected: 0, 500, 500, 0, 0, 0
        let expected = [0.0, 500.0, 500.0, 0.0, 0.0, 0.0];

        for (i, expected_val) in expected.iter().enumerate() {
            let actual = drawdowns.get(i).expect("Missing value");
            assert_eq!(
                actual, *expected_val,
                "Drawdown mismatch at row {}: expected {}, found {}",
                i, expected_val, actual
            );
        }
    }

    // ========================================================================
    // Test: Empty Journal
    // ========================================================================

    #[test]
    fn test_empty_journal() {
        let empty_df = DataFrame::empty_with_schema(&Journal::to_schema());
        let journal = Journal::new(empty_df, RiskMetricsConfig::default())
            .expect("Failed to create empty Journal");

        let result = CumulativeReturns::try_from(&journal);
        assert!(result.is_ok(), "Should handle empty Journal");

        let cum_ret = result.unwrap();
        let df = cum_ret.as_df();
        assert_eq!(df.height(), 0, "Empty journal should produce 0 rows");
    }

    // ========================================================================
    // Test: Last Peak Timestamp Logic
    // ========================================================================

    #[test]
    fn test_last_peak_timestamp() {
        let journal = load_journal_fixture();
        let cum_ret = CumulativeReturns::try_from(&journal).expect("Conversion failed");
        let df = cum_ret.as_df();

        let peak_timestamps = df
            .column(CumulativeReturnCol::LastPeakTimestamp.as_str())
            .expect("Missing peak timestamp column")
            .datetime()
            .expect("Column is not datetime");

        // Should have no nulls (forward fill ensures this)
        assert_eq!(
            peak_timestamps.null_count(),
            0,
            "Peak timestamp should not contain nulls"
        );

        // First peak should be at row 0 (2025-01-04)
        // Stays same for rows 1-2 (drawdown period)
        // New peak at row 3 (2026-01-10)
        // New peak at row 4 (2026-02-20)
        // New peak at row 5 (2026-05-02)
        let exit_timestamps = df
            .column(CumulativeReturnCol::CumulativeTimestamp.as_str())
            .expect("Missing exit timestamp")
            .datetime()
            .expect("Not datetime");

        // Row 0 is first peak
        let first_peak = exit_timestamps
            .physical()
            .get(0)
            .expect("Missing timestamp");
        // Rows 0, 1, 2 should all reference first peak
        for i in 0..3 {
            let peak_ts = peak_timestamps
                .physical()
                .get(i)
                .expect("Missing peak timestamp");
            assert_eq!(
                peak_ts, first_peak,
                "Rows 0-2 should reference first peak timestamp"
            );
        }
    }

    // ========================================================================
    // Test: Recovery Factor Calculation
    // ========================================================================

    #[test]
    fn test_rolling_recovery_factor() {
        let journal = load_journal_fixture();
        let cum_ret = CumulativeReturns::try_from(&journal).expect("Conversion failed");
        let df = cum_ret.as_df();

        let recovery_factors = df
            .column(CumulativeReturnCol::RollingRecoveryFactor.as_str())
            .expect("Missing recovery factor column")
            .f64()
            .expect("Column is not f64");

        // Recovery factor = cumulative_return / drawdown
        // Row 0: 9000 / 0 = inf
        // Row 1: 8500 / 500 = 17.0
        // Row 2: 8500 / 500 = 17.0
        // Row 3+: All at new peaks, so inf

        let val_0 = recovery_factors.get(0).expect("Missing value");
        assert!(val_0.is_infinite(), "Row 0 should be inf (no drawdown)");

        let val_1 = recovery_factors.get(1).expect("Missing value");
        assert_eq!(val_1, 17.0, "Row 1 recovery factor mismatch");

        let val_3 = recovery_factors.get(3).expect("Missing value");
        assert!(val_3.is_infinite(), "Row 3 should be inf (at peak)");
    }
}
