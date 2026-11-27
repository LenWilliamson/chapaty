use std::{convert::TryFrom, sync::Arc};

use polars::{
    frame::DataFrame,
    prelude::{
        DataType, ExplodeOptions, Expr, Field, FillNullStrategy, IntoLazy, Null, PlSmallStr,
        Schema, SchemaRef, TimeUnit, TimeZone, col, cols, lit, when,
    },
};
use serde::{Deserialize, Serialize};
use strum::{Display, EnumCount, EnumIter, EnumString, IntoEnumIterator, IntoStaticStr};

use crate::{
    error::{ChapatyError, ChapatyResult},
    report::{
        grouped::GroupedJournal,
        io::{Report, ReportName, ToSchema, generate_dynamic_base_name},
        journal::{ExprDefineExt, Journal, JournalCol, JournalExprExt},
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
        let fields = CumulativeReturnCol::iter()
            .map(|col| {
                let dtype = match col {
                    CumulativeReturnCol::RowId
                    | CumulativeReturnCol::EpisodeId
                    | CumulativeReturnCol::TradeId => DataType::UInt32,

                    CumulativeReturnCol::AgentUid => DataType::UInt64,

                    CumulativeReturnCol::AgentId
                    | CumulativeReturnCol::DataBroker
                    | CumulativeReturnCol::Symbol
                    | CumulativeReturnCol::Exchange
                    | CumulativeReturnCol::MarketKind
                    | CumulativeReturnCol::TradeKind
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
            .collect::<Vec<_>>();

        Arc::new(Schema::from_iter(fields))
    }
}

impl TryFrom<&Journal> for CumulativeReturns {
    type Error = ChapatyError;

    fn try_from(j: &Journal) -> ChapatyResult<Self> {
        if j.as_df().height() == 0 {
            return Ok(Self::default());
        }

        let init_val = j.risk_metrics_config().initial_portfolio_value();
        let df = j
            .as_df()
            .clone()
            .lazy()
            .filter(col(JournalCol::TradeState).trade_executed())
            .select(exprs(init_val))
            .collect()
            .map_err(convert_err)?;

        Ok(Self { df })
    }
}

impl TryFrom<&GroupedJournal<'_>> for CumulativeReturns {
    type Error = ChapatyError;

    fn try_from(gj: &GroupedJournal) -> ChapatyResult<Self> {
        let init_val = gj.source().risk_metrics_config().initial_portfolio_value();
        let report_cols = CumulativeReturnCol::iter().map(CumulativeReturnCol::as_str);

        // One query for all groups. The aggregation keeps one list of rows per
        // group, and explode turns those lists back into one row per trade.
        let df = gj
            .source()
            .as_df()
            .clone()
            .lazy()
            .filter(col(JournalCol::TradeState).trade_executed())
            .group_by_stable(gj.key_exprs())
            .agg(exprs(init_val))
            .explode(
                cols(report_cols),
                ExplodeOptions {
                    empty_as_null: true,
                    keep_nulls: true,
                },
            )
            .collect()
            .map_err(convert_err)?;

        Ok(Self { df })
    }
}

fn exprs(init_val: u32) -> Vec<Expr> {
    vec![
        // === Identifiers ===
        col(JournalCol::RowId).define_as(CumulativeReturnCol::RowId, DataType::UInt32),
        col(JournalCol::EpisodeId).define_as(CumulativeReturnCol::EpisodeId, DataType::UInt32),
        col(JournalCol::TradeId).define_as(CumulativeReturnCol::TradeId, DataType::UInt32),
        col(JournalCol::AgentId).define_as(CumulativeReturnCol::AgentId, DataType::String),
        col(JournalCol::AgentUid).define_as(CumulativeReturnCol::AgentUid, DataType::UInt64),
        // === Market spec ===
        col(JournalCol::DataBroker).define_as(CumulativeReturnCol::DataBroker, DataType::String),
        col(JournalCol::Exchange).define_as(CumulativeReturnCol::Exchange, DataType::String),
        col(JournalCol::Symbol).define_as(CumulativeReturnCol::Symbol, DataType::String),
        col(JournalCol::MarketKind).define_as(CumulativeReturnCol::MarketKind, DataType::String),
        // === Trade configuration ===
        col(JournalCol::TradeKind).define_as(CumulativeReturnCol::TradeKind, DataType::String),
        col(JournalCol::Quantity).define_as(CumulativeReturnCol::Quantity, DataType::Float64),
        // === Time ===
        col(JournalCol::ExitTimestamp).define_as(
            CumulativeReturnCol::CumulativeTimestamp,
            DataType::Datetime(TimeUnit::Microseconds, Some(TimeZone::UTC)),
        ),
        last_peak_timestamp_expr(init_val).define_as(
            CumulativeReturnCol::LastPeakTimestamp,
            DataType::Datetime(TimeUnit::Microseconds, Some(TimeZone::UTC)),
        ),
        // === Equity curve metrics ===
        peak_cumulative_return_usd_expr(init_val).define_as(
            CumulativeReturnCol::PeakCumulativeReturnUsd,
            DataType::Float64,
        ),
        drawdown_from_peak_usd_expr(init_val)
            .define_as(CumulativeReturnCol::DrawdownFromPeakUsd, DataType::Float64),
        drawdown_from_peak_pct_expr(init_val).define_as(
            CumulativeReturnCol::DrawdownFromPeakPercentage,
            DataType::Float64,
        ),
        // === Performance ratio ===
        rolling_recovery_factor_expr(init_val).define_as(
            CumulativeReturnCol::RollingRecoveryFactor,
            DataType::Float64,
        ),
        // === Return outcomes ===
        col(JournalCol::ExitReason).define_as(CumulativeReturnCol::ExitReason, DataType::String),
        cumulative_realized_return_usd_expr(init_val).define_as(
            CumulativeReturnCol::CumulativeRealizedReturnUsd,
            DataType::Float64,
        ),
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
    let peak_ts = when(cum_ret.eq(peak_ret))
        .then(exit_ts)
        .otherwise(polars::prelude::lit(Null {}));

    // Forward fill to propagate the last peak timestamp. Until the first new
    // high, the high is the initial capital, held since the first entry.
    peak_ts
        .fill_null_with_strategy(FillNullStrategy::Forward(None))
        .fill_null(col(JournalCol::EntryTimestamp).first())
}

// ================================================================================================
// === Equity curve metrics ===
// ================================================================================================

/// The high water mark: the highest portfolio value reached so far.
///
/// The initial capital is the first high. Without it, a series that starts
/// with a loss would treat the value after that loss as its high and report no
/// drawdown for it.
pub(super) fn peak_cumulative_return_usd_expr(initial_value: u32) -> Expr {
    let initial = lit(initial_value).cast(DataType::Float64);
    let highest = cumulative_realized_return_usd_expr(initial_value).cum_max(false);
    when(highest.clone().gt(initial.clone()))
        .then(highest)
        .otherwise(initial)
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

/// Represents a point in the cumulative return trajectory of a trading
/// strategy.
///
/// Captures the evolution of the strategy’s performance over time, including
/// drawdown and return metrics. This structure is equivalent to an equity curve
/// in traditional finance, or cumulative return in reinforcement learning.
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
    /// Row identifier for the cumulative return entry (globally unique per
    /// row).
    RowId,
    /// Identifier for the episode this trade occurred in.
    EpisodeId,
    /// Identifier for the trade that triggered this return.
    TradeId,
    /// Identifier for the agent executing the trade.
    AgentId,
    /// Unique id of the agent within a grid search. `0` for a single run.
    AgentUid,

    // === Market spec ===
    /// The market data broker (e.g., `binance`).
    DataBroker,
    /// The exchange of the data broker (e.g., `cme` from data broker
    /// `ninjatrader`).
    Exchange,
    /// The trading symbol (e.g., `btc-usdt`).
    Symbol,
    /// The kind of instrument (e.g., `spot`, `futures`).
    MarketKind,

    // === Trade configuration ===
    /// The kind of trade (e.g., `long`, `short`).
    TradeKind,
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
    /// Rolling ratio of total return to maximum drawdown — a measure of
    /// recovery strength.
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
    #[must_use]
    pub fn name(self) -> PlSmallStr {
        self.into()
    }

    #[must_use]
    pub fn as_str(self) -> &'static str {
        self.into()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use polars::prelude::SchemaExt;
    use strum::IntoEnumIterator;

    use super::*;
    use crate::{data::common::RiskMetricsConfig, report::test_support::load_journal_fixture};

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

        let expected_columns = CumulativeReturnCol::iter().collect::<Vec<_>>();

        for col in &expected_columns {
            assert!(
                df.column(col.as_str()).is_ok(),
                "Missing expected column: {col}"
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
                    .map(std::string::ToString::to_string)
                    .collect::<HashSet<_>>();
                let expected = expected_columns
                    .iter()
                    .map(std::string::ToString::to_string)
                    .collect::<HashSet<_>>();
                let missing = expected.difference(&actual).cloned().collect::<Vec<_>>();
                let extra = actual.difference(&expected).cloned().collect::<Vec<_>>();
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
                .unwrap_or_else(|_| panic!("Column '{col_name}' not found"))
                .dtype();

            assert_eq!(
                actual_dtype, expected_dtype,
                "Data type mismatch for '{col_name}': expected {expected_dtype:?}, found {actual_dtype:?}"
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
            assert_f64_eq!(
                actual,
                *expected_val,
                "Cumulative return mismatch at row {i}: expected {expected_val}, found {actual}"
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

        // Peaks should be monotonically non-decreasing and never below the
        // initial capital of 10000.
        let expected = [10000.0, 10000.0, 10000.0, 10500.0, 11000.0, 12000.0];

        for (i, expected_val) in expected.iter().enumerate() {
            let actual = peaks.get(i).expect("Missing value");
            assert_f64_eq!(
                actual,
                *expected_val,
                "Peak return mismatch at row {i}: expected {expected_val}, found {actual}"
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
        // The first trade loses 1000 against the initial capital.
        let expected = [1000.0, 1500.0, 1500.0, 0.0, 0.0, 0.0];

        for (i, expected_val) in expected.iter().enumerate() {
            let actual = drawdowns.get(i).expect("Missing value");
            assert_f64_eq!(
                actual,
                *expected_val,
                "Drawdown mismatch at row {i}: expected {expected_val}, found {actual}"
            );
        }
    }

    // ========================================================================
    // Test: Empty Journal
    // ========================================================================

    #[test]
    fn test_empty_journal() {
        let empty_df = DataFrame::empty_with_schema(&Journal::to_schema());
        let journal = Journal::new(&empty_df, RiskMetricsConfig::default())
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

        // Rows 0-2 stay below the initial capital, whose time is the first
        // entry (2025-01-04 12:00)
        // New peak at row 3 (2026-01-10)
        // New peak at row 4 (2026-02-20)
        // New peak at row 5 (2026-05-02)
        let exit_timestamps = df
            .column(CumulativeReturnCol::CumulativeTimestamp.as_str())
            .expect("Missing exit timestamp")
            .datetime()
            .expect("Not datetime");

        let first_entry = journal
            .as_df()
            .column(JournalCol::EntryTimestamp.as_str())
            .expect("Missing entry timestamp")
            .datetime()
            .expect("Not datetime")
            .physical()
            .get(0)
            .expect("Missing timestamp");
        for i in 0..3 {
            let peak_ts = peak_timestamps
                .physical()
                .get(i)
                .expect("Missing peak timestamp");
            assert_eq!(
                peak_ts, first_entry,
                "Rows 0-2 should reference the first entry"
            );
        }
        // Row 3 is a new high, at its own exit.
        assert_eq!(
            peak_timestamps.physical().get(3),
            exit_timestamps.physical().get(3),
        );
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
        // Row 0: 9000 / 1000 = 9.0
        // Row 1: 8500 / 1500
        // Row 2: 8500 / 1500
        // Row 3+: All at new peaks, so inf

        let val_0 = recovery_factors.get(0).expect("Missing value");
        assert_f64_eq!(val_0, 9.0, "Row 0 recovery factor mismatch");

        let val_1 = recovery_factors.get(1).expect("Missing value");
        assert_f64_eq!(val_1, 8500.0 / 1500.0, "Row 1 recovery factor mismatch");

        let val_3 = recovery_factors.get(3).expect("Missing value");
        assert!(val_3.is_infinite(), "Row 3 should be inf (at peak)");
    }
}
