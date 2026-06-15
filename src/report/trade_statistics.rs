use std::sync::Arc;

use polars::{
    frame::DataFrame,
    prelude::{
        DataType, Expr, Field, IntoLazy, PlSmallStr, QuantileMethod, Schema, SchemaRef,
        SortMultipleOptions, TimeUnit, UnionArgs, col, len, lit, when,
    },
};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
use strum::{Display, EnumCount, EnumIter, EnumString, IntoEnumIterator, IntoStaticStr};

use crate::{
    error::{ChapatyError, ChapatyResult, DataError},
    gym::trading::types::{StateKind, TradeKind},
    report::{
        grouped::GroupedJournal,
        io::{Report, ReportName, ToSchema, generate_dynamic_base_name},
        journal::{ExprDefineExt, Journal, JournalCol, JournalExprExt},
        polars_ext::polars_to_chapaty_error,
    },
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeStatistics {
    pub df: DataFrame,
}

impl Default for TradeStatistics {
    fn default() -> Self {
        let df = DataFrame::empty_with_schema(&Self::to_schema());
        Self { df }
    }
}

impl ReportName for TradeStatistics {
    fn base_name(&self) -> String {
        generate_dynamic_base_name(&self.df, "trade_statistics")
    }
}

impl Report for TradeStatistics {
    fn as_df(&self) -> &DataFrame {
        &self.df
    }

    fn as_df_mut(&mut self) -> &mut DataFrame {
        &mut self.df
    }
}

impl ToSchema for TradeStatistics {
    fn to_schema() -> SchemaRef {
        let fields = TradeStatCol::iter()
            .map(|col| {
                let dtype = match col {
                    TradeStatCol::WinningTradeCount
                    | TradeStatCol::LosingTradeCount
                    | TradeStatCol::TotalTradeCount
                    | TradeStatCol::MaxConsecutiveWins
                    | TradeStatCol::MaxConsecutiveLosses
                    | TradeStatCol::MaxConsecutiveUnrealizedWins
                    | TradeStatCol::MaxConsecutiveUnrealizedLosses
                    | TradeStatCol::UnrealizedWinCount
                    | TradeStatCol::UnrealizedLossCount
                    | TradeStatCol::UnrealizedTradeCount
                    | TradeStatCol::PendingCount
                    | TradeStatCol::LongestPendingStreak
                    | TradeStatCol::LongTradeCount
                    | TradeStatCol::ShortTradeCount => DataType::UInt32,

                    TradeStatCol::AvgTradeDuration
                    | TradeStatCol::MedianTradeDuration
                    | TradeStatCol::MinTradeDuration
                    | TradeStatCol::MaxTradeDuration
                    | TradeStatCol::LowerQuantileTradeDuration
                    | TradeStatCol::UpperQuantileTradeDuration
                    | TradeStatCol::AvgWinDuration
                    | TradeStatCol::MedianWinDuration
                    | TradeStatCol::LowerQuantileWinDuration
                    | TradeStatCol::UpperQuantileWinDuration
                    | TradeStatCol::AvgLossDuration
                    | TradeStatCol::MedianLossDuration
                    | TradeStatCol::LowerQuantileLossDuration
                    | TradeStatCol::UpperQuantileLossDuration => {
                        DataType::Duration(TimeUnit::Microseconds)
                    }
                };
                Field::new(col.into(), dtype)
            })
            .collect::<Vec<_>>();

        Arc::new(Schema::from_iter(fields))
    }
}

impl TryFrom<&Journal> for TradeStatistics {
    type Error = ChapatyError;

    fn try_from(j: &Journal) -> ChapatyResult<Self> {
        if j.as_df().height() == 0 {
            return Ok(Self::default());
        }

        let df = j
            .as_df()
            .clone()
            .lazy()
            .select(exprs()?)
            .collect()
            .map_err(convert_err)?;

        Ok(Self { df })
    }
}

impl TryFrom<&GroupedJournal<'_>> for TradeStatistics {
    type Error = ChapatyError;

    fn try_from(gj: &GroupedJournal) -> ChapatyResult<Self> {
        if gj.source().as_df().height() == 0 {
            return Ok(Self::default());
        }

        let (partitions, keys) = gj.to_partitions()?;
        let lazy_computations = partitions
            .into_par_iter()
            .map(|df| {
                let mut selection = Vec::with_capacity(keys.len() + TradeStatCol::COUNT);
                for k in &keys {
                    selection.push(col(k).first());
                }
                selection.extend(exprs()?);

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

fn exprs() -> ChapatyResult<Vec<Expr>> {
    let return_col = JournalCol::RealizedReturnInTicks;
    let trade_state_col = JournalCol::TradeState;
    let trade_type_col = JournalCol::TradeType;

    let exprs = vec![
        // === Trade counts ===
        winning_trade_count_expr(return_col, trade_state_col)
            .define_as(TradeStatCol::WinningTradeCount, DataType::UInt32),
        losing_trade_count_expr(return_col, trade_state_col)
            .define_as(TradeStatCol::LosingTradeCount, DataType::UInt32),
        executed_trade_count_expr(trade_state_col)
            .define_as(TradeStatCol::TotalTradeCount, DataType::UInt32),
        // === Trade streaks ===
        max_consecutive_wins_expr(return_col, trade_state_col)?
            .define_as(TradeStatCol::MaxConsecutiveWins, DataType::UInt32),
        max_consecutive_losses_expr(return_col, trade_state_col)?
            .define_as(TradeStatCol::MaxConsecutiveLosses, DataType::UInt32),
        max_consecutive_unrealized_wins_expr(trade_state_col, return_col)?
            .define_as(TradeStatCol::MaxConsecutiveUnrealizedWins, DataType::UInt32),
        max_consecutive_unrealized_losses_expr(trade_state_col, return_col)?.define_as(
            TradeStatCol::MaxConsecutiveUnrealizedLosses,
            DataType::UInt32,
        ),
        // === Trade durations ===
        avg_trade_duration_expr(trade_state_col).define_as(
            TradeStatCol::AvgTradeDuration,
            DataType::Duration(TimeUnit::Microseconds),
        ),
        median_trade_duration_expr(trade_state_col).define_as(
            TradeStatCol::MedianTradeDuration,
            DataType::Duration(TimeUnit::Microseconds),
        ),
        min_trade_duration_expr(trade_state_col).define_as(
            TradeStatCol::MinTradeDuration,
            DataType::Duration(TimeUnit::Microseconds),
        ),
        max_trade_duration_expr(trade_state_col).define_as(
            TradeStatCol::MaxTradeDuration,
            DataType::Duration(TimeUnit::Microseconds),
        ),
        lower_quantile_trade_duration_expr(trade_state_col).define_as(
            TradeStatCol::LowerQuantileTradeDuration,
            DataType::Duration(TimeUnit::Microseconds),
        ),
        upper_quantile_trade_duration_expr(trade_state_col).define_as(
            TradeStatCol::UpperQuantileTradeDuration,
            DataType::Duration(TimeUnit::Microseconds),
        ),
        // === Duration breakdown by outcome ===
        avg_win_duration_expr(return_col, trade_state_col).define_as(
            TradeStatCol::AvgWinDuration,
            DataType::Duration(TimeUnit::Microseconds),
        ),
        median_win_duration_expr(return_col, trade_state_col).define_as(
            TradeStatCol::MedianWinDuration,
            DataType::Duration(TimeUnit::Microseconds),
        ),
        lower_quantile_win_duration_expr(return_col, trade_state_col).define_as(
            TradeStatCol::LowerQuantileWinDuration,
            DataType::Duration(TimeUnit::Microseconds),
        ),
        upper_quantile_win_duration_expr(return_col, trade_state_col).define_as(
            TradeStatCol::UpperQuantileWinDuration,
            DataType::Duration(TimeUnit::Microseconds),
        ),
        avg_loss_duration_expr(return_col, trade_state_col).define_as(
            TradeStatCol::AvgLossDuration,
            DataType::Duration(TimeUnit::Microseconds),
        ),
        median_loss_duration_expr(return_col, trade_state_col).define_as(
            TradeStatCol::MedianLossDuration,
            DataType::Duration(TimeUnit::Microseconds),
        ),
        lower_quantile_loss_duration_expr(return_col, trade_state_col).define_as(
            TradeStatCol::LowerQuantileLossDuration,
            DataType::Duration(TimeUnit::Microseconds),
        ),
        upper_quantile_loss_duration_expr(return_col, trade_state_col).define_as(
            TradeStatCol::UpperQuantileLossDuration,
            DataType::Duration(TimeUnit::Microseconds),
        ),
        // === Unrealized tracking ===
        unrealized_win_count_expr(trade_state_col, return_col)
            .define_as(TradeStatCol::UnrealizedWinCount, DataType::UInt32),
        unrealized_loss_count_expr(trade_state_col, return_col)
            .define_as(TradeStatCol::UnrealizedLossCount, DataType::UInt32),
        unrealized_trade_count_expr(trade_state_col)
            .define_as(TradeStatCol::UnrealizedTradeCount, DataType::UInt32),
        // === Entry quality ===
        pending_count_expr(trade_state_col).define_as(TradeStatCol::PendingCount, DataType::UInt32),
        longest_pending_streak_expr(trade_state_col)?
            .define_as(TradeStatCol::LongestPendingStreak, DataType::UInt32),
        long_trade_count_expr(trade_type_col, trade_state_col)
            .define_as(TradeStatCol::LongTradeCount, DataType::UInt32),
        short_trade_count_expr(trade_type_col, trade_state_col)
            .define_as(TradeStatCol::ShortTradeCount, DataType::UInt32),
    ];
    Ok(exprs)
}

// ================================================================================================
// === Trade counts ===
// ================================================================================================
fn winning_trade_count_expr(return_col: JournalCol, trade_state_col: JournalCol) -> Expr {
    col(trade_state_col)
        .trade_executed()
        .and(col(return_col).gt(lit(0)))
        .count_true()
}

fn losing_trade_count_expr(return_col: JournalCol, trade_state_col: JournalCol) -> Expr {
    col(trade_state_col)
        .trade_executed()
        .and(col(return_col).lt_eq(lit(0)))
        .count_true()
}

pub(super) fn executed_trade_count_expr(trade_state_col: JournalCol) -> Expr {
    col(trade_state_col).trade_executed().count_true()
}

// ================================================================================================
// === Trade streaks ===
// ================================================================================================

fn max_consecutive_wins_expr(
    return_col: JournalCol,
    trade_state_col: JournalCol,
) -> ChapatyResult<Expr> {
    let predicate = col(trade_state_col)
        .trade_executed()
        .and(col(return_col).gt(lit(0)));
    max_consecutive_streak_expr(predicate)
}

fn max_consecutive_losses_expr(
    return_col: JournalCol,
    trade_state_col: JournalCol,
) -> ChapatyResult<Expr> {
    let predicate = col(trade_state_col)
        .trade_executed()
        .and(col(return_col).lt_eq(lit(0)));
    max_consecutive_streak_expr(predicate)
}

fn max_consecutive_unrealized_wins_expr(
    trade_state_col: JournalCol,
    return_col: JournalCol,
) -> ChapatyResult<Expr> {
    let predicate = col(trade_state_col)
        .eq(lit(StateKind::Active.as_str()))
        .and(col(return_col).gt(lit(0)));

    max_consecutive_streak_expr(predicate)
}

fn max_consecutive_unrealized_losses_expr(
    trade_state_col: JournalCol,
    return_col: JournalCol,
) -> ChapatyResult<Expr> {
    let predicate = col(trade_state_col)
        .eq(lit(StateKind::Active.as_str()))
        .and(col(return_col).lt_eq(lit(0)));

    max_consecutive_streak_expr(predicate)
}

// ================================================================================================
// === Trade durations ===
// ================================================================================================
fn avg_trade_duration_expr(trade_state_col: JournalCol) -> Expr {
    trade_duration_expr()
        .filter(col(trade_state_col).trade_executed())
        .mean()
}

fn median_trade_duration_expr(trade_state_col: JournalCol) -> Expr {
    trade_duration_expr()
        .filter(col(trade_state_col).trade_executed())
        .median()
}

fn min_trade_duration_expr(trade_state_col: JournalCol) -> Expr {
    trade_duration_expr()
        .filter(col(trade_state_col).trade_executed())
        .min()
}

fn max_trade_duration_expr(trade_state_col: JournalCol) -> Expr {
    trade_duration_expr()
        .filter(col(trade_state_col).trade_executed())
        .max()
}

fn lower_quantile_trade_duration_expr(trade_state_col: JournalCol) -> Expr {
    trade_duration_expr()
        .filter(col(trade_state_col).trade_executed())
        .quantile(lit(0.25), QuantileMethod::Linear)
}

fn upper_quantile_trade_duration_expr(trade_state_col: JournalCol) -> Expr {
    trade_duration_expr()
        .filter(col(trade_state_col).trade_executed())
        .quantile(lit(0.75), QuantileMethod::Linear)
}

// ================================================================================================
// === Duration breakdown by outcome ===
// ================================================================================================
fn avg_win_duration_expr(return_col: JournalCol, trade_state_col: JournalCol) -> Expr {
    trade_duration_expr()
        .filter(
            col(trade_state_col)
                .trade_executed()
                .and(col(return_col).gt(lit(0))),
        )
        .mean()
}

fn median_win_duration_expr(return_col: JournalCol, trade_state_col: JournalCol) -> Expr {
    trade_duration_expr()
        .filter(
            col(trade_state_col)
                .trade_executed()
                .and(col(return_col).gt(lit(0))),
        )
        .median()
}

fn lower_quantile_win_duration_expr(return_col: JournalCol, trade_state_col: JournalCol) -> Expr {
    quantile_duration_expr(return_col, trade_state_col, 0.25, true)
}

fn upper_quantile_win_duration_expr(return_col: JournalCol, trade_state_col: JournalCol) -> Expr {
    quantile_duration_expr(return_col, trade_state_col, 0.75, true)
}

fn avg_loss_duration_expr(return_col: JournalCol, trade_state_col: JournalCol) -> Expr {
    trade_duration_expr()
        .filter(
            col(trade_state_col)
                .trade_executed()
                .and(col(return_col).lt_eq(lit(0))),
        )
        .mean()
}

fn median_loss_duration_expr(return_col: JournalCol, trade_state_col: JournalCol) -> Expr {
    trade_duration_expr()
        .filter(
            col(trade_state_col)
                .trade_executed()
                .and(col(return_col).lt_eq(lit(0))),
        )
        .median()
}

fn lower_quantile_loss_duration_expr(return_col: JournalCol, trade_state_col: JournalCol) -> Expr {
    quantile_duration_expr(return_col, trade_state_col, 0.25, false)
}

fn upper_quantile_loss_duration_expr(return_col: JournalCol, trade_state_col: JournalCol) -> Expr {
    quantile_duration_expr(return_col, trade_state_col, 0.75, false)
}

// ================================================================================================
// === Unrealized tracking ===
// ================================================================================================
fn unrealized_win_count_expr(trade_state_col: JournalCol, return_col: JournalCol) -> Expr {
    (col(trade_state_col)
        .eq(lit(StateKind::Active.as_str()))
        .and(col(return_col).gt(lit(0))))
    .count_true()
}

fn unrealized_loss_count_expr(trade_state_col: JournalCol, return_col: JournalCol) -> Expr {
    (col(trade_state_col)
        .eq(lit(StateKind::Active.as_str()))
        .and(col(return_col).lt_eq(lit(0))))
    .count_true()
}

fn unrealized_trade_count_expr(trade_state_col: JournalCol) -> Expr {
    (col(trade_state_col).eq(lit(StateKind::Active.as_str()))).count_true()
}

// ================================================================================================
// === Entry quality ===
// ================================================================================================
fn pending_count_expr(trade_state_col: JournalCol) -> Expr {
    col(trade_state_col)
        .eq(lit(StateKind::Pending.as_str()))
        .count_true()
}

fn longest_pending_streak_expr(trade_state_col: JournalCol) -> ChapatyResult<Expr> {
    let predicate = col(trade_state_col).eq(lit(StateKind::Pending.as_str()));
    max_consecutive_streak_expr(predicate)
}

fn long_trade_count_expr(trade_type_col: JournalCol, trade_state_col: JournalCol) -> Expr {
    col(trade_state_col)
        .trade_executed()
        .and(col(trade_type_col).eq(lit(TradeKind::Long.as_str())))
        .count_true()
}

fn short_trade_count_expr(trade_type_col: JournalCol, trade_state_col: JournalCol) -> Expr {
    col(trade_state_col)
        .trade_executed()
        .and(col(trade_type_col).eq(lit(TradeKind::Short.as_str())))
        .count_true()
}

// ================================================================================================
// Helper Functions
// ================================================================================================
/// Create an expression that computes the maximum length of consecutive `true` values
/// in a boolean predicate expression, often used to identify streaks in a column.
///
/// Reference: <https://stackoverflow.com/a/75405310>
fn max_consecutive_streak_expr(predicate: Expr) -> ChapatyResult<Expr> {
    let rle = predicate.clone().rle_id();
    len()
        .over([rle])
        .map(|streak_len| when(predicate).then(streak_len).otherwise(lit(0)).max())
        .map_err(convert_err)
}

/// Returns the trade duration as a Polars `Duration(Microseconds)` column.
fn trade_duration_expr() -> Expr {
    col(JournalCol::ExitTimestamp) - col(JournalCol::EntryTimestamp)
}

fn quantile_duration_expr(
    return_col: JournalCol,
    trade_state_col: JournalCol,
    quantile: f64,
    is_win: bool,
) -> Expr {
    let filter_expr = if is_win {
        col(trade_state_col)
            .trade_executed()
            .and(col(return_col).gt(lit(0)))
    } else {
        col(trade_state_col)
            .trade_executed()
            .and(col(return_col).lt_eq(lit(0)))
    };

    trade_duration_expr()
        .filter(filter_expr)
        .quantile(lit(quantile), QuantileMethod::Linear)
}

fn convert_err(e: polars::error::PolarsError) -> ChapatyError {
    polars_to_chapaty_error("trade statistics", e)
}

/// Descriptive statistics and behavioral patterns derived from individual trades.
///
/// These metrics provide insights into trade counts, durations, timing behaviors, and streaks.
/// They are useful for diagnosing the consistency and execution profile of a strategy.
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
pub enum TradeStatCol {
    // === Trade counts ===
    /// Total number of winning trades.
    WinningTradeCount,
    /// Total number of losing trades.
    LosingTradeCount,
    /// Total number of trades executed.
    TotalTradeCount,

    // === Trade streaks ===
    /// Maximum number of consecutive winning trades.
    MaxConsecutiveWins,
    /// Maximum number of consecutive losing trades.
    MaxConsecutiveLosses,
    /// Maximum number of consecutive winning trades that are unrealized.
    MaxConsecutiveUnrealizedWins,
    /// Maximum number of consecutive losing trades that are unrealized.
    MaxConsecutiveUnrealizedLosses,

    // === Trade durations ===
    /// Mean duration of all trades.
    AvgTradeDuration,
    /// Median duration of all trades.
    MedianTradeDuration,
    /// Shortest trade duration observed.
    MinTradeDuration,
    /// Longest trade duration observed.
    MaxTradeDuration,
    /// 25th percentile of all trade durations.
    LowerQuantileTradeDuration,
    /// 75th percentile of all trade durations.
    UpperQuantileTradeDuration,

    // === Duration breakdown by outcome ===
    AvgWinDuration,
    MedianWinDuration,
    LowerQuantileWinDuration,
    UpperQuantileWinDuration,
    AvgLossDuration,
    MedianLossDuration,
    LowerQuantileLossDuration,
    UpperQuantileLossDuration,

    // === Unrealized tracking ===
    UnrealizedWinCount,
    UnrealizedLossCount,
    UnrealizedTradeCount,

    // === Entry quality ===
    PendingCount,
    LongestPendingStreak,
    LongTradeCount,
    ShortTradeCount,
}

impl From<TradeStatCol> for PlSmallStr {
    fn from(value: TradeStatCol) -> Self {
        value.as_str().into()
    }
}

impl TradeStatCol {
    #[must_use]
    pub fn name(&self) -> PlSmallStr {
        (*self).into()
    }

    #[must_use]
    pub fn as_str(&self) -> &'static str {
        self.into()
    }
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, clippy::expect_used)]
    use std::{collections::HashSet, path::PathBuf};

    use polars::prelude::{LazyCsvReader, LazyFileListReader, PlRefPath, SchemaExt};

    use crate::data::common::RiskMetricsConfig;

    use super::*;

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
        let df = LazyCsvReader::new(PlRefPath::new(
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

        Journal::new(&df, RiskMetricsConfig::default()).expect("Failed to create Journal")
    }

    // ========================================================================
    // Test: Journal to TradeStatistics Conversion
    // ========================================================================

    #[test]
    fn test_journal_to_trade_statistics() {
        let journal = load_journal_fixture();
        let result = TradeStatistics::try_from(&journal);

        assert!(
            result.is_ok(),
            "Failed to convert Journal to TradeStatistics: {:?}",
            result.err()
        );

        let stats = result.unwrap();
        let df = stats.as_df();

        // Should produce exactly 1 row (aggregated statistics)
        assert_eq!(
            df.height(),
            1,
            "TradeStatistics should have 1 row (aggregated statistics)"
        );
    }

    // ========================================================================
    // Test: All Expected Columns Present
    // ========================================================================

    #[test]
    fn test_all_trade_statistics_fields_present() {
        let journal = load_journal_fixture();
        let stats = TradeStatistics::try_from(&journal).expect("Conversion failed");
        let df = stats.as_df();

        let expected_columns: Vec<_> = TradeStatCol::iter().collect();

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
    fn test_trade_statistics_data_types() {
        let journal = load_journal_fixture();
        let stats = TradeStatistics::try_from(&journal).expect("Conversion failed");
        let df = stats.as_df();
        let expected_schema = TradeStatistics::to_schema();

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
    // Test: Trade Counts
    // ========================================================================

    #[test]
    fn test_trade_counts() {
        let journal = load_journal_fixture();
        let stats = TradeStatistics::try_from(&journal).expect("Conversion failed");
        let df = stats.as_df();

        // Fixture has 3 winning trades (+2000, +500, +1000)
        let winning_count = df
            .column(TradeStatCol::WinningTradeCount.as_str())
            .expect("Missing winning_trade_count column")
            .u32()
            .expect("Column is not u32")
            .get(0)
            .expect("Missing value");

        assert_eq!(winning_count, 3, "Should have 3 winning trades");

        // Fixture has 3 losing trades (-1000, -500, 0)
        let losing_count = df
            .column(TradeStatCol::LosingTradeCount.as_str())
            .expect("Missing losing_trade_count column")
            .u32()
            .expect("Column is not u32")
            .get(0)
            .expect("Missing value");

        assert_eq!(
            losing_count, 3,
            "Should have 3 losing trades (including break-even)"
        );

        // Total executed trades: 5 closed + 1 active = 6
        let total_count = df
            .column(TradeStatCol::TotalTradeCount.as_str())
            .expect("Missing total_trade_count column")
            .u32()
            .expect("Column is not u32")
            .get(0)
            .expect("Missing value");

        assert_eq!(total_count, 6, "Should have 6 total executed trades");
    }

    // ========================================================================
    // Test: Consecutive Win/Loss Streaks
    // ========================================================================

    #[test]
    fn test_consecutive_streaks() {
        let journal = load_journal_fixture();
        let stats = TradeStatistics::try_from(&journal).expect("Conversion failed");
        let df = stats.as_df();

        // Fixture sequence: -1000, -500, 0, +2000, +500, +1000
        // Wins: positions 3, 4, 5 (3 consecutive)
        // Losses: positions 0, 1, 2 (3 consecutive)
        let max_wins = df
            .column(TradeStatCol::MaxConsecutiveWins.as_str())
            .expect("Missing max_consecutive_wins column")
            .u32()
            .expect("Column is not u32")
            .get(0)
            .expect("Missing value");

        assert_eq!(
            max_wins, 3,
            "Max consecutive wins should be 3 (rows 4, 5, 6)"
        );

        let max_losses = df
            .column(TradeStatCol::MaxConsecutiveLosses.as_str())
            .expect("Missing max_consecutive_losses column")
            .u32()
            .expect("Column is not u32")
            .get(0)
            .expect("Missing value");

        assert_eq!(
            max_losses, 3,
            "Max consecutive losses should be 3 (rows 1, 2, 3)"
        );
    }

    // ========================================================================
    // Test: Unrealized Trade Counts
    // ========================================================================

    #[test]
    fn test_unrealized_trade_counts() {
        let journal = load_journal_fixture();
        let stats = TradeStatistics::try_from(&journal).expect("Conversion failed");
        let df = stats.as_df();

        // Row 6 is active with +1000 unrealized profit
        let unrealized_win_count = df
            .column(TradeStatCol::UnrealizedWinCount.as_str())
            .expect("Missing unrealized_win_count column")
            .u32()
            .expect("Column is not u32")
            .get(0)
            .expect("Missing value");

        assert_eq!(
            unrealized_win_count, 1,
            "Should have 1 unrealized winning trade"
        );

        let unrealized_loss_count = df
            .column(TradeStatCol::UnrealizedLossCount.as_str())
            .expect("Missing unrealized_loss_count column")
            .u32()
            .expect("Column is not u32")
            .get(0)
            .expect("Missing value");

        assert_eq!(
            unrealized_loss_count, 0,
            "Should have 0 unrealized losing trades"
        );

        let total_unrealized = df
            .column(TradeStatCol::UnrealizedTradeCount.as_str())
            .expect("Missing unrealized_trade_count column")
            .u32()
            .expect("Column is not u32")
            .get(0)
            .expect("Missing value");

        assert_eq!(total_unrealized, 1, "Should have 1 total unrealized trade");
    }

    // ========================================================================
    // Test: Unrealized Streaks
    // ========================================================================

    #[test]
    fn test_unrealized_streaks() {
        let journal = load_journal_fixture();
        let stats = TradeStatistics::try_from(&journal).expect("Conversion failed");
        let df = stats.as_df();

        // Only 1 active trade at the end, so max streak is 1
        let unrealized_wins_streak = df
            .column(TradeStatCol::MaxConsecutiveUnrealizedWins.as_str())
            .expect("Missing max_consecutive_unrealized_wins column")
            .u32()
            .expect("Column is not u32")
            .get(0)
            .expect("Missing value");

        assert_eq!(
            unrealized_wins_streak, 1,
            "Max consecutive unrealized wins should be 1"
        );

        let unrealized_losses_streak = df
            .column(TradeStatCol::MaxConsecutiveUnrealizedLosses.as_str())
            .expect("Missing max_consecutive_unrealized_losses column")
            .u32()
            .expect("Column is not u32")
            .get(0)
            .expect("Missing value");

        assert_eq!(
            unrealized_losses_streak, 0,
            "Max consecutive unrealized losses should be 0"
        );
    }

    // ========================================================================
    // Test: Trade Durations (Existence)
    // ========================================================================

    #[test]
    fn test_trade_durations_computed() {
        let journal = load_journal_fixture();
        let stats = TradeStatistics::try_from(&journal).expect("Conversion failed");
        let df = stats.as_df();

        // Verify duration columns exist and have values
        let avg_duration = df
            .column(TradeStatCol::AvgTradeDuration.as_str())
            .expect("Missing avg_trade_duration column")
            .duration()
            .expect("Column is not duration");

        assert!(
            avg_duration.physical().get(0).is_some(),
            "Average trade duration should be calculated"
        );

        let median_duration = df
            .column(TradeStatCol::MedianTradeDuration.as_str())
            .expect("Missing median_trade_duration column")
            .duration()
            .expect("Column is not duration");

        assert!(
            median_duration.physical().get(0).is_some(),
            "Median trade duration should be calculated"
        );

        let min_duration = df
            .column(TradeStatCol::MinTradeDuration.as_str())
            .expect("Missing min_trade_duration column")
            .duration()
            .expect("Column is not duration");

        assert!(
            min_duration.physical().get(0).is_some(),
            "Min trade duration should be calculated"
        );

        let max_duration = df
            .column(TradeStatCol::MaxTradeDuration.as_str())
            .expect("Missing max_trade_duration column")
            .duration()
            .expect("Column is not duration");

        assert!(
            max_duration.physical().get(0).is_some(),
            "Max trade duration should be calculated"
        );
    }

    // ========================================================================
    // Test: Duration Quantiles
    // ========================================================================

    #[test]
    fn test_duration_quantiles() {
        let journal = load_journal_fixture();
        let stats = TradeStatistics::try_from(&journal).expect("Conversion failed");
        let df = stats.as_df();

        let lower_quantile = df
            .column(TradeStatCol::LowerQuantileTradeDuration.as_str())
            .expect("Missing lower_quantile_trade_duration column")
            .duration()
            .expect("Column is not duration");

        assert!(
            lower_quantile.physical().get(0).is_some(),
            "Lower quantile duration should be calculated"
        );

        let upper_quantile = df
            .column(TradeStatCol::UpperQuantileTradeDuration.as_str())
            .expect("Missing upper_quantile_trade_duration column")
            .duration()
            .expect("Column is not duration");

        assert!(
            upper_quantile.physical().get(0).is_some(),
            "Upper quantile duration should be calculated"
        );

        // Lower quantile should be <= upper quantile
        let lower_val = lower_quantile.physical().get(0).unwrap();
        let upper_val = upper_quantile.physical().get(0).unwrap();
        assert!(
            lower_val <= upper_val,
            "Lower quantile should be <= upper quantile"
        );
    }

    // ========================================================================
    // Test: Win Duration Statistics
    // ========================================================================

    #[test]
    fn test_win_durations() {
        let journal = load_journal_fixture();
        let stats = TradeStatistics::try_from(&journal).expect("Conversion failed");
        let df = stats.as_df();

        // Winning trades: rows 4, 5, 6
        let avg_win_duration = df
            .column(TradeStatCol::AvgWinDuration.as_str())
            .expect("Missing avg_win_duration column")
            .duration()
            .expect("Column is not duration");

        assert!(
            avg_win_duration.physical().get(0).is_some(),
            "Average win duration should be calculated"
        );

        let median_win_duration = df
            .column(TradeStatCol::MedianWinDuration.as_str())
            .expect("Missing median_win_duration column")
            .duration()
            .expect("Column is not duration");

        assert!(
            median_win_duration.physical().get(0).is_some(),
            "Median win duration should be calculated"
        );

        let lower_q_win = df
            .column(TradeStatCol::LowerQuantileWinDuration.as_str())
            .expect("Missing lower_quantile_win_duration column")
            .duration()
            .expect("Column is not duration");

        assert!(
            lower_q_win.physical().get(0).is_some(),
            "Lower quantile win duration should be calculated"
        );

        let upper_q_win = df
            .column(TradeStatCol::UpperQuantileWinDuration.as_str())
            .expect("Missing upper_quantile_win_duration column")
            .duration()
            .expect("Column is not duration");

        assert!(
            upper_q_win.physical().get(0).is_some(),
            "Upper quantile win duration should be calculated"
        );
    }

    // ========================================================================
    // Test: Loss Duration Statistics
    // ========================================================================

    #[test]
    fn test_loss_durations() {
        let journal = load_journal_fixture();
        let stats = TradeStatistics::try_from(&journal).expect("Conversion failed");
        let df = stats.as_df();

        // Losing trades: rows 1, 2, 3
        let avg_loss_duration = df
            .column(TradeStatCol::AvgLossDuration.as_str())
            .expect("Missing avg_loss_duration column")
            .duration()
            .expect("Column is not duration");

        assert!(
            avg_loss_duration.physical().get(0).is_some(),
            "Average loss duration should be calculated"
        );

        let median_loss_duration = df
            .column(TradeStatCol::MedianLossDuration.as_str())
            .expect("Missing median_loss_duration column")
            .duration()
            .expect("Column is not duration");

        assert!(
            median_loss_duration.physical().get(0).is_some(),
            "Median loss duration should be calculated"
        );

        let lower_q_loss = df
            .column(TradeStatCol::LowerQuantileLossDuration.as_str())
            .expect("Missing lower_quantile_loss_duration column")
            .duration()
            .expect("Column is not duration");

        assert!(
            lower_q_loss.physical().get(0).is_some(),
            "Lower quantile loss duration should be calculated"
        );

        let upper_q_loss = df
            .column(TradeStatCol::UpperQuantileLossDuration.as_str())
            .expect("Missing upper_quantile_loss_duration column")
            .duration()
            .expect("Column is not duration");

        assert!(
            upper_q_loss.physical().get(0).is_some(),
            "Upper quantile loss duration should be calculated"
        );
    }

    // ========================================================================
    // Test: Pending Trade Statistics
    // ========================================================================

    #[test]
    fn test_pending_statistics() {
        let journal = load_journal_fixture();
        let stats = TradeStatistics::try_from(&journal).expect("Conversion failed");
        let df = stats.as_df();

        // Fixture has no pending trades (all are closed or active)
        let pending_count = df
            .column(TradeStatCol::PendingCount.as_str())
            .expect("Missing pending_count column")
            .u32()
            .expect("Column is not u32")
            .get(0)
            .expect("Missing value");

        assert_eq!(pending_count, 1, "Should have 0 pending trades");

        let longest_pending_streak = df
            .column(TradeStatCol::LongestPendingStreak.as_str())
            .expect("Missing longest_pending_streak column")
            .u32()
            .expect("Column is not u32")
            .get(0)
            .expect("Missing value");

        assert_eq!(
            longest_pending_streak, 1,
            "Longest pending streak should be 0"
        );
    }

    // ========================================================================
    // Test: Trade Type Counts
    // ========================================================================

    #[test]
    fn test_trade_type_counts() {
        let journal = load_journal_fixture();
        let stats = TradeStatistics::try_from(&journal).expect("Conversion failed");
        let df = stats.as_df();

        // Fixture: rows 1,2 are short, row 3 is long, rows 4,5 are short, row 6 is long
        // Total: 4 short, 2 long
        let long_count = df
            .column(TradeStatCol::LongTradeCount.as_str())
            .expect("Missing long_trade_count column")
            .u32()
            .expect("Column is not u32")
            .get(0)
            .expect("Missing value");

        assert_eq!(long_count, 2, "Should have 2 long trades");

        let short_count = df
            .column(TradeStatCol::ShortTradeCount.as_str())
            .expect("Missing short_trade_count column")
            .u32()
            .expect("Column is not u32")
            .get(0)
            .expect("Missing value");

        assert_eq!(short_count, 4, "Should have 4 short trades");
    }

    // ========================================================================
    // Test: Empty Journal
    // ========================================================================

    #[test]
    fn test_empty_journal() {
        let empty_df = DataFrame::empty_with_schema(&Journal::to_schema());
        let journal = Journal::new(&empty_df, RiskMetricsConfig::default())
            .expect("Failed to create empty Journal");

        let result = TradeStatistics::try_from(&journal);
        assert!(result.is_ok(), "Should handle empty Journal");

        let stats = result.unwrap();
        let df = stats.as_df();
        assert_eq!(df.height(), 0, "Empty journal should produce 0 rows");
    }

    // ========================================================================
    // Test: Min/Max Duration Relationship
    // ========================================================================

    #[test]
    fn test_min_max_duration_relationship() {
        let journal = load_journal_fixture();
        let stats = TradeStatistics::try_from(&journal).expect("Conversion failed");
        let df = stats.as_df();

        let min_duration = df
            .column(TradeStatCol::MinTradeDuration.as_str())
            .expect("Missing min_trade_duration column")
            .duration()
            .expect("Column is not duration")
            .physical()
            .get(0)
            .expect("Missing value");

        let max_duration = df
            .column(TradeStatCol::MaxTradeDuration.as_str())
            .expect("Missing max_trade_duration column")
            .duration()
            .expect("Column is not duration")
            .physical()
            .get(0)
            .expect("Missing value");

        let avg_duration = df
            .column(TradeStatCol::AvgTradeDuration.as_str())
            .expect("Missing avg_trade_duration column")
            .duration()
            .expect("Column is not duration")
            .physical()
            .get(0)
            .expect("Missing value");

        // Logical constraints
        assert!(
            min_duration <= max_duration,
            "Min duration should be <= max duration"
        );
        assert!(
            min_duration <= avg_duration,
            "Min duration should be <= avg duration"
        );
        assert!(
            avg_duration <= max_duration,
            "Avg duration should be <= max duration"
        );
    }

    // ========================================================================
    // Test: Win/Loss Count Consistency
    // ========================================================================

    #[test]
    fn test_win_loss_count_consistency() {
        let journal = load_journal_fixture();
        let stats = TradeStatistics::try_from(&journal).expect("Conversion failed");
        let df = stats.as_df();

        let winning_count = df
            .column(TradeStatCol::WinningTradeCount.as_str())
            .expect("Missing winning_trade_count")
            .u32()
            .expect("Not u32")
            .get(0)
            .expect("Missing value");

        let losing_count = df
            .column(TradeStatCol::LosingTradeCount.as_str())
            .expect("Missing losing_trade_count")
            .u32()
            .expect("Not u32")
            .get(0)
            .expect("Missing value");

        let total_count = df
            .column(TradeStatCol::TotalTradeCount.as_str())
            .expect("Missing total_trade_count")
            .u32()
            .expect("Not u32")
            .get(0)
            .expect("Missing value");

        // Total should equal wins + losses
        assert_eq!(
            winning_count + losing_count,
            total_count,
            "Winning + losing should equal total trade count"
        );
    }
}
