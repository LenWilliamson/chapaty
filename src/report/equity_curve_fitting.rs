use std::sync::Arc;

use polars::prelude::{
    DataFrame, DataType, Expr, Field, IntoLazy, PlSmallStr, Schema, SchemaRef, col, lit,
};
use serde::{Deserialize, Serialize};
use strum::{Display, EnumIter, EnumString, IntoEnumIterator, IntoStaticStr};

use crate::{
    error::{ChapatyError, ChapatyResult, SystemError},
    report::{
        cumulative_returns::{CumulativeReturnCol, CumulativeReturns},
        io::{Report, ReportName, ToSchema, generate_dynamic_base_name},
        journal::Journal,
        polars_ext::{ExprExt, polars_to_chapaty_error},
        portfolio_performance::{PortfolioPerformance, PortfolioPerformanceCol},
    },
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EquityCurveFitting {
    df: DataFrame,
}

impl Default for EquityCurveFitting {
    fn default() -> Self {
        let df = DataFrame::empty_with_schema(&Self::to_schema());
        Self { df }
    }
}

impl ReportName for EquityCurveFitting {
    fn base_name(&self) -> String {
        generate_dynamic_base_name(&self.df, "equity_curve_fitting")
    }
}

impl Report for EquityCurveFitting {
    fn as_df(&self) -> &DataFrame {
        &self.df
    }

    fn as_df_mut(&mut self) -> &mut DataFrame {
        &mut self.df
    }
}

impl ToSchema for EquityCurveFitting {
    fn to_schema() -> SchemaRef {
        let fields: Vec<Field> = FittingCol::iter()
            .map(|col| {
                let dtype = match col {
                    FittingCol::RsquaredMedian
                    | FittingCol::RsquaredExpected
                    | FittingCol::RsquaredMean => DataType::Float64,
                };
                Field::new(col.into(), dtype)
            })
            .collect();

        Arc::new(Schema::from_iter(fields))
    }
}

impl TryFrom<&Journal> for EquityCurveFitting {
    type Error = ChapatyError;

    fn try_from(j: &Journal) -> ChapatyResult<Self> {
        if j.as_df().is_empty() {
            return Ok(Self::default());
        }

        let pp: PortfolioPerformance = j.try_into()?;
        let accessor = pp.accessor()?;
        let cr: CumulativeReturns = j.try_into()?;

        let get_required = |col: PortfolioPerformanceCol| -> ChapatyResult<f64> {
            accessor.get(col).ok_or_else(|| {
                ChapatyError::System(SystemError::MissingField(format!(
                    "Required metric '{}' is null/missing for EquityCurveFitting",
                    col
                )))
            })
        };

        let avg_trade_profit = get_required(PortfolioPerformanceCol::AvgTradeProfit)?;
        let expected_val = get_required(PortfolioPerformanceCol::ExpectedValuePerTrade)?;
        let median_return = get_required(PortfolioPerformanceCol::MedianTradeReturn)?;

        let init_val = j.risk_metrics_config().initial_portfolio_value();

        let df = cr
            .as_df()
            .clone()
            .lazy()
            .select([
                r_squared_expr(lit(avg_trade_profit), init_val).alias(FittingCol::RsquaredMean),
                r_squared_expr(lit(expected_val), init_val).alias(FittingCol::RsquaredExpected),
                r_squared_expr(lit(median_return), init_val).alias(FittingCol::RsquaredMedian),
            ])
            .collect()
            .map_err(convert_err)?;

        Ok(Self { df })
    }
}

// ================================================================================================
// === R-Squared Expressions ===
// ================================================================================================
/// Generic helper to compute R-squared for a given linear predictor.
/// R^2 = 1 - (Sum of Squared Residuals / Total Sum of Squares)
fn r_squared_expr(predictor: Expr, initial_value: u32) -> Expr {
    let y_actual = col(CumulativeReturnCol::CumulativeRealizedReturnUsd);
    // Create a time index `t` (1, 2, 3, ...) for the linear model.
    let t = col(CumulativeReturnCol::RowId)
        .cum_count(false)
        .cast(polars::prelude::DataType::Float64);
    // Predicted equity: y_predicted = initial_value + t * predictor
    let y_predicted = lit(initial_value as f64) + t * predictor;
    // Sum of Squared Residuals: sum((y_actual - y_predicted)^2)
    let ss_res = (y_actual.clone() - y_predicted).pow(2.0).sum();
    // Total Sum of Squares: sum((y_actual - mean(y_actual))^2)
    let ss_tot = (y_actual.clone() - y_actual.mean()).pow(2.0).sum();
    // R^2 = 1 - (ss_res / ss_tot)
    lit(1.0) - ss_res.safe_div(ss_tot, Some(0.0))
}

// ================================================================================================
// === Helper Functions ===
// ================================================================================================

fn convert_err(e: polars::error::PolarsError) -> ChapatyError {
    polars_to_chapaty_error("equity curve fit metrics", e)
}

/// Model fit diagnostics for cumulative return trajectories.
///
/// These metrics evaluate how well simple linear predictors (e.g., median trade return)
/// explain the shape of the equity curve. R^2 (coefficient of determination) is used
/// to assess the goodness-of-fit in each case.
///
/// This provides insight into the consistency, predictability, and linearity of returns
/// over the sequence of trades.
///
/// # Predictor Summary
///
/// | Variant            | Formula                                | Interpretation                                                                          | Usefulness                                                         |
/// |--------------------|----------------------------------------|-----------------------------------------------------------------------------------------|--------------------------------------------------------------------|
/// | `RsquaredMedian`   | `equity_t ≈ t × median(trade_returns)` | Measures how well the *typical* trade explains cumulative growth. Robust to outliers.   | Useful for skewed or heavy-tailed return distributions.            |
/// | `RsquaredExpected` | `equity_t ≈ t × expected_return`       | Assumes a probability-weighted model of return. Reflects probabilistic policy behavior. | Best when your strategy is driven by expectations.                 |
/// | `RsquaredMean`     | `equity_t ≈ t × mean(trade_returns)`   | Classic average-based equity model. Sensitive to extreme values.                        | Good baseline for stationary returns; limited in volatile regimes. |
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
pub enum FittingCol {
    /// R^2 when using the median trade return as a linear predictor of equity.
    RsquaredMedian,
    /// R^2 when using the expected trade return as a linear predictor of equity.
    RsquaredExpected,
    /// R^2 when using the mean trade return as a linear predictor of equity.
    RsquaredMean,
}

impl From<FittingCol> for PlSmallStr {
    fn from(value: FittingCol) -> Self {
        value.as_str().into()
    }
}

impl FittingCol {
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
    use polars::prelude::{
        LazyCsvReader, LazyFileListReader, PlPath, SchemaExt, StrptimeOptions, TimeUnit, TimeZone,
        df,
    };
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
    // Test: Journal to EquityCurveFitting Conversion
    // ========================================================================

    #[test]
    fn test_journal_to_equity_curve_fitting() {
        let journal = load_journal_fixture();
        let result = EquityCurveFitting::try_from(&journal);

        assert!(
            result.is_ok(),
            "Failed to convert Journal to EquityCurveFitting: {:?}",
            result.err()
        );

        let fitting = result.unwrap();
        let df = fitting.as_df();

        // Should produce exactly 1 row (single R^2 metric per predictor)
        assert_eq!(
            df.height(),
            1,
            "EquityCurveFitting should have 1 row (aggregated metrics)"
        );
    }

    // ========================================================================
    // Test: All Expected Columns Present
    // ========================================================================

    #[test]
    fn test_all_equity_curve_fitting_fields_present() {
        let journal = load_journal_fixture();
        let fitting = EquityCurveFitting::try_from(&journal).expect("Conversion failed");
        let df = fitting.as_df();

        let expected_columns: Vec<_> = FittingCol::iter().collect();

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
                let actual: HashSet<_> = df
                    .get_column_names()
                    .iter()
                    .map(|s| s.to_string())
                    .collect();
                let expected: HashSet<_> = expected_columns.iter().map(|c| c.to_string()).collect();
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
    fn test_equity_curve_fitting_data_types() {
        let journal = load_journal_fixture();
        let fitting = EquityCurveFitting::try_from(&journal).expect("Conversion failed");
        let df = fitting.as_df();
        let expected_schema = EquityCurveFitting::to_schema();

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
    // Test: R^2 Values Are Valid (0-1 range or reasonable)
    // ========================================================================

    #[test]
    fn test_r_squared_values_valid() {
        let journal = load_journal_fixture();
        let fitting = EquityCurveFitting::try_from(&journal).expect("Conversion failed");
        let df = fitting.as_df();

        for col in FittingCol::iter() {
            let values = df
                .column(col.as_str())
                .expect("Missing column")
                .f64()
                .expect("Column is not f64");

            let val = values.get(0).expect("Missing R^2 value");

            // R^2 can be negative for very poor fits, but typically in [-inf, 1]
            // For this fixture with mixed wins/losses, we expect reasonable values
            assert!(!val.is_nan(), "{} should not be NaN", col);

            // R^2 typically in [-∞, 1], but let's check it's at least computed
            // For a sanity check, we expect something in [-10, 1] range
            assert!(
                (-10.0..=1.0).contains(&val),
                "{} has unreasonable value: {}. Expected [-10, 1]",
                col,
                val
            );
        }
    }

    // ========================================================================
    // Test: R^2 Mean vs Median vs Expected (Relative Ordering)
    // ========================================================================

    #[test]
    fn test_r_squared_relative_values() {
        let journal = load_journal_fixture();
        let fitting = EquityCurveFitting::try_from(&journal).expect("Conversion failed");
        let df = fitting.as_df();

        let r2_mean = df
            .column(FittingCol::RsquaredMean.as_str())
            .expect("Missing mean")
            .f64()
            .expect("Not f64")
            .get(0)
            .expect("Missing value");

        let r2_median = df
            .column(FittingCol::RsquaredMedian.as_str())
            .expect("Missing median")
            .f64()
            .expect("Not f64")
            .get(0)
            .expect("Missing value");

        let r2_expected = df
            .column(FittingCol::RsquaredExpected.as_str())
            .expect("Missing expected")
            .f64()
            .expect("Not f64")
            .get(0)
            .expect("Missing value");

        // All three should be valid numbers
        assert!(!r2_mean.is_nan(), "R^2 mean should not be NaN");
        assert!(!r2_median.is_nan(), "R^2 median should not be NaN");
        assert!(!r2_expected.is_nan(), "R^2 expected should not be NaN");
    }

    // ========================================================================
    // Test: Empty Journal
    // ========================================================================

    #[test]
    fn test_empty_journal() {
        let empty_df = DataFrame::empty_with_schema(&Journal::to_schema());
        let journal = Journal::new(empty_df, RiskMetricsConfig::default())
            .expect("Failed to create empty Journal");

        let result = EquityCurveFitting::try_from(&journal);
        assert!(result.is_ok(), "Should handle empty Journal");

        let fitting = result.unwrap();
        let df = fitting.as_df();
        assert_eq!(df.height(), 0, "Empty journal should produce 0 rows");
    }

    // ========================================================================
    // Test: Fixture-Specific R^2 Calculation Spot Check
    // ========================================================================

    #[test]
    fn test_r_squared_calculation_spot_check() {
        let journal = load_journal_fixture();
        let fitting = EquityCurveFitting::try_from(&journal).expect("Conversion failed");
        let df = fitting.as_df();

        // Fixture returns: -1000, -500, 0, +2000, +500, +1000
        // Net profit: 1000
        // Mean return: 1000/6 = 166.67
        // Median return: median([-1000, -500, 0, +500, +1000, +2000]) = 250
        // Expected value: (win_rate * avg_win) - (loss_rate * avg_loss)
        //   = (3/6 * 1166.67) - (3/6 * 500) = 583.33 - 250 = 333.33

        // Given the linearity assumption (equity_t = initial + t * predictor),
        // we expect R^2 to reflect how well each predictor explains cumulative equity

        let r2_mean = df
            .column(FittingCol::RsquaredMean.as_str())
            .unwrap()
            .f64()
            .unwrap()
            .get(0)
            .unwrap();

        // For this fixture with high variance, R^2 might be low but should be computed
        // We just verify it's in a reasonable range
        assert!(
            r2_mean > -5.0 && r2_mean <= 1.0,
            "R^2 mean out of expected range: {}",
            r2_mean
        );

        let r2_median = df
            .column(FittingCol::RsquaredMedian.as_str())
            .unwrap()
            .f64()
            .unwrap()
            .get(0)
            .unwrap();

        assert!(
            r2_median > -5.0 && r2_median <= 1.0,
            "R^2 median out of expected range: {}",
            r2_median
        );

        let r2_expected = df
            .column(FittingCol::RsquaredExpected.as_str())
            .unwrap()
            .f64()
            .unwrap()
            .get(0)
            .unwrap();

        assert!(
            r2_expected > -5.0 && r2_expected <= 1.0,
            "R^2 expected out of expected range: {}",
            r2_expected
        );
    }

    // ========================================================================
    // Test: Single Winning Trade (Edge Case)
    // ========================================================================

    #[test]
    fn test_single_winning_trade() {
        // Create a journal with just one winning trade
        let single_trade_df = df![
            "row_id" => &[1u32],
            "episode_id" => &[1u32],
            "trade_id" => &[1u32],
            "trade_state" => &["closed"],
            "agent_id" => &["agent_a"],
            "data_broker" => &["binance"],
            "exchange" => &["binance"],
            "symbol" => &["btc-usdt"],
            "market_type" => &["spot"],
            "trade_type" => &["long"],
            "entry_price" => &[50000.0],
            "stop_loss_price" => &[49000.0],
            "take_profit_price" => &[52000.0],
            "quantity" => &[1.0],
            "expected_loss_in_ticks" => &[1000],
            "expected_profit_in_ticks" => &[2000],
            "expected_loss_dollars" => &[1000.0],
            "expected_profit_dollars" => &[2000.0],
            "risk_reward_ratio" => &[2.0],
            "entry_timestamp" => &["2025-01-01T12:00:00Z"],
            "exit_timestamp" => &["2025-01-01T14:00:00Z"],
            "exit_price" => &[52000.0],
            "exit_reason" => &["take_profit"],
            "realized_return_in_ticks" => &[2000],
            "realized_return_dollars" => &[2000.0],
        ]
        .expect("Failed to create single trade DataFrame")
        .lazy()
        .with_column(col("entry_timestamp").str().to_datetime(
            Some(TimeUnit::Microseconds),
            Some(TimeZone::UTC),
            StrptimeOptions::default(),
            lit("raise"),
        ))
        .with_column(col("exit_timestamp").str().to_datetime(
            Some(TimeUnit::Microseconds),
            Some(TimeZone::UTC),
            StrptimeOptions::default(),
            lit("raise"),
        ))
        .collect()
        .expect("Failed to collect single trade DataFrame");

        let journal = Journal::new(single_trade_df, RiskMetricsConfig::default())
            .expect("Failed to create Journal");

        let result = EquityCurveFitting::try_from(&journal);

        // With a single trade, R^2 calculation might be undefined (SS_tot = 0)
        // The implementation should handle this gracefully
        assert!(
            result.is_ok(),
            "Should handle single trade: {:?}",
            result.err()
        );
    }
}
