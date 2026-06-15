use std::sync::Arc;

use polars::{
    prelude::{
        DataFrame, DataType, Expr, Field, PlSmallStr, Schema, SchemaRef, SortMultipleOptions,
        TimeUnit, TimeZone, lit,
    },
    series::IsSorted,
};
use serde::{Deserialize, Serialize};
use strum::{Display, EnumIter, EnumString, IntoEnumIterator, IntoStaticStr};

use crate::{
    data::common::RiskMetricsConfig,
    error::{ChapatyError, ChapatyResult, DataError},
    gym::trading::StateKind,
    report::{
        cumulative_returns::CumulativeReturns,
        grouped::{GroupCol, GroupedJournal},
        io::{Report, ReportName, ToSchema, generate_dynamic_base_name},
        portfolio_performance::PortfolioPerformance,
        trade_statistics::TradeStatistics,
    },
};

/// Represents the detailed journal recording every individual trade.
///
/// This journal serves as a comprehensive log of all trades executed during backtesting.
/// It is analogous to a trade journal or transaction log in traditional finance,
/// capturing raw trade details used for analysis and performance evaluation.
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
pub enum JournalCol {
    // === Identifiers ===
    /// Row identifier for the journal entry (globally unique per row).
    RowId,
    /// Identifier for the episode this trade occurred in.
    EpisodeId,
    /// Identifier for the trade within the episode.
    TradeId,
    /// State for the trade at the end of the episode.
    TradeState,
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
    /// The price at which the trade was entered.
    EntryPrice,
    /// The price at which the trade will be stopped to limit loss.
    StopLossPrice,
    /// The target price for taking profit.
    TakeProfitPrice,
    /// The quantity of the asset traded.
    Quantity,

    // === Expected outcomes ===
    /// The expected loss in native market price increments (e.g., ticks for futures, pips for FX).
    ExpectedLossInTicks,
    /// The expected profit in native market price increments.
    ExpectedProfitInTicks,
    /// The expected loss in dollars.
    ExpectedLossDollars,
    /// The expected profit in dollars.
    ExpectedProfitDollars,
    /// The ratio of expected reward to risk.
    RiskRewardRatio,

    // === Timestamps ===
    /// The timestamp when the trade was entered.
    EntryTimestamp,
    /// The timestamp when the trade was exited.
    ExitTimestamp,

    // === Realized outcomes ===
    /// The price at which the trade was exited.
    ExitPrice,
    /// The reason the trade was exited (e.g., `take_profit`, `stop_loss`).
    ExitReason,
    /// The realized reward in native market price increments.
    RealizedReturnInTicks,
    /// The realized reward in dollars.
    RealizedReturnDollars,
}

impl TryFrom<JournalCol> for GroupCol {
    type Error = ChapatyError;

    fn try_from(value: JournalCol) -> Result<Self, Self::Error> {
        match value {
            // === Identifiers ===
            JournalCol::EpisodeId => Ok(Self::EpisodeId),
            JournalCol::TradeState => Ok(Self::TradeState),
            JournalCol::AgentId => Ok(Self::AgentId),
            // === Market spec ===
            JournalCol::DataBroker => Ok(Self::DataBroker),
            JournalCol::Exchange => Ok(Self::Exchange),
            JournalCol::Symbol => Ok(Self::Symbol),
            JournalCol::MarketType => Ok(Self::MarketType),
            // === Trade configuration ===
            JournalCol::TradeType => Ok(Self::TradeType),
            // === Timestamps ===
            JournalCol::EntryTimestamp => {
                Err(DataError::UnexpectedEnumVariant(
                    "Cannot convert JournalCol::EntryTimestamp to GroupCol: ambiguous mapping (could be EntryYear, EntryQuarter, or EntryMonth)".to_string()
                ).into())
            }
            JournalCol::ExitTimestamp => {
                Err(DataError::UnexpectedEnumVariant(
                    "Cannot convert JournalCol::ExitTimestamp to GroupCol: ambiguous mapping (could be ExitYear, ExitQuarter, or ExitMonth)".to_string()
                ).into())
            }
            // === Realized outcomes ===
            JournalCol::ExitReason => Ok(Self::ExitReason),
            // === Any other JournalCol variants that don't have GroupCol equivalents ===
            JournalCol::RowId
            | JournalCol::TradeId
            | JournalCol::EntryPrice
            | JournalCol::StopLossPrice
            | JournalCol::TakeProfitPrice
            | JournalCol::Quantity
            | JournalCol::ExpectedLossInTicks
            | JournalCol::ExpectedProfitInTicks
            | JournalCol::ExpectedLossDollars
            | JournalCol::ExpectedProfitDollars
            | JournalCol::RiskRewardRatio
            | JournalCol::ExitPrice
            | JournalCol::RealizedReturnInTicks
            | JournalCol::RealizedReturnDollars => Err(DataError::UnexpectedEnumVariant(
                format!("JournalCol variant '{value}' has no corresponding GroupCol mapping")
            )
            .into()),
        }
    }
}

impl From<JournalCol> for PlSmallStr {
    fn from(value: JournalCol) -> Self {
        value.as_str().into()
    }
}

impl JournalCol {
    #[must_use]
    pub fn name(&self) -> PlSmallStr {
        (*self).into()
    }

    #[must_use]
    pub fn as_str(&self) -> &'static str {
        self.into()
    }
}

/// The Journal struct acts as a wrapper around the analytical data.
#[derive(Debug, Clone)]
pub struct Journal {
    df: DataFrame,
    risk_metrics_config: RiskMetricsConfig,
}

impl ReportName for Journal {
    fn base_name(&self) -> String {
        generate_dynamic_base_name(&self.df, "journal")
    }
}

impl Report for Journal {
    fn as_df(&self) -> &DataFrame {
        &self.df
    }

    fn as_df_mut(&mut self) -> &mut DataFrame {
        &mut self.df
    }
}

impl Journal {
    /// Computes cumulative-return metrics from this journal.
    ///
    /// # Errors
    /// Returns an error if conversion to [`CumulativeReturns`] fails.
    pub fn cumulative_returns(&self) -> ChapatyResult<CumulativeReturns> {
        self.try_into()
    }

    /// Computes portfolio-performance metrics from this journal.
    ///
    /// # Errors
    /// Returns an error if conversion to [`PortfolioPerformance`] fails.
    pub fn portfolio_performance(&self) -> ChapatyResult<PortfolioPerformance> {
        self.try_into()
    }

    /// Computes trade-statistics metrics from this journal.
    ///
    /// # Errors
    /// Returns an error if conversion to [`TradeStatistics`] fails.
    pub fn trade_stats(&self) -> ChapatyResult<TradeStatistics> {
        self.try_into()
    }

    pub const fn risk_metrics_config(&self) -> RiskMetricsConfig {
        self.risk_metrics_config
    }

    pub fn group_by<I>(&self, keys: I) -> GroupedJournal<'_>
    where
        I: IntoIterator<Item = GroupCol>,
    {
        GroupedJournal::new(self, keys)
    }
}

impl Journal {
    pub(crate) fn new(df: &DataFrame, config: RiskMetricsConfig) -> ChapatyResult<Self> {
        let sorted_df = df
            .sort(
                [JournalCol::EntryTimestamp.as_str()],
                SortMultipleOptions::default(),
            )
            .map_err(|e| ChapatyError::Data(DataError::DataFrame(e.to_string())))?;

        sorted_df
            .column(JournalCol::EntryTimestamp.as_str())
            .ok()
            .map(|s| s.is_sorted_flag() == IsSorted::Ascending)
            .ok_or_else(|| {
                ChapatyError::Data(DataError::DataFrame(
                    "Journal must be sorted by entry timestamp".to_string(),
                ))
            })?;

        Ok(Self {
            df: sorted_df,
            risk_metrics_config: config,
        })
    }
}

impl Default for Journal {
    fn default() -> Self {
        let df = DataFrame::empty_with_schema(&Self::to_schema());
        let config = RiskMetricsConfig::default();
        Self {
            df,
            risk_metrics_config: config,
        }
    }
}

impl ToSchema for Journal {
    fn to_schema() -> SchemaRef {
        let fields = JournalCol::iter()
            .map(|col| {
                let dtype = match col {
                    JournalCol::RowId | JournalCol::EpisodeId => DataType::UInt32,

                    JournalCol::TradeState
                    | JournalCol::AgentId
                    | JournalCol::DataBroker
                    | JournalCol::Exchange
                    | JournalCol::Symbol
                    | JournalCol::MarketType
                    | JournalCol::TradeType
                    | JournalCol::ExitReason => DataType::String,

                    JournalCol::Quantity
                    | JournalCol::EntryPrice
                    | JournalCol::StopLossPrice
                    | JournalCol::TakeProfitPrice
                    | JournalCol::ExpectedLossDollars
                    | JournalCol::ExpectedProfitDollars
                    | JournalCol::RiskRewardRatio
                    | JournalCol::ExitPrice
                    | JournalCol::RealizedReturnDollars => DataType::Float64,

                    JournalCol::TradeId
                    | JournalCol::ExpectedLossInTicks
                    | JournalCol::ExpectedProfitInTicks
                    | JournalCol::RealizedReturnInTicks => DataType::Int64,

                    JournalCol::EntryTimestamp | JournalCol::ExitTimestamp => {
                        DataType::Datetime(TimeUnit::Microseconds, Some(TimeZone::UTC))
                    }
                };
                Field::new(col.into(), dtype)
            })
            .collect::<Vec<_>>();

        Arc::new(Schema::from_iter(fields))
    }
}

// ================================================================================================
// Helper
// ================================================================================================

pub trait ExprDefineExt {
    /// Casts the expression to the specified data type and aliases it using the provided column name.
    fn define_as<C: Into<PlSmallStr>>(self, col: C, dtype: DataType) -> Expr;
}

impl ExprDefineExt for Expr {
    fn define_as<C: Into<PlSmallStr>>(self, col: C, dtype: DataType) -> Expr {
        self.cast(dtype).alias(col)
    }
}

pub trait JournalExprExt {
    /// Evaluates to true if the expression resolves to an Active or Closed state.
    fn trade_executed(self) -> Expr;

    /// Converts a boolean mask into a sum of occurrences.
    fn count_true(self) -> Expr;
}

impl JournalExprExt for Expr {
    fn trade_executed(self) -> Expr {
        self.clone()
            .eq(lit(StateKind::Active.as_str()))
            .or(self.eq(lit(StateKind::Closed.as_str())))
    }

    fn count_true(self) -> Expr {
        self.cast(DataType::UInt32).sum()
    }
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use polars::prelude::{IntoLazy, LazyCsvReader, LazyFileListReader, PlRefPath, col};

    use super::*;

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

    #[test]
    fn test_journal_creation_and_schema_validation() {
        let journal = load_journal_fixture();
        let df = &journal.as_df();

        let current_schema = df.schema();
        let expected_schema = Journal::to_schema();

        for (name, expected_dtype) in expected_schema.iter() {
            let actual_dtype = current_schema.get(name);
            assert!(
                actual_dtype.is_some(),
                "Missing column in Journal DataFrame: {name}"
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
    fn test_is_executed_expr_filters_correctly() {
        let journal = load_journal_fixture();

        // Apply the filter logic
        let filtered_df = journal
            .as_df()
            .clone()
            .lazy()
            .filter(col(JournalCol::TradeState).trade_executed())
            .collect()
            .expect("Failed to apply is_executed_expr filter");

        // The fixture has 8 rows: 5 closed, 1 active, 1 pending, 1 canceled.
        // Exactly 6 rows should remain.
        assert_eq!(
            filtered_df.height(),
            6,
            "is_executed_expr should retain exactly 6 rows (Active/Closed) from the fixture, dropping Pending/Canceled."
        );

        // Explicitly verify the values left in the TradeState column
        let states = filtered_df
            .column(JournalCol::TradeState.as_str())
            .expect("Missing TradeState column")
            .str()
            .expect("TradeState column is not of type String");

        for state_opt in states.iter() {
            let state = state_opt.expect("Encountered null state");
            assert!(
                state == StateKind::Active.as_str() || state == StateKind::Closed.as_str(),
                "Found unexecuted state in filtered results: {state}"
            );
        }
    }
}
