use std::sync::Arc;

use polars::{
    frame::DataFrame,
    prelude::{
        DataType, Field, PlSmallStr, Schema, SchemaRef, SortMultipleOptions, TimeUnit, TimeZone,
    },
    series::IsSorted,
};
use serde::{Deserialize, Serialize};
use strum::{Display, EnumIter, EnumString, IntoEnumIterator, IntoStaticStr};

use crate::{
    data::common::RiskMetricsConfig,
    error::{ChapatyError, ChapatyResult, DataError},
    report::{
        cumulative_returns::CumulativeReturns,
        equity_curve_fitting::EquityCurveFitting,
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
            JournalCol::EpisodeId => Ok(GroupCol::EpisodeId),
            JournalCol::TradeState => Ok(GroupCol::TradeState),
            JournalCol::AgentId => Ok(GroupCol::AgentId),
            // === Market spec ===
            JournalCol::DataBroker => Ok(GroupCol::DataBroker),
            JournalCol::Exchange => Ok(GroupCol::Exchange),
            JournalCol::Symbol => Ok(GroupCol::Symbol),
            JournalCol::MarketType => Ok(GroupCol::MarketType),
            // === Trade configuration ===
            JournalCol::TradeType => Ok(GroupCol::TradeType),
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
            JournalCol::ExitReason => Ok(GroupCol::ExitReason),
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
    pub fn name(&self) -> PlSmallStr {
        (*self).into()
    }

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
    pub fn cumulative_returns(&self) -> ChapatyResult<CumulativeReturns> {
        self.try_into()
    }

    pub fn equity_curve_fitting(&self) -> ChapatyResult<EquityCurveFitting> {
        self.try_into()
    }

    pub fn portfolio_performance(&self) -> ChapatyResult<PortfolioPerformance> {
        self.try_into()
    }

    pub fn trade_stats(&self) -> ChapatyResult<TradeStatistics> {
        self.try_into()
    }

    pub fn risk_metrics_config(&self) -> RiskMetricsConfig {
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
    pub(crate) fn new(df: DataFrame, config: RiskMetricsConfig) -> ChapatyResult<Self> {
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
        let fields: Vec<Field> = JournalCol::iter()
            .map(|col| {
                let dtype = match col {
                    JournalCol::RowId | JournalCol::EpisodeId => DataType::UInt32,

                    JournalCol::TradeId => DataType::Int64,

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

                    JournalCol::ExpectedLossInTicks
                    | JournalCol::ExpectedProfitInTicks
                    | JournalCol::RealizedReturnInTicks => DataType::Int64,

                    JournalCol::EntryTimestamp | JournalCol::ExitTimestamp => {
                        DataType::Datetime(TimeUnit::Microseconds, Some(TimeZone::UTC))
                    }
                };
                Field::new(col.into(), dtype)
            })
            .collect();

        Arc::new(Schema::from_iter(fields))
    }
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use polars::prelude::{LazyCsvReader, LazyFileListReader, PlPath};

    use super::*;

    #[test]
    fn test_journal_creation_and_schema_validation() {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let pb = PathBuf::from(manifest_dir).join("tests/fixtures/report/input/journal.csv");
        let path = PlPath::new(
            pb.as_os_str()
                .to_str()
                .expect("Failed to convert input file path to string"),
        );

        let schema = Journal::to_schema();
        let df = LazyCsvReader::new(path)
            .with_has_header(true)
            .with_schema(Some(schema.clone()))
            .with_try_parse_dates(true)
            .finish()
            .expect("Failed to create LazyFrame from CSV")
            .collect()
            .expect("Failed to collect DataFrame from LazyFrame");

        let journal = Journal::new(df, RiskMetricsConfig::default())
            .expect("Failed to create Journal from DataFrame");
        let df = &journal.as_df();

        let current_schema = df.schema();
        let expected_schema = Journal::to_schema();

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
}
