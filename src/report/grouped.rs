use polars::{
    frame::DataFrame,
    prelude::{Expr, IntoLazy, LazyGroupBy, PlSmallStr, col},
};
use strum::{Display, EnumIter, EnumString, IntoStaticStr};

use crate::{
    error::{ChapatyResult, DataError},
    report::{
        cumulative_returns::CumulativeReturns,
        io::Report,
        journal::{Journal, JournalCol},
        portfolio_performance::PortfolioPerformance,
        trade_statistics::TradeStatistics,
    },
};

/// A Journal in a "Grouped" state.
/// Operations performed on this struct will return results per group.
pub struct GroupedJournal<'a> {
    journal: &'a Journal,
    group_keys: Vec<GroupCol>,
}

impl<'a> GroupedJournal<'a> {
    /// Access raw Polars lazy API for custom queries
    pub fn lazy(&self) -> LazyGroupBy {
        let group_cols: Vec<Expr> = self.group_keys.iter().map(GroupCol::as_expr).collect();
        self.journal.as_df().clone().lazy().group_by(group_cols)
    }

    pub fn cumulative_returns(&self) -> ChapatyResult<CumulativeReturns> {
        self.try_into()
    }

    pub fn portfolio_performance(&self) -> ChapatyResult<PortfolioPerformance> {
        self.try_into()
    }

    pub fn trade_stats(&self) -> ChapatyResult<TradeStatistics> {
        self.try_into()
    }

    pub fn source(&self) -> &Journal {
        self.journal
    }

    pub fn group_criteria(&self) -> &[GroupCol] {
        &self.group_keys
    }
}

impl<'a> GroupedJournal<'a> {
    pub(crate) fn new(journal: &'a Journal, keys: impl IntoIterator<Item = GroupCol>) -> Self {
        Self {
            journal,
            group_keys: keys.into_iter().collect(),
        }
    }

    /// Materializes virtual group columns and partitions the DataFrame.
    ///
    /// # Returns
    /// * `Vec<DataFrame>` - The partitions (one per group).
    /// * `Vec<GroupCol>` - The group keys (e.g., [GroupCol::Symbol, GroupCol::EntryYear]).
    pub(crate) fn to_partitions(&self) -> ChapatyResult<(Vec<DataFrame>, Vec<GroupCol>)> {
        let group_exprs = self
            .group_keys
            .iter()
            .map(GroupCol::as_expr)
            .collect::<Vec<_>>();

        let df_enriched = self
            .journal
            .as_df()
            .clone()
            .lazy()
            .with_columns(group_exprs)
            .collect()
            .map_err(|e| DataError::DataFrame(format!("Failed to materialize group cols: {e}")))?;

        let partitions = df_enriched
            .partition_by_stable(&self.group_keys, true)
            .map_err(|e| DataError::DataFrame(format!("Partitioning failed: {e}")))?;

        Ok((partitions, self.group_keys.clone()))
    }
}

/// Represents the subset of columns valid for grouping operations.
///
/// This strictly enforces that users cannot group by continuous variables
/// (like Price or PnL) or unique identifiers (like RowId), preventing
/// logical errors at compile time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumString, Display, IntoStaticStr, EnumIter)]
#[strum(serialize_all = "snake_case", prefix = "__")]
pub enum GroupCol {
    // === Identifiers ===
    /// Identifier for the episode this trade occurred in.
    EpisodeId,
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

    // === Timestamps ===
    /// The year when the trade was entered.
    EntryYear,
    /// The year when the trade was exited.
    ExitYear,
    /// The quarter when the trade was entered.
    EntryQuarter,
    /// The quarter when the trade was exited.
    ExitQuarter,
    /// The month when the trade was entered.
    EntryMonth,
    /// The month when the trade was exited.
    ExitMonth,

    // === Realized outcomes ===
    /// The reason the trade was exited (e.g., `take_profit`, `stop_loss`).
    ExitReason,
}

impl From<GroupCol> for JournalCol {
    fn from(col: GroupCol) -> Self {
        match col {
            // === Identifiers ===
            GroupCol::EpisodeId => JournalCol::EpisodeId,
            GroupCol::TradeState => JournalCol::TradeState,
            GroupCol::AgentId => JournalCol::AgentId,

            // === Market spec ===
            GroupCol::DataBroker => JournalCol::DataBroker,
            GroupCol::Exchange => JournalCol::Exchange,
            GroupCol::Symbol => JournalCol::Symbol,
            GroupCol::MarketType => JournalCol::MarketType,

            // === Trade configuration ===
            GroupCol::TradeType => JournalCol::TradeType,

            // === Timestamps (Mapped to parent TS columns) ===
            GroupCol::EntryYear | GroupCol::EntryQuarter | GroupCol::EntryMonth => {
                JournalCol::EntryTimestamp
            }
            GroupCol::ExitYear | GroupCol::ExitQuarter | GroupCol::ExitMonth => {
                JournalCol::ExitTimestamp
            }

            // === Realized outcomes ===
            GroupCol::ExitReason => JournalCol::ExitReason,
        }
    }
}

impl From<GroupCol> for PlSmallStr {
    fn from(value: GroupCol) -> Self {
        value.as_str().into()
    }
}

impl From<&GroupCol> for PlSmallStr {
    fn from(value: &GroupCol) -> Self {
        value.as_str().into()
    }
}

impl GroupCol {
    pub fn name(&self) -> PlSmallStr {
        (*self).into()
    }

    pub fn as_str(&self) -> &'static str {
        self.into()
    }

    /// Converts the group column into a Polars Expression.
    pub fn as_expr(&self) -> Expr {
        let source_col: JournalCol = (*self).into();

        let expr = match self {
            // === Simple Passthrough Columns ===
            Self::EpisodeId
            | Self::TradeState
            | Self::AgentId
            | Self::DataBroker
            | Self::Exchange
            | Self::Symbol
            | Self::MarketType
            | Self::TradeType
            | Self::ExitReason => col(source_col),

            // === Virtual Time Columns (Entry) ===
            Self::EntryYear => col(source_col).dt().year(),
            Self::EntryQuarter => col(source_col).dt().quarter(),
            Self::EntryMonth => col(source_col).dt().month(),

            // === Virtual Time Columns (Exit) ===
            Self::ExitYear => col(source_col).dt().year(),
            Self::ExitQuarter => col(source_col).dt().quarter(),
            Self::ExitMonth => col(source_col).dt().month(),
        };

        expr.alias(*self)
    }
}

#[cfg(test)]
mod tests {
    use polars::prelude::{
        LazyCsvReader, LazyFileListReader, PlPath, StrptimeOptions, TimeUnit, TimeZone, df, lit,
    };

    use super::*;
    use crate::{
        data::common::RiskMetricsConfig,
        report::{
            io::ToSchema, portfolio_performance::PortfolioPerformanceCol,
            trade_statistics::TradeStatCol,
        },
    };
    use std::path::PathBuf;

    #[test]
    fn test_to_partitions_logic() {
        // 1. Setup Data: Minimal Journal with Symbol and Dates
        // We create 4 rows:
        // - AAPL (2025)
        // - AAPL (2025)
        // - AAPL (2026)
        // - MSFT (2025)
        let df = df![
            "symbol" => &["AAPL", "AAPL", "AAPL", "MSFT"],
            "entry_timestamp" => &[
                "2025-01-01T12:00:00Z",
                "2025-06-01T12:00:00Z",
                "2026-01-01T12:00:00Z",
                "2025-01-01T12:00:00Z"
            ]
        ]
        .expect("Failed to create mock DF");

        // Cast strings to strictly typed Datetime for the Journal logic to work
        let df = df
            .lazy()
            .with_column(col("entry_timestamp").str().to_datetime(
                Some(TimeUnit::Microseconds),
                Some(TimeZone::UTC),
                StrptimeOptions::default(),
                lit("raise"),
            ))
            .collect()
            .expect("Failed to cast dates");

        let journal =
            Journal::new(df, RiskMetricsConfig::default()).expect("Failed to instantiate Journal");

        // 2. Create Grouped Journal
        // Grouping by Symbol AND EntryYear
        let grouped = journal.group_by([GroupCol::Symbol, GroupCol::EntryYear]);

        // 3. Execute `to_partitions`
        let (partitions, keys) = grouped.to_partitions().expect("to_partitions failed");

        // === Assertions ===

        // A. Verify Keys Returned
        assert_eq!(
            keys,
            vec![GroupCol::Symbol, GroupCol::EntryYear],
            "Should return the strictly named group keys"
        );

        // B. Verify Partition Count
        // Expected groups: (AAPL, 2025), (AAPL, 2026), (MSFT, 2025) -> 3 groups
        assert_eq!(partitions.len(), 3, "Should result in exactly 3 partitions");

        // C. Verify Partition Content
        let mut partition_summary = partitions
            .iter()
            .map(|df| {
                let symbol = df
                    .column(GroupCol::Symbol.as_str())
                    .unwrap()
                    .str()
                    .unwrap()
                    .get(0)
                    .unwrap()
                    .to_string();
                let year = df
                    .column(GroupCol::EntryYear.as_str())
                    .unwrap()
                    .i32()
                    .unwrap()
                    .get(0)
                    .unwrap();
                let height = df.height();
                (symbol, year, height)
            })
            .collect::<Vec<(_, _, _)>>();

        partition_summary.sort();

        assert_eq!(
            partition_summary,
            vec![
                ("AAPL".to_string(), 2025, 2),
                ("AAPL".to_string(), 2026, 1),
                ("MSFT".to_string(), 2025, 1),
            ]
        );
    }

    #[test]
    fn test_journal_grouping_logic() {
        // ========================================================================
        // 1. Load Test Data
        // ========================================================================
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let fixture_path =
            PathBuf::from(manifest_dir).join("tests/fixtures/report/input/journal.csv");

        assert!(
            fixture_path.exists(),
            "Test fixture missing: {}",
            fixture_path.display()
        );

        // Load with strict schema enforcement
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

        let journal =
            Journal::new(df, RiskMetricsConfig::default()).expect("Failed to create Journal");

        // ========================================================================
        // 2. Group by Symbol + Entry Year
        // ========================================================================
        // Expected groups based on fixture:
        // - BTC/2025: 3 rows (rows 1, 2, 3)
        // - ETH/2026: 2 rows (rows 4, 5)
        // - BTC/2026: 1 row  (row 6)
        // Total: 3 groups, 6 rows
        let grouped = journal.group_by([GroupCol::Symbol, GroupCol::EntryYear]);

        // ========================================================================
        // 3. Test Trade Statistics (Aggregation: N -> 1 per group)
        // ========================================================================
        let stats = grouped
            .trade_stats()
            .expect("Failed to calculate trade statistics");
        let df_stats = stats.as_df();

        // Should have exactly 1 row per group
        assert_eq!(df_stats.height(), 3, "Expected 3 stat rows (1 per group)");

        // Verify BTC/2025 aggregation (3 trades: -1000, -500, 0 = -1500 total)
        let btc_2025 = filter_group(&df_stats, "btc-usdt", 2025);
        assert_eq!(btc_2025.height(), 1, "BTC/2025 should have 1 stat row");

        let trade_count = btc_2025
            .column(TradeStatCol::TotalTradeCount.as_str())
            .unwrap()
            .u32()
            .unwrap()
            .get(0)
            .unwrap();
        assert_eq!(trade_count, 3, "BTC/2025 should aggregate 3 trades");

        // ========================================================================
        // 4. Test Portfolio Performance (Aggregation: N -> 1 per group)
        // ========================================================================
        let perf = grouped
            .portfolio_performance()
            .expect("Failed to calculate portfolio performance");
        let df_perf = perf.as_df();

        assert_eq!(df_perf.height(), 3, "Expected 3 perf rows (1 per group)");

        // Verify ETH/2026 net profit (2000 + 500 = 2500)
        let eth_2026 = filter_group(&df_perf, "eth-usdt", 2026);
        assert_eq!(eth_2026.height(), 1, "ETH/2026 should have 1 perf row");

        let net_profit = eth_2026
            .column(PortfolioPerformanceCol::NetProfit.as_str())
            .unwrap()
            .f64()
            .unwrap()
            .get(0)
            .unwrap();
        assert_eq!(net_profit, 2500.0, "ETH/2026 net profit should be 2500");

        // ========================================================================
        // 5. Test Cumulative Returns (Transformation: N -> N, no aggregation)
        // ========================================================================
        let cum_ret = grouped
            .cumulative_returns()
            .expect("Failed to calculate cumulative returns");
        let df_cum = cum_ret.as_df();

        // CRITICAL: Must preserve all original rows
        assert_eq!(
            df_cum.height(),
            6,
            "Cumulative returns must preserve all 6 rows"
        );

        // Verify group columns are materialized
        assert!(
            df_cum.column(GroupCol::Symbol.as_str()).is_ok(),
            "Missing group column: {}",
            GroupCol::Symbol
        );
        assert!(
            df_cum.column(GroupCol::EntryYear.as_str()).is_ok(),
            "Missing group column: {}",
            GroupCol::EntryYear
        );

        // Verify calculated metric exists
        assert!(
            df_cum.column("peak_cumulative_return_usd").is_ok(),
            "Missing calculated metric: peak_cumulative_return_usd"
        );

        // Verify group integrity: BTC/2025 should still have 3 rows
        let btc_2025_cum = filter_group(&df_cum, "btc-usdt", 2025);
        assert_eq!(
            btc_2025_cum.height(),
            3,
            "BTC/2025 should retain 3 rows after transformation"
        );

        // Verify row-level calculation occurred (non-null values)
        let peak_values = btc_2025_cum
            .column("peak_cumulative_return_usd")
            .unwrap()
            .f64()
            .unwrap();
        let non_null_count = peak_values.iter().filter(|v| v.is_some()).count();
        assert_eq!(
            non_null_count, 3,
            "All BTC/2025 rows should have calculated metrics"
        );
    }

    // ========================================================================
    // Helper Function
    // ========================================================================

    /// Filters DataFrame to a specific (symbol, year) group.
    fn filter_group(df: &DataFrame, symbol: &str, year: i32) -> DataFrame {
        df.clone()
            .lazy()
            .filter(
                col(GroupCol::Symbol.as_str())
                    .eq(lit(symbol))
                    .and(col(GroupCol::EntryYear.as_str()).eq(lit(year))),
            )
            .collect()
            .expect("Group filter failed")
    }
}
