use polars::prelude::{Expr, IntoLazy, LazyGroupBy, PlSmallStr, col};
use strum::{Display, EnumIter, EnumString, IntoStaticStr};

use crate::{
    error::ChapatyResult,
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

impl GroupedJournal<'_> {
    /// Access raw Polars lazy API for custom queries.
    ///
    /// Groups come out in order of first appearance, and rows keep the
    /// journal order (by entry time) within each group.
    #[must_use]
    pub fn lazy(&self) -> LazyGroupBy {
        self.journal
            .as_df()
            .clone()
            .lazy()
            .group_by_stable(self.key_exprs())
    }

    /// Computes grouped cumulative-return reports.
    ///
    /// # Errors
    /// Returns an error if grouped conversion to cumulative returns fails.
    pub fn cumulative_returns(&self) -> ChapatyResult<CumulativeReturns> {
        self.try_into()
    }

    /// Computes grouped portfolio-performance reports.
    ///
    /// # Errors
    /// Returns an error if grouped conversion to portfolio performance fails.
    pub fn portfolio_performance(&self) -> ChapatyResult<PortfolioPerformance> {
        self.try_into()
    }

    /// Computes grouped trade-statistics reports.
    ///
    /// # Errors
    /// Returns an error if grouped conversion to trade statistics fails.
    pub fn trade_stats(&self) -> ChapatyResult<TradeStatistics> {
        self.try_into()
    }

    #[must_use]
    pub const fn source(&self) -> &Journal {
        self.journal
    }

    #[must_use]
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

    /// The group keys as expressions, virtual time columns included.
    pub(crate) fn key_exprs(&self) -> Vec<Expr> {
        self.group_keys.iter().map(GroupCol::as_expr).collect()
    }
}

/// Represents the subset of columns valid for grouping operations.
///
/// This strictly enforces that users cannot group by continuous variables
/// (like Price or `PnL`) or unique identifiers (like `RowId`), preventing
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
            GroupCol::EpisodeId => Self::EpisodeId,
            GroupCol::TradeState => Self::TradeState,
            GroupCol::AgentId => Self::AgentId,
            GroupCol::AgentUid => Self::AgentUid,

            // === Market spec ===
            GroupCol::DataBroker => Self::DataBroker,
            GroupCol::Exchange => Self::Exchange,
            GroupCol::Symbol => Self::Symbol,
            GroupCol::MarketKind => Self::MarketKind,

            // === Trade configuration ===
            GroupCol::TradeKind => Self::TradeKind,

            // === Timestamps (Mapped to parent TS columns) ===
            GroupCol::EntryYear | GroupCol::EntryQuarter | GroupCol::EntryMonth => {
                Self::EntryTimestamp
            }
            GroupCol::ExitYear | GroupCol::ExitQuarter | GroupCol::ExitMonth => Self::ExitTimestamp,

            // === Realized outcomes ===
            GroupCol::ExitReason => Self::ExitReason,
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
    #[must_use]
    pub fn name(&self) -> PlSmallStr {
        (*self).into()
    }

    #[must_use]
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
            | Self::AgentUid
            | Self::DataBroker
            | Self::Exchange
            | Self::Symbol
            | Self::MarketKind
            | Self::TradeKind
            | Self::ExitReason => col(source_col),

            // === Virtual Time Columns ===
            Self::EntryYear | Self::ExitYear => col(source_col).dt().year(),
            Self::EntryQuarter | Self::ExitQuarter => col(source_col).dt().quarter(),
            Self::EntryMonth | Self::ExitMonth => col(source_col).dt().month(),
        };

        expr.alias(*self)
    }
}

#[cfg(test)]
mod tests {
    #![expect(
        clippy::unwrap_used,
        clippy::expect_used,
        reason = "tests assert against known-valid fixtures; unwrap and expect surface failures as panics that fail the test"
    )]
    use std::path::PathBuf;

    use chrono::DateTime;
    use polars::prelude::{
        DataFrame, DataType, LazyCsvReader, LazyFileListReader, PlRefPath, StrptimeOptions,
        TimeUnit, TimeZone, df, len, lit,
    };
    use strum::IntoEnumIterator;

    use super::*;
    use crate::{
        data::common::RiskMetricsConfig,
        report::{
            cumulative_returns::CumulativeReturnCol, io::ToSchema, journal::JournalExprExt,
            portfolio_performance::PortfolioPerformanceCol, trade_statistics::TradeStatCol,
        },
    };

    #[test]
    fn test_grouping_logic() {
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
            Journal::new(&df, RiskMetricsConfig::default()).expect("Failed to instantiate Journal");

        // 2. Create Grouped Journal
        // Grouping by Symbol AND EntryYear
        let grouped = journal.group_by([GroupCol::Symbol, GroupCol::EntryYear]);

        // 3. Execute the grouping: one row per group, with the group height
        let groups = grouped
            .lazy()
            .agg([len().alias("height")])
            .collect()
            .expect("grouping failed");

        // === Assertions ===

        // A. Verify Keys Returned
        assert_eq!(
            grouped.group_criteria(),
            [GroupCol::Symbol, GroupCol::EntryYear],
            "Should return the strictly named group keys"
        );

        // B. Verify Group Count
        // Expected groups: (AAPL, 2025), (AAPL, 2026), (MSFT, 2025) -> 3 groups
        assert_eq!(groups.height(), 3, "Should result in exactly 3 groups");

        // C. Verify Group Content
        let mut partition_summary = (0..groups.height())
            .map(|i| {
                let symbol = groups
                    .column(GroupCol::Symbol.as_str())
                    .unwrap()
                    .str()
                    .unwrap()
                    .get(i)
                    .unwrap()
                    .to_string();
                let year = groups
                    .column(GroupCol::EntryYear.as_str())
                    .unwrap()
                    .i32()
                    .unwrap()
                    .get(i)
                    .unwrap();
                let height = groups
                    .column("height")
                    .unwrap()
                    .u32()
                    .unwrap()
                    .get(i)
                    .unwrap();
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

        let journal =
            Journal::new(&df, RiskMetricsConfig::default()).expect("Failed to create Journal");

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
        let btc_2025 = filter_group(df_stats, "btc-usdt", 2025);
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
        let eth_2026 = filter_group(df_perf, "eth-usdt", 2026);
        assert_eq!(eth_2026.height(), 1, "ETH/2026 should have 1 perf row");

        let net_profit = eth_2026
            .column(PortfolioPerformanceCol::NetProfit.as_str())
            .unwrap()
            .f64()
            .unwrap()
            .get(0)
            .unwrap();
        assert_f64_eq!(net_profit, 2500.0, "ETH/2026 net profit should be 2500");

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
        let btc_2025_cum = filter_group(df_cum, "btc-usdt", 2025);
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
        let non_null_count = peak_values.iter().flatten().count();
        assert_eq!(
            non_null_count, 3,
            "All BTC/2025 rows should have calculated metrics"
        );
    }

    // ========================================================================
    // Parity: grouped reports equal the ungrouped report of each group
    // ========================================================================

    /// Key sets covering real columns, virtual time columns, a key that is
    /// null for some rows (`ExitReason`), groups without executed trades
    /// (`TradeState`), and the grid agent uid.
    fn parity_key_sets() -> Vec<Vec<GroupCol>> {
        vec![
            vec![GroupCol::Symbol, GroupCol::EntryYear],
            vec![GroupCol::AgentUid],
            vec![GroupCol::AgentId, GroupCol::ExitMonth],
            vec![GroupCol::TradeState],
            vec![GroupCol::ExitReason],
        ]
    }

    #[test]
    fn test_grouped_portfolio_performance_matches_each_group() {
        let journal = synthetic_journal(400);
        for keys in parity_key_sets() {
            let grouped = journal
                .group_by(keys.clone())
                .portfolio_performance()
                .unwrap();
            let metrics: Vec<&str> = PortfolioPerformanceCol::iter()
                .map(|c| c.as_str())
                .collect();

            let groups = journal_per_group(&journal, &keys);
            assert_eq!(grouped.as_df().height(), groups.len(), "keys {keys:?}");

            for (i, (key_row, group_journal)) in groups.iter().enumerate() {
                let expected = group_journal.portfolio_performance().unwrap();
                let actual = grouped.as_df().slice(i64::try_from(i).unwrap(), 1);
                assert_same_keys(&actual, key_row, &keys);
                assert_same_columns(&actual, expected.as_df(), &metrics, &keys);
            }
        }
    }

    #[test]
    fn test_grouped_trade_stats_matches_each_group() {
        let journal = synthetic_journal(400);
        for keys in parity_key_sets() {
            let grouped = journal.group_by(keys.clone()).trade_stats().unwrap();
            let metrics: Vec<&str> = TradeStatCol::iter().map(|c| c.as_str()).collect();

            let groups = journal_per_group(&journal, &keys);
            assert_eq!(grouped.as_df().height(), groups.len(), "keys {keys:?}");

            for (i, (key_row, group_journal)) in groups.iter().enumerate() {
                let expected = group_journal.trade_stats().unwrap();
                let actual = grouped.as_df().slice(i64::try_from(i).unwrap(), 1);
                assert_same_keys(&actual, key_row, &keys);
                assert_same_columns(&actual, expected.as_df(), &metrics, &keys);
            }
        }
    }

    #[test]
    fn test_grouped_cumulative_returns_matches_each_group() {
        let journal = synthetic_journal(400);
        for keys in parity_key_sets() {
            let grouped = journal.group_by(keys.clone()).cumulative_returns().unwrap();
            let columns: Vec<&str> = CumulativeReturnCol::iter().map(|c| c.as_str()).collect();

            // Cumulative returns only cover executed trades. Groups appear in
            // the order of their first executed trade, and groups without
            // executed trades have no rows. Walk the grouped output with a
            // running offset.
            let executed = executed_only(&journal);
            let mut offset = 0;
            for (key_row, group_journal) in journal_per_group(&executed, &keys) {
                let expected = group_journal.cumulative_returns().unwrap();
                let height = expected.as_df().height();
                let actual = grouped
                    .as_df()
                    .slice(i64::try_from(offset).unwrap(), height);
                for row in 0..height {
                    let row = i64::try_from(row).unwrap();
                    assert_same_keys(&actual.slice(row, 1), &key_row, &keys);
                }
                assert_same_columns(&actual, expected.as_df(), &columns, &keys);
                offset += height;
            }
            assert_eq!(grouped.as_df().height(), offset, "keys {keys:?}");
        }
    }

    /// Splits the journal into one journal per group, in order of first
    /// appearance, together with the group's key values.
    fn journal_per_group(journal: &Journal, keys: &[GroupCol]) -> Vec<(DataFrame, Journal)> {
        let key_names: Vec<&str> = keys.iter().map(GroupCol::as_str).collect();
        let enriched = journal
            .as_df()
            .clone()
            .lazy()
            .with_columns(keys.iter().map(GroupCol::as_expr).collect::<Vec<_>>())
            .collect()
            .unwrap();

        enriched
            .partition_by_stable(key_names.clone(), true)
            .unwrap()
            .into_iter()
            .map(|part| {
                let key_row = part.select(key_names.clone()).unwrap().head(Some(1));
                let rows = part.drop_many(key_names.clone());
                let group_journal = Journal::new(&rows, journal.risk_metrics_config()).unwrap();
                (key_row, group_journal)
            })
            .collect()
    }

    fn assert_same_keys(actual: &DataFrame, key_row: &DataFrame, keys: &[GroupCol]) {
        let names: Vec<&str> = keys.iter().map(GroupCol::as_str).collect();
        let actual_keys = actual.select(names).unwrap();
        assert!(
            actual_keys.equals_missing(key_row),
            "keys {keys:?}: group key mismatch\nactual: {actual_keys}\nexpected: {key_row}"
        );
    }

    fn assert_same_columns(
        actual: &DataFrame,
        expected: &DataFrame,
        columns: &[&str],
        keys: &[GroupCol],
    ) {
        assert_eq!(
            actual.height(),
            expected.height(),
            "keys {keys:?}: row count"
        );
        for name in columns {
            let a = actual.column(name).unwrap().as_materialized_series();
            let e = expected.column(name).unwrap().as_materialized_series();
            assert_eq!(a.dtype(), e.dtype(), "keys {keys:?}: dtype of {name}");

            // Grouped kernels may sum in a different order, so floats can
            // differ in the last bits. Everything else must match exactly.
            if a.dtype().is_float() {
                let a = a.f64().unwrap();
                let e = e.f64().unwrap();
                for (x, y) in a.iter().zip(e.iter()) {
                    let same = match (x, y) {
                        (None, None) => true,
                        (Some(x), Some(y))
                            if x.to_bits() == y.to_bits() || (x.is_nan() && y.is_nan()) =>
                        {
                            true
                        }
                        (Some(x), Some(y)) => {
                            (x - y).abs() <= 1e-12 * x.abs().max(y.abs()).max(1.0)
                        }
                        _ => false,
                    };
                    assert!(same, "keys {keys:?}: {name} differs: {x:?} vs {y:?}");
                }
            } else {
                assert!(
                    a.equals_missing(e),
                    "keys {keys:?}: {name} differs\nactual: {a}\nexpected: {e}"
                );
            }
        }
    }

    /// The journal reduced to executed trades (active or closed).
    fn executed_only(journal: &Journal) -> Journal {
        let df = journal
            .as_df()
            .clone()
            .lazy()
            .filter(col(JournalCol::TradeState).trade_executed())
            .collect()
            .unwrap();
        Journal::new(&df, journal.risk_metrics_config()).unwrap()
    }

    /// A deterministic journal with every trade state, several agents, uids,
    /// symbols and two years of trades. Entry times are strictly increasing,
    /// so the order within every group is unambiguous.
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_precision_loss,
        clippy::cast_possible_wrap,
        clippy::too_many_lines,
        reason = "test generator maps small pseudo-random integers onto fixture values and builds every journal column in one place"
    )]
    fn synthetic_journal(rows: usize) -> Journal {
        // Small linear congruential generator, so the data is the same on
        // every run without a rand dependency in the test.
        let mut state: u64 = 0x2545_F491_4F6C_DD1D;
        let mut next = move |modulo: u64| -> u64 {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (state >> 33) % modulo
        };

        let start = DateTime::parse_from_rfc3339("2025-01-01T00:00:00Z")
            .unwrap()
            .timestamp_micros();
        let hour = 3_600_000_000_i64;

        let mut row_id = Vec::new();
        let mut episode_id = Vec::new();
        let mut trade_id = Vec::new();
        let mut trade_state = Vec::new();
        let mut agent_id = Vec::new();
        let mut uid = Vec::new();
        let mut symbol = Vec::new();
        let mut trade_kind = Vec::new();
        let mut entry_price = Vec::new();
        let mut quantity = Vec::new();
        let mut entry_ts = Vec::new();
        let mut exit_ts: Vec<Option<i64>> = Vec::new();
        let mut exit_price: Vec<Option<f64>> = Vec::new();
        let mut exit_reason: Vec<Option<&str>> = Vec::new();
        let mut return_ticks = Vec::new();
        let mut return_usd = Vec::new();

        let mut entry = start;
        for i in 0..rows {
            entry += hour * (1 + next(72) as i64);
            let state_kind = match next(20) {
                0..12 => "closed",
                12..15 => "active",
                15..18 => "pending",
                _ => "canceled",
            };
            let ticks = match state_kind {
                "closed" | "active" => next(2001) as i64 - 1000,
                _ => 0,
            };

            row_id.push(i as u32);
            episode_id.push((i / 5) as u32);
            trade_id.push(i as i64);
            trade_state.push(state_kind);
            agent_id.push(if next(2) == 0 { "agent_a" } else { "agent_b" });
            uid.push(next(4));
            symbol.push(if next(2) == 0 { "btc-usdt" } else { "eth-usdt" });
            trade_kind.push(if next(2) == 0 { "long" } else { "short" });
            entry_price.push(100.0 + next(50) as f64);
            quantity.push(1.0 + next(5) as f64);
            entry_ts.push(entry);
            return_ticks.push(ticks);
            return_usd.push(ticks as f64 * 0.5);

            match state_kind {
                "closed" => {
                    exit_ts.push(Some(entry + hour * (1 + next(48) as i64)));
                    exit_price.push(Some(100.0 + next(50) as f64));
                    exit_reason.push(Some(if ticks > 0 {
                        "take_profit"
                    } else {
                        "stop_loss"
                    }));
                }
                "canceled" => {
                    exit_ts.push(Some(entry + hour));
                    exit_price.push(None);
                    exit_reason.push(Some("canceled"));
                }
                _ => {
                    exit_ts.push(None);
                    exit_price.push(None);
                    exit_reason.push(None);
                }
            }
        }

        let no_floats: Vec<Option<f64>> = vec![None; rows];
        let no_ints: Vec<Option<i64>> = vec![None; rows];
        let df = df![
            JournalCol::RowId.as_str() => row_id,
            JournalCol::EpisodeId.as_str() => episode_id,
            JournalCol::TradeId.as_str() => trade_id,
            JournalCol::TradeState.as_str() => trade_state,
            JournalCol::AgentId.as_str() => agent_id,
            JournalCol::AgentUid.as_str() => uid,
            JournalCol::DataBroker.as_str() => vec!["binance"; rows],
            JournalCol::Exchange.as_str() => vec!["binance"; rows],
            JournalCol::Symbol.as_str() => symbol,
            JournalCol::MarketKind.as_str() => vec!["spot"; rows],
            JournalCol::TradeKind.as_str() => trade_kind,
            JournalCol::EntryPrice.as_str() => entry_price,
            JournalCol::StopLossPrice.as_str() => no_floats.clone(),
            JournalCol::TakeProfitPrice.as_str() => no_floats.clone(),
            JournalCol::Quantity.as_str() => quantity,
            JournalCol::ExpectedLossInTicks.as_str() => no_ints.clone(),
            JournalCol::ExpectedProfitInTicks.as_str() => no_ints,
            JournalCol::ExpectedLossDollars.as_str() => no_floats.clone(),
            JournalCol::ExpectedProfitDollars.as_str() => no_floats.clone(),
            JournalCol::RiskRewardRatio.as_str() => no_floats,
            JournalCol::EntryTimestamp.as_str() => entry_ts,
            JournalCol::ExitTimestamp.as_str() => exit_ts,
            JournalCol::ExitPrice.as_str() => exit_price,
            JournalCol::ExitReason.as_str() => exit_reason,
            JournalCol::RealizedReturnInTicks.as_str() => return_ticks,
            JournalCol::RealizedReturnDollars.as_str() => return_usd,
        ]
        .unwrap();

        let utc_micros = DataType::Datetime(TimeUnit::Microseconds, Some(TimeZone::UTC));
        let df = df
            .lazy()
            .with_columns([
                col(JournalCol::EntryTimestamp).cast(utc_micros.clone()),
                col(JournalCol::ExitTimestamp).cast(utc_micros),
            ])
            .collect()
            .unwrap();

        Journal::new(&df, RiskMetricsConfig::default()).unwrap()
    }

    // ========================================================================
    // Helper Function
    // ========================================================================

    /// Filters `DataFrame` to a specific (symbol, year) group.
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
