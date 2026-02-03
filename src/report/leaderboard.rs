use std::{
    cmp::{Ordering, Reverse},
    collections::{BinaryHeap, HashMap, HashSet},
    sync::Arc,
};

use ordered_float::OrderedFloat;
use polars::{
    df,
    frame::DataFrame,
    prelude::{DataType, Field, PlSmallStr, Schema, SchemaRef},
};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use strum::{Display, EnumCount, EnumIter, EnumString, IntoEnumIterator, IntoStaticStr};

use crate::{
    error::{ChapatyError, ChapatyResult, DataError, IoError, SystemError},
    report::{
        io::{Report, ReportName, ToSchema, generate_dynamic_base_name},
        portfolio_performance::PortfolioPerformanceCol,
    },
    sorted_vec_map::SortedVecMap,
};

const METRIC_COUNT: usize = PortfolioPerformanceCol::COUNT;

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
    EnumCount,
    IntoStaticStr,
)]
#[strum(serialize_all = "snake_case")]
pub enum LeaderboardCol {
    PortfolioPerformanceMetric,
    Rank,
    Value,
    AgentUid,
    AgentParameterization,
}

impl From<LeaderboardCol> for PlSmallStr {
    fn from(value: LeaderboardCol) -> Self {
        value.as_str().into()
    }
}

impl LeaderboardCol {
    pub fn name(&self) -> PlSmallStr {
        (*self).into()
    }

    pub fn as_str(&self) -> &'static str {
        self.into()
    }
}

impl ToSchema for Leaderboard {
    fn to_schema() -> SchemaRef {
        let fields: Vec<Field> = LeaderboardCol::iter()
            .map(|col| {
                let dtype = match col {
                    LeaderboardCol::Rank => DataType::UInt32,
                    LeaderboardCol::AgentUid => DataType::UInt64,

                    LeaderboardCol::AgentParameterization
                    | LeaderboardCol::PortfolioPerformanceMetric => DataType::String,

                    LeaderboardCol::Value => DataType::Float64,
                };
                Field::new(col.into(), dtype)
            })
            .collect();

        Arc::new(Schema::from_iter(fields))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Leaderboard {
    /// A typed report representing the **leaderboard of agents**.
    ///
    /// Each row corresponds to a single agent’s placement for a given portfolio performance
    /// metric. The schema is defined by the [`Leaderboard`] key type, ensuring column consistency.
    ///
    /// # Columns
    ///
    /// - `Metric`: Portfolio performance metric (e.g. `sharpe_ratio`, `net_profit`).
    /// - `Rank`: Position of the agent for the given metric (1 = best).
    /// - `AgentUid`: Unique identifier of the agent.
    /// - `Value`: Numeric value of the metric for this agent.
    /// - `AgentParameterization`: JSON serialization of the agent’s parameterization/configuration.
    ///
    /// # Example Table
    ///
    /// | portfolio_performance_metric | rank | value     | agent_uid  | agent_parameterization                                         |
    /// |------------------------------|------|-----------|------------|----------------------------------------------------------------|
    /// | sharpe_ratio                 | 1    | 2.85      | 45201      | { "wait_duration": 300, "take_profit_risk_factor": 1.0, ... }  |
    /// | sharpe_ratio                 | 2    | 2.81      | 18934      | { "wait_duration": 600, "take_profit_risk_factor": 0.8, ... }  |
    /// | net_profit                   | 1    | 150234.60 | 78103      | { "wait_duration": 120, "take_profit_risk_factor": 1.2, ... }  |
    /// | net_profit                   | 2    | 149875.10 | 45201      | { "wait_duration": 300, "take_profit_risk_factor": 1.0, ... }  |
    ///
    /// This makes it easy to export leaderboard data to external systems (e.g. DataFrames, CSV, or
    /// dashboards) while retaining schema guarantees.
    df: DataFrame,
}

impl ReportName for Leaderboard {
    fn base_name(&self) -> String {
        generate_dynamic_base_name(&self.df, "leaderboard")
    }
}

impl Report for Leaderboard {
    fn as_df(&self) -> &DataFrame {
        &self.df
    }

    fn as_df_mut(&mut self) -> &mut DataFrame {
        &mut self.df
    }
}

// ================================================================================================
// The Accumulator
// ================================================================================================

/// A report tracking the top-k performing agents for each performance metric.
///
/// This report maintains a **min-heap** (`BinaryHeap<Reverse<LeaderboardEntry>>`)  
/// to efficiently track the **top-k best-performing agents** per metric.  
#[derive(Clone, Debug)]
pub(crate) struct AgentLeaderboard<T> {
    /// A mapping from performance metrics to **min-heaps** tracking the top-k performing agents.
    ///
    /// Each entry in the heap is wrapped in `Reverse` to ensure that the smallest (i.e.  
    /// the worst-performing among the top-k) entry is always at the top.
    pub top_per_metric:
        SortedVecMap<PortfolioPerformanceCol, BinaryHeap<Reverse<LeaderboardEntry>>>,

    /// The maximum number of top-performing agent entries to track per metric.
    pub k: usize,

    // Store the JSON params ONLY for agents currently in the Top K
    pub agent_data: HashMap<u64, T>,
}

impl<T> TryFrom<AgentLeaderboard<T>> for Leaderboard
where
    T: Serialize,
{
    type Error = ChapatyError;

    fn try_from(value: AgentLeaderboard<T>) -> Result<Self, Self::Error> {
        Ok(Self {
            df: value.leaderboard_soa()?.try_into()?,
        })
    }
}

impl<T> AgentLeaderboard<T> {
    /// Creates a new, empty `AgentPerformanceReport` with the specified heap capacity `k`.
    ///
    /// # Arguments
    ///
    /// * `k` - The maximum number of top entries to retain for each reward statistic.
    ///
    /// # Returns
    ///
    /// A new instance of `AgentPerformanceReport` with empty internal heaps.
    pub(crate) fn new(k: usize) -> Self {
        Self {
            top_per_metric: PortfolioPerformanceCol::iter()
                .map(|metric| (metric, BinaryHeap::with_capacity(k)))
                .collect(),
            k,
            agent_data: HashMap::with_capacity(k * METRIC_COUNT),
        }
    }

    pub(crate) fn update(&mut self, new_entries: &[LeaderboardEntry], agent: T) {
        let mut is_global_winner = false;
        let mut potentially_evicted = SmallVec::<[u64; METRIC_COUNT]>::new();

        for entry in new_entries {
            match self.process_entry(entry) {
                HeapAction::Rejected => {}
                HeapAction::Added => {
                    is_global_winner = true;
                }
                HeapAction::Swapped(evicted_uid) => {
                    is_global_winner = true;
                    if !potentially_evicted.contains(&evicted_uid) {
                        potentially_evicted.push(evicted_uid);
                    }
                }
            }
        }

        if is_global_winner {
            self.agent_data
                .entry(new_entries[0].agent_uid)
                .or_insert(agent);
        }

        self.garbage_collect(new_entries[0].agent_uid, &potentially_evicted);
    }

    pub(crate) fn merge(mut self, other: Self) -> Self {
        for (metric, other_heap) in other.top_per_metric {
            let heap = self.top_per_metric.get_mut(&metric).expect(
                "Critical Logic Error: Leaderboard metric missing. This should be unreachable.",
            );
            heap.extend(other_heap);
            while heap.len() > self.k {
                heap.pop();
            }
        }

        self.agent_data.extend(other.agent_data);
        let surviving_uids = self.top_per_metric.values().fold(
            HashSet::with_capacity(self.k * METRIC_COUNT),
            |mut set, heap| {
                set.extend(heap.iter().map(|entry| entry.0.agent_uid));
                set
            },
        );
        self.agent_data
            .retain(|uid, _| surviving_uids.contains(uid));

        self
    }
}

impl<T> AgentLeaderboard<T> {
    fn is_agent_tracked(&self, uid: u64) -> bool {
        self.top_per_metric
            .values()
            .any(|heap| heap.iter().any(|entry| entry.0.agent_uid == uid))
    }

    fn process_entry(&mut self, entry: &LeaderboardEntry) -> HeapAction {
        let heap = self
            .top_per_metric
            .get_mut(&entry.metric())
            .expect("Metric missing");

        if heap.len() < self.k {
            heap.push(Reverse(*entry));
            return HeapAction::Added;
        }

        let qualifies = heap.peek().is_none_or(|Reverse(worst)| entry > worst);
        if qualifies && let Some(Reverse(evicted)) = heap.pop() {
            heap.push(Reverse(*entry));
            return HeapAction::Swapped(evicted.agent_uid);
        }

        HeapAction::Rejected
    }

    fn garbage_collect(&mut self, safe_uid: u64, candidates: &[u64]) {
        for &uid in candidates {
            if uid != safe_uid && !self.is_agent_tracked(uid) {
                self.agent_data.remove(&uid);
            }
        }
    }
}

impl<T> AgentLeaderboard<T>
where
    T: Serialize,
{
    fn leaderboard_soa(self) -> ChapatyResult<LeaderboardSoA> {
        let capacity = self.top_per_metric.len() * self.k;
        let mut metric_col = Vec::with_capacity(capacity);
        let mut rank_col = Vec::with_capacity(capacity);
        let mut agent_uid_col = Vec::with_capacity(capacity);
        let mut value_col = Vec::with_capacity(capacity);
        let mut agent_parameterization_col = Vec::with_capacity(capacity);

        for (metric, heap) in self.top_per_metric {
            let top_k = heap.into_sorted_vec().into_iter().map(|rev| rev.0);

            for (i, entry) in top_k.enumerate() {
                let uid = entry.agent_uid();

                let agent = self.agent_data.get(&uid).ok_or_else(|| {
                    SystemError::MissingField(format!("Agent UID {} missing from cache", uid))
                })?;

                let param_str = serde_json::to_string(agent).map_err(IoError::Json)?;

                metric_col.push(metric.to_string());
                rank_col.push((i + 1) as u32);
                agent_uid_col.push(uid);
                value_col.push(entry.denormalized_reward());
                agent_parameterization_col.push(param_str);
            }
        }

        Ok(LeaderboardSoA {
            portofolio_performance_metric: metric_col,
            rank: rank_col,
            value: value_col,
            agent_uid: agent_uid_col,
            agent_parameterization: agent_parameterization_col,
        })
    }
}

enum HeapAction {
    Added,
    Swapped(u64),
    Rejected,
}

struct LeaderboardSoA {
    portofolio_performance_metric: Vec<String>,
    rank: Vec<u32>,
    value: Vec<f64>,
    agent_uid: Vec<u64>,
    agent_parameterization: Vec<String>,
}

impl TryFrom<LeaderboardSoA> for DataFrame {
    type Error = ChapatyError;

    fn try_from(value: LeaderboardSoA) -> Result<Self, Self::Error> {
        df!(
            LeaderboardCol::PortfolioPerformanceMetric.to_string() => value.portofolio_performance_metric,
            LeaderboardCol::Rank.to_string() => value.rank,
            LeaderboardCol::AgentUid.to_string() => value.agent_uid,
            LeaderboardCol::Value.to_string() => value.value,
            LeaderboardCol::AgentParameterization.to_string() => value.agent_parameterization,
        )
        .map_err(|e| DataError::DataFrame(e.to_string()).into())
    }
}

/// Represents a single performance record for an agent.
///
/// This structure wraps an agent along with its corresponding reward statistic and the
/// calculated reward value. The ordering for this structure is defined solely by the `reward`
/// field, meaning that two entries are compared based on their reward value. This makes it
/// convenient for use in a min-heap (by wrapping with `std::cmp::Reverse`) when keeping track
/// of the top-k performers.
#[derive(Copy, Clone, Debug)]
pub struct LeaderboardEntry {
    /// The agent associated with this performance record.
    pub agent_uid: u64,
    /// The reward metric corresponding to this entry.
    pub metric: PortfolioPerformanceCol,
    /// The reward value calculated for the agent.
    pub reward: OrderedFloat<f64>,
}

impl LeaderboardEntry {
    /// Returns the unique identifier of the agent associated with this entry.
    pub fn agent_uid(&self) -> u64 {
        self.agent_uid
    }

    /// Returns the performance metric used for ranking this entry
    /// (e.g. Sharpe ratio, net profit, drawdown).
    pub fn metric(&self) -> PortfolioPerformanceCol {
        self.metric
    }

    /// Returns the **normalized reward** used internally for heap ordering.
    ///
    /// Certain metrics (e.g. drawdowns, errors) are minimized, so they are
    /// transformed into a score where *larger is always better*. This value
    /// is what the leaderboard compares to decide ordering.
    pub fn normalized_reward(&self) -> f64 {
        self.reward.0
    }

    /// Returns the **denormalized reward**, i.e. the original metric value.
    ///
    /// This is the human-facing score as reported in evaluation tables
    /// (e.g. Sharpe ratio `2.85`, net profit `150_234.60`).
    /// Use this when displaying results instead of the normalized value.
    pub fn denormalized_reward(&self) -> f64 {
        self.metric.from_heap_score(self.normalized_reward())
    }
}

impl PartialEq for LeaderboardEntry {
    fn eq(&self, other: &Self) -> bool {
        self.reward == other.reward
    }
}

impl Eq for LeaderboardEntry {}

impl PartialOrd for LeaderboardEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for LeaderboardEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.reward.cmp(&other.reward)
    }
}

#[cfg(test)]
mod tests {
    use polars::prelude::{IntoLazy, col, lit};
    use serde::{Deserialize, Serialize};

    use super::*;

    /// A minimal test agent struct that implements Serialize.
    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct TestAgent {
        id: u64,
    }

    impl TestAgent {
        fn new(id: u64) -> Self {
            Self { id }
        }
    }

    /// Helper to create a LeaderboardEntry for a given metric.
    fn make_entry(
        agent_uid: u64,
        metric: PortfolioPerformanceCol,
        reward: f64,
    ) -> LeaderboardEntry {
        LeaderboardEntry {
            agent_uid,
            metric,
            reward: OrderedFloat(reward),
        }
    }

    // ============================================================================================
    // 1. Initialization
    // ============================================================================================

    #[test]
    fn test_leaderboard_initializes_all_metrics() {
        // Arrange
        let k = 5;

        // Act
        let leaderboard = AgentLeaderboard::<TestAgent>::new(k);

        // Assert
        for metric in PortfolioPerformanceCol::iter() {
            assert!(
                leaderboard.top_per_metric.contains_key(&metric),
                "Leaderboard missing initialization for metric: {:?}",
                metric
            );
        }

        assert_eq!(
            leaderboard.top_per_metric.len(),
            METRIC_COUNT,
            "Leaderboard map length does not match Enum variant count"
        );

        for (_, heap) in leaderboard.top_per_metric.iter() {
            assert!(heap.is_empty(), "New heaps must be empty");
            assert!(heap.capacity() >= k, "Heap capacity should be at least k");
        }
    }

    // ============================================================================================
    // 2. `update` Logic (The Hot Loop)
    // ============================================================================================

    #[test]
    fn test_update_fills_capacity() {
        // Arrange
        let k = 3;
        let mut board = AgentLeaderboard::<TestAgent>::new(k);
        let metric = PortfolioPerformanceCol::SharpeRatio;

        // Act: Insert K entries
        for i in 0..k {
            let uid = i as u64;
            let entry = make_entry(uid, metric, (i + 1) as f64 * 10.0);
            let agent = TestAgent::new(uid);
            board.update(&[entry], agent);
        }

        // Assert: heap is exactly size K
        let heap = board.top_per_metric.get(&metric).unwrap();
        assert_eq!(heap.len(), k, "Heap should be exactly at capacity K");

        // Assert: all agents are cached
        assert_eq!(board.agent_data.len(), k, "All K agents should be cached");
    }

    #[test]
    fn test_update_respects_ordering_and_eviction() {
        // Arrange
        let k = 3;
        let mut board = AgentLeaderboard::<TestAgent>::new(k);
        let metric = PortfolioPerformanceCol::SharpeRatio;

        // Fill with rewards 10.0, 20.0, 30.0 (agents 1, 2, 3)
        for i in 1..=k {
            let uid = i as u64;
            let entry = make_entry(uid, metric, i as f64 * 10.0);
            board.update(&[entry], TestAgent::new(uid));
        }

        // Act 1: Attempt to insert a WORSE agent (reward = 5.0)
        let worse_entry = make_entry(100, metric, 5.0);
        board.update(&[worse_entry], TestAgent::new(100));

        // Assert 1: Worse agent rejected
        let heap = board.top_per_metric.get(&metric).unwrap();
        assert_eq!(heap.len(), k, "Heap size should remain unchanged");

        // Verify agent 100 is NOT in the heap
        let uids = heap.iter().map(|r| r.0.agent_uid).collect::<Vec<_>>();
        assert!(
            !uids.contains(&100),
            "Worse agent should not be in the heap"
        );

        // Verify agent 100 is NOT in the cache
        assert!(
            !board.agent_data.contains_key(&100),
            "Worse agent should not be cached"
        );

        // Act 2: Attempt to insert a BETTER agent (reward = 100.0)
        let better_entry = make_entry(200, metric, 100.0);
        board.update(&[better_entry], TestAgent::new(200));

        // Assert 2: Better agent accepted, worst evicted
        let heap = board.top_per_metric.get(&metric).unwrap();
        assert_eq!(heap.len(), k, "Heap size should remain at K");

        // Agent 200 should be present
        let uids = heap.iter().map(|r| r.0.agent_uid).collect::<Vec<_>>();
        assert!(uids.contains(&200), "Better agent should be in the heap");

        // Agent 1 (the worst, reward=10.0) should be evicted
        assert!(
            !uids.contains(&1),
            "Worst agent should be evicted after better insertion"
        );

        // Agent 1 should be garbage collected from cache
        assert!(
            !board.agent_data.contains_key(&1),
            "Evicted agent should be garbage collected"
        );

        // Agent 200 should be cached
        assert!(
            board.agent_data.contains_key(&200),
            "New winner should be cached"
        );
    }

    #[test]
    fn test_update_rejects_ties() {
        // Arrange
        let k = 2;
        let mut board = AgentLeaderboard::<TestAgent>::new(k);
        let metric = PortfolioPerformanceCol::SharpeRatio;

        // Fill heap: rewards 10.0 and 20.0
        board.update(&[make_entry(1, metric, 10.0)], TestAgent::new(1));
        board.update(&[make_entry(2, metric, 20.0)], TestAgent::new(2));

        // Act: Insert agent with reward EQUAL to worst (10.0)
        let tie_entry = make_entry(999, metric, 10.0);
        board.update(&[tie_entry], TestAgent::new(999));

        // Assert: Tie should be rejected (strict > inequality)
        let heap = board.top_per_metric.get(&metric).unwrap();
        let uids = heap.iter().map(|r| r.0.agent_uid).collect::<Vec<_>>();
        assert!(!uids.contains(&999), "Tied agent should not be in the heap");
        assert_eq!(heap.len(), k, "Heap size should remain unchanged");
        assert!(
            !board.agent_data.contains_key(&999),
            "Tied agent should not be cached"
        );
    }

    // ============================================================================================
    // 3. `merge` Logic (The Reduction Step)
    // ============================================================================================

    #[test]
    fn test_merge_correctness() {
        // Arrange
        let k = 3;
        let metric = PortfolioPerformanceCol::SharpeRatio;

        // Board A: agents with rewards 10, 20, 30
        let mut board_a = AgentLeaderboard::<TestAgent>::new(k);
        for i in 1..=3u64 {
            let entry = make_entry(i, metric, i as f64 * 10.0);
            board_a.update(&[entry], TestAgent::new(i));
        }

        // Board B: agents with rewards 25, 35, 45
        let mut board_b = AgentLeaderboard::<TestAgent>::new(k);
        for i in 1..=3u64 {
            let uid = 100 + i;
            let entry = make_entry(uid, metric, (i as f64 * 10.0) + 15.0);
            board_b.update(&[entry], TestAgent::new(uid));
        }

        // Act
        let merged = board_a.merge(board_b);

        // Assert: Global top-3 should be 30, 35, 45 -> agents 3, 102, 103
        let heap = merged.top_per_metric.get(&metric).unwrap();
        assert_eq!(heap.len(), k, "Merged heap should have exactly K entries");

        let uids = heap.iter().map(|r| r.0.agent_uid).collect::<Vec<_>>();
        assert!(uids.contains(&3), "Agent 3 (reward=30) should survive");
        assert!(uids.contains(&102), "Agent 102 (reward=35) should survive");
        assert!(uids.contains(&103), "Agent 103 (reward=45) should survive");

        // Evicted agents should not be present
        assert!(!uids.contains(&1), "Agent 1 (reward=10) should be evicted");
        assert!(!uids.contains(&2), "Agent 2 (reward=20) should be evicted");
        assert!(
            !uids.contains(&101),
            "Agent 101 (reward=25) should be evicted"
        );
    }

    #[test]
    fn test_merge_garbage_collection() {
        // Arrange
        let k = 2;
        let metric = PortfolioPerformanceCol::SharpeRatio;

        // Board A: agents 1 and 2 with rewards 10.0, 20.0
        let mut board_a = AgentLeaderboard::<TestAgent>::new(k);
        board_a.update(&[make_entry(1, metric, 10.0)], TestAgent::new(1));
        board_a.update(&[make_entry(2, metric, 20.0)], TestAgent::new(2));

        // Board B: agents 101 and 102 with BETTER rewards 100.0, 200.0
        let mut board_b = AgentLeaderboard::<TestAgent>::new(k);
        board_b.update(&[make_entry(101, metric, 100.0)], TestAgent::new(101));
        board_b.update(&[make_entry(102, metric, 200.0)], TestAgent::new(102));

        // Act
        let merged = board_a.merge(board_b);

        // Assert: Only agents 101 and 102 should survive (global top-2)
        let heap = merged.top_per_metric.get(&metric).unwrap();
        let surviving_uids = heap.iter().map(|r| r.0.agent_uid).collect::<Vec<_>>();
        assert!(surviving_uids.contains(&101));
        assert!(surviving_uids.contains(&102));
        assert!(!surviving_uids.contains(&1));
        assert!(!surviving_uids.contains(&2));

        // CRUCIAL: Evicted agents' data should be garbage collected
        assert!(
            !merged.agent_data.contains_key(&1),
            "Agent 1's data should be garbage collected"
        );
        assert!(
            !merged.agent_data.contains_key(&2),
            "Agent 2's data should be garbage collected"
        );

        // Surviving agents' data should still exist
        assert!(
            merged.agent_data.contains_key(&101),
            "Agent 101's data should survive"
        );
        assert!(
            merged.agent_data.contains_key(&102),
            "Agent 102's data should survive"
        );

        // Verify the correct number of cached agents
        assert_eq!(
            merged.agent_data.len(),
            k,
            "Only K agent data entries should remain"
        );
    }

    // ============================================================================================
    // 4. Data Structure & Conversion (`LeaderboardSoA`)
    // ============================================================================================

    #[test]
    fn test_into_dataframe_schema_and_sorting() {
        // Arrange
        let k = 3;
        let metric = PortfolioPerformanceCol::SharpeRatio;

        let mut board = AgentLeaderboard::<TestAgent>::new(k);

        // Insert agents with known rewards (out of order to test sorting)
        let entries = [
            (42u64, 50.0),
            (17u64, 100.0), // Best
            (99u64, 75.0),
        ];

        for (uid, reward) in entries {
            board.update(&[make_entry(uid, metric, reward)], TestAgent::new(uid));
        }

        // Act
        let leaderboard: Leaderboard = board.try_into().expect("Conversion should succeed");
        let df = leaderboard.as_df();

        // Filter to our test metric only using Polars lazy API
        let metric_str = metric.to_string();
        let filtered = df
            .clone()
            .lazy()
            .filter(col(LeaderboardCol::PortfolioPerformanceMetric).eq(lit(metric_str)))
            .collect()
            .unwrap();

        // Assert: Correct number of rows for this metric
        assert_eq!(filtered.height(), k, "Should have K rows for the metric");

        // Assert: Rank column is strictly 1, 2, 3
        let ranks = filtered
            .column(LeaderboardCol::Rank.as_str())
            .unwrap()
            .u32()
            .unwrap()
            .into_no_null_iter()
            .collect::<Vec<_>>();
        assert_eq!(ranks, vec![1, 2, 3], "Ranks should be sequential 1, 2, 3");

        // Assert: Rank 1 corresponds to highest reward (agent 17, reward=100.0)
        let values = filtered
            .column(LeaderboardCol::Value.as_str())
            .unwrap()
            .f64()
            .unwrap()
            .into_no_null_iter()
            .collect::<Vec<_>>();
        assert_eq!(
            values[0], 100.0,
            "Rank 1 should have the highest reward value"
        );
        assert!(
            values[0] > values[1] && values[1] > values[2],
            "Values should be in descending order"
        );

        // Assert: Agent UIDs match expected ranking
        let uids = filtered
            .column(LeaderboardCol::AgentUid.as_str())
            .unwrap()
            .u64()
            .unwrap()
            .into_no_null_iter()
            .collect::<Vec<_>>();

        // Rank 1 should be agent 17
        assert_eq!(uids[0], 17, "Rank 1 should be agent 17");
        // Rank 2 should be agent 99
        assert_eq!(uids[1], 99, "Rank 2 should be agent 99");
        // Rank 3 should be agent 42
        assert_eq!(uids[2], 42, "Rank 3 should be agent 42");

        // Assert: Agent parameterization is valid JSON matching the UID
        let params = filtered
            .column(LeaderboardCol::AgentParameterization.as_str())
            .unwrap()
            .str()
            .unwrap()
            .into_no_null_iter()
            .collect::<Vec<_>>();

        // Verify JSON can be parsed and contains correct id
        for (i, param_str) in params.iter().enumerate() {
            let parsed: TestAgent = serde_json::from_str(param_str).expect("Should be valid JSON");
            assert_eq!(parsed.id, uids[i], "Serialized agent id should match UID");
        }
    }

    // ============================================================================================
    // 5. Error Handling
    // ============================================================================================

    #[test]
    fn test_soa_generation_missing_data() {
        // Arrange
        let k = 2;
        let metric = PortfolioPerformanceCol::SharpeRatio;

        let mut board = AgentLeaderboard::<TestAgent>::new(k);
        board.update(&[make_entry(1, metric, 10.0)], TestAgent::new(1));
        board.update(&[make_entry(2, metric, 20.0)], TestAgent::new(2));

        // Deliberately remove agent 2's data to simulate corruption
        board.agent_data.remove(&2);

        // Act
        let result = board.try_into() as ChapatyResult<Leaderboard>;

        // Assert
        assert!(result.is_err(), "Should fail when agent data is missing");
        let err = result.unwrap_err();
        match err {
            ChapatyError::System(SystemError::MissingField(msg)) => {
                assert!(
                    msg.contains("2"),
                    "Error should mention the missing UID: {}",
                    msg
                );
            }
            other => panic!("Expected SystemError::MissingField, got: {:?}", other),
        }
    }

    // ============================================================================================
    // 6. Edge Cases
    // ============================================================================================

    #[test]
    fn test_update_with_multiple_metrics() {
        // Arrange
        let k = 2;
        let mut board = AgentLeaderboard::<TestAgent>::new(k);
        let metric_a = PortfolioPerformanceCol::SharpeRatio;
        let metric_b = PortfolioPerformanceCol::NetProfit;

        // Act: Insert entries for different metrics (same agent)
        let entries = vec![
            make_entry(1, metric_a, 10.0),
            make_entry(1, metric_b, 1000.0),
        ];
        board.update(&entries, TestAgent::new(1));

        // Assert: Agent wins in both metrics
        let heap_a = board.top_per_metric.get(&metric_a).unwrap();
        let heap_b = board.top_per_metric.get(&metric_b).unwrap();

        assert_eq!(heap_a.len(), 1);
        assert_eq!(heap_b.len(), 1);
        assert_eq!(heap_a.peek().unwrap().0.agent_uid, 1);
        assert_eq!(heap_b.peek().unwrap().0.agent_uid, 1);

        // Agent should only be cached once
        assert_eq!(board.agent_data.len(), 1);
        assert!(board.agent_data.contains_key(&1));
    }

    #[test]
    fn test_empty_leaderboard_to_dataframe() {
        // Arrange
        let k = 3;
        let board = AgentLeaderboard::<TestAgent>::new(k);

        // Act
        let leaderboard: Leaderboard = board.try_into().expect("Empty conversion should succeed");

        // Assert
        assert_eq!(leaderboard.as_df().height(), 0, "Empty board = empty df");
    }

    #[test]
    fn test_update_garbage_collects_during_eviction() {
        // Arrange
        let k = 1;
        let mut board = AgentLeaderboard::<TestAgent>::new(k);
        let metric = PortfolioPerformanceCol::SharpeRatio;

        // Insert agent 1
        board.update(&[make_entry(1, metric, 10.0)], TestAgent::new(1));
        assert!(board.agent_data.contains_key(&1));

        // Act: Insert better agent 2, which should evict agent 1
        board.update(&[make_entry(2, metric, 20.0)], TestAgent::new(2));

        // Assert: Agent 1 should be garbage collected immediately
        assert!(
            !board.agent_data.contains_key(&1),
            "Evicted agent should be garbage collected during update"
        );
        assert!(
            board.agent_data.contains_key(&2),
            "New winner should be cached"
        );
        assert_eq!(board.agent_data.len(), 1, "Only one agent should be cached");
    }

    #[test]
    fn test_garbage_collection_protects_multi_metric_winners() {
        // Arrange
        let k = 1;
        let mut board = AgentLeaderboard::<TestAgent>::new(k);
        let metric_a = PortfolioPerformanceCol::SharpeRatio;
        let metric_b = PortfolioPerformanceCol::NetProfit;

        // Agent 1 wins in BOTH metrics
        let entries = vec![
            make_entry(1, metric_a, 10.0),
            make_entry(1, metric_b, 100.0),
        ];
        board.update(&entries, TestAgent::new(1));

        // Act: Agent 2 beats Agent 1 ONLY in Metric A
        // Agent 1 should be evicted from A, but STAY in B.
        board.update(&[make_entry(2, metric_a, 20.0)], TestAgent::new(2));

        // Assert
        // 1. Heap A has Agent 2
        let heap_a = board.top_per_metric.get(&metric_a).unwrap();
        assert_eq!(heap_a.peek().unwrap().0.agent_uid, 2);

        // 2. Heap B still has Agent 1
        let heap_b = board.top_per_metric.get(&metric_b).unwrap();
        assert_eq!(heap_b.peek().unwrap().0.agent_uid, 1);

        // 3. CRITICAL: Agent 1's data must NOT be deleted because it holds title in B
        assert!(
            board.agent_data.contains_key(&1),
            "Agent 1 should survive because it leads in metric B"
        );
        assert!(board.agent_data.contains_key(&2), "Agent 2 should be added");
        assert_eq!(board.agent_data.len(), 2);
    }

    #[test]
    fn test_garbage_collection_removes_agent_lost_all_titles() {
        // Arrange
        let k = 1;
        let mut board = AgentLeaderboard::<TestAgent>::new(k);
        let metric_a = PortfolioPerformanceCol::SharpeRatio;
        let metric_b = PortfolioPerformanceCol::NetProfit;

        // Agent 1 wins in BOTH metrics
        let entries = vec![
            make_entry(1, metric_a, 10.0),
            make_entry(1, metric_b, 100.0),
        ];
        board.update(&entries, TestAgent::new(1));

        // Act 1: Agent 2 beats Agent 1 in Metric A
        board.update(&[make_entry(2, metric_a, 20.0)], TestAgent::new(2));

        // Agent 1 is still safe (holds B)
        assert!(board.agent_data.contains_key(&1));

        // Act 2: Agent 3 beats Agent 1 in Metric B
        board.update(&[make_entry(3, metric_b, 200.0)], TestAgent::new(3));

        // Assert: Agent 1 has lost ALL titles. Must be deleted.
        assert!(
            !board.agent_data.contains_key(&1),
            "Agent 1 lost all metrics and should be GC'd"
        );
        assert!(board.agent_data.contains_key(&2));
        assert!(board.agent_data.contains_key(&3));
    }
}
