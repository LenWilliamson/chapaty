use std::{fmt::Debug, sync::Arc};

use indicatif::{ProgressBar, ProgressStyle};
use rayon::iter::ParallelIterator;
use serde::Serialize;
use strum::{EnumCount, IntoEnumIterator};
use tracing::warn;

use crate::{
    SerdeFormat, StorageLocation,
    agent::Agent,
    data::{common::RiskMetricsConfig, episode::Episode, view::MarketView},
    error::{ChapatyError, ChapatyResult, EnvError},
    gym::{
        EnvStatus, InvalidActionPenalty, Reward, StepOutcome, trading::{
            Env,
            action::Actions,
            config::ExecutionBias,
            context::{ActionCtx, ActionSummary, UpdateCtx},
            ledger::Ledger,
            observation::Observation,
            state::States,
        }
    },
    report::{
        journal::Journal,
        leaderboard::{AgentLeaderboard, Leaderboard, LeaderboardEntry},
        portfolio_performance::{PortfolioPerformance, PortfolioPerformanceCol},
    },
    sim::{cursor_group::CursorGroup, data::SimulationData},
};

#[derive(Clone, Debug)]
pub struct Environment {
    // === Public (configurable) ===
    /// Determines how ambiguous trade outcomes are resolved during evaluation.
    ///
    /// In edge cases where a trade could both hit stop-loss and take-profit
    /// within the same timeframe, this mode selects the evaluation strategy:
    ///
    /// - [`ExecutionBias::Optimistic`] chooses the most favorable result for the agent.
    /// - [`ExecutionBias::Pessimistic`] chooses the least favorable outcome.
    ///
    /// Defaults to [`ExecutionBias::Pessimistic`] for conservative evaluation.
    bias: ExecutionBias,

    /// Penalty applied when an agent submits an **invalid action**.
    ///
    /// Invalid actions are ignored, but the environment still advances time
    /// and returns this penalty as the reward for the step.
    ///
    /// Defaults to `Reward(0)` but can be tuned per environment to control
    /// exploration pressure.
    invalid_action_penalty: InvalidActionPenalty,

    /// Configuration for risk metrics calculations, such as Sharpe ratio.
    risk_metrics_cfg: RiskMetricsConfig,

    // === Internal only ===
    /// Shared simulation data backing the environment.
    sim_data: Arc<SimulationData>,

    /// Current states of the environment within an epoch, organized per market.
    ledger: Ledger,

    /// Cursor group tracking current simulation position.
    cursor: CursorGroup,

    /// Current Episode
    ep: Episode,

    /// Snapshot of the episode at the initial simulation state, used for resetting the environment.
    initial_ep: Episode,

    /// Current status of the environment (e.g. running, done).
    env_status: EnvStatus,
}

impl Environment {
    pub fn with_execution_bias(self, bias: ExecutionBias) -> Self {
        Self { bias, ..self }
    }

    pub fn with_invalid_action_penalty(self, invalid_action_penalty: InvalidActionPenalty) -> Self {
        Self {
            invalid_action_penalty,
            ..self
        }
    }

    pub fn with_risk_metrics_cfg(self, cfg: RiskMetricsConfig) -> Self {
        Self {
            risk_metrics_cfg: cfg,
            ..self
        }
    }

    /// Caches the heavy static simulation data (OHLCV, events) to storage.
    ///
    /// This allows subsequent runs to use `chapaty::load` with the same configuration
    /// to skip the expensive data fetching and building steps.
    ///
    /// # Naming Convention
    /// The file will be named automatically based on the hash of the environment configuration:
    /// `<hash>.<format>` (e.g., `a1b2c3d4.postcard`).
    ///
    /// # Arguments
    ///
    /// * `location` - The storage location where the data will be written
    /// * `format` - The serialization format to use (Postcard or Pickle)
    /// * `buffer_size` - Size of the internal write buffer, in bytes.
    ///
    ///   This controls how much data is buffered in memory before being flushed
    ///   to the underlying storage. Larger values generally improve throughput
    ///   for large writes at the cost of higher memory usage, while smaller values
    ///   reduce memory usage but may result in more frequent I/O operations.
    ///
    ///   If unsure, a good default is `128 * 1024` (128 KiB).
    pub async fn cache(
        &self,
        location: &StorageLocation<'_>,
        format: SerdeFormat,
        buffer_size: usize,
    ) -> ChapatyResult<()> {
        self.sim_data
            .clone()
            .write(location, format, buffer_size)
            .await
    }

    pub fn evaluate_agent<T: Agent>(&mut self, agent: &mut T) -> ChapatyResult<Journal> {
        self.reset()?;
        self.eval(agent)?;
        self.journal()
    }

    /// Evaluates a stream of agents in parallel, returning a leaderboard of the top performers.
    ///
    /// This method uses `rayon` to distribute agent evaluation across all available CPU cores.
    /// It maintains a min-heap of the top `top_k` results to minimize memory usage, allowing
    /// for the evaluation of massive datasets (e.g., 1M+ agents) with constant RAM overhead.
    ///
    /// # Arguments
    ///
    /// * `agents` - A parallel iterator yielding `(usize, Agent)`. The `usize` is treated as
    ///   the unique **Agent UID**. This is typically created by calling `.enumerate()` on your
    ///   configuration iterator (e.g., `args.enumerate()`) before converting it to parallel.
    /// * `top_k` - The maximum number of agents to retain in the leaderboard.
    /// * `stream_len` - The total number of agents expected. This is used solely to initialize
    ///   the progress bar's length, as parallel iterators may not always know their exact bounds.
    ///
    /// # Runtime Estimation
    ///
    /// Before launching a massive grid search (e.g., 1M+ agents), it is highly recommended
    /// to benchmark a single representative agent first.
    ///
    /// You can use [`Environment::evaluate_agent`] to run one agent sequentially:
    ///
    /// 1. Pick a random configuration from your grid.
    /// 2. Measure the time it takes to run `env.evaluate_agent(&mut agent)`.
    /// 3. Estimate your total wait time: `(Single Time * Total Agents) / CPU Cores`.
    ///
    /// This simple check prevents surprises—like discovering a 1M run will take 2 weeks
    /// instead of 2 hours!
    pub fn evaluate_agents<T>(
        &mut self,
        agents: impl ParallelIterator<Item = (usize, T)>,
        top_k: usize,
        stream_len: u64,
    ) -> ChapatyResult<Leaderboard>
    where
        T: Agent + Send + Serialize,
    {
        self.reset()?;
        let pb = progress_bar(stream_len)?;
        pb.set_message("Running evaluation...");

        let agent_leaderboard = agents
            .try_fold(
                || AgentLeaderboard::new(top_k),
                |mut board, (uid, mut agent)| {
                    let entries = self.worker(&mut agent, uid as u64)?;
                    board.update(&entries, agent);
                    pb.inc(1);
                    Ok(board)
                },
            )
            .try_reduce(
                || AgentLeaderboard::new(top_k),
                |a_board, b_board| Ok::<_, ChapatyError>(a_board.merge(b_board)),
            )?;

        pb.finish_with_message("Evaluation complete.");
        agent_leaderboard.try_into()
    }

    pub fn episode(&self) -> Episode {
        self.ep
    }

    pub fn episode_pnl(&self, ep: &Episode) -> ChapatyResult<f64> {
        self.ledger.episode_pnl(ep)
    }

    pub fn status(&self) -> EnvStatus {
        self.env_status
    }

    pub fn journal(&self) -> ChapatyResult<Journal> {
        let df = self.ledger.as_df()?;
        Journal::new(df, self.risk_metrics_cfg)
    }
}

impl Env for Environment {
    #[tracing::instrument(skip(self), fields(ep_id = %self.ep.id().0))]
    fn reset(&mut self) -> ChapatyResult<(Observation<'_>, Reward, StepOutcome)> {
        use EnvStatus::*;

        match self.env_status {
            EpisodeDone => {
                // This is the only state where we should try to advance.
                if let Some(next_ep) = self
                    .cursor
                    .advance_to_next_episode(&self.sim_data, self.ep)?
                {
                    // If an episode WAS found, the cursor is now correctly positioned,
                    // and we simply fall through to the common logic below.
                    self.ep = next_ep;
                    tracing::info!(
                        episode_id = %self.ep.id().0,
                        start_time = %self.ep.start(),
                        "Episode Starting (Next Sequence)"
                    );
                } else {
                    // No more episodes were found. As per the lifecycle,
                    // this triggers a full restart of the entire run.
                    self.restart();
                    tracing::info!("End of Data Reached. Performing Full Environment Reset.");
                }
            }
            Ready | Done | Running => {
                // For any other state, the lifecycle dictates that reset()
                // is a full restart of the entire run.
                self.restart();
                tracing::info!("Environment Reset Initiated.");
            }
        }

        // Per lifecycle, the state is ALWAYS Running after a reset.
        self.env_status = EnvStatus::Running;

        let obs = Observation {
            market_view: MarketView::new(&self.sim_data, &self.cursor)?,
            states: self.states(&self.ep)?,
        };

        // Per lifecycle, the outcome is ALWAYS InProgress after a reset.
        Ok((obs, Reward(0), StepOutcome::InProgress))
    }

    fn step(&mut self, actions: Actions) -> ChapatyResult<(Observation<'_>, Reward, StepOutcome)> {
        self.check_step_status()?;
        let episode = self.episode();

        // 1. Snapshot S(t)
        let market_before = MarketView::new(&self.sim_data, &self.cursor)?;

        // 2. Apply Actions A(t) -> S'(t)
        // (Validation & Execution at current prices)
        let action_ctx = ActionCtx {
            actions,
            market: market_before,
        };
        let summary = self.ledger.apply_actions(&episode, action_ctx)?;

        // 3. Transition Dynamics (Time passes: t -> t+1)
        let (outcome, total_reward) = self.transition(&episode, summary)?;

        // 4. Update Status (Safe mutation!)
        self.update_env_status(outcome)?;

        // 5. Observe S(t+1)
        let market_final = MarketView::new(&self.sim_data, &self.cursor)?;
        let obs = Observation {
            market_view: market_final,
            states: self.states(&episode)?,
        };

        Ok((obs, total_reward, outcome))
    }
}

impl Environment {
    fn portfolio_performance(&self) -> ChapatyResult<PortfolioPerformance> {
        self.journal()?.portfolio_performance()
    }

    fn restart(&mut self) {
        self.cursor.reset(&self.sim_data);
        self.ledger.clear();
        self.ep = self.initial_ep;
    }

    fn transition(
        &mut self,
        ep: &Episode,
        summary: ActionSummary,
    ) -> ChapatyResult<(StepOutcome, Reward)> {
        self.advance_market()?;
        let market_after = MarketView::new(&self.sim_data, &self.cursor)?;

        let update_ctx = UpdateCtx {
            market: &market_after,
            bias: self.bias,
        };
        self.ledger.apply_updates(ep, update_ctx)?;

        let reward_delta = self.ledger.pop_step_reward(ep)?;
        let penalty = self.penalty(summary);
        let total = reward_delta + penalty;

        let outcome = self.evaluate_outcome(ep)?;

        Ok((outcome, total))
    }

    fn eval<T: Agent>(&mut self, agent: &mut T) -> ChapatyResult<()> {
        while self.env_status != EnvStatus::Done {
            self.run_episode(agent)?;
        }
        Ok(())
    }

    fn run_episode<T: Agent>(&mut self, agent: &mut T) -> ChapatyResult<()> {
        let (mut obs, _, mut outcome) = self.reset()?;

        while !outcome.is_terminal() {
            let actions = agent.act(obs)?;
            (obs, _, outcome) = self.step(actions)?;
        }

        agent.reset();
        Ok(())
    }

    #[tracing::instrument(skip(self, agent), fields(agent_uid = %agent_uid))]
    fn worker<T>(&self, agent: &mut T, agent_uid: u64) -> ChapatyResult<Vec<LeaderboardEntry>>
    where
        T: Agent,
    {
        let mut thread_env = self.clone();
        thread_env.eval(agent)?;

        let pp = thread_env.portfolio_performance()?;
        let accessor = pp.accessor()?;

        let mut entries = Vec::with_capacity(PortfolioPerformanceCol::COUNT);

        for metric in PortfolioPerformanceCol::iter() {
            if let Some(value) = accessor.get(metric) {
                entries.push(LeaderboardEntry {
                    agent_uid,
                    metric,
                    reward: metric.to_heap_score(value).into(),
                });
            } else {
                warn!(?metric, "Metric produced null value");
            }
        }

        Ok(entries)
    }

    fn states(&self, ep: &Episode) -> ChapatyResult<&States> {
        self.ledger.get(ep)
    }

    fn penalty(&self, report: ActionSummary) -> Reward {
        Reward((report.rejected as i64) * self.invalid_action_penalty.0.0)
    }

    fn check_step_status(&self) -> ChapatyResult<()> {
        use EnvStatus::*;
        match self.env_status {
            Running => Ok(()),
            Ready => Err(EnvError::InvalidState(
                "Environment is not started. Call `reset()` before stepping.".to_string(),
            )
            .into()),
            EpisodeDone => Err(EnvError::InvalidState(
                "Episode is done. Call `reset()` before stepping.".to_string(),
            )
            .into()),
            Done => Err(EnvError::InvalidState(
                "Simulation is finished. No further steps allowed. Call `reset()` to restart."
                    .to_string(),
            )
            .into()),
        }
    }

    fn advance_market(&mut self) -> ChapatyResult<()> {
        self.cursor.step(&self.sim_data, &self.ep)
    }

    fn evaluate_outcome(&self, ep: &Episode) -> ChapatyResult<StepOutcome> {
        if self.ep.is_episode_end(self.cursor.current_ts())
            || self.cursor.is_end_of_data(&self.sim_data)
        {
            if self.ledger.is_terminal(ep)? {
                Ok(StepOutcome::Terminated)
            } else {
                Ok(StepOutcome::Truncated)
            }
        } else {
            Ok(StepOutcome::InProgress)
        }
    }

    fn update_env_status(&mut self, outcome: StepOutcome) -> ChapatyResult<()> {
        if outcome.is_terminal() {
            self.env_status = if self.cursor.is_end_of_data(&self.sim_data) {
                EnvStatus::Done
            } else {
                EnvStatus::EpisodeDone
            };
        }
        Ok(())
    }
}

// ================================================================================================
// Building
// ================================================================================================
impl Environment {
    pub(super) fn new(
        cursor: CursorGroup,
        sim_data: Arc<SimulationData>,
        ledger: Ledger,
        ep: Episode,
    ) -> Self {
        Self {
            sim_data,
            ledger,
            cursor,
            initial_ep: ep,
            ep,
            bias: ExecutionBias::Pessimistic,
            invalid_action_penalty: InvalidActionPenalty::default(),
            env_status: EnvStatus::Ready,
            risk_metrics_cfg: Default::default(),
        }
    }
}

// ================================================================================================
// Helper Functions
// ================================================================================================
fn progress_bar(capacity: u64) -> ChapatyResult<ProgressBar> {
    let bar = ProgressBar::new(capacity);
    bar.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta_precise}) {msg}")
            .map_err(EnvError::ProgressBar)?
            .progress_chars("#>-"));
    Ok(bar)
}
