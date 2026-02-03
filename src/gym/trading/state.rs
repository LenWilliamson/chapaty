use std::{collections::HashMap, fmt::Debug};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::{
    agent::AgentIdentifier,
    data::{
        domain::{Instrument, Price, Quantity, Symbol, Tick, TradeId},
        event::MarketId,
        view::MarketView,
    },
    error::{AgentError, ChapatyError, ChapatyResult, DataError, SystemError},
    gym::{
        Reward,
        trading::{
            action::{CancelCmd, MarketCloseCmd, ModifyCmd, OpenCmd},
            context::UpdateCtx,
            state::active::CloseOutcome,
            types::{RiskRewardRatio, StateKind, TerminationReason, TradeType},
        },
    },
    sorted_vec_map::SortedVecMap,
};

mod active;
mod canceled;
mod closed;
mod pending;

// ================================================================================================
// The Entity Definition & Typestate
// ================================================================================================

pub trait TradeState: Debug + Clone + Send + Sync + 'static {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pending {
    created_at: DateTime<Utc>,
    limit_price: Price,
}

impl Pending {
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
    pub fn limit_price(&self) -> Price {
        self.limit_price
    }
}

impl TradeState for Pending {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Active {
    entry_ts: DateTime<Utc>,
    entry_price: Price,
    current_ts: DateTime<Utc>,
    current_price: Price,
    unrealized_pnl: f64,
}

impl Active {
    pub fn entry_ts(&self) -> DateTime<Utc> {
        self.entry_ts
    }

    pub fn entry_price(&self) -> Price {
        self.entry_price
    }

    pub fn current_ts(&self) -> DateTime<Utc> {
        self.current_ts
    }

    pub fn current_price(&self) -> Price {
        self.current_price
    }

    pub fn unrealized_pnl(&self) -> f64 {
        self.unrealized_pnl
    }
}

impl TradeState for Active {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Closed {
    entry_ts: DateTime<Utc>,
    entry_price: Price,
    exit_ts: DateTime<Utc>,
    exit_price: Price,
    termination_reason: TerminationReason,
    realized_pnl: f64,
}

impl Closed {
    pub fn entry_ts(&self) -> DateTime<Utc> {
        self.entry_ts
    }

    pub fn entry_price(&self) -> Price {
        self.entry_price
    }

    pub fn exit_ts(&self) -> DateTime<Utc> {
        self.exit_ts
    }

    pub fn exit_price(&self) -> Price {
        self.exit_price
    }

    pub fn termination_reason(&self) -> TerminationReason {
        self.termination_reason
    }

    pub fn realized_pnl(&self) -> f64 {
        self.realized_pnl
    }
}

impl TradeState for Closed {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Canceled {
    created_at: DateTime<Utc>,
    canceled_at: DateTime<Utc>,
    limit_price: Price,
}

impl Canceled {
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    pub fn canceled_at(&self) -> DateTime<Utc> {
        self.canceled_at
    }

    pub fn termination_reason(&self) -> TerminationReason {
        TerminationReason::Canceled
    }

    pub fn limit_price(&self) -> Price {
        self.limit_price
    }
}

impl TradeState for Canceled {}

/// The Generic Trade Entity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade<S: TradeState> {
    uid: TradeId,
    agent_id: AgentIdentifier,
    trade_type: TradeType,
    quantity: Quantity,
    stop_loss: Option<Price>,
    take_profit: Option<Price>,
    state: S,
}

impl<S: TradeState> Trade<S> {
    pub fn uid(&self) -> TradeId {
        self.uid
    }

    pub fn agent_id(&self) -> &AgentIdentifier {
        &self.agent_id
    }

    pub fn trade_type(&self) -> &TradeType {
        &self.trade_type
    }

    pub fn quantity(&self) -> Quantity {
        self.quantity
    }

    pub fn stop_loss(&self) -> Option<Price> {
        self.stop_loss
    }

    pub fn take_profit(&self) -> Option<Price> {
        self.take_profit
    }

    pub fn state(&self) -> &S {
        &self.state
    }
}

// ================================================================================================
// Storage Wrapper (Enum)
// ================================================================================================

#[derive(Debug, Clone)]
pub enum State {
    Pending(Trade<Pending>),
    Active(Trade<Active>),
    Closed(Trade<Closed>),
    Canceled(Trade<Canceled>),
}

impl TryFrom<State> for Trade<Pending> {
    type Error = ChapatyError;

    fn try_from(state: State) -> Result<Self, Self::Error> {
        match state {
            State::Pending(t) => Ok(t),
            other => Err(DataError::UnexpectedEnumVariant(format!(
                "Expected Pending, got {:?}",
                StateKind::from(other)
            ))
            .into()),
        }
    }
}

impl TryFrom<State> for Trade<Active> {
    type Error = ChapatyError;

    fn try_from(state: State) -> Result<Self, Self::Error> {
        match state {
            State::Active(t) => Ok(t),
            other => Err(DataError::UnexpectedEnumVariant(format!(
                "Expected Active, got {:?}",
                StateKind::from(other)
            ))
            .into()),
        }
    }
}

impl TryFrom<State> for Trade<Closed> {
    type Error = ChapatyError;

    fn try_from(state: State) -> Result<Self, Self::Error> {
        match state {
            State::Closed(t) => Ok(t),
            other => Err(DataError::UnexpectedEnumVariant(format!(
                "Expected Closed, got {:?}",
                StateKind::from(other)
            ))
            .into()),
        }
    }
}

impl TryFrom<State> for Trade<Canceled> {
    type Error = ChapatyError;

    fn try_from(state: State) -> Result<Self, Self::Error> {
        match state {
            State::Canceled(t) => Ok(t),
            other => Err(DataError::UnexpectedEnumVariant(format!(
                "Expected Canceled, got {:?}",
                StateKind::from(other)
            ))
            .into()),
        }
    }
}

impl State {
    pub fn trade_id(&self) -> TradeId {
        match self {
            State::Pending(t) => t.uid,
            State::Active(t) => t.uid,
            State::Closed(t) => t.uid,
            State::Canceled(t) => t.uid,
        }
    }

    pub fn agent_id(&self) -> &AgentIdentifier {
        match self {
            State::Pending(t) => &t.agent_id,
            State::Active(t) => &t.agent_id,
            State::Closed(t) => &t.agent_id,
            State::Canceled(t) => &t.agent_id,
        }
    }

    pub fn trade_type(&self) -> &TradeType {
        match self {
            State::Pending(t) => &t.trade_type,
            State::Active(t) => &t.trade_type,
            State::Closed(t) => &t.trade_type,
            State::Canceled(t) => &t.trade_type,
        }
    }

    pub fn quantity(&self) -> Quantity {
        match self {
            State::Pending(t) => t.quantity,
            State::Active(t) => t.quantity,
            State::Closed(t) => t.quantity,
            State::Canceled(t) => t.quantity,
        }
    }

    pub fn stop_loss(&self) -> Option<Price> {
        match self {
            State::Pending(t) => t.stop_loss,
            State::Active(t) => t.stop_loss,
            State::Closed(t) => t.stop_loss,
            State::Canceled(t) => t.stop_loss,
        }
    }

    pub fn take_profit(&self) -> Option<Price> {
        match self {
            State::Pending(t) => t.take_profit,
            State::Active(t) => t.take_profit,
            State::Closed(t) => t.take_profit,
            State::Canceled(t) => t.take_profit,
        }
    }

    pub fn anticipated_entry_price(&self) -> Price {
        match self {
            State::Pending(t) => t.state.limit_price,
            State::Active(t) => t.state.entry_price,
            State::Closed(t) => t.state.entry_price,
            State::Canceled(t) => t.state.limit_price,
        }
    }

    /// Calculates the expected loss in Ticks based on the Stop Loss.
    pub fn expected_loss_in_ticks(&self, symbol: &Symbol) -> Option<Tick> {
        let (ref_price, sl) = self.get_risk_params()?;
        let diff = self.trade_type().price_diff(ref_price, sl);
        // Result is usually negative for a Stop Loss; we want magnitude (absolute ticks).
        Some(Tick(symbol.price_to_ticks(diff).0.abs()))
    }

    /// Calculates the expected profit in Ticks based on the Take Profit.
    pub fn expected_profit_in_ticks(&self, symbol: &Symbol) -> Option<Tick> {
        let (ref_price, tp) = self.get_reward_params()?;
        let diff = self.trade_type().price_diff(ref_price, tp);
        Some(Tick(symbol.price_to_ticks(diff).0.abs()))
    }

    /// Calculates the expected loss in USD (Absolute Value) based on Stop Loss.
    pub fn expected_loss_in_usd(&self, symbol: &Symbol) -> Option<f64> {
        let (ref_price, sl) = self.get_risk_params()?;
        let qty = self.quantity(); // Uses the helper we defined earlier

        // Use clean PnL math
        let pnl = self.trade_type().calculate_pnl(ref_price, sl, qty, symbol);
        Some(pnl.abs())
    }

    /// Calculates the expected profit in USD (Absolute Value) based on Take Profit.
    pub fn expected_profit_in_usd(&self, symbol: &Symbol) -> Option<f64> {
        let (ref_price, tp) = self.get_reward_params()?;
        let qty = self.quantity();

        let pnl = self.trade_type().calculate_pnl(ref_price, tp, qty, symbol);
        Some(pnl.abs())
    }

    /// Computes the Risk-Reward Ratio based on SL/TP settings.
    pub fn risk_reward_ratio(&self, symbol: &Symbol) -> Option<RiskRewardRatio> {
        // We need both parameters to exist to calculate a ratio
        let risk = self.expected_loss_in_usd(symbol)?;
        let reward = self.expected_profit_in_usd(symbol)?;

        Some(RiskRewardRatio::new(risk, reward))
    }

    /// Returns the "Clean" USD PnL directly from the storage fields.
    pub fn pnl_usd(&self) -> Option<f64> {
        match self {
            // Already calculated via tick-math during `update`
            State::Active(t) => Some(t.state.unrealized_pnl),

            // Already calculated via tick-math during `close`
            State::Closed(t) => Some(t.state.realized_pnl),

            _ => None,
        }
    }

    /// Calculates the Price Distance in Ticks.
    ///
    /// We re-calculate this on the fly because `Active` state stores USD, not Ticks.
    /// However, because `entry` and `current/exit` are **Guaranteed Clean** (snapped to grid),
    /// this calculation is strictly deterministic and free of artifacts.
    pub fn pnl_ticks(&self, symbol: &Symbol) -> Option<Tick> {
        match self {
            State::Active(t) => {
                // Use your existing helper
                let diff = t
                    .trade_type
                    .price_diff(t.state.entry_price, t.state.current_price);
                Some(symbol.price_to_ticks(diff))
            }
            State::Closed(t) => {
                // Use your existing helper
                let diff = t
                    .trade_type
                    .price_diff(t.state.entry_price, t.state.exit_price);
                Some(symbol.price_to_ticks(diff))
            }
            _ => None,
        }
    }

    /// Returns the timestamp when the trade was effectively entered (Active/Closed only).
    pub fn entry_ts(&self) -> Option<DateTime<Utc>> {
        match self {
            State::Active(t) => Some(t.state.entry_ts),
            State::Closed(t) => Some(t.state.entry_ts),
            // Pending/Canceled never entered the market
            State::Pending(_) | State::Canceled(_) => None,
        }
    }

    /// Returns the timestamp when the trade ended (Closed/Canceled only).
    pub fn exit_ts(&self) -> Option<DateTime<Utc>> {
        match self {
            State::Closed(t) => Some(t.state.exit_ts),
            State::Canceled(t) => Some(t.state.canceled_at),
            _ => None,
        }
    }

    /// Returns the price at which the trade was closed.
    pub fn exit_price(&self) -> Option<Price> {
        match self {
            State::Closed(t) => Some(t.state.exit_price),
            // Canceled orders don't have a price execution
            _ => None,
        }
    }

    /// Returns the reason why the trade ended.
    pub fn exit_reason(&self) -> Option<TerminationReason> {
        match self {
            State::Closed(t) => Some(t.state.termination_reason()),
            // Canceled usually implies "Cancel" reason, handled by the struct helper
            State::Canceled(t) => Some(t.state.termination_reason()),
            _ => None,
        }
    }

    pub fn kind(&self) -> StateKind {
        self.into()
    }

    pub fn is_pending(&self) -> bool {
        matches!(self, State::Pending(_))
    }

    pub fn is_active(&self) -> bool {
        matches!(self, State::Active(_))
    }

    pub fn is_closed(&self) -> bool {
        matches!(self, State::Closed(_))
    }

    pub fn is_canceled(&self) -> bool {
        matches!(self, State::Canceled(_))
    }
}

impl State {
    // ========================================================================
    // Internal Helpers to Extract Params
    // ========================================================================

    /// Extracts (Entry/Limit Price, Stop Loss Price) if SL exists.
    fn get_risk_params(&self) -> Option<(Price, Price)> {
        let sl = self.stop_loss()?;
        let ref_price = self.anticipated_entry_price();
        Some((ref_price, sl))
    }

    /// Extracts (Entry/Limit Price, Take Profit Price) if TP exists.
    fn get_reward_params(&self) -> Option<(Price, Price)> {
        let tp = self.take_profit()?;
        let ref_price = self.anticipated_entry_price();
        Some((ref_price, tp))
    }
}

impl From<&State> for StateKind {
    fn from(value: &State) -> Self {
        match value {
            State::Pending(_) => Self::Pending,
            State::Active(_) => Self::Active,
            State::Closed(_) => Self::Closed,
            State::Canceled(_) => Self::Canceled,
        }
    }
}

impl From<State> for StateKind {
    fn from(value: State) -> Self {
        match value {
            State::Pending(_) => Self::Pending,
            State::Active(_) => Self::Active,
            State::Closed(_) => Self::Closed,
            State::Canceled(_) => Self::Canceled,
        }
    }
}

// ================================================================================================
// Repository (States)
// ================================================================================================

#[derive(Debug, Clone)]
pub struct States {
    /// **HOT PATH:** Only Active and Pending trades.
    /// Contiguous memory. Fast iteration.
    live: SortedVecMap<MarketId, Vec<State>>,

    /// **COLD PATH:** Closed and Canceled trades.
    /// We never iterate this during updates.
    archive: SortedVecMap<MarketId, Vec<State>>,

    /// Secondary Index: TradeId -> idx
    /// Allows O(1) lookup of any trade in live.
    live_index: HashMap<TradeId, (MarketId, usize)>,

    /// **TRANSIENT:** Accumulated reward for the current step only.
    /// Resets to `0.0` after every step.
    step_reward: f64,

    /// **PERSISTENT:** `Total Realized + Unrealized PnL` since the episode began.
    /// Does NOT reset. Monotonically tracks the portfolio curve.
    cumulative_pnl: f64,
}

impl Default for States {
    fn default() -> Self {
        Self {
            live: SortedVecMap::new(),
            archive: SortedVecMap::new(),
            live_index: HashMap::default(),
            step_reward: 0.0,
            cumulative_pnl: 0.0,
        }
    }
}

impl States {
    // ========================================================================
    // Global Metrics
    // ========================================================================

    /// Returns `true` if there are NO active or pending trades currently in the system.
    /// This checks the Hot Path only (O(M) where M is number of markets).
    pub fn all_closed(&self) -> bool {
        // If the map is empty, or all vectors within it are empty
        self.live.iter().all(|(_, list)| list.is_empty())
    }

    pub fn pnl(&self) -> f64 {
        self.cumulative_pnl
    }

    /// Returns the markets that currently have allocated memory for live trades.
    pub fn markets(&self) -> impl Iterator<Item = &MarketId> {
        self.live.keys()
    }

    // ========================================================================
    // Hot Path Iteration (O(N_live))
    // ========================================================================

    /// Iterates over **live** trades only.
    /// Use this for observing the agent's current exposure.
    pub fn iter_live(&self) -> impl Iterator<Item = &State> {
        self.live.iter().flat_map(|(_, list)| list.iter())
    }

    /// Iterates Active/Pending trades tupled with their MarketId.
    pub fn iter_live_with_market(&self) -> impl Iterator<Item = (MarketId, &State)> {
        self.live
            .iter()
            .flat_map(|(m_id, list)| list.iter().map(move |s| (*m_id, s)))
    }

    // ========================================================================
    // Cold Path / Full History Iteration
    // ========================================================================

    /// Iterates over **Closed and Canceled** trades only.
    /// Use this for generating episode reports or history analysis.
    pub fn iter_archive(&self) -> impl Iterator<Item = &State> {
        self.archive.iter().flat_map(|(_, list)| list.iter())
    }

    /// Iterates **ALL** trades (Hot + Cold) in the system.
    /// Useful for global auditing or reconstructing the full timeline.
    pub fn iter_all(&self) -> impl Iterator<Item = &State> {
        self.iter_live().chain(self.iter_archive())
    }

    // ========================================================================
    // O(1) Lookups & Agent Queries
    // ========================================================================

    /// Returns the active vector for a specific market if it exists, else empty slice.
    pub fn get_live_trades(&self, market: &MarketId) -> &[State] {
        self.live
            .get(market)
            .map(|v| v.as_slice())
            .unwrap_or_default()
    }

    /// Retrieves a reference to a LIVE trade.
    /// Returns None if the trade is closed/canceled or doesn't exist.
    pub fn get_by_id(&self, uid: &TradeId) -> Option<&State> {
        let (m_id, idx) = self.live_index.get(uid)?;
        self.live.get(m_id)?.get(*idx)
    }

    /// Checks if a specific agent has any **Active** (not just pending) positions.
    /// Filters the Hot Path (fast).
    pub fn any_active_trade_for_agent(&self, id: &AgentIdentifier) -> bool {
        self.iter_live()
            .any(|s| s.is_active() && s.agent_id() == id)
    }

    /// Finds the first active trade for a specific agent.
    pub fn find_active_trade_for_agent(&self, id: &AgentIdentifier) -> Option<(MarketId, &State)> {
        self.iter_live_with_market()
            .find(|(_, s)| s.is_active() && s.agent_id() == id)
    }
}

impl States {
    /// Iterates over ALL markets and applies updates to all active trades.
    pub(super) fn update_all_live_trades<F>(
        &mut self,
        ctx: &UpdateCtx,
        mut on_update: F,
    ) -> ChapatyResult<()>
    where
        F: FnMut(MarketId, ChapatyResult<Option<TerminationReason>>) -> ChapatyResult<()>,
    {
        let markets = self.markets().copied().collect::<Vec<_>>();
        markets.into_iter().try_for_each(|market_id| {
            self.update_live_trades_scan(market_id, ctx, |result| on_update(market_id, result))
        })
    }

    /// Factory: Creates a new State repository with pre-allocated memory.
    ///
    /// # Performance
    /// Allocating memory upfront ensures that `handle_open` and `handle_close`
    /// never trigger a `malloc` or `memcpy` during the hot loop of the episode.
    pub(super) fn with_capacity(markets: &[MarketId], trade_hint_per_market: usize) -> Self {
        // 1. Pre-allocate Hot Storage (Active/Pending)
        let live: SortedVecMap<MarketId, Vec<State>> = markets
            .iter()
            .map(|id| (*id, Vec::with_capacity(trade_hint_per_market)))
            .collect();

        // 2. Pre-allocate Cold Storage (Closed/Canceled)
        // We assume roughly same volume, or you could tune this separately.
        let archive: SortedVecMap<MarketId, Vec<State>> = markets
            .iter()
            .map(|id| (*id, Vec::with_capacity(trade_hint_per_market)))
            .collect();

        // 3. Estimate Index Load
        let total_capacity_hint = live.len() * trade_hint_per_market;

        Self {
            live,
            archive,
            live_index: HashMap::with_capacity(total_capacity_hint),
            step_reward: 0.0,
            cumulative_pnl: 0.0,
        }
    }

    pub(super) fn clear(&mut self) {
        self.live_index.clear();
        self.step_reward = 0.0;
        self.live.iter_mut().for_each(|(_, s)| s.clear());
        self.archive.iter_mut().for_each(|(_, s)| s.clear());
    }

    /// Consumes and resets the accumulated step reward.
    pub(super) fn pop_reward(&mut self) -> Reward {
        let r = self.step_reward;
        self.step_reward = 0.0;
        Reward(r.round() as i64)
    }

    pub(super) fn open(
        &mut self,
        market_id: MarketId,
        cmd: OpenCmd,
        market: &MarketView,
    ) -> ChapatyResult<()> {
        if self.live_index.contains_key(&cmd.trade_id) {
            return Err(AgentError::InvalidInput(format!(
                "Trade ID {:?} already exists",
                cmd.trade_id
            ))
            .into());
        }

        let ts = market.current_timestamp();
        let symbol = &market_id.symbol;

        let state = if let Some(limit_price) = cmd.entry_price {
            // Case A: Limit Order -> Pending
            State::Pending(Trade::<Pending>::new(cmd, limit_price, ts, symbol)?)
        } else {
            // Case B: Market Order -> Active
            let raw_price = market.try_resolved_close_price(symbol)?.0;
            State::Active(Trade::<Active>::new(cmd, Price(raw_price), ts, symbol)?)
        };

        // New trades always go to Hot Path
        self.insert_new_live(market_id, state);
        Ok(())
    }

    pub(super) fn modify(&mut self, cmd: ModifyCmd) -> ChapatyResult<()> {
        let (m_id, loc) = self.get_index(&cmd.trade_id)?;

        self.modify_state_at(m_id, loc, |state| match state {
            State::Active(mut t) => {
                t.modify(&cmd, &m_id.symbol)?;
                Ok(Transition {
                    new_state: State::Active(t),
                    output: (),
                })
            }
            State::Pending(mut t) => {
                t.modify(&cmd, &m_id.symbol)?;
                Ok(Transition {
                    new_state: State::Pending(t),
                    output: (),
                })
            }
            // Modifying a Closed/Canceled trade is generally invalid
            other => {
                Err(AgentError::InvalidInput(format!("Cannot modify state {:?}", other)).into())
            }
        })
    }

    pub(super) fn market_close(
        &mut self,
        cmd: MarketCloseCmd,
        market: &MarketView,
    ) -> ChapatyResult<()> {
        let (m_id, loc) = self.get_index(&cmd.trade_id)?;
        let ts = market.current_timestamp();
        let symbol = &m_id.symbol;
        let exit_price = market.try_resolved_close_price(symbol)?;

        // Output tuple: (Reward, Option<TradeToArchive>)
        let (reward, trade_to_archive) = self.modify_state_at(m_id, loc, |state| {
            let t: Trade<Active> = state.try_into()?;
            let (outcome, reward) = t.market_close(&cmd, Price(exit_price.0), ts, symbol)?;

            match outcome {
                // Case A: Full Close
                // The state changes to Closed.
                // The Guard::commit() will automatically move it from Active -> Archive.
                CloseOutcome::FullyClosed(closed) => Ok(Transition {
                    new_state: State::Closed(closed),
                    output: (reward, None),
                }),

                // Case B: Partial Close
                // 1. The remaining portion stays Active (Active -> Active).
                //    Guard::commit() will update it in-place in the Active vector.
                // 2. The 'Closed' portion is returned as 'output'.
                //    We must manually archive this split child.
                CloseOutcome::PartiallyClosed { closed, remaining } => Ok(Transition {
                    new_state: State::Active(remaining),
                    output: (reward, Some(State::Closed(closed))),
                }),
            }
        })?;

        // Handle the split child from Partial Close
        if let Some(archived) = trade_to_archive {
            self.archive_state(m_id, archived);
        }

        self.record_pnl_change(reward);

        Ok(())
    }

    pub(super) fn cancel(&mut self, cmd: CancelCmd, market: &MarketView) -> ChapatyResult<()> {
        let (m_id, loc) = self.get_index(&cmd.trade_id)?;
        let ts = market.current_timestamp();

        self.modify_state_at(m_id, loc, |state| {
            let t: Trade<Pending> = state.try_into()?;
            let canceled = t.cancel(&cmd, ts)?;

            // State changes to Canceled.
            // Guard::commit() will automatically move it from Active -> Archive.
            Ok(Transition {
                new_state: State::Canceled(canceled),
                output: (),
            })
        })
    }

    /// Returns an iterator over ALL states (Live + Archived) coupled with their MarketId.
    /// Useful for reporting, logging, or serialization of the entire state.
    pub(super) fn flattened(&self) -> impl Iterator<Item = (&MarketId, &State)> {
        // 1. Iterator for Hot Path
        let active_iter = self
            .live
            .iter()
            .flat_map(|(m_id, list)| list.iter().map(move |s| (m_id, s)));

        // 2. Iterator for Cold Path
        let archive_iter = self
            .archive
            .iter()
            .flat_map(|(m_id, list)| list.iter().map(move |s| (m_id, s)));

        // 3. Chain them together
        active_iter.chain(archive_iter)
    }
}

impl States {
    /// Internal helper to register a PnL change.
    /// This updates BOTH the transient signal and the persistent score.
    fn record_pnl_change(&mut self, delta: f64) {
        self.step_reward += delta;
        self.cumulative_pnl += delta;
    }

    // ============================================================================
    // Lifecycle Management (RAII Transaction Kernel)
    // ============================================================================

    /// Iterates over all active trades in a market, applying updates safely.
    /// The caller provides a callback `f` to process the result of each update.
    ///
    /// This handles the "Swap-Remove" shift automatically:
    /// - If `f` returns a closed state, we stay at `idx` (because a new trade swapped in).
    /// - If `f` returns an active state, we advance `idx`.
    fn update_live_trades_scan<F>(
        &mut self,
        m_id: MarketId,
        ctx: &UpdateCtx,
        mut on_update: F,
    ) -> ChapatyResult<()>
    where
        F: FnMut(ChapatyResult<Option<TerminationReason>>) -> ChapatyResult<()>,
    {
        let mut idx = 0;

        // We re-evaluate len() every loop because it shrinks when trades close.
        while idx < self.live.get(&m_id).map(|v| v.len()).unwrap_or(0) {
            // 1. Perform the transactional update
            let update_result = self.update_single_at_idx(m_id, idx, ctx);

            // 2. Capture the outcome (Did it close?)
            let is_closed = match &update_result {
                Ok(Some(_)) => true, // Closed/Canceled
                Ok(None) => false,   // Still Active
                Err(_) => false,     // Error (state didn't change)
            };

            // 3. Notify the caller (Ledger) so it can log/react
            // We pass the result *before* we decide index logic, so Ledger knows what happened.
            on_update(update_result)?;

            // 4. Control Flow (The "Core" Safety Logic)
            if is_closed {
                // Trade Closed.
                // A new trade (from the end) is now at 'idx'.
                // Do NOT increment idx. Process this slot again.
            } else {
                // Trade stayed Active (or error occurred).
                // Move to next slot.
                idx += 1;
            }
        }
        Ok(())
    }

    /// Internal helper for the scan loop.
    /// Performs a single state transition on a live trade at the given index.
    fn update_single_at_idx(
        &mut self,
        m_id: MarketId,
        idx: usize,
        ctx: &UpdateCtx,
    ) -> ChapatyResult<Option<TerminationReason>> {
        let (reward, exit) = self.modify_state_at(m_id, idx, |state| {
            // 1. Delegate Logic
            let (new_state, r) = match state {
                State::Active(t) => active::update(t, &m_id, ctx)?,
                State::Pending(t) => pending::update(t, &m_id, ctx)?,
                other => {
                    // This branch implies data corruption (Cold trade in Hot vec)
                    warn!(
                        ?m_id,
                        state = ?other,
                        "Internal error: Non-updatable state found in Active Vector. Ignoring."
                    );
                    return Ok(Transition {
                        new_state: other,
                        output: (0.0, None),
                    });
                }
            };

            // 2. Extract Event
            // We use the helper methods on State to keep this clean
            let reason = if new_state.is_closed() || new_state.is_canceled() {
                new_state.exit_reason()
            } else {
                None
            };

            // 3. Return transition
            Ok(Transition {
                new_state,
                output: (r, reason),
            })
        })?;

        self.record_pnl_change(reward);
        Ok(exit)
    }

    /// The Core Transaction Kernel.
    /// Executes a transaction on a specific trade location (Hot or Cold).
    ///
    /// # Safety
    /// This uses `StateGuard` to clone the state first. The vector is NOT modified
    /// until the closure returns successfully and `guard.commit()` is called.
    fn modify_state_at<F, R>(&mut self, m_id: MarketId, idx: usize, f: F) -> ChapatyResult<R>
    where
        // Closure returns a clear 'Transition' struct
        F: FnOnce(State) -> ChapatyResult<Transition<R>>,
    {
        // 1. Create Guard (Clones state, leaves vector untouched)
        let guard = StateGuard::new(self, m_id, idx)?;

        // 2. Work on the Clone
        let working_copy = guard.get().clone();

        // 3. Logic & Commit
        match f(working_copy) {
            Ok(t) => {
                guard.commit(t.new_state); // Commit the state
                Ok(t.output) // Return the output
            }
            Err(e) => Err(e), // Guard drops, rolling back automatically
        }
    }

    // ============================================================================
    // Index Management
    // ============================================================================

    fn get_index(&self, uid: &TradeId) -> ChapatyResult<(MarketId, usize)> {
        self.live_index
            .get(uid)
            .copied()
            .ok_or_else(|| ChapatyError::Data(DataError::KeyNotFound(format!("Trade {uid:?}"))))
    }

    fn insert_new_live(&mut self, market_id: MarketId, state: State) {
        let uid = state.trade_id();
        let vec = self.live.entry(market_id).or_default();
        vec.push(state);

        self.live_index.insert(uid, (market_id, vec.len() - 1));
    }

    fn archive_state(&mut self, market_id: MarketId, state: State) {
        self.archive.entry(market_id).or_default().push(state);
    }
}

// ================================================================================================
// StateGuard: The Traffic Controller
// ================================================================================================

/// RAII Guard that manages state transitions between Hot (Active) and Cold (Archive) storage.
///
/// # Mechanism
/// 1. `new()`: Clones the state from source vector. Does NOT remove it yet (Snapshot).
/// 2. `commit()`: Determines if state stays Hot or moves to Cold.
///    - If Hot->Hot: Overwrites the slot in-place (O(1)).
///    - If Hot->Cold: Swap-removes from Hot, pushes to Cold (O(1)).
/// 3. `drop()`: If not committed, does nothing. The original state remains in the vector (Rollback).
#[must_use = "StateGuard must be committed to persist changes"]
#[derive(Debug)]
struct StateGuard<'a> {
    market_id: MarketId,
    idx: usize,
    state: Option<State>,

    live_vec: &'a mut Vec<State>,
    archive_vec: &'a mut Vec<State>,
    live_index: &'a mut HashMap<TradeId, (MarketId, usize)>,
}

impl<'a> StateGuard<'a> {
    fn new(states: &'a mut States, market_id: MarketId, idx: usize) -> ChapatyResult<Self> {
        let live_vec = states.live.entry(market_id).or_default();
        let archive_vec = states.archive.entry(market_id).or_default();

        let working_copy = live_vec.get(idx).cloned().ok_or_else(|| {
            SystemError::IndexOutOfBounds(format!("Idx {} len {}", idx, live_vec.len()))
        })?;

        Ok(Self {
            market_id,
            idx,
            state: Some(working_copy),
            live_vec,
            archive_vec,
            live_index: &mut states.live_index,
        })
    }

    fn get(&self) -> &State {
        self.state
            .as_ref()
            .expect("StateGuard invariant violated: state missing")
    }

    fn commit(mut self, new_state: State) {
        let uid = new_state.trade_id();

        if new_state.is_active() || new_state.is_pending() {
            // Case A: Stay Live (Update)
            self.live_vec[self.idx] = new_state;
            // Index stays valid (uid->idx mapping didn't change)
        } else {
            // Case B: Close (Live -> Archive)

            // 1. Swap Remove from Live
            self.live_vec.swap_remove(self.idx);

            // 2. Repair Index for the swapped element
            if self.idx < self.live_vec.len() {
                let swapped_uid = self.live_vec[self.idx].trade_id();
                self.live_index
                    .insert(swapped_uid, (self.market_id, self.idx));
            }

            // 3. Remove THIS trade from Index (It's dead)
            self.live_index.remove(&uid);

            // 4. Log to Archive
            self.archive_vec.push(new_state);
        }
        self.state = None;
    }
}

impl<'a> Drop for StateGuard<'a> {
    fn drop(&mut self) {
        // If we drop without commit, we do NOTHING.
        // The original state is still sitting safely in the vector.
        // This effectively "Rolls Back" to the state before the transaction.
    }
}

// ================================================================================================
// Helper Functions
// ================================================================================================

fn sanitize_price(symbol: &Symbol, original: f64, field_name: &str) -> f64 {
    let sanitized = symbol.normalize_price(original);

    if (original - sanitized).abs() > f64::EPSILON * 100.0 {
        tracing::debug!(
            field = field_name,
            original = original,
            sanitized = sanitized,
            "Input price snapped to grid"
        );
    }
    sanitized
}

// ================================================================================================
// Helper Sructs
// ================================================================================================

/// Represents a successful state transition.
/// - `state`: The new state to commit to storage.
/// - `output`: The value to return to the caller (e.g., Reward, Events).
struct Transition<T> {
    new_state: State,
    output: T,
}

#[cfg(test)]
mod tests {
    use chrono::Duration;

    use super::*;
    use crate::{
        data::{
            domain::{DataBroker, Exchange, Period, SpotPair},
            event::{Ohlcv, OhlcvId},
        },
        gym::trading::config::EnvConfig,
        sim::{
            cursor_group::CursorGroup,
            data::{SimulationData, SimulationDataBuilder},
        },
    };
    use std::panic::{self, AssertUnwindSafe};

    // ========================================================================
    // 0. The "Invariant Checker"
    // ========================================================================

    fn check_invariants(states: &States) {
        let mut counted_states = 0;

        // 1. Forward Check: Index -> Data (Live only)
        for (uid, (m_id, idx)) in &states.live_index {
            let list = states
                .live
                .get(m_id)
                .expect("Index points to missing Market ID");
            let state = list.get(*idx).expect("Index points to OOB vector slot");

            assert_eq!(
                state.trade_id(),
                *uid,
                "Index mismatch! Map says uid {uid:?} is at idx {idx} but found {:?}",
                state.trade_id()
            );
            counted_states += 1;
        }

        // 2. Backward Check: Live count must match index count
        // (Archive is not indexed)
        let total_live = states.live.values().map(|v| v.len()).sum::<usize>();

        assert_eq!(
            counted_states, total_live,
            "Orphaned states detected in live vectors"
        );
    }

    // ========================================================================
    // Test Setup & Helpers
    // ========================================================================

    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    fn t0() -> DateTime<Utc> {
        ts("2026-01-26T11:30:00Z")
    }

    fn mock_market() -> MarketId {
        MarketId {
            exchange: Exchange::Binance,
            symbol: Symbol::Spot(SpotPair::BtcUsdt),
            broker: DataBroker::Binance,
        }
    }

    fn mock_pending_trade(uid: i64) -> State {
        State::Pending(Trade {
            uid: TradeId(uid),
            agent_id: AgentIdentifier::Random,
            trade_type: TradeType::Long,
            quantity: Quantity(1.0),
            stop_loss: None,
            take_profit: None,
            state: Pending {
                created_at: t0(),
                limit_price: Price(100.0),
            },
        })
    }

    fn mock_canceled_trade(uid: i64) -> State {
        State::Canceled(Trade {
            uid: TradeId(uid),
            agent_id: AgentIdentifier::Random,
            trade_type: TradeType::Long,
            quantity: Quantity(1.0),
            stop_loss: None,
            take_profit: None,
            state: Canceled {
                created_at: t0(),
                canceled_at: t0() + Duration::minutes(10),
                limit_price: Price(100.0),
            },
        })
    }

    /// Creates an Active trade.
    /// Backdates timestamp to ensure the update logic processes the new candle.
    fn mock_active_trade(uid: i64, qty: f64) -> State {
        // Backdate by 1 minute so the update logic perceives a time delta
        let ts = t0() - Duration::minutes(1);

        State::Active(Trade {
            uid: TradeId(uid),
            agent_id: AgentIdentifier::Random,
            trade_type: TradeType::Long,
            quantity: Quantity(qty),
            stop_loss: None,
            take_profit: None,
            state: Active {
                entry_ts: ts,
                entry_price: Price(100.0),
                current_ts: ts,
                current_price: Price(100.0),
                unrealized_pnl: 0.0,
            },
        })
    }

    /// Helper to create a trade with a specific Stop Loss.
    fn mock_active_trade_with_sl(uid: i64, sl: f64) -> State {
        let mut s = mock_active_trade(uid, 1.0);
        if let State::Active(ref mut t) = s {
            t.stop_loss = Some(Price(sl));
        }
        s
    }

    fn setup_ledger(ids: Vec<i64>) -> (States, MarketId) {
        let m_id = mock_market();
        let mut states = States::with_capacity(&[m_id], 10);
        for id in ids {
            states.insert_new_live(m_id, mock_pending_trade(id));
        }
        check_invariants(&states);
        (states, m_id)
    }

    /// A lightweight wrapper around the heavy SimulationData.
    struct MarketFixture {
        sim_data: SimulationData,
        cursor: CursorGroup,
    }

    impl MarketFixture {
        fn new(
            target: MarketId,
            timestamp: DateTime<Utc>,
            low: f64,
            high: f64,
            close: f64,
        ) -> Self {
            let id = OhlcvId {
                broker: target.broker,
                exchange: target.exchange,
                symbol: target.symbol,
                period: Period::Minute(1),
            };

            let candle = Ohlcv {
                open_timestamp: timestamp,
                close_timestamp: timestamp + chrono::Duration::minutes(1),
                open: Price((low + high) / 2.0),
                high: Price(high),
                low: Price(low),
                close: Price(close),
                volume: Quantity(1000.0),
                quote_asset_volume: None,
                number_of_trades: None,
                taker_buy_base_asset_volume: None,
                taker_buy_quote_asset_volume: None,
            };

            let mut map = SortedVecMap::new();
            map.insert(id, vec![candle].into_boxed_slice());

            let sim_data = SimulationDataBuilder::new()
                .with_ohlcv(map)
                .build(EnvConfig::default())
                .expect("Failed to build sim data");

            let cursor = CursorGroup::new(&sim_data).expect("Failed to create cursor");

            Self { sim_data, cursor }
        }

        fn view(&self) -> MarketView<'_> {
            MarketView::new(&self.sim_data, &self.cursor).unwrap()
        }
    }

    // ========================================================================
    // Part 1: State Guard Transactions
    // ========================================================================

    #[test]
    fn test_guard_acquisition_clones_state_without_modifying_vector() {
        let (mut states, m_id) = setup_ledger(vec![10, 20, 30]);

        // PRE-CHECK: Establish baseline truth
        // We know from setup that Qty is 1.0, but let's be explicit.
        let original_ptr = states.live.get(&m_id).unwrap().as_ptr();
        let original_val = states.get_by_id(&TradeId(20)).unwrap().clone();
        assert_eq!(original_val.quantity().0, 1.0);
        check_invariants(&states);

        // Action: Checkout Trade(20) at index 1.
        {
            let guard = StateGuard::new(&mut states, m_id, 1).expect("Guard failed");

            // CHECK 1: Guard holds a perfect CLONE
            assert_eq!(guard.get().trade_id().0, 20);
            assert_eq!(guard.get().quantity().0, 1.0);

            // CHECK 2: Vector is UNTOUCHED
            // The original state "20" is still sitting in the vector.
            assert_eq!(guard.live_vec.len(), 3);
            assert_eq!(guard.live_vec[1].trade_id().0, 20);
            assert_eq!(guard.live_vec[1].quantity().0, 1.0); // Data integrity check

            // CHECK 3: Memory Stability (Optional but powerful)
            // Prove that the vector didn't reallocate or shift
            assert_eq!(
                guard.live_vec.as_ptr(),
                original_ptr,
                "Vector memory should be stable"
            );
        } // Drop happens here (Rollback)

        // Post-condition: Everything intact
        check_invariants(&states);

        // Final sanity check: The data is still there after drop
        let final_state = states.get_by_id(&TradeId(20)).unwrap();
        assert_eq!(final_state.quantity().0, 1.0);
        check_invariants(&states);
    }

    #[test]
    fn test_guard_commit_live_updates_in_place() {
        let (mut states, m_id) = setup_ledger(vec![10, 20, 30]);

        // PRE-CHECK: Verify initial quantity is 1.0
        let original = states.get_by_id(&TradeId(20)).unwrap();
        assert_eq!(original.quantity().0, 1.0, "Pre-condition failed");
        check_invariants(&states);

        // Checkout Trade(20) at index 1
        let guard = StateGuard::new(&mut states, m_id, 1).unwrap();

        // Update: Stay Pending (Active -> Active)
        let mut modified = mock_pending_trade(20);
        if let State::Pending(ref mut t) = modified {
            t.quantity = Quantity(999.0); // CHANGE DATA
        }

        guard.commit(modified);

        // CHECK 1: Vector size unchanged
        let vec = states.live.get(&m_id).unwrap();
        assert_eq!(vec.len(), 3);

        // CHECK 2: Order preserved (Stable Update)
        assert_eq!(vec[0].trade_id().0, 10);
        assert_eq!(vec[1].trade_id().0, 20);
        assert_eq!(vec[2].trade_id().0, 30);

        // CHECK 3: Data was actually mutated
        assert_eq!(vec[1].quantity().0, 999.0, "Update was lost!");

        // CHECK 4: Index Map unchanged
        assert_eq!(states.get_index(&TradeId(20)).unwrap().1, 1);

        check_invariants(&states);
    }

    #[test]
    fn test_guard_commit_closed_moves_to_archive() {
        let (mut states, m_id) = setup_ledger(vec![10, 20, 30]);

        // PRE-CHECK: Archive must be empty initially
        // This proves that the trade we find later definitely came from OUR action.
        assert!(
            states.archive.get(&m_id).unwrap_or(&vec![]).is_empty(),
            "Archive should start empty"
        );
        check_invariants(&states);

        // Checkout Trade(20) at index 1
        let guard = StateGuard::new(&mut states, m_id, 1).unwrap();

        // Action: Commit a Canceled state (Active -> Archive)
        let canceled_state = mock_canceled_trade(20);

        guard.commit(canceled_state);

        // CHECK 1: Active Vector shrank
        // Trade 20 removed. Trade 30 (end) moved to index 1.
        let active_vec = states.live.get(&m_id).unwrap();
        assert_eq!(active_vec.len(), 2);
        assert_eq!(active_vec[0].trade_id().0, 10);
        assert_eq!(active_vec[1].trade_id().0, 30); // 30 swapped into 1

        // CHECK 2: Archive Vector grew
        let archive_vec = states.archive.get(&m_id).unwrap();
        assert_eq!(archive_vec.len(), 1);
        assert_eq!(archive_vec[0].trade_id().0, 20);

        // CHECK 3: Indices updated
        // Trade 30 swapped to index 1
        assert_eq!(states.get_index(&TradeId(30)).unwrap().1, 1);
        // Trade 20 removed from index (archived trades are NOT indexed)
        assert!(states.get_index(&TradeId(20)).is_err());

        check_invariants(&states);
    }

    #[test]
    fn test_guard_commit_swap_remove_from_start_verifies_topology() {
        // Setup: [10, 20, 30]
        let (mut states, m_id) = setup_ledger(vec![10, 20, 30]);

        // PRE-CHECK
        assert!(states.archive.get(&m_id).unwrap_or(&vec![]).is_empty());
        let initial_vec = states.live.get(&m_id).unwrap();
        assert_eq!(initial_vec[0].trade_id().0, 10);
        assert_eq!(initial_vec[2].trade_id().0, 30); // The element that will jump
        check_invariants(&states);

        // Action: Checkout Trade(10) at Index 0 (The Head)
        let guard = StateGuard::new(&mut states, m_id, 0).unwrap();

        // Update: Cancel it (Live -> Archive)
        guard.commit(mock_canceled_trade(10));

        // CHECK 1: Topology Verification
        // swap_remove(0) should take the LAST element (30) and put it at 0.
        // The resulting vector should be [30, 20].
        let active_vec = states.live.get(&m_id).unwrap();
        assert_eq!(active_vec.len(), 2);

        // Crucial Assertion: Did 30 jump to the front?
        assert_eq!(
            active_vec[0].trade_id().0,
            30,
            "Last element (30) must move to Index 0"
        );
        assert_eq!(
            active_vec[1].trade_id().0,
            20,
            "Middle element (20) must stay at Index 1"
        );

        // CHECK 2: Index Integrity
        // Trade 30 must now point to Index 0
        assert_eq!(
            states.get_index(&TradeId(30)).unwrap().1,
            0,
            "Index for 30 not updated"
        );
        // Trade 20 must still point to Index 1
        assert_eq!(
            states.get_index(&TradeId(20)).unwrap().1,
            1,
            "Index for 20 should be stable"
        );
        // Trade 10 must be gone
        assert!(states.get_index(&TradeId(10)).is_err());

        // CHECK 3: Archive
        let archive_vec = states.archive.get(&m_id).unwrap();
        assert_eq!(archive_vec[0].trade_id().0, 10);

        check_invariants(&states);
    }

    #[test]
    fn test_panic_safety_leaves_vector_untouched() {
        let (mut states, m_id) = setup_ledger(vec![10, 20]);
        let original_qty = states.get_by_id(&TradeId(20)).unwrap().quantity();

        // PRE-CHECK: Confirm it starts at Index 1
        assert_eq!(
            states.get_index(&TradeId(20)).unwrap().1,
            1,
            "Pre-condition: Trade 20 must start at idx 1"
        );
        check_invariants(&states);

        let result = panic::catch_unwind(AssertUnwindSafe(|| {
            let _ = states.modify_state_at(m_id, 1, |state| -> ChapatyResult<Transition<()>> {
                assert_eq!(state.trade_id().0, 20);

                panic!(
                    "\n\n\
                        ┌───────────────────────────────────────────────────────┐\n\
                        │ **[TEST EXPECTED]:** Intentional Panic Triggered.     │\n\
                        │ PURPOSE: Simulating crash to verify Clone Safety.     │\n\
                        │ STATUS: If you see this, the test harness is working. │\n\
                        └───────────────────────────────────────────────────────┘\n\n"
                );
            });
        }));

        assert!(result.is_err(), "Should have caught the panic");

        check_invariants(&states);

        // Verify Data Integrity
        let state = states.get_by_id(&TradeId(20)).unwrap();
        assert_eq!(state.quantity(), original_qty);

        // CHECK: It is STILL at index 1
        assert_eq!(states.get_index(&TradeId(20)).unwrap().1, 1);
        check_invariants(&states);
    }

    #[test]
    fn test_modify_logic_error_aborts_transaction() {
        let (mut states, m_id) = setup_ledger(vec![10, 20]);

        // PRE-CHECK: Establish the Baseline
        // We must prove it starts at 1.0 to prove it was 'restored' to 1.0
        let original = states.live.get(&m_id).unwrap().first().unwrap();
        assert_eq!(
            original.quantity().0,
            1.0,
            "Pre-condition: Quantity must start at 1.0"
        );
        check_invariants(&states);

        // Action: Attempt modification that fails
        let _ = states.modify_state_at(m_id, 0, |mut s| -> ChapatyResult<Transition<()>> {
            // Mutate the local CLONE
            if let State::Pending(ref mut t) = s {
                t.quantity = Quantity(99999.0);
            }
            // Return Error (Abort Transaction)
            Err(AgentError::InvalidInput("No way".to_string()).into())
        });

        check_invariants(&states);

        // Verify the vector state was NOT updated (Rollback successful)
        let current = states.live.get(&m_id).unwrap().first().unwrap();

        // It should NOT be the mutated value
        assert_ne!(
            current.quantity().0,
            99999.0,
            "Transaction should not have committed"
        );
        // It SHOULD be the original value
        assert_eq!(
            current.quantity().0,
            1.0,
            "State should have rolled back to 1.0"
        );
        check_invariants(&states);
    }

    #[test]
    fn test_guard_commit_last_element_removal_safe() {
        // Setup: [10, 20, 30]
        // We use 3 elements to prove that the prefix [10, 20] remains stable.
        let (mut states, m_id) = setup_ledger(vec![10, 20, 30]);

        // PRE-CHECK 1: Archive empty
        assert!(states.archive.get(&m_id).unwrap_or(&vec![]).is_empty());

        // PRE-CHECK 2: Verify Topology
        let initial_vec = states.live.get(&m_id).unwrap();
        assert_eq!(initial_vec.len(), 3);
        assert_eq!(
            initial_vec[2].trade_id().0,
            30,
            "Trade 30 must be at the end"
        );
        check_invariants(&states);

        // Action: Checkout Trade(30) at index 2 (The LAST element)
        let guard = StateGuard::new(&mut states, m_id, 2).unwrap();

        // Update: Cancel it (Live -> Archive)
        let canceled = mock_canceled_trade(30);
        guard.commit(canceled);

        // CHECK 1: Live vector shrank correctly
        let live_vec = states.live.get(&m_id).unwrap();
        assert_eq!(live_vec.len(), 2);

        // CHECK 2: Stability (The Critical Check)
        // Verify that NEITHER 10 nor 20 moved.
        // This proves the operation acted as a stable 'pop', avoiding the reordering of a typical swap_remove.
        assert_eq!(
            live_vec[0].trade_id().0,
            10,
            "Trade 10 should be stable at 0"
        );
        assert_eq!(
            live_vec[1].trade_id().0,
            20,
            "Trade 20 should be stable at 1"
        );

        // CHECK 3: Archive has the trade
        let archive_vec = states.archive.get(&m_id).unwrap();
        assert_eq!(archive_vec.len(), 1);
        assert_eq!(archive_vec[0].trade_id().0, 30);

        // CHECK 4: Indices
        assert_eq!(states.get_index(&TradeId(10)).unwrap().1, 0);
        assert_eq!(states.get_index(&TradeId(20)).unwrap().1, 1);
        // Trade 30 removed from index
        assert!(states.get_index(&TradeId(30)).is_err());

        check_invariants(&states);
    }

    #[test]
    fn test_guard_new_returns_error_on_oob_index() {
        let (mut states, m_id) = setup_ledger(vec![10]);
        check_invariants(&states);

        // Action: Try to acquire index 99
        let result = StateGuard::new(&mut states, m_id, 99);

        // CHECK: Returns explicit Error, does not Panic
        assert!(result.is_err());
        match result.unwrap_err() {
            ChapatyError::System(SystemError::IndexOutOfBounds(_)) => {}  // Expected
            e => panic!("Expected IndexOutOfBounds, got {:?}", e),
        }
        check_invariants(&states);
    }

    // ========================================================================
    // Part 2: Financial Metrics & Invariants
    // ========================================================================

    #[test]
    fn test_pnl_accumulation() {
        let (mut states, _) = setup_ledger(vec![]);

        // PRE-CHECK: Start at absolute zero
        assert_eq!(states.step_reward, 0.0);
        assert_eq!(states.cumulative_pnl, 0.0);
        check_invariants(&states);

        // Record first PnL change
        states.record_pnl_change(100.0);
        assert_eq!(states.step_reward, 100.0);
        assert_eq!(states.cumulative_pnl, 100.0);

        // Record second PnL change - both accumulate
        states.record_pnl_change(50.5);
        assert_eq!(states.step_reward, 150.5);
        assert_eq!(states.cumulative_pnl, 150.5);

        // Record negative PnL (loss)
        states.record_pnl_change(-30.0);
        assert_eq!(states.step_reward, 120.5);
        assert_eq!(states.cumulative_pnl, 120.5);

        check_invariants(&states);
    }

    #[test]
    fn test_pop_reward_resets_step_only_and_handles_rounding() {
        let (mut states, _) = setup_ledger(vec![]);

        // Setup: Accumulate 400.6 (Should round up to 401)
        states.record_pnl_change(400.6);

        // PRE-CHECK: Verify float state before pop
        assert_eq!(states.step_reward, 400.6);
        assert_eq!(states.cumulative_pnl, 400.6);
        check_invariants(&states);

        // Action: Pop the reward
        let reward = states.pop_reward();

        // CHECK 1: Rounding Logic (round() vs floor cast)
        // If implementation is `as i64` (truncate), this is 400.
        // If implementation is `round() as i64`, this is 401.
        assert_eq!(reward.0, 401, "Reward should round to nearest integer");

        // CHECK 2: Step Reset (Transient)
        assert_eq!(states.step_reward, 0.0);

        // CHECK 3: Persistence (Cumulative is UNTOUCHED)
        assert_eq!(states.cumulative_pnl, 400.6);

        // Action 2: Pop empty
        let reward2 = states.pop_reward();
        assert_eq!(reward2.0, 0);

        check_invariants(&states);
    }

    #[test]
    fn test_all_closed_logic_verifies_structural_emptiness() {
        let m_id = mock_market();

        // Case 1: Empty States
        let empty_states = States::with_capacity(&[m_id], 10);
        assert!(empty_states.all_closed());
        // STRUCTURAL CHECK:
        assert!(empty_states.live.get(&m_id).unwrap().is_empty());
        check_invariants(&empty_states);

        // Case 2: One active trade
        let (states_with_active, _) = setup_ledger(vec![10]);
        assert!(!states_with_active.all_closed());
        // STRUCTURAL CHECK:
        assert_eq!(states_with_active.live.get(&m_id).unwrap().len(), 1);
        check_invariants(&states_with_active);

        // Case 3: Only archived trades (The tricky one)
        let mut states = States::with_capacity(&[m_id], 10);
        states.archive_state(m_id, mock_canceled_trade(99));

        // Logic says true...
        assert!(states.all_closed(), "Should report closed");

        // ...But is it REALLY empty in the Hot Path?
        // This ensures archive_state didn't accidentally push to 'live'
        assert!(
            states.live.get(&m_id).unwrap().is_empty(),
            "Live vector must be strictly empty"
        );
        // And confirms Archive has data
        assert_eq!(states.archive.get(&m_id).unwrap().len(), 1);
        check_invariants(&states);
    }

    // ========================================================================
    // Part 3: Command Handlers (Business Logic)
    // ========================================================================

    #[test]
    fn test_handle_open_duplicate_prevention() {
        let m_id = mock_market();

        let mut states = States::with_capacity(&[m_id], 10);
        let fixture = MarketFixture::new(m_id, t0(), 90.0, 110.0, 100.0);
        let market = fixture.view();

        // First open with UID 42 (limit order to avoid price lookup)
        let cmd1 = OpenCmd {
            agent_id: AgentIdentifier::Random,
            trade_id: TradeId(42),
            trade_type: TradeType::Long,
            quantity: Quantity(1.0),
            entry_price: Some(Price(100.0)), // Limit order
            stop_loss: None,
            take_profit: None,
        };

        // PRE-CHECK: System is empty
        assert!(states.get_by_id(&TradeId(42)).is_none());
        check_invariants(&states);

        let result1 = states.open(m_id, cmd1, &market);
        assert!(result1.is_ok(), "First open should succeed");

        // PRE-CHECK 2: Confirm first trade is settled
        let trade = states.get_by_id(&TradeId(42)).unwrap();
        assert_eq!(trade.quantity().0, 1.0);
        check_invariants(&states);

        // Second open with SAME UID 42 - should FAIL
        let cmd2 = OpenCmd {
            agent_id: AgentIdentifier::Random,
            trade_id: TradeId(42), // Duplicate!
            trade_type: TradeType::Long,
            quantity: Quantity(2.0),
            entry_price: Some(Price(110.0)),
            stop_loss: None,
            take_profit: None,
        };

        let result2 = states.open(m_id, cmd2, &market);
        assert!(result2.is_err(), "Duplicate UID should be rejected");

        // Verify original trade is untouched
        let trade_after = states.get_by_id(&TradeId(42)).unwrap();
        assert_eq!(trade_after.quantity().0, 1.0);
        check_invariants(&states);
    }

    #[test]
    fn test_handle_open_routing_limit_to_pending() {
        let m_id = mock_market();
        let mut states = States::with_capacity(&[m_id], 10);
        let fixture = MarketFixture::new(m_id, t0(), 90.0, 110.0, 100.0);
        let market = fixture.view();

        // PRE-CHECK: Trade must not exist
        assert!(states.get_by_id(&TradeId(1)).is_none());
        check_invariants(&states);

        // Limit Order (has entry_price) -> should go to Pending
        let limit_cmd = OpenCmd {
            agent_id: AgentIdentifier::Random,
            trade_id: TradeId(1),
            trade_type: TradeType::Long,
            quantity: Quantity(1.0),
            entry_price: Some(Price(50000.0)), // Limit price
            stop_loss: Some(Price(49000.0)),
            take_profit: Some(Price(51000.0)),
        };

        let result = states.open(m_id, limit_cmd, &market);
        assert!(result.is_ok(), "Limit order open should succeed");

        // Verify it's in Pending state
        let trade = states.get_by_id(&TradeId(1)).unwrap();
        assert!(trade.is_pending(), "Limit order should be Pending");

        // Verify it's in Hot Path (live) - index exists
        let (_, idx) = states.get_index(&TradeId(1)).unwrap();
        assert_eq!(idx, 0, "Should be at index 0");
        check_invariants(&states);
    }

    #[test]
    fn test_handle_modify_routing() {
        let (mut states, m_id) = setup_ledger(vec![10, 20]);

        // PRE-CHECK: Verify original entry price
        let original = states.get_by_id(&TradeId(10)).unwrap();
        assert_eq!(original.anticipated_entry_price().0, 100.0); // Based on mock default
        check_invariants(&states);

        // Case 1: Modify existing Pending trade - should succeed
        let modify_cmd = ModifyCmd {
            agent_id: AgentIdentifier::Random,
            trade_id: TradeId(10),
            new_entry_price: Some(Price(105.0)), // Pending can modify entry
            new_stop_loss: None,
            new_take_profit: None,
        };

        let result = states.modify(modify_cmd);
        assert!(result.is_ok(), "Modifying Pending trade should succeed");

        // Verify the entry price was updated
        let trade = states.get_by_id(&TradeId(10)).unwrap();
        assert_eq!(trade.anticipated_entry_price().0, 105.0);

        // Case 2: Modify non-existent trade - should fail
        let bad_cmd = ModifyCmd {
            agent_id: AgentIdentifier::Random,
            trade_id: TradeId(99999), // Does not exist
            new_entry_price: None,
            new_stop_loss: Some(Price(50.0)),
            new_take_profit: None,
        };

        let result2 = states.modify(bad_cmd);
        assert!(result2.is_err(), "Modifying non-existent trade should fail");

        // Case 3: Modify archived (Canceled) trade - should fail
        // First, archive a trade
        {
            // PRE-CHECK: Trade 10 is still Live before we archive it manually
            assert!(states.get_by_id(&TradeId(10)).is_some());
            let guard = StateGuard::new(&mut states, m_id, 0).unwrap();
            guard.commit(mock_canceled_trade(10));
            // Check manual archival worked
            assert!(states.get_by_id(&TradeId(10)).is_none());
            check_invariants(&states);
        }

        let archived_cmd = ModifyCmd {
            agent_id: AgentIdentifier::Random,
            trade_id: TradeId(10), // Now archived
            new_entry_price: None,
            new_stop_loss: Some(Price(50.0)),
            new_take_profit: None,
        };

        let result3 = states.modify(archived_cmd);
        assert!(
            result3.is_err(),
            "Modifying archived (Canceled) trade should fail"
        );
        check_invariants(&states);
    }

    #[test]
    fn test_handle_cancel_moves_to_archive() {
        let (mut states, m_id) = setup_ledger(vec![10, 20, 30]);
        let fixture = MarketFixture::new(m_id, t0(), 90.0, 110.0, 100.0);
        let market = fixture.view();

        // PRE-CHECK 1: Verify initial state - 3 trades in Live
        assert_eq!(states.live.get(&m_id).unwrap().len(), 3);
        assert_eq!(states.archive.get(&m_id).unwrap().len(), 0);

        // PRE-CHECK 2: Verify target existence
        assert!(states.get_by_id(&TradeId(20)).is_some());
        check_invariants(&states);

        // Cancel trade 20
        let cancel_cmd = CancelCmd {
            agent_id: AgentIdentifier::Random,
            trade_id: TradeId(20),
        };

        let result = states.cancel(cancel_cmd, &market);
        assert!(result.is_ok(), "Cancel should succeed");

        // Verify: Live should have 2, Archive should have 1
        assert_eq!(states.live.get(&m_id).unwrap().len(), 2);
        assert_eq!(states.archive.get(&m_id).unwrap().len(), 1);

        // Verify the canceled trade is in Archive (check directly, not via index)
        let archive_vec = states.archive.get(&m_id).unwrap();
        assert!(
            archive_vec[0].is_canceled(),
            "Trade should be Canceled state"
        );
        assert_eq!(archive_vec[0].trade_id().0, 20);

        // Verify trade is NO LONGER indexed (archived trades are not indexed)
        assert!(states.get_index(&TradeId(20)).is_err());

        check_invariants(&states);
    }

    #[test]
    fn test_handle_open_market_order_goes_immediately_to_active() {
        let m_id = mock_market();
        let mut states = States::with_capacity(&[m_id], 10);

        // SETUP: A valid market trading at 100.0
        let fixture = MarketFixture::new(m_id, t0(), 99.0, 101.0, 100.0);
        let market = fixture.view();

        // PRE-CHECK 1: Trade must not exist yet
        assert!(
            states.get_by_id(&TradeId(1)).is_none(),
            "Trade ID must be fresh"
        );

        // PRE-CHECK 2: Market vector should be empty
        assert!(states.live.get(&m_id).unwrap().is_empty());
        check_invariants(&states);

        // Market Order (No entry_price)
        let cmd = OpenCmd {
            agent_id: AgentIdentifier::Random,
            trade_id: TradeId(1),
            trade_type: TradeType::Long,
            quantity: Quantity(1.0),
            entry_price: None, // MARKET ORDER
            stop_loss: None,
            take_profit: None,
        };

        // Action
        let result = states.open(m_id, cmd, &market);
        assert!(
            result.is_ok(),
            "Market order should succeed with valid data"
        );

        // VERIFY
        // 1. Trade exists
        let trade = states.get_by_id(&TradeId(1)).unwrap();

        // 2. State is Active (Not Pending)
        assert!(trade.is_active(), "Market order must be Active immediately");

        // 3. Entry Price matches Market Close (100.0)
        let active_trade: Trade<Active> = trade.clone().try_into().unwrap();
        assert_eq!(active_trade.state().entry_price().0, 100.0);

        // 4. Index is correct
        assert_eq!(states.get_index(&TradeId(1)).unwrap().1, 0);
        check_invariants(&states);
    }

    #[test]
    fn test_handle_market_close_execution() {
        let m_id = mock_market();
        let mut states = States::with_capacity(&[m_id], 10);

        // SETUP: Market moves to 110.0 (Profit scenario)
        let fixture = MarketFixture::new(m_id, t0(), 109.0, 111.0, 110.0);
        let market = fixture.view();

        // 1. Insert Active Trade entered at 100.0 (Qty 1.0)
        states.insert_new_live(m_id, mock_active_trade(10, 1.0));

        // PRE-CHECK 1: Trade must be in Live
        assert!(states.get_by_id(&TradeId(10)).is_some());
        assert_eq!(states.live.get(&m_id).unwrap().len(), 1);

        // PRE-CHECK 2: Archive must be empty (Crucial!)
        assert!(
            states.archive.get(&m_id).unwrap_or(&vec![]).is_empty(),
            "Archive must start empty to prove the move happened"
        );

        // PRE-CHECK 3: PnL must be zero
        assert_eq!(states.pnl(), 0.0, "PnL must start at zero");
        check_invariants(&states);

        // 2. Command: Close at Market
        let cmd = MarketCloseCmd {
            agent_id: AgentIdentifier::Random,
            trade_id: TradeId(10),
            quantity: None, // Full close
        };

        // Action
        let result = states.market_close(cmd, &market);
        assert!(result.is_ok());

        // VERIFY
        // 1. Trade moved to Archive
        let archive = states.archive.get(&m_id).unwrap();
        assert_eq!(archive.len(), 1);
        let closed = &archive[0];

        // 2. Live is empty (since it was full close)
        assert!(states.live.get(&m_id).unwrap().is_empty());

        // 3. Exit Price matches Market (110.0)
        let close_trade: Trade<Closed> = closed.clone().try_into().unwrap();
        assert_eq!(close_trade.state().exit_price().0, 110.0);

        // 4. PnL captured correctly (Long: 110 - 100 = 10)
        assert_eq!(states.pnl(), 10.0);
        check_invariants(&states);
    }

    // ========================================================================
    // Part 4: The Critical Edge Case - Partial Close
    // ========================================================================

    #[test]
    fn test_handle_market_close_partial_split() {
        let m_id = mock_market();
        let mut states = States::with_capacity(&[m_id], 10);

        // SETUP: Market moves from 100 -> 110 (+10.0 per unit)
        let fixture = MarketFixture::new(m_id, t0(), 109.0, 111.0, 110.0);
        let market = fixture.view();

        // 1. Insert Active Trade (Qty 2.0, Entry 100.0)
        states.insert_new_live(m_id, mock_active_trade(10, 2.0));

        // PRE-CHECK: Verify baseline
        let initial = states.get_by_id(&TradeId(10)).unwrap();
        assert_eq!(initial.quantity().0, 2.0);
        assert_eq!(states.pnl(), 0.0);
        check_invariants(&states);

        // 2. Command: Close 0.5 Units (Partial)
        let cmd = MarketCloseCmd {
            agent_id: AgentIdentifier::Random,
            trade_id: TradeId(10),
            quantity: Some(Quantity(0.5)), // Explicit Partial Close
        };

        // Action
        let result = states.market_close(cmd, &market);
        assert!(result.is_ok());

        // VERIFY 1: The Remainder stays in Live (Hot Path)
        // Original Qty 2.0 - Closed 0.5 = 1.5
        let live_trade = states.get_by_id(&TradeId(10)).unwrap();
        assert_eq!(live_trade.quantity().0, 1.5, "Live trade should shrink");
        assert!(live_trade.is_active());

        // VERIFY 2: The Closed Portion goes to Archive (Cold Path)
        let archive = states.archive.get(&m_id).unwrap();
        assert_eq!(archive.len(), 1, "Archive should receive the split portion");
        let closed_portion = &archive[0];

        assert_eq!(closed_portion.trade_id().0, 10, "UIDs match");
        assert_eq!(
            closed_portion.quantity().0,
            0.5,
            "Archived qty matches command"
        );

        // VERIFY 3: PnL Calculation
        // Profit = (Exit 110 - Entry 100) * Closed Qty 0.5 = 5.0
        assert_eq!(
            states.pnl(),
            5.0,
            "PnL should only reflect the closed portion"
        );
        check_invariants(&states);
    }

    // ========================================================================
    // Part 5: Iteration Safety
    // ========================================================================

    #[test]
    fn test_update_all_live_trades_iterates_all_markets() {
        use crate::gym::trading::config::ExecutionBias;

        // 1. Define distinct markets
        let m1 = MarketId {
            exchange: Exchange::Binance,
            symbol: Symbol::Spot(SpotPair::BtcUsdt),
            broker: DataBroker::Binance,
        };
        let m2 = MarketId {
            exchange: Exchange::Binance,
            symbol: Symbol::Spot(SpotPair::EthUsdt),
            broker: DataBroker::Binance,
        };

        let mut states = States::with_capacity(&[m1, m2], 10);

        // 2. Insert trades
        states.insert_new_live(m1, mock_pending_trade(1));
        states.insert_new_live(m1, mock_pending_trade(2));
        states.insert_new_live(m2, mock_pending_trade(3));

        // PRE-CHECK 1: Verify exact population of Market 1
        let m1_vec = states.live.get(&m1).expect("Market 1 should exist");
        assert_eq!(m1_vec.len(), 2, "Market 1 must start with 2 trades");

        // PRE-CHECK 2: Verify exact population of Market 2
        let m2_vec = states.live.get(&m2).expect("Market 2 should exist");
        assert_eq!(m2_vec.len(), 1, "Market 2 must start with 1 trade");

        // PRE-CHECK 3: Total index count
        // This ensures no ID collisions or overwrites happened during insert
        assert_eq!(states.live_index.len(), 3);
        check_invariants(&states);

        // 3. Setup Context
        let m_id = mock_market(); // ID for the Context data (doesn't have to match m1/m2 for this test)
        let fixture = MarketFixture::new(m_id, t0(), 90.0, 110.0, 100.0);
        let market = fixture.view();
        let ctx = UpdateCtx {
            market: &market,
            bias: ExecutionBias::Pessimistic,
        };

        // 4. Action: Update Loop
        let mut seen_markets: Vec<MarketId> = Vec::new();
        let _ = states.update_all_live_trades(&ctx, |market_id, _result| {
            seen_markets.push(market_id);
            Ok(())
        });

        // 5. Verify Iteration Coverage
        let m1_count = seen_markets.iter().filter(|&&m| m == m1).count();
        let m2_count = seen_markets.iter().filter(|&&m| m == m2).count();

        // Exact Assertion: We expect exactly 2 visits for m1 and 1 for m2
        // because we have 2 and 1 trades respectively.
        assert_eq!(m1_count, 2, "Should visit Market 1 exactly twice");
        assert_eq!(m2_count, 1, "Should visit Market 2 exactly once");
        check_invariants(&states);
    }

    #[test]
    fn test_iterating_live_trades_handles_swap_remove_topology() {
        use crate::gym::trading::config::ExecutionBias;

        let m_id = mock_market();
        let mut states = States::with_capacity(&[m_id], 10);

        // SETUP:
        // We create a fixture where Price drops to 95.0.
        // We set High to 100.0 to simulate the drop starting from our entry.
        // We carefully set up 3 trades so that the 1st and 3rd SHOULD close.
        let fixture = MarketFixture::new(m_id, t0(), 94.0, 100.0, 95.0);
        let market = fixture.view();
        let ctx = UpdateCtx {
            market: &market,
            bias: ExecutionBias::Pessimistic,
        };

        // Trade 10: SL 99.0. Market 95.0 < 99.0. -> CLOSES.
        // Trade 20: SL 90.0. Market 95.0 > 90.0. -> STAYS.
        // Trade 30: SL 99.0. Market 95.0 < 99.0. -> CLOSES.
        states.insert_new_live(m_id, mock_active_trade_with_sl(10, 99.0));
        states.insert_new_live(m_id, mock_active_trade_with_sl(20, 90.0));
        states.insert_new_live(m_id, mock_active_trade_with_sl(30, 99.0));

        // PRE-CHECK 1: Verify Initial Topology [10, 20, 30]
        let vec = states.live.get(&m_id).unwrap();
        assert_eq!(vec.len(), 3);
        assert_eq!(vec[0].trade_id().0, 10);
        assert_eq!(vec[1].trade_id().0, 20);
        assert_eq!(vec[2].trade_id().0, 30);

        // PRE-CHECK 2: Verify Archives are empty
        assert!(states.archive.get(&m_id).unwrap_or(&vec![]).is_empty());
        check_invariants(&states);

        // Action: Update All
        let mut updates_count = 0;
        let result = states.update_all_live_trades(&ctx, |_mid, _res| {
            updates_count += 1;
            Ok(())
        });
        assert!(result.is_ok());

        // VERIFY TOPOLOGY
        let live = states.live.get(&m_id).unwrap();

        // 1. Only T20 should remain.
        // If the loop was buggy (incrementing index after swap), it would have skipped T30.
        assert_eq!(live.len(), 1, "Only T20 should remain active");
        assert_eq!(live[0].trade_id().0, 20, "T20 should be at index 0");

        // 2. T10 and T30 should be in archive
        let archive = states.archive.get(&m_id).unwrap();
        assert_eq!(archive.len(), 2, "Two trades should be archived");

        // 3. Verify Visit Count
        // It should have visited index 0 three times:
        // 1st iter: T10 (Closes, T30 swaps in)
        // 2nd iter: T30 (Closes, T20 swaps in)
        // 3rd iter: T20 (Stays, idx increments)
        assert_eq!(
            updates_count, 3,
            "Should visit all 3 trades despite removals"
        );
        check_invariants(&states);
    }
}
