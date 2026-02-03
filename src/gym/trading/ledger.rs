use std::mem;

use chrono::{DateTime, Utc};
use polars::{
    df,
    error::PolarsError,
    prelude::{DataFrame, DataType, IntoLazy, SortMultipleOptions, TimeUnit, col},
};

use crate::{
    agent::AgentIdentifier,
    data::{
        domain::{DataBroker, Exchange, MarketType, Price, Quantity, Symbol, Tick, TradeId},
        episode::{Episode, EpisodeId},
        event::MarketId,
    },
    error::{ChapatyError, ChapatyResult, DataError, SystemError},
    gym::{
        Reward,
        trading::{
            action::{Action, Command},
            context::{ActionCtx, ActionSummary, UpdateCtx},
            state::{State, States},
            types::{RiskRewardRatio, StateKind, TerminationReason, TradeType},
        },
    },
    report::journal::JournalCol,
};

/// The authoritative record of all trading states across an epoch.
///
/// The `Ledger` acts as both the active memory for the current episode
/// and the historical record for past episodes. It manages state transitions
/// and ensures account integrity.
#[derive(Debug, Clone, Default)]
pub(super) struct Ledger(Vec<States>);

impl Ledger {
    pub fn clear(&mut self) {
        self.0.iter_mut().for_each(|states| states.clear());
    }

    pub fn get(&self, episode: &Episode) -> ChapatyResult<&States> {
        self.0
            .get(episode.id().0)
            .ok_or_else(|| ep_not_found_err(episode))
    }

    pub fn get_mut(&mut self, episode: &Episode) -> ChapatyResult<&mut States> {
        self.0
            .get_mut(episode.id().0)
            .ok_or_else(|| ep_not_found_err(episode))
    }

    pub fn is_terminal(&self, episode: &Episode) -> ChapatyResult<bool> {
        Ok(self
            .0
            .get(episode.id().0)
            .ok_or_else(|| ep_not_found_err(episode))?
            .all_closed())
    }

    pub fn with_capacity(capacity: usize, states: States) -> Self {
        Self(vec![states; capacity])
    }

    /// Applies agent actions (Open, Close, Modify).
    /// Returns a report summarizing successes and failures for reward shaping.
    #[tracing::instrument(skip(self, ctx), fields(ep_id = %ep.id().0, ts = %ctx.market.current_timestamp()))]
    pub fn apply_actions(&mut self, ep: &Episode, ctx: ActionCtx) -> ChapatyResult<ActionSummary> {
        let mut report = ActionSummary::default();
        let market_view = ctx.market;

        let states = self.get_mut(ep)?;

        for (market_id, action) in ctx.actions.into_sorted_iter() {
            // A. Trace the Intent (Command Sourcing Requirement)
            let span = tracing::debug_span!(
                "cmd",
                market = ?market_id,
                agent = ?action.agent_id(),
                trade = ?action.trade_id(),
                payload = ?action
            );
            let _enter = span.enter();

            // B. Intrinsic Validation
            if let Err(e) = action.validate() {
                tracing::warn!(reason = "validation", error = %e, "Command Rejected");
                report.rejected += 1;
                continue;
            }

            // C. Application
            let result = match action {
                Action::Open(cmd) => states.open(market_id, cmd, &market_view),
                Action::Modify(cmd) => states.modify(cmd),
                Action::MarketClose(cmd) => states.market_close(cmd, &market_view),
                Action::Cancel(cmd) => states.cancel(cmd, &market_view),
            };

            // D. Trace the Outcome
            match result {
                Ok(_) => {
                    // For Command Sourcing, an "Applied" log confirms the state
                    // machine accepted the transition.
                    tracing::debug!(outcome = "applied", "State Transition Successful");
                    report.executed += 1;
                }
                Err(e) => {
                    // "Rejected" means this command had NO EFFECT on state.
                    tracing::warn!(
                        outcome = "rejected",
                        error = %e,
                        "Command Failed Business Logic"
                    );
                    report.rejected += 1;
                }
            }
        }

        Ok(report)
    }

    /// Performs Mark-to-Market updates on all active and pending positions.
    #[tracing::instrument(skip(self, ctx), fields(ep_id = %ep.id().0, ts = %ctx.market.current_timestamp()))]
    pub fn apply_updates(&mut self, ep: &Episode, ctx: UpdateCtx) -> ChapatyResult<()> {
        self.get_mut(ep)?
            .update_all_live_trades(&ctx, |m_id, result| {
                match result {
                    Ok(exit_event) => {
                        // Log Lifecycle Events
                        // For recovery validation, it helps to log the financial result here.
                        if let Some(reason) = exit_event {
                            // Access the trade state safely to log PnL (Optional but helpful)
                            // Note: In a real replay, you rely on deterministic market data
                            // to produce the same exit, but this log verifies it.
                            tracing::info!(
                                market = ?m_id,
                                reason = ?reason,
                                "Trade Finalized (Exit Triggered)"
                            );
                        }
                        Ok(())
                    }
                    Err(e) => {
                        tracing::error!(
                            market = ?m_id,
                            error = %e,
                            "CRITICAL: Mark-to-Market Calculation Failed"
                        );
                        // Return Err(e) to stop the whole loop.
                        Err(e)
                    }
                }
            })
    }

    /// Consumes the accumulated reward delta for the specific episode.
    pub fn pop_step_reward(&mut self, episode: &Episode) -> ChapatyResult<Reward> {
        self.get_mut(episode).map(States::pop_reward)
    }

    pub fn episode_pnl(&self, episode: &Episode) -> ChapatyResult<f64> {
        self.get(episode).map(States::pnl)
    }

    pub fn as_df(&self) -> ChapatyResult<DataFrame> {
        self.flattened()
            .try_map_ledger_entry_to_journal_entry()
            .try_collect_soa()?
            .try_into()
    }
}

impl Ledger {
    fn flattened(&self) -> impl Iterator<Item = LedgerEntry<'_>> {
        self.0.iter().enumerate().flat_map(|(ep, states)| {
            states
                .flattened()
                .map(move |(market_id, state)| LedgerEntry {
                    episode: EpisodeId(ep),
                    market_id,
                    state,
                })
        })
    }
}

// ================================================================================================
// Helper Structs
// ================================================================================================

struct LedgerEntry<'env> {
    pub episode: EpisodeId,
    pub market_id: &'env MarketId,
    pub state: &'env State,
}

struct JournalEntry {
    episode_id: EpisodeId,
    trade_id: TradeId,
    trade_state: StateKind,
    agent_id: AgentIdentifier,
    data_broker: DataBroker,
    exchange: Exchange,
    symbol: Symbol,
    market_type: MarketType,
    trade_type: TradeType,
    entry_price: Price,
    stop_loss: Option<Price>,
    take_profit: Option<Price>,
    quantity: Quantity,
    expected_loss_in_ticks: Option<Tick>,
    expected_profit_in_ticks: Option<Tick>,
    expected_loss_usd: Option<f64>,
    expected_profit_usd: Option<f64>,
    risk_reward_ratio: Option<RiskRewardRatio>,
    entry_timestamp: Option<DateTime<Utc>>,
    exit_timestamp: Option<DateTime<Utc>>,
    exit_price: Option<Price>,
    exit_reason: Option<TerminationReason>,
    realized_return_in_ticks: Tick,
    realized_return_dollars: f64,
}

/// Column-oriented, tabular representation of a ledger entry set.
///
/// This is the transposed struct of array (SoA) equivalent of `Vec<LedgerEntry>`,
/// optimized for columnar processing, serialization, and analysis.
#[derive(Default, Debug)]
struct JournalSoA {
    episode_id: Vec<EpisodeId>,
    trade_id: Vec<TradeId>,
    trade_state: Vec<StateKind>,
    agent_id: Vec<AgentIdentifier>,
    data_broker: Vec<DataBroker>,
    exchange: Vec<Exchange>,
    symbol: Vec<Symbol>,
    market_type: Vec<MarketType>,
    trade_type: Vec<TradeType>,
    entry_price: Vec<Price>,
    stop_loss: Vec<Option<Price>>,
    take_profit: Vec<Option<Price>>,
    quantity: Vec<Quantity>,
    expected_loss_in_ticks: Vec<Option<Tick>>,
    expected_profit_in_ticks: Vec<Option<Tick>>,
    expected_loss_usd: Vec<Option<f64>>,
    expected_profit_usd: Vec<Option<f64>>,
    risk_reward_ratio: Vec<Option<RiskRewardRatio>>,
    entry_timestamp: Vec<Option<DateTime<Utc>>>,
    exit_timestamp: Vec<Option<DateTime<Utc>>>,
    exit_price: Vec<Option<Price>>,
    exit_reason: Vec<Option<TerminationReason>>,
    realized_return_in_ticks: Vec<Tick>,
    realized_return_dollars: Vec<f64>,
}

impl JournalSoA {
    pub fn episode_ids(&mut self) -> ChapatyResult<Vec<u32>> {
        mem::take(&mut self.episode_id)
            .into_iter()
            .map(|ep| {
                ep.0.try_into().map_err(|e: std::num::TryFromIntError| {
                    ChapatyError::Data(DataError::DataFrame(e.to_string()))
                })
            })
            .collect()
    }

    pub fn trade_ids(&mut self) -> Vec<i64> {
        mem::take(&mut self.trade_id)
            .into_iter()
            .map(|t| t.0)
            .collect()
    }

    pub fn trade_states(&mut self) -> Vec<String> {
        mem::take(&mut self.trade_state)
            .into_iter()
            .map(|s| s.to_string())
            .collect()
    }

    pub fn agent_ids(&mut self) -> Vec<String> {
        mem::take(&mut self.agent_id)
            .into_iter()
            .map(|a| a.to_string())
            .collect()
    }

    pub fn data_brokers(&mut self) -> Vec<String> {
        mem::take(&mut self.data_broker)
            .into_iter()
            .map(|d| d.to_string())
            .collect()
    }

    pub fn exchanges(&mut self) -> Vec<String> {
        mem::take(&mut self.exchange)
            .into_iter()
            .map(|e| e.to_string())
            .collect()
    }

    pub fn symbols(&mut self) -> Vec<String> {
        mem::take(&mut self.symbol)
            .into_iter()
            .map(|s| s.to_string())
            .collect()
    }

    pub fn market_types(&mut self) -> Vec<String> {
        mem::take(&mut self.market_type)
            .into_iter()
            .map(|m| m.to_string())
            .collect()
    }

    pub fn trade_types(&mut self) -> Vec<String> {
        mem::take(&mut self.trade_type)
            .into_iter()
            .map(|t| t.to_string())
            .collect()
    }

    pub fn entry_prices(&mut self) -> Vec<f64> {
        mem::take(&mut self.entry_price)
            .into_iter()
            .map(|p| p.0)
            .collect()
    }

    pub fn stop_losses(&mut self) -> Vec<Option<f64>> {
        mem::take(&mut self.stop_loss)
            .into_iter()
            .map(|opt| opt.map(|p| p.0))
            .collect()
    }

    pub fn take_profits(&mut self) -> Vec<Option<f64>> {
        mem::take(&mut self.take_profit)
            .into_iter()
            .map(|opt| opt.map(|p| p.0))
            .collect()
    }

    pub fn quantities(&mut self) -> Vec<f64> {
        mem::take(&mut self.quantity)
            .into_iter()
            .map(|q| q.0)
            .collect()
    }

    pub fn expected_loss_ticks(&mut self) -> Vec<Option<i64>> {
        mem::take(&mut self.expected_loss_in_ticks)
            .into_iter()
            .map(|opt| opt.map(|t| t.0))
            .collect()
    }

    pub fn expected_profit_ticks(&mut self) -> Vec<Option<i64>> {
        mem::take(&mut self.expected_profit_in_ticks)
            .into_iter()
            .map(|opt| opt.map(|t| t.0))
            .collect()
    }

    pub fn expected_loss_usd(&mut self) -> Vec<Option<f64>> {
        mem::take(&mut self.expected_loss_usd)
    }

    pub fn expected_profit_usd(&mut self) -> Vec<Option<f64>> {
        mem::take(&mut self.expected_profit_usd)
    }

    pub fn risk_reward_ratios(&mut self) -> Vec<Option<f64>> {
        mem::take(&mut self.risk_reward_ratio)
            .into_iter()
            .map(|opt| opt.map(|r| r.ratio()))
            .collect()
    }

    pub fn entry_timestamps_micros(&mut self) -> Vec<Option<i64>> {
        mem::take(&mut self.entry_timestamp)
            .into_iter()
            .map(|opt| opt.map(|dt| dt.timestamp_micros()))
            .collect()
    }

    pub fn exit_timestamps_micros(&mut self) -> Vec<Option<i64>> {
        mem::take(&mut self.exit_timestamp)
            .into_iter()
            .map(|opt| opt.map(|dt| dt.timestamp_micros()))
            .collect()
    }

    pub fn exit_prices(&mut self) -> Vec<Option<f64>> {
        mem::take(&mut self.exit_price)
            .into_iter()
            .map(|opt| opt.map(|p| p.0))
            .collect()
    }

    pub fn exit_reasons(&mut self) -> Vec<Option<String>> {
        mem::take(&mut self.exit_reason)
            .into_iter()
            .map(|opt| opt.map(|r| r.to_string()))
            .collect()
    }

    pub fn realized_return_ticks(&mut self) -> Vec<i64> {
        mem::take(&mut self.realized_return_in_ticks)
            .into_iter()
            .map(|t| t.0)
            .collect()
    }

    pub fn realized_return_usd(&mut self) -> Vec<f64> {
        mem::take(&mut self.realized_return_dollars)
    }
}

// ================================================================================================
// Trait Extensions
// ================================================================================================

trait LogEntryTryMapExt<'a>: Iterator<Item = LedgerEntry<'a>> + Sized {
    fn try_map_ledger_entry_to_journal_entry(
        self,
    ) -> impl Iterator<Item = ChapatyResult<JournalEntry>> {
        self.map(TryInto::try_into)
    }
}

impl<'a, I: Iterator<Item = LedgerEntry<'a>>> LogEntryTryMapExt<'a> for I {}

trait TryCollectJournalSoA: Iterator<Item = ChapatyResult<JournalEntry>> + Sized {
    fn try_collect_soa(self) -> ChapatyResult<JournalSoA> {
        let mut soa = JournalSoA::default();

        for result in self {
            let JournalEntry {
                episode_id,
                trade_id,
                trade_state,
                agent_id,
                data_broker,
                exchange,
                symbol,
                market_type,
                trade_type,
                entry_price,
                stop_loss,
                take_profit,
                quantity,
                expected_loss_in_ticks,
                expected_profit_in_ticks,
                expected_loss_usd,
                expected_profit_usd,
                risk_reward_ratio,
                entry_timestamp,
                exit_timestamp,
                exit_price,
                exit_reason,
                realized_return_in_ticks,
                realized_return_dollars,
            } = result?;

            soa.episode_id.push(episode_id);
            soa.trade_id.push(trade_id);
            soa.trade_state.push(trade_state);
            soa.agent_id.push(agent_id);
            soa.data_broker.push(data_broker);
            soa.exchange.push(exchange);
            soa.symbol.push(symbol);
            soa.market_type.push(market_type);
            soa.trade_type.push(trade_type);
            soa.entry_price.push(entry_price);
            soa.stop_loss.push(stop_loss);
            soa.take_profit.push(take_profit);
            soa.quantity.push(quantity);
            soa.expected_loss_in_ticks.push(expected_loss_in_ticks);
            soa.expected_profit_in_ticks.push(expected_profit_in_ticks);
            soa.expected_loss_usd.push(expected_loss_usd);
            soa.expected_profit_usd.push(expected_profit_usd);
            soa.risk_reward_ratio.push(risk_reward_ratio);
            soa.entry_timestamp.push(entry_timestamp);
            soa.exit_timestamp.push(exit_timestamp);
            soa.exit_price.push(exit_price);
            soa.exit_reason.push(exit_reason);
            soa.realized_return_in_ticks.push(realized_return_in_ticks);
            soa.realized_return_dollars.push(realized_return_dollars);
        }

        Ok(soa)
    }
}

impl<I> TryCollectJournalSoA for I where I: Iterator<Item = ChapatyResult<JournalEntry>> {}

// ================================================================================================
// Type Conversions
// ================================================================================================

impl<'a> TryFrom<LedgerEntry<'a>> for JournalEntry {
    type Error = ChapatyError;

    fn try_from(log_entry: LedgerEntry<'a>) -> ChapatyResult<Self> {
        let market_id = log_entry.market_id;
        let symbol = &market_id.symbol;
        let state = log_entry.state;

        Ok(JournalEntry {
            // === Identifiers ===
            episode_id: log_entry.episode,
            trade_id: state.trade_id(),
            agent_id: state.agent_id().clone(),
            trade_state: state.into(),

            // === Market Context ===
            data_broker: market_id.broker,
            exchange: market_id.exchange,
            symbol: market_id.symbol,
            market_type: market_id.symbol.into(),

            // === Trade Data ===
            trade_type: *state.trade_type(),
            quantity: state.quantity(),

            // For Pending/Canceled, we use limit_price as the intended entry
            entry_price: state.anticipated_entry_price(),

            stop_loss: state.stop_loss(),
            take_profit: state.take_profit(),

            // === Risk / Reward Expectations (Delegated to helpers) ===
            // Assuming you implement these on State or use the existing logic
            expected_loss_in_ticks: state.expected_loss_in_ticks(symbol),
            expected_profit_in_ticks: state.expected_profit_in_ticks(symbol),

            expected_loss_usd: state.expected_loss_in_usd(symbol),
            expected_profit_usd: state.expected_profit_in_usd(symbol),

            // We pass 'symbol' here as required by the new signature
            risk_reward_ratio: state.risk_reward_ratio(symbol),

            // === Lifecycle Events (Using new state methods) ===
            // Note: No '?' needed, types match (Option -> Option)
            entry_timestamp: state.entry_ts(),
            exit_timestamp: state.exit_ts(),
            exit_price: state.exit_price(),
            exit_reason: state.exit_reason(),

            // === Realized Performance ===
            // Target is concrete (Tick/f64), Source is Option.
            // Default to 0 for Pending/Active trades.
            realized_return_in_ticks: state.pnl_ticks(symbol).unwrap_or(Tick(0)),
            realized_return_dollars: state.pnl_usd().unwrap_or(0.0),
        })
    }
}

impl TryFrom<JournalSoA> for DataFrame {
    type Error = ChapatyError;

    fn try_from(mut soa: JournalSoA) -> ChapatyResult<Self> {
        let df = df![
            // === Identifiers ===
            JournalCol::EpisodeId.to_string()               => soa.episode_ids()?,
            JournalCol::TradeId.to_string()                 => soa.trade_ids(),
            JournalCol::TradeState.to_string()              => soa.trade_states(),
            JournalCol::AgentId.to_string()                 => soa.agent_ids(),

            // === Market spec ===
            JournalCol::DataBroker.to_string()              => soa.data_brokers(),
            JournalCol::Exchange.to_string()                => soa.exchanges(),
            JournalCol::Symbol.to_string()                  => soa.symbols(),
            JournalCol::MarketType.to_string()              => soa.market_types(),
            JournalCol::TradeType.to_string()               => soa.trade_types(),

            // === Trade configuration ===
            JournalCol::EntryPrice.to_string()              => soa.entry_prices(),
            JournalCol::StopLossPrice.to_string()           => soa.stop_losses(),
            JournalCol::TakeProfitPrice.to_string()         => soa.take_profits(),
            JournalCol::Quantity.to_string()                => soa.quantities(),

            // === Expected outcomes ===
            JournalCol::ExpectedLossInTicks.to_string()     => soa.expected_loss_ticks(),
            JournalCol::ExpectedProfitInTicks.to_string()   => soa.expected_profit_ticks(),
            JournalCol::ExpectedLossDollars.to_string()     => soa.expected_loss_usd(),
            JournalCol::ExpectedProfitDollars.to_string()   => soa.expected_profit_usd(),
            JournalCol::RiskRewardRatio.to_string()         => soa.risk_reward_ratios(),

            // === Timestamps ===
            JournalCol::EntryTimestamp.to_string()          => soa.entry_timestamps_micros(),
            JournalCol::ExitTimestamp.to_string()           => soa.exit_timestamps_micros(),

            // === Realized outcomes ===
            JournalCol::ExitPrice.to_string()               => soa.exit_prices(),
            JournalCol::ExitReason.to_string()              => soa.exit_reasons(),
            JournalCol::RealizedReturnInTicks.to_string()   => soa.realized_return_ticks(),
            JournalCol::RealizedReturnDollars.to_string()   => soa.realized_return_usd(),
        ]
        .map_err(polars_to_chapaty_error)?;

        df.lazy()
            .with_columns([
                col(JournalCol::EntryTimestamp.to_string()).cast(DataType::Datetime(
                    TimeUnit::Microseconds,
                    Some(polars::prelude::TimeZone::UTC),
                )),
                col(JournalCol::ExitTimestamp.to_string()).cast(DataType::Datetime(
                    TimeUnit::Microseconds,
                    Some(polars::prelude::TimeZone::UTC),
                )),
            ])
            .sort(
                [&JournalCol::EntryTimestamp.to_string()],
                SortMultipleOptions::default().with_maintain_order(false),
            )
            .collect()
            .map_err(polars_to_chapaty_error)?
            .with_row_index(JournalCol::RowId.into(), None)
            .map_err(polars_to_chapaty_error)
    }
}

fn polars_to_chapaty_error(e: PolarsError) -> ChapatyError {
    DataError::DataFrame(e.to_string()).into()
}

fn ep_not_found_err(episode: &Episode) -> ChapatyError {
    ChapatyError::System(SystemError::IndexOutOfBounds(format!(
        "Episode {:?} not present in EpisodeLog",
        episode
    )))
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use polars::prelude::SchemaExt;
    use strum::IntoEnumIterator;

    use crate::{
        data::{
            domain::{Period, SpotPair},
            event::{Ohlcv, OhlcvId},
            view::MarketView,
        },
        gym::trading::config::EnvConfig,
        report::{io::ToSchema, journal::Journal},
        sim::{
            cursor_group::CursorGroup,
            data::{SimulationData, SimulationDataBuilder},
        },
        sorted_vec_map::SortedVecMap,
    };

    use super::*;

    // ============================================================================
    // Test Helpers
    // ============================================================================

    /// Parse RFC3339 timestamp
    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    /// A lightweight wrapper around the heavy SimulationData.
    /// It allows us to create a valid MarketView with a simple (low, high, close) API.
    struct MarketFixture {
        sim_data: SimulationData,
        cursor: CursorGroup,
    }

    impl MarketFixture {
        /// Creates a fixture that perfectly matches the target market ID.
        fn new(timestamp: DateTime<Utc>, low: f64, high: f64, close: f64) -> Self {
            // Map MarketId -> OhlcvId (Simplification for test)
            let id = OhlcvId {
                broker: DataBroker::Binance,
                exchange: Exchange::Binance,
                symbol: Symbol::Spot(SpotPair::BtcUsdt),
                period: Period::Minute(1),
            };

            // 1. Create Data
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

            // 2. Create Cursor (Auto-initialized to start)
            let cursor = CursorGroup::new(&sim_data).expect("Failed to create cursor");

            Self { sim_data, cursor }
        }

        fn view(&self) -> MarketView<'_> {
            MarketView::new(&self.sim_data, &self.cursor).unwrap()
        }
    }

    /// Creates a minimal JournalEntry for testing transformations.
    /// This is the core helper for white-box testing of JournalSoA.
    fn sample_journal_entry(
        episode: usize,
        trade_id: i64,
        trade_state: StateKind,
        realized_pnl: f64,
    ) -> JournalEntry {
        JournalEntry {
            episode_id: EpisodeId(episode),
            trade_id: TradeId(trade_id),
            trade_state,
            agent_id: AgentIdentifier::Random,
            data_broker: DataBroker::Binance,
            exchange: Exchange::Binance,
            symbol: Symbol::Spot(SpotPair::BtcUsdt),
            market_type: MarketType::Spot,
            trade_type: TradeType::Long,
            entry_price: Price(50000.0),
            stop_loss: Some(Price(49000.0)),
            take_profit: Some(Price(52000.0)),
            quantity: Quantity(1.0),
            expected_loss_in_ticks: Some(Tick(100)),
            expected_profit_in_ticks: Some(Tick(200)),
            expected_loss_usd: Some(1000.0),
            expected_profit_usd: Some(2000.0),
            risk_reward_ratio: Some(RiskRewardRatio::new(1000.0, 2000.0)),
            entry_timestamp: Some(ts("2026-01-19T10:00:00Z")),
            exit_timestamp: if trade_state == StateKind::Closed {
                Some(ts("2026-01-19T11:00:00Z"))
            } else {
                None
            },
            exit_price: if trade_state == StateKind::Closed {
                Some(Price(51000.0))
            } else {
                None
            },
            exit_reason: if trade_state == StateKind::Closed {
                Some(TerminationReason::MarketClose)
            } else {
                None
            },
            realized_return_in_ticks: Tick((realized_pnl / 10.0) as i64),
            realized_return_dollars: realized_pnl,
        }
    }

    /// Populates a JournalSoA with a single entry (helper to reduce boilerplate).
    fn populate_soa_single(soa: &mut JournalSoA, entry: JournalEntry) {
        soa.episode_id.push(entry.episode_id);
        soa.trade_id.push(entry.trade_id);
        soa.trade_state.push(entry.trade_state);
        soa.agent_id.push(entry.agent_id);
        soa.data_broker.push(entry.data_broker);
        soa.exchange.push(entry.exchange);
        soa.symbol.push(entry.symbol);
        soa.market_type.push(entry.market_type);
        soa.trade_type.push(entry.trade_type);
        soa.entry_price.push(entry.entry_price);
        soa.stop_loss.push(entry.stop_loss);
        soa.take_profit.push(entry.take_profit);
        soa.quantity.push(entry.quantity);
        soa.expected_loss_in_ticks
            .push(entry.expected_loss_in_ticks);
        soa.expected_profit_in_ticks
            .push(entry.expected_profit_in_ticks);
        soa.expected_loss_usd.push(entry.expected_loss_usd);
        soa.expected_profit_usd.push(entry.expected_profit_usd);
        soa.risk_reward_ratio.push(entry.risk_reward_ratio);
        soa.entry_timestamp.push(entry.entry_timestamp);
        soa.exit_timestamp.push(entry.exit_timestamp);
        soa.exit_price.push(entry.exit_price);
        soa.exit_reason.push(entry.exit_reason);
        soa.realized_return_in_ticks
            .push(entry.realized_return_in_ticks);
        soa.realized_return_dollars
            .push(entry.realized_return_dollars);
    }

    // ============================================================================
    // Part 1: Transformation Tests (White-Box for JournalSoA -> DataFrame)
    // ============================================================================

    #[test]
    fn test_journal_soa_to_dataframe_schema() {
        // Construct JournalSoA directly with test data
        let mut soa = JournalSoA::default();
        populate_soa_single(
            &mut soa,
            sample_journal_entry(0, 1, StateKind::Closed, 1000.0),
        );

        let df: DataFrame = soa
            .try_into()
            .expect("Failed to convert JournalSoA to DataFrame");

        // Verify all columns present
        let expected_cols: HashSet<_> = JournalCol::iter().map(|c| c.as_str()).collect();
        let actual_cols: HashSet<_> = df.get_column_names().iter().map(|s| s.as_str()).collect();
        assert_eq!(expected_cols, actual_cols, "Schema mismatch");

        // Verify row count
        assert_eq!(df.height(), 1);
    }

    #[test]
    fn test_journal_soa_field_values_closed_trade() {
        let mut soa = JournalSoA::default();
        populate_soa_single(
            &mut soa,
            sample_journal_entry(5, 42, StateKind::Closed, 500.0),
        );

        let df: DataFrame = soa.try_into().expect("Conversion failed");

        // Verify specific values
        let episode = df
            .column(JournalCol::EpisodeId.as_str())
            .unwrap()
            .u32()
            .unwrap()
            .get(0);
        assert_eq!(episode, Some(5));

        let trade_id = df
            .column(JournalCol::TradeId.as_str())
            .unwrap()
            .i64()
            .unwrap()
            .get(0);
        assert_eq!(trade_id, Some(42));

        let trade_state = df
            .column(JournalCol::TradeState.as_str())
            .unwrap()
            .str()
            .unwrap()
            .get(0);
        assert_eq!(trade_state, Some("closed"));

        let pnl = df
            .column(JournalCol::RealizedReturnDollars.as_str())
            .unwrap()
            .f64()
            .unwrap()
            .get(0);
        assert_eq!(pnl, Some(500.0));

        // Closed trades should have exit data
        let exit_price = df
            .column(JournalCol::ExitPrice.as_str())
            .unwrap()
            .f64()
            .unwrap()
            .get(0);
        assert!(exit_price.is_some());
    }

    #[test]
    fn test_journal_soa_field_values_active_trade() {
        let mut soa = JournalSoA::default();

        // Manually create an Active trade entry with no exit data
        let entry = JournalEntry {
            episode_id: EpisodeId(0),
            trade_id: TradeId(10),
            trade_state: StateKind::Active,
            agent_id: AgentIdentifier::Random,
            data_broker: DataBroker::Binance,
            exchange: Exchange::Binance,
            symbol: Symbol::Spot(SpotPair::EthUsdt),
            market_type: MarketType::Spot,
            trade_type: TradeType::Short,
            entry_price: Price(3000.0),
            stop_loss: None,
            take_profit: None,
            quantity: Quantity(2.5),
            expected_loss_in_ticks: None,
            expected_profit_in_ticks: None,
            expected_loss_usd: None,
            expected_profit_usd: None,
            risk_reward_ratio: None,
            entry_timestamp: Some(ts("2026-01-20T14:30:00Z")),
            exit_timestamp: None,
            exit_price: None,
            exit_reason: None,
            realized_return_in_ticks: Tick(0),
            realized_return_dollars: 0.0,
        };
        populate_soa_single(&mut soa, entry);

        let df: DataFrame = soa.try_into().expect("Conversion failed");

        let trade_state = df
            .column(JournalCol::TradeState.as_str())
            .unwrap()
            .str()
            .unwrap()
            .get(0);
        assert_eq!(trade_state, Some("active"));

        let trade_type = df
            .column(JournalCol::TradeType.as_str())
            .unwrap()
            .str()
            .unwrap()
            .get(0);
        assert_eq!(trade_type, Some("short"));

        let qty = df
            .column(JournalCol::Quantity.as_str())
            .unwrap()
            .f64()
            .unwrap()
            .get(0);
        assert_eq!(qty, Some(2.5));

        // Active trades should NOT have exit data
        let exit_price = df
            .column(JournalCol::ExitPrice.as_str())
            .unwrap()
            .f64()
            .unwrap()
            .get(0);
        assert_eq!(exit_price, None);

        let exit_reason = df
            .column(JournalCol::ExitReason.as_str())
            .unwrap()
            .str()
            .unwrap()
            .get(0);
        assert_eq!(exit_reason, None);
    }

    #[test]
    fn test_journal_soa_field_values_pending_trade() {
        let mut soa = JournalSoA::default();

        let entry = JournalEntry {
            episode_id: EpisodeId(1),
            trade_id: TradeId(30),
            trade_state: StateKind::Pending,
            agent_id: AgentIdentifier::Random,
            data_broker: DataBroker::Binance,
            exchange: Exchange::Binance,
            symbol: Symbol::Spot(SpotPair::BtcUsdt),
            market_type: MarketType::Spot,
            trade_type: TradeType::Long,
            entry_price: Price(48000.0), // Limit price
            stop_loss: None,
            take_profit: None,
            quantity: Quantity(1.0),
            expected_loss_in_ticks: None,
            expected_profit_in_ticks: None,
            expected_loss_usd: None,
            expected_profit_usd: None,
            risk_reward_ratio: None,
            entry_timestamp: None, // Pending has no entry timestamp
            exit_timestamp: None,
            exit_price: None,
            exit_reason: None,
            realized_return_in_ticks: Tick(0),
            realized_return_dollars: 0.0,
        };
        populate_soa_single(&mut soa, entry);

        let df: DataFrame = soa.try_into().expect("Conversion failed");

        let trade_state = df
            .column(JournalCol::TradeState.as_str())
            .unwrap()
            .str()
            .unwrap()
            .get(0);
        assert_eq!(trade_state, Some("pending"));

        // Pending should have limit price as entry price
        let entry_price = df
            .column(JournalCol::EntryPrice.as_str())
            .unwrap()
            .f64()
            .unwrap()
            .get(0);
        assert_eq!(entry_price, Some(48000.0));

        // Pending has no entry timestamp
        let entry_ts = df
            .column(JournalCol::EntryTimestamp.as_str())
            .unwrap()
            .datetime()
            .unwrap()
            .as_datetime_iter()
            .next()
            .flatten();
        assert!(entry_ts.is_none());
    }

    #[test]
    fn test_journal_soa_field_values_canceled_trade() {
        let mut soa = JournalSoA::default();

        let entry = JournalEntry {
            episode_id: EpisodeId(0),
            trade_id: TradeId(40),
            trade_state: StateKind::Canceled,
            agent_id: AgentIdentifier::Random,
            data_broker: DataBroker::Binance,
            exchange: Exchange::Binance,
            symbol: Symbol::Spot(SpotPair::BtcUsdt),
            market_type: MarketType::Spot,
            trade_type: TradeType::Long,
            entry_price: Price(47000.0),
            stop_loss: None,
            take_profit: None,
            quantity: Quantity(1.0),
            expected_loss_in_ticks: None,
            expected_profit_in_ticks: None,
            expected_loss_usd: None,
            expected_profit_usd: None,
            risk_reward_ratio: None,
            entry_timestamp: None,
            exit_timestamp: Some(ts("2026-01-19T12:00:00Z")),
            exit_price: None,
            exit_reason: Some(TerminationReason::Canceled),
            realized_return_in_ticks: Tick(0),
            realized_return_dollars: 0.0,
        };
        populate_soa_single(&mut soa, entry);

        let df: DataFrame = soa.try_into().expect("Conversion failed");

        let trade_state = df
            .column(JournalCol::TradeState.as_str())
            .unwrap()
            .str()
            .unwrap()
            .get(0);
        assert_eq!(trade_state, Some("canceled"));

        let exit_reason = df
            .column(JournalCol::ExitReason.as_str())
            .unwrap()
            .str()
            .unwrap()
            .get(0);
        assert_eq!(exit_reason, Some("canceled"));
    }

    #[test]
    fn test_journal_soa_multiple_rows() {
        let mut soa = JournalSoA::default();

        // Row 1: Closed trade (episode 0)
        populate_soa_single(
            &mut soa,
            sample_journal_entry(0, 1, StateKind::Closed, 1000.0),
        );

        // Row 2: Active trade (episode 0, later timestamp)
        let mut active_entry = sample_journal_entry(0, 2, StateKind::Active, 0.0);
        active_entry.entry_timestamp = Some(ts("2026-01-19T11:00:00Z"));
        active_entry.exit_timestamp = None;
        active_entry.exit_price = None;
        active_entry.exit_reason = None;
        populate_soa_single(&mut soa, active_entry);

        // Row 3: Pending trade (episode 1, no entry timestamp - tests null sorting)
        let mut pending_entry = sample_journal_entry(1, 3, StateKind::Pending, 0.0);
        pending_entry.entry_timestamp = None;
        pending_entry.exit_timestamp = None;
        pending_entry.exit_price = None;
        pending_entry.exit_reason = None;
        populate_soa_single(&mut soa, pending_entry);

        let df: DataFrame = soa.try_into().expect("Conversion failed");

        assert_eq!(df.height(), 3, "Should have 3 rows");

        // Verify episode distribution
        let episodes: Vec<u32> = df
            .column(JournalCol::EpisodeId.as_str())
            .unwrap()
            .u32()
            .unwrap()
            .into_iter()
            .flatten()
            .collect();
        assert_eq!(episodes.iter().filter(|&&e| e == 0).count(), 2);
        assert_eq!(episodes.iter().filter(|&&e| e == 1).count(), 1);
    }

    #[test]
    fn test_journal_soa_empty() {
        let soa = JournalSoA::default();
        let df: DataFrame = soa.try_into().expect("Empty SoA should convert");

        assert_eq!(df.height(), 0);

        // All columns should still be present
        let expected_cols: HashSet<_> = JournalCol::iter().map(|c| c.as_str()).collect();
        let actual_cols: HashSet<_> = df.get_column_names().iter().map(|s| s.as_str()).collect();
        assert_eq!(expected_cols, actual_cols);
    }

    #[test]
    fn test_journal_soa_data_types_match_schema() {
        let mut soa = JournalSoA::default();
        populate_soa_single(
            &mut soa,
            sample_journal_entry(0, 1, StateKind::Closed, 1000.0),
        );

        let df: DataFrame = soa.try_into().expect("Conversion failed");

        let expected_schema = Journal::to_schema();
        let actual_schema = df.schema();

        for field in expected_schema.iter_fields() {
            let col_name = field.name();
            let expected_dtype = field.dtype();
            let actual_dtype = actual_schema
                .get(col_name)
                .unwrap_or_else(|| panic!("Column '{}' missing", col_name));

            assert_eq!(
                actual_dtype, expected_dtype,
                "Type mismatch for '{}': expected {:?}, got {:?}",
                col_name, expected_dtype, actual_dtype
            );
        }
    }

    #[test]
    fn test_journal_soa_row_id_generated() {
        let mut soa = JournalSoA::default();
        populate_soa_single(
            &mut soa,
            sample_journal_entry(0, 1, StateKind::Closed, 100.0),
        );

        let mut entry2 = sample_journal_entry(0, 2, StateKind::Closed, 200.0);
        entry2.entry_timestamp = Some(ts("2026-01-19T11:00:00Z"));
        populate_soa_single(&mut soa, entry2);

        let df: DataFrame = soa.try_into().expect("Conversion failed");

        let row_id_col = df
            .column(JournalCol::RowId.as_str())
            .expect("RowId column missing");
        let row_ids: Vec<u32> = row_id_col.u32().unwrap().into_iter().flatten().collect();

        assert_eq!(
            row_ids,
            vec![0, 1],
            "Row IDs should be 0-indexed sequential"
        );
    }

    // ============================================================================
    // Part 2: Isolation Tests
    // ============================================================================

    #[test]
    fn test_episode_isolation_get() {
        // Create ledger with 3 episodes
        let ledger = Ledger::with_capacity(3, States::default());

        let ep0 = Episode::default();
        let ep1 = ep0.next(ts("2026-01-20T00:00:00Z"));
        let ep2 = ep1.next(ts("2026-01-21T00:00:00Z"));

        // Each episode should be independently accessible
        assert!(ledger.get(&ep0).is_ok());
        assert!(ledger.get(&ep1).is_ok());
        assert!(ledger.get(&ep2).is_ok());

        // Episode 3 (out of bounds) should error
        let ep3 = ep2.next(ts("2026-01-22T00:00:00Z"));
        assert!(ledger.get(&ep3).is_err());
    }

    #[test]
    fn test_episode_isolation_pnl() {
        let ledger = Ledger::with_capacity(2, States::default());

        let ep0 = Episode::default();
        let ep1 = ep0.next(ts("2026-01-20T00:00:00Z"));

        // PnL should be independent per episode
        let pnl0 = ledger.episode_pnl(&ep0).expect("Episode 0 should exist");
        let pnl1 = ledger.episode_pnl(&ep1).expect("Episode 1 should exist");

        assert_eq!(pnl0, 0.0, "Initial PnL should be 0");
        assert_eq!(pnl1, 0.0, "Initial PnL should be 0");
    }

    #[test]
    fn test_episode_isolation_terminal_state() {
        let ledger = Ledger::with_capacity(2, States::default());

        let ep0 = Episode::default();
        let ep1 = ep0.next(ts("2026-01-20T00:00:00Z"));

        // Both episodes should be terminal (empty = all closed)
        assert!(ledger.is_terminal(&ep0).unwrap());
        assert!(ledger.is_terminal(&ep1).unwrap());
    }

    #[test]
    fn test_ledger_clear_resets_all_episodes() {
        let mut ledger = Ledger::with_capacity(3, States::default());

        let ep0 = Episode::default();
        let ep1 = ep0.next(ts("2026-01-20T00:00:00Z"));

        // Ledger should be accessible before and after clear
        assert!(ledger.get(&ep0).is_ok());
        assert!(ledger.get(&ep1).is_ok());

        ledger.clear();

        // Still accessible after clear
        assert!(ledger.get(&ep0).is_ok());
        assert!(ledger.get(&ep1).is_ok());
    }

    // ============================================================================
    // Part 3: Transience Tests (pop_step_reward queue behavior)
    // ============================================================================

    #[test]
    fn test_pop_step_reward_initial_zero() {
        let mut ledger = Ledger::with_capacity(1, States::default());
        let ep = Episode::default();

        let reward = ledger.pop_step_reward(&ep).expect("Should get reward");
        assert_eq!(reward.0, 0, "Initial reward should be 0");
    }

    #[test]
    fn test_pop_step_reward_clears_after_pop() {
        let mut ledger = Ledger::with_capacity(1, States::default());
        let ep = Episode::default();

        // First pop
        let reward1 = ledger.pop_step_reward(&ep).expect("First pop");

        // Second pop should still be 0 (cleared)
        let reward2 = ledger.pop_step_reward(&ep).expect("Second pop");

        assert_eq!(reward1.0, 0);
        assert_eq!(reward2.0, 0, "Reward should be cleared after pop");
    }

    #[test]
    fn test_pop_step_reward_episode_independence() {
        let mut ledger = Ledger::with_capacity(2, States::default());

        let ep0 = Episode::default();
        let ep1 = ep0.next(ts("2026-01-20T00:00:00Z"));

        // Pop from episode 0 should not affect episode 1
        let _reward0 = ledger.pop_step_reward(&ep0).expect("Pop ep0");
        let reward1 = ledger.pop_step_reward(&ep1).expect("Pop ep1");

        assert_eq!(reward1.0, 0, "Episode 1 should be independent");
    }

    #[test]
    fn test_pop_step_reward_invalid_episode() {
        let mut ledger = Ledger::with_capacity(1, States::default());

        let ep0 = Episode::default();
        let ep_invalid = ep0
            .next(ts("2026-01-20T00:00:00Z"))
            .next(ts("2026-01-21T00:00:00Z")); // Episode 2, but ledger only has 1

        let result = ledger.pop_step_reward(&ep_invalid);
        assert!(result.is_err(), "Should error for invalid episode");
    }

    // ============================================================================
    // Part 4: Resilience Tests
    // ============================================================================

    #[test]
    fn test_get_invalid_episode_returns_error() {
        let ledger = Ledger::with_capacity(1, States::default());

        let ep_valid = Episode::default();
        let ep_invalid = ep_valid
            .next(ts("2026-01-20T00:00:00Z"))
            .next(ts("2026-01-21T00:00:00Z"));

        assert!(ledger.get(&ep_valid).is_ok());
        assert!(ledger.get(&ep_invalid).is_err());
    }

    #[test]
    fn test_get_mut_invalid_episode_returns_error() {
        let mut ledger = Ledger::with_capacity(1, States::default());

        let ep_valid = Episode::default();
        let ep_invalid = ep_valid
            .next(ts("2026-01-20T00:00:00Z"))
            .next(ts("2026-01-21T00:00:00Z"));

        assert!(ledger.get_mut(&ep_valid).is_ok());
        assert!(ledger.get_mut(&ep_invalid).is_err());
    }

    #[test]
    fn test_is_terminal_invalid_episode_returns_error() {
        let ledger = Ledger::with_capacity(1, States::default());

        let ep_invalid = Episode::default()
            .next(ts("2026-01-20T00:00:00Z"))
            .next(ts("2026-01-21T00:00:00Z"));

        assert!(ledger.is_terminal(&ep_invalid).is_err());
    }

    #[test]
    fn test_episode_pnl_invalid_episode_returns_error() {
        let ledger = Ledger::with_capacity(1, States::default());

        let ep_invalid = Episode::default()
            .next(ts("2026-01-20T00:00:00Z"))
            .next(ts("2026-01-21T00:00:00Z"));

        assert!(ledger.episode_pnl(&ep_invalid).is_err());
    }

    #[test]
    fn test_empty_ledger_as_df() {
        let ledger = Ledger::default();

        let df = ledger
            .as_df()
            .expect("Empty ledger should produce valid DataFrame");

        assert_eq!(df.height(), 0);

        // Should still have all columns
        let expected_cols: HashSet<_> = JournalCol::iter().map(|c| c.as_str()).collect();
        let actual_cols: HashSet<_> = df.get_column_names().iter().map(|s| s.as_str()).collect();
        assert_eq!(expected_cols, actual_cols);
    }

    #[test]
    fn test_ledger_with_empty_states_as_df() {
        let ledger = Ledger::with_capacity(5, States::default());

        let df = ledger
            .as_df()
            .expect("Ledger with empty States should work");

        assert_eq!(df.height(), 0, "No trades means 0 rows");
    }

    #[test]
    fn test_apply_actions_mixed_valid_invalid_returns_correct_summary() {
        use crate::gym::trading::action::{Action, Actions, OpenCmd};

        // 1. Setup: Create a Ledger with a single active Episode.
        let mut ledger = Ledger::with_capacity(1, States::default());
        let ep = Episode::default();

        // 2. Create a minimal MarketView (empty, but provides timestamp).
        let fixture = MarketFixture::new(ts("2026-01-24T10:00:00Z"), 90.0, 110.0, 100.0);
        let market = fixture.view();

        // Market ID for testing
        let market_id = MarketId {
            broker: DataBroker::Binance,
            exchange: Exchange::Binance,
            symbol: Symbol::Spot(SpotPair::BtcUsdt),
        };

        // 3. Create two actions:
        //    Action A (Invalid): OpenCmd with Quantity(0.0) - fails validate()
        //    Action B (Valid):   OpenCmd with valid Quantity - passes validate()
        //                        but will fail in handle_open because no price data
        //                        (still exercises the rejection path at state level)

        let invalid_open = Action::Open(OpenCmd {
            agent_id: AgentIdentifier::Random,
            trade_id: TradeId(1),
            trade_type: TradeType::Long,
            quantity: Quantity(0.0), // <-- Invalid: will fail validate()
            entry_price: None,
            stop_loss: None,
            take_profit: None,
        });

        let valid_open = Action::Open(OpenCmd {
            agent_id: AgentIdentifier::Random,
            trade_id: TradeId(2),
            trade_type: TradeType::Long,
            quantity: Quantity(1.0),           // <-- Valid quantity
            entry_price: Some(Price(50000.0)), // Limit order to avoid price lookup
            stop_loss: Some(Price(49000.0)),
            take_profit: Some(Price(51000.0)),
        });

        // Bundle into Actions
        let mut actions = Actions::new();
        actions.add(market_id, invalid_open);
        actions.add(market_id, valid_open);

        let ctx = ActionCtx { actions, market };

        // 4. Execute apply_actions
        let result = ledger.apply_actions(&ep, ctx);

        // 5. Assert the result
        assert!(result.is_ok(), "apply_actions should return Ok, not panic");

        let summary = result.unwrap();
        // Invalid action (zero quantity) -> rejected by validate()
        // Valid action (limit order) -> executed successfully
        assert_eq!(
            summary.rejected, 1,
            "One action should be rejected (invalid quantity)"
        );
        assert_eq!(
            summary.executed, 1,
            "One action should be executed (valid limit order)"
        );
    }

    // ============================================================================
    // Part 5: Integration / Smoke Tests
    // ============================================================================

    #[test]
    fn test_ledger_default_creates_empty() {
        let ledger = Ledger::default();

        // Default ledger has no episodes
        let ep = Episode::default();
        assert!(ledger.get(&ep).is_err(), "Default ledger has no episodes");
    }

    #[test]
    fn test_ledger_with_capacity_creates_episodes() {
        let ledger = Ledger::with_capacity(10, States::default());

        // Should be able to access episodes 0-9
        let mut ep = Episode::default();
        for _ in 0..10 {
            assert!(ledger.get(&ep).is_ok(), "Episode should exist");
            ep = ep.next(ts("2026-01-20T00:00:00Z"));
        }

        // Episode 10 should not exist
        assert!(ledger.get(&ep).is_err(), "Episode 10 should not exist");
    }

    #[test]
    fn test_as_df_pipeline_smoke_test() {
        // Minimal smoke test: construct empty ledger, call as_df, verify no panic
        let ledger = Ledger::with_capacity(1, States::default());

        let result = ledger.as_df();

        assert!(result.is_ok(), "as_df should succeed for valid ledger");

        let df = result.unwrap();
        // Empty states produce 0 rows, but schema columns are present
        assert_eq!(
            df.height(),
            0,
            "Empty ledger should produce empty DataFrame"
        );
        assert!(
            !df.get_column_names().is_empty(),
            "DataFrame should have columns"
        );
    }

    #[test]
    fn test_try_collect_soa_from_empty_iterator() {
        let empty_iter = std::iter::empty::<ChapatyResult<JournalEntry>>();

        let soa = empty_iter
            .try_collect_soa()
            .expect("Empty iterator should work");

        assert!(soa.episode_id.is_empty());
        assert!(soa.trade_id.is_empty());
    }

    #[test]
    fn test_try_collect_soa_with_entries() {
        let entries = vec![
            Ok(sample_journal_entry(0, 1, StateKind::Closed, 500.0)),
            Ok(sample_journal_entry(0, 2, StateKind::Active, 0.0)),
            Ok(sample_journal_entry(1, 3, StateKind::Pending, 0.0)),
        ];

        let soa = entries
            .into_iter()
            .try_collect_soa()
            .expect("Should collect");

        assert_eq!(soa.episode_id.len(), 3);
        assert_eq!(soa.trade_id.len(), 3);
        assert_eq!(soa.trade_state.len(), 3);
    }

    #[test]
    fn test_try_collect_soa_propagates_error() {
        let entries: Vec<ChapatyResult<JournalEntry>> = vec![
            Ok(sample_journal_entry(0, 1, StateKind::Closed, 500.0)),
            Err(ChapatyError::Data(DataError::DataFrame(
                "Test error".to_string(),
            ))),
            Ok(sample_journal_entry(0, 2, StateKind::Active, 0.0)),
        ];

        let result = entries.into_iter().try_collect_soa();
        assert!(result.is_err(), "Should propagate error");
    }

    // ============================================================================
    // Part 6: JournalSoA Field Accessor Tests
    // ============================================================================

    #[test]
    fn test_journal_soa_episode_ids_conversion() {
        let mut soa = JournalSoA::default();
        soa.episode_id.push(EpisodeId(0));
        soa.episode_id.push(EpisodeId(1));
        soa.episode_id.push(EpisodeId(100));

        let result = soa.episode_ids().expect("Conversion should succeed");

        assert_eq!(result, vec![0u32, 1, 100]);
    }

    #[test]
    fn test_journal_soa_trade_ids_conversion() {
        let mut soa = JournalSoA::default();
        soa.trade_id.push(TradeId(1));
        soa.trade_id.push(TradeId(-1));
        soa.trade_id.push(TradeId(i64::MAX));

        let result = soa.trade_ids();

        assert_eq!(result, vec![1, -1, i64::MAX]);
    }

    #[test]
    fn test_journal_soa_timestamps_to_micros() {
        let mut soa = JournalSoA::default();
        let dt = ts("2026-01-19T10:30:45.123456Z");
        soa.entry_timestamp.push(Some(dt));
        soa.entry_timestamp.push(None);

        let result = soa.entry_timestamps_micros();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0], Some(dt.timestamp_micros()));
        assert_eq!(result[1], None);
    }

    #[test]
    fn test_journal_soa_prices_to_f64() {
        let mut soa = JournalSoA::default();
        soa.entry_price.push(Price(50000.123));
        soa.entry_price.push(Price(0.00001));

        let result = soa.entry_prices();

        assert_eq!(result, vec![50000.123, 0.00001]);
    }

    #[test]
    fn test_journal_soa_optional_prices() {
        let mut soa = JournalSoA::default();
        soa.stop_loss.push(Some(Price(49000.0)));
        soa.stop_loss.push(None);
        soa.stop_loss.push(Some(Price(48000.0)));

        let result = soa.stop_losses();

        assert_eq!(result, vec![Some(49000.0), None, Some(48000.0)]);
    }

    #[test]
    fn test_journal_soa_risk_reward_ratios() {
        let mut soa = JournalSoA::default();
        soa.risk_reward_ratio
            .push(Some(RiskRewardRatio::new(100.0, 200.0)));
        soa.risk_reward_ratio.push(None);
        soa.risk_reward_ratio
            .push(Some(RiskRewardRatio::new(50.0, 150.0)));

        let result = soa.risk_reward_ratios();

        assert_eq!(result.len(), 3);
        assert!(result[0].is_some());
        assert!(result[1].is_none());
        assert!(result[2].is_some());
    }

    #[test]
    fn test_journal_soa_exit_reasons() {
        let mut soa = JournalSoA::default();
        soa.exit_reason.push(Some(TerminationReason::MarketClose));
        soa.exit_reason.push(None);
        soa.exit_reason.push(Some(TerminationReason::Canceled));

        let result = soa.exit_reasons();

        assert_eq!(result.len(), 3);
        assert_eq!(result[0], Some("market_close".to_string()));
        assert_eq!(result[1], None);
        assert_eq!(result[2], Some("canceled".to_string()));
    }

    #[test]
    fn test_journal_soa_realized_returns() {
        let mut soa = JournalSoA::default();
        soa.realized_return_in_ticks.push(Tick(100));
        soa.realized_return_in_ticks.push(Tick(-50));
        soa.realized_return_in_ticks.push(Tick(0));

        soa.realized_return_dollars.push(1000.0);
        soa.realized_return_dollars.push(-500.0);
        soa.realized_return_dollars.push(0.0);

        let ticks = soa.realized_return_ticks();
        let dollars = soa.realized_return_usd();

        assert_eq!(ticks, vec![100i64, -50, 0]);
        assert_eq!(dollars, vec![1000.0, -500.0, 0.0]);
    }

    #[test]
    fn test_journal_soa_quantities() {
        let mut soa = JournalSoA::default();
        soa.quantity.push(Quantity(1.0));
        soa.quantity.push(Quantity(0.5));
        soa.quantity.push(Quantity(0.00001));

        let result = soa.quantities();

        assert_eq!(result, vec![1.0, 0.5, 0.00001]);
    }

    #[test]
    fn test_journal_soa_expected_values() {
        let mut soa = JournalSoA::default();
        soa.expected_loss_in_ticks.push(Some(Tick(100)));
        soa.expected_loss_in_ticks.push(None);

        soa.expected_profit_in_ticks.push(Some(Tick(200)));
        soa.expected_profit_in_ticks.push(None);

        soa.expected_loss_usd.push(Some(1000.0));
        soa.expected_loss_usd.push(None);

        soa.expected_profit_usd.push(Some(2000.0));
        soa.expected_profit_usd.push(None);

        let loss_ticks = soa.expected_loss_ticks();
        let profit_ticks = soa.expected_profit_ticks();
        let loss_usd = soa.expected_loss_usd();
        let profit_usd = soa.expected_profit_usd();

        assert_eq!(loss_ticks, vec![Some(100i64), None]);
        assert_eq!(profit_ticks, vec![Some(200i64), None]);
        assert_eq!(loss_usd, vec![Some(1000.0), None]);
        assert_eq!(profit_usd, vec![Some(2000.0), None]);
    }

    // ============================================================================
    // Part 7: Market Context Field Tests
    // ============================================================================

    #[test]
    fn test_journal_soa_market_fields() {
        let mut soa = JournalSoA::default();
        soa.data_broker.push(DataBroker::Binance);
        soa.exchange.push(Exchange::Binance);
        soa.symbol.push(Symbol::Spot(SpotPair::EthUsdt));
        soa.market_type.push(MarketType::Spot);

        let brokers = soa.data_brokers();
        let exchanges = soa.exchanges();
        let symbols = soa.symbols();
        let market_types = soa.market_types();

        assert_eq!(brokers, vec!["binance".to_string()]);
        assert_eq!(exchanges, vec!["binance".to_string()]);
        assert_eq!(symbols, vec!["eth-usdt".to_string()]);
        assert_eq!(market_types, vec!["spot".to_string()]);
    }

    #[test]
    fn test_journal_soa_trade_types() {
        let mut soa = JournalSoA::default();
        soa.trade_type.push(TradeType::Long);
        soa.trade_type.push(TradeType::Short);

        let result = soa.trade_types();

        assert_eq!(result, vec!["long".to_string(), "short".to_string()]);
    }

    #[test]
    fn test_journal_soa_trade_states() {
        let mut soa = JournalSoA::default();
        soa.trade_state.push(StateKind::Active);
        soa.trade_state.push(StateKind::Closed);
        soa.trade_state.push(StateKind::Pending);
        soa.trade_state.push(StateKind::Canceled);

        let result = soa.trade_states();

        assert_eq!(
            result,
            vec![
                "active".to_string(),
                "closed".to_string(),
                "pending".to_string(),
                "canceled".to_string()
            ]
        );
    }

    #[test]
    fn test_journal_soa_agent_ids() {
        let mut soa = JournalSoA::default();
        soa.agent_id.push(AgentIdentifier::Random);

        let result = soa.agent_ids();

        assert_eq!(result.len(), 1);
        // AgentIdentifier::Random should have some string representation
        assert!(!result[0].is_empty());
    }

    // ============================================================================
    // Part 8: Comprehensive Scenario (All State Types)
    // ============================================================================

    #[test]
    fn test_journal_soa_all_state_types() {
        let mut soa = JournalSoA::default();

        // Closed trade
        populate_soa_single(
            &mut soa,
            sample_journal_entry(0, 1, StateKind::Closed, 500.0),
        );

        // Active trade
        let mut active = sample_journal_entry(0, 2, StateKind::Active, 0.0);
        active.exit_timestamp = None;
        active.exit_price = None;
        active.exit_reason = None;
        populate_soa_single(&mut soa, active);

        // Pending trade
        let mut pending = sample_journal_entry(1, 3, StateKind::Pending, 0.0);
        pending.entry_timestamp = None;
        pending.exit_timestamp = None;
        pending.exit_price = None;
        pending.exit_reason = None;
        populate_soa_single(&mut soa, pending);

        // Canceled trade
        let mut canceled = sample_journal_entry(1, 4, StateKind::Canceled, 0.0);
        canceled.entry_timestamp = None;
        canceled.exit_reason = Some(TerminationReason::Canceled);
        canceled.exit_price = None;
        populate_soa_single(&mut soa, canceled);

        let df: DataFrame = soa.try_into().expect("Conversion failed");

        assert_eq!(df.height(), 4, "Should have 4 trades");

        let states: HashSet<_> = df
            .column(JournalCol::TradeState.as_str())
            .unwrap()
            .str()
            .unwrap()
            .into_iter()
            .flatten()
            .collect();

        assert!(states.contains("closed"));
        assert!(states.contains("active"));
        assert!(states.contains("pending"));
        assert!(states.contains("canceled"));

        // Verify schema compliance
        let expected_schema = Journal::to_schema();
        for field in expected_schema.iter_fields() {
            assert!(
                df.column(field.name()).is_ok(),
                "Missing column: {}",
                field.name()
            );
        }
    }
}
