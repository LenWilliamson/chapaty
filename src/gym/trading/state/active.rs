use chrono::{DateTime, Utc};

use crate::{
    data::{
        domain::{Price, Quantity, Symbol},
        event::MarketId,
    },
    error::{AgentError, ChapatyError, ChapatyResult, SystemError},
    gym::trading::{
        action::{MarketCloseCmd, ModifyCmd, OpenCmd},
        config::ExecutionBias,
        state::{Active, Closed, State, Trade, UpdateCtx, sanitize_price},
        types::TerminationReason,
    },
};

pub enum CloseOutcome {
    FullyClosed(Trade<Closed>),
    PartiallyClosed {
        closed: Trade<Closed>,
        remaining: Trade<Active>,
    },
}

impl Trade<Active> {
    /// Factory: Creates an Active trade, enforcing grid snapping.
    pub(super) fn new(
        cmd: OpenCmd,
        entry_price: Price,
        ts: DateTime<Utc>,
        symbol: Symbol,
    ) -> ChapatyResult<Self> {
        // 1. Sanitize (Snap to Grid)
        let clean_entry_val = sanitize_price(symbol, entry_price.0, "entry");
        let clean_entry = Price(clean_entry_val);

        let clean_sl = cmd
            .stop_loss
            .map(|p| Price(sanitize_price(symbol, p.0, "sl")));
        let clean_tp = cmd
            .take_profit
            .map(|p| Price(sanitize_price(symbol, p.0, "tp")));

        // 2. Validate Logic (The Guard)
        // We check if the Initial State is valid.
        cmd.trade_type
            .price_ordering_validation(clean_sl, Some(clean_entry), clean_tp)?;

        // 3. Construct
        Ok(Self {
            uid: cmd.trade_id,
            agent_id: cmd.agent_id,
            kind: cmd.trade_type,
            quantity: cmd.quantity,
            stop_loss: clean_sl,
            take_profit: clean_tp,
            state: Active {
                entry_ts: ts,
                entry_price: clean_entry,
                current_ts: ts,
                current_price: clean_entry,
                unrealized_pnl: 0.0,
            },
        })
    }

    /// Adjusts the stop-loss / take-profit of an Active trade.
    ///
    /// Consumes the trade and returns a fresh one with the new protective
    /// orders applied. Validation runs against the candidate (SL, entry, TP)
    /// ordering before anything is committed.
    pub(super) fn modify(self, cmd: &ModifyCmd, symbol: Symbol) -> ChapatyResult<Self> {
        if self.agent_id != cmd.agent_id {
            return Err(ChapatyError::System(SystemError::AccessDenied(
                "Agent mismatch".to_string(),
            )));
        }

        // Active trades cannot modify entry price
        if cmd.new_entry_price.is_some() {
            return Err(AgentError::InvalidInput(
                "Cannot modify Entry Price of an ACTIVE trade.".to_string(),
            )
            .into());
        }

        // 1. Calculate Candidates (Transactional Preparation)
        // If the command has a new value, sanitize it. Otherwise, keep the current value.
        let candidate_sl = if let Some(raw_sl) = cmd.new_stop_loss {
            Some(Price(sanitize_price(symbol, raw_sl.0, "modify_sl")))
        } else {
            self.stop_loss
        };

        let candidate_tp = if let Some(raw_tp) = cmd.new_take_profit {
            Some(Price(sanitize_price(symbol, raw_tp.0, "modify_tp")))
        } else {
            self.take_profit
        };

        // 2. Validate Logic
        // We check if the NEW combination of (SL, Entry, TP) is valid.
        // Note: Active trades always have a fixed entry price.
        self.kind.price_ordering_validation(
            candidate_sl,
            Some(self.state.entry_price),
            candidate_tp,
        )?;

        // 3. Commit (only reached if validation passed): a fresh trade with the
        // new protective orders, every other field carried over unchanged.
        Ok(self
            .with_stop_loss(candidate_sl)
            .with_take_profit(candidate_tp))
    }

    pub(super) fn market_close(
        self,
        cmd: &MarketCloseCmd,
        exit_price: Price,
        ts: DateTime<Utc>,
        symbol: Symbol,
    ) -> ChapatyResult<(CloseOutcome, f64)> {
        if self.agent_id != cmd.agent_id {
            return Err(SystemError::AccessDenied("Agent mismatch".to_string()).into());
        }
        let qty = cmd.quantity.unwrap_or(self.quantity);
        if (qty.0 - self.quantity.0) > f64::EPSILON {
            return Err(AgentError::InvalidInput("Close qty > Open qty".to_string()).into());
        }

        Ok(self.execute_close(&CloseParams {
            qty,
            exit_price,
            ts,
            reason: TerminationReason::MarketClose,
            symbol,
        }))
    }

    /// Advances an Active trade by one market step.
    ///
    /// Consumes the trade and returns its next state plus the **reward increment**
    /// for this step (change in `PnL` since the previous mark):
    /// - No exit: a fresh `Active` clone marked to the current price.
    /// - SL/TP hit: the resulting `Closed` trade.
    ///
    /// On an exit we intentionally do **not** re-mark first: the trade fills at the
    /// SL/TP price, not the bar close, so `self.state.unrealized_pnl` is left at the
    /// previous mark and `execute_close` reads it as the baseline. The current-bar
    /// mark is only meaningful (and only applied) when the trade survives.
    pub(super) fn update(self, m_id: MarketId, ctx: &UpdateCtx) -> ChapatyResult<(State, f64)> {
        let symbol = m_id.symbol;

        // 1. Capture START value.
        let prev_unrealized_pnl = self.state.unrealized_pnl;

        // 2. Resolve this bar's mark price / timestamp.
        let raw_price = ctx.market.try_resolved_close_price(symbol)?.0;
        let current_price = Price(sanitize_price(symbol, raw_price, "mark_price"));
        let ts = ctx.market.current_timestamp();

        // Clean (tick-multiple) unrealized PnL at the current price. Used only on
        // the survival branch; on an exit the trade fills at the SL/TP price instead.
        let current_unrealized_pnl =
            self.kind
                .calculate_pnl(self.state.entry_price, current_price, self.quantity, symbol);

        // 3. Check Triggers.
        let tp_exit = self
            .take_profit
            .filter(|&tp| ctx.market.reached_price(tp, symbol, self.kind))
            .map(|tp| (TerminationReason::TakeProfit, tp.0));

        let sl_exit = self
            .stop_loss
            .filter(|&sl| ctx.market.reached_price(sl, symbol, self.kind))
            .map(|sl| (TerminationReason::StopLoss, sl.0));

        // Resolve conflict (priority by execution bias).
        let exit = match ctx.bias {
            // Pessimistic: StopLoss wins if both trigger.
            ExecutionBias::Pessimistic => sl_exit.or(tp_exit),
            // Optimistic: TakeProfit wins if both trigger.
            ExecutionBias::Optimistic => tp_exit.or(sl_exit),
        };

        // 4. Execute exit if triggered.
        if let Some((reason, raw_exit_price)) = exit {
            let exit_price = Price(sanitize_price(symbol, raw_exit_price, "exit_price"));
            let qty = self.quantity;

            // `self.state.unrealized_pnl` is still `prev` (we never re-marked), so
            // `execute_close` reads the correct baseline off the trade itself.
            let (outcome, step_delta) = self.execute_close(&CloseParams {
                qty,
                exit_price,
                ts,
                reason,
                symbol,
            });

            match outcome {
                CloseOutcome::FullyClosed(c) => Ok((State::Closed(c), step_delta)),
                CloseOutcome::PartiallyClosed { .. } => Err(SystemError::InvariantViolation(
                    "execute_close(full_qty) returned Partial. Logic Error.".to_string(),
                )
                .into()),
            }
        } else {
            // Survives: produce a fresh marked clone (Active -> Active) via the
            // functor map, carrying the new mark.
            let marked = self.map(|s| Active {
                current_ts: ts,
                current_price,
                unrealized_pnl: current_unrealized_pnl,
                ..s
            });
            let step_delta = current_unrealized_pnl - prev_unrealized_pnl;
            Ok((State::Active(marked), step_delta))
        }
    }
}

impl Trade<Active> {
    /// Closes all (or part) of the position and reports the **reward increment**.
    ///
    /// The reward is a _delta against the trade's last recorded mark_, not the
    /// absolute realized `PnL`. Every reward emitted is the "change since the
    /// previous mark", and a close is just a final mark at the exit price. The
    /// baseline is read straight off `self.state.unrealized_pnl`. The closed trade's `realized_pnl`
    /// still stores the _absolute_ realized `PnL` for the journal.
    fn execute_close(self, close_params: &CloseParams) -> (CloseOutcome, f64) {
        let CloseParams {
            qty,
            exit_price,
            ts,
            reason,
            symbol,
        } = close_params;
        let clean_exit_price = Price(sanitize_price(*symbol, exit_price.0, "exit"));
        let last_marked_unrealized_pnl = self.state.unrealized_pnl;

        let realized_pnl =
            self.kind
                .calculate_pnl(self.state.entry_price, clean_exit_price, *qty, *symbol);
        let is_full_close = (self.quantity.0 - qty.0).abs() < f64::EPSILON;

        if is_full_close {
            // The whole booked unrealized belongs to this close.
            let step_delta = realized_pnl - last_marked_unrealized_pnl;

            let closed = self.map(|s| Closed {
                entry_ts: s.entry_ts,
                entry_price: s.entry_price,
                exit_ts: *ts,
                exit_price: clean_exit_price,
                termination_reason: *reason,
                realized_pnl,
            });
            (CloseOutcome::FullyClosed(closed), step_delta)
        } else {
            // Split the booked unrealized between the closed slice and the survivor,
            // proportional to quantity.
            let closed_fraction = qty.0 / self.quantity.0;
            let closed_booked_unrealized = last_marked_unrealized_pnl * closed_fraction;
            let remaining_booked_unrealized = last_marked_unrealized_pnl - closed_booked_unrealized;

            // The closed slice only adds, what it gained beyond its already-booked share.
            let step_delta = realized_pnl - closed_booked_unrealized;

            let remaining = Self {
                quantity: self.quantity - *qty,
                state: Active {
                    // The survivor must carry only its share of the unrealized PnL.
                    unrealized_pnl: remaining_booked_unrealized,
                    ..self.state.clone()
                },
                ..self.clone()
            };

            let closed = Trade {
                quantity: *qty,
                ..self.map(|s| Closed {
                    entry_ts: s.entry_ts,
                    entry_price: s.entry_price,
                    exit_ts: *ts,
                    exit_price: clean_exit_price,
                    termination_reason: *reason,
                    realized_pnl,
                })
            };

            (
                CloseOutcome::PartiallyClosed { closed, remaining },
                step_delta,
            )
        }
    }
}

// ================================================================================================
// Helper Types
// ================================================================================================

/// Parameters for closing (all or part of) an [`Active`] trade.
struct CloseParams {
    /// Quantity to close. Equal to the trade's quantity for a full close, or
    /// strictly less for a partial close (the remainder stays [`Active`]).
    qty: Quantity,

    /// The fill price for this close, before grid snapping.
    exit_price: Price,

    /// Timestamp of the close.
    ts: DateTime<Utc>,

    /// Why the trade is closing.
    reason: TerminationReason,

    /// The instrument's symbol, used for tick-grid price sanitization and for the
    /// discrete, tick-multiple `PnL` computation in `calculate_pnl`.
    symbol: Symbol,
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, clippy::expect_used)]
    use crate::{
        data::{
            domain::{
                ContractMonth, ContractYear, DataBroker, Exchange, FutureContract, FutureRoot,
                Period, Price, Quantity, Symbol, TradeId,
            },
            event::{MarketId, Ohlcv, OhlcvId},
            view::MarketView,
        },
        gym::{
            AgentIdentifier,
            trading::{
                config::{EnvConfig, ExecutionBias},
                types::TradeKind,
            },
        },
        sim::{
            cursor_group::CursorGroup,
            data::{SimulationData, SimulationDataBuilder, Streams},
        },
        sorted_vec_map::SortedVecMap,
    };

    use super::*;

    // ============================================================================
    // Test Helpers
    // ============================================================================

    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    fn ohlcv_id() -> OhlcvId {
        OhlcvId {
            broker: DataBroker::NinjaTrader,
            exchange: Exchange::Cme,
            symbol: Symbol::Future(FutureContract {
                root: FutureRoot::EurUsd,
                month: ContractMonth::December,
                year: ContractYear::Y5,
            }),
            period: Period::Minute(1),
        }
    }

    /// A lightweight wrapper around the heavy `SimulationData`.
    /// It allows us to create a valid `MarketView` with a simple (low, high, close) API.
    struct MarketFixture {
        sim_data: SimulationData,
        cursor: CursorGroup,
    }

    impl MarketFixture {
        fn new(timestamp: DateTime<Utc>, low: f64, high: f64, close: f64) -> Self {
            let id = ohlcv_id();

            // 1. Create Data
            let candle = Ohlcv {
                open_timestamp: timestamp,
                close_timestamp: timestamp + chrono::Duration::minutes(1),
                open: Price(f64::midpoint(low, high)),
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

            let streams = Streams::default().with_ohlcv(map);
            let sim_data = SimulationDataBuilder::new(streams)
                .build(&EnvConfig::default())
                .expect("Failed to build sim data");

            // 2. Create Cursor (Auto-initialized to start)
            let cursor = CursorGroup::new(&sim_data);

            Self { sim_data, cursor }
        }

        /// Returns a valid `MarketView` borrowing from the owned `SimulationData`
        fn view(&self) -> MarketView<'_> {
            MarketView::new(&self.sim_data, &self.cursor).unwrap()
        }
    }

    /// Create a basic Long Active trade
    fn create_long_active(entry_price: f64, sl: Option<f64>, tp: Option<f64>) -> Trade<Active> {
        let symbol = ohlcv_id().symbol;
        Trade::<Active>::new(
            OpenCmd {
                trade_id: TradeId(1),
                agent_id: AgentIdentifier::Random,
                trade_type: TradeKind::Long,
                quantity: Quantity(1.0),
                stop_loss: sl.map(Price),
                take_profit: tp.map(Price),
                entry_price: None,
            },
            Price(entry_price),
            ts("2026-01-19T10:00:00Z"),
            symbol,
        )
        .expect("invalid trade configuration")
    }

    /// Create a basic Short Active trade
    fn create_short_active(entry_price: f64, sl: Option<f64>, tp: Option<f64>) -> Trade<Active> {
        let symbol = ohlcv_id().symbol;
        Trade::<Active>::new(
            OpenCmd {
                trade_id: TradeId(2),
                agent_id: AgentIdentifier::Random,
                trade_type: TradeKind::Short,
                quantity: Quantity(1.0),
                stop_loss: sl.map(Price),
                take_profit: tp.map(Price),
                entry_price: None,
            },
            Price(entry_price),
            ts("2026-01-19T10:00:00Z"),
            symbol,
        )
        .expect("invalid trade configuration")
    }

    // ============================================================================
    // Part 1: Standard PnL Updates
    // ============================================================================

    #[test]
    fn test_long_unrealized_pnl_positive() {
        let trade = create_long_active(1.1, None, None);
        let m_id: MarketId = ohlcv_id().into();

        // Initial state: unrealized = 0.0
        assert_f64_eq!(trade.state.unrealized_pnl, 0.0);

        // Market moves up to 1.105
        let fixture = MarketFixture::new(ts("2026-01-19T10:01:00Z"), 1.1, 1.105, 1.105);
        let view = fixture.view();
        let ctx = UpdateCtx {
            market: &view,
            bias: ExecutionBias::Optimistic,
        };
        let (new_state, step_delta) = trade.update(m_id, &ctx).unwrap();

        // Extract new trade
        let State::Active(updated) = new_state else {
            panic!("Expected Active state");
        };

        // New unrealized should be positive (price went up, long position)
        // Assuming calculate_pnl returns clean values
        let new_unrealized = updated.state.unrealized_pnl;
        assert!(
            new_unrealized > 0.0,
            "Long should have positive PnL when price rises"
        );

        // step_delta should equal new_unrealized - prev_unrealized
        assert_f64_eq!(step_delta, new_unrealized - 0.0);
    }

    #[test]
    fn test_long_unrealized_pnl_negative() {
        let trade = create_long_active(1.1, None, None);
        let m_id: MarketId = ohlcv_id().into();

        // Initial state: unrealized = 0.0
        assert_f64_eq!(trade.state.unrealized_pnl, 0.0);

        // Market moves down to 1.095
        let fixture = MarketFixture::new(ts("2026-01-19T10:01:00Z"), 1.095, 1.1, 1.095);
        let view = fixture.view();
        let ctx = UpdateCtx {
            market: &view,
            bias: ExecutionBias::Optimistic,
        };
        let (new_state, step_delta) = trade.update(m_id, &ctx).unwrap();

        let State::Active(updated) = new_state else {
            panic!("Expected Active state");
        };

        let new_unrealized = updated.state.unrealized_pnl;
        assert!(
            new_unrealized < 0.0,
            "Long should have negative PnL when price falls"
        );
        assert_f64_eq!(step_delta, new_unrealized);
    }

    #[test]
    fn test_short_unrealized_pnl_positive() {
        let trade = create_short_active(1.1, None, None);
        let m_id: MarketId = ohlcv_id().into();

        // Initial state: unrealized = 0.0
        assert_f64_eq!(trade.state.unrealized_pnl, 0.0);

        // Market moves up to 1.105 (unfavorable for short)
        let fixture = MarketFixture::new(ts("2026-01-19T10:01:00Z"), 1.1, 1.105, 1.105);
        let view = fixture.view();
        let ctx = UpdateCtx {
            market: &view,
            bias: ExecutionBias::Optimistic,
        };
        let (new_state, step_delta) = trade.update(m_id, &ctx).unwrap();

        let State::Active(updated) = new_state else {
            panic!("Expected Active state");
        };

        let new_unrealized = updated.state.unrealized_pnl;
        assert!(
            new_unrealized < 0.0,
            "Short should have negative PnL when price rises"
        );
        assert_f64_eq!(step_delta, new_unrealized);
    }

    #[test]
    fn test_short_unrealized_pnl_negative() {
        let trade = create_short_active(1.1, None, None);
        let m_id: MarketId = ohlcv_id().into();

        // Initial state: unrealized = 0.0
        assert_f64_eq!(trade.state.unrealized_pnl, 0.0);

        // Market moves down to 1.095 (favorable for short)
        let fixture = MarketFixture::new(ts("2026-01-19T10:01:00Z"), 1.095, 1.1, 1.095);
        let view = fixture.view();
        let ctx = UpdateCtx {
            market: &view,
            bias: ExecutionBias::Optimistic,
        };
        let (new_state, step_delta) = trade.update(m_id, &ctx).unwrap();

        let State::Active(updated) = new_state else {
            panic!("Expected Active state");
        };

        let new_unrealized = updated.state.unrealized_pnl;
        assert!(
            new_unrealized > 0.0,
            "Short should have positive PnL when price falls"
        );
        assert_f64_eq!(step_delta, new_unrealized);
    }

    #[test]
    fn test_pnl_delta_calculation() {
        let trade = create_long_active(1.1, None, None);
        let m_id: MarketId = ohlcv_id().into();

        // Initial state: unrealized = 0.0
        assert_f64_eq!(trade.state.unrealized_pnl, 0.0);

        // Step 1: Price moves to 1.102
        let fixture1 = MarketFixture::new(ts("2026-01-19T10:01:00Z"), 1.1, 1.102, 1.102);
        let view1 = fixture1.view();
        let ctx1 = UpdateCtx {
            market: &view1,
            bias: ExecutionBias::Optimistic,
        };
        let (state1, delta1) = trade.update(m_id, &ctx1).unwrap();

        let State::Active(trade1) = state1 else {
            panic!("Expected Active");
        };
        let pnl1 = trade1.state.unrealized_pnl;

        // Step 2: Price moves to 1.10400
        let fixture2 = MarketFixture::new(ts("2026-01-19T10:02:00Z"), 1.102, 1.10400, 1.10400);
        let view2 = fixture2.view();
        let ctx2 = UpdateCtx {
            market: &view2,
            bias: ExecutionBias::Optimistic,
        };
        let (state2, delta2) = trade1.update(m_id, &ctx2).unwrap();

        let State::Active(trade2) = state2 else {
            panic!("Expected Active");
        };
        let pnl2 = trade2.state.unrealized_pnl;

        // Verify delta is incremental
        assert_f64_eq!(delta1, pnl1 - 0.0, "First delta should be pnl1 - 0");
        assert_f64_eq!(delta2, pnl2 - pnl1, "Second delta should be pnl2 - pnl1");
    }

    // ============================================================================
    // Part 2: Trigger Priority - God Candle Scenarios
    // ============================================================================

    #[test]
    fn test_god_candle_pessimistic_sl_priority() {
        // Setup: Long @ 1.1, SL @ 1.095, TP @ 1.105
        // Candle hits BOTH: low=1.095, high=1.105
        let trade = create_long_active(1.1, Some(1.095), Some(1.105));
        let m_id: MarketId = ohlcv_id().into();

        // Pessimistic bias: SL takes priority
        let fixture = MarketFixture::new(ts("2026-01-19T10:01:00Z"), 1.095, 1.105, 1.1);
        let view = fixture.view();
        let ctx = UpdateCtx {
            market: &view,
            bias: ExecutionBias::Pessimistic,
        };
        let (new_state, _step_delta) = trade.update(m_id, &ctx).unwrap();

        // Must close with StopLoss
        match new_state {
            State::Closed(c) => {
                assert_eq!(c.state.termination_reason, TerminationReason::StopLoss);
                assert_eq!(c.state.exit_price, Price(1.095));
            }
            _ => panic!("Expected Closed state with StopLoss"),
        }
    }

    #[test]
    fn test_god_candle_optimistic_tp_priority() {
        // Setup: Long @ 1.1, SL @ 1.095, TP @ 1.105
        // Candle hits BOTH
        let trade = create_long_active(1.1, Some(1.095), Some(1.105));
        let m_id: MarketId = ohlcv_id().into();

        // Optimistic bias: TP takes priority
        let fixture = MarketFixture::new(ts("2026-01-19T10:01:00Z"), 1.095, 1.105, 1.1);
        let view = fixture.view();
        let ctx = UpdateCtx {
            market: &view,
            bias: ExecutionBias::Optimistic,
        };
        let (new_state, _step_delta) = trade.update(m_id, &ctx).unwrap();

        // Must close with TakeProfit
        match new_state {
            State::Closed(c) => {
                assert_eq!(c.state.termination_reason, TerminationReason::TakeProfit);
                assert_eq!(c.state.exit_price, Price(1.105));
            }
            _ => panic!("Expected Closed state with TakeProfit"),
        }
    }

    #[test]
    fn test_god_candle_short_pessimistic() {
        // Short @ 1.1, SL @ 1.105, TP @ 1.095
        // Candle hits both
        let trade = create_short_active(1.1, Some(1.105), Some(1.095));
        let m_id: MarketId = ohlcv_id().into();

        let fixture = MarketFixture::new(ts("2026-01-19T10:01:00Z"), 1.095, 1.105, 1.1);
        let view = fixture.view();
        let ctx = UpdateCtx {
            market: &view,
            bias: ExecutionBias::Pessimistic,
        };
        let (new_state, _) = trade.update(m_id, &ctx).unwrap();

        match new_state {
            State::Closed(c) => {
                assert_eq!(c.state.termination_reason, TerminationReason::StopLoss);
            }
            _ => panic!("Expected StopLoss"),
        }
    }

    #[test]
    fn test_only_tp_hit() {
        let trade = create_long_active(1.1, Some(1.095), Some(1.105));
        let m_id: MarketId = ohlcv_id().into();

        // Only TP is hit (low doesn't reach SL)
        let fixture = MarketFixture::new(ts("2026-01-19T10:01:00Z"), 1.1, 1.105, 1.105);
        let view = fixture.view();
        let ctx = UpdateCtx {
            market: &view,
            bias: ExecutionBias::Optimistic,
        };
        let (new_state, _) = trade.update(m_id, &ctx).unwrap();

        match new_state {
            State::Closed(c) => {
                assert_eq!(c.state.termination_reason, TerminationReason::TakeProfit);
            }
            _ => panic!("Expected TakeProfit"),
        }
    }

    #[test]
    fn test_only_sl_hit() {
        let trade = create_long_active(1.1, Some(1.095), Some(1.105));
        let m_id: MarketId = ohlcv_id().into();

        // Only SL is hit (high doesn't reach TP)
        let fixture = MarketFixture::new(ts("2026-01-19T10:01:00Z"), 1.095, 1.1, 1.098);
        let view = fixture.view();
        let ctx = UpdateCtx {
            market: &view,
            bias: ExecutionBias::Optimistic,
        };
        let (new_state, _) = trade.update(m_id, &ctx).unwrap();

        match new_state {
            State::Closed(c) => {
                assert_eq!(c.state.termination_reason, TerminationReason::StopLoss);
            }
            _ => panic!("Expected StopLoss"),
        }
    }

    // ============================================================================
    // Part 3: Price Sanitization
    // ============================================================================

    #[test]
    fn test_prices_are_sanitized() {
        // Create trade with off-grid prices
        let symbol = ohlcv_id().symbol;
        let trade = Trade::<Active>::new(
            OpenCmd {
                trade_id: TradeId(10),
                agent_id: AgentIdentifier::Random,
                trade_type: TradeKind::Long,
                quantity: Quantity(1.0),
                stop_loss: Some(Price(1.095_567)),   // Off-grid
                take_profit: Some(Price(1.105_123)), // Off-grid
                entry_price: None,
            },
            Price(1.100_789), // Off-grid entry
            ts("2026-01-19T10:00:00Z"),
            symbol,
        )
        .expect("invalid trade configuration");

        // All prices should be snapped to tick_size grid
        // Assuming sanitize_price rounds to nearest 0.00005
        // 1.100789 -> 1.10080
        // 1.095567 -> 1.09555
        // 1.105123 -> 1.10510

        assert_f64_eq!(trade.state.entry_price.0, 1.10080);
        assert_f64_eq!(trade.stop_loss.unwrap().0, 1.09555);
        assert_f64_eq!(trade.take_profit.unwrap().0, 1.10510);
        // Check they're on grid (divisible by tick_size with some tolerance)
        let remainder = (trade.state.entry_price.0 / 0.00005) % 1.0;
        assert!(remainder.abs() < f64::EPSILON, "Entry price not on grid");
    }

    // ============================================================================
    // Part 4: Modify Tests
    // ============================================================================

    #[test]
    fn test_modify_active_cannot_change_entry() {
        let trade = create_long_active(1.1, Some(1.095), Some(1.105));
        let symbol = ohlcv_id().symbol;

        let cmd = ModifyCmd {
            agent_id: trade.agent_id.clone(),
            trade_id: trade.uid,
            new_entry_price: Some(Price(1.11)), // Attempt to change entry
            new_stop_loss: None,
            new_take_profit: None,
        };

        let result = trade.modify(&cmd, symbol);
        assert!(
            result.is_err(),
            "Should not allow modifying entry price of Active trade"
        );
    }

    #[test]
    fn test_modify_active_valid_sl_tp() {
        let trade = create_long_active(1.1, Some(1.095), Some(1.105));
        let symbol = ohlcv_id().symbol;

        let cmd = ModifyCmd {
            agent_id: trade.agent_id.clone(),
            trade_id: trade.uid,
            new_entry_price: None,
            new_stop_loss: Some(Price(1.098)),
            new_take_profit: Some(Price(1.11)),
        };

        let trade = trade.modify(&cmd, symbol).unwrap();

        assert_eq!(trade.stop_loss, Some(Price(1.098)));
        assert_eq!(trade.take_profit, Some(Price(1.11)));
    }

    // ============================================================================
    // Part 5: Manual Close Tests
    // ============================================================================

    #[test]
    fn test_manual_close_full() {
        let trade = create_long_active(1.1, None, None);
        let symbol = ohlcv_id().symbol;

        let cmd = MarketCloseCmd {
            agent_id: trade.agent_id.clone(),
            trade_id: trade.uid,
            quantity: None, // Full close
        };

        let (outcome, reward) = trade
            .market_close(&cmd, Price(1.105), ts("2026-01-19T12:00:00Z"), symbol)
            .unwrap();

        match outcome {
            CloseOutcome::FullyClosed(c) => {
                assert_eq!(c.state.termination_reason, TerminationReason::MarketClose);
                assert!(reward > 0.0, "Should have positive PnL");
                assert_f64_eq!(reward, 625.0);
            }
            CloseOutcome::PartiallyClosed { .. } => panic!("Expected FullyClosed"),
        }
    }

    #[test]
    fn test_manual_close_partial() {
        // Setup: Long EUR/USD @ 1.1, Qty 1.0
        let trade = create_long_active(1.1, None, None);
        let symbol = ohlcv_id().symbol;

        let cmd = MarketCloseCmd {
            agent_id: trade.agent_id.clone(),
            trade_id: trade.uid,
            quantity: Some(Quantity(0.5)), // Partial Close: 0.5
        };

        // Action: Close at 1.105 (Profit)
        let (outcome, reward) = trade
            .market_close(&cmd, Price(1.105), ts("2026-01-19T12:00:00Z"), symbol)
            .unwrap();

        // Verification: Reward Logic
        // Diff: 0.00500 -> 100 ticks
        // Value: 100 ticks * $6.25 * 0.5 qty = $312.50
        assert_f64_eq!(
            reward,
            312.5,
            "Reward calculation incorrect for partial close"
        );

        match outcome {
            CloseOutcome::PartiallyClosed { closed, remaining } => {
                // Check Closed Portion
                assert_eq!(closed.quantity, Quantity(0.5));
                assert_f64_eq!(
                    closed.state.realized_pnl,
                    312.5,
                    "Closed state PnL mismatch"
                );
                assert_eq!(
                    closed.state.termination_reason,
                    TerminationReason::MarketClose
                );

                // Check Remaining Portion
                assert_eq!(remaining.quantity, Quantity(0.5));
                assert_eq!(remaining.state.entry_price, Price(1.1));
            }
            CloseOutcome::FullyClosed(_) => panic!("Expected PartiallyClosed outcome"),
        }
    }

    #[test]
    fn test_close_qty_exceeds_position() {
        let trade = create_long_active(1.1, None, None);
        let symbol = ohlcv_id().symbol;

        let cmd = MarketCloseCmd {
            agent_id: trade.agent_id.clone(),
            trade_id: trade.uid,
            quantity: Some(Quantity(2.0)), // More than position
        };

        let result = trade.market_close(&cmd, Price(1.105), ts("2026-01-19T12:00:00Z"), symbol);
        assert!(result.is_err(), "Should reject close qty > position qty");
    }

    // ============================================================================
    // Part 6: Transactional
    // ============================================================================

    #[test]
    fn test_modify_active_invalid_ordering() {
        let trade = create_long_active(1.1, Some(1.095), Some(1.105));
        let symbol = ohlcv_id().symbol;

        // Try to set SL above entry (invalid for long)
        let cmd = ModifyCmd {
            agent_id: trade.agent_id.clone(),
            trade_id: trade.uid,
            new_entry_price: None,
            new_stop_loss: Some(Price(1.11)), // Above entry!
            new_take_profit: None,
        };

        let result = trade.clone().modify(&cmd, symbol);
        assert!(result.is_err(), "Should reject invalid SL ordering");

        // Verify state unchanged (transactional)
        assert_eq!(trade.stop_loss, Some(Price(1.095)));
    }

    #[test]
    fn test_modify_is_transactional() {
        let symbol = Symbol::Future(FutureContract {
            root: FutureRoot::EurUsd,
            month: ContractMonth::December,
            year: ContractYear::Y5,
        });

        // Setup: Long Pending Trade @ 1.1, SL @ 1.09000
        let trade = Trade::<Active>::new(
            OpenCmd {
                trade_id: TradeId(0),
                agent_id: AgentIdentifier::Random,
                trade_type: TradeKind::Long,
                quantity: Quantity(1.0),
                stop_loss: Some(Price(1.09000)),
                take_profit: None,
                entry_price: Some(Price(1.1)),
            },
            Price(1.1),
            Utc::now(),
            symbol,
        )
        .expect("invalid trade configuration");

        // Action: Try to modify SL to 1.11 (ABOVE Entry -> INVALID for Long)
        // AND try to set TP to 1.12000 (VALID)
        let cmd = ModifyCmd {
            agent_id: trade.agent_id.clone(),
            trade_id: trade.uid,
            new_entry_price: None,
            new_stop_loss: Some(Price(1.11)), // Invalid: > Entry
            new_take_profit: Some(Price(1.12000)), // Valid
        };

        let result = trade.clone().modify(&cmd, symbol);

        // 1. Assert Error
        assert!(
            result.is_err(),
            "Modification should fail due to invalid SL"
        );

        // 2. Assert State Unchanged (Transactional check)
        // TP should still be None, not 1.12000
        assert_eq!(
            trade.take_profit, None,
            "Trade state was partially modified!"
        );
        assert_eq!(
            trade.stop_loss,
            Some(Price(1.09000)),
            "SL should remain unchanged"
        );
    }

    #[test]
    fn test_market_close_reports_increment_after_marking() {
        // Regression: a marked-then-market-closed trade must add its PnL to the
        // curve exactly once. Pre-fix this returned the full realized on top of
        // the already-booked unrealized (the 2x bug).
        let trade = create_long_active(1.1, None, None);
        let m_id: MarketId = ohlcv_id().into();
        let symbol = ohlcv_id().symbol;

        // Mark to 1.103: +60 ticks * $6.25 * qty 1.0 = $375 unrealized.
        let fx = MarketFixture::new(ts("2026-01-19T10:01:00Z"), 1.1, 1.103, 1.103);
        let view = fx.view();
        let ctx = UpdateCtx {
            market: &view,
            bias: ExecutionBias::Optimistic,
        };
        let (state, mark_delta) = trade.update(m_id, &ctx).unwrap();
        let State::Active(marked) = state else {
            panic!("Expected Active after marking");
        };
        assert_f64_eq!(marked.state.unrealized_pnl, 375.0);
        assert_f64_eq!(mark_delta, 375.0);

        // Market close at 1.105: realized = 100 ticks * $6.25 * 1.0 = $625.
        let cmd = MarketCloseCmd {
            agent_id: marked.agent_id.clone(),
            trade_id: marked.uid,
            quantity: None,
        };
        let (outcome, close_delta) = marked
            .market_close(&cmd, Price(1.105), ts("2026-01-19T10:02:00Z"), symbol)
            .unwrap();

        let CloseOutcome::FullyClosed(closed) = outcome else {
            panic!("Expected FullyClosed");
        };
        // Absolute realized is preserved for the journal.
        assert_f64_eq!(closed.state.realized_pnl, 625.0);
        // Reward channel reports only the increment beyond the last mark.
        assert_f64_eq!(close_delta, 625.0 - 375.0);
        // Telescoping invariant: marks + close == realized, booked exactly once.
        assert_f64_eq!(mark_delta + close_delta, 625.0);
    }

    #[test]
    fn test_partial_market_close_splits_unrealized_and_survivor_continues() {
        let trade = create_long_active(1.1, None, None);
        let m_id: MarketId = ohlcv_id().into();
        let symbol = ohlcv_id().symbol;

        // Mark full position to 1.102: +40 ticks * $6.25 * 1.0 = $250.
        let fx1 = MarketFixture::new(ts("2026-01-19T10:01:00Z"), 1.1, 1.102, 1.102);
        let v1 = fx1.view();
        let c1 = UpdateCtx {
            market: &v1,
            bias: ExecutionBias::Optimistic,
        };
        let (s1, d1) = trade.update(m_id, &c1).unwrap();
        let State::Active(marked) = s1 else {
            panic!("Expected Active");
        };
        assert_f64_eq!(marked.state.unrealized_pnl, 250.0);
        assert_f64_eq!(d1, 250.0);

        // Close 0.5 @ 1.105: realized = 100 ticks * $6.25 * 0.5 = $312.5.
        let cmd = MarketCloseCmd {
            agent_id: marked.agent_id.clone(),
            trade_id: marked.uid,
            quantity: Some(Quantity(0.5)),
        };
        let (outcome, close_delta) = marked
            .market_close(&cmd, Price(1.105), ts("2026-01-19T10:02:00Z"), symbol)
            .unwrap();

        let CloseOutcome::PartiallyClosed { closed, remaining } = outcome else {
            panic!("Expected PartiallyClosed");
        };
        assert_eq!(closed.quantity, Quantity(0.5));
        assert_f64_eq!(closed.state.realized_pnl, 312.5);
        // Closed slice's prior share was 250 * 0.5 = 125; increment = 312.5 - 125.
        assert_f64_eq!(close_delta, 187.5);
        // FIX: survivor carries only its 0.5 share of the unrealized, not the full 250.
        assert_eq!(remaining.quantity, Quantity(0.5));
        assert_f64_eq!(remaining.state.unrealized_pnl, 125.0);

        // Survivor must mark correctly off its own baseline, not a stale full-position one.
        // Mark to 1.103: +60 ticks * $6.25 * 0.5 = $187.5.
        let fx2 = MarketFixture::new(ts("2026-01-19T10:03:00Z"), 1.102, 1.103, 1.103);
        let v2 = fx2.view();
        let c2 = UpdateCtx {
            market: &v2,
            bias: ExecutionBias::Optimistic,
        };
        let (s2, d2) = remaining.update(m_id, &c2).unwrap();
        let State::Active(marked2) = s2 else {
            panic!("Expected Active");
        };
        assert_f64_eq!(marked2.state.unrealized_pnl, 187.5);
        assert_f64_eq!(d2, 62.5); // 187.5 - 125.0, not polluted by the stale 250 baseline

        // End-to-end: every recorded delta sums to realized(closed) + unrealized(survivor).
        assert_f64_eq!(d1 + close_delta + d2, 312.5 + 187.5);
    }
}
