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
    pub fn new(
        cmd: OpenCmd,
        entry_price: Price,
        ts: DateTime<Utc>,
        symbol: &Symbol,
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
            trade_type: cmd.trade_type,
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

    pub fn modify(&mut self, cmd: &ModifyCmd, symbol: &Symbol) -> ChapatyResult<()> {
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
        self.trade_type.price_ordering_validation(
            candidate_sl,
            Some(self.state.entry_price),
            candidate_tp,
        )?;

        // 3. Commit Changes (Only reached if validation passes)
        self.stop_loss = candidate_sl;
        self.take_profit = candidate_tp;

        Ok(())
    }

    pub fn market_close(
        self,
        cmd: &MarketCloseCmd,
        exit_price: Price,
        ts: DateTime<Utc>,
        symbol: &Symbol,
    ) -> ChapatyResult<(CloseOutcome, f64)> {
        if self.agent_id != cmd.agent_id {
            return Err(SystemError::AccessDenied("Agent mismatch".to_string()).into());
        }

        let qty = cmd.quantity.unwrap_or(self.quantity);
        if (qty.0 - self.quantity.0) > f64::EPSILON {
            return Err(AgentError::InvalidInput("Close qty > Open qty".to_string()).into());
        }

        self.execute_close(qty, exit_price, ts, TerminationReason::MarketClose, symbol)
    }
}

impl Trade<Active> {
    fn execute_close(
        self,
        qty: Quantity,
        exit_price: Price,
        ts: DateTime<Utc>,
        reason: TerminationReason,
        symbol: &Symbol,
    ) -> ChapatyResult<(CloseOutcome, f64)> {
        let clean_exit_val = sanitize_price(symbol, exit_price.0, "exit");
        let clean_exit_price = Price(clean_exit_val);

        let realized_pnl =
            self.trade_type
                .calculate_pnl(self.state.entry_price, clean_exit_price, qty, symbol);

        let closed = Trade {
            uid: self.uid,
            agent_id: self.agent_id.clone(),
            trade_type: self.trade_type,
            quantity: qty,
            stop_loss: self.stop_loss,
            take_profit: self.take_profit,
            state: Closed {
                entry_ts: self.state.entry_ts,
                entry_price: self.state.entry_price,
                exit_ts: ts,
                exit_price: clean_exit_price,
                termination_reason: reason,
                realized_pnl,
            },
        };

        if (self.quantity.0 - qty.0).abs() < f64::EPSILON {
            Ok((CloseOutcome::FullyClosed(closed), realized_pnl))
        } else {
            let remaining = Trade {
                quantity: self.quantity - qty,
                ..self
            };
            Ok((
                CloseOutcome::PartiallyClosed { closed, remaining },
                realized_pnl,
            ))
        }
    }
}

pub fn update(
    mut trade: Trade<Active>,
    m_id: &MarketId,
    ctx: &UpdateCtx,
) -> ChapatyResult<(State, f64)> {
    let symbol = &m_id.symbol;

    // 1. Capture START Value
    let prev_unrealized_pnl = trade.state.unrealized_pnl;

    // 2. Mark to Market
    let raw_price = ctx.market.try_resolved_close_price(symbol)?.0;
    let current_price = Price(sanitize_price(symbol, raw_price, "mark_price"));
    let ts = ctx.market.current_timestamp();

    trade.state.current_price = current_price;
    trade.state.current_ts = ts;

    // Clean Unrealized PnL
    let current_unrealized_pnl = trade.trade_type.calculate_pnl(
        trade.state.entry_price,
        current_price,
        trade.quantity,
        symbol,
    );
    trade.state.unrealized_pnl = current_unrealized_pnl;

    // 3. Check Triggers
    // A. Detect Triggers (Independent Checks)
    let tp_exit = trade
        .take_profit
        .filter(|&tp| ctx.market.reached_price(tp, symbol))
        .map(|tp| (TerminationReason::TakeProfit, tp.0));

    let sl_exit = trade
        .stop_loss
        .filter(|&sl| ctx.market.reached_price(sl, symbol))
        .map(|sl| (TerminationReason::StopLoss, sl.0));

    // B. Resolve Conflict (Priority Logic)
    let exit = match ctx.bias {
        // Pessimistic: StopLoss triggers first (overrides TP if both occur)
        // If SL didn't trigger, we check if TP triggered.
        ExecutionBias::Pessimistic => sl_exit.or(tp_exit),

        // Optimistic: TakeProfit triggers first (overrides SL if both occur)
        // If TP didn't trigger, we check if SL triggered.
        ExecutionBias::Optimistic => tp_exit.or(sl_exit),
    };

    // 4. Execute Exit if triggered
    if let Some((reason, raw_exit_price)) = exit {
        let exit_price = Price(sanitize_price(symbol, raw_exit_price, "exit_price"));

        let qty = trade.quantity;
        let (outcome, clean_realized_pnl) =
            trade.execute_close(qty, exit_price, ts, reason, symbol)?;

        match outcome {
            CloseOutcome::FullyClosed(c) => {
                let step_delta = clean_realized_pnl - prev_unrealized_pnl;
                Ok((State::Closed(c), step_delta))
            }
            _ => Err(SystemError::InvariantViolation(
                "execute_close(full_qty) returned Partial. Logic Error.".to_string(),
            )
            .into()),
        }
    } else {
        let step_delta = current_unrealized_pnl - prev_unrealized_pnl;
        Ok((State::Active(trade), step_delta))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        agent::AgentIdentifier,
        data::{
            domain::{
                ContractMonth, ContractYear, DataBroker, Exchange, FutureContract, FutureRoot,
                Period, Price, Quantity, Symbol, TradeId,
            },
            event::{MarketId, Ohlcv, OhlcvId},
            view::MarketView,
        },
        gym::trading::{
            config::{EnvConfig, ExecutionBias},
            types::TradeType,
        },
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

    /// A lightweight wrapper around the heavy SimulationData.
    /// It allows us to create a valid MarketView with a simple (low, high, close) API.
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

        /// Returns a valid MarketView borrowing from the owned SimulationData
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
                trade_type: TradeType::Long,
                quantity: Quantity(1.0),
                stop_loss: sl.map(Price),
                take_profit: tp.map(Price),
                entry_price: None,
            },
            Price(entry_price),
            ts("2026-01-19T10:00:00Z"),
            &symbol,
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
                trade_type: TradeType::Short,
                quantity: Quantity(1.0),
                stop_loss: sl.map(Price),
                take_profit: tp.map(Price),
                entry_price: None,
            },
            Price(entry_price),
            ts("2026-01-19T10:00:00Z"),
            &symbol,
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
        assert_eq!(trade.state.unrealized_pnl, 0.0);

        // Market moves up to 1.105
        let fixture = MarketFixture::new(ts("2026-01-19T10:01:00Z"), 1.1, 1.105, 1.105);
        let view = fixture.view();
        let ctx = UpdateCtx {
            market: &view,
            bias: ExecutionBias::Optimistic,
        };
        let (new_state, step_delta) = super::update(trade.clone(), &m_id, &ctx).unwrap();

        // Extract new trade
        let updated = match new_state {
            State::Active(t) => t,
            _ => panic!("Expected Active state"),
        };

        // New unrealized should be positive (price went up, long position)
        // Assuming calculate_pnl returns clean values
        let new_unrealized = updated.state.unrealized_pnl;
        assert!(
            new_unrealized > 0.0,
            "Long should have positive PnL when price rises"
        );

        // step_delta should equal new_unrealized - prev_unrealized
        assert_eq!(step_delta, new_unrealized - 0.0);
    }

    #[test]
    fn test_long_unrealized_pnl_negative() {
        let trade = create_long_active(1.1, None, None);
        let m_id: MarketId = ohlcv_id().into();

        // Initial state: unrealized = 0.0
        assert_eq!(trade.state.unrealized_pnl, 0.0);

        // Market moves down to 1.095
        let fixture = MarketFixture::new(ts("2026-01-19T10:01:00Z"), 1.095, 1.1, 1.095);
        let view = fixture.view();
        let ctx = UpdateCtx {
            market: &view,
            bias: ExecutionBias::Optimistic,
        };
        let (new_state, step_delta) = super::update(trade, &m_id, &ctx).unwrap();

        let updated = match new_state {
            State::Active(t) => t,
            _ => panic!("Expected Active state"),
        };

        let new_unrealized = updated.state.unrealized_pnl;
        assert!(
            new_unrealized < 0.0,
            "Long should have negative PnL when price falls"
        );
        assert_eq!(step_delta, new_unrealized);
    }

    #[test]
    fn test_short_unrealized_pnl_positive() {
        let trade = create_short_active(1.1, None, None);
        let m_id: MarketId = ohlcv_id().into();

        // Initial state: unrealized = 0.0
        assert_eq!(trade.state.unrealized_pnl, 0.0);

        // Market moves up to 1.105 (unfavorable for short)
        let fixture = MarketFixture::new(ts("2026-01-19T10:01:00Z"), 1.1, 1.105, 1.105);
        let view = fixture.view();
        let ctx = UpdateCtx {
            market: &view,
            bias: ExecutionBias::Optimistic,
        };
        let (new_state, step_delta) = super::update(trade, &m_id, &ctx).unwrap();

        let updated = match new_state {
            State::Active(t) => t,
            _ => panic!("Expected Active state"),
        };

        let new_unrealized = updated.state.unrealized_pnl;
        assert!(
            new_unrealized < 0.0,
            "Short should have negative PnL when price rises"
        );
        assert_eq!(step_delta, new_unrealized);
    }

    #[test]
    fn test_short_unrealized_pnl_negative() {
        let trade = create_short_active(1.1, None, None);
        let m_id: MarketId = ohlcv_id().into();

        // Initial state: unrealized = 0.0
        assert_eq!(trade.state.unrealized_pnl, 0.0);

        // Market moves down to 1.095 (favorable for short)
        let fixture = MarketFixture::new(ts("2026-01-19T10:01:00Z"), 1.095, 1.1, 1.095);
        let view = fixture.view();
        let ctx = UpdateCtx {
            market: &view,
            bias: ExecutionBias::Optimistic,
        };
        let (new_state, step_delta) = super::update(trade, &m_id, &ctx).unwrap();

        let updated = match new_state {
            State::Active(t) => t,
            _ => panic!("Expected Active state"),
        };

        let new_unrealized = updated.state.unrealized_pnl;
        assert!(
            new_unrealized > 0.0,
            "Short should have positive PnL when price falls"
        );
        assert_eq!(step_delta, new_unrealized);
    }

    #[test]
    fn test_pnl_delta_calculation() {
        let trade = create_long_active(1.1, None, None);
        let m_id: MarketId = ohlcv_id().into();

        // Initial state: unrealized = 0.0
        assert_eq!(trade.state.unrealized_pnl, 0.0);

        // Step 1: Price moves to 1.102
        let fixture1 = MarketFixture::new(ts("2026-01-19T10:01:00Z"), 1.1, 1.102, 1.102);
        let view1 = fixture1.view();
        let ctx1 = UpdateCtx {
            market: &view1,
            bias: ExecutionBias::Optimistic,
        };
        let (state1, delta1) = super::update(trade, &m_id, &ctx1).unwrap();

        let trade1 = match state1 {
            State::Active(t) => t,
            _ => panic!("Expected Active"),
        };
        let pnl1 = trade1.state.unrealized_pnl;

        // Step 2: Price moves to 1.10400
        let fixture2 = MarketFixture::new(ts("2026-01-19T10:02:00Z"), 1.102, 1.10400, 1.10400);
        let view2 = fixture2.view();
        let ctx2 = UpdateCtx {
            market: &view2,
            bias: ExecutionBias::Optimistic,
        };
        let (state2, delta2) = super::update(trade1, &m_id, &ctx2).unwrap();

        let trade2 = match state2 {
            State::Active(t) => t,
            _ => panic!("Expected Active"),
        };
        let pnl2 = trade2.state.unrealized_pnl;

        // Verify delta is incremental
        assert_eq!(delta1, pnl1 - 0.0, "First delta should be pnl1 - 0");
        assert_eq!(delta2, pnl2 - pnl1, "Second delta should be pnl2 - pnl1");
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
        let (new_state, _step_delta) = super::update(trade, &m_id, &ctx).unwrap();

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
        let (new_state, _step_delta) = super::update(trade, &m_id, &ctx).unwrap();

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
        let (new_state, _) = super::update(trade, &m_id, &ctx).unwrap();

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
        let (new_state, _) = super::update(trade, &m_id, &ctx).unwrap();

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
        let (new_state, _) = super::update(trade, &m_id, &ctx).unwrap();

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
                trade_type: TradeType::Long,
                quantity: Quantity(1.0),
                stop_loss: Some(Price(1.095567)),   // Off-grid
                take_profit: Some(Price(1.105123)), // Off-grid
                entry_price: None,
            },
            Price(1.100789), // Off-grid entry
            ts("2026-01-19T10:00:00Z"),
            &symbol,
        )
        .expect("invalid trade configuration");

        // All prices should be snapped to tick_size grid
        // Assuming sanitize_price rounds to nearest 0.00005
        // 1.100789 -> 1.10080
        // 1.095567 -> 1.09555
        // 1.105123 -> 1.10510

        assert_eq!(trade.state.entry_price.0, 1.10080);
        assert_eq!(trade.stop_loss.unwrap().0, 1.09555);
        assert_eq!(trade.take_profit.unwrap().0, 1.10510);
        // Check they're on grid (divisible by tick_size with some tolerance)
        let remainder = (trade.state.entry_price.0 / 0.00005) % 1.0;
        assert!(remainder.abs() < f64::EPSILON, "Entry price not on grid");
    }

    // ============================================================================
    // Part 4: Modify Tests
    // ============================================================================

    #[test]
    fn test_modify_active_cannot_change_entry() {
        let mut trade = create_long_active(1.1, Some(1.095), Some(1.105));
        let symbol = ohlcv_id().symbol;

        let cmd = ModifyCmd {
            agent_id: trade.agent_id.clone(),
            trade_id: trade.uid,
            new_entry_price: Some(Price(1.11)), // Attempt to change entry
            new_stop_loss: None,
            new_take_profit: None,
        };

        let result = trade.modify(&cmd, &symbol);
        assert!(
            result.is_err(),
            "Should not allow modifying entry price of Active trade"
        );
    }

    #[test]
    fn test_modify_active_valid_sl_tp() {
        let mut trade = create_long_active(1.1, Some(1.095), Some(1.105));
        let symbol = ohlcv_id().symbol;

        let cmd = ModifyCmd {
            agent_id: trade.agent_id.clone(),
            trade_id: trade.uid,
            new_entry_price: None,
            new_stop_loss: Some(Price(1.098)),
            new_take_profit: Some(Price(1.11)),
        };

        trade.modify(&cmd, &symbol).unwrap();

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
            .market_close(&cmd, Price(1.105), ts("2026-01-19T12:00:00Z"), &symbol)
            .unwrap();

        match outcome {
            CloseOutcome::FullyClosed(c) => {
                assert_eq!(c.state.termination_reason, TerminationReason::MarketClose);
                assert!(reward > 0.0, "Should have positive PnL");
                assert_eq!(reward, 625.0);
            }
            _ => panic!("Expected FullyClosed"),
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
            .market_close(&cmd, Price(1.105), ts("2026-01-19T12:00:00Z"), &symbol)
            .unwrap();

        // Verification: Reward Logic
        // Diff: 0.00500 -> 100 ticks
        // Value: 100 ticks * $6.25 * 0.5 qty = $312.50
        assert_eq!(
            reward, 312.5,
            "Reward calculation incorrect for partial close"
        );

        match outcome {
            CloseOutcome::PartiallyClosed { closed, remaining } => {
                // Check Closed Portion
                assert_eq!(closed.quantity, Quantity(0.5));
                assert_eq!(
                    closed.state.realized_pnl, 312.5,
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
            _ => panic!("Expected PartiallyClosed outcome"),
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

        let result = trade.market_close(&cmd, Price(1.105), ts("2026-01-19T12:00:00Z"), &symbol);
        assert!(result.is_err(), "Should reject close qty > position qty");
    }

    // ============================================================================
    // Part 6: Transactional
    // ============================================================================

    #[test]
    fn test_modify_active_invalid_ordering() {
        let mut trade = create_long_active(1.1, Some(1.095), Some(1.105));
        let symbol = ohlcv_id().symbol;

        // Try to set SL above entry (invalid for long)
        let cmd = ModifyCmd {
            agent_id: trade.agent_id.clone(),
            trade_id: trade.uid,
            new_entry_price: None,
            new_stop_loss: Some(Price(1.11)), // Above entry!
            new_take_profit: None,
        };

        let result = trade.modify(&cmd, &symbol);
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
        let mut trade = Trade::<Active>::new(
            OpenCmd {
                trade_id: TradeId(0),
                agent_id: AgentIdentifier::Random,
                trade_type: TradeType::Long,
                quantity: Quantity(1.0),
                stop_loss: Some(Price(1.09000)),
                take_profit: None,
                entry_price: Some(Price(1.1)),
            },
            Price(1.1),
            Utc::now(),
            &symbol,
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

        let result = trade.modify(&cmd, &symbol);

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
}
