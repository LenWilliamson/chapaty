use chrono::{DateTime, Utc};

use crate::{
    data::{
        domain::{Price, Symbol},
        event::MarketId,
    },
    error::{ChapatyError, ChapatyResult, SystemError},
    gym::trading::{
        action::{CancelCmd, ModifyCmd, OpenCmd},
        config::ExecutionBias,
        state::{Active, Canceled, Pending, State, Trade, UpdateCtx, active, sanitize_price},
    },
};

impl Trade<Pending> {
    /// Factory: Creates a Pending trade, enforcing grid snapping on prices.
    pub fn new(
        cmd: OpenCmd,
        limit_price: Price,
        ts: DateTime<Utc>,
        symbol: &Symbol,
    ) -> ChapatyResult<Self> {
        // 1. Sanitize
        let clean_limit_val = sanitize_price(symbol, limit_price.0, "limit");
        let clean_limit = Price(clean_limit_val);

        let clean_sl = cmd
            .stop_loss
            .map(|p| Price(sanitize_price(symbol, p.0, "sl")));
        let clean_tp = cmd
            .take_profit
            .map(|p| Price(sanitize_price(symbol, p.0, "tp")));

        // 2. Validate Logic
        cmd.trade_type
            .price_ordering_validation(clean_sl, Some(clean_limit), clean_tp)?;

        // 3. Construct
        Ok(Self {
            uid: cmd.trade_id,
            agent_id: cmd.agent_id,
            trade_type: cmd.trade_type,
            quantity: cmd.quantity,
            stop_loss: clean_sl,
            take_profit: clean_tp,
            state: Pending {
                created_at: ts,
                limit_price: clean_limit,
            },
        })
    }

    pub fn modify(&mut self, cmd: &ModifyCmd, symbol: &Symbol) -> ChapatyResult<()> {
        if self.agent_id != cmd.agent_id {
            return Err(ChapatyError::System(SystemError::AccessDenied(
                "Agent mismatch".into(),
            )));
        }

        // 1. Calculate Candidates (Transactional Preparation)
        // Candidate Entry (Limit)
        let candidate_entry = if let Some(raw_entry) = cmd.new_entry_price {
            Price(sanitize_price(symbol, raw_entry.0, "modify_limit"))
        } else {
            self.state.limit_price
        };

        // Candidate Stop Loss
        let candidate_sl = if let Some(raw_sl) = cmd.new_stop_loss {
            Some(Price(sanitize_price(symbol, raw_sl.0, "modify_sl")))
        } else {
            self.stop_loss
        };

        // Candidate Take Profit
        let candidate_tp = if let Some(raw_tp) = cmd.new_take_profit {
            Some(Price(sanitize_price(symbol, raw_tp.0, "modify_tp")))
        } else {
            self.take_profit
        };

        // 2. Validate Logic
        self.trade_type.price_ordering_validation(
            candidate_sl,
            Some(candidate_entry),
            candidate_tp,
        )?;

        // 3. Commit Changes
        self.state.limit_price = candidate_entry;
        self.stop_loss = candidate_sl;
        self.take_profit = candidate_tp;

        Ok(())
    }

    pub fn cancel(self, cmd: &CancelCmd, ts: DateTime<Utc>) -> ChapatyResult<Trade<Canceled>> {
        if self.agent_id != cmd.agent_id {
            return Err(ChapatyError::System(SystemError::AccessDenied(
                "Agent mismatch".into(),
            )));
        }

        Ok(Trade {
            uid: self.uid,
            agent_id: self.agent_id,
            trade_type: self.trade_type,
            quantity: self.quantity,
            stop_loss: self.stop_loss,
            take_profit: self.take_profit,
            state: Canceled {
                created_at: self.state.created_at,
                canceled_at: ts,
                limit_price: self.state.limit_price,
            },
        })
    }
}

/// Updates a Pending trade. Checks for Limit activation.
pub fn update(
    trade: Trade<Pending>,
    m_id: &MarketId,
    ctx: &UpdateCtx,
) -> ChapatyResult<(State, f64)> {
    let limit_price = trade.state.limit_price;
    let hit_entry = ctx.market.reached_price(limit_price, &m_id.symbol);

    if !hit_entry {
        return Ok((State::Pending(trade), 0.0));
    }

    // 2. Construct Transient Active State
    // We assume it entered exactly at the limit price.
    let ts = ctx.market.current_timestamp();

    let mut transient_active = Trade {
        uid: trade.uid,
        agent_id: trade.agent_id,
        trade_type: trade.trade_type,
        quantity: trade.quantity,
        stop_loss: trade.stop_loss,
        take_profit: trade.take_profit,
        state: Active {
            entry_ts: ts,
            entry_price: limit_price,
            current_ts: ts,
            current_price: limit_price,
            unrealized_pnl: 0.0,
        },
    };

    // 3. Apply "God Candle" Bias Logic
    // Store originals to restore later
    let original_tp = transient_active.take_profit;
    let original_sl = transient_active.stop_loss;

    match ctx.bias {
        ExecutionBias::Pessimistic => {
            // Pessimistic: We assume we missed the TP (happened before Entry).
            // Blind the TP so active::update checks SL only.
            transient_active.take_profit = None;
        }
        ExecutionBias::Optimistic => {
            // Optimistic: We assume we avoided the SL (happened before Entry).
            // Blind the SL so active::update checks TP only.
            transient_active.stop_loss = None;
        }
    }

    // 4. Delegate to Active Logic
    let (new_state, reward_delta) = active::update(transient_active, m_id, ctx)?;

    // 5. Post-Process (Restore Logic)
    match new_state {
        State::Active(mut t) => {
            // The trade survived the candle.
            // Restore whatever we blinded so the state is correct for the next tick.
            match ctx.bias {
                ExecutionBias::Pessimistic => t.take_profit = original_tp,
                ExecutionBias::Optimistic => t.stop_loss = original_sl,
            }
            Ok((State::Active(t), reward_delta))
        }
        other => Ok((other, reward_delta)),
    }
}

#[cfg(test)]
mod test {

    use std::sync::Arc;

    use super::*;
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
            types::{TerminationReason, TradeType},
        },
        sim::{
            cursor_group::CursorGroup,
            data::{SimulationData, SimulationDataBuilder},
        },
        sorted_vec_map::SortedVecMap,
    };

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

    fn create_long_pending(limit_price: f64, sl: Option<f64>, tp: Option<f64>) -> Trade<Pending> {
        let symbol = ohlcv_id().symbol;
        Trade::<Pending>::new(
            OpenCmd {
                trade_id: TradeId(100),
                agent_id: AgentIdentifier::Random,
                trade_type: TradeType::Long,
                quantity: Quantity(1.0),
                stop_loss: sl.map(Price),
                take_profit: tp.map(Price),
                entry_price: Some(Price(limit_price)),
            },
            Price(limit_price),
            ts("2026-01-19T10:00:00Z"),
            &symbol,
        )
        .expect("invalid trade configuration")
    }

    fn create_short_pending(limit_price: f64, sl: Option<f64>, tp: Option<f64>) -> Trade<Pending> {
        let symbol = ohlcv_id().symbol;
        Trade::<Pending>::new(
            OpenCmd {
                trade_id: TradeId(101),
                agent_id: AgentIdentifier::Random,
                trade_type: TradeType::Short,
                quantity: Quantity(1.0),
                stop_loss: sl.map(Price),
                take_profit: tp.map(Price),
                entry_price: Some(Price(limit_price)),
            },
            Price(limit_price),
            ts("2026-01-19T10:00:00Z"),
            &symbol,
        )
        .expect("invalid trade configuration")
    }

    // ============================================================================
    // Part 1: No Trigger
    // ============================================================================

    #[test]
    fn test_pending_no_trigger_stays_pending() {
        let trade = create_long_pending(1.09000, None, None);
        let m_id: MarketId = ohlcv_id().into();

        // Market range doesn't hit limit
        let fixture = MarketFixture::new(ts("2026-01-19T10:01:00Z"), 1.10000, 1.11000, 1.10500);
        let view = fixture.view();
        let ctx = UpdateCtx {
            market: &view,
            bias: ExecutionBias::Optimistic,
        };
        let (new_state, reward) = super::update(trade, &m_id, &ctx).unwrap();

        match new_state {
            State::Pending(t) => {
                assert_eq!(t.state.limit_price, Price(1.09000));
            }
            _ => panic!("Expected to remain Pending"),
        }

        assert_eq!(reward, 0.0, "No reward when pending");
    }

    // ============================================================================
    // Part 2: Clean Entry (No SL/TP Hit)
    // ============================================================================

    #[test]
    fn test_pending_clean_entry_becomes_active() {
        let trade = create_long_pending(1.09500, None, None);
        let m_id: MarketId = ohlcv_id().into();

        // Market hits limit but no SL/TP
        let fixture = MarketFixture::new(ts("2026-01-19T10:01:00Z"), 1.09500, 1.10000, 1.09800);
        let view = fixture.view();
        let ctx = UpdateCtx {
            market: &view,
            bias: ExecutionBias::Optimistic,
        };
        let (new_state, reward) = super::update(trade, &m_id, &ctx).unwrap();

        match new_state {
            State::Active(t) => {
                assert_eq!(t.state.entry_price, Price(1.09500));
                // Reward should be based on closing at 1.09800 vs entry at 1.09500
                assert!(reward != 0.0, "Should have some PnL from price movement");
            }
            _ => panic!("Expected Active state"),
        }
    }

    // ============================================================================
    // Part 3: God Candle Entry - Complex Scenarios
    // ============================================================================

    #[test]
    fn test_god_candle_entry_pessimistic_missed_tp() {
        // Long pending @ 1.09500, TP @ 1.10000
        // Candle hits BOTH entry and TP (Range: 1.095 - 1.100)
        let trade = create_long_pending(1.09500, None, Some(1.10000));
        let m_id: MarketId = ohlcv_id().into();

        // Pessimistic: Assumes TP happened before entry (missed it)
        // Close is 1.09800
        let fixture = MarketFixture::new(ts("2026-01-19T10:01:00Z"), 1.09500, 1.10000, 1.09800);
        let view = fixture.view();
        let ctx = UpdateCtx {
            market: &view,
            bias: ExecutionBias::Pessimistic,
        };
        let (new_state, reward) = super::update(trade, &m_id, &ctx).unwrap();

        match new_state {
            State::Active(t) => {
                // Trade should be Active (survived)
                assert_eq!(t.state.entry_price, Price(1.09500));
                // TP should be restored
                assert_eq!(t.take_profit, Some(Price(1.10000)), "TP should be restored");

                // Reward Calculation:
                // Entry 1.09500 -> Current 1.09800 = +0.00300
                // Ticks: 0.00300 / 0.00005 = 60 ticks
                // Value: 60 * 6.25 = 375.0
                assert_eq!(reward, 375.0);
            }
            _ => panic!("Expected Active state (missed TP opportunity)"),
        }
    }

    #[test]
    fn test_god_candle_entry_optimistic_hit_tp() {
        // Long pending @ 1.09500, TP @ 1.10000
        // Candle hits BOTH
        let trade = create_long_pending(1.09500, None, Some(1.10000));
        let m_id: MarketId = ohlcv_id().into();

        // Optimistic: Entry happened before TP
        let fixture = MarketFixture::new(ts("2026-01-19T10:01:00Z"), 1.09500, 1.10000, 1.09800);
        let view = fixture.view();
        let ctx = UpdateCtx {
            market: &view,
            bias: ExecutionBias::Optimistic,
        };
        let (new_state, reward) = super::update(trade, &m_id, &ctx).unwrap();

        match new_state {
            State::Closed(c) => {
                assert_eq!(c.state.termination_reason, TerminationReason::TakeProfit);
                assert_eq!(c.state.exit_price, Price(1.10000));
                assert!(reward > 0.0, "Should have positive PnL from TP");
            }
            _ => panic!("Expected Closed state with TakeProfit"),
        }
    }

    #[test]
    fn test_god_candle_entry_and_sl_optimistic() {
        // Long pending @ 1.09500, SL @ 1.09000
        // Candle hits both entry (1.095) and SL (1.090)
        let trade = create_long_pending(1.09500, Some(1.09000), None);
        let m_id: MarketId = ohlcv_id().into();

        let fixture = MarketFixture::new(ts("2026-01-19T10:01:00Z"), 1.09000, 1.09500, 1.09200);
        let view = fixture.view();
        let ctx = UpdateCtx {
            market: &view,
            bias: ExecutionBias::Optimistic,
        };
        let (new_state, reward) = super::update(trade, &m_id, &ctx).unwrap();

        // UPDATED EXPECTATION:
        // Optimistic bias assumes SL happened *before* Entry (or we got lucky).
        // Therefore, the trade enters and SURVIVES this candle.
        match new_state {
            State::Active(t) => {
                assert_eq!(t.stop_loss, Some(Price(1.09000)), "SL should be restored");
                // PnL based on Close (1.092) vs Entry (1.095) -> Negative
                assert!(reward < 0.0, "Active but losing position");
            }
            _ => panic!("Expected Active state (Optimistic logic ignores SL on entry candle)"),
        }
    }

    #[test]
    fn test_god_candle_entry_and_sl_pessimistic() {
        // Long pending @ 1.09500, SL @ 1.09000
        // Candle hits both Entry (1.095) and SL (1.090)
        let trade = create_long_pending(1.09500, Some(1.09000), None);
        let m_id: MarketId = ohlcv_id().into();

        let fixture = MarketFixture::new(ts("2026-01-19T10:01:00Z"), 1.09000, 1.09500, 1.09200);
        let view = fixture.view();
        let ctx = UpdateCtx {
            market: &view,
            bias: ExecutionBias::Pessimistic,
        };
        let (new_state, reward) = super::update(trade, &m_id, &ctx).unwrap();

        // EXPECTATION:
        // Pessimistic bias assumes Entry happened -> Then Price fell to SL.
        // Therefore, the trade enters and immediately DIES.
        match new_state {
            State::Closed(c) => {
                assert_eq!(c.state.termination_reason, TerminationReason::StopLoss);
                assert_eq!(c.state.exit_price, Price(1.09000));
                assert!(reward < 0.0, "Immediate loss on entry candle");
            }
            _ => panic!("Expected Closed state (Pessimistic logic assumes SL hit after Entry)"),
        }
    }

    #[test]
    fn test_god_candle_entry_sl_and_tp_pessimistic() {
        // Long @ 1.09500, SL @ 1.09000, TP @ 1.10000
        // All three prices hit in one candle
        let trade = create_long_pending(1.09500, Some(1.09000), Some(1.10000));
        let m_id: MarketId = ohlcv_id().into();

        // Pessimistic: TP is missed, but SL can still trigger after entry
        let fixture = MarketFixture::new(ts("2026-01-19T10:01:00Z"), 1.09000, 1.10000, 1.09200);
        let view = fixture.view();
        let ctx = UpdateCtx {
            market: &view,
            bias: ExecutionBias::Pessimistic,
        };
        let (new_state, _) = super::update(trade, &m_id, &ctx).unwrap();

        // Should close on SL (TP was "missed")
        match new_state {
            State::Closed(c) => {
                assert_eq!(c.state.termination_reason, TerminationReason::StopLoss);
            }
            _ => panic!("Expected Closed on SL"),
        }
    }

    #[test]
    fn test_god_candle_entry_sl_and_tp_optimistic() {
        // Long @ 1.09500, SL @ 1.09000, TP @ 1.10000
        let trade = create_long_pending(1.09500, Some(1.09000), Some(1.10000));
        let m_id: MarketId = ohlcv_id().into();

        // Optimistic: TP takes priority. SL is assumed dodged.
        let fixture = MarketFixture::new(ts("2026-01-19T10:01:00Z"), 1.09000, 1.10000, 1.09800);
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
            _ => panic!("Expected TakeProfit in optimistic mode"),
        }
    }

    // ============================================================================
    // Part 4: Modify Tests
    // ============================================================================

    #[test]
    fn test_modify_pending_all_fields() {
        let mut trade = create_long_pending(1.09000, Some(1.08500), Some(1.09500));
        let symbol = ohlcv_id().symbol;

        let cmd = ModifyCmd {
            agent_id: trade.agent_id.clone(),
            trade_id: trade.uid,
            new_entry_price: Some(Price(1.09200)),
            new_stop_loss: Some(Price(1.08800)),
            new_take_profit: Some(Price(1.09700)),
        };

        trade.modify(&cmd, &symbol).unwrap();

        assert_eq!(trade.state.limit_price, Price(1.09200));
        assert_eq!(trade.stop_loss, Some(Price(1.08800)));
        assert_eq!(trade.take_profit, Some(Price(1.09700)));
    }

    #[test]
    fn test_modify_pending_partial_update() {
        let mut trade = create_long_pending(1.09000, Some(1.08500), Some(1.09500));
        let symbol = ohlcv_id().symbol;

        // Only modify TP
        let cmd = ModifyCmd {
            agent_id: trade.agent_id.clone(),
            trade_id: trade.uid,
            new_entry_price: None,
            new_stop_loss: None,
            new_take_profit: Some(Price(1.09800)),
        };

        trade.modify(&cmd, &symbol).unwrap();

        // Only TP should change
        assert_eq!(trade.state.limit_price, Price(1.09000));
        assert_eq!(trade.stop_loss, Some(Price(1.08500)));
        assert_eq!(trade.take_profit, Some(Price(1.09800)));
    }

    #[test]
    fn test_modify_pending_invalid_ordering_short() {
        let mut trade = create_short_pending(1.10000, Some(1.10500), Some(1.09500));
        let symbol = ohlcv_id().symbol;

        // Try to set SL below entry (invalid for short)
        let cmd = ModifyCmd {
            agent_id: trade.agent_id.clone(),
            trade_id: trade.uid,
            new_entry_price: None,
            new_stop_loss: Some(Price(1.09000)), // Below entry - invalid for short
            new_take_profit: None,
        };

        let result = trade.modify(&cmd, &symbol);
        assert!(result.is_err(), "Should reject SL below entry for short");
    }

    // ============================================================================
    // Part 5: Cancel Tests
    // ============================================================================

    #[test]
    fn test_cancel_pending() {
        let trade = create_long_pending(1.09000, Some(1.08500), Some(1.09500));

        let cmd = CancelCmd {
            agent_id: trade.agent_id.clone(),
            trade_id: trade.uid,
        };

        let canceled = trade.cancel(&cmd, ts("2026-01-19T12:00:00Z")).unwrap();

        assert_eq!(
            canceled.state.termination_reason(),
            TerminationReason::Canceled
        );
        assert_eq!(canceled.state.limit_price, Price(1.09000));
        assert_eq!(canceled.state.created_at, ts("2026-01-19T10:00:00Z"));
        assert_eq!(canceled.state.canceled_at, ts("2026-01-19T12:00:00Z"));
    }

    #[test]
    fn test_cancel_agent_mismatch() {
        let trade = create_long_pending(1.09000, None, None);

        let cmd = CancelCmd {
            agent_id: AgentIdentifier::Named(Arc::new("Different".to_string())),
            trade_id: trade.uid,
        };

        let result = trade.cancel(&cmd, ts("2026-01-19T12:00:00Z"));
        assert!(result.is_err(), "Should reject cancel from wrong agent");
    }

    // ============================================================================
    // Part 6: Price Sanitization
    // ============================================================================

    #[test]
    fn test_pending_prices_sanitized() {
        let symbol = ohlcv_id().symbol;

        // Create with off-grid prices
        let trade = Trade::<Pending>::new(
            OpenCmd {
                trade_id: TradeId(200),
                agent_id: AgentIdentifier::Random,
                trade_type: TradeType::Long,
                quantity: Quantity(1.0),
                stop_loss: Some(Price(1.085567)),
                take_profit: Some(Price(1.095123)),
                entry_price: Some(Price(1.090789)),
            },
            Price(1.090789),
            ts("2026-01-19T10:00:00Z"),
            &symbol,
        )
        .expect("invalid trade configuration");

        // Verify limit price was sanitized
        assert_eq!(trade.state.limit_price.0, 1.0908);
        assert_eq!(trade.stop_loss.unwrap().0, 1.08555);
        assert_eq!(trade.take_profit.unwrap().0, 1.09510);

        // Check it's on grid
        let remainder = (trade.state.limit_price.0 / 0.00005) % 1.0;
        assert!(remainder.abs() < f64::EPSILON, "Limit price not on grid");
    }

    // ============================================================================
    // Part 7: Edge Cases
    // ============================================================================

    #[test]
    fn test_pending_entry_exactly_at_limit() {
        let trade = create_long_pending(1.09500, None, None);
        let m_id: MarketId = ohlcv_id().into();

        // Price touches limit exactly (low == high == close == limit)
        let fixture = MarketFixture::new(ts("2026-01-19T10:01:00Z"), 1.09500, 1.09500, 1.09500);
        let view = fixture.view();
        let ctx = UpdateCtx {
            market: &view,
            bias: ExecutionBias::Optimistic,
        };
        let (new_state, reward) = super::update(trade, &m_id, &ctx).unwrap();

        match new_state {
            State::Active(t) => {
                assert_eq!(t.state.entry_price, Price(1.09500));
                assert_eq!(t.state.current_price, Price(1.09500));
                assert_eq!(t.state.unrealized_pnl, 0.0);
                assert_eq!(reward, 0.0, "No PnL when entry == close");
            }
            _ => panic!("Should become Active"),
        }
    }

    #[test]
    fn test_pending_short_clean_entry() {
        let trade = create_short_pending(1.10500, Some(1.11000), Some(1.10000));
        let m_id: MarketId = ohlcv_id().into();

        // Market hits limit but no triggers
        let fixture = MarketFixture::new(ts("2026-01-19T10:01:00Z"), 1.10100, 1.10500, 1.10200);
        let view = fixture.view();
        let ctx = UpdateCtx {
            market: &view,
            bias: ExecutionBias::Optimistic,
        };
        let (new_state, reward) = super::update(trade, &m_id, &ctx).unwrap();

        match new_state {
            State::Active(t) => {
                assert_eq!(t.state.entry_price, Price(1.10500));
                // Short entered at 1.10500, closed at 1.10200 -> positive PnL
                assert!(reward > 0.0, "Short should profit from price drop");
            }
            other => panic!("Expected Active, got {other:?}"),
        }
    }

    #[test]
    fn test_pending_no_sl_no_tp() {
        let trade = create_long_pending(1.09500, None, None);
        let m_id: MarketId = ohlcv_id().into();

        // Entry hit, no exits possible
        let fixture = MarketFixture::new(ts("2026-01-19T10:01:00Z"), 1.09500, 1.10000, 1.09800);
        let view = fixture.view();
        let ctx = UpdateCtx {
            market: &view,
            bias: ExecutionBias::Pessimistic,
        };
        let (new_state, _) = super::update(trade, &m_id, &ctx).unwrap();

        match new_state {
            State::Active(t) => {
                assert!(t.stop_loss.is_none());
                assert!(t.take_profit.is_none());
            }
            _ => panic!("Should become Active"),
        }
    }

    #[test]
    fn test_pending_multiple_updates_no_trigger() {
        let trade = create_long_pending(1.09000, None, None);
        let m_id: MarketId = ohlcv_id().into();

        // Step 1: No trigger
        let fixture1 = MarketFixture::new(ts("2026-01-19T10:01:00Z"), 1.10000, 1.11000, 1.10500);
        let view1 = fixture1.view();
        let ctx1 = UpdateCtx {
            market: &view1,
            bias: ExecutionBias::Optimistic,
        };
        let (state1, reward1) = super::update(trade, &m_id, &ctx1).unwrap();

        let trade1 = match state1 {
            State::Pending(t) => t,
            _ => panic!("Should stay Pending"),
        };
        assert_eq!(reward1, 0.0);

        // Step 2: Still no trigger
        let fixture2 = MarketFixture::new(ts("2026-01-19T10:02:00Z"), 1.09500, 1.10500, 1.10000);
        let view2 = fixture2.view();
        let ctx2 = UpdateCtx {
            market: &view2,
            bias: ExecutionBias::Optimistic,
        };
        let (state2, reward2) = super::update(trade1, &m_id, &ctx2).unwrap();

        match state2 {
            State::Pending(t) => {
                assert_eq!(t.state.limit_price, Price(1.09000));
            }
            _ => panic!("Should stay Pending"),
        }
        assert_eq!(reward2, 0.0);
    }

    // ============================================================================
    // Part 8: Transactional
    // ============================================================================

    #[test]
    fn test_modify_pending_transactional() {
        let mut trade = create_long_pending(1.09000, Some(1.08500), None);
        let symbol = ohlcv_id().symbol;

        // Try to set invalid SL (above entry)
        let cmd = ModifyCmd {
            agent_id: trade.agent_id.clone(),
            trade_id: trade.uid,
            new_entry_price: None,
            new_stop_loss: Some(Price(1.10000)), // Invalid: above entry
            new_take_profit: Some(Price(1.09500)), // Valid
        };

        let result = trade.modify(&cmd, &symbol);
        assert!(result.is_err(), "Should reject invalid SL");

        // Verify state unchanged (transactional)
        assert_eq!(
            trade.stop_loss,
            Some(Price(1.08500)),
            "SL should be unchanged"
        );
        assert_eq!(
            trade.take_profit, None,
            "TP should still be None (not partially committed)"
        );
    }

    #[test]
    fn test_modify_is_transactional() {
        let symbol = Symbol::Future(FutureContract {
            root: FutureRoot::EurUsd,
            month: ContractMonth::December,
            year: ContractYear::Y5,
        });

        // Setup: Long Pending Trade @ 1.10000, SL @ 1.09000
        let mut trade = Trade::<Pending>::new(
            OpenCmd {
                trade_id: TradeId(0),
                agent_id: AgentIdentifier::Random,
                trade_type: TradeType::Long,
                quantity: Quantity(1.0),
                stop_loss: Some(Price(1.09000)),
                take_profit: None,
                entry_price: Some(Price(1.1)),
            },
            Price(1.10000),
            Utc::now(),
            &symbol,
        )
        .expect("invalid trade configuration");

        // Action: Try to modify SL to 1.11000 (ABOVE Entry -> INVALID for Long)
        // AND try to set TP to 1.12000 (VALID)
        let cmd = ModifyCmd {
            agent_id: trade.agent_id.clone(),
            trade_id: trade.uid,
            new_entry_price: None,
            new_stop_loss: Some(Price(1.11000)), // Invalid: > Entry
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
