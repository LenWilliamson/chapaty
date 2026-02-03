use rand::{Rng, rngs::ThreadRng};

use crate::{
    agent::AgentIdentifier,
    data::{
        domain::{Instrument, Price, Quantity, Symbol, TradeId},
        view::MarketView,
    },
    error::{ChapatyResult, SystemError},
    gym::trading::{
        action::{Action, Actions, MarketCloseCmd, ModifyCmd, OpenCmd},
        state::{State, States},
        types::TradeType,
    },
};

pub struct ActionSpace<'env> {
    states: &'env States,
    view: MarketView<'env>,
    rng: ThreadRng,
}

impl<'env> ActionSpace<'env> {
    pub fn new(states: &'env States, view: MarketView<'env>) -> Self {
        Self {
            states,
            view,
            rng: rand::rng(),
        }
    }

    pub fn sample(&mut self) -> ChapatyResult<Actions> {
        let mut action_list = Vec::new();

        for market_id in &*self.view.market_ids() {
            let market_trade = self.states.get_live_trades(market_id).first();

            if let Some(state) = market_trade {
                // === STATE: HAS POSITION ===
                // 20% Close, 30% Modify, 50% Hold
                let choice = self.rng.random_range(0..10);
                match choice {
                    0..2 => {
                        // CLOSE
                        action_list.push((
                            *market_id,
                            Action::MarketClose(MarketCloseCmd {
                                agent_id: AgentIdentifier::Random,
                                trade_id: state.trade_id(),
                                quantity: Some(state.quantity()),
                            }),
                        ));
                    }
                    2..5 => {
                        // MODIFY
                        // 1. Get the reference price (Current or Limit)
                        let current_price = match state {
                            State::Active(t) => t.state().current_price().0,
                            State::Pending(t) => t.state().limit_price().0,
                            dead_state => {
                                return Err(SystemError::InvariantViolation(format!(
                                    "Found dead state in live_vec for {market_id:?}: {dead_state:?}",
                                ))
                                .into());
                            }
                        };

                        // 2. Drift
                        let drift = self.rng.random_range(0.90..1.10);
                        let base_price = market_id.symbol.normalize_price(current_price * drift);

                        // 3. Stochastic SL/TP based on Trade Direction
                        // We sample a percentage distance, not a fixed scalar.
                        let (sl_price, tp_price) = match state.trade_type() {
                            TradeType::Long => {
                                // Long: SL is BELOW (-5% to -15%), TP is ABOVE (+5% to +25%)
                                let sl_pct = self.rng.random_range(0.85..0.95);
                                let tp_pct = self.rng.random_range(1.05..1.25);
                                (base_price * sl_pct, base_price * tp_pct)
                            }
                            TradeType::Short => {
                                // Short: SL is ABOVE (+5% to +15%), TP is BELOW (-5% to -25%)
                                let sl_pct = self.rng.random_range(1.05..1.15);
                                let tp_pct = self.rng.random_range(0.75..0.95);
                                (base_price * sl_pct, base_price * tp_pct)
                            }
                        };

                        action_list.push((
                            *market_id,
                            Action::Modify(ModifyCmd {
                                agent_id: AgentIdentifier::Random,
                                trade_id: state.trade_id(),
                                new_entry_price: None,
                                new_stop_loss: Some(Price(
                                    market_id.symbol.normalize_price(sl_price),
                                )),
                                new_take_profit: Some(Price(
                                    market_id.symbol.normalize_price(tp_price),
                                )),
                            }),
                        ));
                    }
                    _ => { /* No-Op (Hold) */ }
                }
            } else {
                // === STATE: NO POSITION ===
                // 20% Open, 80% No-Op (Hold)
                if self.rng.random_bool(0.20) {
                    // A. Get Current Price
                    if let Ok(current_price_struct) =
                        self.view.try_resolved_close_price(&market_id.symbol)
                    {
                        let current_price = current_price_struct.0;

                        // B. Define Target Notional (e.g. $10,000 Risk)
                        let target_notional = 10_000.0;

                        // C. Calculate Quantity based on Instrument Type
                        let quantity = match market_id.symbol {
                            Symbol::Spot(_) => {
                                // Spot: Qty = Target / Price
                                Quantity(target_notional / current_price)
                            }
                            Symbol::Future(f) => {
                                // Futures: Qty = Target / Contract_Value
                                // Point Value ($ per 1.0 price movement)
                                let point_value = f.root.tick_value_usd() / f.root.tick_size();
                                let contract_value = current_price * point_value;

                                // Ensure at least 1 contract
                                let contracts = (target_notional / contract_value).round().max(1.0);
                                Quantity(contracts)
                            }
                        };

                        let new_uid = TradeId(self.rng.random());
                        let side = if self.rng.random_bool(0.5) {
                            TradeType::Long
                        } else {
                            TradeType::Short
                        };

                        action_list.push((
                            *market_id,
                            Action::Open(OpenCmd {
                                agent_id: AgentIdentifier::Random,
                                trade_id: new_uid,
                                trade_type: side,
                                quantity,
                                entry_price: None,
                                stop_loss: None,
                                take_profit: None,
                            }),
                        ));
                    }
                }
            }
        }

        Ok(action_list.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        data::{
            domain::{
                ContractMonth, ContractYear, DataBroker, Exchange, FutureContract, FutureRoot,
                Period, SpotPair,
            },
            event::{MarketId, Ohlcv, OhlcvId},
            view::MarketView,
        },
        gym::trading::config::EnvConfig,
        sim::{
            cursor_group::CursorGroup,
            data::{SimulationData, SimulationDataBuilder},
        },
        sorted_vec_map::SortedVecMap,
    };
    use chrono::{DateTime, Utc};

    // ========================================================================
    // 1. Fixtures & Helpers
    // ========================================================================

    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    /// Market ID Helper: Spot (BTC/USDT)
    fn spot_id() -> OhlcvId {
        OhlcvId {
            broker: DataBroker::Binance,
            exchange: Exchange::Binance,
            symbol: Symbol::Spot(SpotPair::BtcUsdt),
            period: Period::Minute(1),
        }
    }

    /// Market ID Helper: Future (6EZ5)
    fn future_id() -> OhlcvId {
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

    /// Helper to inject an active trade using the REAL production logic.
    fn inject_active_trade(
        states: &mut States,
        view: &MarketView,
        market_id: MarketId,
        uid: i64,
        qty: f64,
    ) {
        let cmd = OpenCmd {
            agent_id: AgentIdentifier::Random,
            trade_id: TradeId(uid),
            trade_type: TradeType::Long,
            quantity: Quantity(qty),
            // Market Order (None) -> handle_open will resolve price from 'view'
            entry_price: None,
            stop_loss: None,
            take_profit: None,
        };

        // Direct call to pub(super) method
        states
            .open(market_id, cmd, view)
            .expect("Failed to open test trade via handle_open");
    }

    // ========================================================================
    // 2. The Market Fixture (Your Implementation)
    // ========================================================================

    fn create_fixture(market_id: OhlcvId, price: f64) -> (SimulationData, CursorGroup) {
        let timestamp = ts("2026-01-19T10:00:00Z");

        let candle = Ohlcv {
            open_timestamp: timestamp,
            close_timestamp: timestamp + chrono::Duration::minutes(1),
            open: Price(price),
            high: Price(price),
            low: Price(price),
            close: Price(price),
            volume: Quantity(1000.0),
            quote_asset_volume: None,
            number_of_trades: None,
            taker_buy_base_asset_volume: None,
            taker_buy_quote_asset_volume: None,
        };

        let mut map = SortedVecMap::new();
        map.insert(market_id, vec![candle].into_boxed_slice());

        let sim_data = SimulationDataBuilder::new()
            .with_ohlcv(map)
            .build(EnvConfig::default())
            .expect("Failed to build sim data");

        // Cursor initializes at the start of data
        let cursor = CursorGroup::new(&sim_data).expect("Failed to create cursor");

        (sim_data, cursor)
    }

    // ========================================================================
    // 3. Test Cases
    // ========================================================================

    #[test]
    fn test_sample_no_position_generates_opens_only() {
        let oid = spot_id();
        let market_id: MarketId = oid.into();

        // 1. Setup Real Market Data (Price = 50,000)
        let (sim_data, cursor) = create_fixture(oid, 50_000.0);
        let view = MarketView::new(&sim_data, &cursor).unwrap();

        let states = States::default();
        let mut space = ActionSpace::new(&states, view);

        let mut generated_open = false;

        // Run multiple samples to catch the 20% probability
        for _ in 0..100 {
            let actions = space
                .sample()
                .expect("sampling action space should succeed");

            for (m_id, action) in actions.into_sorted_iter() {
                assert_eq!(m_id, market_id);

                match action {
                    Action::Open(cmd) => {
                        generated_open = true;
                        // LOGIC CHECK: Sizing
                        // Target Risk $10,000 / Price 50,000 = 0.2
                        let expected = 0.2;
                        assert!(
                            (cmd.quantity.0 - expected).abs() < 0.0001,
                            "Spot sizing incorrect. Got {}, Expected {}",
                            cmd.quantity.0,
                            expected
                        );
                    }
                    Action::MarketClose(_) | Action::Modify(_) => {
                        panic!("Generated Close/Modify command for empty state!");
                    }
                    _ => {}
                }
            }
        }

        assert!(
            generated_open,
            "Failed to generate any Open actions in 100 tries"
        );
    }

    #[test]
    fn test_sample_has_position_generates_close_or_modify() {
        let oid = spot_id();
        let market_id: MarketId = oid.into();

        // 1. Setup Data & View (Price = 50,000)
        let (sim_data, cursor) = create_fixture(oid, 50_000.0);
        let mut states = States::default();
        let view = MarketView::new(&sim_data, &cursor).unwrap();

        // Use the simplified helper
        inject_active_trade(&mut states, &view, market_id, 123, 1.0);

        // 3. Create ActionSpace
        let mut space = ActionSpace::new(&states, view);

        let mut generated_close = false;
        let mut generated_modify = false;

        // 4. Run Sampling
        for _ in 0..200 {
            let actions = space
                .sample()
                .expect("sampling action space should succeed");

            for (_, action) in actions.into_sorted_iter() {
                match action {
                    Action::MarketClose(cmd) => {
                        generated_close = true;
                        assert_eq!(cmd.trade_id.0, 123);
                    }
                    Action::Modify(cmd) => {
                        generated_modify = true;
                        assert_eq!(cmd.trade_id.0, 123);
                    }
                    Action::Open(_) => {
                        panic!("Generated Open command when position already exists!");
                    }
                    _ => {}
                }
            }
        }

        assert!(generated_close, "Failed to generate Close action");
        assert!(generated_modify, "Failed to generate Modify action");
    }

    #[test]
    fn test_futures_sizing_logic() {
        // Setup View with Price = 1.10
        let (sim_data, cursor) = create_fixture(future_id(), 1.10);
        let view = MarketView::new(&sim_data, &cursor).unwrap();

        let states = States::default();
        let mut space = ActionSpace::new(&states, view);

        // 6E Calculation Verification:
        // Tick Size: 0.00005, Tick Value: $6.25
        // Point Value = 6.25 / 0.00005 = $125,000 per 1.0 movement
        // Contract Value = 1.10 * 125,000 = $137,500
        // Target Risk = $10,000
        // Contracts = 10,000 / 137,500 = 0.072...
        // Logic clamps to min(1.0)

        let mut found = false;
        for _ in 0..100 {
            let actions = space
                .sample()
                .expect("sampling action space should succeed");

            if let Some((_, Action::Open(cmd))) = actions.into_sorted_iter().next() {
                assert_eq!(
                    cmd.quantity.0, 1.0,
                    "Futures should clamp to min 1.0 contract"
                );
                found = true;
                break;
            }
        }
        assert!(found, "Did not generate futures trade");
    }
}
