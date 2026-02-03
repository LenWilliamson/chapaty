use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::{
    agent::{Agent, AgentIdentifier},
    data::{
        domain::{Quantity, TradeId},
        event::{ClosePriceProvider, OhlcvId, SmaId},
        view::StreamView,
    },
    error::ChapatyResult,
    gym::trading::{
        action::{Action, Actions, MarketCloseCmd, OpenCmd},
        observation::Observation,
        types::TradeType,
    },
    math::indicator::{StreamingIndicator, StreamingSma},
};

// ================================================================================================
// Streaming SMA Crossover
// ================================================================================================
#[derive(Debug, Clone, Serialize)]
pub struct StreamingCrossover {
    #[serde(skip)]
    ohlcv_id: OhlcvId,
    fast_period: u16,
    slow_period: u16,
    #[serde(skip)]
    fast_sma: StreamingSma,
    #[serde(skip)]
    slow_sma: StreamingSma,
    #[serde(skip)]
    current_fast: Option<f64>,
    #[serde(skip)]
    current_slow: Option<f64>,
    #[serde(skip)]
    trade_counter: i64,
    #[serde(skip)]
    last_processed_ts: Option<DateTime<Utc>>,
}

impl StreamingCrossover {
    pub fn new(ohlcv_id: OhlcvId, fast_period: u16, slow_period: u16) -> Self {
        Self {
            ohlcv_id,
            fast_sma: StreamingSma::new(fast_period),
            slow_sma: StreamingSma::new(slow_period),
            fast_period,
            slow_period,
            trade_counter: 0,
            current_fast: None,
            current_slow: None,
            last_processed_ts: None,
        }
    }
}

impl Agent for StreamingCrossover {
    fn identifier(&self) -> AgentIdentifier {
        AgentIdentifier::Named(Arc::new("StreamingCrossover".to_string()))
    }

    fn reset(&mut self) {
        self.fast_sma.reset();
        self.slow_sma.reset();
        self.trade_counter = 0;
        self.current_fast = None;
        self.current_slow = None;
        self.last_processed_ts = None;
    }

    fn act(&mut self, obs: Observation) -> ChapatyResult<Actions> {
        let market_view = &obs.market_view;

        // 1. Fetch the latest candle
        let Some(candle) = market_view.ohlcv().last_event(&self.ohlcv_id) else {
            return Ok(Actions::no_op());
        };

        // 2. Update Internal State (Idempotency check)
        // We only push to the SMA buffer if we have moved to a new timestamp.
        if self.last_processed_ts != Some(candle.close_timestamp) {
            self.current_fast = self.fast_sma.update(candle.close.0);
            self.current_slow = self.slow_sma.update(candle.close.0);
            self.last_processed_ts = Some(candle.close_timestamp);
        }

        // 3. Check Signal Validity
        let (Some(fast), Some(slow)) = (self.current_fast, self.current_slow) else {
            // SMAs are not warm yet
            return Ok(Actions::no_op());
        };

        // 4. Determine Position Status
        let agent_id = self.identifier();
        let active_trade = obs.states.find_active_trade_for_agent(&agent_id);

        // 5. Signal Logic
        if fast > slow {
            // Golden Cross (Bullish): Fast > Slow
            // If we are not already Long, we enter.
            if active_trade.is_none() {
                self.trade_counter += 1;

                let cmd = OpenCmd {
                    agent_id,
                    trade_id: TradeId(self.trade_counter),
                    trade_type: TradeType::Long,
                    quantity: Quantity(1.0),
                    entry_price: None, // Market Order
                    stop_loss: None,
                    take_profit: None,
                };

                return Ok(Actions::from((self.ohlcv_id.into(), Action::Open(cmd))));
            }
        } else if fast < slow {
            // Death Cross (Bearish): Fast < Slow
            // If we have an open Long position, close it.
            if let Some((_, state)) = active_trade {
                let cmd = MarketCloseCmd {
                    agent_id,
                    trade_id: state.trade_id(),
                    quantity: None, // Close Full Position
                };
                return Ok(Actions::from((
                    self.ohlcv_id.into(),
                    Action::MarketClose(cmd),
                )));
            }
        }

        Ok(Actions::no_op())
    }
}

// ================================================================================================
// Precomputed SMA Crossover
// ================================================================================================

#[derive(Debug, Clone, Serialize)]
pub struct PrecomputedCrossover {
    #[serde(skip)]
    ohlcv_id: OhlcvId,
    fast_sma_id: SmaId,
    slow_sma_id: SmaId,

    #[serde(skip)]
    trade_counter: i64,
}

impl PrecomputedCrossover {
    pub fn new(ohlcv_id: OhlcvId, fast_sma_id: SmaId, slow_sma_id: SmaId) -> Self {
        Self {
            ohlcv_id,
            fast_sma_id,
            slow_sma_id,
            trade_counter: 0,
        }
    }
}

impl Agent for PrecomputedCrossover {
    fn identifier(&self) -> AgentIdentifier {
        AgentIdentifier::Named(Arc::new("PrecomputedCrossover".to_string()))
    }

    fn reset(&mut self) {
        self.trade_counter = 0;
    }

    fn act(&mut self, obs: Observation) -> ChapatyResult<Actions> {
        let view = &obs.market_view;

        // 1. Get pre-computed values directly from the environment
        let fast_event = view.sma().last_event(&self.fast_sma_id);
        let slow_event = view.sma().last_event(&self.slow_sma_id);

        let (Some(fast_evt), Some(slow_evt)) = (fast_event, slow_event) else {
            return Ok(Actions::no_op());
        };

        let fast = fast_evt.close_price();
        let slow = slow_evt.close_price();

        // 2. Position Management
        let agent_id = self.identifier();
        let active_trade = obs.states.find_active_trade_for_agent(&agent_id);

        // 3. Signal Logic
        if fast > slow {
            // Buy Signal
            if active_trade.is_none() {
                self.trade_counter += 1;

                let cmd = OpenCmd {
                    agent_id,
                    trade_id: TradeId(self.trade_counter),
                    trade_type: TradeType::Long,
                    quantity: Quantity(1.0),
                    entry_price: None,
                    stop_loss: None,
                    take_profit: None,
                };
                return Ok(Actions::from((self.ohlcv_id.into(), Action::Open(cmd))));
            }
        } else if fast < slow {
            // Sell Signal
            if let Some((_, state)) = active_trade {
                let cmd = MarketCloseCmd {
                    agent_id,
                    trade_id: state.trade_id(),
                    quantity: None,
                };
                return Ok(Actions::from((
                    self.ohlcv_id.into(),
                    Action::MarketClose(cmd),
                )));
            }
        }

        Ok(Actions::no_op())
    }
}
