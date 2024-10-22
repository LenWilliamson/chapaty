use std::{marker::PhantomData, sync::Arc};

use crate::{
    enums::trade_and_pre_trade::{TradeCloseKind, TradeDirectionKind},
    strategy::Strategy,
    MarketKind,
};

use super::market_simulation_data::Market;

pub struct Idle;
pub struct Active;

#[derive(Debug, Clone)]
pub struct Close;

/// Represents a trade in a discrete finite automaton (DFA) that simulates the behavior of a trading strategy during market data simulation.
///
/// This structure encapsulates the state of a trade and its associated parameters. The trade can be in one of three states:
/// `Idle`, `Active`, or `Close`. Only `Idle` and `Active` states are considered accepting states during the simulation.
/// At the end of the simulation process, all active trades will transition to the `Close` state and ultimately to the accepting `Idle` state.
///
/// The following trait will be implemented to indicate accepting states:
///
/// ```rust
/// pub trait Accepting {}
/// impl Accepting for Idle {}
/// impl Accepting for Active {}
/// ```
///
/// TODO
/// **Note:** The trait bound for `Accepting` is not yet enforced in the current implementation.
/// Ensure that trades passed to functions or structures requiring accepting states implement this trait.
/// This will be implemented in a future update.
#[derive(Debug, Clone, Copy)]
pub struct Trade<'a, State> {
    pub entry_ts: Option<i64>,
    pub entry_price: Option<f64>,
    pub current_ts: Option<i64>,
    pub current_price: Option<f64>,
    pub trade_direction_kind: Option<TradeDirectionKind>,
    pub current_profit: Option<f64>,
    pub stop_loss: Option<f64>,
    pub take_profit: Option<f64>,
    pub close_event: Option<TradeCloseKind>,
    pub strategy: Option<&'a (dyn Strategy + Send + Sync)>,
    pub _state: PhantomData<State>,
}

pub enum TradeResult<'a> {
    Idle(Trade<'a, Idle>),
    Active(Trade<'a, Active>),
    Close(Trade<'a, Close>),
}

impl<'a> TradeResult<'a> {
    pub fn update_on_market_event(&mut self, market_event: &Market) {
        match self {
            TradeResult::Idle(trade) => trade.update_on_market_event(market_event),
            TradeResult::Active(trade) => trade.update_on_market_event(market_event),
            TradeResult::Close(trade) => trade.update_on_market_event(market_event),
        }
    }
}

pub struct ActivationEvent<'a> {
    pub entry_ts: i64,
    pub entry_price: f64,
    pub stop_loss: f64,
    pub take_profit: f64,
    pub trade_direction_kind: TradeDirectionKind,
    pub strategy: &'a (dyn Strategy + Send + Sync),
}

pub struct CloseEvent {
    pub exit_ts: i64,
    pub exit_price: f64,
    pub close_event_kind: TradeCloseKind,
}

impl<'a> Trade<'a, Idle> {
    pub fn new() -> TradeResult<'a> {
        let idle_trade = Self {
            entry_ts: None,
            entry_price: None,
            current_ts: None,
            current_price: None,
            trade_direction_kind: None,
            current_profit: None,
            stop_loss: None,
            take_profit: None,
            close_event: None,
            strategy: None,
            _state: PhantomData,
        };
        TradeResult::Idle(idle_trade)
    }

    pub fn activation_event(self, activation_event: &ActivationEvent<'a>) -> TradeResult<'a> {
        let mut active_trade = Trade::<Active> {
            entry_ts: Some(activation_event.entry_ts),
            entry_price: Some(activation_event.entry_price),
            current_ts: self.current_ts,
            current_price: self.current_price,
            trade_direction_kind: Some(activation_event.trade_direction_kind),
            current_profit: Some(0.0),
            stop_loss: Some(activation_event.stop_loss),
            take_profit: Some(activation_event.take_profit),
            close_event: None,
            strategy: Some(activation_event.strategy),
            _state: PhantomData,
        };
        active_trade.update_profit();
        TradeResult::Active(active_trade)
    }
}

impl<'a> Trade<'a, Active> {
    fn close_trade(self, close_event: &CloseEvent) -> TradeResult<'a> {
        let close_trade = Trade::<Close> {
            entry_ts: self.entry_ts,
            entry_price: self.entry_price,
            current_ts: Some(close_event.exit_ts),
            current_price: Some(close_event.exit_price),
            trade_direction_kind: self.trade_direction_kind,
            current_profit: self.compute_profit(close_event.exit_price),
            stop_loss: self.stop_loss,
            take_profit: self.take_profit,
            close_event: Some(close_event.close_event_kind),
            strategy: self.strategy,
            _state: PhantomData,
        };
        TradeResult::Close(close_trade)
    }

    pub fn close_event(self, close_event: &CloseEvent) -> TradeResult<'a> {
        self.close_trade(close_event)
    }

    pub fn timeout_event(self, close_event: &CloseEvent) -> TradeResult<'a> {
        self.close_trade(close_event)
    }

    pub fn pivot_event(self, pivot_event: &ActivationEvent) -> TradeResult<'a> {
        self.close_trade(&CloseEvent {
            exit_ts: pivot_event.entry_ts,
            exit_price: pivot_event.entry_price,
            close_event_kind: TradeCloseKind::Pivot,
        })
    }
}

impl<'a> Trade<'a, Close> {
    pub fn pivot_event(self, pivot_event: &ActivationEvent<'a>) -> TradeResult<'a> {
        match self.reset() {
            TradeResult::Idle(idle_trade) => idle_trade.activation_event(pivot_event),
            _ => panic!("Expected Trade<Close> to be reset to Trade<Idle>."),
        }
    }

    pub fn reset(self) -> TradeResult<'a> {
        Trade::<Idle>::new()
    }
}

impl<'a, State> Trade<'a, State> {
    pub fn compute_profit(&self, price: f64) -> Option<f64> {
        let entry_px = self.entry_price?;
        let direction = self.trade_direction_kind?;

        match direction {
            TradeDirectionKind::Short => Some(entry_px - price),
            TradeDirectionKind::Long => Some(price - entry_px),
            TradeDirectionKind::None => None,
        }
    }

    pub fn update_profit(&mut self) {
        if let Some(price) = self.current_price {
            self.current_profit = self.compute_profit(price);
        }
    }

    /// Updates the trade based on a new market event (e.g., a new OHLC candle).
    /// This sets the current price and timestamp from the market event.
    /// If the trade is not idle, it also updates the profit.
    pub fn update_on_market_event(&mut self, market_event: &Market) {
        self.current_price = market_event.ohlc.close;
        self.current_ts = market_event.ohlc.close_ts;
        self.update_profit();
    }

    pub fn curate_precision(&mut self, market: &MarketKind) {
        self.entry_price = self
            .entry_price
            .and_then(|px| Some(market.round_float_to_correct_decimal_place(px)));

        self.current_price = self
            .current_price
            .and_then(|px| Some(market.round_float_to_correct_decimal_place(px)));

        self.current_profit = self
            .current_profit
            .and_then(|px| Some(market.round_float_to_correct_decimal_place(px)));

        self.stop_loss = self
            .stop_loss
            .and_then(|px| Some(market.round_float_to_correct_decimal_place(px)));

        self.take_profit = self
            .take_profit
            .and_then(|px| Some(market.round_float_to_correct_decimal_place(px)));
    }

    pub fn expected_win_in_tick(&self, tick_factor: f64) -> Option<f64> {
        Some((self.compute_profit(self.take_profit?)? / tick_factor).round())
    }

    pub fn expected_loss_in_tick(&self, tick_factor: f64) -> Option<f64> {
        Some((self.compute_profit(self.stop_loss?)? / tick_factor).round())
    }

    pub fn compute_risk_reward_ratio(&self, tick_factor: f64) -> Option<f64> {
        let win = self.expected_win_in_tick(tick_factor)?;
        let loss = self.expected_loss_in_tick(tick_factor)?;
        if loss != 0.0 {
            Some((win / loss).abs())
        } else {
            Some(f64::MAX)
        }
    }
}

// #[cfg(test)]
// mod test {

//     use super::*;

//     #[tokio::test]
//     async fn test_compute_trade_values() {
//         let trade_long = Trade {
//             entry_price: 100.0,
//             stop_loss: Some(-1.0),
//             take_profit: Some(-1.0),
//             trade_kind: TradeDirectionKind::Long,
//             is_valid: true,
//         };

//         assert_eq!(1.0, trade_long.profit(101.0));
//         assert_eq!(0.0, trade_long.profit(100.0));
//         assert_eq!(-1.0, trade_long.profit(99.0));

//         let trade_short = Trade {
//             entry_price: 100.0,
//             stop_loss: Some(-1.0),
//             take_profit: Some(-1.0),
//             trade_kind: TradeDirectionKind::Short,
//             is_valid: true,
//         };

//         assert_eq!(-1.0, trade_short.profit(101.0));
//         assert_eq!(0.0, trade_short.profit(100.0));
//         assert_eq!(1.0, trade_short.profit(99.0));
//     }
// }
