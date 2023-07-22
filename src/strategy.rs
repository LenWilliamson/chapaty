use std::str::FromStr;

use crate::{
    calculator::pre_trade_values_calculator::PreTradeValues,
    enums::{
        self,
        bots::{PreTradeDataKind, StrategyKind, TradingIndicatorKind},
        error::ChapatyError,
        trades::TradeKind,
    }, bot::trade::Trade,
};

pub mod ppp;

#[derive(Clone, Copy)]
pub struct StopLoss {
    pub condition: enums::strategies::StopLossKind,
    pub offset: f64,
}

#[derive(Clone, Copy)]
pub struct TakeProfit {
    pub condition: enums::strategies::TakeProfitKind,
    pub offset: f64,
}

pub trait Strategy {
    fn set_stop_loss(&mut self, sl: StopLoss);
    fn set_take_profit(&mut self, tp: TakeProfit);
    fn register_trading_indicators(&self) -> Vec<TradingIndicatorKind>;
    fn required_pre_trade_data(&self) -> Vec<PreTradeDataKind>;
    fn get_entry_price(&self, pre_trade_values: &PreTradeValues) -> f64;
    fn get_trade(&self, pre_trade_values: &PreTradeValues) -> Trade;
    fn get_trade_kind(&self, pre_trade_values: &PreTradeValues) -> TradeKind;
    fn get_sl_price(&self, pre_trade_values: &PreTradeValues) -> f64;
    fn get_tp_price(&self, pre_trade_values: &PreTradeValues) -> f64;
    fn get_bot_kind(&self) -> StrategyKind;
}
