pub mod ppp;
use crate::{
    bot::trade::Trade,
    calculator::pre_trade_values_calculator::PreTradeValues,
    enums::{
        bot::{StopLossKind, StrategyKind, TakeProfitKind},
        error::ChapatyErrorKind,
        indicator::TradingIndicatorKind,
        trade_and_pre_trade::{PreTradeDataKind, TradeDirectionKind},
    },
};
use mockall::automock;
use std::str::FromStr;

#[derive(Clone, Copy)]
pub struct StopLoss {
    pub condition: StopLossKind,
    pub offset: f64,
}

#[derive(Clone, Copy)]
pub struct TakeProfit {
    pub condition: TakeProfitKind,
    pub offset: f64,
}

#[automock]
pub trait Strategy {
    fn set_stop_loss(&mut self, sl: StopLoss);
    fn set_take_profit(&mut self, tp: TakeProfit);
    fn register_trading_indicators(&self) -> Vec<TradingIndicatorKind>;
    fn required_pre_trade_data(&self) -> Vec<PreTradeDataKind>;
    fn get_entry_price(&self, pre_trade_values: &PreTradeValues) -> f64;
    fn get_trade(&self, pre_trade_values: &PreTradeValues) -> Trade;
    fn get_trade_kind(&self, pre_trade_values: &PreTradeValues) -> TradeDirectionKind;
    fn get_sl_price(&self, pre_trade_values: &PreTradeValues) -> f64;
    fn get_tp_price(&self, pre_trade_values: &PreTradeValues) -> f64;
    fn get_bot_kind(&self) -> StrategyKind;
}
