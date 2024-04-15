pub mod news;
pub mod ppp;

use crate::{
    bot::trade::Trade,
    calculator::pre_trade_values_calculator::RequiredPreTradeValuesWithData,
    enums::{
        bot::{StopLossKind, TakeProfitKind},
        error::ChapatyErrorKind,
        indicator::TradingIndicatorKind,
        trade_and_pre_trade::{PreTradeDataKind, TradeDirectionKind},
    },
    trading_indicator::initial_balance::InitialBalance,
    MarketKind,
};
use chrono::NaiveDate;
use mockall::automock;
use std::{collections::HashSet, str::FromStr};

#[derive(Clone, Copy)]
pub struct StopLoss {
    pub kind: StopLossKind,
    pub offset: f64,
}

#[derive(Clone, Copy)]
pub struct TakeProfit {
    pub kind: TakeProfitKind,
    pub offset: f64,
}

pub struct TradeRequestObject {
    pub pre_trade_values: RequiredPreTradeValuesWithData,
    pub initial_balance: Option<InitialBalance>,
    pub market: MarketKind,
}

#[derive(Clone)]
pub struct RequriedPreTradeValues {
    pub market_values: Vec<PreTradeDataKind>,
    pub trading_indicators: Vec<TradingIndicatorKind>,
}

#[automock]
pub trait Strategy {
    fn get_trade(&self, trade_request_object: &TradeRequestObject) -> Trade;
    fn get_required_pre_trade_values(&self) -> RequriedPreTradeValues;
    fn get_entry_price(&self, pre_trade_values: &RequiredPreTradeValuesWithData) -> f64;
    fn get_trade_kind(
        &self,
        pre_trade_values: &RequiredPreTradeValuesWithData,
    ) -> TradeDirectionKind;
    fn get_name(&self) -> String;
    fn is_pre_trade_day_equal_to_trade_day(&self) -> bool;
    fn is_trading_on_news(&self) -> bool;
    fn get_news(&self) -> HashSet<NaiveDate>;
}
