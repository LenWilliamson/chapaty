pub mod news_counter;
pub mod news_rassler;
pub mod news_rassler_with_confirmation;
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
    /// Offset is used by bot diffrently, can be USD or percentage, etc..
    pub offset: f64,
}

#[derive(Clone, Copy)]
pub struct TakeProfit {
    pub kind: TakeProfitKind,
    /// Offset is used by bot diffrently, can be USD or percentage, etc..
    pub offset: f64,
}

pub struct TradeRequestObject {
    pub pre_trade_values: RequiredPreTradeValuesWithData,
    pub initial_balance: Option<InitialBalance>,
    pub market: MarketKind,
}

#[derive(Clone, Default)]
pub struct RequriedPreTradeValues {
    pub market_values: Vec<PreTradeDataKind>,
    pub trading_indicators: Vec<TradingIndicatorKind>,
}

#[automock]
pub trait Strategy {
    fn get_trade(&self, trade_request_object: &TradeRequestObject) -> Trade;
    fn get_required_pre_trade_values(&self) -> RequriedPreTradeValues;
    fn get_entry_price(&self, pre_trade_values: &RequiredPreTradeValuesWithData) -> Option<f64>;
    
    /// Returns the entry timestamp and a flag indicating whether
    /// the entry timestamp should be computed if `None` is returned.
    ///
    /// ### Arguments
    ///
    /// * `pre_trade_values` - A reference to the required pre-trade values with data.
    ///
    /// ### Returns
    ///
    /// * `(Option<i64>, bool)` - The first value is the entry timestamp (if available).
    ///   The second value is a boolean indicating if the timestamp should be computed
    ///   when the `Option<i64>` is `None`.
    fn get_entry_ts(&self, pre_trade_values: &RequiredPreTradeValuesWithData) -> (Option<i64>, bool);
    fn get_trade_kind(
        &self,
        pre_trade_values: &RequiredPreTradeValuesWithData,
    ) -> TradeDirectionKind;
    fn get_name(&self) -> String;
    fn is_pre_trade_day_equal_to_trade_day(&self) -> bool;

    /// Returns `true` if the strategy shall only be evaluated on news. Otherwise the strategy 
    /// is evaluated on all days but news.
    fn is_only_trading_on_news(&self) -> bool;
    fn get_news(&self) -> HashSet<NaiveDate>;
}
