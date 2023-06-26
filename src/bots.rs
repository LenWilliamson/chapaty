use std::{str::FromStr, sync::Arc};

use crate::{enums::{bots::{PreTradeDataKind, BotKind}, trades::TradeKind, error::Error}, producers::profit_loss_report::{StopLoss, TakeProfit}};
use enum_map::EnumMap;

use self::ppp::Ppp;

pub mod ppp;

pub trait Bot {
    fn set_stop_loss(&mut self, sl: StopLoss);
    fn set_take_profit(&mut self, tp: TakeProfit);
    fn register_pre_trade_data(&self) -> Vec<PreTradeDataKind>;
    // fn set_pre_trade_data(&mut self, pre_trade_data: &'a EnumMap<PreTradeDataKind, f64>);
    fn get_entry_price(&self, data: &EnumMap<PreTradeDataKind, f64>) -> f64;
    fn get_trade_kind(&self, data: &EnumMap<PreTradeDataKind, f64>) -> Option<TradeKind>;
    fn get_sl_price(&self, data: &EnumMap<PreTradeDataKind, f64>) -> f64;
    fn get_tp_price(&self, data: &EnumMap<PreTradeDataKind, f64>) -> f64;
    fn get_bot_kind(&self) -> BotKind;
}



#[cfg(test)]
mod tests {
    use enum_map::{Enum, EnumMap};

    #[test]
    fn test() {
        #[derive(Enum)]
        enum Example {
            A,
            B,
            C,
            D,
        }

        let mut map = EnumMap::default();
        // new initializes map with default values
        assert_eq!(map[Example::A], 0);
        map[Example::A] = 3;
        assert_eq!(map[Example::A], 3);
    }
}
