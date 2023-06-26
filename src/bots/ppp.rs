use super::*;
use crate::{
    enums::{
        bots::{BotKind, PreTradeDataKind},
        strategies::{StopLossKind, TakeProfitKind},
    },
    producers::profit_loss_report::{StopLoss, TakeProfit}, // trade::PreTradeData,
};

pub struct Ppp {
    // entry_condition
    stop_loss: StopLoss,
    take_profit: TakeProfit,
    // pre_trade_data: Option<&'a EnumMap<PreTradeDataKind, f64>>,
    bot_kind: BotKind,
}

impl Ppp {
    pub fn new() -> Self {
        Ppp {
            stop_loss: StopLoss {
                condition: StopLossKind::PrevLow, // is equivalent to previous max
                offset: 0.0,
            },
            take_profit: TakeProfit {
                condition: TakeProfitKind::PrevClose,
                offset: 0.0,
            },
            // pre_trade_data: None,
            bot_kind: BotKind::Ppp,
        }
    }   
}

impl FromStr for Ppp {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Ppp" => Ok(Ppp::new()),
            _ => Err(Self::Err::ParseBotError("This bot does currently not exist".to_string()))
        }
    }
}


impl Bot for Ppp {

    fn set_stop_loss(&mut self, sl: StopLoss) {
        self.stop_loss = sl;
    }

    fn set_take_profit(&mut self, tp: TakeProfit) {
        self.take_profit = tp;
    }

    fn register_pre_trade_data(&self) -> Vec<PreTradeDataKind> {
        vec![
            PreTradeDataKind::Poc,
            PreTradeDataKind::LastTradePrice,
            PreTradeDataKind::LowestTradePrice,
            PreTradeDataKind::HighestTradePrice,
        ]
    }

    // fn set_pre_trade_data(&mut self, pre_trade_data: &'a EnumMap<PreTradeDataKind, f64>) {
    //     self.pre_trade_data = Some(pre_trade_data);
    // }

    fn get_entry_price(&self, data: &EnumMap<PreTradeDataKind, f64>) -> f64 {
        // let data = self.pre_trade_data.unwrap();
        data[PreTradeDataKind::Poc]
    }

    /// This function determines the `TradeKind` based on the entry price and last traded price.
    /// * `Short` - last traded price < entry price
    /// * `Long` - last traded price > entry price
    /// * `None` - last traded price = poc
    fn get_trade_kind(&self, data: &EnumMap<PreTradeDataKind, f64>) -> Option<TradeKind> {
        // let data = self.pre_trade_data.unwrap();
        let last_trade_price = data[PreTradeDataKind::LastTradePrice];
        let entry_price = self.get_entry_price(data);

        if last_trade_price < entry_price {
            Some(TradeKind::Short)
        } else if last_trade_price > entry_price {
            Some(TradeKind::Long)
        } else {
            None
        }
    }

    fn get_sl_price(&self, data: &EnumMap<PreTradeDataKind, f64>) -> f64 {
        // let data = self.pre_trade_data.unwrap();
        let lowest_trad_price = data[PreTradeDataKind::LowestTradePrice];
        let highest_trad_price = data[PreTradeDataKind::HighestTradePrice];

        if let Some(trade_kind) = self.get_trade_kind(data) {
            match trade_kind {
                TradeKind::Long => match self.stop_loss.condition {
                    StopLossKind::PrevPoc => self.get_entry_price(data) - self.stop_loss.offset,
                    StopLossKind::PrevLow => lowest_trad_price - self.stop_loss.offset,
                    // PrevHigh is counter intutitve
                    StopLossKind::PrevHigh => lowest_trad_price - self.stop_loss.offset,
                },

                TradeKind::Short => match self.stop_loss.condition {
                    StopLossKind::PrevPoc => self.get_entry_price(data) + self.stop_loss.offset,
                    // PrevLow is counter intutitve
                    StopLossKind::PrevLow => highest_trad_price + self.stop_loss.offset,
                    StopLossKind::PrevHigh => highest_trad_price + self.stop_loss.offset,
                },

                TradeKind::None => {
                    panic!("Remove TradeKind::None")
                }
            }
        } else {
            panic!("Cannot compute stop-loss condition for TradeKind::None")
        }
    }

    fn get_tp_price(&self, data: &EnumMap<PreTradeDataKind, f64>) -> f64 {
        // let data = self.pre_trade_data.unwrap();
        let lst_trade_price = data[PreTradeDataKind::LastTradePrice];

        if let Some(trade_kind) = self.get_trade_kind(data) {
            match trade_kind {
                TradeKind::Long => match self.take_profit.condition {
                    TakeProfitKind::PrevClose => lst_trade_price + self.take_profit.offset,
                    TakeProfitKind::PrevPoc => panic!("PrevPoc not implemented for PPP"),
                },

                TradeKind::Short => match self.take_profit.condition {
                    TakeProfitKind::PrevClose => lst_trade_price - self.take_profit.offset,
                    TakeProfitKind::PrevPoc => panic!("PrevPoc not implemented for PPP"),
                },

                TradeKind::None => {
                    panic!("Remove TradeKind::None")
                }
            }
        } else {
            panic!("Cannot compute stop-loss condition for TradeKind::None")
        }
    }

    fn get_bot_kind(&self) -> BotKind {
        self.bot_kind
    }
}

#[cfg(test)]
mod tests {

    use enum_map::enum_map;

    use super::*;

    /// This unit test determines if the `TradeKind` based on the POC and last traded price.
    /// * `Short` - last traded price < poc,
    /// * `Long` - last traded price > poc,
    /// * `None` - last traded price = poc,
    ///
    /// is computed correctly.
    #[tokio::test]
    async fn test_get_trade_kind() {
        let bot = Ppp::new();
        let poc = 100.0;

        let mut pre_trade_data_map = enum_map! {PreTradeDataKind::Poc => poc, _ => 0.0 };

        pre_trade_data_map[PreTradeDataKind::LastTradePrice] = 99.0;
        assert!(matches!(
            bot.get_trade_kind(&pre_trade_data_map).unwrap(),
            TradeKind::Short
        ));
        pre_trade_data_map[PreTradeDataKind::LastTradePrice] = 101.0;
        assert!(matches!(
            bot.get_trade_kind(&pre_trade_data_map).unwrap(),
            TradeKind::Long
        ));
        pre_trade_data_map[PreTradeDataKind::LastTradePrice] = 100.0;
        assert!(matches!(bot.get_trade_kind(&pre_trade_data_map), None));
    }
}
