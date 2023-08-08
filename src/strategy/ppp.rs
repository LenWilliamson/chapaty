use super::*;
use crate::enums::indicator::PriceHistogramKind;

pub struct Ppp {
    stop_loss: StopLoss,
    take_profit: TakeProfit,
    entry: TradingIndicatorKind,
}

pub struct PppBuilder {
    stop_loss: Option<StopLoss>,
    take_profit: Option<TakeProfit>,
    entry: Option<TradingIndicatorKind>,
}

impl PppBuilder {
    pub fn new() -> Self {
        Self {
            stop_loss: None,
            take_profit: None,
            entry: None,
        }
    }

    pub fn with_stop_loss(self, stop_loss: StopLoss) -> Self {
        Self {
            stop_loss: Some(stop_loss),
            ..self
        }
    }

    pub fn with_take_profit(self, take_profit: TakeProfit) -> Self {
        Self {
            take_profit: Some(take_profit),
            ..self
        }
    }

    pub fn with_entry(self, entry: TradingIndicatorKind) -> Self {
        Self {
            entry: Some(entry),
            ..self
        }
    }

    pub fn build(self) -> Ppp {
        Ppp {
            stop_loss: self.stop_loss.unwrap(),
            take_profit: self.take_profit.unwrap(),
            entry: self.entry.unwrap(),
        }
    }
}

impl Ppp {
    fn get_sl_price(&self, pre_trade_values: &RequiredPreTradeValuesWithData) -> f64 {
        match self.get_trade_kind(pre_trade_values) {
            TradeDirectionKind::Long => self.get_sl_price_long(pre_trade_values),
            TradeDirectionKind::Short => self.get_sl_price_short(pre_trade_values),
            TradeDirectionKind::None => {
                dbg!("Cannot compute stop-loss condition for TradeDirection::None");
                -1.0
            }
        }
    }

    fn get_tp_price(&self, pre_trade_values: &RequiredPreTradeValuesWithData) -> f64 {
        match self.get_trade_kind(pre_trade_values) {
            TradeDirectionKind::Long => self.get_tp_long(pre_trade_values),
            TradeDirectionKind::Short => self.get_tp_short(pre_trade_values),
            TradeDirectionKind::None => {
                dbg!("Cannot compute take-profit condition for TradeDirection::None");
                -1.0
            }
        }
    }
    
    fn get_sl_price_long(&self, pre_trade_values: &RequiredPreTradeValuesWithData) -> f64 {
        let ph = PriceHistogramKind::Tpo1m;
        let value_area_low = pre_trade_values.value_area_low(ph);
        let lowest_trade_price = pre_trade_values.lowest_trade_price();
        let entry_price = self.get_entry_price(pre_trade_values);
        match self.stop_loss.kind {
            StopLossKind::PriceUponTradeEntry => entry_price - self.stop_loss.offset,
            StopLossKind::PrevHighOrLow => lowest_trade_price - self.stop_loss.offset,
            StopLossKind::ValueAreaHighOrLow => value_area_low - self.stop_loss.offset,
        }
    }

    fn get_sl_price_short(&self, pre_trade_values: &RequiredPreTradeValuesWithData) -> f64 {
        let ph = PriceHistogramKind::Tpo1m;
        let value_area_high = pre_trade_values.value_area_high(ph);
        let highest_trade_price = pre_trade_values.highest_trade_price();
        let entry_price = self.get_entry_price(pre_trade_values);
        match self.stop_loss.kind {
            StopLossKind::PriceUponTradeEntry => entry_price + self.stop_loss.offset,
            StopLossKind::PrevHighOrLow => highest_trade_price + self.stop_loss.offset,
            StopLossKind::ValueAreaHighOrLow => value_area_high + self.stop_loss.offset,
        }
    }

    fn get_tp_long(&self, pre_trade_values: &RequiredPreTradeValuesWithData) -> f64 {
        let ph = PriceHistogramKind::Tpo1m;
        let value_area_high = pre_trade_values.value_area_high(ph);
        let lst_trade_price = pre_trade_values.last_trade_price();
        let highest_trade_price = pre_trade_values.highest_trade_price();
        let entry_price = self.get_entry_price(pre_trade_values);
        match self.take_profit.kind {
            TakeProfitKind::PrevClose => lst_trade_price + self.take_profit.offset,
            TakeProfitKind::PriceUponTradeEntry => entry_price + self.take_profit.offset,
            TakeProfitKind::PrevHighOrLow => highest_trade_price + self.take_profit.offset,
            TakeProfitKind::ValueAreaHighOrLow => value_area_high + self.take_profit.offset,
        }
    }
    
    fn get_tp_short(&self, pre_trade_values: &RequiredPreTradeValuesWithData) -> f64 {
        let ph = PriceHistogramKind::Tpo1m;
        let value_area_low = pre_trade_values.value_area_low(ph);
        let lst_trade_price = pre_trade_values.last_trade_price();
        let lowest_trade_price = pre_trade_values.lowest_trade_price();
        let entry_price = self.get_entry_price(pre_trade_values);
        match self.take_profit.kind {
            TakeProfitKind::PrevClose => lst_trade_price - self.take_profit.offset,
            TakeProfitKind::PriceUponTradeEntry => entry_price - self.take_profit.offset,
            TakeProfitKind::PrevHighOrLow => lowest_trade_price - self.take_profit.offset,
            TakeProfitKind::ValueAreaHighOrLow => value_area_low - self.take_profit.offset,
        }
    }
}

impl FromStr for PppBuilder {
    type Err = ChapatyErrorKind;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "PPP" | "Ppp" | "ppp" => Ok(PppBuilder::new()),
            _ => Err(Self::Err::ParseBotError(format!(
                "This strategy <{s}> does not exist"
            ))),
        }
    }
}

impl Strategy for Ppp {
    fn get_trade(&self, pre_trade_values: &RequiredPreTradeValuesWithData) -> Trade {
        Trade {
            entry_price: self.get_entry_price(pre_trade_values),
            stop_loss: self.get_sl_price(pre_trade_values),
            take_profit: self.get_tp_price(pre_trade_values),
            trade_kind: self.get_trade_kind(pre_trade_values),
        }
    }

    fn get_required_pre_trade_vales(&self) -> RequriedPreTradeValues {
        let market_values = vec![
            PreTradeDataKind::LastTradePrice,
            PreTradeDataKind::LowestTradePrice,
            PreTradeDataKind::HighestTradePrice,
        ];
        let trading_indicators = vec![
            TradingIndicatorKind::Poc(PriceHistogramKind::Tpo1m),
            TradingIndicatorKind::ValueAreaHigh(PriceHistogramKind::Tpo1m),
            TradingIndicatorKind::ValueAreaLow(PriceHistogramKind::Tpo1m),
        ];
        RequriedPreTradeValues {
            market_values,
            trading_indicators,
        }
    }

    fn get_entry_price(&self, pre_trade_values: &RequiredPreTradeValuesWithData) -> f64 {
        match self.entry {
            TradingIndicatorKind::Poc(ph) => pre_trade_values.poc(ph),
            TradingIndicatorKind::ValueAreaHigh(ph) => pre_trade_values.value_area_high(ph),
            TradingIndicatorKind::ValueAreaLow(ph) => pre_trade_values.value_area_low(ph),
        }
    }

    /// This function determines the `TradeKind` based on the entry price and last traded price.
    /// * `Short` - last traded price < entry price
    /// * `Long` - last traded price > entry price
    /// * `None` - last traded price = poc
    fn get_trade_kind(
        &self,
        pre_trade_values: &RequiredPreTradeValuesWithData,
    ) -> TradeDirectionKind {
        let last_trade_price = pre_trade_values.last_trade_price();
        let entry_price = self.get_entry_price(pre_trade_values);

        if last_trade_price < entry_price {
            TradeDirectionKind::Short
        } else if last_trade_price > entry_price {
            TradeDirectionKind::Long
        } else {
            TradeDirectionKind::None
        }
    }

    fn get_name(&self) -> String {
        "ppp".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        calculator::pre_trade_values_calculator::RequiredPreTradeValuesWithData,
        enums::{
            indicator::{PriceHistogramKind, TradingIndicatorKind},
            trade_and_pre_trade::TradeDirectionKind,
        },
    };
    use std::collections::HashMap;

    /// This unit test determines if the `TradeKind` based on the POC and last traded price.
    /// * `Short` - last traded price < poc,
    /// * `Long` - last traded price > poc,
    /// * `None` - last traded price = poc,
    ///
    /// is computed correctly.
    #[tokio::test]
    async fn test_get_trade_kind() {
        let sl = StopLoss {
            kind: StopLossKind::PrevHighOrLow, // is equivalent to previous max
            offset: 0.0,
        };
        let tp = TakeProfit {
            kind: TakeProfitKind::PrevClose,
            offset: 0.0,
        };
        let strategy = PppBuilder::new()
            .with_stop_loss(sl)
            .with_take_profit(tp)
            .with_entry(TradingIndicatorKind::Poc(PriceHistogramKind::Tpo1m))
            .build();
        let poc = 100.0;

        let mut trading_indicators = HashMap::new();
        trading_indicators.insert(TradingIndicatorKind::Poc(PriceHistogramKind::Tpo1m), poc);

        let mut pre_trade_data_map = HashMap::new();
        pre_trade_data_map.insert(PreTradeDataKind::LastTradePrice, 99.0);

        let mut pre_trade_values = RequiredPreTradeValuesWithData {
            indicator_values: trading_indicators,
            market_valeus: pre_trade_data_map,
        };
        assert_eq!(
            strategy.get_trade_kind(&pre_trade_values),
            TradeDirectionKind::Short
        );
        pre_trade_values
            .market_valeus
            .insert(PreTradeDataKind::LastTradePrice, 101.0);
        assert_eq!(
            strategy.get_trade_kind(&pre_trade_values),
            TradeDirectionKind::Long
        );

        pre_trade_values
            .market_valeus
            .insert(PreTradeDataKind::LastTradePrice, poc);
        assert_eq!(
            strategy.get_trade_kind(&pre_trade_values),
            TradeDirectionKind::None
        );
    }
}
