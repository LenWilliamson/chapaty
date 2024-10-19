use super::*;
use crate::{bot::pre_trade_data, enums::{indicator::PriceHistogramKind, trade_and_pre_trade::TradeCloseKind}};

#[derive(Debug, Clone, Copy)]
pub struct Ppp {
    stop_loss: StopLoss,
    take_profit: TakeProfit,
    entry: TradingIndicatorKind,
    market_simulation_data_kind: MarketSimulationDataKind,
}

pub struct PppBuilder {
    stop_loss: Option<StopLoss>,
    take_profit: Option<TakeProfit>,
    entry: Option<TradingIndicatorKind>,
    market_simulation_data_kind: Option<MarketSimulationDataKind>,
}

impl PppBuilder {
    pub fn new() -> Self {
        Self {
            stop_loss: None,
            take_profit: None,
            entry: None,
            market_simulation_data_kind: None,
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

    pub fn with_market_simulation_data_kind(
        self,
        market_simulation_data_kind: MarketSimulationDataKind,
    ) -> Self {
        Self {
            market_simulation_data_kind: Some(market_simulation_data_kind),
            ..self
        }
    }

    pub fn build(self) -> Ppp {
        Ppp {
            stop_loss: self.stop_loss.unwrap(),
            take_profit: self.take_profit.unwrap(),
            entry: self.entry.unwrap(),
            market_simulation_data_kind: self.market_simulation_data_kind.unwrap(),
        }
    }
}

impl Ppp {
    fn get_sl_price(&self, request: &SimulationEvent) -> Option<f64> {
        match self.get_trade_kind(&request.pre_trade_values) {
            TradeDirectionKind::Long => Some(self.get_sl_price_long(request)),
            TradeDirectionKind::Short => Some(self.get_sl_price_short(request)),
            TradeDirectionKind::None => None,
        }
    }

    fn get_tp_price(&self, request: &SimulationEvent) -> Option<f64> {
        match self.get_trade_kind(&request.pre_trade_values) {
            TradeDirectionKind::Long => Some(self.get_tp_long(request)),
            TradeDirectionKind::Short => Some(self.get_tp_short(request)),
            TradeDirectionKind::None => None,
        }
    }

    fn compute_sl_price(&self, request: &SimulationEvent, is_long: bool) -> f64 {
        let pre_trade_values = &request.pre_trade_values;
        let ph = PriceHistogramKind::Tpo1m;
        let (value_area, trade_price, sign) = if is_long {
            (
                pre_trade_values.value_area_low(ph),
                pre_trade_values.lowest_trade_price(),
                -1.0,
            )
        } else {
            (
                pre_trade_values.value_area_high(ph),
                pre_trade_values.highest_trade_price(),
                1.0,
            )
        };
        let entry_price = self.get_entry_price(pre_trade_values).unwrap();
        let offset = request
            .market_kind
            .try_offset_in_tick(self.stop_loss.offset);

        match self.stop_loss.kind {
            StopLossKind::PriceUponTradeEntry => entry_price + sign * offset,
            StopLossKind::PrevHighOrLow => trade_price + sign * offset,
            StopLossKind::ValueAreaHighOrLow => value_area + sign * offset,
        }
    }

    fn compute_tp_price(&self, request: &SimulationEvent, is_long: bool) -> f64 {
        let pre_trade_values = &request.pre_trade_values;
        let ph = PriceHistogramKind::Tpo1m;
        let (value_area, trade_price, sign) = if is_long {
            (
                pre_trade_values.value_area_high(ph),
                pre_trade_values.highest_trade_price(),
                1.0,
            )
        } else {
            (
                pre_trade_values.value_area_low(ph),
                pre_trade_values.lowest_trade_price(),
                -1.0,
            )
        };
        let entry_price = self.get_entry_price(pre_trade_values).unwrap();
        let lst_trade_price = pre_trade_values.last_trade_price();
        let offset = request
            .market_kind
            .try_offset_in_tick(self.take_profit.offset);

        match self.take_profit.kind {
            TakeProfitKind::PrevClose => lst_trade_price + sign * offset,
            TakeProfitKind::PriceUponTradeEntry => entry_price + sign * offset,
            TakeProfitKind::PrevHighOrLow => trade_price + sign * offset,
            TakeProfitKind::ValueAreaHighOrLow => value_area + sign * offset,
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
        let entry_price = self.get_entry_price(pre_trade_values).unwrap();

        if last_trade_price < entry_price {
            TradeDirectionKind::Short
        } else if last_trade_price > entry_price {
            TradeDirectionKind::Long
        } else {
            TradeDirectionKind::None
        }
    }

    fn get_sl_price_long(&self, request: &SimulationEvent) -> f64 {
        self.compute_sl_price(request, true)
    }

    fn get_sl_price_short(&self, request: &SimulationEvent) -> f64 {
        self.compute_sl_price(request, false)
    }

    fn get_tp_long(&self, request: &SimulationEvent) -> f64 {
        self.compute_tp_price(request, true)
    }

    fn get_tp_short(&self, request: &SimulationEvent) -> f64 {
        self.compute_tp_price(request, false)
    }

    // fn get_trade(&self, request: &SimulationEvent) -> Trade {
    //     Trade {
    //         entry_price: self.get_entry_price(&request.pre_trade_values).unwrap(),
    //         stop_loss: self.get_sl_price(request),
    //         take_profit: self.get_tp_price(request),
    //         trade_kind: self.get_trade_kind(&request.pre_trade_values),
    //         is_valid: true,
    //     }
    // }

    fn get_entry_price(&self, pre_trade_values: &RequiredPreTradeValuesWithData) -> Option<f64> {
        match self.entry {
            TradingIndicatorKind::Poc(ph) => Some(pre_trade_values.poc(ph)),
            TradingIndicatorKind::ValueAreaHigh(ph) => Some(pre_trade_values.value_area_high(ph)),
            TradingIndicatorKind::ValueAreaLow(ph) => Some(pre_trade_values.value_area_low(ph)),
        }
    }

    fn get_entry_ts(
        &self,
        _pre_trade_values: &RequiredPreTradeValuesWithData,
    ) -> (Option<i64>, bool) {
        (None, true)
    }

    fn is_pre_trade_day_equal_to_trade_day(&self) -> bool {
        false
    }

    fn is_only_trading_on_news(&self) -> bool {
        false
    }
}

impl FromStr for PppBuilder {
    type Err = ChapatyErrorKind;
    fn from_str(s: &str) -> Result<Self, ChapatyErrorKind> {
        match s {
            "PPP" | "Ppp" | "ppp" => Ok(PppBuilder::new()),
            _ => Err(Self::Err::ParseBotError(format!(
                "This strategy <{s}> does not exist"
            ))),
        }
    }
}

impl Strategy for Ppp {
    fn get_required_pre_trade_values(&self) -> RequriedPreTradeValues {
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

    fn get_market_simulation_data_kind(&self) -> MarketSimulationDataKind {
        self.market_simulation_data_kind
    }

    fn check_activation_event(
        &self,
        simulation_event: &SimulationEvent,
    ) -> Option<ActivationEvent> {
        let ots = simulation_event.market_event.last().unwrap().ohlc.open_ts.unwrap();
        let low = simulation_event.market_event.last().unwrap().ohlc.low.unwrap();
        let high = simulation_event.market_event.last().unwrap().ohlc.high.unwrap();
        let pre_trade_values = simulation_event.pre_trade_values;
        match self.entry {
            TradingIndicatorKind::Poc(ph) => {
                let entry_price = pre_trade_values.poc(ph);
                if low <= entry_price && entry_price <= high {
                    Some(ActivationEvent {
                        entry_ts: ots,
                        entry_price: simulation_event
                            .market_event
                            .last()
                            .unwrap()
                            .ohlc
                            .open
                            .unwrap(),
                        stop_loss: self.get_sl_price(simulation_event).unwrap(),
                        take_profit: self.get_tp_price(simulation_event).unwrap(),
                        trade_direction_kind: TradeDirectionKind::Long, // self.get_trade_kind(pre_trade_values),
                        strategy: self,
                    })
                } else {
                    None
                }
            
            },
            TradingIndicatorKind::ValueAreaHigh(ph) => {
                let entry_price = pre_trade_values.value_area_high(ph);
                if low <= entry_price && entry_price <= high {
                    Some(ActivationEvent {
                        entry_ts: ots,
                        entry_price: simulation_event
                            .market_event
                            .last()
                            .unwrap()
                            .ohlc
                            .open
                            .unwrap(),
                        stop_loss: self.get_sl_price(simulation_event).unwrap(),
                        take_profit: self.get_tp_price(simulation_event).unwrap(),
                        trade_direction_kind: TradeDirectionKind::Long, // self.get_trade_kind(pre_trade_values),
                        strategy: self,
                    })
                } else {
                    None
                }
            
            },
            TradingIndicatorKind::ValueAreaLow(ph) => {
                let entry_price = pre_trade_values.value_area_low(ph);
                if low <= entry_price && entry_price <= high {
                    Some(ActivationEvent {
                        entry_ts: ots,
                        entry_price: simulation_event
                            .market_event
                            .last()
                            .unwrap()
                            .ohlc
                            .open
                            .unwrap(),
                        stop_loss: self.get_sl_price(simulation_event).unwrap(),
                        take_profit: self.get_tp_price(simulation_event).unwrap(),
                        trade_direction_kind: TradeDirectionKind::Long, // self.get_trade_kind(pre_trade_values),
                        strategy: self,
                    })
                } else {
                    None
                }
            
            },
        }
    }

    fn check_cancelation_event(
        &self,
        simulation_event: &SimulationEvent,
        trade: &Trade<Active>,
    ) -> Option<CloseEvent> {
        let ohlc = &simulation_event.market_event.last().unwrap().ohlc;
        if ohlc.low <= trade.stop_loss && trade.stop_loss <= ohlc.high {
            Some(CloseEvent {
                exit_ts: ohlc.close_ts.unwrap(),
                exit_price: trade.current_price.unwrap(),
                close_event_kind: TradeCloseKind::StopLoss,
            })
        } else if ohlc.low <= trade.take_profit && trade.take_profit <= ohlc.high {
            Some(CloseEvent {
                exit_ts: ohlc.close_ts.unwrap(),
                exit_price: trade.current_price.unwrap(),
                close_event_kind: TradeCloseKind::TakeProfit,
            })
        } else {
            None
        }
    }

    fn filter_on_economic_news_event(&self) -> Option<HashSet<NaiveDate>> {
        None
    }

    fn get_strategy_kind(&self) -> StrategyKind {
        StrategyKind::Ppp
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
        types::ohlc::OhlcCandle,
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
        pre_trade_data_map.insert(
            PreTradeDataKind::LastTradePrice,
            Some(OhlcCandle::new().with_close(99.0)),
        );

        let mut pre_trade_values = RequiredPreTradeValuesWithData {
            indicator_values: trading_indicators,
            market_values: pre_trade_data_map,
        };
        assert_eq!(
            strategy.get_trade_kind(&pre_trade_values),
            TradeDirectionKind::Short
        );
        pre_trade_values.market_values.insert(
            PreTradeDataKind::LastTradePrice,
            Some(OhlcCandle::new().with_close(101.0)),
        );
        assert_eq!(
            strategy.get_trade_kind(&pre_trade_values),
            TradeDirectionKind::Long
        );

        pre_trade_values.market_values.insert(
            PreTradeDataKind::LastTradePrice,
            Some(OhlcCandle::new().with_close(poc)),
        );
        assert_eq!(
            strategy.get_trade_kind(&pre_trade_values),
            TradeDirectionKind::None
        );
    }
}
