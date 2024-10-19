use polars::datatypes::AnyValue;

use crate::{
    converter::timeformat::timestamp_in_milli_to_naive_date_time_tuple,
    enums::{news::NewsKind, trade_and_pre_trade::TradeCloseKind},
    types::ohlc::OhlcCandle,
};

use super::*;

#[derive(Debug, Clone, Copy)]
pub struct NewsRasslerWithConfirmation {
    news_kind: NewsKind,
    stop_loss: StopLoss,
    take_profit_kind: TakeProfitKind,
    number_candles_to_wait: i32,
    earliest_candle_to_enter: i32,
    market_simulation_data_kind: MarketSimulationDataKind,

    /// The number of loser trades it takes to counterbalance a winner
    loss_to_win_ratio: f64,
}

pub struct NewsRasslerWithConfirmationBuilder {
    news_kind: Option<NewsKind>,
    stop_loss: Option<StopLoss>,
    take_profit_kind: Option<TakeProfitKind>,
    number_candles_to_wait: Option<i32>,
    earliest_candle_to_enter: Option<i32>,
    market_simulation_data_kind: Option<MarketSimulationDataKind>,

    /// The number of loser trades it takes to counterbalance a winner
    loss_to_win_ratio: Option<f64>,
}

impl NewsRasslerWithConfirmationBuilder {
    pub fn new() -> Self {
        Self {
            news_kind: None,
            stop_loss: None,
            take_profit_kind: None,
            number_candles_to_wait: None,
            earliest_candle_to_enter: None,
            loss_to_win_ratio: None,
            market_simulation_data_kind: None,
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

    pub fn with_news_kind(self, news_kind: NewsKind) -> Self {
        Self {
            news_kind: Some(news_kind),
            ..self
        }
    }

    pub fn with_stop_loss(self, stop_loss: StopLoss) -> Self {
        Self {
            stop_loss: Some(stop_loss),
            ..self
        }
    }

    pub fn with_take_profit_kind(self, take_profit_kind: TakeProfitKind) -> Self {
        Self {
            take_profit_kind: Some(take_profit_kind),
            ..self
        }
    }

    pub fn with_number_candles_to_wait(self, n: i32) -> Self {
        Self {
            number_candles_to_wait: Some(n),
            ..self
        }
    }

    pub fn with_earliest_candle_to_enter(self, n: i32) -> Self {
        Self {
            earliest_candle_to_enter: Some(n),
            ..self
        }
    }

    /// The number of loser trades it takes to counterbalance a winner
    pub fn with_loss_to_win_ratio(self, loss_to_win_ratio: f64) -> Self {
        if loss_to_win_ratio == 0.0 {
            panic!("loss_to_win_ratio needs to be greater 0")
        }
        Self {
            loss_to_win_ratio: Some(loss_to_win_ratio.abs()),
            ..self
        }
    }

    pub fn build(self) -> NewsRasslerWithConfirmation {
        NewsRasslerWithConfirmation {
            news_kind: self.news_kind.unwrap(),
            stop_loss: self.stop_loss.unwrap(),
            take_profit_kind: self.take_profit_kind.unwrap(),
            number_candles_to_wait: self.number_candles_to_wait.unwrap(),
            earliest_candle_to_enter: self.earliest_candle_to_enter.unwrap(),
            loss_to_win_ratio: self.loss_to_win_ratio.unwrap(),
            market_simulation_data_kind: self.market_simulation_data_kind.unwrap(),
        }
    }
}

fn news_candle_trade_direction(news_candle: &OhlcCandle) -> TradeDirectionKind {
    let open_price = news_candle.open.unwrap();
    let close_price = news_candle.close.unwrap();
    match AnyValue::Float64(open_price - close_price) {
        AnyValue::Float64(x) if x == 0.0 => TradeDirectionKind::None,
        AnyValue::Float64(x) if x > 0.0 => TradeDirectionKind::Short,
        AnyValue::Float64(x) if x < 0.0 => TradeDirectionKind::Long,
        _ => panic!("Matching against wrong value"),
    }
}

impl NewsRasslerWithConfirmation {
    fn compute_offset(&self, news_candle: &OhlcCandle, multiplier: f64) -> f64 {
        let open_px = news_candle.open.unwrap();
        let close_px = news_candle.close.unwrap();

        (open_px - close_px).abs() * (multiplier - 1.0)
    }

    fn compute_sl_offset(&self, news_candle: &OhlcCandle) -> f64 {
        self.compute_offset(news_candle, self.stop_loss.offset)
    }

    fn get_sl_price(&self, request: &SimulationData) -> Option<f64> {
        match self.get_trade_kind(&request.pre_trade_values) {
            TradeDirectionKind::Long => Some(self.get_sl_price_long(request)),
            TradeDirectionKind::Short => Some(self.get_sl_price_short(request)),
            TradeDirectionKind::None => None,
        }
    }

    fn get_tp_price(&self, request: &SimulationData) -> Option<f64> {
        let pre_trade_values = &request.pre_trade_values;
        let trade_direction = self.get_trade_kind(pre_trade_values);
        match trade_direction {
            TradeDirectionKind::Long => Some(self.get_tp_price_long(request)),
            TradeDirectionKind::Short => Some(self.get_tp_price_short(request)),
            TradeDirectionKind::None => None,
        }
    }

    fn compute_sl_price(&self, request: &SimulationData, is_long_trade: bool) -> f64 {
        let pre_trade_values = &request.pre_trade_values;
        let news_candle = pre_trade_values.news_candle(&self.news_kind, 0).unwrap();
        let open = news_candle.open.unwrap();
        let offset = self.compute_sl_offset(news_candle);
        let sign = if is_long_trade { -1.0 } else { 1.0 };

        match self.stop_loss.kind {
            StopLossKind::PriceUponTradeEntry => open + sign * offset,
            StopLossKind::PrevHighOrLow => panic!("No PrevHighOrLow available for News Trade!"),
            StopLossKind::ValueAreaHighOrLow => panic!("No Value Area available for News Trade!"),
        }
    }

    /// Function to compute the stop loss price for long trades
    fn get_sl_price_long(&self, request: &SimulationData) -> f64 {
        self.compute_sl_price(request, true)
    }

    /// Function to compute the stop loss price for short trades
    fn get_sl_price_short(&self, request: &SimulationData) -> f64 {
        self.compute_sl_price(request, false)
    }

    fn compute_tp_price(&self, request: &SimulationData, is_long_trade: bool) -> f64 {
        let pre_trade_values = &request.pre_trade_values;
        let entry_price = self.get_entry_price(pre_trade_values).unwrap();
        let offset = (self.compute_sl_price(request, is_long_trade) - entry_price).abs()
            * self.loss_to_win_ratio;
        let sign = if is_long_trade { 1.0 } else { -1.0 };

        match self.take_profit_kind {
            TakeProfitKind::PriceUponTradeEntry => entry_price + sign * offset,
            TakeProfitKind::PrevClose => panic!("No PrevClose available for News Trade!"),
            TakeProfitKind::PrevHighOrLow => panic!("No Value Area available for News Trade!"),
            TakeProfitKind::ValueAreaHighOrLow => panic!("No Value Area available for News Trade!"),
        }
    }

    /// Function to compute the take profit for long trades
    fn get_tp_price_long(&self, request: &SimulationData) -> f64 {
        self.compute_tp_price(request, true)
    }

    /// Function to compute the take profit for short trades
    fn get_tp_price_short(&self, request: &SimulationData) -> f64 {
        self.compute_tp_price(request, false)
    }

    // fn get_trade(&self, request: &SimulationData) -> Trade {
    //     let entry_price = self.get_entry_price(&request.pre_trade_values);
    //     if entry_price.is_none() {
    //         return Trade {
    //             entry_price: 0.0,
    //             stop_loss: None,
    //             take_profit: None,
    //             trade_kind: TradeDirectionKind::None,
    //             is_valid: false,
    //         };
    //     }

    //     let take_profit = self.get_tp_price(request);

    //     let stop_loss = take_profit.map(|_| self.get_sl_price(request)).flatten();

    //     let is_valid_trade = take_profit.and(stop_loss).is_some();

    //     let entry_price = self
    //         .get_entry_ts(&request.pre_trade_values)
    //         .0
    //         .map_or(0.0, |_| entry_price.unwrap());

    //     Trade {
    //         entry_price,
    //         stop_loss,
    //         take_profit,
    //         trade_kind: self.get_trade_kind(&request.pre_trade_values),
    //         is_valid: is_valid_trade,
    //     }
    // }

    fn get_entry_price(&self, pre_trade_values: &RequiredPreTradeValuesWithData) -> Option<f64> {
        let news_candle = match pre_trade_values.news_candle(&self.news_kind, 0) {
            Some(x) => x,
            None => return None,
        };

        let mut trade_kind = TradeDirectionKind::None;
        let mut entry_candle = self.earliest_candle_to_enter.max(1);
        for t in self.earliest_candle_to_enter.max(1)..=self.number_candles_to_wait {
            let candle = pre_trade_values
                .news_candle(&self.news_kind, t as u32)
                .unwrap();
            if news_candle.high.le(&candle.high) && news_candle.low.ge(&candle.low) {
                // News candle is inside the next candle
                return None;
            }

            if news_candle.high < candle.close {
                trade_kind = TradeDirectionKind::Long;
                entry_candle = t;
                break;
            }

            if news_candle.low > candle.close {
                trade_kind = TradeDirectionKind::Short;
                entry_candle = t;
                break;
            }
        }
        if entry_candle >= self.number_candles_to_wait || trade_kind == TradeDirectionKind::None {
            return None;
        }

        pre_trade_values
            .news_candle(&self.news_kind, (entry_candle + 1) as u32)
            .unwrap()
            .open
    }

    // fn get_entry_ts(
    //     &self,
    //     pre_trade_values: &RequiredPreTradeValuesWithData,
    // ) -> (Option<i64>, bool) {
    //     let news_candle = match pre_trade_values.news_candle(&self.news_kind, 0) {
    //         Some(x) => x,
    //         None => return (None, false),
    //     };

    //     let mut trade_kind = TradeDirectionKind::None;
    //     let mut entry_candle = 0;
    //     for t in 1..=self.number_candles_to_wait {
    //         let candle = pre_trade_values
    //             .news_candle(&self.news_kind, t as u32)
    //             .unwrap();
    //         if news_candle.high.le(&candle.high) && news_candle.low.ge(&candle.low) {
    //             // News candle is inside the next candle
    //             return (None, false);
    //         }

    //         if news_candle.high < candle.close {
    //             trade_kind = TradeDirectionKind::Long;
    //             entry_candle = t;
    //             break;
    //         }

    //         if news_candle.low > candle.close {
    //             trade_kind = TradeDirectionKind::Short;
    //             entry_candle = t;
    //             break;
    //         }
    //     }
    //     if entry_candle >= self.number_candles_to_wait || trade_kind == TradeDirectionKind::None {
    //         return (None, false);
    //     }

    //     (
    //         pre_trade_values
    //             .news_candle(&self.news_kind, (entry_candle + 1) as u32)
    //             .and_then(|ohlc_candle| ohlc_candle.open_ts),
    //         false,
    //     )
    // }

    fn get_trade_kind(
        &self,
        pre_trade_values: &RequiredPreTradeValuesWithData,
    ) -> TradeDirectionKind {
        // Rassler vs. Counter -> anhand is_counter_trade unterscheiden
        if let Some(news_candle) = pre_trade_values.news_candle(&self.news_kind, 0) {
            news_candle_trade_direction(&news_candle)
        } else {
            TradeDirectionKind::None
        }
    }
}

impl FromStr for NewsRasslerWithConfirmationBuilder {
    type Err = ChapatyErrorKind;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "NEWS" | "News" | "news" => Ok(NewsRasslerWithConfirmationBuilder::new()),
            _ => Err(Self::Err::ParseBotError(format!(
                "This strategy <{s}> does not exists"
            ))),
        }
    }
}

impl Strategy for NewsRasslerWithConfirmation {
    fn get_required_pre_trade_values(&self) -> RequriedPreTradeValues {
        let market_values =
            (0..=self.number_candles_to_wait)
                .into_iter()
                .fold(Vec::new(), |mut acc, n| {
                    acc.push(PreTradeDataKind::News(self.news_kind, n as u32));
                    acc
                });
        RequriedPreTradeValues {
            market_values,
            trading_indicators: Vec::new(),
        }
    }

    fn get_market_simulation_data_kind(&self) -> MarketSimulationDataKind {
        self.market_simulation_data_kind
    }

    fn check_activation_event<'a>(
        &'a self,
        market_trajectory: &Box<Vec<Market>>,
        sim_data: &Box<SimulationData>,
    ) -> Option<ActivationEvent<'a>> {
        let last_ohlc = &market_trajectory.last().unwrap().ohlc;
        let ots = last_ohlc.open_ts.unwrap();
        let (date, time) = timestamp_in_milli_to_naive_date_time_tuple(ots);
        let news_time = self.news_kind.utc_time_daylight_saving_adjusted(&date);
        let delta = time.signed_duration_since(news_time).num_minutes();

        if self.news_kind.get_news_dates().contains(&date)
            && 1 <= delta
            && delta <= self.number_candles_to_wait as i64
        {
            let n = market_trajectory.len();
            let news_candle = &market_trajectory.get(n - delta as usize).unwrap().ohlc;
            if news_candle.high.le(&last_ohlc.high) && news_candle.low.ge(&last_ohlc.low) {
                // News candle is inside the next candle
                return None;
            }

            let second_last_ohlc = &market_trajectory.get(n - 2).unwrap().ohlc;
            if news_candle.high < second_last_ohlc.close {
                Some(ActivationEvent {
                    entry_ts: ots,
                    entry_price: last_ohlc.open.unwrap(),
                    stop_loss: self.get_sl_price(sim_data).unwrap(),
                    take_profit: self.get_tp_price(sim_data).unwrap(),
                    trade_direction_kind: TradeDirectionKind::Long, // self.get_trade_kind(pre_trade_values),
                    strategy: self,
                })
            } else if news_candle.low > second_last_ohlc.close {
                Some(ActivationEvent {
                    entry_ts: ots,
                    entry_price: last_ohlc.open.unwrap(),
                    stop_loss: self.get_sl_price(sim_data).unwrap(),
                    take_profit: self.get_tp_price(sim_data).unwrap(),
                    trade_direction_kind: TradeDirectionKind::Short, // self.get_trade_kind(pre_trade_values),
                    strategy: self,
                })
            } else {
                None
            }
        } else {
            None
        }
    }

    fn check_cancelation_event(
        &self,
        market_trajectory: &Box<Vec<Market>>,
        _sim_data: &Box<SimulationData>,
        trade: &Trade<Active>,
    ) -> Option<CloseEvent> {
        let ohlc = &market_trajectory.last().unwrap().ohlc;
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
        Some(self.news_kind.get_news_dates())
    }

    fn get_strategy_kind(&self) -> StrategyKind {
        StrategyKind::NewsRasslerWithConfirmation
    }

    fn get_name(&self) -> String {
        format!(
            "NewsRasslerWithConfirmation::{}",
            self.news_kind.to_string()
        )
    }
}
