use polars::datatypes::AnyValue;

use crate::{
    converter::timeformat::timestamp_in_milli_to_naive_date_time_tuple,
    enums::{news::NewsKind, trade_and_pre_trade::TradeCloseKind},
    types::ohlc::OhlcCandle,
};

use super::*;

#[derive(Debug, Clone, Copy)]
pub struct NewsRasslerConf {
    news_kind: NewsKind,
    stop_loss: StopLoss,
    take_profit_kind: TakeProfitKind,
    number_candles_to_wait: i32,
    earliest_candle_to_enter: i32,
    market_simulation_data_kind: MarketSimulationDataKind,

    /// The number of loser trades it takes to counterbalance a winner
    loss_to_win_ratio: f64,
}

pub struct NewsRasslerConfBuilder {
    news_kind: Option<NewsKind>,
    stop_loss: Option<StopLoss>,
    take_profit_kind: Option<TakeProfitKind>,
    number_candles_to_wait: Option<i32>,
    earliest_candle_to_enter: Option<i32>,
    market_simulation_data_kind: Option<MarketSimulationDataKind>,

    /// The number of loser trades it takes to counterbalance a winner
    loss_to_win_ratio: Option<f64>,
}

impl NewsRasslerConfBuilder {
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

    pub fn build(self) -> NewsRasslerConf {
        NewsRasslerConf {
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

impl NewsRasslerConf {
    fn compute_offset(&self, news_candle: &OhlcCandle, multiplier: f64) -> f64 {
        let open_px = news_candle.open.unwrap();
        let close_px = news_candle.close.unwrap();

        (open_px - close_px).abs() * (multiplier - 1.0)
    }

    fn compute_sl_offset(&self, news_candle: &OhlcCandle) -> f64 {
        self.compute_offset(news_candle, self.stop_loss.offset)
    }

    fn get_sl_price(&self, ohlc: &OhlcCandle) -> Option<f64> {
        match self.get_trade_kind(ohlc) {
            TradeDirectionKind::Long => Some(self.get_sl_price_long(ohlc)),
            TradeDirectionKind::Short => Some(self.get_sl_price_short(ohlc)),
            TradeDirectionKind::None => None,
        }
    }

    fn get_tp_price(
        &self,
        ohlc: &OhlcCandle,
        sl_price: f64,
        trade_direction: &TradeDirectionKind,
    ) -> Option<f64> {
        match trade_direction {
            TradeDirectionKind::Long => Some(self.get_tp_price_long(ohlc, sl_price)),
            TradeDirectionKind::Short => Some(self.get_tp_price_short(ohlc, sl_price)),
            TradeDirectionKind::None => None,
        }
    }

    fn compute_sl_price(&self, ohlc: &OhlcCandle, is_long_trade: bool) -> f64 {
        let news_candle = ohlc;
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
    fn get_sl_price_long(&self, ohlc: &OhlcCandle) -> f64 {
        self.compute_sl_price(ohlc, true)
    }

    /// Function to compute the stop loss price for short trades
    fn get_sl_price_short(&self, ohlc: &OhlcCandle) -> f64 {
        self.compute_sl_price(ohlc, false)
    }

    fn compute_tp_price(&self, ohlc: &OhlcCandle, is_long_trade: bool, sl_price: f64) -> f64 {
        let entry_price = ohlc.open.unwrap();
        let offset = (sl_price - entry_price).abs() * self.loss_to_win_ratio;
        let sign = if is_long_trade { 1.0 } else { -1.0 };

        match self.take_profit_kind {
            TakeProfitKind::PriceUponTradeEntry => entry_price + sign * offset,
            TakeProfitKind::PrevClose => panic!("No PrevClose available for News Trade!"),
            TakeProfitKind::PrevHighOrLow => panic!("No Value Area available for News Trade!"),
            TakeProfitKind::ValueAreaHighOrLow => panic!("No Value Area available for News Trade!"),
        }
    }

    /// Function to compute the take profit for long trades
    fn get_tp_price_long(&self, ohlc: &OhlcCandle, sl_price: f64) -> f64 {
        self.compute_tp_price(ohlc, true, sl_price)
    }

    /// Function to compute the take profit for short trades
    fn get_tp_price_short(&self, ohlc: &OhlcCandle, sl_price: f64) -> f64 {
        self.compute_tp_price(ohlc, false, sl_price)
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

    // fn get_entry_price(&self, pre_trade_values: &RequiredPreTradeValuesWithData) -> Option<f64> {
    //     let news_candle = match pre_trade_values.news_candle(&self.news_kind, 0) {
    //         Some(x) => x,
    //         None => return None,
    //     };

    //     let mut trade_kind = TradeDirectionKind::None;
    //     let mut entry_candle = self.earliest_candle_to_enter.max(1);
    //     for t in self.earliest_candle_to_enter.max(1)..=self.number_candles_to_wait {
    //         let candle = pre_trade_values
    //             .news_candle(&self.news_kind, t as u32)
    //             .unwrap();
    //         if news_candle.high.le(&candle.high) && news_candle.low.ge(&candle.low) {
    //             // News candle is inside the next candle
    //             return None;
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
    //         return None;
    //     }

    //     pre_trade_values
    //         .news_candle(&self.news_kind, (entry_candle + 1) as u32)
    //         .unwrap()
    //         .open
    // }

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

    fn get_trade_kind(&self, news_candle: &OhlcCandle) -> TradeDirectionKind {
        // Rassler vs. Counter -> anhand is_counter_trade unterscheiden
        news_candle_trade_direction(&news_candle)
        // if let Some(news_candle) = pre_trade_values.news_candle(&self.news_kind, 0) {
        // } else {
        //     TradeDirectionKind::None
        // }
    }

    fn add_all_entry_signals<'a>(
        &'a self,
        market_trajectory: &[Market],
        signals: &mut Vec<ActivationEvent<'a>>,
    ) {
        let latest_ohlc = &market_trajectory.last().unwrap().ohlc;
        let ots = latest_ohlc.open_ts.unwrap();
        let (date, time) = timestamp_in_milli_to_naive_date_time_tuple(ots);
        let news_time = self.news_kind.utc_time_daylight_saving_adjusted(&date);
        let delta = time.signed_duration_since(news_time).num_minutes()
            / self.market_simulation_data_kind.duration_in_minutes();

        if self.news_kind.get_news_dates().contains(&date)
            && 1 <= delta
            && delta < self.number_candles_to_wait as i64
        {
            let n = market_trajectory.len();
            let news_candle = &market_trajectory.get(n - delta as usize - 1).unwrap().ohlc;

            let (_, news_time) =
                timestamp_in_milli_to_naive_date_time_tuple(news_candle.open_ts.unwrap());
            if news_time != self.news_kind.utc_time_daylight_saving_adjusted(&date) {
                // No news candle in data
                return;
            }

            if news_candle.high.le(&latest_ohlc.high) && news_candle.low.ge(&latest_ohlc.low) {
                // News candle is inside the next candle
                return;
            }

            let trade_direction_kind = self.get_trade_kind(news_candle);
            if TradeDirectionKind::None == trade_direction_kind {
                // Doji Candle, skip
                return;
            }

            let second_last_ohlc = &market_trajectory.get(n - 2).unwrap().ohlc;
            if news_candle.high < second_last_ohlc.close || news_candle.low > second_last_ohlc.close
            {
                let stop_loss = self.get_sl_price(&news_candle).unwrap();
                let take_profit = self
                    .get_tp_price(&latest_ohlc, stop_loss, &trade_direction_kind)
                    .unwrap();
                signals.push(ActivationEvent {
                    entry_ts: ots,
                    entry_price: latest_ohlc.open.unwrap(),
                    stop_loss,
                    take_profit,
                    trade_direction_kind, // self.get_trade_kind(pre_trade_values),
                    strategy: self,
                });
            } else {
                return;
            }
        }
    }
}

impl FromStr for NewsRasslerConfBuilder {
    type Err = ChapatyErrorKind;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "NEWS" | "News" | "news" => Ok(NewsRasslerConfBuilder::new()),
            _ => Err(Self::Err::ParseBotError(format!(
                "This strategy <{s}> does not exists"
            ))),
        }
    }
}

impl Strategy for NewsRasslerConf {
    fn get_required_pre_trade_values(&self) -> Option<RequriedPreTradeValues> {
        None
    }

    fn get_market_simulation_data_kind(&self) -> MarketSimulationDataKind {
        self.market_simulation_data_kind
    }

    fn check_activation_event<'a>(
        &'a self,
        market_trajectory: &Box<Vec<Market>>,
        _sim_data: &Box<SimulationData>,
    ) -> Option<ActivationEvent<'a>> {
        let latest_ohlc = &market_trajectory.last().unwrap().ohlc;
        let ots = latest_ohlc.open_ts.unwrap();
        let (date, time) = timestamp_in_milli_to_naive_date_time_tuple(ots);
        let news_time = self.news_kind.utc_time_daylight_saving_adjusted(&date);
        let delta = time.signed_duration_since(news_time).num_minutes()
            / self.market_simulation_data_kind.duration_in_minutes();

        if self.news_kind.get_news_dates().contains(&date)
            && 1 <= delta
            && delta < self.number_candles_to_wait as i64
        {
            let mut signals: Vec<ActivationEvent<'a>> = Vec::new();

            let n = market_trajectory.len();
            for i in 0..delta {
                let earlier_market_trajectory = &market_trajectory[..(n - i as usize)];
                self.add_all_entry_signals(earlier_market_trajectory, &mut signals);
            }

            if signals.len() == 0 || signals.len() > 1 {
                None
            } else {
                if signals[0].entry_ts == ots {
                    // If the last candle is the entry signal then it is an entry, compare 2023-08-10
                    Some(signals[0])
                } else {
                    None
                }
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

    fn filter_on_economic_news_event(&self) -> Option<Vec<NewsKind>> {
        Some(vec![self.news_kind])
    }

    fn get_strategy_kind(&self) -> StrategyKind {
        StrategyKind::NewsRasslerConf
    }

    fn get_name(&self) -> String {
        format!("NewsRasslerConf::{}", self.news_kind.to_string())
    }
}
