use chrono::TimeDelta;
use polars::datatypes::AnyValue;

use crate::{
    converter::timeformat::timestamp_in_milli_to_naive_date_time_tuple,
    enums::{news::NewsKind, trade_and_pre_trade::TradeCloseKind},
    types::ohlc::OhlcCandle,
};

use super::*;

#[derive(Debug, Clone, Copy)]
pub struct NewsRassler {
    news_kind: NewsKind,
    stop_loss: StopLoss,
    take_profit_kind: TakeProfitKind,
    number_candles_to_wait: i32,
    market_simulation_data_kind: MarketSimulationDataKind,

    /// The number of loser trades it takes to counterbalance a winner
    loss_to_win_ratio: f64,
}

pub struct NewsRasslerBuilder {
    news_kind: Option<NewsKind>,
    stop_loss: Option<StopLoss>,
    take_profit_kind: Option<TakeProfitKind>,
    number_candles_to_wait: Option<i32>,
    market_simulation_data_kind: Option<MarketSimulationDataKind>,

    /// The number of loser trades it takes to counterbalance a winner
    loss_to_win_ratio: Option<f64>,
}

impl NewsRasslerBuilder {
    pub fn new() -> Self {
        Self {
            news_kind: None,
            stop_loss: None,
            take_profit_kind: None,
            number_candles_to_wait: None,
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

    pub fn build(self) -> NewsRassler {
        NewsRassler {
            news_kind: self.news_kind.unwrap(),
            stop_loss: self.stop_loss.unwrap(),
            take_profit_kind: self.take_profit_kind.unwrap(),
            number_candles_to_wait: self.number_candles_to_wait.unwrap(),
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

impl NewsRassler {
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
    //     let take_profit = self.get_tp_price(request);

    //     let stop_loss = take_profit.map(|_| self.get_sl_price(request)).flatten();

    //     let is_valid_trade = take_profit.and(stop_loss).is_some();

    //     let entry_price = self
    //         .get_entry_ts(&request.pre_trade_values)
    //         .0
    //         .map_or(0.0, |_| {
    //             self.get_entry_price(&request.pre_trade_values).unwrap()
    //         });

    //     Trade {
    //         entry_price,
    //         stop_loss,
    //         take_profit,
    //         trade_kind: self.get_trade_kind(&request.pre_trade_values),
    //         is_valid: is_valid_trade,
    //     }
    // }

    fn get_entry_price(&self, pre_trade_values: &RequiredPreTradeValuesWithData) -> Option<f64> {
        pre_trade_values
            .news_candle(
                &self.news_kind,
                self.number_candles_to_wait.try_into().unwrap(),
            )
            .unwrap()
            .open
    }

    // fn get_entry_ts(
    //     &self,
    //     pre_trade_values: &RequiredPreTradeValuesWithData,
    // ) -> (Option<i64>, bool) {
    //     (
    //         pre_trade_values
    //             .news_candle(
    //                 &self.news_kind,
    //                 self.number_candles_to_wait.try_into().unwrap(),
    //             )
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
}

impl FromStr for NewsRasslerBuilder {
    type Err = ChapatyErrorKind;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "NEWS" | "News" | "news" => Ok(NewsRasslerBuilder::new()),
            _ => Err(Self::Err::ParseBotError(format!(
                "This strategy <{s}> does not exists"
            ))),
        }
    }
}

impl Strategy for NewsRassler {
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
        let entry_time = news_time
            .overflowing_add_signed(
                TimeDelta::try_minutes(
                    self.number_candles_to_wait as i64
                        * self.market_simulation_data_kind.duration_in_minutes(),
                )
                .unwrap(),
            )
            .0;

        if self.news_kind.get_news_dates().contains(&date) && time == entry_time {
            let n = market_trajectory.len();
            let news_candle = &market_trajectory
                .get(n - self.number_candles_to_wait as usize - 1)
                .unwrap()
                .ohlc;
            if TradeDirectionKind::None == self.get_trade_kind(news_candle) {
                // Doji Candle, skip
                return None;
            }
            let trade_direction_kind = self.get_trade_kind(news_candle);
            let stop_loss = self.get_sl_price(&news_candle).unwrap();
            let take_profit = self
                .get_tp_price(&latest_ohlc, stop_loss, &trade_direction_kind)
                .unwrap();
            Some(ActivationEvent {
                entry_ts: ots,
                entry_price: latest_ohlc.open.unwrap(),
                stop_loss,
                take_profit,
                trade_direction_kind,
                strategy: self,
            })
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
        StrategyKind::NewsRassler
    }

    fn get_name(&self) -> String {
        format!("NewsRassler::{}", self.news_kind.to_string())
    }
}
