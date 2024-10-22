use chrono::TimeDelta;
use polars::datatypes::AnyValue;

use crate::{
    converter::timeformat::timestamp_in_milli_to_naive_date_time_tuple,
    enums::{news::NewsKind, trade_and_pre_trade::TradeCloseKind},
    types::ohlc::OhlcCandle,
};

use super::*;

#[derive(Debug, Clone, Copy)]
pub struct NewsCounter {
    news_kind: NewsKind,
    stop_loss_kind: StopLossKind,
    take_profit: TakeProfit,
    number_candles_to_wait: i32,
    market_simulation_data_kind: MarketSimulationDataKind,

    /// The number of loser trades it takes to counterbalance a winner
    loss_to_win_ratio: f64,
}

pub struct NewsCounterBuilder {
    news_kind: Option<NewsKind>,
    stop_loss_kind: Option<StopLossKind>,
    take_profit: Option<TakeProfit>,
    number_candles_to_wait: Option<i32>,
    market_simulation_data_kind: Option<MarketSimulationDataKind>,

    /// The number of loser trades it takes to counterbalance a winner
    loss_to_win_ratio: Option<f64>,
}

impl NewsCounterBuilder {
    pub fn new() -> Self {
        Self {
            news_kind: None,
            stop_loss_kind: None,
            take_profit: None,
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

    pub fn with_stop_loss_kind(self, stop_loss_kind: StopLossKind) -> Self {
        Self {
            stop_loss_kind: Some(stop_loss_kind),
            ..self
        }
    }

    pub fn with_take_profit(self, take_profit: TakeProfit) -> Self {
        Self {
            take_profit: Some(take_profit),
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

    pub fn build(self) -> NewsCounter {
        NewsCounter {
            news_kind: self.news_kind.unwrap(),
            stop_loss_kind: self.stop_loss_kind.unwrap(),
            take_profit: self.take_profit.unwrap(),
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

impl NewsCounter {
    fn is_no_entry(
        &self,
        entry_price: f64,
        take_profit: f64,
        trade_direction: &TradeDirectionKind,
    ) -> bool {
        match trade_direction {
            TradeDirectionKind::Long => entry_price > take_profit,
            TradeDirectionKind::Short => entry_price < take_profit,
            TradeDirectionKind::None => true,
        }
    }

    fn compute_offset(&self, news_candle: &OhlcCandle, multiplier: f64) -> f64 {
        let open_px = news_candle.open.unwrap();
        let close_px = news_candle.close.unwrap();

        (open_px - close_px).abs() * (multiplier - 1.0)
    }

    fn compute_tp_offset(&self, news_candle: &OhlcCandle) -> f64 {
        self.compute_offset(news_candle, self.take_profit.offset)
    }

    fn get_sl_price(
        &self,
        ohlc: &OhlcCandle,
        tp_price: f64,
        trade_direction: &TradeDirectionKind,
    ) -> Option<f64> {
        match trade_direction {
            TradeDirectionKind::Long => Some(self.get_sl_price_long(ohlc, tp_price)),
            TradeDirectionKind::Short => Some(self.get_sl_price_short(ohlc, tp_price)),
            TradeDirectionKind::None => None,
        }
    }

    fn get_tp_price(&self, ohlc: &OhlcCandle) -> Option<f64> {
        let trade_direction = self.get_trade_kind(ohlc);
        let some_take_profit = match trade_direction {
            TradeDirectionKind::Long => Some(self.get_tp_price_long(ohlc)),
            TradeDirectionKind::Short => Some(self.get_tp_price_short(ohlc)),
            TradeDirectionKind::None => None,
        };

        some_take_profit

        // some_take_profit.and_then(|take_profit| {
        //     let entry_px = self.get_entry_price(pre_trade_values).unwrap();
        //     if self.is_no_entry(entry_px, take_profit, &trade_direction) {
        //         None
        //     } else {
        //         Some(take_profit)
        //     }
        // })
    }

    fn compute_sl_price(&self, ohlc: &OhlcCandle, is_long_trade: bool, tp_price: f64) -> f64 {
        let entry_price = ohlc.open.unwrap();
        let offset = (tp_price - entry_price).abs() * (1.0 / self.loss_to_win_ratio);
        let sign = if is_long_trade { -1.0 } else { 1.0 };

        match self.stop_loss_kind {
            StopLossKind::PriceUponTradeEntry => entry_price + sign * offset,
            StopLossKind::PrevHighOrLow => panic!("No PrevHighOrLow available for News Trade!"),
            StopLossKind::ValueAreaHighOrLow => panic!("No Value Area available for News Trade!"),
        }
    }

    /// Function to compute the stop loss price for long trades
    fn get_sl_price_long(&self, ohlc: &OhlcCandle, tp_price: f64) -> f64 {
        self.compute_sl_price(ohlc, true, tp_price)
    }

    /// Function to compute the stop loss price for short trades
    fn get_sl_price_short(&self, ohlc: &OhlcCandle, tp_price: f64) -> f64 {
        self.compute_sl_price(ohlc, false, tp_price)
    }

    fn compute_tp_price(&self, ohlc: &OhlcCandle, is_long_trade: bool) -> f64 {
        let open = ohlc.open.unwrap();
        let offset = self.compute_tp_offset(ohlc);
        let sign = if is_long_trade { 1.0 } else { -1.0 };

        match self.take_profit.kind {
            TakeProfitKind::PriceUponTradeEntry => open + sign * offset,
            TakeProfitKind::PrevClose => panic!("No PrevClose available for News Trade!"),
            TakeProfitKind::PrevHighOrLow => panic!("No Value Area available for News Trade!"),
            TakeProfitKind::ValueAreaHighOrLow => panic!("No Value Area available for News Trade!"),
        }
    }

    /// Function to compute the take profit for long trades
    fn get_tp_price_long(&self, ohlc: &OhlcCandle) -> f64 {
        self.compute_tp_price(ohlc, true)
    }

    /// Function to compute the take profit for short trades
    fn get_tp_price_short(&self, ohlc: &OhlcCandle) -> f64 {
        self.compute_tp_price(ohlc, false)
    }

    // fn get_trade(&self, request: &TradeRequestObject) -> Trade {
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

    // fn get_entry_price(&self, pre_trade_values: &OhlcCandle) -> Option<f64> {
    //     pre_trade_values
    //         .news_candle(
    //             &self.news_kind,
    //             self.number_candles_to_wait.try_into().unwrap(),
    //         )
    //         .unwrap()
    //         .open
    // }

    fn get_trade_kind(&self, news_candle: &OhlcCandle) -> TradeDirectionKind {
        match news_candle_trade_direction(news_candle) {
            TradeDirectionKind::Long => TradeDirectionKind::Short,
            TradeDirectionKind::Short => TradeDirectionKind::Long,
            TradeDirectionKind::None => TradeDirectionKind::None,
        }
    }
}

impl FromStr for NewsCounterBuilder {
    type Err = ChapatyErrorKind;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "NEWS" | "News" | "news" => Ok(NewsCounterBuilder::new()),
            _ => Err(Self::Err::ParseBotError(format!(
                "This strategy <{s}> does not exists"
            ))),
        }
    }
}

impl Strategy for NewsCounter {
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
        let ohlc = market_trajectory.last().as_ref().unwrap().ohlc;
        let ots = ohlc.open_ts.unwrap();
        let (date, time) = timestamp_in_milli_to_naive_date_time_tuple(ots);
        let entry_time = self
            .news_kind
            .utc_time_daylight_saving_adjusted(&date)
            .overflowing_add_signed(
                TimeDelta::try_minutes(self.number_candles_to_wait as i64).unwrap(),
            )
            .0;

        if self.news_kind.get_news_dates().contains(&date) && time == entry_time {
            let news_candle = market_trajectory
                .get(market_trajectory.len() - self.number_candles_to_wait as usize - 1)
                .unwrap()
                .ohlc;
            let (_, news_time) =
                timestamp_in_milli_to_naive_date_time_tuple(news_candle.open_ts.unwrap());
            if news_time != self.news_kind.utc_time_daylight_saving_adjusted(&date) {
                // No news candle in data
                return None;
            }
            let take_profit = self.get_tp_price(&news_candle).unwrap();
            let entry_price = ohlc.open.unwrap();
            let trade_direction_kind = self.get_trade_kind(&news_candle);
            if self.is_no_entry(entry_price, take_profit, &trade_direction_kind) {
                return None;
            }
            let stop_loss = self
                .get_sl_price(&ohlc, take_profit, &trade_direction_kind)
                .unwrap();
            Some(ActivationEvent {
                entry_ts: ots,
                entry_price,
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
        StrategyKind::NewsCounter
    }

    fn get_name(&self) -> String {
        format!("NewsCounter::{}", self.news_kind.to_string())
    }
}
