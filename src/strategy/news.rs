use crate::enums::news::NewsKind;

use super::*;

pub struct News {
    news_kind: NewsKind,
    stop_loss: StopLoss,
    take_profit: TakeProfit,
    number_candles_to_wait: i32,
    is_counter_trade: bool,
}

pub struct NewsBuilder {
    news_kind: Option<NewsKind>,
    stop_loss: Option<StopLoss>,
    take_profit: Option<TakeProfit>,
    number_candles_to_wait: Option<i32>,
    is_counter_trade: Option<bool>,
}

impl NewsBuilder {
    pub fn new() -> Self {
        Self {
            news_kind: None,
            stop_loss: None,
            take_profit: None,
            number_candles_to_wait: None,
            is_counter_trade: None,
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

    pub fn with_is_counter_trade(self, is_counter_trade: bool) -> Self {
        Self {
            is_counter_trade: Some(is_counter_trade),
            ..self
        }
    }

    pub fn build(self) -> News {
        News {
            news_kind: self.news_kind.unwrap(),
            stop_loss: self.stop_loss.unwrap(),
            take_profit: self.take_profit.unwrap(),
            number_candles_to_wait: self.number_candles_to_wait.unwrap(),
            is_counter_trade: self.is_counter_trade.unwrap(),
        }
    }
}

impl FromStr for NewsBuilder {
    type Err = ChapatyErrorKind;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "NEWS" | "News" | "news" => Ok(NewsBuilder::new()),
            _ => Err(Self::Err::ParseBotError(format!(
                "This strategy <{s}> does not exists"
            ))),
        }
    }
}

impl Strategy for News {
    fn get_trade(&self, request: &TradeRequestObject) -> Trade {
        Trade {
            entry_price: self.get_entry_price(&request.pre_trade_values),
            stop_loss: None, //self.get_sl_price(request),
            take_profit: None, //self.get_tp_price(request),
            trade_kind: self.get_trade_kind(&request.pre_trade_values),
        }
    }

    fn get_required_pre_trade_values(&self) -> RequriedPreTradeValues {
        let market_values = (0..=self.number_candles_to_wait).into_iter().fold(Vec::new(), |mut acc, n| {
            acc.push(PreTradeDataKind::News(self.news_kind, self.number_candles_to_wait.try_into().unwrap()));
            acc
        });
        RequriedPreTradeValues {
            market_values,
            trading_indicators: Vec::new(),
        }
    }

    fn get_entry_price(&self, pre_trade_values: &RequiredPreTradeValuesWithData) -> f64 {
        pre_trade_values
            .news_candle(&self.news_kind, self.number_candles_to_wait.try_into().unwrap())
            .unwrap()
            .open
            .unwrap()
    }

    fn get_trade_kind(&self,pre_trade_values: &RequiredPreTradeValuesWithData) -> TradeDirectionKind {
        TradeDirectionKind::None
    }

    fn get_name(&self) -> String {
        self.news_kind.to_string()
    }

    fn is_pre_trade_day_equal_to_trade_day(&self) -> bool {
        true
    }

    fn is_trading_on_news(&self) -> bool {
        true
    }

    fn get_news(&self) -> HashSet<NaiveDate> {
        self.news_kind.get_news_dates()
    }


}
