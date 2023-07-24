use std::collections::HashMap;

use polars::prelude::DataFrame;

use crate::enums::indicator::TradingIndicatorKind;



#[derive(Clone)]
pub struct PreTradeData {
    pub market_sim_data: DataFrame,
    pub indicators: HashMap<TradingIndicatorKind, DataFrame>,
}

pub struct PreTradeDataBuilder {
    market_sim_data: Option<DataFrame>,
    indicators: Option<HashMap<TradingIndicatorKind, DataFrame>>,
}

impl PreTradeDataBuilder {
    pub fn new() -> Self {
        Self {
            market_sim_data: None,
            indicators: None,
        }
    }

    pub fn with_market_simd_data(self, market_sim_data: DataFrame) -> Self {
        Self {
            market_sim_data: Some(market_sim_data),
            ..self
        }
    }

    pub fn with_indicators(self, indicators: HashMap<TradingIndicatorKind, DataFrame>) -> Self {
        Self {
            indicators: Some(indicators),
            ..self
        }
    }

    pub fn build(self) -> PreTradeData {
        PreTradeData {
            market_sim_data: self.market_sim_data.unwrap(),
            indicators: self.indicators.unwrap(),
        }
    }
}
