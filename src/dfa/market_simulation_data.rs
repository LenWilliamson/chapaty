use polars::{error::PolarsError, frame::DataFrame};

use crate::{
    calculator::pre_trade_values_calculator::RequiredPreTradeValuesWithData,
    trading_indicator::initial_balance::InitialBalance, types::ohlc::OhlcCandle,
    DataProviderColumnKind, MarketKind, MarketSimulationDataKind,
};

pub struct SimulationData {
    pub market: Vec<Market>,
    pub pre_trade_values: RequiredPreTradeValuesWithData,
    pub market_kind: MarketKind,
    pub market_sim_data_kind: MarketSimulationDataKind,
}
pub struct SimulationEvent<'a> {
    pub market_event: Vec<&'a Market>,
    pub initial_balance: Option<&'a InitialBalance>,
    pub pre_trade_values: &'a RequiredPreTradeValuesWithData,
    pub market_kind: MarketKind,
    pub market_sim_data_kind: MarketSimulationDataKind,
}

impl<'a> SimulationEvent<'a> {
    pub fn new(market_event: Vec<&'a Market>, sim_data: &'a SimulationData) -> Self {
        SimulationEvent {
            market_event,
            initial_balance: None,
            pre_trade_values: &sim_data.pre_trade_values,
            market_kind: sim_data.market_kind,
            market_sim_data_kind: sim_data.market_sim_data_kind,
        }
    }

    pub fn update_on_market_event(&mut self, market_event: &'a Market) {
        self.market_event.push(market_event);
    }
}

// TODO more values, extend with possible tick and ohlcv values or sma50, rsi14, etc?
pub struct Market {
    pub ohlc: OhlcCandle,
}

pub struct SimulationDataBuilder {
    pub market: Option<DataFrame>,
    pub pre_trade_values: Option<RequiredPreTradeValuesWithData>,
    pub market_kind: Option<MarketKind>,
    pub market_sim_data_kind: Option<MarketSimulationDataKind>,
}

impl SimulationDataBuilder {
    pub fn new() -> Self {
        Self {
            market: None,
            pre_trade_values: None,
            market_kind: None,
            market_sim_data_kind: None,
        }
    }

    pub fn with_ohlc_candle(self, market: DataFrame) -> Self {
        Self {
            market: Some(market),
            ..self
        }
    }

    pub fn with_pre_trade_values_with_data(self, data: RequiredPreTradeValuesWithData) -> Self {
        Self {
            pre_trade_values: Some(data),
            ..self
        }
    }

    pub fn with_market_kind(self, market_kind: MarketKind) -> Self {
        Self {
            market_kind: Some(market_kind),
            ..self
        }
    }

    pub fn with_market_sim_data_kind(self, kind: MarketSimulationDataKind) -> Self {
        Self {
            market_sim_data_kind: Some(kind),
            ..self
        }
    }

    pub fn build(self) -> Result<SimulationData, PolarsError> {
        Ok(SimulationData {
            market: MarketDataFrame(self.market.unwrap()).try_into()?,
            pre_trade_values: self.pre_trade_values.unwrap(),
            market_kind: self.market_kind.unwrap(),
            market_sim_data_kind: self.market_sim_data_kind.unwrap(),
        })
    }
}

pub struct MarketDataFrame(DataFrame);

impl TryFrom<MarketDataFrame> for Vec<Market> {
    type Error = PolarsError;

    fn try_from(value: MarketDataFrame) -> Result<Self, Self::Error> {
        let ots = value
            .0
            .column(&DataProviderColumnKind::OpenTime.to_string())?
            .i64()?;
        let open = value
            .0
            .column(&DataProviderColumnKind::Open.to_string())?
            .f64()?;
        let high = value
            .0
            .column(&DataProviderColumnKind::High.to_string())?
            .f64()?;
        let low = value
            .0
            .column(&DataProviderColumnKind::Low.to_string())?
            .f64()?;
        let close = value
            .0
            .column(&DataProviderColumnKind::Close.to_string())?
            .f64()?;
        let cts = value
            .0
            .column(&DataProviderColumnKind::CloseTime.to_string())?
            .i64()?;

        let mut market_sim_data: Vec<Market> = Vec::with_capacity(value.0.height());

        for i in 0..value.0.height() {
            let data = Market {
                ohlc: OhlcCandle {
                    open_ts: ots.get(i),
                    open: open.get(i),
                    high: high.get(i),
                    low: low.get(i),
                    close: close.get(i),
                    close_ts: cts.get(i),
                    is_end_of_day: Some(i == value.0.height() - 1),
                },
            };

            market_sim_data.push(data);
        }

        Ok(market_sim_data)
    }
}
