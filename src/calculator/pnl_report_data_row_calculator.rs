use super::{
    pre_trade_values_calculator::{
        PreTradeValuesCalculatorBuilder, RequiredPreTradeValuesWithData,
    },
    trade_pnl_calculator::{TradePnL, TradePnLCalculatorBuilder},
    trade_values_calculator::{TradeValuesCalculatorBuilder, TradeValuesWithData},
};
use crate::{
    bot::{pre_trade_data::PreTradeData, time_frame_snapshot::TimeFrameSnapshot, trade::Trade},
    data_provider::DataProvider,
    enums::markets::MarketKind,
    lazy_frame_operations::trait_extensions::MyLazyFrameOperations,
    strategy::{Strategy, TradeRequestObject},
    MarketSimulationDataKind,
};
use polars::prelude::{DataFrame, IntoLazy, LazyFrame};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct PnLReportDataRow {
    pub market: MarketKind,
    pub year: u32,
    pub strategy_name: String,
    pub time_frame_snapshot: TimeFrameSnapshot,
    pub trade: Trade,
    pub trade_pnl: Option<TradePnL>,
}

pub struct PnLReportDataRowCalculator {
    pub data_provider: Arc<dyn DataProvider>,
    pub strategy: Arc<dyn Strategy>,
    pub market_sim_data: DataFrame,
    pub pre_trade_data: PreTradeData,
    pub market: MarketKind,
    pub year: u32,
    pub time_frame_snapshot: TimeFrameSnapshot,
    pub market_sim_data_kind: MarketSimulationDataKind,
}

#[derive(Clone)]
pub struct TradeAndPreTradeValuesWithData {
    pub trade: Option<TradeValuesWithData>,
    pub pre_trade: RequiredPreTradeValuesWithData,
}

impl PnLReportDataRowCalculator {
    pub fn compute(&self) -> PnLReportDataRow {
        let data = self.get_trade_and_pre_trade_values_with_data();
        match data.trade {
            Some(_) => self.handle_trade(data),
            None => self.handle_no_entry(data),
        }
    }

    fn handle_no_entry(&self, values: TradeAndPreTradeValuesWithData) -> PnLReportDataRow {
        let request = self.trade_object_request(&values);
        PnLReportDataRow {
            market: self.market.clone(),
            year: self.year,
            strategy_name: self.strategy.get_name(),
            time_frame_snapshot: self.time_frame_snapshot,
            trade: self.strategy.get_trade(&request),
            trade_pnl: None,
        }
    }

    fn handle_trade(&self, values: TradeAndPreTradeValuesWithData) -> PnLReportDataRow {
        let request = self.trade_object_request(&values);
        let entry_ts = values.trade.as_ref().unwrap().entry_ts();
        let trade = self.strategy.get_trade(&request);
        let trade_pnl = TradePnLCalculatorBuilder::new()
            .with_entry_ts(entry_ts)
            .with_trade(trade.clone())
            .with_market_sim_data_since_entry(self.market_sim_data_since_entry_ts(entry_ts))
            .with_trade_and_pre_trade_values(values)
            .build_and_compute();

        PnLReportDataRow {
            market: self.market,
            year: self.year,
            strategy_name: self.strategy.get_name(),
            time_frame_snapshot: self.time_frame_snapshot,
            trade,
            trade_pnl: Some(trade_pnl),
        }
    }

    fn trade_object_request(&self, values: &TradeAndPreTradeValuesWithData) -> TradeRequestObject {
        let initial_balance = values
            .trade
            .as_ref()
            .and_then(|trade| Some(trade.initial_balance()));
        TradeRequestObject {
            pre_trade_values: values.pre_trade.clone(),
            initial_balance,
            market: self.market,
        }
    }

    fn market_sim_data_since_entry_ts(&self, entry_ts: i64) -> LazyFrame {
        self.market_sim_data
            .clone()
            .lazy()
            .drop_rows_before_entry_ts(entry_ts)
    }

    fn compute_pre_trade_values(&self) -> RequiredPreTradeValuesWithData {
        let calculator_builder: PreTradeValuesCalculatorBuilder = self.into();
        calculator_builder
            .with_required_pre_trade_values(self.strategy.get_required_pre_trade_vales())
            .build_and_compute()
    }

    fn compute_trade_values(
        &self,
        pre_trade_values: &RequiredPreTradeValuesWithData,
    ) -> Option<TradeValuesWithData> {
        let calculator_builder: TradeValuesCalculatorBuilder = self.into();
        calculator_builder
            .with_entry_price(self.strategy.get_entry_price(&pre_trade_values))
            .build_and_compute()
    }

    fn get_trade_and_pre_trade_values_with_data(&self) -> TradeAndPreTradeValuesWithData {
        let pre_trade = self.compute_pre_trade_values();
        let trade = self.compute_trade_values(&pre_trade);
        TradeAndPreTradeValuesWithData { trade, pre_trade }
    }
}

pub struct PnLReportDataRowCalculatorBuilder {
    data_provider: Option<Arc<dyn DataProvider>>,
    strategy: Option<Arc<dyn Strategy>>,
    market_sim_data: Option<DataFrame>,
    pre_trade_data: Option<PreTradeData>,
    market: Option<MarketKind>,
    year: Option<u32>,
    time_frame_snapshot: Option<TimeFrameSnapshot>,
    market_sim_data_kind: Option<MarketSimulationDataKind>,
}

impl PnLReportDataRowCalculatorBuilder {
    pub fn new() -> Self {
        Self {
            data_provider: None,
            strategy: None,
            market_sim_data: None,
            pre_trade_data: None,
            market: None,
            year: None,
            time_frame_snapshot: None,
            market_sim_data_kind: None,
        }
    }

    pub fn with_data_provider(self, data_provider: Arc<dyn DataProvider>) -> Self {
        Self {
            data_provider: Some(data_provider),
            ..self
        }
    }

    pub fn with_strategy(self, strategy: Arc<dyn Strategy>) -> Self {
        Self {
            strategy: Some(strategy),
            ..self
        }
    }

    pub fn with_market_sim_data(self, market_sim_data: DataFrame) -> Self {
        Self {
            market_sim_data: Some(market_sim_data),
            ..self
        }
    }

    pub fn with_pre_trade_data(self, pre_trade_data: PreTradeData) -> Self {
        Self {
            pre_trade_data: Some(pre_trade_data),
            ..self
        }
    }

    pub fn with_market(self, market: MarketKind) -> Self {
        Self {
            market: Some(market),
            ..self
        }
    }

    pub fn with_year(self, year: u32) -> Self {
        Self {
            year: Some(year),
            ..self
        }
    }

    pub fn with_time_frame_snapshot(self, snapshot: TimeFrameSnapshot) -> Self {
        Self {
            time_frame_snapshot: Some(snapshot),
            ..self
        }
    }

    pub fn with_market_sim_data_kind(self, market_sim_data_kind: MarketSimulationDataKind) -> Self {
        Self {
            market_sim_data_kind: Some(market_sim_data_kind),
            ..self
        }
    }

    pub fn build(self) -> PnLReportDataRowCalculator {
        PnLReportDataRowCalculator {
            data_provider: self.data_provider.unwrap(),
            strategy: self.strategy.unwrap(),
            market_sim_data: self.market_sim_data.unwrap(),
            pre_trade_data: self.pre_trade_data.unwrap(),
            market: self.market.unwrap(),
            year: self.year.unwrap(),
            time_frame_snapshot: self.time_frame_snapshot.unwrap(),
            market_sim_data_kind: self.market_sim_data_kind.unwrap(),
        }
    }

    pub fn build_and_compute(self) -> PnLReportDataRow {
        self.build().compute()
    }
}
