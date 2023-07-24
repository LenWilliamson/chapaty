use std::{collections::HashMap, sync::Arc};

use polars::prelude::{DataFrame, IntoLazy, LazyFrame};

use crate::{
    backtest_result::pnl_report::PnLReportDataRow,
    bot::{pre_trade_data::PreTradeData, time_frame_snapshot::TimeFrameSnapshot},
    data_provider::DataProvider,
    enums::{trade_and_pre_trade::TradeDataKind, markets::MarketKind, my_any_value::MyAnyValueKind, },
    lazy_frame_operations::trait_extensions::MyLazyFrameOperations,
    strategy::Strategy,
};

use super::{
    pre_trade_values_calculator::{PreTradeValues, PreTradeValuesCalculatorBuilder},
    trade_pnl_calculator::TradePnLCalculatorBuilder,
    trade_values_calculator::TradeValuesCalculatorBuilder,
};

pub struct PnLReportDataRowCalculator {
    pub data_provider: Arc<dyn DataProvider>,
    pub strategy: Arc<dyn Strategy>,
    pub market_sim_data: DataFrame,
    pub pre_trade_data: PreTradeData,
    pub market: MarketKind,
    pub year: u32,
    pub time_frame_snapshot: TimeFrameSnapshot,
}

#[derive(Clone)]
pub struct TradeAndPreTradeValues {
    pub trade: HashMap<TradeDataKind, MyAnyValueKind>,
    pub pre_trade: PreTradeValues,
}

impl PnLReportDataRowCalculator {
    pub fn compute(&self) -> PnLReportDataRow {
        let pre_trade = self.compute_pre_trade_values();
        self.compute_trade_values(&pre_trade).map_or_else(
            || self.handle_no_entry(pre_trade.clone()),
            |trade| {
                self.handle_trade(TradeAndPreTradeValues {
                    trade,
                    pre_trade: pre_trade.clone(),
                })
            },
        )
    }

    fn handle_no_entry(&self, pre_trade: PreTradeValues) -> PnLReportDataRow {
        PnLReportDataRow {
            market: self.market.clone(),
            year: self.year,
            strategy: self.strategy.get_bot_kind(),
            time_frame_snapshot: self.time_frame_snapshot,
            trade: self.strategy.get_trade(&pre_trade),
            trade_pnl: None,
        }
    }

    fn handle_trade<'a>(&self, values: TradeAndPreTradeValues) -> PnLReportDataRow {
        let entry_ts = get_entry_ts(&values.trade);
        let trade = self.strategy.get_trade(&values.pre_trade);
        let trade_pnl = TradePnLCalculatorBuilder::new()
            .with_entry_ts(entry_ts)
            .with_trade(trade.clone())
            .with_market_sim_data_since_entry(self.market_sim_data_since_entry_ts(entry_ts))
            .with_trade_and_pre_trade_values(values)
            .build_and_compute();

        PnLReportDataRow {
            market: self.market,
            year: self.year,
            strategy: self.strategy.get_bot_kind(),
            time_frame_snapshot: self.time_frame_snapshot,
            trade,
            trade_pnl: Some(trade_pnl),
        }
    }

    fn market_sim_data_since_entry_ts(&self, entry_ts: i64) -> LazyFrame {
        self.market_sim_data
            .clone()
            .lazy()
            .drop_rows_before_entry_ts(entry_ts)
    }

    fn compute_pre_trade_values(&self) -> PreTradeValues {
        let calculator_builder: PreTradeValuesCalculatorBuilder = self.into();
        calculator_builder
            .with_required_market_sim_values(self.strategy.required_pre_trade_data())
            .with_required_indicator_values(self.strategy.register_trading_indicators())
            .build_and_compute()
    }

    fn compute_trade_values<'a>(
        &self,
        pre_trade_values: &PreTradeValues,
    ) -> Option<HashMap<TradeDataKind, MyAnyValueKind>> {
        let calculator_builder: TradeValuesCalculatorBuilder = self.into();
        calculator_builder
            .with_entry_price(self.strategy.get_entry_price(&pre_trade_values))
            .build_and_compute()
    }
}

fn get_entry_ts<'a>(trade_values: &HashMap<TradeDataKind, MyAnyValueKind>) -> i64 {
    trade_values
        .get(&TradeDataKind::EntryTimestamp)
        .unwrap()
        .clone()
        .unwrap_int64()
}

pub struct PnLReportDataRowCalculatorBuilder {
    data_provider: Option<Arc<dyn DataProvider>>,
    strategy: Option<Arc<dyn Strategy>>,
    market_sim_data: Option<DataFrame>,
    pre_trade_data: Option<PreTradeData>,
    market: Option<MarketKind>,
    year: Option<u32>,
    time_frame_snapshot: Option<TimeFrameSnapshot>,
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

    pub fn build(self) -> PnLReportDataRowCalculator {
        PnLReportDataRowCalculator {
            data_provider: self.data_provider.unwrap(),
            strategy: self.strategy.unwrap(),
            market_sim_data: self.market_sim_data.unwrap(),
            pre_trade_data: self.pre_trade_data.unwrap(),
            market: self.market.unwrap(),
            year: self.year.unwrap(),
            time_frame_snapshot: self.time_frame_snapshot.unwrap(),
        }
    }

    pub fn build_and_compute(self) -> PnLReportDataRow {
        self.build().compute()
    }
}
