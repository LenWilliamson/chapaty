use std::{collections::HashMap, sync::Arc};

use crate::{
    bot::pre_trade_data::PreTradeData,
    data_provider::DataProvider,
    enums::{
        self,
        bots::{PreTradeDataKind, TradingIndicatorKind},
        columns::{Columns, OhlcColumnNames},
    },
    trading_indicator::price_histogram::PriceHistogram, converter::any_value::AnyValueConverter,
};

use polars::prelude::{col, IntoLazy};

use super::pnl_report_data_row_calculator::PnLReportDataRowCalculator;

#[derive(Clone)]
pub struct PreTradeValues {
    pub market_valeus: HashMap<PreTradeDataKind, f64>,
    pub indicator_values: HashMap<TradingIndicatorKind, f64>,
}

pub struct PreTradeValuesCalculator {
    pre_trade_data: PreTradeData,
    data_provider: Arc<dyn DataProvider>,
    required_market_sim_values: Vec<PreTradeDataKind>,
    required_indicator_values: Vec<TradingIndicatorKind>,
}

impl PreTradeValuesCalculator {
    pub fn compute(&self) -> PreTradeValues {
        PreTradeValues {
            market_valeus: self.compute_market_values(),
            indicator_values: self.compute_indicator_values(),
        }
    }

    fn compute_market_values(&self) -> HashMap<PreTradeDataKind, f64> {
        self.required_market_sim_values
            .iter()
            .fold(HashMap::new(), |acc, val| {
                self.update_market_value_map(acc, val)
            })
    }

    fn update_market_value_map(
        &self,
        mut map: HashMap<PreTradeDataKind, f64>,
        val: &PreTradeDataKind,
    ) -> HashMap<PreTradeDataKind, f64> {
        match val {
            PreTradeDataKind::LastTradePrice => {
                let res = self.compute_last_trade_price();
                map.insert(PreTradeDataKind::LastTradePrice, res);
            }
            PreTradeDataKind::LowestTradePrice => {
                let res = self.compute_lowest_trade_price();
                map.insert(PreTradeDataKind::LowestTradePrice, res);
            }
            PreTradeDataKind::HighestTradePrice => {
                let res = self.compute_highest_trade_price();
                map.insert(PreTradeDataKind::HighestTradePrice, res);
            }
            _ => panic!("Not yet implemented!"),
        };

        map
    }

    fn compute_indicator_values(&self) -> HashMap<TradingIndicatorKind, f64> {
        self.required_indicator_values
            .iter()
            .fold(HashMap::new(), |acc, val| {
                self.update_indicator_value_map(acc, val)
            })
    }

    fn update_indicator_value_map(
        &self,
        mut map: HashMap<TradingIndicatorKind, f64>,
        val: &TradingIndicatorKind,
    ) -> HashMap<TradingIndicatorKind, f64> {
        match val {
            TradingIndicatorKind::Poc(_) => {
                let res = self.handle_price_histogram_indicator(val);
                map.insert(
                    TradingIndicatorKind::Poc(enums::bots::PriceHistogram::Tpo1m),
                    res,
                );
            }
            _ => panic!("Not yet implemented!"),
        };

        map
    }

    fn handle_price_histogram_indicator(&self, indicator: &TradingIndicatorKind) -> f64 {
        let df = self
            .pre_trade_data
            .indicators
            .get(&indicator)
            .unwrap()
            .clone();
        let ph = PriceHistogram::new(self.data_provider.clone(), df);

        match indicator {
            TradingIndicatorKind::Poc(_) => ph.poc(),
            _ => panic!("Not yet implemented!"),
        }
    }

    fn compute_last_trade_price(&self) -> f64 {
        let dp = self.data_provider.clone();
        let df = self.pre_trade_data.market_sim_data.clone();

        let close = dp.column_name_as_str(&Columns::Ohlc(OhlcColumnNames::Close));
        let filt = df.lazy().select([col(&close).last()]).collect().unwrap();

        let v = filt.get(0).unwrap();
        let last_trade_price = v[0].unwrap_float64();
        last_trade_price
    }

    fn compute_lowest_trade_price(&self) -> f64 {
        let dp = self.data_provider.clone();
        let df = self.pre_trade_data.market_sim_data.clone();

        let low = dp.column_name_as_str(&Columns::Ohlc(OhlcColumnNames::Low));
        let filt = df.lazy().select([col(&low).min()]).collect().unwrap();

        let v = filt.get(0).unwrap();
        let lowest_trade_price = v[0].unwrap_float64();
        lowest_trade_price
    }

    fn compute_highest_trade_price(&self) -> f64 {
        let dp = self.data_provider.clone();
        let df = self.pre_trade_data.market_sim_data.clone();

        let high = dp.column_name_as_str(&Columns::Ohlc(OhlcColumnNames::High));
        let filt = df.lazy().select([col(&high).max()]).collect().unwrap();

        let v = filt.get(0).unwrap();
        let highest_trade_price = v[0].unwrap_float64();
        highest_trade_price
    }
}

pub struct PreTradeValuesCalculatorBuilder {
    pre_trade_data: Option<PreTradeData>,
    data_provider: Option<Arc<dyn DataProvider>>,
    required_market_sim_values: Option<Vec<PreTradeDataKind>>,
    required_indicator_values: Option<Vec<TradingIndicatorKind>>,
}

impl From<&PnLReportDataRowCalculator> for PreTradeValuesCalculatorBuilder {
    fn from(value: &PnLReportDataRowCalculator) -> Self {
        Self {
            pre_trade_data: Some(value.pre_trade_data.clone()),
            data_provider: Some(value.data_provider.clone()),
            required_market_sim_values: None,
            required_indicator_values: None,
        }
    }
}

impl PreTradeValuesCalculatorBuilder {


    pub fn with_required_market_sim_values(
        self,
        required_market_sim_values: Vec<PreTradeDataKind>,
    ) -> Self {
        Self {
            required_market_sim_values: Some(required_market_sim_values),
            ..self
        }
    }

    pub fn with_required_indicator_values(
        self,
        required_indicator_values: Vec<TradingIndicatorKind>,
    ) -> Self {
        Self {
            required_indicator_values: Some(required_indicator_values),
            ..self
        }
    }

    pub fn build(self) -> PreTradeValuesCalculator {
        PreTradeValuesCalculator {
            pre_trade_data: self.pre_trade_data.unwrap(),
            data_provider: self.data_provider.unwrap(),
            required_market_sim_values: self.required_market_sim_values.unwrap(),
            required_indicator_values: self.required_indicator_values.unwrap(),
        }
    }

    pub fn build_and_compute(self) -> PreTradeValues {
        self.build().compute()
    }
}
