use super::pnl_report_data_row_calculator::PnLReportDataRowCalculator;
use crate::{
    bot::pre_trade_data::PreTradeData,
    converter::any_value::AnyValueConverter,
    enums::{
        column_names::DataProviderColumnKind, indicator::TradingIndicatorKind,
        trade_and_pre_trade::PreTradeDataKind,
    },
    strategy::RequriedPreTradeValues,
    trading_indicator::price_histogram::PriceHistogram,
    PriceHistogramKind,
};
use polars::prelude::{col, IntoLazy};
use std::collections::HashMap;

#[derive(Clone)]
pub struct RequiredPreTradeValuesWithData {
    pub market_valeus: HashMap<PreTradeDataKind, f64>,
    pub indicator_values: HashMap<TradingIndicatorKind, f64>,
}

impl RequiredPreTradeValuesWithData {
    pub fn lowest_trade_price(&self) -> f64 {
        *self
            .market_valeus
            .get(&PreTradeDataKind::LowestTradePrice)
            .unwrap()
    }
    pub fn highest_trade_price(&self) -> f64 {
        *self
            .market_valeus
            .get(&PreTradeDataKind::HighestTradePrice)
            .unwrap()
    }
    pub fn last_trade_price(&self) -> f64 {
        *self
            .market_valeus
            .get(&PreTradeDataKind::LastTradePrice)
            .unwrap()
    }
    pub fn value_area_high(&self, ph: PriceHistogramKind) -> f64 {
        *self
            .indicator_values
            .get(&TradingIndicatorKind::ValueAreaHigh(ph))
            .unwrap()
    }
    pub fn value_area_low(&self, ph: PriceHistogramKind) -> f64 {
        *self
            .indicator_values
            .get(&TradingIndicatorKind::ValueAreaLow(ph))
            .unwrap()
    }
    pub fn poc(&self, ph: PriceHistogramKind) -> f64 {
        *self
            .indicator_values
            .get(&TradingIndicatorKind::Poc(ph))
            .unwrap()
    }
}

pub struct PreTradeValuesCalculator {
    pre_trade_data: PreTradeData,
    required_pre_trade_values: RequriedPreTradeValues,
}

impl PreTradeValuesCalculator {
    pub fn compute(&self) -> RequiredPreTradeValuesWithData {
        RequiredPreTradeValuesWithData {
            market_valeus: self.compute_market_values(),
            indicator_values: self.compute_indicator_values(),
        }
    }

    fn compute_market_values(&self) -> HashMap<PreTradeDataKind, f64> {
        self.required_pre_trade_values
            .market_values
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
        };

        map
    }

    fn compute_indicator_values(&self) -> HashMap<TradingIndicatorKind, f64> {
        self.required_pre_trade_values
            .trading_indicators
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
            TradingIndicatorKind::Poc(ph) => {
                map.insert(TradingIndicatorKind::Poc(*ph), self.get_poc(val));
            }
            TradingIndicatorKind::ValueAreaHigh(ph) | TradingIndicatorKind::ValueAreaLow(ph) => {
                let (value_area_low, value_area_high) = self.get_value_area(val);
                map.insert(TradingIndicatorKind::ValueAreaHigh(*ph), value_area_low);
                map.insert(TradingIndicatorKind::ValueAreaLow(*ph), value_area_high);
            }
        };

        map
    }

    fn get_poc(&self, indicator: &TradingIndicatorKind) -> f64 {
        let df = self
            .pre_trade_data
            .indicators
            .get(&indicator)
            .unwrap()
            .clone();
        let ph = PriceHistogram::new(df);
        ph.poc()
    }

    fn get_value_area(&self, indicator: &TradingIndicatorKind) -> (f64, f64) {
        let df = self
            .pre_trade_data
            .indicators
            .get(&indicator)
            .unwrap()
            .clone();
        let ph = PriceHistogram::new(df);
        ph.value_area(0.63)
    }

    fn compute_last_trade_price(&self) -> f64 {
        let df = self.pre_trade_data.market_sim_data.clone();

        let close = DataProviderColumnKind::Close.to_string();
        let filt = df.lazy().select([col(&close).last()]).collect().unwrap();

        let v = filt.get(0).unwrap();
        let last_trade_price = v[0].unwrap_float64();
        last_trade_price
    }

    fn compute_lowest_trade_price(&self) -> f64 {
        let df = self.pre_trade_data.market_sim_data.clone();

        let low = DataProviderColumnKind::Low.to_string();
        let filt = df.lazy().select([col(&low).min()]).collect().unwrap();

        let v = filt.get(0).unwrap();
        let lowest_trade_price = v[0].unwrap_float64();
        lowest_trade_price
    }

    fn compute_highest_trade_price(&self) -> f64 {
        let df = self.pre_trade_data.market_sim_data.clone();

        let high = DataProviderColumnKind::High.to_string();
        let filt = df.lazy().select([col(&high).max()]).collect().unwrap();

        let v = filt.get(0).unwrap();
        let highest_trade_price = v[0].unwrap_float64();
        highest_trade_price
    }
}

pub struct PreTradeValuesCalculatorBuilder {
    pre_trade_data: Option<PreTradeData>,

    required_pre_trade_values: Option<RequriedPreTradeValues>,
}

impl From<&PnLReportDataRowCalculator> for PreTradeValuesCalculatorBuilder {
    fn from(value: &PnLReportDataRowCalculator) -> Self {
        Self {
            pre_trade_data: Some(value.pre_trade_data.clone()),
            required_pre_trade_values: None,
        }
    }
}

impl PreTradeValuesCalculatorBuilder {
    pub fn with_required_pre_trade_values(
        self,
        required_pre_trade_values: RequriedPreTradeValues,
    ) -> Self {
        Self {
            required_pre_trade_values: Some(required_pre_trade_values),
            ..self
        }
    }

    pub fn build(self) -> PreTradeValuesCalculator {
        PreTradeValuesCalculator {
            pre_trade_data: self.pre_trade_data.unwrap(),
            required_pre_trade_values: self.required_pre_trade_values.unwrap(),
        }
    }

    pub fn build_and_compute(self) -> RequiredPreTradeValuesWithData {
        self.build().compute()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        calculator::pre_trade_values_calculator::PreTradeData,
        cloud_api::api_for_unit_tests::download_df,
    };
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_compute_last_trade_price() {
        let df = download_df(
            "chapaty-ai-test".to_string(),
            "ppp/_test_data_files/pre_trade_data.csv".to_string(),
        )
        .await;

        let pre_trade_data = PreTradeData {
            market_sim_data: df,
            indicators: HashMap::new(),
        };

        let required_pre_trade_values = RequriedPreTradeValues {
            market_values: Vec::new(),
            trading_indicators: Vec::new(),
        };

        let caclulator = PreTradeValuesCalculator {
            pre_trade_data,
            required_pre_trade_values,
        };

        assert_eq!(43_578.87, caclulator.compute_last_trade_price());
    }

    #[tokio::test]
    async fn test_compute_lowest_trade_price() {
        let df = download_df(
            "chapaty-ai-test".to_string(),
            "ppp/_test_data_files/pre_trade_data.csv".to_string(),
        )
        .await;

        let pre_trade_data = PreTradeData {
            market_sim_data: df,
            indicators: HashMap::new(),
        };

        let required_pre_trade_values = RequriedPreTradeValues {
            market_values: Vec::new(),
            trading_indicators: Vec::new(),
        };

        let caclulator = PreTradeValuesCalculator {
            pre_trade_data,
            required_pre_trade_values,
        };

        assert_eq!(37_934.89, caclulator.compute_lowest_trade_price());
    }

    #[tokio::test]
    async fn test_compute_highest_trade_price() {
        let df = download_df(
            "chapaty-ai-test".to_string(),
            "ppp/_test_data_files/pre_trade_data.csv".to_string(),
        )
        .await;

        let pre_trade_data = PreTradeData {
            market_sim_data: df,
            indicators: HashMap::new(),
        };

        let required_pre_trade_values = RequriedPreTradeValues {
            market_values: Vec::new(),
            trading_indicators: Vec::new(),
        };

        let caclulator = PreTradeValuesCalculator {
            pre_trade_data,
            required_pre_trade_values,
        };

        assert_eq!(44_225.84, caclulator.compute_highest_trade_price());
    }
}
