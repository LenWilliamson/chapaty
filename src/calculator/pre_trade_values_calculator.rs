use super::pnl_report_data_row_calculator::PnLReportDataRowCalculator;
use crate::{
    bot::pre_trade_data::PreTradeData,
    converter::any_value::AnyValueConverter,
    enums::{
        column_names::DataProviderColumnKind,
        indicator::{PriceHistogramKind, TradingIndicatorKind},
        trade_and_pre_trade::PreTradeDataKind,
    },
    trading_indicator::price_histogram::PriceHistogram,
};
use polars::prelude::{col, IntoLazy};
use std::collections::HashMap;

#[derive(Clone)]
pub struct PreTradeValues {
    pub market_valeus: HashMap<PreTradeDataKind, f64>,
    pub indicator_values: HashMap<TradingIndicatorKind, f64>,
}

pub struct PreTradeValuesCalculator {
    pre_trade_data: PreTradeData,
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
            } // _ => panic!("Not yet implemented!"),
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
                map.insert(TradingIndicatorKind::Poc(PriceHistogramKind::Tpo1m), res);
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
        let ph = PriceHistogram::new(df);

        match indicator {
            TradingIndicatorKind::Poc(_) => ph.poc(),
            _ => {
                ph.volume_area(0.3);
                panic!("Not yet implemented!")
            }
        }
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

    required_market_sim_values: Option<Vec<PreTradeDataKind>>,
    required_indicator_values: Option<Vec<TradingIndicatorKind>>,
}

impl From<&PnLReportDataRowCalculator> for PreTradeValuesCalculatorBuilder {
    fn from(value: &PnLReportDataRowCalculator) -> Self {
        Self {
            pre_trade_data: Some(value.pre_trade_data.clone()),

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

            required_market_sim_values: self.required_market_sim_values.unwrap(),
            required_indicator_values: self.required_indicator_values.unwrap(),
        }
    }

    pub fn build_and_compute(self) -> PreTradeValues {
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

        let caclulator = PreTradeValuesCalculator {
            pre_trade_data,
            required_indicator_values: Vec::new(),
            required_market_sim_values: Vec::new(),
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

        let caclulator = PreTradeValuesCalculator {
            pre_trade_data,
            required_indicator_values: Vec::new(),
            required_market_sim_values: Vec::new(),
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

        let caclulator = PreTradeValuesCalculator {
            pre_trade_data,
            required_indicator_values: Vec::new(),
            required_market_sim_values: Vec::new(),
        };

        assert_eq!(44_225.84, caclulator.compute_highest_trade_price());
    }
}
