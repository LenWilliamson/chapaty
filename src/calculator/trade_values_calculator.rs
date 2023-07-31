use std::collections::HashMap;

use polars::prelude::{DataFrame, IntoLazy};

use crate::{
    converter::any_value::AnyValueConverter,
    enums::{my_any_value::MyAnyValueKind, trade_and_pre_trade::TradeDataKind},
    lazy_frame_operations::trait_extensions::MyLazyFrameOperations,
};

use super::pnl_report_data_row_calculator::PnLReportDataRowCalculator;

pub struct TradeValuesCalculator {
    market_sim_data: DataFrame,
    entry_price: f64,
}

impl TradeValuesCalculator {
    pub fn compute(&self) -> Option<HashMap<TradeDataKind, MyAnyValueKind>> {
        self.market_sim_data
            .clone()
            .lazy()
            .find_timestamp_when_price_reached(self.entry_price)
            .map_or_else(|| None, |entry_ts| Some(self.get_result(entry_ts)))
    }

    fn get_result(&self, entry_ts: i64) -> HashMap<TradeDataKind, MyAnyValueKind> {
        self.get_trade_values_since_entry_timestamp(entry_ts)
            .into_iter()
            .fold(
                self.initialize_trade_value_map(entry_ts),
                update_trade_value_map,
            )
    }

    fn get_trade_values_since_entry_timestamp(
        &self,
        entry_ts: i64,
    ) -> Vec<(TradeDataKind, MyAnyValueKind)> {
        match_trade_values_with_trade_kind(self.get_trade_values_as_df(entry_ts))
    }

    fn initialize_trade_value_map(&self, entry_ts: i64) -> HashMap<TradeDataKind, MyAnyValueKind> {
        HashMap::from([
            (
                TradeDataKind::EntryPrice,
                MyAnyValueKind::Float64(self.entry_price),
            ),
            (
                TradeDataKind::EntryTimestamp,
                MyAnyValueKind::Int64(entry_ts),
            ),
        ])
    }

    /// # Returns
    /// This function returns a `DataFrame` with a single row, containing the following column values at index
    /// * Index 0: last trade price
    /// * Index 1: lowest trade price
    /// * Index 2: highest trade price
    /// * Index 3: timestamp highest trade price
    /// * Index 4: timestamp lowest trade price
    fn get_trade_values_as_df(&self, entry_ts: i64) -> DataFrame {
        self.market_sim_data
            .clone()
            .lazy()
            .drop_rows_before_entry_ts(entry_ts)
            .filter_trade_data_kind_values()
            .collect()
            .unwrap()
    }
}

fn match_trade_values_with_trade_kind(
    trade_values: DataFrame,
) -> Vec<(TradeDataKind, MyAnyValueKind)> {
    let row = trade_values.get(0).unwrap();
    vec![
        (
            TradeDataKind::LastTradePrice,
            MyAnyValueKind::Float64(row[0].unwrap_float64()),
        ),
        (
            TradeDataKind::LowestTradePriceSinceEntry,
            MyAnyValueKind::Float64(row[1].unwrap_float64()),
        ),
        (
            TradeDataKind::HighestTradePriceSinceEntry,
            MyAnyValueKind::Float64(row[2].unwrap_float64()),
        ),
        (
            TradeDataKind::HighestTradePriceSinceEntryTimestamp,
            MyAnyValueKind::Int64(row[3].unwrap_int64()),
        ),
        (
            TradeDataKind::LowestTradePriceSinceEntryTimestamp,
            MyAnyValueKind::Int64(row[4].unwrap_int64()),
        ),
    ]
}

fn update_trade_value_map(
    mut trade_data_map: HashMap<TradeDataKind, MyAnyValueKind>,
    val: (TradeDataKind, MyAnyValueKind),
) -> HashMap<TradeDataKind, MyAnyValueKind> {
    trade_data_map.insert(val.0, val.1);
    trade_data_map
}

pub struct TradeValuesCalculatorBuilder {
    market_sim_data: Option<DataFrame>,
    entry_price: Option<f64>,
}

impl From<&PnLReportDataRowCalculator> for TradeValuesCalculatorBuilder {
    fn from(value: &PnLReportDataRowCalculator) -> Self {
        Self {
            market_sim_data: Some(value.market_sim_data.clone()),
            entry_price: None,
        }
    }
}

impl TradeValuesCalculatorBuilder {
    pub fn with_entry_price(self, entry_price: f64) -> Self {
        Self {
            entry_price: Some(entry_price),
            ..self
        }
    }

    pub fn build(self) -> TradeValuesCalculator {
        TradeValuesCalculator {
            market_sim_data: self.market_sim_data.unwrap(),
            entry_price: self.entry_price.unwrap(),
        }
    }

    pub fn build_and_compute(self) -> Option<HashMap<TradeDataKind, MyAnyValueKind>> {
        self.build().compute()
    }
}

#[cfg(test)]
mod test {
    use crate::cloud_api::api_for_unit_tests::download_df;

    use super::*;

    #[tokio::test]
    async fn test_compute_trade_values() {
        let market_sim_data = download_df(
            "chapaty-ai-test".to_string(),
            "ppp/_test_data_files/trade_data.csv".to_string(),
        )
        .await;
        let mut calculator = TradeValuesCalculator {
            entry_price: 42_000.0,
            market_sim_data: market_sim_data.clone(),
        };

        // Define target values from test data
        let entry_price = 42_000.0;
        let ts = 1646085600000_i64;
        let lst_tp = 43_578.87;
        let low = 41628.99;
        let high = 44_225.84;
        let low_ots = 1646085600000_i64;
        let high_ots = 1646085600000_i64;
        let mut target = HashMap::new();

        target.insert(
            TradeDataKind::EntryPrice,
            MyAnyValueKind::Float64(entry_price),
        );
        target.insert(TradeDataKind::EntryTimestamp, MyAnyValueKind::Int64(ts));
        target.insert(
            TradeDataKind::LastTradePrice,
            MyAnyValueKind::Float64(lst_tp),
        );
        target.insert(
            TradeDataKind::LowestTradePriceSinceEntry,
            MyAnyValueKind::Float64(low),
        );
        target.insert(
            TradeDataKind::LowestTradePriceSinceEntryTimestamp,
            MyAnyValueKind::Int64(low_ots),
        );
        target.insert(
            TradeDataKind::HighestTradePriceSinceEntry,
            MyAnyValueKind::Float64(high),
        );
        target.insert(
            TradeDataKind::HighestTradePriceSinceEntryTimestamp,
            MyAnyValueKind::Int64(high_ots),
        );

        assert_eq!(target, calculator.compute().unwrap());

        calculator = TradeValuesCalculator {
            entry_price: 0.0,
            market_sim_data
        };

        assert_eq!(None, calculator.compute())


    }
}
