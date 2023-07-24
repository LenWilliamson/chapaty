use std::{collections::HashMap, sync::Arc};

use polars::prelude::{ DataFrame, IntoLazy};

use crate::{
    data_provider::DataProvider,
    enums::{bots::TradeDataKind, MyAnyValue},
    lazy_frame_operations::trait_extensions::MyLazyFrameOperations, converter::any_value::AnyValueConverter,
};

use super::pnl_report_data_row_calculator::PnLReportDataRowCalculator;

pub struct TradeValuesCalculator {
    data_provider: Arc<dyn DataProvider>,
    market_sim_data: DataFrame,
    entry_price: f64,
}

impl TradeValuesCalculator {
    /// This function computes the `Option<TradeData>` consisting of
    /// * `entry_ts` - entry time stamp where the market hits the `poc`
    /// * `last_trade_price` - closing price of this week
    /// * `lowest_price_since_entry` - lowest traded price since we entered our trade
    /// * `highest_price_since_entry` - highest traded price since we entered our trade
    /// * `lowest_price_since_entry_ts` - time stamp of lowest traded price since we entered our trade
    /// * `highest_price_since_entry_ts` - time stamp of highest traded price since we entered our trade
    ///
    /// We determine in this function if we enter a trade or not. We return
    /// * `None` - if we don't enter a trade
    /// * `Some(TradeData)` - if we enter a trade
    ///
    /// # Note
    ///
    /// A Trade has two phases which are each in two separates but consecutive calendar weeks. The first
    /// phase is the pre trade phase (`PreTradeData`). The second phase is the trade phase (`TradeData`). In each phase we collect data.
    /// If we don't enter the trade in the second week, the `trade_data` object is `None`.
    ///
    /// Collects the data if a trade occurs
    pub fn compute(&self) -> Option<HashMap<TradeDataKind, MyAnyValue>> {
        self.market_sim_data
            .clone()
            .lazy()
            .find_timestamp_when_price_reached(self.entry_price, self.data_provider.clone())
            .map_or_else(|| None, |entry_ts| Some(self.get_result(entry_ts)))
    }

    fn get_result(&self, entry_ts: i64) -> HashMap<TradeDataKind, MyAnyValue> {
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
    ) -> Vec<(TradeDataKind, MyAnyValue)> {
        match_trade_values_with_trade_kind(self.get_trade_values_as_df(entry_ts))
    }

    fn initialize_trade_value_map(
        &self,
        entry_ts: i64,
    ) -> HashMap<TradeDataKind, MyAnyValue> {
        HashMap::from([
            (
                TradeDataKind::EntryPrice,
                MyAnyValue::Float64(self.entry_price),
            ),
            (TradeDataKind::EntryTimestamp, MyAnyValue::Int64(entry_ts)),
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
        let dp = self.data_provider.clone();
        // let ots = dp.column_name_as_str(&Columns::Ohlc(OhlcColumnNames::OpenTime));
        // let high = dp.column_name_as_str(&Columns::Ohlc(OhlcColumnNames::High));
        // let low = dp.column_name_as_str(&Columns::Ohlc(OhlcColumnNames::Low));
        // let close = dp.column_name_as_str(&Columns::Ohlc(OhlcColumnNames::Close));

        self.market_sim_data
            .clone()
            .lazy()
            .drop_rows_before_entry_ts(entry_ts, dp.clone())
            .filter_trade_data_kind_values(dp)
            .collect()
            .unwrap()
    }
}

fn match_trade_values_with_trade_kind(
    trade_values: DataFrame,
) -> Vec<(TradeDataKind, MyAnyValue)> {
    let row = trade_values.get(0).unwrap();
    vec![
        (TradeDataKind::LastTradePrice, MyAnyValue::Float64(row[0].unwrap_float64())),
        (TradeDataKind::LowestTradePriceSinceEntry, MyAnyValue::Float64(row[1].unwrap_float64())),
        (TradeDataKind::HighestTradePriceSinceEntry, MyAnyValue::Float64(row[2].unwrap_float64())),
        (TradeDataKind::HighestTradePriceSinceEntryTimestamp, MyAnyValue::Int64(row[3].unwrap_int64())),
        (TradeDataKind::LowestTradePriceSinceEntryTimestamp, MyAnyValue::Int64(row[4].unwrap_int64())),
    ]
}

fn update_trade_value_map(
    mut trade_data_map: HashMap<TradeDataKind, MyAnyValue>,
    val: (TradeDataKind, MyAnyValue),
) -> HashMap<TradeDataKind, MyAnyValue> {
    trade_data_map.insert(val.0, val.1);
    trade_data_map
}

pub struct TradeValuesCalculatorBuilder {
    data_provider: Option<Arc<dyn DataProvider>>,
    market_sim_data: Option<DataFrame>,
    entry_price: Option<f64>,
}

impl From<&PnLReportDataRowCalculator> for TradeValuesCalculatorBuilder {
    fn from(value: &PnLReportDataRowCalculator) -> Self {
        Self {
            data_provider: Some(value.data_provider.clone()),
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
            data_provider: self.data_provider.unwrap(),
            market_sim_data: self.market_sim_data.unwrap(),
            entry_price: self.entry_price.unwrap(),
        }
    }

    pub fn build_and_compute(self) -> Option<HashMap<TradeDataKind, MyAnyValue>> {
        self.build().compute()
    }
}
