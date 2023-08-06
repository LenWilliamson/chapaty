use std::{convert::identity, rc::Rc};

use crate::{
    converter::any_value::AnyValueConverter,
    data_frame_operations::trait_extensions::MyDataFrameOperations,
    enums::{
        column_names::{DataProviderColumnKind, VolumeProfileColumnKind},
        value_area::ValueAreaKind,
    },
    lazy_frame_operations::trait_extensions::MyLazyFrameOperations,
};

use polars::prelude::{col, AnyValue, DataFrame, IntoLazy};

pub struct PriceHistogram {
    df: DataFrame,
}

struct ValueArea {
    low: ValueAreaPart,
    high: ValueAreaPart,
    price_histogram: Rc<DataFrame>,
}

struct ValueAreaBuilder {
    low: Option<ValueAreaPart>,
    high: Option<ValueAreaPart>,
    price_histogram: Option<Rc<DataFrame>>,
}

impl From<&ValueArea> for ValueAreaBuilder {
    fn from(value: &ValueArea) -> Self {
        Self {
            low: Some(value.low),
            high: Some(value.high),
            price_histogram: Some(value.price_histogram.clone()),
        }
    }
}

impl ValueAreaBuilder {
    fn with_value_area_low(self, value_area_low: &ValueAreaPart) -> Self {
        Self {
            low: Some(*value_area_low),
            ..self
        }
    }
    fn with_value_area_high(self, value_area_high: &ValueAreaPart) -> Self {
        Self {
            high: Some(*value_area_high),
            ..self
        }
    }
    fn with_updated_value_area_part(self, part: ValueAreaPart) -> Self {
        match part.value_area_kind {
            ValueAreaKind::High => self.with_value_area_high(&part),
            ValueAreaKind::Low => self.with_value_area_low(&part),
        }
    }

    fn build(self) -> ValueArea {
        ValueArea {
            low: self.low.unwrap(),
            high: self.high.unwrap(),
            price_histogram: self.price_histogram.unwrap(),
        }
    }
}

#[derive(Copy, Clone)]
struct ValueAreaPart {
    price: f64,
    row_idx: u32,
    tpo_count: f64,
    is_tail_reached: bool,
    value_area_kind: ValueAreaKind,
}

#[derive(Copy, Clone)]
struct ValueAreaPartBuilder {
    price: Option<f64>,
    row_idx: Option<u32>,
    tpo_count: Option<f64>,
    value_area_kind: Option<ValueAreaKind>,
}

impl ValueAreaPartBuilder {
    fn new() -> Self {
        Self {
            price: None,
            row_idx: None,
            tpo_count: None,
            value_area_kind: None,
        }
    }

    fn with_price(self, price: f64) -> Self {
        Self {
            price: Some(price),
            ..self
        }
    }

    fn with_row_index(self, row_idx: u32) -> Self {
        Self {
            row_idx: Some(row_idx),
            ..self
        }
    }

    fn with_tpo_count(self, tpo_count: f64) -> Self {
        Self {
            tpo_count: Some(tpo_count),
            ..self
        }
    }

    fn with_value_area_kind(self, value_area_kind: ValueAreaKind) -> Self {
        Self {
            value_area_kind: Some(value_area_kind),
            ..self
        }
    }

    fn build(self) -> ValueAreaPart {
        ValueAreaPart {
            price: self.price.unwrap(),
            row_idx: self.row_idx.unwrap(),
            tpo_count: self.tpo_count.unwrap(),
            is_tail_reached: false,
            value_area_kind: self.value_area_kind.unwrap(),
        }
    }
}

impl ValueArea {
    fn new(price_histogram: &DataFrame, poc: f64) -> Self {
        let ph = price_histogram.df_with_row_count("row", None);
        let row_index = get_row_idx_from_df(&ph.clone().lazy().get_row_of_poc_as_df(poc));
        let value_area_part_builder = ValueAreaPartBuilder::new()
            .with_price(poc)
            .with_row_index(row_index)
            .with_tpo_count(0.0);
        let low = value_area_part_builder
            .clone()
            .with_value_area_kind(ValueAreaKind::Low)
            .build();
        let high = value_area_part_builder
            .with_value_area_kind(ValueAreaKind::High)
            .build();

        ValueArea {
            low,
            high,
            price_histogram: Rc::new(ph),
        }
    }

    fn update_value_area_parts(&self) -> Self {
        let low_update = self
            .low
            .update(&self.price_histogram)
            .map_or_else(|| self.low.with_tail_is_reached(), identity);
        let high_update = self
            .high
            .update(&self.price_histogram)
            .map_or_else(|| self.high.with_tail_is_reached(), identity);

        Self {
            low: low_update,
            high: high_update,
            price_histogram: self.price_histogram.clone(),
        }
    }

    fn update_value_area_part(&self, va_with_updated_parts: &ValueArea) -> Self {
        let value_area_builder: ValueAreaBuilder = self.into();
        match va_with_updated_parts.part_with_greater_tpo_delta(self) {
            None => value_area_builder.build(),
            Some(part) => value_area_builder
                .with_updated_value_area_part(part)
                .build(),
        }
    }

    fn part_with_greater_tpo_delta(&self, prev: &ValueArea) -> Option<ValueAreaPart> {
        if self.is_tail_reached_by_value_area_low_and_value_area_high() {
            None
        } else if self.is_tail_not_reached_by_value_area_low_and_value_area_high() {
            Some(self.get_part_with_max_tpo_delta(&prev))
        } else {
            Some(self.value_area_part_not_at_tail())
        }
    }

    fn determine_tpo_delta(&self, prev: &ValueArea) -> Option<f64> {
        if self.is_tail_reached_by_value_area_low_and_value_area_high() {
            None
        } else if self.is_tail_not_reached_by_value_area_low_and_value_area_high() {
            self.get_max_tpo_delta(&prev)
        } else {
            let part = self.value_area_part_not_at_tail();
            match part.value_area_kind {
                ValueAreaKind::High => Some(part.determine_tpo_delta(&prev.high)),
                ValueAreaKind::Low => Some(part.determine_tpo_delta(&prev.low)),
            }
        }
    }

    fn get_part_with_max_tpo_delta(&self, prev: &ValueArea) -> ValueAreaPart {
        if self.is_value_area_high_tpo_delta_larger(prev) {
            self.high
        } else {
            self.low
        }
    }

    fn is_value_area_high_tpo_delta_larger(&self, prev: &ValueArea) -> bool {
        self.high.determine_tpo_delta(&prev.high) > self.low.determine_tpo_delta(&prev.low)
    }

    fn get_max_tpo_delta(&self, prev: &ValueArea) -> Option<f64> {
        Some(f64::max(
            self.high.determine_tpo_delta(&prev.high),
            self.low.determine_tpo_delta(&prev.low),
        ))
    }

    fn value_area_part_not_at_tail(&self) -> ValueAreaPart {
        if self.low.is_tail_reached {
            self.high.clone()
        } else {
            self.low.clone()
        }
    }

    fn is_tail_reached_by_value_area_low_and_value_area_high(&self) -> bool {
        self.low.is_tail_reached && self.high.is_tail_reached
    }

    fn is_tail_not_reached_by_value_area_low_and_value_area_high(&self) -> bool {
        !self.low.is_tail_reached && !self.high.is_tail_reached
    }
}

impl ValueAreaPart {
    fn determine_tpo_delta(&self, prev_part: &ValueAreaPart) -> f64 {
        self.tpo_count - prev_part.tpo_count
    }

    fn with_tail_is_reached(self) -> Self {
        Self {
            is_tail_reached: true,
            ..self
        }
    }

    fn update(&self, price_histogram: &DataFrame) -> Option<Self> {
        self.try_update_value_area_part_with_next_row(price_histogram)
            .and_then(|part| {
                self.try_update_value_area_part_with_next_but_one_row(price_histogram, part)
            })
    }

    fn try_update_value_area_part_with_next_row(
        &self,
        price_histogram: &DataFrame,
    ) -> Option<Self> {
        if self.is_tail_reached {
            None
        } else {
            let next_row_index = self.get_next_row_index();
            next_row_index.and_then(|next_row| {
                price_histogram
                    .get(next_row)
                    .and_then(|row| self.then_update_value_area_part_with_row(row))
            })
        }
    }

    fn then_update_value_area_part_with_row<'a>(&self, row: Vec<AnyValue<'a>>) -> Option<Self> {
        Some(ValueAreaPart {
            price: get_price_from_df_row(&row),
            row_idx: get_row_index_from_df_row(&row),
            tpo_count: self.tpo_count + get_tpo_count_from_df_row(&row),
            is_tail_reached: self.is_tail_reached,
            value_area_kind: self.value_area_kind,
        })
    }

    fn try_update_value_area_part_with_next_but_one_row(
        &self,
        price_histogram: &DataFrame,
        part: ValueAreaPart,
    ) -> Option<Self> {
        let next_but_one_row_index = self.get_next_but_one_row_index();
        next_but_one_row_index.and_then(|next_but_one_row| {
            price_histogram.get(next_but_one_row).map_or_else(
                || Some(part),
                |row| part.then_update_value_area_part_with_row(row),
            )
        })
    }

    fn get_next_row_index(&self) -> Option<usize> {
        match self.value_area_kind {
            ValueAreaKind::High => self
                .row_idx
                .checked_add(1)
                .and_then(|res| Some(res as usize)),
            ValueAreaKind::Low => self
                .row_idx
                .checked_sub(1)
                .and_then(|res| Some(res as usize)),
        }
    }

    fn get_next_but_one_row_index(&self) -> Option<usize> {
        match self.value_area_kind {
            ValueAreaKind::High => self
                .row_idx
                .checked_add(2)
                .and_then(|res| Some(res as usize)),
            ValueAreaKind::Low => self
                .row_idx
                .checked_sub(2)
                .and_then(|res| Some(res as usize)),
        }
    }
}

fn get_row_idx_from_df(df: &DataFrame) -> u32 {
    df.column("row").unwrap().get(0).unwrap().unwrap_uint32()
}

fn get_row_index_from_df_row<'a>(row: &Vec<AnyValue<'a>>) -> u32 {
    row[0].unwrap_uint32()
}

fn get_price_from_df_row<'a>(row: &Vec<AnyValue<'a>>) -> f64 {
    row[1].unwrap_float64()
}

fn get_tpo_count_from_df_row<'a>(row: &Vec<AnyValue<'a>>) -> f64 {
    row[2].unwrap_float64()
}

impl PriceHistogram {
    pub fn new(df: DataFrame) -> Self {
        Self { df }
    }

    /// This function computes the POC for the given volume profile. The POC is the point of control. Hence,
    /// the price where the highest volume for a given time interval was traded.
    ///
    /// # Arguments
    /// * `df_vol` - volume profile
    pub fn poc(&self) -> f64 {
        let qx = VolumeProfileColumnKind::Quantity.to_string();
        let px = VolumeProfileColumnKind::Price.to_string();

        self.df
            .clone()
            .lazy()
            .select([col(&px).filter(col(&qx).eq(col(&qx).max()))])
            .collect()
            .unwrap()
            .get(0)
            .unwrap()[0]
            .unwrap_float64()
    }

    #[allow(dead_code)]
    /// Computes the initial balance described in <https://www.vtad.de/lexikon/market-profile/>
    pub fn initial_balance(&self) -> (f64, f64) {
        let first_candle = self.df.get(0).unwrap();
        let second_candle = self.df.get(1).unwrap();
        let start = self.initial_balance_start_price(&first_candle, &second_candle);
        let end = self.initial_balance_end_price(&first_candle, &second_candle);

        (start, end)
    }

    /// Computes the volume area described in <https://www.vtad.de/lexikon/market-profile/>
    /// # Arguments
    /// * `std_dev` - standard deviation
    pub fn value_area(&self, std_dev: f64) -> (f64, f64) {
        let (poc, poc_vol) = self.get_poc_with_vol_tuple();
        let total_tpo_count = self.get_total_tpo_count();
        let initial_value_area = ValueArea::new(&self.df, poc);
        let initial_tpo_count = total_tpo_count * std_dev - poc_vol;
        self.compute_value_area(initial_tpo_count, initial_value_area)
    }

    fn compute_value_area(&self, total_tpo_count: f64, va: ValueArea) -> (f64, f64) {
        if total_tpo_count <= 0.0 {
            return (va.low.price, va.high.price);
        }

        let va_with_updated_parts = va.update_value_area_parts();
        let tpo_delta = va_with_updated_parts
            .determine_tpo_delta(&va)
            .map_or_else(|| total_tpo_count, identity);
        let new_va = va.update_value_area_part(&va_with_updated_parts);

        self.compute_value_area(total_tpo_count - tpo_delta, new_va)
    }

    fn get_poc_with_vol_tuple(&self) -> (f64, f64) {
        let qx = VolumeProfileColumnKind::Quantity.to_string();

        let row = self
            .df
            .clone()
            .lazy()
            .filter(col(&qx).eq(col(&qx).max()))
            .collect()
            .unwrap();
        let row = row.get(0).unwrap();

        let poc = row[0].unwrap_float64();
        let poc_vol = row[1].unwrap_float64();
        (poc, poc_vol)
    }

    fn get_total_tpo_count(&self) -> f64 {
        let qx_col = self
            .df
            .find_idx_by_name(VolumeProfileColumnKind::Quantity.to_string().as_str())
            .unwrap();
        self.df.get_columns()[qx_col].sum().unwrap()
    }

    fn initial_balance_start_price<'a>(
        &self,
        first_candle: &Vec<AnyValue<'a>>,
        second_candle: &Vec<AnyValue<'a>>,
    ) -> f64 {
        let col_name_low = DataProviderColumnKind::Low.to_string();
        let idx = self.df.find_idx_by_name(&col_name_low).unwrap();
        let first_low = first_candle[idx].unwrap_float64();
        let second_low = second_candle[idx].unwrap_float64();
        f64::min(first_low, second_low)
    }

    fn initial_balance_end_price<'a>(
        &self,
        first_candle: &Vec<AnyValue<'a>>,
        second_candle: &Vec<AnyValue<'a>>,
    ) -> f64 {
        let col_name_high = DataProviderColumnKind::High.to_string();
        let idx = self.df.find_idx_by_name(&col_name_high).unwrap();
        let first_high = first_candle[idx].unwrap_float64();
        let second_high = second_candle[idx].unwrap_float64();
        f64::max(first_high, second_high)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        bot::time_frame_snapshot::TimeFrameSnapshotBuilder,
        cloud_api::api_for_unit_tests::{download_df, download_df_map},
    };
    use polars::{df, prelude::NamedFrom};

    #[tokio::test]
    async fn test_poc() {
        let df_map = download_df_map(
            "ppp/btcusdt/2022/Mon1h0m-Fri23h0m/1w/target_vol-aggTrades.json".to_string(),
        )
        .await;

        let mut snapshot = TimeFrameSnapshotBuilder::new(12).build();
        let mut df = df_map.get(&snapshot).unwrap().clone();
        assert_eq!(42000.0, PriceHistogram { df }.poc());

        snapshot = TimeFrameSnapshotBuilder::new(8).build();
        df = df_map.get(&snapshot).unwrap().clone();
        assert_eq!(38100.0, PriceHistogram { df }.poc());

        snapshot = TimeFrameSnapshotBuilder::new(9).build();
        df = df_map.get(&snapshot).unwrap().clone();
        assert_eq!(42100.0, PriceHistogram { df }.poc());

        snapshot = TimeFrameSnapshotBuilder::new(10).build();
        df = df_map.get(&snapshot).unwrap().clone();
        assert_eq!(42200.0, PriceHistogram { df }.poc());

        df = df!(
            "px" => &[1.0, 2.0, 3.0, 4.0],
            "qx" => &[10, 10, 9, 10]
        )
        .unwrap();
        assert_eq!(1.0, PriceHistogram { df }.poc());

        df = df!(
            "px" => &[ 83_200.0, 38_100.0, 38_000.0, 1.0],
            "qx" => &[100.0, 300.0, 150.0, 300.0],
        )
        .unwrap();
        assert_eq!(38_100.0, PriceHistogram { df }.poc());
    }

    #[tokio::test]
    async fn test_initial_balance() {
        let df = download_df(
            "chapaty-ai-hdb-test".to_string(),
            "cme/ohlc/ohlc_data_for_tpo_test.csv".to_string(),
        )
        .await;

        let initial_balance = PriceHistogram { df }.initial_balance();
        assert_eq!((1.16095, 1.16275), initial_balance);
    }

    #[test]
    fn test_poc_computations() {
        let df = df!(
            "px" => &[0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
            "qx" => &[0.0, 10.0, 20.0, 30.0, 40.0, 50.0, 20.0, 15.0, 10.0, 5.0, 0.0],
        )
        .unwrap();
        let ph = PriceHistogram { df };

        assert_eq!(ph.poc(), ph.get_poc_with_vol_tuple().0);
    }

    #[test]
    fn test_compute_value_area() {
        let df = df!(
            "px" => &[0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
            "qx" => &[0.0, 10.0, 20.0, 30.0, 40.0, 50.0, 20.0, 15.0, 10.0, 5.0, 0.0],
        )
        .unwrap();
        assert_eq!((3.0, 5.0), PriceHistogram { df }.value_area(0.3));
    }

    /// This test computes the value area from the example given in <https://www.vtad.de/lexikon/market-profile/> in the section
    /// `Berechnung der Value Area`
    #[test]
    fn test_compute_value_area_from_vtad() {
        let px: Vec<_> = (0..=200)
            .step_by(10)
            .map(|x| 5000.0 + f64::try_from(x).unwrap())
            .collect();
        let df = df!(
            "px" => &px,
            "qx" => &[0.0, 0.0, 1.0, 1.0, 1.0, 2.0, 2.0, 4.0, 4.0, 4.0, 5.0, 7.0, 8.0, 6.0, 5.0, 5.0, 3.0, 3.0, 1.0, 1.0, 0.0],
        )
        .unwrap();
        assert_eq!((5080.0, 5160.0), PriceHistogram { df }.value_area(0.68));
    }

    #[tokio::test]
    async fn test_compute_value_area_from_test_file() {
        let df = download_df(
            "chapaty-ai-test".to_string(),
            "ppp/_test_data_files/target_ohlc_tpo_for_tpo_test.csv".to_string(),
        )
        .await;
        assert_eq!((1.15195, 1.15845), PriceHistogram { df }.value_area(0.68))
    }
}
