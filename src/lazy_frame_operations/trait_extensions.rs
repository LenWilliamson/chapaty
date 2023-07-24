use std::path::PathBuf;

use polars::{
    lazy::dsl::GetOutput,
    prelude::{col, lit, DataFrame, LazyCsvReader, LazyFileListReader, LazyFrame},
};

use crate::{
    bot::time_interval::{InInterval, TimeInterval},
    converter::any_value::AnyValueConverter,
    data_frame_operations::is_not_an_empty_frame,
    enums::{
        bots::StrategyKind,
        column_names::{DataProviderColumns, PerformanceReport},
        markets::TimeFrame,
    },
};

use super::closures::{get_cw_from_ts, get_weekday_from_ts};

pub trait MyLazyFrameOperations {
    fn add_cw_col(self, ts_col: &str) -> Self;
    fn add_weekday_col(self, ts_col: &str) -> Self;
    fn add_hour_col(self, ts_col: &str) -> Self;
    fn add_min_col(self, ts_col: &str) -> Self;
    fn append_strategy_col(self, strategy: StrategyKind) -> Self;
    fn filter_ts_col_by_time_interval(
        self,
        ts_col: &str,
        time_interval: TimeInterval,
        time_frame: TimeFrame,
    ) -> Self;
    fn filter_ts_col_by_price(self, px: f64) -> Self;
    fn drop_rows_before_entry_ts(self, entry_ts: i64) -> Self;
    fn filter_trade_data_kind_values(self) -> Self;
    fn find_timestamp_when_price_reached(self, px: f64) -> Option<i64>;
}

impl MyLazyFrameOperations for LazyFrame {
    // TODO rausnehmen
    fn append_strategy_col(self, strategy: StrategyKind) -> Self {
        let name = &PerformanceReport::Strategy.to_string();
        self.with_column(lit(strategy.to_string()).alias(name))
    }

    fn add_cw_col(self, ts_col: &str) -> Self {
        self.with_column(
            col(&ts_col)
                .apply(|x| Ok(Some(get_cw_from_ts(&x))), GetOutput::default())
                .alias("cw"),
        )
    }
    fn add_weekday_col(self, ts_col: &str) -> Self {
        self.with_column(
            col(&ts_col)
                .apply(|x| Ok(Some(get_weekday_from_ts(&x))), GetOutput::default())
                .alias("weekday"),
        )
    }
    fn add_hour_col(self, _ts_col: &str) -> Self {
        // self.with_column(
        //     col(&ts_col)
        //         .apply(|x| Ok(Some(get_hour_from_ts(&x))), GetOutput::default())
        //         .alias("hour"),
        // )
        self
    }
    fn add_min_col(self, _ts_col: &str) -> Self {
        // self.with_column(
        //     col(&ts_col)
        //         .apply(|x| Ok(Some(get_min_from_ts(&x))), GetOutput::default())
        //         .alias("min"),
        // )
        self
    }

    fn filter_ts_col_by_time_interval(
        self,
        ts_col: &str,
        time_interval: TimeInterval,
        time_frame: TimeFrame,
    ) -> Self {
        self.with_column(
            col(&ts_col)
                .apply(
                    move |x| Ok(Some(time_interval.in_time_interval(&x, &time_frame))),
                    GetOutput::default(),
                )
                .alias("in_interval"),
        )
        .filter(col("in_interval").eq(lit(true)))
        .select([col("*")])
    }

    /// # Returns
    /// This function returns a `DaraFrame` with a single column, the `timestamp` column
    fn filter_ts_col_by_price(self, px: f64) -> Self {
        let high = DataProviderColumns::High.to_string();
        let low = DataProviderColumns::Low.to_string();
        let ots = DataProviderColumns::OpenTime.to_string();
        self.select([col(&ots).filter(col(&low).lt_eq(lit(px)).and(col(&high).gt_eq(lit(px))))])
    }

    fn drop_rows_before_entry_ts(self, entry_ts: i64) -> Self {
        let col_name = DataProviderColumns::OpenTime.to_string();
        self.filter(col(&col_name).gt_eq(lit(entry_ts)))
    }

    /// # Returns
    /// This function returns a `DataFrame` with a single row, containing the following column values at index
    /// * Index 0: last trade price
    /// * Index 1: lowest trade price
    /// * Index 2: highest trade price
    /// * Index 3: timestamp highest trade price
    /// * Index 4: timestamp lowest trade price
    fn filter_trade_data_kind_values(self) -> Self {
        let ots = DataProviderColumns::OpenTime.to_string();
        let high = DataProviderColumns::High.to_string();
        let low = DataProviderColumns::Low.to_string();
        let close = DataProviderColumns::Close.to_string();
        self.select([
            col(&close).last(),
            col(&low).min(),
            col(&high).max(),
            col(&ots)
                .filter(col(&high).eq(col(&high).max()))
                .first()
                .alias("high_ts"),
            col(&ots)
                .filter(col(&low).eq(col(&low).min()))
                .first()
                .alias("low_ts"),
        ])
    }

    fn find_timestamp_when_price_reached(self, px: f64) -> Option<i64> {
        let df = self.filter_ts_col_by_price(px).first().collect().unwrap();
        if is_not_an_empty_frame(&df) {
            Some(df.get(0).unwrap()[0].unwrap_int64())
        } else {
            None
        }
    }
}

pub trait MyLazyFrameVecOperations {
    fn concatenate_to_data_frame(self) -> DataFrame;
    fn concatenate_to_lazy_frame(self) -> LazyFrame;
}

impl MyLazyFrameVecOperations for Vec<LazyFrame> {
    fn concatenate_to_data_frame(self) -> DataFrame {
        LazyCsvReader::new(PathBuf::from(""))
            .with_rechunk(true)
            .concat_impl(self)
            .unwrap()
            .collect()
            .unwrap()
    }
    fn concatenate_to_lazy_frame(self) -> LazyFrame {
        LazyCsvReader::new(PathBuf::from(""))
            .with_rechunk(true)
            .concat_impl(self)
            .unwrap()
    }
}
