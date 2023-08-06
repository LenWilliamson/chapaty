use super::closures::{get_cw_from_ts, get_weekday_from_ts};
use crate::{
    bot::time_interval::{InInterval, TimeInterval},
    converter::any_value::AnyValueConverter,
    data_frame_operations::trait_extensions::MyDataFrameOperations,
    enums::{bot::TimeFrameKind, column_names::DataProviderColumnKind},
};
use polars::{
    lazy::dsl::GetOutput,
    prelude::{col, lit, DataFrame, LazyCsvReader, LazyFileListReader, LazyFrame},
};
use std::path::PathBuf;

pub trait MyLazyFrameOperations {
    fn add_cw_col(self, ts_col: &str) -> Self;
    fn add_weekday_col(self, ts_col: &str) -> Self;
    fn add_hour_col(self, ts_col: &str) -> Self;
    fn add_min_col(self, ts_col: &str) -> Self;
    fn filter_ts_col_by_time_interval(
        self,
        ts_col: &str,
        time_interval: TimeInterval,
        time_frame: TimeFrameKind,
    ) -> Self;
    fn filter_ts_col_by_price(self, px: f64) -> Self;
    fn drop_rows_before_entry_ts(self, entry_ts: i64) -> Self;
    fn filter_trade_data_kind_values(self) -> Self;
    fn find_timestamp_when_price_reached(self, px: f64) -> Option<i64>;
    fn get_row_of_poc_as_df(self, poc: f64) -> DataFrame;
}

impl MyLazyFrameOperations for LazyFrame {
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
        time_frame: TimeFrameKind,
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
        let high = DataProviderColumnKind::High.to_string();
        let low = DataProviderColumnKind::Low.to_string();
        let ots = DataProviderColumnKind::OpenTime.to_string();
        self.select([col(&ots).filter(col(&low).lt_eq(lit(px)).and(col(&high).gt_eq(lit(px))))])
    }

    fn drop_rows_before_entry_ts(self, entry_ts: i64) -> Self {
        let col_name = DataProviderColumnKind::OpenTime.to_string();
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
        let ots = DataProviderColumnKind::OpenTime.to_string();
        let high = DataProviderColumnKind::High.to_string();
        let low = DataProviderColumnKind::Low.to_string();
        let close = DataProviderColumnKind::Close.to_string();
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
        if df.is_not_an_empty_frame() {
            Some(df.get(0).unwrap()[0].unwrap_int64())
        } else {
            None
        }
    }

    fn get_row_of_poc_as_df(self, poc: f64) -> DataFrame {
        self.filter(col("px").eq(lit(poc)))
            .select(&[col("*")])
            .collect()
            .unwrap()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cloud_api::api_for_unit_tests::download_df;
    use polars::prelude::IntoLazy;

    #[tokio::test]
    async fn test_compute_cw_and_weekday_col() {
        let df = download_df(
            "chapaty-ai-hdb-test".to_string(),
            "binance/ohlcv/test_file_compute_cw_and_weekday_col.csv".to_string(),
        )
        .await;
        let target_df = download_df(
            "chapaty-ai-hdb-test".to_string(),
            "binance/ohlcv/target_file_compute_cw_and_weekday_col.csv".to_string(),
        )
        .await;

        let res = df
            .lazy()
            .add_cw_col("ots")
            .add_weekday_col("ots")
            .collect()
            .unwrap();
        assert_eq!(target_df, res);
    }

    #[tokio::test]
    async fn test_find_timestamp_when_price_reached() {
        let ldf = download_df(
            "chapaty-ai-test".to_string(),
            "ppp/_test_data_files/pre_trade_data.csv".to_string(),
        )
        .await
        .lazy();

        let target_ts_taken = 1646085600000_i64;
        let px_taken = 42_000.0;
        let px_not_taken = 0.0;

        match ldf.clone().find_timestamp_when_price_reached(px_taken) {
            Some(result_taken) => assert_eq!(result_taken, target_ts_taken),
            None => assert!(false),
        };

        match ldf.find_timestamp_when_price_reached(px_not_taken) {
            Some(_) => assert!(false),
            None => assert!(true),
        };
    }
}
