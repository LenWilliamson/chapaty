// Internal crates
use crate::{
    chapaty,
    enums::columns::{AggTradeColumnNames, Columns, VolumeProfileColumnNames},
    data_provider::DataProvider, lazy_frame_operations::closures::round,
};

// External crates
use polars::{
    lazy::dsl::GetOutput,
    prelude::{col, DataFrame, IntoLazy},
};
use rayon::{iter::ParallelIterator, prelude::IntoParallelIterator};
use std::sync::Arc;

pub struct AggTradesVolume {
    data_provider: Arc<dyn DataProvider + Send + Sync>,
}

impl AggTradesVolume {
    pub fn new(data_provider: Arc<dyn DataProvider + Send + Sync>) -> Self {
        Self { data_provider }
    }

    pub fn from_df_map(
        &self,
        df_map: chapaty::types::DataFrameMap,
    ) -> chapaty::types::DataFrameMap {
        df_map
            .into_par_iter()
            .map(|(key, df)| (key, self.vol_profile(df)))
            .collect()
    }

    /// This function computes the volume profile for the given DataFrame and sorts it by price in ascending order.
    /// The values for the price columns are rounded.
    ///
    /// # Arguments
    /// * `dp` - producer of type `DataProducer`
    /// * `df` - the `DataFrame` we want to compute the volume profile for
    /// * `exact` - is `true` if we don't want to round floats to their closest integer values, otherwise `false`
    ///
    /// # Example
    /// Assume `px` and `qx` are some column names.
    /// Calling this function on the following DataFrame will result into the target.
    /// ```
    /// let df = df!(
    ///    &px => &[1.0, 1.4, 2.49, 2.5, 3.1],
    ///    &qx => &[1.0, 1.0,  2.0, 3.0, 3.0],
    /// );
    ///
    ///
    /// let target = df!(
    ///     &px => &[1.0, 2.0, 3.0],
    ///     &qx => &[2.0, 2.0, 6.0],
    /// );
    ///
    /// // Calling function to compute volume_profile
    /// let result = tick::volume_profile(df.unwrap()).unwrap();
    /// assert_eq!(result.frame_equal(&target.unwrap()), true)
    ///
    /// ```
    fn vol_profile(&self, df: DataFrame) -> DataFrame {
        let dp = self.data_provider.clone();
        let px = dp.column_name_as_str(&Columns::AggTrade(AggTradeColumnNames::Price));
        let qx = dp.column_name_as_str(&Columns::AggTrade(AggTradeColumnNames::Quantity));
        let px_vol = dp.column_name_as_str(&Columns::Vol(VolumeProfileColumnNames::Price));
        let qx_vol = dp.column_name_as_str(&Columns::Vol(VolumeProfileColumnNames::Quantity));

        df.lazy()
            .select([
                col(&px).apply(|x| Ok(Some(round(&x))), GetOutput::default()),
                col(&qx),
            ])
            .groupby([col(&px_vol)])
            .agg([col(&qx_vol).sum()])
            .collect()
            .unwrap()
    }
}