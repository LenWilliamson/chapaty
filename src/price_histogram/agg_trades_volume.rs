use crate::{
    chapaty,
    enums::column_names::{DataProviderColumnKind, VolumeProfileColumnKind},
    lazy_frame_operations::closures::round,
};

use polars::{
    lazy::dsl::GetOutput,
    prelude::{col, DataFrame, IntoLazy},
};
use rayon::{iter::ParallelIterator, prelude::IntoParallelIterator};

pub struct AggTradesVolume {}

impl AggTradesVolume {
    pub fn new() -> Self {
        Self {}
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

    fn vol_profile(&self, df: DataFrame) -> DataFrame {
        let px = DataProviderColumnKind::Price.to_string();
        let qx = DataProviderColumnKind::Quantity.to_string();
        let px_vol = VolumeProfileColumnKind::Price.to_string();
        let qx_vol = VolumeProfileColumnKind::Quantity.to_string();

        df.lazy()
            .select([
                col(&px).apply(|x| Ok(Some(round(&x))), GetOutput::default()),
                col(&qx),
            ])
            .groupby([col(&px_vol)])
            .agg([col(&qx_vol).sum()])
            .sort(&px_vol, Default::default())
            .collect()
            .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use polars::{df, prelude::NamedFrom};

    /// This unit test asserts, if:
    /// * the `volume_profile` of a DataFrame is computed correctly
    /// * the result is sorted in ascending order
    /// * the values for the price columns are rounded
    #[tokio::test]
    async fn test_volume_profile() {
        let agg_trades_volume = AggTradesVolume {};
        let df = df!(
            "px" => &[2.49, 1.0, 1.4, 2.5, 3.1],
            "qx" => &[2.0, 1.0, 1.0,  3.0, 3.0],
        )
        .unwrap();

        // The target DataFrame is sorted in ascending order and the values for the price columns are rounded
        let target = df!(
            "px" => &[1.0, 2.0, 3.0],
            "qx" => &[2.0, 2.0, 6.0],
        )
        .unwrap();

        let result = agg_trades_volume.vol_profile(df);
        assert_eq!(result.frame_equal(&target), true)
    }
}
