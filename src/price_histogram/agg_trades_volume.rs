
use crate::{
    chapaty,
 lazy_frame_operations::closures::round, enums::column_names::{DataProviderColumnKind, VolumeProfileColumnKind},
};

use polars::{
    lazy::dsl::GetOutput,
    prelude::{col, DataFrame, IntoLazy},
};
use rayon::{iter::ParallelIterator, prelude::IntoParallelIterator};

pub struct AggTradesVolume {

}

impl AggTradesVolume {
    pub fn new() -> Self {
        Self {  }
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
        let px_vol =  VolumeProfileColumnKind::Price.to_string();
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