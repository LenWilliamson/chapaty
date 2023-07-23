use polars::prelude::DataFrame;

use crate::{bot::time_frame_snapshot::TimeFrameSnapshot, chapaty};

pub fn serialize_data_frame_map(df_map: &chapaty::types::DataFrameMap) -> String {
    let df_map_as_vec: Vec<(&TimeFrameSnapshot, &DataFrame)> = df_map.iter().collect();
    serde_json::to_string(&df_map_as_vec).unwrap()
}
