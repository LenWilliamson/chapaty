use polars::prelude::DataFrame;

use crate::{chapaty, bot::time_frame_snapshot::TimeFrameSnapshot};
use std::str::from_utf8;

pub fn deserialize_data_frame_map(bytes: Vec<u8>) -> chapaty::types::DataFrameMap {
    let df_map_as_str = from_utf8(&bytes).expect("DataFrameMapVec is not valid UTF-8");
    let df_map_as_vec: Vec<(TimeFrameSnapshot, DataFrame)> = serde_json::from_str(&df_map_as_str).unwrap();
    df_map_as_vec.into_iter().collect()
}
