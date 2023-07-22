use crate::chapaty;
use std::str::from_utf8;

pub fn deserialize_data_frame_map(bytes: Vec<u8>) -> chapaty::types::DataFrameMap {
    let df_map_as_str = from_utf8(&bytes).expect("DataFrameMap is not valid UTF-8");
    serde_json::from_str(&df_map_as_str).unwrap()
}
