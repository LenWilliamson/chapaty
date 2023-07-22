use crate::chapaty;

pub fn serialize_data_frame_map(df_map: &chapaty::types::DataFrameMap) -> String {
    serde_json::to_string(df_map).unwrap()
}
