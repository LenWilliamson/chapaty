pub mod binance;
pub mod cme;
use crate::enums::{self, data::HdbSourceDirKind};
use polars::prelude::{DataFrame, DataType, Field, Schema};
use std::str::FromStr;

pub struct BytesToDataFrameRequest {
    pub df_as_bytes: Vec<u8>,
    pub bytes_source_dir: HdbSourceDirKind,
}

pub trait DataProvider {
    fn get_name(&self) -> String;
    fn get_df_from_bytes(&self, bytes_to_data_frame_request: BytesToDataFrameRequest) -> DataFrame;
}
