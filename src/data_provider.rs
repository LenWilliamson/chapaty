pub mod binance;
pub mod cme;
use crate::enums::{
    self, bot::DataProviderKind, data::HdbSourceDirKind,
};
use polars::prelude::{DataFrame, DataType, Field, Schema};
use std::str::FromStr;

pub trait DataProvider {
    fn get_data_producer_kind(&self) -> DataProviderKind;
    fn get_df(&self, df_as_bytes: Vec<u8>, data: &HdbSourceDirKind) -> DataFrame;
}
