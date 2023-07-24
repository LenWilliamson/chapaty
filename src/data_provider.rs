pub mod binance;
pub mod cme;
use crate::enums::{
    self,
    data::HdbSourceDir,
    producers::ProducerKind, column_names::DataProviderColumns,
};

use polars::prelude::{DataFrame, DataType, Field, Schema};
use std::str::FromStr;

pub trait DataProvider {
    fn get_data_producer_kind(&self) -> ProducerKind;
    fn schema(&self, data: &HdbSourceDir) -> Schema;
    fn column_name_as_int(&self, col: &DataProviderColumns) -> usize;
    fn get_df(&self, df_as_bytes: Vec<u8>, data: &HdbSourceDir) -> DataFrame;
}
