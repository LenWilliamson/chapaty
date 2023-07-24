pub mod binance;
pub mod cme;
use crate::enums::{
    self,
    data::LeafDir,
    producers::ProducerKind, column_names::DataProviderColumns,
};

use polars::prelude::{DataFrame, DataType, Field, Schema};
use std::str::FromStr;

pub trait DataProvider {

    fn get_data_producer_kind(&self) -> ProducerKind;
    fn schema(&self, data: &LeafDir) -> Schema;
    fn delimiter(&self) -> u8;
    fn column_name_as_int(&self, col: &DataProviderColumns) -> usize;
    fn get_ts_col_as_str(&self, data: &LeafDir) -> String;
    fn get_df(&self, df_as_bytes: Vec<u8>, data: &LeafDir) -> DataFrame;
}
