use polars::prelude::{CsvWriter, DataFrame, SerWriter};
use std::fs::File;

pub fn save_df_as_csv(df: &mut DataFrame, file_name: &str) {
    let mut file = File::create(format!("{file_name}.csv")).unwrap();
    CsvWriter::new(&mut file).finish(df).unwrap();
}