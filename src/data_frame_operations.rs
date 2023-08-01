use polars::prelude::{CsvWriter, DataFrame, SerWriter};
use std::fs::File;

pub fn is_not_an_empty_frame(df: &DataFrame) -> bool {
    let (number_of_rows, _) = df.shape();
    number_of_rows > 0
}

pub fn save_df_as_csv(df: &mut DataFrame, file_name: &str) {
    let mut file = File::create(format!("{file_name}.csv")).unwrap();
    CsvWriter::new(&mut file).finish(df).unwrap();
}
