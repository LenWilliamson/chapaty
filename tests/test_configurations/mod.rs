pub mod bot_config;
pub mod test_result;
pub mod test_runner;

use chapaty::DataFrame;
use polars::{io::SerReader, prelude::CsvReadOptions};

pub fn get_expected_result(file_name: &str) -> DataFrame {
    CsvReadOptions::default()
        .with_has_header(true)
        .try_into_reader_with_file_path(Some(file_name.into()))
        .unwrap()
        .finish()
        .unwrap()
}
