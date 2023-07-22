use polars::prelude::DataFrame;


pub fn is_not_an_empty_frame(df: &DataFrame) -> bool {
    let (number_of_rows, _) = df.shape();
    number_of_rows > 0
}