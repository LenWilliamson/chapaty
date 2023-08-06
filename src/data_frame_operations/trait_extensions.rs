use polars::prelude::DataFrame;

pub trait MyDataFrameOperations {
    fn is_not_an_empty_frame(&self) -> bool;
    fn df_with_row_count(&self, name: &str, offset: Option<u32>) -> DataFrame;
}

impl MyDataFrameOperations for DataFrame {
    
    fn is_not_an_empty_frame(&self) -> bool {
        let (number_of_rows, _) = self.shape();
        number_of_rows > 0
    }
    
    fn df_with_row_count(&self, name: &str, offset: Option<u32>) -> DataFrame {
        self.with_row_count(name, offset).unwrap()
    }
    
}