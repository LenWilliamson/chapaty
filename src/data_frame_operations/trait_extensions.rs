use polars::prelude::{DataFrame, IntoLazy, LazyFrame};

pub trait MyDataFrameOperations {
    fn is_not_an_empty_frame(&self) -> bool;
    fn df_with_row_count(&self, name: &str, offset: Option<u32>) -> DataFrame;
}

pub trait IntoLazyVec {
    fn lazy(self) -> Vec<LazyFrame>;
}

impl MyDataFrameOperations for DataFrame {
    fn is_not_an_empty_frame(&self) -> bool {
        let (number_of_rows, _) = self.shape();
        number_of_rows > 0
    }

    fn df_with_row_count(&self, name: &str, offset: Option<u32>) -> DataFrame {
        self.with_row_index(name.into(), offset).unwrap()
    }
}

impl IntoLazyVec for Vec<DataFrame> {
    fn lazy(self) -> Vec<LazyFrame> {
        self.into_iter().map(|df| df.lazy()).collect()
    }
}
