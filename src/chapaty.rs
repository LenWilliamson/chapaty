pub mod types {
    use crate::bot::time_frame_snapshot::TimeFrameSnapshot;
    use polars::prelude::DataFrame;
    use std::collections::HashMap;

    pub type DataFrameMap = HashMap<TimeFrameSnapshot, DataFrame>;
}
