pub mod types {
    use std::collections::HashMap;

    use polars::prelude::DataFrame;

    use crate::bot::time_frame_snapshot::TimeFrameSnapshot;

    pub type DataFrameMap = HashMap<TimeFrameSnapshot, DataFrame>;
}
