pub mod deserialize;
pub mod serialize;

#[cfg(test)]
mod tests {

    use std::collections::HashMap;

    use polars::{df, prelude::NamedFrom};

    use crate::bot::time_frame_snapshot::TimeFrameSnapshotBuilder;

    use super::{serialize::serialize_data_frame_map, deserialize::deserialize_data_frame_map};

    #[test]
    fn serde_test() {
        let time_frame_snapshot = TimeFrameSnapshotBuilder::new(1).with_weekday(1).build();
        let df = df!(
            "a" => [1, 2],
            "b" => [1.0, 2.0]
        );

        let df_map = HashMap::from([(time_frame_snapshot, df.unwrap())]);

        let ser = serialize_data_frame_map(&df_map);
        let bytes: Vec<u8> = ser.into_bytes();
        let des = deserialize_data_frame_map(bytes);

        assert_eq!(df_map, des);
    }
}
