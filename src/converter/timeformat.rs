use chrono::{DateTime, NaiveDate};

pub fn timestamp_in_milli_to_string(ts: i64) -> String {
    DateTime::from_timestamp(ts / 1000, 0)
        .unwrap()
        .format("%Y-%m-%d %H:%M:%S")
        .to_string()
}

// pub fn naive_date_time_with_from_str(ts: &str, format: &str) -> NaiveDateTime {
//     NaiveDateTime::parse_from_str(ts, format)
//         .unwrap()
// }

pub fn naive_date_from_str(date: &str, format: &str) -> NaiveDate {
    NaiveDate::parse_from_str(date, format).unwrap()
}
