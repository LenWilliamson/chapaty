use polars::prelude::AnyValue;

pub trait AnyValueConverter {
    /// Unwrap an polars `AnyValue` to a `f64` type.
    fn unwrap_float64(self) -> f64;
    /// Unwrap an polars `AnyValue` to a `u32` type.
    fn unwrap_uint32(self) -> u32;
    /// Unwrap an polars `AnyValue` to an `i64` type.
    fn unwrap_int64(self) -> i64;
    /// Unwrap an polars `AnyValue` to a String.
    fn unwrap_utf8(self) -> String;
}

impl AnyValueConverter for &AnyValue<'_> {
    fn unwrap_float64(self) -> f64 {
        match self {
            AnyValue::Float64(x) => *x,
            AnyValue::Null => panic!("Matching against NULL value"),
            _ => panic!("Matching against wrong value"),
        }
    }

    fn unwrap_uint32(self) -> u32 {
        match self {
            AnyValue::UInt32(x) => *x,
            AnyValue::Null => panic!("Matching against NULL value"),
            _ => panic!("Matching against wrong value"),
        }
    }

    fn unwrap_int64(self) -> i64 {
        match self {
            AnyValue::Int64(x) => *x,
            AnyValue::Null => panic!("Matching against NULL value"),
            _ => panic!("Matching against wrong value"),
        }
    }

    fn unwrap_utf8(self) -> String {
        match self {
            AnyValue::Utf8(x) => x.to_string(),
            AnyValue::Null => panic!("Matching against NULL value"),
            _ => panic!("Matching against wrong value"),
        }
    }
}
