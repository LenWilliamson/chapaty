use polars::prelude::AnyValue;

/// Unwrap an polars `AnyValue` to a `f64` type.
pub fn unwrap_float64(v: &AnyValue) -> f64 {
    match v {
        AnyValue::Float64(x) => *x,
        AnyValue::Null => panic!("Matching against NULL value"),
        _ => panic!("Matching against wrong value"),
    }
}

/// Unwrap an polars `AnyValue` to a `u32` type.
pub fn unwrap_uint32(v: &AnyValue) -> u32 {
    match v {
        AnyValue::UInt32(x) => *x,
        AnyValue::Null => panic!("Matching against NULL value"),
        _ => panic!("Matching against wrong value"),
    }
}

/// Unwrap an polars `AnyValue` to an `i64` type.
pub fn unwrap_int64(v: &AnyValue) -> i64 {
    match v {
        AnyValue::Int64(x) => *x,
        AnyValue::Null => panic!("Matching against NULL value"),
        _ => panic!("Matching against wrong value"),
    }
}