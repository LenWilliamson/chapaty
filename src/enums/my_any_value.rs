
#[derive(Debug, Clone)]
pub enum MyAnyValueKind {
    Int64(i64),
    UInt32(u32),
    Float64(f64),
    Utf8(String),
    Null,
}

impl MyAnyValueKind {
    pub fn unwrap_float64(self) -> f64 {
        match self {
            MyAnyValueKind::Float64(x) => x,
            MyAnyValueKind::Null => panic!("Matching against NULL value"),
            _ => panic!("Matching against wrong value"),
        }
    }

    pub fn unwrap_uint32(self) -> u32 {
        match self {
            MyAnyValueKind::UInt32(x) => x,
            MyAnyValueKind::Null => panic!("Matching against NULL value"),
            _ => panic!("Matching against wrong value"),
        }
    }

    pub fn unwrap_int64(self) -> i64 {
        match self {
            MyAnyValueKind::Int64(x) => x,
            MyAnyValueKind::Null => panic!("Matching against NULL value"),
            _ => panic!("Matching against wrong value"),
        }
    }

    pub fn unwrap_utf8(self) -> String {
        match self {
            MyAnyValueKind::Utf8(x) => x,
            MyAnyValueKind::Null => panic!("Matching against NULL value"),
            _ => panic!("Matching against wrong value"),
        }
    }
}