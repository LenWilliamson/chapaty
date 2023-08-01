#[derive(Debug, Clone, PartialEq)]
pub enum MyAnyValueKind {
    Int64(i64),
    Float64(f64),
}

impl MyAnyValueKind {
    pub fn unwrap_float64(self) -> f64 {
        match self {
            MyAnyValueKind::Float64(x) => x,
            _ => panic!("Matching against wrong value"),
        }
    }

    pub fn unwrap_int64(self) -> i64 {
        match self {
            MyAnyValueKind::Int64(x) => x,
            _ => panic!("Matching against wrong value"),
        }
    }
}
