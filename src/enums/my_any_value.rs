use crate::trading_indicator::initial_balance::InitialBalance;



#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MyAnyValueKind {
    Int64(i64),
    Float64(f64),
    InitialBalance(InitialBalance)
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

    pub fn unwrap_initial_balance(self) -> InitialBalance {
        match self {
            MyAnyValueKind::InitialBalance(ib) => ib,
            _ => panic!("Matching against wrong value"),
        }
    }
}
