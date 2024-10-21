#[derive(Clone, Copy, PartialEq, Debug)]
pub struct OhlcCandle {
    pub open_ts: Option<i64>,
    pub open: Option<f64>,
    pub high: Option<f64>,
    pub low: Option<f64>,
    pub close: Option<f64>,
    pub close_ts: Option<i64>,
}

impl OhlcCandle {
    pub fn new() -> Self {
        Self {
            open_ts: None,
            open: None,
            high: None,
            low: None,
            close: None,
            close_ts: None,
        }
    }

    pub fn get_last_trade_price(&self) -> Option<f64> {
        self.close
    }

    pub fn get_lowest_trade_price(&self) -> Option<f64> {
        self.low
    }

    pub fn get_highest_trade_price(&self) -> Option<f64> {
        self.high
    }

    pub fn with_open_ts(self, open_ts: i64) -> Self {
        Self {
            open_ts: Some(open_ts),
            ..self
        }
    }

    pub fn with_open(self, open: f64) -> Self {
        Self {
            open: Some(open),
            ..self
        }
    }

    pub fn with_high(self, high: f64) -> Self {
        Self {
            high: Some(high),
            ..self
        }
    }

    pub fn with_low(self, low: f64) -> Self {
        Self {
            low: Some(low),
            ..self
        }
    }

    pub fn with_close(self, close: f64) -> Self {
        Self {
            close: Some(close),
            ..self
        }
    }

    pub fn with_close_ts(self, close_ts: i64) -> Self {
        Self {
            close_ts: Some(close_ts),
            ..self
        }
    }
}
