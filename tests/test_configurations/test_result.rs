use chapaty::{backtest_result::BacktestResult, MarketKind};

pub struct TestResult {
    pub market: MarketKind,
    pub year: u32,
    pub backtest_result: BacktestResult,
}