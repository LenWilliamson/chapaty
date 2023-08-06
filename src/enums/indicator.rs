#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub enum TradingIndicatorKind {
    Poc(PriceHistogramKind),
    ValueAreaLow(PriceHistogramKind),
    ValueAreaHigh(PriceHistogramKind),
}

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub enum PriceHistogramKind {
    Tpo1m,
    Tpo1h,
    VolTick,
    VolAggTrades,
}
