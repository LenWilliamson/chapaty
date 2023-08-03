#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub enum TradingIndicatorKind {
    Poc(PriceHistogramKind),
    VolumeAreaLow(PriceHistogramKind),
    VolumeAreaHigh(PriceHistogramKind),
}

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub enum PriceHistogramKind {
    Tpo1m,
    Tpo1h,
    VolTick,
    VolAggTrades,
}
