use strum_macros::Display;

use super::news::NewsKind;

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum TradeDataKind {
    EntryPrice,
    EntryTimestamp,
    LastTradePrice,
    LowestTradePriceSinceEntry,
    LowestTradePriceSinceEntryTimestamp,
    HighestTradePriceSinceEntry,
    HighestTradePriceSinceEntryTimestamp,
    InitialBalance,
}
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub enum PreTradeDataKind {
    LastTradePrice,
    LowestTradePrice,
    HighestTradePrice,

    /// The news to trade, where
    /// * `NewsKind` - determines which news
    /// * `i32` - sets the number of `N` candles to wait
    News(NewsKind, i32),
}

#[derive(Debug, Copy, Clone, Display, PartialEq)]
pub enum TradeDirectionKind {
    Long,
    Short,

    #[strum(serialize = "Not Clear")]
    None,
}
