use strum_macros::Display;

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum TradeDataKind {
    EntryPrice,
    EntryTimestamp,
    LastTradePrice,
    LowestTradePriceSinceEntry,
    LowestTradePriceSinceEntryTimestamp,
    HighestTradePriceSinceEntry,
    HighestTradePriceSinceEntryTimestamp,
}
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub enum PreTradeDataKind {
    LastTradePrice,
    LowestTradePrice,
    HighestTradePrice,
}

#[derive(Debug, Copy, Clone, Display, PartialEq)]
pub enum TradeDirectionKind {
    Long,
    Short,

    #[strum(serialize = "Not Clear")]
    None,
}
