use strum_macros::{Display, EnumString};
use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Debug, Display, EnumString, Serialize, Deserialize)]
pub enum StrategyKind {
    #[strum(serialize = "magneto")]
    Magneto,
    #[strum(serialize = "ppp")]
    Ppp,
}

#[derive(Copy, Clone, Debug, Display, EnumString)]
pub enum DataProviderKind {
    #[strum(serialize = "binance")]
    Binance,
    #[strum(serialize = "cme")]
    Cme,
}

#[derive(Copy, Clone, Debug, EnumString, Display)]
pub enum TimeFrameKind {
    #[strum(serialize = "1w")]
    Weekly,
    #[strum(serialize = "1d")]
    Daily,
}

#[derive(Debug, Copy, Clone,Display, EnumString)]
pub enum StopLossKind {
    #[strum(serialize = "PriceUponTradeEntry")]
    PriceUponTradeEntry,
    #[strum(serialize = "PrevLow")]
    PrevLow,
    #[strum(serialize = "PrevHigh")]
    PrevHigh,
}

#[derive(Debug, Copy, Clone, EnumString)]
pub enum TakeProfitKind {
    #[strum(serialize = "PrevClose")]
    PrevClose,
    #[strum(serialize = "PriceUponTradeEntry")]
    PriceUponTradeEntry,
    #[strum(serialize = "PrevLow")]
    PrevLow,
    #[strum(serialize = "PrevHigh")]
    PrevHigh,
}