use strum_macros::{Display, EnumString};

#[derive(Copy, Clone, Debug, Display, EnumString)]
pub enum DataProviderColumnKind {
    #[strum(serialize = "ots")]
    OpenTime,
    #[strum(serialize = "open")]
    Open,
    #[strum(serialize = "high")]
    High,
    #[strum(serialize = "low")]
    Low,
    #[strum(serialize = "close")]
    Close,
    #[strum(serialize = "cts")]
    CloseTime,
    #[strum(serialize = "vol")]
    Volume,
    #[strum(serialize = "qav")]
    QuoteAssetVol,
    #[strum(serialize = "not")]
    NumberOfTrades,
    #[strum(serialize = "tbbav")]
    TakerBuyBaseAssetVol,
    #[strum(serialize = "tbqav")]
    TakerBuyQuoteAssetVol,
    #[strum(serialize = "ignore")]
    Ignore,
    #[strum(serialize = "atid")]
    AggTradeId,
    #[strum(serialize = "px")]
    Price,
    #[strum(serialize = "qx")]
    Quantity,
    #[strum(serialize = "ftid")]
    FirstTradeId,
    #[strum(serialize = "ltid")]
    LastTradeId,
    #[strum(serialize = "ts")]
    Timestamp,
    #[strum(serialize = "bm")]
    BuyerEqualsMaker,
    #[strum(serialize = "btpm")]
    BestTradePriceMatch,
}

#[derive(Copy, Clone, Debug, Display, EnumString)]
pub enum VolumeProfileColumnKind {
    #[strum(serialize = "px")]
    Price = 0,
    #[strum(serialize = "qx")]
    Quantity = 1,
}

#[derive(Copy, Clone, Debug, Display)]
pub enum PnLReportColumnKind {
    CalendarWeek = 0,
    Date = 1,
    Strategy = 2,
    Market = 3,
    TradeDirection = 4,
    Entry = 5,
    TakeProfit = 6,
    StopLoss = 7,
    ExpectedWinTick = 8,
    ExpectedLossTick = 9,
    ExpectedWinDollar = 10,
    ExpectedLossDollar = 11,
    Crv = 12,
    EntryTimestamp = 13,
    TakeProfitTimestamp = 14,
    StopLossTimestamp = 15,
    ExitPrice = 16,
    Status = 17,
    PlTick = 18,
    PlDollar = 19,
}

#[derive(Copy, Clone, Debug, Display)]
pub enum PerformanceReportColumnKind {
    Year = 0,
    Strategy = 1,
    Market = 2,
    NetProfit = 3,
    AvgWinnByTrade = 4,
    MaxDrawDownAbs = 5,
    MaxDrawDownRel = 6,
    PercentageProfitability = 7,
    RatioAvgWinByAvgLoss = 8,
    AvgWin = 9,
    AvgLoss = 10,
    ProfitFactor = 11,
}

#[derive(Copy, Clone, Debug, Display)]
pub enum TradeBreakDownReportColumnKind {
    Year = 0,
    Strategy = 1,
    Market = 2,
    TotalWin = 3,
    TotalLoss = 4,
    CleanWin = 5,
    TimeoutWin = 6,
    CleanLoss = 7,
    TimeoutLoss = 8,
    TotalNumberWinnerTrades = 9,
    TotalNumberLoserTrades = 10,
    TotalNumberTrades = 11,
    NumberWinnerTrades = 12,
    NumberLoserTrades = 13,
    NumberTimeoutWinnerTrades = 14,
    NumberTimeoutLoserTrades = 15,
    NumberTimeoutTrades = 16,
    NumberNoEntry = 17,
}
