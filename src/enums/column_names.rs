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
    Uid = 0,
    Id = 1,
    CalendarWeek = 2,
    Date = 3,
    Strategy = 4,
    Market = 5,
    TradeDirection = 6,
    Entry = 7,
    TakeProfit = 8,
    StopLoss = 9,
    ExpectedWinTick = 10,
    ExpectedLossTick = 11,
    ExpectedWinDollar = 12,
    ExpectedLossDollar = 13,
    Crv = 14,
    EntryTimestamp = 15,
    TakeProfitTimestamp = 16,
    StopLossTimestamp = 17,
    ExitPrice = 18,
    Status = 19,
    PlTick = 20,
    PlDollar = 21,
}

#[derive(Copy, Clone, Debug, Display)]
pub enum PerformanceReportColumnKind {
    Id = 0,
    Year = 1,
    Strategy = 2,
    Market = 3,
    NetProfit = 4,
    AvgWinnByTrade = 5,
    MaxDrawDownAbs = 6,
    MaxDrawDownRel = 7,
    PercentageProfitability = 8,
    RatioAvgWinByAvgLoss = 9,
    AvgWin = 10,
    AvgLoss = 11,
    ProfitFactor = 12,
}

#[derive(Copy, Clone, Debug, Display)]
pub enum TradeBreakDownReportColumnKind {
    Id = 0,
    Year = 1,
    Strategy = 2,
    Market = 3,
    TotalWin = 4,
    TotalLoss = 5,
    CleanWin = 6,
    TimeoutWin = 7,
    CleanLoss = 8,
    TimeoutLoss = 9,
    TotalNumberWinnerTrades = 10,
    TotalNumberLoserTrades = 11,
    TotalNumberTrades = 12,
    NumberWinnerTrades = 13,
    NumberLoserTrades = 14,
    NumberTimeoutWinnerTrades = 15,
    NumberTimeoutLoserTrades = 16,
    NumberTimeoutTrades = 17,
    NumberNoEntry = 18,
}
