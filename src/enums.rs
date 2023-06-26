#![allow(dead_code)]

pub mod bots {
    use enum_map::Enum;
    use strum_macros::Display;

    #[derive(Copy, Clone, Debug, Display)]
    pub enum BotKind {
        Magneto,
        Ppp,
    }

    #[derive(Enum, Copy, Clone, Debug)]
    pub enum PreTradeDataKind {
        Poc,
        LastTradePrice,
        LowestTradePrice,
        HighestTradePrice,
        VolumeAreaLow,
        VolumeAreaHigh,
    }

    #[derive(Enum, Copy, Clone, Debug)]
    pub enum TradeDataKind {
        EntryPrice,
        EntryTimestamp,
        LastTradePrice,
        LowestTradePriceSinceEntry,
        LowestTradePriceSinceEntryTimestamp,
        HighestTradePriceSinceEntry,
        HighestTradePriceSinceEntryTimestamp,
    }
}

pub mod columns {
    use strum_macros::Display;

    #[derive(Copy, Clone, Debug)]
    pub enum Columns {
        Ohlcv(OhlcvColumnNames),
        Ohlc(OhlcColumnNames),
        AggTrade(AggTradeColumnNames),
        Vol(VolumeProfileColumnNames),
    }

    #[derive(Copy, Clone, Debug, Display)]
    pub enum PerformanceStatisticColumnNames {
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
        TotalWin = 12,
        TotalLoss = 13,
        CleanWin = 14,
        TimeoutWin = 15,
        CleanLoss = 16,
        TimeoutLoss = 17,
        TotalNumberWinnerTrades = 18,
        TotalNumberLoserTrades = 19,
        TotalNumberTrades = 20,
        NumberWinnerTrades = 21,
        NumberLoserTrades = 22,
        NumberTimeoutWinnerTrades = 23,
        NumberTimeoutLoserTrades = 24,
        NumberTimeoutTrades = 25,
        NumberNoEntry = 26,
    }

    #[derive(Copy, Clone, Debug, Display)]
    pub enum ProfitAndLossColumnNames {
        CalendarWeek = 0,
        Date = 1,
        Strategy = 2,
        Market = 3,
        TradeDirection = 4,
        Entry = 5,
        TakeProfit = 6,
        StopLoss = 7,
        ExpectedWinTik = 8,
        ExpectedLossTik = 9,
        ExpectedWinDollar = 10,
        ExpectedLossDollar = 11,
        Crv = 12,
        EntryTimestamp = 13,
        TargetTimestamp = 14,
        StopLossTimestamp = 15,
        ExitPrice = 16,
        Status = 17,
        PlTik = 18,
        PlDollar = 19,
    }

    #[derive(Copy, Clone, Debug)]
    pub enum OhlcvColumnNames {
        OpenTime = 0,
        Open = 1,
        High = 2,
        Low = 3,
        Close = 4,
        Volume = 5,
        CloseTime = 6,
        // QuoteAssetVol,
        // NumberOfTrades,
        // TakerBuyBaseAssetVol,
        // TakerBuyQuoteAssetVol,
        // Ignore,
    }

    #[derive(Copy, Clone, Debug)]
    pub enum OhlcColumnNames {
        OpenTime = 0,
        Open = 1,
        High = 2,
        Low = 3,
        Close = 4,
        // Volume = 5,
        CloseTime = 6,
        // QuoteAssetVol,
        // NumberOfTrades,
        // TakerBuyBaseAssetVol,
        // TakerBuyQuoteAssetVol,
        // Ignore,
    }

    #[derive(Copy, Clone, Debug)]
    pub enum AggTradeColumnNames {
        AggTradeId = 0,
        Price = 1,
        Quantity = 2,
        FirstTradeId = 3,
        LastTradeId = 4,
        Timestamp = 5,
        BuyerEqualsMaker = 6,
        BestTradePriceMatch = 7,
    }
    #[derive(Copy, Clone, Debug)]
    pub enum VolumeProfileColumnNames {
        Price = 0,
        Quantity = 1,
    }
}

pub mod data {
    use strum_macros::EnumString;
    #[derive(Copy, Clone, Debug, EnumString, PartialEq)]
    pub enum LeafDir {
        #[strum(serialize = "Ohlc1m")]
        Ohlc1m,

        #[strum(serialize = "Ohlc30m")]
        Ohlc30m,

        #[strum(serialize = "Ohlc60m")]
        Ohlc60m,

        #[strum(serialize = "Ohlcv1m")]
        Ohlcv1m,

        #[strum(serialize = "Ohlcv30m")]
        Ohlcv30m,

        #[strum(serialize = "Ohlcv60m")]
        Ohlcv60m,

        #[strum(serialize = "Tick")]
        Tick,

        #[strum(serialize = "AggTrades")]
        AggTrades,

        #[strum(serialize = "Vol")]
        Vol,

        #[strum(serialize = "ProfitAndLoss")]
        ProfitAndLoss,
    }

    #[derive(Copy, Clone, Debug)]
    pub enum RootDir {
        Data,
        Strategy,
    }
}

pub mod jobs {
    #[derive(Copy, Clone, Debug)]
    pub enum JobKind {
        Chart,
        Volume,
    }
}

pub mod markets {

    use strum_macros::{Display, EnumString};

    #[derive(Copy, Clone, Debug, Display, EnumString)]
    pub enum MarketKind {
        #[strum(serialize = "BtcUsdt")]
        BtcUsdt,

        #[strum(serialize = "6A", serialize = "AudUsd")]
        AudUsd,

        #[strum(serialize = "6E", serialize = "EurUsd")]
        EurUsd,

        #[strum(serialize = "6B", serialize = "GbpUsd")]
        GbpUsd,

        #[strum(serialize = "6C", serialize = "CadUsd")]
        CadUsd,

        #[strum(serialize = "6J", serialize = "YenUsd")]
        YenUsd,

        #[strum(serialize = "6N", serialize = "NzdUsd")]
        NzdUsd,

        #[strum(serialize = "6BTC", serialize = "BtcUsdFuture")]
        BtcUsdFuture,
    }

    impl MarketKind {
        /// Returns the number of decimal places for the `market`.
        ///
        /// # Arguments
        /// * `market` - where we want to know the number of decimal places
        ///
        /// # Example
        /// * `MarketKind::BtcUsdt` for Binance has two decimal digits for cents, e.g. `1258.33`
        /// * `MarketKind::EurUsd` for Ninja has five decimal digits for ticks, e.g. `1.39457`
        pub fn number_of_digits(&self) -> i32 {
            match self {
                MarketKind::BtcUsdt => 2,
                MarketKind::EurUsd => 5,
                MarketKind::AudUsd => 5,
                MarketKind::GbpUsd => 4,
                MarketKind::CadUsd => 5,
                MarketKind::YenUsd => 7,
                MarketKind::NzdUsd => 5,
                MarketKind::BtcUsdFuture => 0,
            }
        }

        /// This function returns the tik step size for a market that uses tiks as units. Otherwise we return `None`.
        ///
        /// # Arguments
        /// * `market` - we want to get tik step size
        ///
        /// # Examples
        /// ```
        /// // BtcUsdt does not use tiks as unit
        /// assert_eq!(tik_step(MarketKind::BtcUsdt).is_some(), false)
        /// // EurUsd uses tiks as unit
        /// assert_eq!(tik_step(MarketKind::EurUsd).is_some(), true)
        /// ```
        pub fn tik_step(&self) -> Option<f64> {
            match self {
                MarketKind::BtcUsdt => None,
                MarketKind::EurUsd => Some(0.00005),
                MarketKind::AudUsd => Some(0.00005),
                MarketKind::GbpUsd => Some(0.0001),
                MarketKind::CadUsd => Some(0.00005),
                MarketKind::YenUsd => Some(0.0000005),
                MarketKind::NzdUsd => Some(0.00005),
                MarketKind::BtcUsdFuture => Some(5.0),
            }
        }

        /// This function returns the tik to dollar conversion factor for a market that uses tiks as units. Otherwise we return `None`.
        ///
        /// # Arguments
        /// * `market` - we want to get tik step size
        ///
        /// # Examples
        /// ```
        /// // BtcUsdt does not use tiks as unit
        /// assert_eq!(tik_to_dollar_conversion_factor(MarketKind::BtcUsdt).is_some(), false)
        /// // EurUsd uses tiks as unit
        /// assert_eq!(tik_to_dollar_conversion_factor(MarketKind::EurUsd).is_some(), true)
        /// ```
        pub fn tik_to_dollar_conversion_factor(&self) -> Option<f64> {
            match self {
                MarketKind::BtcUsdt => None,
                MarketKind::EurUsd => Some(6.25),
                MarketKind::AudUsd => Some(5.0),
                MarketKind::GbpUsd => Some(6.25),
                MarketKind::CadUsd => Some(5.0),
                MarketKind::YenUsd => Some(6.25),
                MarketKind::NzdUsd => Some(5.0),
                MarketKind::BtcUsdFuture => Some(25.0),
            }
        }
    }

    #[derive(Copy, Clone, Debug, EnumString)]
    pub enum GranularityKind {
        #[strum(serialize = "Weekly", serialize = "weekly")]
        Weekly,
        #[strum(serialize = "Daily", serialize = "daily")]
        Daily,
    }
}
pub mod producers {
    #[derive(Copy, Clone, Debug)]
    pub enum ProducerKind {
        Binance,
        Test,
        Ninja,
    }
}
pub mod strategies {
    use strum_macros::EnumString;

    #[derive(Debug, Copy, Clone, EnumString)]
    pub enum StopLossKind {
        #[strum(serialize = "PrevPoc")]
        PrevPoc,
        #[strum(serialize = "PrevLow")]
        PrevLow,
        #[strum(serialize = "PrevHigh")]
        PrevHigh,
    }

    #[derive(Debug, Copy, Clone, EnumString)]
    pub enum TakeProfitKind {
        #[strum(serialize = "PrevClose")]
        PrevClose,
        #[strum(serialize = "PrevPoc")]
        PrevPoc,
    }
}
pub mod trades {
    use strum_macros::Display;

    #[derive(Debug, Copy, Clone, Display)]
    pub enum TradeKind {
        Long,
        Short,

        #[strum(serialize = "NotClear")]
        None,
    }
}

pub mod error {

    #[derive(Debug, Clone)]
    pub enum Error {
        ParseBotError(String),
        ParseDataProducerError(String),
    }
}
