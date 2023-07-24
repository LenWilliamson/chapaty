pub mod column_names;

pub mod bots {
    use strum_macros::{Display, EnumString};

    #[derive(Copy, Clone, Debug, Display, EnumString)]
    pub enum StrategyKind {
        #[strum(serialize = "magneto")]
        Magneto,
        #[strum(serialize = "ppp")]
        Ppp,
    }

    #[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
    pub enum TradingIndicatorKind {
        Poc(PriceHistogram),
        VolumeAreaLow(PriceHistogram),
        VolumeAreaHigh(PriceHistogram),
    }

    #[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
    pub enum PreTradeDataKind {
        LastTradePrice,
        LowestTradePrice,
        HighestTradePrice,
    }

    #[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
    pub enum PriceHistogram {
        Tpo1m,
        VolTick,
        VolAggTrades,
    }

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
}

pub mod data {
    use strum_macros::{Display, EnumString};

    use super::bots::{PriceHistogram, TradingIndicatorKind};

    #[derive(Copy, Clone, Debug, EnumString, PartialEq, Display)]
    pub enum CandlestickKind {
        #[strum(serialize = "ohlc-1m")]
        Ohlc1m,

        #[strum(serialize = "ohlc-30m")]
        Ohlc30m,

        #[strum(serialize = "ohlc-1h")]
        Ohlc1h,

        #[strum(serialize = "ohlcv-1m")]
        Ohlcv1m,

        #[strum(serialize = "ohlcv-30m")]
        Ohlcv30m,

        #[strum(serialize = "ohlcv-1h")]
        Ohlcv1h,
    }

    #[derive(Copy, Clone, Debug, EnumString, PartialEq)]

    pub enum TickDataKind {
        #[strum(serialize = "tick")]
        Tick,

        #[strum(serialize = "aggTrades")]
        AggTrades,
    }

    #[derive(Copy, Clone, Debug, EnumString, PartialEq, Eq, Hash, Display)]
    /// Former known as LeafDir
    pub enum HdbSourceDir {
        #[strum(serialize = "ohlc-1m")]
        Ohlc1m,

        #[strum(serialize = "ohlc-30m")]
        Ohlc30m,

        #[strum(serialize = "ohlc-1h")]
        Ohlc1h,

        #[strum(serialize = "ohlcv-1m")]
        Ohlcv1m,

        #[strum(serialize = "ohlcv-30m")]
        Ohlcv30m,

        #[strum(serialize = "ohlcv-1h")]
        Ohlcv1h,

        #[strum(serialize = "tick")]
        Tick,

        #[strum(serialize = "aggTrades")]
        AggTrades,
    }

    impl From<CandlestickKind> for HdbSourceDir {
        fn from(value: CandlestickKind) -> Self {
            match value {
                CandlestickKind::Ohlc1m => HdbSourceDir::Ohlc1m,
                CandlestickKind::Ohlc30m => HdbSourceDir::Ohlc30m,
                CandlestickKind::Ohlc1h => HdbSourceDir::Ohlc1h,
                CandlestickKind::Ohlcv1m => HdbSourceDir::Ohlcv1m,
                CandlestickKind::Ohlcv30m => HdbSourceDir::Ohlcv30m,
                CandlestickKind::Ohlcv1h => HdbSourceDir::Ohlcv1h,
            }
        }
    }

    impl From<TradingIndicatorKind> for HdbSourceDir {
        fn from(value: TradingIndicatorKind) -> Self {
            match value {
                TradingIndicatorKind::Poc(price_histogram) => match price_histogram {
                    PriceHistogram::Tpo1m => HdbSourceDir::Ohlc1m,
                    PriceHistogram::VolAggTrades => HdbSourceDir::AggTrades,
                    PriceHistogram::VolTick => HdbSourceDir::Tick,
                },
                TradingIndicatorKind::VolumeAreaLow(price_histogram) => match price_histogram {
                    PriceHistogram::Tpo1m => HdbSourceDir::Ohlc1m,
                    PriceHistogram::VolAggTrades => HdbSourceDir::AggTrades,
                    PriceHistogram::VolTick => HdbSourceDir::Tick,
                },
                TradingIndicatorKind::VolumeAreaHigh(price_histogram) => match price_histogram {
                    PriceHistogram::Tpo1m => HdbSourceDir::Ohlc1m,
                    PriceHistogram::VolAggTrades => HdbSourceDir::AggTrades,
                    PriceHistogram::VolTick => HdbSourceDir::Tick,
                },
            }
        }
    }

    impl HdbSourceDir {
        pub fn split_ohlc_dir_in_parts(&self) -> (String, String) {
            match self {
                HdbSourceDir::AggTrades | HdbSourceDir::Tick  => {
                    panic!("Only call this function on LeafDir's of type <Ohlc>")
                }
                ohlc_variant => {
                    let t = ohlc_variant.to_string();
                    let parts: Vec<&str> = t.split("-").collect();
                    let ohlc_part = parts[0];
                    let timestamp_part = parts[1];
                    (ohlc_part.to_string(), timestamp_part.to_string())
                }
            }
        }
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

    #[derive(Copy, Clone, Debug, Display, EnumString, PartialEq, Eq, Hash)]
    pub enum MarketKind {
        #[strum(serialize = "btcusdt")]
        BtcUsdt,

        #[strum(serialize = "6a")]
        AudUsdFuture,

        #[strum(serialize = "6e")]
        EurUsdFuture,

        #[strum(serialize = "6b")]
        GbpUsdFuture,

        #[strum(serialize = "6c")]
        CadUsdFuture,

        #[strum(serialize = "6j")]
        YenUsdFuture,

        #[strum(serialize = "6n")]
        NzdUsdFuture,

        #[strum(serialize = "6btc")]
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
        pub fn decimal_places(&self) -> i32 {
            match self {
                MarketKind::BtcUsdt => 2,
                MarketKind::EurUsdFuture => 5,
                MarketKind::AudUsdFuture => 5,
                MarketKind::GbpUsdFuture => 4,
                MarketKind::CadUsdFuture => 5,
                MarketKind::YenUsdFuture => 7,
                MarketKind::NzdUsdFuture => 5,
                MarketKind::BtcUsdFuture => 0,
            }
        }

        pub fn tick_step_size(&self) -> Option<f64> {
            match self {
                MarketKind::BtcUsdt => None,
                MarketKind::EurUsdFuture => Some(0.00005),
                MarketKind::AudUsdFuture => Some(0.00005),
                MarketKind::GbpUsdFuture => Some(0.0001),
                MarketKind::CadUsdFuture => Some(0.00005),
                MarketKind::YenUsdFuture => Some(0.0000005),
                MarketKind::NzdUsdFuture => Some(0.00005),
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
                MarketKind::EurUsdFuture => Some(6.25),
                MarketKind::AudUsdFuture => Some(5.0),
                MarketKind::GbpUsdFuture => Some(6.25),
                MarketKind::CadUsdFuture => Some(5.0),
                MarketKind::YenUsdFuture => Some(6.25),
                MarketKind::NzdUsdFuture => Some(5.0),
                MarketKind::BtcUsdFuture => Some(25.0),
            }
        }
    }

    #[derive(Copy, Clone, Debug, EnumString, Display)]
    pub enum TimeFrame {
        #[strum(serialize = "1w")]
        Weekly,
        #[strum(serialize = "1d")]
        Daily,
    }
}
pub mod producers {
    use strum_macros::{Display, EnumString};

    #[derive(Copy, Clone, Debug, Display, EnumString)]
    pub enum ProducerKind {
        #[strum(serialize = "binance")]
        Binance,
        #[strum(serialize = "cme")]
        Cme,
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

    #[derive(Debug, Copy, Clone, Display, PartialEq)]
    pub enum TradeKind {
        Long,
        Short,

        #[strum(serialize = "NotClear")]
        None,
    }
}

pub mod error {
    use tokio::task::JoinError;

    #[derive(Debug, Clone)]
    pub enum ChapatyError {
        ParseBotError(String),
        ParseDataProducerError(String),
        BuildBotError(String),
        FailedToConvertPathBufToString(String),
        FailedToComputeProfitAndLossReport(String),
        FailedToFetchDataFrameFromMap(String),
        FailedToInitalizeProfitAndLossDataFrame(String),
        FailedToJoinFuturesInProfitAndLossComputation(String),
        FileNotFound(String),
        UnknownGoogleCloudStorageError(String),
        FailedToReadDataFrameFromCsv(String),
        FailedToCreateDataFrameMap(String),
        DeserealizeError(String),
        FailedApplyingMyLazyFrameOperations(String),
    }

    impl From<JoinError> for ChapatyError {
        fn from(value: JoinError) -> Self {
            ChapatyError::FailedToJoinFuturesInProfitAndLossComputation(value.to_string())
        }
    }
}

#[derive(Debug, Clone)]
pub enum MyAnyValue {
    Int64(i64),
    UInt32(u32),
    Float64(f64),
    Utf8(String),
    Null,
}

impl MyAnyValue {
    pub fn unwrap_float64(self) -> f64 {
        match self {
            MyAnyValue::Float64(x) => x,
            MyAnyValue::Null => panic!("Matching against NULL value"),
            _ => panic!("Matching against wrong value"),
        }
    }

    pub fn unwrap_uint32(self) -> u32 {
        match self {
            MyAnyValue::UInt32(x) => x,
            MyAnyValue::Null => panic!("Matching against NULL value"),
            _ => panic!("Matching against wrong value"),
        }
    }

    pub fn unwrap_int64(self) -> i64 {
        match self {
            MyAnyValue::Int64(x) => x,
            MyAnyValue::Null => panic!("Matching against NULL value"),
            _ => panic!("Matching against wrong value"),
        }
    }

    pub fn unwrap_utf8(self) -> String {
        match self {
            MyAnyValue::Utf8(x) => x,
            MyAnyValue::Null => panic!("Matching against NULL value"),
            _ => panic!("Matching against wrong value"),
        }
    }
}