use strum_macros::{Display, EnumString};

use super::{
    column_names::DataProviderColumnKind,
    indicator::{PriceHistogramKind, TradingIndicatorKind},
};

#[derive(Copy, Clone, Debug, EnumString, PartialEq, Display)]
pub enum MarketSimulationDataKind {
    #[strum(serialize = "ohlc-1m")]
    Ohlc1m,

    #[strum(serialize = "ohlc-5m")]
    Ohlc5m,

    #[strum(serialize = "ohlc-15m")]
    Ohlc15m,

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
pub enum HdbSourceDirKind {
    #[strum(serialize = "ohlc-1m")]
    Ohlc1m,

    #[strum(serialize = "ohlc-5m")]
    Ohlc5m,

    #[strum(serialize = "ohlc-15m")]
    Ohlc15m,

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

impl HdbSourceDirKind {
    pub fn get_ts_col_as_str(&self) -> String {
        match self {
            HdbSourceDirKind::Ohlc1m
            | HdbSourceDirKind::Ohlc5m
            | HdbSourceDirKind::Ohlc15m
            | HdbSourceDirKind::Ohlc30m
            | HdbSourceDirKind::Ohlc1h
            | HdbSourceDirKind::Ohlcv1m
            | HdbSourceDirKind::Ohlcv30m
            | HdbSourceDirKind::Ohlcv1h => DataProviderColumnKind::OpenTime.to_string(),
            HdbSourceDirKind::Tick => panic!("Tick data not yet supported."),
            HdbSourceDirKind::AggTrades => DataProviderColumnKind::Timestamp.to_string(),
        }
    }
}

impl From<MarketSimulationDataKind> for HdbSourceDirKind {
    fn from(value: MarketSimulationDataKind) -> Self {
        match value {
            MarketSimulationDataKind::Ohlc1m => HdbSourceDirKind::Ohlc1m,
            MarketSimulationDataKind::Ohlc5m => HdbSourceDirKind::Ohlc5m,
            MarketSimulationDataKind::Ohlc15m => HdbSourceDirKind::Ohlc15m,
            MarketSimulationDataKind::Ohlc30m => HdbSourceDirKind::Ohlc30m,
            MarketSimulationDataKind::Ohlc1h => HdbSourceDirKind::Ohlc1h,
            MarketSimulationDataKind::Ohlcv1m => HdbSourceDirKind::Ohlcv1m,
            MarketSimulationDataKind::Ohlcv30m => HdbSourceDirKind::Ohlcv30m,
            MarketSimulationDataKind::Ohlcv1h => HdbSourceDirKind::Ohlcv1h,
        }
    }
}

impl From<TradingIndicatorKind> for HdbSourceDirKind {
    fn from(value: TradingIndicatorKind) -> Self {
        match value {
            TradingIndicatorKind::Poc(price_histogram) => match price_histogram {
                PriceHistogramKind::Tpo1m => HdbSourceDirKind::Ohlc1m,
                PriceHistogramKind::Tpo1h => HdbSourceDirKind::Ohlc1h,
                PriceHistogramKind::VolAggTrades => HdbSourceDirKind::AggTrades,
                PriceHistogramKind::VolTick => HdbSourceDirKind::Tick,
            },
            TradingIndicatorKind::ValueAreaLow(price_histogram) => match price_histogram {
                PriceHistogramKind::Tpo1m => HdbSourceDirKind::Ohlc1m,
                PriceHistogramKind::Tpo1h => HdbSourceDirKind::Ohlc1h,
                PriceHistogramKind::VolAggTrades => HdbSourceDirKind::AggTrades,
                PriceHistogramKind::VolTick => HdbSourceDirKind::Tick,
            },
            TradingIndicatorKind::ValueAreaHigh(price_histogram) => match price_histogram {
                PriceHistogramKind::Tpo1m => HdbSourceDirKind::Ohlc1m,
                PriceHistogramKind::Tpo1h => HdbSourceDirKind::Ohlc1h,
                PriceHistogramKind::VolAggTrades => HdbSourceDirKind::AggTrades,
                PriceHistogramKind::VolTick => HdbSourceDirKind::Tick,
            },
        }
    }
}

impl HdbSourceDirKind {
    pub fn split_ohlc_dir_in_parts(&self) -> (String, String) {
        match self {
            HdbSourceDirKind::AggTrades | HdbSourceDirKind::Tick => {
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

impl MarketSimulationDataKind {
    pub fn duration_in_minutes(&self) -> i64 {
        match self {
            MarketSimulationDataKind::Ohlc1m => 1,
            MarketSimulationDataKind::Ohlc5m => 5,
            MarketSimulationDataKind::Ohlc15m => 15,
            MarketSimulationDataKind::Ohlc30m => 30,
            MarketSimulationDataKind::Ohlc1h => 60,
            MarketSimulationDataKind::Ohlcv1m => 1,
            MarketSimulationDataKind::Ohlcv30m => 30,
            MarketSimulationDataKind::Ohlcv1h => 60,
        }
    }
}
