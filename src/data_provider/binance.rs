use std::{io::Cursor, sync::Arc};

use polars::prelude::{CsvReader, SerReader};

use crate::enums::column_names::DataProviderColumns;

use super::*;

pub struct Binance {
    producer_kind: ProducerKind,
}

impl FromStr for Binance {
    type Err = enums::error::ChapatyError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Binance" | "binance" => Ok(Binance::new()),
            _ => Err(Self::Err::ParseDataProducerError(
                "Data Producer Does not Exists".to_string(),
            )),
        }
    }
}

impl Binance {
    pub fn new() -> Self {
        Binance {
            producer_kind: ProducerKind::Binance,
        }
    }
}

impl DataProvider for Binance {
    fn get_data_producer_kind(&self) -> ProducerKind {
        self.producer_kind.clone()
    }

    fn schema(&self, data: &HdbSourceDir) -> Schema {
        match data {
            HdbSourceDir::Ohlc1m | HdbSourceDir::Ohlc30m | HdbSourceDir::Ohlc1h => ohlc_schema(),
            HdbSourceDir::Ohlcv1m | HdbSourceDir::Ohlcv30m | HdbSourceDir::Ohlcv1h => {
                ohlcv_schema()
            }
            HdbSourceDir::Tick => {
                panic!("DataKind::Tick not yet implemented for DataProducer Binance")
            }
            HdbSourceDir::AggTrades => aggtrades_schema(),
        }
    }

    fn column_name_as_int(&self, col: &DataProviderColumns) -> usize {
        match col {
            // OHLCV Column names
            DataProviderColumns::OpenTime => 0,
            DataProviderColumns::Open => 1,
            DataProviderColumns::High => 2,
            DataProviderColumns::Low => 3,
            DataProviderColumns::Close => 4,
            DataProviderColumns::Volume => 5,
            DataProviderColumns::CloseTime => 6,
            DataProviderColumns::QuoteAssetVol => 7,
            DataProviderColumns::NumberOfTrades => 8,
            DataProviderColumns::TakerBuyBaseAssetVol => 9,
            DataProviderColumns::TakerBuyQuoteAssetVol => 10,
            DataProviderColumns::Ignore => 11,

            // AggTrades Column names
            DataProviderColumns::AggTradeId => 0,
            DataProviderColumns::Price => 1,
            DataProviderColumns::Quantity => 2,
            DataProviderColumns::FirstTradeId => 3,
            DataProviderColumns::LastTradeId => 4,
            DataProviderColumns::Timestamp => 5,
            DataProviderColumns::BuyerEqualsMaker => 6,
            DataProviderColumns::BestTradePriceMatch => 7,
        }
    }

    fn get_df(&self, df_as_bytes: Vec<u8>, data: &HdbSourceDir) -> DataFrame {
        CsvReader::new(Cursor::new(df_as_bytes))
            .has_header(false)
            .with_schema(Arc::new(self.schema(data)))
            .finish()
            .unwrap()
    }

    fn get_ts_col_as_str(&self, data: &HdbSourceDir) -> String {
        match data {
            HdbSourceDir::Ohlc1m
            | HdbSourceDir::Ohlc30m
            | HdbSourceDir::Ohlc1h
            | HdbSourceDir::Ohlcv1m
            | HdbSourceDir::Ohlcv30m
            | HdbSourceDir::Ohlcv1h => DataProviderColumns::OpenTime.to_string(),
            HdbSourceDir::Tick => panic!("Tick data not yet supported."),
            HdbSourceDir::AggTrades => DataProviderColumns::Timestamp.to_string(),
        }
    }
}

/// Returns the OHLC `Schema` for `Binance`
fn ohlc_schema() -> Schema {
    Schema::from_iter(
        vec![
            Field::new("ots", DataType::Int64),
            Field::new("open", DataType::Float64),
            Field::new("high", DataType::Float64),
            Field::new("low", DataType::Float64),
            Field::new("close", DataType::Float64),
            Field::new("vol", DataType::Float64),
            Field::new("cts", DataType::Int64),
            Field::new("qav", DataType::Float64),
            Field::new("not", DataType::Int64),
            Field::new("tbbav", DataType::Float64),
            Field::new("tbqav", DataType::Float64),
            Field::new("ignore", DataType::Int64),
        ]
        .into_iter(),
    )
}

/// Returns the OHLCV `Schema` for `Binance`
fn ohlcv_schema() -> Schema {
    Schema::from_iter(
        vec![
            Field::new("ots", DataType::Int64),
            Field::new("open", DataType::Float64),
            Field::new("high", DataType::Float64),
            Field::new("low", DataType::Float64),
            Field::new("close", DataType::Float64),
            Field::new("vol", DataType::Float64),
            Field::new("cts", DataType::Int64),
            Field::new("qav", DataType::Float64),
            Field::new("not", DataType::Int64),
            Field::new("tbbav", DataType::Float64),
            Field::new("tbqav", DataType::Float64),
            Field::new("ignore", DataType::Int64),
        ]
        .into_iter(),
    )
}

/// Returns the AggTrades `Schema` for `Binance`
fn aggtrades_schema() -> Schema {
    Schema::from_iter(
        vec![
            Field::new("atid", DataType::Int64),
            Field::new("px", DataType::Float64),
            Field::new("qx", DataType::Float64),
            Field::new("ftid", DataType::Int64),
            Field::new("ltid", DataType::Int64),
            Field::new("ts", DataType::Int64),
            Field::new("bm", DataType::Boolean),
            Field::new("btpm", DataType::Boolean),
        ]
        .into_iter(),
    )
}
