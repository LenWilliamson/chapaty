use super::*;
use crate::enums::column_names::DataProviderColumnKind;
use polars::prelude::{CsvReader, SerReader};
use std::{io::Cursor, sync::Arc};

pub struct Binance {
    producer_kind: DataProviderKind,
}

impl FromStr for Binance {
    type Err = enums::error::ChapatyErrorKind;
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
            producer_kind: DataProviderKind::Binance,
        }
    }

    fn schema(&self, data: &HdbSourceDirKind) -> Schema {
        match data {
            HdbSourceDirKind::Ohlc1m
            | HdbSourceDirKind::Ohlc30m
            | HdbSourceDirKind::Ohlc1h
            | HdbSourceDirKind::Ohlcv1m
            | HdbSourceDirKind::Ohlcv30m
            | HdbSourceDirKind::Ohlcv1h => ohlcv_schema(),
            HdbSourceDirKind::Tick => {
                panic!("DataProvider <BINANCE> does not implement DataKind::Tick")
            }
            HdbSourceDirKind::AggTrades => aggtrades_schema(),
        }
    }
}

impl DataProvider for Binance {
    fn get_data_producer_kind(&self) -> DataProviderKind {
        self.producer_kind.clone()
    }



    fn get_df(&self, df_as_bytes: Vec<u8>, data: &HdbSourceDirKind) -> DataFrame {
        CsvReader::new(Cursor::new(df_as_bytes))
            .has_header(false)
            .with_schema(Arc::new(self.schema(data)))
            .finish()
            .unwrap()
    }
}

/// Returns the OHLC `Schema` for `Binance`
fn ohlcv_schema() -> Schema {
    Schema::from_iter(
        vec![
            Field::new(
                &DataProviderColumnKind::OpenTime.to_string(),
                DataType::Int64,
            ),
            Field::new(&DataProviderColumnKind::Open.to_string(), DataType::Float64),
            Field::new(&DataProviderColumnKind::High.to_string(), DataType::Float64),
            Field::new(&DataProviderColumnKind::Low.to_string(), DataType::Float64),
            Field::new(
                &DataProviderColumnKind::Close.to_string(),
                DataType::Float64,
            ),
            Field::new(
                &DataProviderColumnKind::Volume.to_string(),
                DataType::Float64,
            ),
            Field::new(
                &DataProviderColumnKind::CloseTime.to_string(),
                DataType::Int64,
            ),
            Field::new(
                &DataProviderColumnKind::QuoteAssetVol.to_string(),
                DataType::Float64,
            ),
            Field::new(
                &DataProviderColumnKind::NumberOfTrades.to_string(),
                DataType::Int64,
            ),
            Field::new(
                &DataProviderColumnKind::TakerBuyBaseAssetVol.to_string(),
                DataType::Float64,
            ),
            Field::new(
                &DataProviderColumnKind::TakerBuyQuoteAssetVol.to_string(),
                DataType::Float64,
            ),
            Field::new(&DataProviderColumnKind::Ignore.to_string(), DataType::Int64),
        ]
        .into_iter(),
    )
}

/// Returns the AggTrades `Schema` for `Binance`
fn aggtrades_schema() -> Schema {
    Schema::from_iter(
        vec![
            Field::new(
                &DataProviderColumnKind::AggTradeId.to_string(),
                DataType::Int64,
            ),
            Field::new(
                &DataProviderColumnKind::Price.to_string(),
                DataType::Float64,
            ),
            Field::new(
                &DataProviderColumnKind::Quantity.to_string(),
                DataType::Float64,
            ),
            Field::new(
                &DataProviderColumnKind::FirstTradeId.to_string(),
                DataType::Int64,
            ),
            Field::new(
                &DataProviderColumnKind::LastTradeId.to_string(),
                DataType::Int64,
            ),
            Field::new(
                &DataProviderColumnKind::Timestamp.to_string(),
                DataType::Int64,
            ),
            Field::new(
                &DataProviderColumnKind::BuyerEqualsMaker.to_string(),
                DataType::Boolean,
            ),
            Field::new(
                &DataProviderColumnKind::BestTradePriceMatch.to_string(),
                DataType::Boolean,
            ),
        ]
        .into_iter(),
    )
}
