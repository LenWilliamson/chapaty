use super::*;
use crate::{enums::bot::DataProviderKind, DataProviderColumnKind};
use polars::prelude::{CsvReader, SerReader};
use std::{io::Cursor, sync::Arc};

pub struct Binance;

impl FromStr for Binance {
    type Err = enums::error::ChapatyErrorKind;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Binance" | "binance" => Ok(Binance),
            _ => Err(Self::Err::ParseDataProducerError(
                "Data Producer Does not Exists".to_string(),
            )),
        }
    }
}

impl DataProvider for Binance {
    fn get_name(&self) -> String {
        DataProviderKind::Binance.to_string()
    }

    fn get_df_from_bytes(&self, request: BytesToDataFrameRequest) -> DataFrame {
        CsvReader::new(Cursor::new(request.df_as_bytes))
            .has_header(false)
            .with_schema(Arc::new(schema(&request.bytes_source_dir)))
            .finish()
            .unwrap()
    }
}

fn schema(data: &HdbSourceDirKind) -> Schema {
    match data {
        HdbSourceDirKind::Ohlc1m
        | HdbSourceDirKind::Ohlc30m
        | HdbSourceDirKind::Ohlc1h
        | HdbSourceDirKind::Ohlcv1m
        | HdbSourceDirKind::Ohlcv30m
        | HdbSourceDirKind::Ohlcv1h => ohlcv_schema(),
        HdbSourceDirKind::AggTrades => aggtrades_schema(),
        HdbSourceDirKind::Tick => {
            panic!("DataProvider <BINANCE> does not implement DataKind::Tick")
        }
    }
}

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
