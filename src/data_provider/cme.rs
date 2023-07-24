use std::{io::Cursor, sync::Arc};

use chrono::Duration;
use polars::{
    lazy::dsl::GetOutput,
    prelude::{col, CsvReader, IntoLazy, SerReader},
};

use crate::{
    enums::column_names::DataProviderColumnKind,
    lazy_frame_operations::closures::{comma_separated_string_to_f64, sub_time},
};

use super::*;

pub struct Cme {
    producer_kind: DataProviderKind,
}

impl FromStr for Cme {
    type Err = enums::error::ChapatyErrorKind;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Ninja" | "ninja" => Ok(Cme::new()),
            _ => Err(Self::Err::ParseDataProducerError(
                "Data Producer Does not Exists".to_string(),
            )),
        }
    }
}

impl Cme {
    pub fn new() -> Self {
        Cme {
            producer_kind: DataProviderKind::Cme,
        }
    }

    /// Returns a OHLC `DataFrame` from a raw data `.csv` file produced by the `Ninja` data profider
    ///
    /// # Arguments
    /// * `file` - path to the `.csv` file we want to load into a `DataFrame`
    /// * `kperiod` - duration of a candle **in minutes**
    ///
    /// # Example
    /// Calling `transform_ninja_df` on the INPUT `.csv` with `kperiod = 60` results in OUTPUT. Note, the
    /// INPUT `.csv` does not have any header. We simply put them in this example to clarify how INPUT and
    /// OUTPUT differ from each other.
    /// // INPUT:
    /// // idx = Index
    /// // id = Uniqe identifier
    /// // val = Example value inside a .csv file
    /// idx:    0                   ,1          ,2          ,3          ,4         
    /// id:     cts                 ,open       ,high       ,low        ,close     
    /// row0:   01.09.2022 00:01:00 ;1,0127     ;1,01295    ;1,01265    ;1,01275
    /// row1:   01.09.2022 00:02:00 ;1,01275    ;1,0129     ;1,01275    ;1,01285
    ///
    /// // OUTPUT:
    /// idx:    0               ,1          ,2          ,3          ,4          ,5  
    /// id:     ots             ,open       ,high       ,low        ,close      ,cts
    /// row0:   1661990400000   ,1.0127     ,1.01295    ,1.01265    ,1.01275    ,1661990459999
    /// row1:   1661990460000   ,1.01275    ,1.0129     ,1.01275    ,1.01285    ,1661990519999
    pub fn transform_ninja_df(&self, df_as_bytes: Vec<u8>, kperiod: i64) -> DataFrame {
        let schema = Schema::from_iter(
            vec![
                Field::new("ots", DataType::Utf8),
                Field::new("open", DataType::Utf8),
                Field::new("high", DataType::Utf8),
                Field::new("low", DataType::Utf8),
                Field::new("close", DataType::Utf8),
            ]
            .into_iter(),
        );
        // has no header
        let df = CsvReader::new(Cursor::new(df_as_bytes))
            .has_header(false)
            .with_delimiter(b';')
            .with_schema(Arc::new(schema))
            .finish()
            .unwrap();
        ninja_raw_to_ohlc_df(df, kperiod)
    }
}

fn ninja_raw_to_ohlc_df(df: DataFrame, offset: i64) -> DataFrame {
    df.lazy()
        .with_columns(vec![
            col("ots").apply(
                move |x| Ok(Some(sub_time(x, Duration::minutes(offset)))),
                GetOutput::default(),
            ),
            col("open").apply(
                |x| Ok(Some(comma_separated_string_to_f64(x))),
                GetOutput::default(),
            ),
            col("high").apply(
                |x| Ok(Some(comma_separated_string_to_f64(x))),
                GetOutput::default(),
            ),
            col("low").apply(
                |x| Ok(Some(comma_separated_string_to_f64(x))),
                GetOutput::default(),
            ),
            col("close").apply(
                |x| Ok(Some(comma_separated_string_to_f64(x))),
                GetOutput::default(),
            ),
            col("ots")
                .apply(
                    |x| Ok(Some(sub_time(x, Duration::milliseconds(1)))),
                    GetOutput::default(),
                )
                .alias("cts"),
        ])
        .collect()
        .unwrap()
}

impl DataProvider for Cme {
    fn get_data_producer_kind(&self) -> DataProviderKind {
        self.producer_kind.clone()
    }

    fn schema(&self, data: &HdbSourceDirKind) -> Schema {
        match data {
            HdbSourceDirKind::Ohlc1m | HdbSourceDirKind::Ohlc30m | HdbSourceDirKind::Ohlc1h => ohlc_schema(),
            _ => panic!("DataProvider <CME> only upports OHLC schema. No schema for <{data}>"),
        }
    }

    fn column_name_as_int(&self, col: &DataProviderColumnKind) -> usize {
        match col {
            DataProviderColumnKind::OpenTime => 0,
            DataProviderColumnKind::Open => 1,
            DataProviderColumnKind::High => 2,
            DataProviderColumnKind::Low => 3,
            DataProviderColumnKind::Close => 4,
            _ => panic!("DataProvider <CME> does not provide data with column {col}"),
        }
    }

    fn get_df(&self, df_as_bytes: Vec<u8>, data: &HdbSourceDirKind) -> DataFrame {
        let offset = match data {
            HdbSourceDirKind::Ohlc1m | HdbSourceDirKind::Ohlcv1m => 1,
            HdbSourceDirKind::Ohlc30m | HdbSourceDirKind::Ohlcv30m => 30,
            HdbSourceDirKind::Ohlc1h | HdbSourceDirKind::Ohlcv1h => 60,
            _ => panic!(
                "DataProvider <CME> can only compute offset for OHLC data. But not for {data}"
            ),
        };
        self.transform_ninja_df(df_as_bytes, offset)
    }
}

/// Returns the OHLC `Schema` for `Ninja`
fn ohlc_schema() -> Schema {
    Schema::from_iter(
        vec![
            Field::new(&DataProviderColumnKind::OpenTime.to_string(), DataType::Int64),
            Field::new(&DataProviderColumnKind::Open.to_string(), DataType::Float64),
            Field::new(&DataProviderColumnKind::High.to_string(), DataType::Float64),
            Field::new(&DataProviderColumnKind::Low.to_string(), DataType::Float64),
            Field::new(&DataProviderColumnKind::Close.to_string(), DataType::Float64),
            Field::new(&DataProviderColumnKind::CloseTime.to_string(), DataType::Int64),
        ]
        .into_iter(),
    )
}
