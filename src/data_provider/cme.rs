use std::{io::Cursor, sync::Arc};

use chrono::Duration;
use polars::{
    lazy::dsl::GetOutput,
    prelude::{col, CsvReader, IntoLazy, SerReader},
};

use crate::{
    enums::column_names::DataProviderColumns,
    lazy_frame_operations::closures::{comma_separated_string_to_f64, sub_time}, data_frame_operations::vol_schema,
};

use super::*;

/// The `Ninja` data provider uses market data from the `Ninja` exchange and its respective
/// market data feeds.
///
/// # Links
/// * Ninjatrader: <https://ninjatrader.com/de/>
/// * Ranchodinero: <https://www.ranchodinero.com>
pub struct Ninja {
    producer_kind: ProducerKind,
}

impl FromStr for Ninja {
    type Err = enums::error::ChapatyError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Ninja" | "ninja" => Ok(Ninja::new()),
            _ => Err(Self::Err::ParseDataProducerError(
                "Data Producer Does not Exists".to_string(),
            )),
        }
    }
}

impl Ninja {
    // #[allow(dead_code)]
    /// Creates a `Ninja` data producer.
    /// # Arguments
    /// * `client` - google cloud storage client
    /// * `bucket` - cloud storage bucket name
    pub fn new() -> Self {
        Ninja {
            producer_kind: ProducerKind::Cme,
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
    /// ```
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
    ///
    /// ```
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

impl DataProvider for Ninja {

    fn delimiter(&self) -> u8 {
        b';'
    }

    fn get_data_producer_kind(&self) -> ProducerKind {
        self.producer_kind.clone()
    }

    fn schema(&self, data: &LeafDir) -> Schema {
        match data {
            LeafDir::Ohlc1m | LeafDir::Ohlc30m | LeafDir::Ohlc1h => ohlc_schema(),
            LeafDir::Ohlcv1m | LeafDir::Ohlcv30m | LeafDir::Ohlcv1h => {
                panic!("DataKind::Ohlcv not yet implemented for DataProducer Ninja")
            }
            LeafDir::Tick => panic!("DataKind::Tick not yet implemented for DataProducer Ninja"),
            LeafDir::AggTrades => {
                panic!("DataKind::AggTrades not yet implemented for DataProducer Ninja")
            }
            LeafDir::Vol => vol_schema(),
            LeafDir::ProfitAndLoss => {
                panic!("Not implemented by DataProvider. TODO Improve API")
            }
        }
    }

    fn column_name_as_int(&self, col: &DataProviderColumns) -> usize {
        match col {
            DataProviderColumns::OpenTime => 0,
            DataProviderColumns::Open => 1,
            DataProviderColumns::High => 2,
            DataProviderColumns::Low => 3,
            DataProviderColumns::Close => 4,
            _ => panic!("No column {col} for DataProvider <CME>"),
        }
    }

    fn get_df(&self, df_as_bytes: Vec<u8>, data: &LeafDir) -> DataFrame {
        let offset = match data {
            LeafDir::Ohlc1m | LeafDir::Ohlcv1m => 1,
            LeafDir::Ohlc30m | LeafDir::Ohlcv30m => 30,
            LeafDir::Ohlc1h | LeafDir::Ohlcv1h => 60,
            _ => panic!("We can only compute offset for ohlc data. But not for {data:?}"),
        };
        self.transform_ninja_df(df_as_bytes, offset)
    }

    fn get_ts_col_as_str(&self, data: &LeafDir) -> String {
        match data {
            LeafDir::Ohlc1m
            | LeafDir::Ohlc30m
            | LeafDir::Ohlc1h
            | LeafDir::Ohlcv1m
            | LeafDir::Ohlcv30m
            | LeafDir::Ohlcv1h => DataProviderColumns::OpenTime.to_string(),
            LeafDir::Tick => panic!("Tick data not supported."),
            LeafDir::AggTrades => panic!("Vol data not supported."),
            LeafDir::Vol => panic!("No timestamp for volume."),
            LeafDir::ProfitAndLoss => {
                panic!("Not implemented by DataProvider. TODO Improve API")
            }
        }
    }
}

/// Returns the OHLC `Schema` for `Ninja`
fn ohlc_schema() -> Schema {
    Schema::from_iter(
        vec![
            Field::new("ots", DataType::Int64),
            Field::new("open", DataType::Float64),
            Field::new("high", DataType::Float64),
            Field::new("low", DataType::Float64),
            Field::new("close", DataType::Float64),
            Field::new("cts", DataType::Int64),
        ]
        .into_iter(),
    )
}