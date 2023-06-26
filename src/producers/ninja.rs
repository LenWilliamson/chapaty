use super::*;
use crate::common::functions::ninja_raw_to_ohlc_df;

/// The `Ninja` data provider uses market data from the `Ninja` exchange and its respective
/// market data feeds.
///
/// # Links
/// * Ninjatrader: https://ninjatrader.com/de/
/// * Ranchodinero: https://www.ranchodinero.com
pub struct Ninja {
    bucket: PathBuf,
    producer_kind: ProducerKind,
}

impl FromStr for Ninja {
    type Err = enums::error::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Ninja" | "ninja" => Ok(Ninja::new(std::path::PathBuf::from(
                config::GCS_DATA_BUCKET,
            ))),
            _ => Err(Self::Err::ParseDataProducerError(
                "Data Producer Does not Exists".to_string(),
            )),
        }
    }
}

impl Ninja {
    #[allow(dead_code)]
    /// Creates a `Ninja` data producer.
    /// # Arguments
    /// * `client` - google cloud storage client
    /// * `bucket` - cloud storage bucket name
    pub fn new(bucket: PathBuf) -> Self {
        Ninja {
            bucket,
            producer_kind: ProducerKind::Ninja,
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
    async fn transform_ninja_df(&self, file: &PathBuf, kperiod: i64) -> DataFrame {
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
        let df = df_from_file(file, Some(schema), Some(b';')).await;
        ninja_raw_to_ohlc_df(df.unwrap(), kperiod)
    }
}

#[async_trait]
impl DataProducer for Ninja {
    fn has_header(&self) {}

    fn get_bucket_name(&self) -> PathBuf {
        self.bucket.clone()
    }

    fn get_data_producer_kind(&self) -> ProducerKind {
        self.producer_kind.clone()
    }

    fn schema(&self, data: &LeafDir) -> Schema {
        match data {
            LeafDir::Ohlc1m | LeafDir::Ohlc30m | LeafDir::Ohlc60m => ohlc_schema(),
            LeafDir::Ohlcv1m | LeafDir::Ohlcv30m | LeafDir::Ohlcv60m => panic!("DataKind::Ohlcv not yet implemented for DataProducer Ninja"),
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
    fn column_name_as_str(&self, col: &Columns) -> String {
        match col {
            Columns::Ohlcv(_) => {
                panic!("DataKind::Ohlcv not yet implemented for DataProducer Ninja")
            }
            Columns::Ohlc(c) => ohlc_column_name_as_str(c),
            Columns::AggTrade(_) => {
                panic!("DataKind::AggTrades not yet implemented for DataProducer Ninja")
            }
            Columns::Vol(c) => vol_column_name_as_str(c),
        }
    }

    fn column_name_as_int(&self, col: &Columns) -> usize {
        match col {
            Columns::Ohlcv(_) => {
                panic!("DataKind::Ohlcv not yet implemented for DataProducer Ninja")
            }
            Columns::Ohlc(c) => usize::try_from(*c as u8).unwrap(),
            Columns::AggTrade(_) => {
                panic!("DataKind::AggTrades not yet implemented for DataProducer Ninja")
            }
            Columns::Vol(c) => usize::try_from(*c as u8).unwrap(),
        }
    }

    async fn get_df(&self, file: &PathBuf, data: &LeafDir) -> Result<DataFrame, Error> {
        let header =
            file.to_str().unwrap().contains("cw") || file.to_str().unwrap().contains("day"); // ends_with("cw");
        if header {
            df_from_file(file, Some(self.schema(data)), None).await
        } else {
            let offset = match data {
                LeafDir::Ohlc1m | LeafDir::Ohlcv1m => 1,
                LeafDir::Ohlc30m | LeafDir::Ohlcv30m => 30,
                LeafDir::Ohlc60m | LeafDir::Ohlcv60m => 60,
                _ => panic!(
                    "We can only compute offset for ohlc data. But not for {data:?}"
                ),
            };
            Ok(self.transform_ninja_df(file, offset).await)
        }
    }

    fn get_ts_col_as_str(&self, data: &LeafDir) -> String {
        match data {
            LeafDir::Ohlc1m | LeafDir::Ohlc30m | LeafDir::Ohlc60m => self.column_name_as_str(&Columns::Ohlc(OhlcColumnNames::OpenTime)),
            LeafDir::Ohlcv1m | LeafDir::Ohlcv30m | LeafDir::Ohlcv60m => panic!("Ohlcv data not yet supported."),
            LeafDir::Tick => panic!("Tick data not yet supported."),
            LeafDir::AggTrades => panic!("Vol data not yet supported."),
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

/// Returns the volume profile `Schema` for `Ninja`
fn vol_schema() -> Schema {
    Schema::from_iter(
        vec![
            Field::new("px", DataType::Float64),
            Field::new("qx", DataType::Float64),
        ]
        .into_iter(),
    )
}

/// Returns the OHLC coloumn name as `String` for a `DataFrame` provided by `Ninja`
/// # Arguments
/// * `c` - Column name we want to obtain a `String` value for
fn ohlc_column_name_as_str(c: &OhlcColumnNames) -> String {
    match c {
        OhlcColumnNames::OpenTime => String::from("ots"),
        OhlcColumnNames::Open => String::from("open"),
        OhlcColumnNames::High => String::from("high"),
        OhlcColumnNames::Low => String::from("low"),
        OhlcColumnNames::Close => String::from("close"),
        OhlcColumnNames::CloseTime => String::from("cts"),
    }
}

/// Returns the volume profile coloumn name as `String` for a `DataFrame` provided by `Ninja`
/// # Arguments
/// * `c` - Column name we want to obtain a `String` value for
fn vol_column_name_as_str(c: &VolumeProfileColumnNames) -> String {
    match c {
        VolumeProfileColumnNames::Price => String::from("px"),
        VolumeProfileColumnNames::Quantity => String::from("qx"),
    }
}

#[cfg(test)]
mod tests {
    // Intern crates
    use super::*;
    use crate::config::GCS_DATA_BUCKET;

    // Extern crates
    use polars::prelude::{df, IndexOfSchema, NamedFrom};

    #[cfg(test)]
    mod ohlc {
        use super::*;
        use crate::config;
        /// This unit test checks if a raw `.csv` by the `Ninja` data producer get`s parsed correctly into a `DataFrame`.
        ///
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
        /// ```
        #[tokio::test]
        async fn test_transform_df() {
            let ninja = Ninja {
                bucket: PathBuf::from(GCS_DATA_BUCKET),
                producer_kind: ProducerKind::Ninja,
            };
            let target = df!(
                "ots" => &[1661990400000_i64, 1661990460000, 1661990520000,1661990580000, 1661990640000,1661990700000],
                "open" => &[1.0127, 1.01275, 1.01285, 1.0127, 1.01275, 1.01285],
                "high" => &[1.01295,1.0129,1.01285,1.01275,1.0128,1.0129],
                "low" => &[1.01265,1.01275,1.01265,1.0127,1.01275,1.01285],
                "close" => &[1.01275,1.01285,1.0127,1.0127,1.0128,1.01285],
                "cts" => &[1661990459999_i64, 1661990519999,1661990579999, 1661990639999,1661990699999, 1661990759999],
            );

            let file = PathBuf::from("data/test/other/ninja/transform_ninja_test_file.csv");
            let result = ninja.transform_ninja_df(&file, 1).await;

            assert_eq!(target.unwrap().frame_equal(&result), true);
        }

        /// This unit test checks if the schema for the OHLC data by `Ninja` matches. We also check the order of the schema elements.
        /// The `.csv` format is
        ///
        /// ```
        /// // idx = Index
        /// // id = Uniqe identifier
        /// // val = Example value inside a .csv file
        /// idx:    0               ,1          ,2          ,3          ,4          ,5  
        /// id:     ots             ,open       ,high       ,low        ,close      ,cts
        /// val:    1643673600000   ,38466.90   ,38627.35   ,38276.43   ,38342.36   ,1643673600000
        /// ```
        ///
        /// It is not enough to test `assert_eq!(result.eq(&target), true);` because the order of the schema elements are negleted.
        /// For example:
        ///
        /// ```
        /// let a = polars::prelude::Schema::from_iter(vec![
        ///     polars::prelude::Field::new("atid", polars::prelude::DataType::UInt64),
        ///     polars::prelude::Field::new("px", polars::prelude::DataType::Float64),
        /// ]);
        ///
        /// let b = polars::prelude::Schema::from_iter(vec![
        ///     polars::prelude::Field::new("px", polars::prelude::DataType::Float64),
        ///     polars::prelude::Field::new("atid", polars::prelude::DataType::UInt64),
        /// ]);
        ///
        /// assert_eq!(a.eq(&b), true) // Test will pass
        /// ```
        #[tokio::test]
        async fn test_schema() {
            let ninja = Ninja {
                bucket: PathBuf::from(GCS_DATA_BUCKET),
                producer_kind: ProducerKind::Ninja,
            };
            let result = ninja.schema(&LeafDir::Ohlc60m);
            assert_eq!(result.index_of("ots").unwrap(), 0);
            assert_eq!(result.index_of("open").unwrap(), 1);
            assert_eq!(result.index_of("high").unwrap(), 2);
            assert_eq!(result.index_of("low").unwrap(), 3);
            assert_eq!(result.index_of("close").unwrap(), 4);
            assert_eq!(result.index_of("cts").unwrap(), 5);
        }

        /// This unit test checks if the enum types for the OHLC data by `Ninja` matches with our id for the column names.
        /// The `.csv` format is
        ///
        /// ```
        /// // idx = Index
        /// // id = Uniqe identifier
        /// // val = Example value inside a .csv file
        /// idx:    0               ,1          ,2          ,3          ,4          ,5  
        /// id:     ots             ,open       ,high       ,low        ,close      ,cts
        /// val:    1643673600000   ,38466.90   ,38627.35   ,38276.43   ,38342.36   ,1643673600000
        /// ```
        #[tokio::test]
        async fn test_column_name_as_str() {
            let ninja = Ninja {
                bucket: PathBuf::from(GCS_DATA_BUCKET),
                producer_kind: ProducerKind::Ninja,
            };
            assert_eq!(
                ninja.column_name_as_str(&Columns::Ohlc(OhlcColumnNames::OpenTime)),
                String::from("ots")
            );
            assert_eq!(
                ninja.column_name_as_str(&Columns::Ohlc(OhlcColumnNames::Open)),
                String::from("open")
            );
            assert_eq!(
                ninja.column_name_as_str(&Columns::Ohlc(OhlcColumnNames::High)),
                String::from("high")
            );
            assert_eq!(
                ninja.column_name_as_str(&Columns::Ohlc(OhlcColumnNames::Low)),
                String::from("low")
            );
            assert_eq!(
                ninja.column_name_as_str(&Columns::Ohlc(OhlcColumnNames::Close)),
                String::from("close")
            );
            assert_eq!(
                ninja.column_name_as_str(&Columns::Ohlc(OhlcColumnNames::CloseTime)),
                String::from("cts")
            );
        }

        /// This unit test checks if the order of the column ids actually match with the order of the loaded schema for the OHLC data by ninja.
        /// The `.csv` format is
        ///
        /// ```
        /// // idx = Index
        /// // id = Uniqe identifier
        /// // val = Example value inside a .csv file
        /// idx:    0               ,1          ,2          ,3          ,4          ,5  
        /// id:     ots             ,open       ,high       ,low        ,close      ,cts
        /// val:    1643673600000   ,38466.90   ,38627.35   ,38276.43   ,38342.36   ,1643673600000
        /// ```
        #[tokio::test]
        async fn test_column_name_as_int() {
            let ninja = Ninja {
                bucket: PathBuf::from(GCS_DATA_BUCKET),
                producer_kind: ProducerKind::Ninja,
            };
            let result = ninja.schema(&LeafDir::Ohlc60m);
            assert_eq!(
                ninja.column_name_as_int(&Columns::Ohlc(OhlcColumnNames::OpenTime)),
                result.index_of("ots").unwrap()
            );
            assert_eq!(
                ninja.column_name_as_int(&Columns::Ohlc(OhlcColumnNames::Open)),
                result.index_of("open").unwrap()
            );
            assert_eq!(
                ninja.column_name_as_int(&Columns::Ohlc(OhlcColumnNames::High)),
                result.index_of("high").unwrap()
            );
            assert_eq!(
                ninja.column_name_as_int(&Columns::Ohlc(OhlcColumnNames::Low)),
                result.index_of("low").unwrap()
            );
            assert_eq!(
                ninja.column_name_as_int(&Columns::Ohlc(OhlcColumnNames::Close)),
                result.index_of("close").unwrap()
            );
            assert_eq!(
                ninja.column_name_as_int(&Columns::Ohlc(OhlcColumnNames::CloseTime)),
                result.index_of("cts").unwrap()
            );
        }
    }

    #[cfg(test)]
    mod vol {
        use super::*;
        use crate::config;
        /// This unit test checks if the schema for the generated volume profile. We also check the order of the schema elements.
        /// The `.csv` format is
        ///
        /// ```
        /// // idx = Index
        /// // id = Uniqe identifier
        /// // val = Example value inside a .csv file
        /// idx:    0       ,1
        /// id:     px      ,qx
        /// val:    100.00  ,38.032
        /// ```
        ///
        /// # Note
        ///
        /// It is not enough to test `assert_eq!(result.eq(&target), true);` because the order of the schema elements are negleted.
        /// For example:
        ///
        /// ```
        /// let a = polars::prelude::Schema::from_iter(vec![
        ///     polars::prelude::Field::new("atid", polars::prelude::DataType::Int64),
        ///     polars::prelude::Field::new("px", polars::prelude::DataType::Float64),
        /// ]);
        ///
        /// let b = polars::prelude::Schema::from_iter(vec![
        ///     polars::prelude::Field::new("px", polars::prelude::DataType::Float64),
        ///     polars::prelude::Field::new("atid", polars::prelude::DataType::Int64),
        /// ]);
        ///
        /// assert_eq!(a.eq(&b), true) // Test will pass
        /// ```
        #[tokio::test]
        async fn test_schema() {
            let ninja = Ninja {
                bucket: PathBuf::from(GCS_DATA_BUCKET),
                producer_kind: ProducerKind::Ninja,
            };
            let result = ninja.schema(&LeafDir::Vol);
            assert_eq!(result.index_of("px").unwrap(), 0);
            assert_eq!(result.index_of("qx").unwrap(), 1);
        }

        /// This unit test checks if the enum types for the generated volume profile matches with our id for the column names.
        /// The `.csv` format is
        ///
        /// ```
        /// // idx = Index
        /// // id = Uniqe identifier
        /// // val = Example value inside a .csv file
        /// idx:    0       ,1
        /// id:     px      ,qx
        /// val:    100.00  ,38.032
        /// ```
        #[tokio::test]
        async fn test_column_name_as_str() {
            let ninja = Ninja {
                bucket: PathBuf::from(GCS_DATA_BUCKET),
                producer_kind: ProducerKind::Ninja,
            };
            assert_eq!(
                ninja.column_name_as_str(&Columns::Vol(VolumeProfileColumnNames::Price)),
                String::from("px")
            );
            assert_eq!(
                ninja.column_name_as_str(&Columns::Vol(VolumeProfileColumnNames::Quantity)),
                String::from("qx")
            );
        }

        /// This unit test checks if the order of the column ids actually match with the order of the loaded schema for the generated volume profile.
        /// The `.csv` format is
        ///
        /// ```
        /// // idx = Index
        /// // id = Uniqe identifier
        /// // val = Example value inside a .csv file
        /// idx:    0       ,1
        /// id:     px      ,qx
        /// val:    100.00  ,38.032
        /// ```
        #[tokio::test]
        async fn test_column_name_as_int() {
            let ninja = Ninja {
                bucket: PathBuf::from(GCS_DATA_BUCKET),
                producer_kind: ProducerKind::Ninja,
            };

            let result = ninja.schema(&LeafDir::Vol);
            assert_eq!(
                ninja.column_name_as_int(&Columns::Vol(VolumeProfileColumnNames::Price)),
                result.index_of("px").unwrap()
            );
            assert_eq!(
                ninja.column_name_as_int(&Columns::Vol(VolumeProfileColumnNames::Quantity)),
                result.index_of("qx").unwrap()
            );
        }
    }
}
