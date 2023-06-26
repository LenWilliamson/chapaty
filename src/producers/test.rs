// Intern crates
use super::*;

/// The `Test` data provider uses market data from the `Test` crypto exchange. It is exclusively
/// used for testing.
///
/// # Links
/// * Binance: https://www.test.com/en
/// * Git repository to `Test` Public Data: https://github.com/binance/binance-public-data
/// * `Test` Market Data: https://data.test.vision
pub struct Test {
    bucket: PathBuf,
    producer_kind: ProducerKind,
}

impl FromStr for Test {
    type Err = enums::error::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Test" | "test" => Ok(Test::new(std::path::PathBuf::from(
                config::GCS_DATA_BUCKET,
            ))),
            _ => Err(Self::Err::ParseDataProducerError(
                "Data Producer Does not Exists".to_string(),
            )),
        }
    }
}

impl Test {
    #[allow(dead_code)]
    /// Creates a `Test` data producer.
    /// # Arguments
    /// * `client` - google cloud storage client
    /// * `bucket` - cloud storage bucket name
    pub fn new(bucket: PathBuf) -> Self {
        Test {
            bucket,
            producer_kind: ProducerKind::Test,
        }
    }
}

#[async_trait]
impl DataProducer for Test {
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
            LeafDir::Ohlcv1m | LeafDir::Ohlcv30m | LeafDir::Ohlcv60m => ohlcv_schema(),
            LeafDir::Tick => panic!("DataKind::Tick not yet implemented for DataProducer Test"),
            LeafDir::AggTrades => aggtrade_schema(),
            LeafDir::Vol => vol_schema(),
            LeafDir::ProfitAndLoss => {
                panic!("Not implemented by DataProvider. TODO Improve API")
            }
        }
    }

    fn column_name_as_str(&self, col: &Columns) -> String {
        match col {
            Columns::Ohlcv(c) => ohlcv_column_name_as_str(c),
            Columns::Ohlc(c) => ohlc_column_name_as_str(c),
            Columns::AggTrade(c) => aggtrade_column_name_as_str(c),
            Columns::Vol(c) => vol_column_name_as_str(c),
        }
    }

    fn column_name_as_int(&self, col: &Columns) -> usize {
        match col {
            Columns::Ohlcv(c) => usize::try_from(*c as u8).unwrap(),
            Columns::Ohlc(c) => usize::try_from(*c as u8).unwrap(),
            Columns::AggTrade(c) => usize::try_from(*c as u8).unwrap(),
            Columns::Vol(c) => usize::try_from(*c as u8).unwrap(),
        }
    }

    async fn get_df(&self, file: &PathBuf, data: &LeafDir) -> Result<DataFrame, Error> {
        df_from_file(file, Some(self.schema(data)), None).await
    }

    fn get_ts_col_as_str(&self, data: &LeafDir) -> String {
        match data {
            LeafDir::Ohlc1m | LeafDir::Ohlc30m | LeafDir::Ohlc60m => self.column_name_as_str(&Columns::Ohlc(OhlcColumnNames::OpenTime)),
            LeafDir::Ohlcv1m | LeafDir::Ohlcv30m | LeafDir::Ohlcv60m => {
                self.column_name_as_str(&Columns::Ohlcv(OhlcvColumnNames::OpenTime))
            }
            LeafDir::Tick => panic!("Tick data not yet supported."),
            LeafDir::AggTrades => {
                self.column_name_as_str(&Columns::AggTrade(AggTradeColumnNames::Timestamp))
            }
            LeafDir::Vol => panic!("No timestamp for volume."),
            LeafDir::ProfitAndLoss => {
                panic!("Not implemented by DataProvider. TODO Improve API")
            }
        }
    }
}

/// Returns the OHLC `Schema` for `Test`
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

/// Returns the OHLCV `Schema` for `Test`
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

/// Returns the AggTrades `Schema` for `Test`
fn aggtrade_schema() -> Schema {
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

/// Returns the volume profile `Schema` for `Test`
fn vol_schema() -> Schema {
    Schema::from_iter(
        vec![
            Field::new("px", DataType::Float64),
            Field::new("qx", DataType::Float64),
        ]
        .into_iter(),
    )
}

/// Returns the OHLC coloumn name as `String` for a `DataFrame` provided by `Test`
/// # Arguments
/// * `c` - Column name we want to obtain a `String` value for
fn ohlc_column_name_as_str(c: &OhlcColumnNames) -> String {
    match c {
        OhlcColumnNames::OpenTime => String::from("ots"),
        OhlcColumnNames::Open => String::from("open"),
        OhlcColumnNames::High => String::from("high"),
        OhlcColumnNames::Low => String::from("low"),
        OhlcColumnNames::Close => String::from("close"),
        // OhlcColumnNames::Volume => String::from("vol"),
        OhlcColumnNames::CloseTime => String::from("cts"),
        // OhlcColumnNames::QuoteAssetVol => String::from("qav"),
        // OhlcColumnNames::NumberOfTrades => String::from("not"),
        // OhlcColumnNames::TakerBuyBaseAssetVol => String::from("tbbav"),
        // OhlcColumnNames::TakerBuyQuoteAssetVol => String::from("tbqav"),
        // OhlcColumnNames::Ignore => String::from("ignore"),
    }
}

/// Returns the OHLCV coloumn name as `String` for a `DataFrame` provided by `Test`
/// # Arguments
/// * `c` - Column name we want to obtain a `String` value for
fn ohlcv_column_name_as_str(c: &OhlcvColumnNames) -> String {
    match c {
        OhlcvColumnNames::OpenTime => String::from("ots"),
        OhlcvColumnNames::Open => String::from("open"),
        OhlcvColumnNames::High => String::from("high"),
        OhlcvColumnNames::Low => String::from("low"),
        OhlcvColumnNames::Close => String::from("close"),
        OhlcvColumnNames::Volume => String::from("vol"),
        OhlcvColumnNames::CloseTime => String::from("cts"),
        // OhlcvColumnNames::QuoteAssetVol => String::from("qav"),
        // OhlcvColumnNames::NumberOfTrades => String::from("not"),
        // OhlcvColumnNames::TakerBuyBaseAssetVol => String::from("tbbav"),
        // OhlcvColumnNames::TakerBuyQuoteAssetVol => String::from("tbqav"),
        // OhlcvColumnNames::Ignore => String::from("ignore"),
    }
}

/// Returns the volume profile coloumn name as `String` for a `DataFrame` provided by `Test`
/// # Arguments
/// * `c` - Column name we want to obtain a `String` value for
fn vol_column_name_as_str(c: &VolumeProfileColumnNames) -> String {
    match c {
        VolumeProfileColumnNames::Price => String::from("px"),
        VolumeProfileColumnNames::Quantity => String::from("qx"),
    }
}

/// Returns the AggTrades coloumn name as `String` for a `DataFrame` provided by `Test`
/// # Arguments
/// * `c` - Column name we want to obtain a `String` value for
fn aggtrade_column_name_as_str(c: &AggTradeColumnNames) -> String {
    match c {
        AggTradeColumnNames::AggTradeId => String::from("atid"),
        AggTradeColumnNames::Price => String::from("px"),
        AggTradeColumnNames::Quantity => String::from("qx"),
        AggTradeColumnNames::FirstTradeId => String::from("ftid"),
        AggTradeColumnNames::LastTradeId => String::from("ltid"),
        AggTradeColumnNames::Timestamp => String::from("ts"),
        AggTradeColumnNames::BuyerEqualsMaker => String::from("bm"),
        AggTradeColumnNames::BestTradePriceMatch => String::from("btpm"),
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use crate::config::GCS_DATA_BUCKET;
    #[allow(unused_imports)]
    use super::*;

    #[allow(unused_imports)]
    use::polars::prelude::IndexOfSchema;

    #[cfg(test)]
    mod agg_trades {
        use crate::config;

        use super::*;

        /// This unit test checks if the schema for the aggTrades data by `Test` matches. We also check the order of the schema elements.
        /// The `.csv` format is
        ///
        /// ```
        /// // idx = Index
        /// // id = Uniqe identifier
        /// // val = Example value inside a .csv file
        /// idx:    0           ,1          ,2      ,3          ,4          ,5              ,6      ,7
        /// id:     atid        ,px         ,qx     ,ftid       ,ltid       ,ts             ,bm     ,btpm
        /// val:    1047037960  ,38200.00   ,200.00 ,1207691977 ,1207691977 ,1645300600000  ,false  ,true
        /// ```
        /// For more information see:
        /// * https://github.com/binance/binance-public-data/#aggtrades
        /// * Data Source: https://data.test.vision/ (for more details: https://github.com/binance/binance-public-data/#where-do-i-access-it)
        ///
        /// # Note
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
            let test = Test {
                bucket: PathBuf::from(GCS_DATA_BUCKET),
                producer_kind: ProducerKind::Test,
            };
            let result = test.schema(&LeafDir::AggTrades);
            assert_eq!(result.index_of("atid").unwrap(), 0);
            assert_eq!(result.index_of("px").unwrap(), 1);
            assert_eq!(result.index_of("qx").unwrap(), 2);
            assert_eq!(result.index_of("ftid").unwrap(), 3);
            assert_eq!(result.index_of("ltid").unwrap(), 4);
            assert_eq!(result.index_of("ts").unwrap(), 5);
            assert_eq!(result.index_of("bm").unwrap(), 6);
            assert_eq!(result.index_of("btpm").unwrap(), 7);
        }

        /// This unit test checks if the enum types for the aggTrades data by `Test` matches with our id for the column names.
        /// The `.csv` format is
        ///
        /// ```
        /// // idx = Index
        /// // id = Uniqe identifier
        /// // val = Example value inside a .csv file
        /// idx:    0           ,1          ,2      ,3          ,4          ,5              ,6      ,7
        /// id:     atid        ,px         ,qx     ,ftid       ,ltid       ,ts             ,bm     ,btpm
        /// val:    1047037960  ,38200.00   ,200.00 ,1207691977 ,1207691977 ,1645300600000  ,false  ,true
        /// ```
        #[tokio::test]
        async fn test_column_name_as_str() {
            let test = Test {
                bucket: PathBuf::from(GCS_DATA_BUCKET),
                producer_kind: ProducerKind::Test,
            };
            assert_eq!(
                test.column_name_as_str(&Columns::AggTrade(AggTradeColumnNames::AggTradeId)),
                String::from("atid")
            );
            assert_eq!(
                test.column_name_as_str(&Columns::AggTrade(AggTradeColumnNames::Price)),
                String::from("px")
            );
            assert_eq!(
                test.column_name_as_str(&Columns::AggTrade(AggTradeColumnNames::Quantity)),
                String::from("qx")
            );
            assert_eq!(
                test.column_name_as_str(&Columns::AggTrade(AggTradeColumnNames::FirstTradeId)),
                String::from("ftid")
            );
            assert_eq!(
                test.column_name_as_str(&Columns::AggTrade(AggTradeColumnNames::LastTradeId)),
                String::from("ltid")
            );
            assert_eq!(
                test.column_name_as_str(&Columns::AggTrade(AggTradeColumnNames::Timestamp)),
                String::from("ts")
            );
            assert_eq!(
                test.column_name_as_str(&Columns::AggTrade(AggTradeColumnNames::BuyerEqualsMaker)),
                String::from("bm")
            );
            assert_eq!(
                test.column_name_as_str(&Columns::AggTrade(AggTradeColumnNames::BestTradePriceMatch)),
                String::from("btpm")
            );
        }

        /// This unit test checks if the order of the column ids actually match with the order of the loaded schema for the aggTrades data by test.
        /// The `.csv` format is
        ///
        /// ```
        /// // idx = Index
        /// // id = Uniqe identifier
        /// // val = Example value inside a .csv file
        /// idx:    0           ,1          ,2      ,3          ,4          ,5              ,6      ,7
        /// id:     atid        ,px         ,qx     ,ftid       ,ltid       ,ts             ,bm     ,btpm
        /// val:    1047037960  ,38200.00   ,200.00 ,1207691977 ,1207691977 ,1645300600000  ,false  ,true
        /// ```
        /// For more information see:
        /// * https://github.com/binance/binance-public-data/#aggtrades
        /// * Data Source: https://data.test.vision/ (for more details: https://github.com/binance/binance-public-data/#where-do-i-access-it)
        #[tokio::test]
        async fn test_column_name_as_int() {
            let test = Test {
                bucket: PathBuf::from(GCS_DATA_BUCKET),
                producer_kind: ProducerKind::Test,
            };
            let result = test.schema(&LeafDir::AggTrades);
            assert_eq!(
                test.column_name_as_int(&Columns::AggTrade(AggTradeColumnNames::AggTradeId)),
                result.index_of("atid").unwrap()
            );
            assert_eq!(
                test.column_name_as_int(&Columns::AggTrade(AggTradeColumnNames::Price)),
                result.index_of("px").unwrap()
            );
            assert_eq!(
                test.column_name_as_int(&Columns::AggTrade(AggTradeColumnNames::Quantity)),
                result.index_of("qx").unwrap()
            );
            assert_eq!(
                test.column_name_as_int(&Columns::AggTrade(AggTradeColumnNames::FirstTradeId)),
                result.index_of("ftid").unwrap()
            );
            assert_eq!(
                test.column_name_as_int(&Columns::AggTrade(AggTradeColumnNames::LastTradeId)),
                result.index_of("ltid").unwrap()
            );
            assert_eq!(
                test.column_name_as_int(&Columns::AggTrade(AggTradeColumnNames::Timestamp)),
                result.index_of("ts").unwrap()
            );
            assert_eq!(
                test.column_name_as_int(&Columns::AggTrade(AggTradeColumnNames::BuyerEqualsMaker)),
                result.index_of("bm").unwrap()
            );
            assert_eq!(
                test.column_name_as_int(&Columns::AggTrade(AggTradeColumnNames::BestTradePriceMatch)),
                result.index_of("btpm").unwrap()
            );
        }
    }

    #[cfg(test)]
    mod ohlc {
        use super::*;
        use crate::config;
        /// This unit test checks if the schema for the OHLC data by `Test` matches. We also check the order of the schema elements.
        /// The `.csv` format is
        ///
        /// ```
        /// // idx = Index
        /// // id = Uniqe identifier
        /// // val = Example value inside a .csv file
        /// idx:    0               ,1          ,2          ,3          ,4          ,5              ,6      ,7      ,8      ,9      ,10     ,11
        /// id:     ots             ,open       ,high       ,low        ,close      ,vol            ,cts    ,qav    ,not    ,tbbav  ,tbqav  ,ignore
        /// val:    1643673600000   ,38466.90   ,38627.35   ,38276.43   ,38342.36   ,1058.42599000  ,...    ,...    ,...    ,...    ,...    ,0
        /// ```
        /// For more information see:
        /// * https://github.com/binance/binance-public-data/#klines
        /// * Data Source: https://data.test.vision/ (for more details: https://github.com/binance/binance-public-data/#where-do-i-access-it)
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
            let test = Test {
                bucket: PathBuf::from(GCS_DATA_BUCKET),
                producer_kind: ProducerKind::Test,
            };
            let result = test.schema(&LeafDir::Ohlc60m);
            assert_eq!(result.index_of("ots").unwrap(), 0);
            assert_eq!(result.index_of("open").unwrap(), 1);
            assert_eq!(result.index_of("high").unwrap(), 2);
            assert_eq!(result.index_of("low").unwrap(), 3);
            assert_eq!(result.index_of("close").unwrap(), 4);
            assert_eq!(result.index_of("vol").unwrap(), 5);
            assert_eq!(result.index_of("cts").unwrap(), 6);
            assert_eq!(result.index_of("qav").unwrap(), 7);
            assert_eq!(result.index_of("not").unwrap(), 8);
            assert_eq!(result.index_of("tbbav").unwrap(), 9);
            assert_eq!(result.index_of("tbqav").unwrap(), 10);
            assert_eq!(result.index_of("ignore").unwrap(), 11);
        }

        /// This unit test checks if the enum types for the OHLC data by `Test` matches with our id for the column names.
        /// The `.csv` format is
        ///
        /// ```
        /// // idx = Index
        /// // id = Uniqe identifier
        /// // val = Example value inside a .csv file
        /// idx:    0               ,1          ,2          ,3          ,4          ,5              ,6      ,7      ,8      ,9      ,10     ,11
        /// id:     ots             ,open       ,high       ,low        ,close      ,vol            ,cts    ,qav    ,not    ,tbbav  ,tbqav  ,ignore
        /// val:    1643673600000   ,38466.90   ,38627.35   ,38276.43   ,38342.36   ,1058.42599000  ,...    ,...    ,...    ,...    ,...    ,0
        /// ```
        #[tokio::test]
        async fn test_column_name_as_str() {
            let test = Test {
                bucket: PathBuf::from(GCS_DATA_BUCKET),
                producer_kind: ProducerKind::Test,
            };
            assert_eq!(
                test.column_name_as_str(&Columns::Ohlc(OhlcColumnNames::OpenTime)),
                String::from("ots")
            );
            assert_eq!(
                test.column_name_as_str(&Columns::Ohlc(OhlcColumnNames::Open)),
                String::from("open")
            );
            assert_eq!(
                test.column_name_as_str(&Columns::Ohlc(OhlcColumnNames::High)),
                String::from("high")
            );
            assert_eq!(
                test.column_name_as_str(&Columns::Ohlc(OhlcColumnNames::Low)),
                String::from("low")
            );
            assert_eq!(
                test.column_name_as_str(&Columns::Ohlc(OhlcColumnNames::Close)),
                String::from("close")
            );
            // assert_eq!(
            //     Ohlc::column_name_as_str(&OhlcColumnNames::inance.Volume),
            //     String::from("vol")
            // );
            assert_eq!(
                test.column_name_as_str(&Columns::Ohlc(OhlcColumnNames::CloseTime)),
                String::from("cts")
            );
            // assert_eq!(
            //     Ohlc::column_name_as_str(&OhlcColumnNames::inance.QuoteAssetVol),
            //     String::from("qav")
            // );
            // assert_eq!(
            //     Ohlc::column_name_as_str(&OhlcColumnNames::inance.NumberOfTrades),
            //     String::from("not")
            // );
            // assert_eq!(
            //     Ohlc::column_name_as_str(&OhlcColumnNames::inance.TakerBuyBaseAssetVol),
            //     String::from("tbbav")
            // );
            // assert_eq!(
            //     Ohlc::column_name_as_str(&OhlcColumnNames::inance.TakerBuyQuoteAssetVol),
            //     String::from("tbqav")
            // );
            // assert_eq!(
            //     Ohlc::column_name_as_str(&OhlcColumnNames::inance.Ignore),
            //     String::from("ignore")
            // );
        }

        /// This unit test checks if the order of the column ids actually match with the order of the loaded schema for the OHLC data by test.
        /// The `.csv` format is
        ///
        /// ```
        /// // idx = Index
        /// // id = Uniqe identifier
        /// // val = Example value inside a .csv file
        /// idx:    0               ,1          ,2          ,3          ,4          ,5              ,6      ,7      ,8      ,9      ,10     ,11
        /// id:     ots             ,open       ,high       ,low        ,close      ,vol            ,cts    ,qav    ,not    ,tbbav  ,tbqav  ,ignore
        /// val:    1643673600000   ,38466.90   ,38627.35   ,38276.43   ,38342.36   ,1058.42599000  ,...    ,...    ,...    ,...    ,...    ,0
        /// ```
        /// For more information see:
        /// * https://github.com/binance/binance-public-data/#klines
        /// * Data Source: https://data.test.vision/ (for more details: https://github.com/binance/binance-public-data/#where-do-i-access-it)
        #[tokio::test]
        async fn test_column_name_as_int() {
            let test = Test {
                bucket: PathBuf::from(GCS_DATA_BUCKET),
                producer_kind: ProducerKind::Test,
            };
            let result = test.schema(&LeafDir::Ohlc60m);
            assert_eq!(
                test.column_name_as_int(&Columns::Ohlc(OhlcColumnNames::OpenTime)),
                result.index_of("ots").unwrap()
            );
            assert_eq!(
                test.column_name_as_int(&Columns::Ohlc(OhlcColumnNames::Open)),
                result.index_of("open").unwrap()
            );
            assert_eq!(
                test.column_name_as_int(&Columns::Ohlc(OhlcColumnNames::High)),
                result.index_of("high").unwrap()
            );
            assert_eq!(
                test.column_name_as_int(&Columns::Ohlc(OhlcColumnNames::Low)),
                result.index_of("low").unwrap()
            );
            assert_eq!(
                test.column_name_as_int(&Columns::Ohlc(OhlcColumnNames::Close)),
                result.index_of("close").unwrap()
            );
            // assert_eq!(
            //     Ohlc::column_name_as_int(&OhlcColumnNames::inance.Volume),
            //     result.index_of("vol").unwrap()
            // );
            assert_eq!(
                test.column_name_as_int(&Columns::Ohlc(OhlcColumnNames::CloseTime)),
                result.index_of("cts").unwrap()
            );
            // assert_eq!(
            //     Ohlc::column_name_as_int(&OhlcColumnNames::inance.QuoteAssetVol),
            //     result.index_of("qav").unwrap()
            // );
            // assert_eq!(
            //     Ohlc::column_name_as_int(&OhlcColumnNames::inance.NumberOfTrades),
            //     result.index_of("not").unwrap()
            // );
            // assert_eq!(
            //     Ohlc::column_name_as_int(&OhlcColumnNames::inance.TakerBuyBaseAssetVol),
            //     result.index_of("tbbav").unwrap()
            // );
            // assert_eq!(
            //     Ohlc::column_name_as_int(&OhlcColumnNames::inance.TakerBuyQuoteAssetVol),
            //     result.index_of("tbqav").unwrap()
            // );
            // assert_eq!(
            //     Ohlc::column_name_as_int(&OhlcColumnNames::inance.Ignore),
            //     result.index_of("ignore").unwrap()
            // );
        }
    }

    #[cfg(test)]
    mod ohlcv {
        use super::*;
        use crate::config;
        /// This unit test checks if the schema for the OHLC data by `Test` matches. We also check the order of the schema elements.
        /// The `.csv` format is
        ///
        /// ```
        /// // idx = Index
        /// // id = Uniqe identifier
        /// // val = Example value inside a .csv file
        /// idx:    0               ,1          ,2          ,3          ,4          ,5              ,6      ,7      ,8      ,9      ,10     ,11
        /// id:     ots             ,open       ,high       ,low        ,close      ,vol            ,cts    ,qav    ,not    ,tbbav  ,tbqav  ,ignore
        /// val:    1643673600000   ,38466.90   ,38627.35   ,38276.43   ,38342.36   ,1058.42599000  ,...    ,...    ,...    ,...    ,...    ,0
        /// ```
        /// For more information see:
        /// * https://github.com/binance/binance-public-data/#klines
        /// * Data Source: https://data.test.vision/ (for more details: https://github.com/binance/binance-public-data/#where-do-i-access-it)
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
            let test = Test {
                bucket: PathBuf::from(GCS_DATA_BUCKET),
                producer_kind: ProducerKind::Test,
            };
            let result = test.schema(&LeafDir::Ohlcv60m);
            assert_eq!(result.index_of("ots").unwrap(), 0);
            assert_eq!(result.index_of("open").unwrap(), 1);
            assert_eq!(result.index_of("high").unwrap(), 2);
            assert_eq!(result.index_of("low").unwrap(), 3);
            assert_eq!(result.index_of("close").unwrap(), 4);
            assert_eq!(result.index_of("vol").unwrap(), 5);
            assert_eq!(result.index_of("cts").unwrap(), 6);
            assert_eq!(result.index_of("qav").unwrap(), 7);
            assert_eq!(result.index_of("not").unwrap(), 8);
            assert_eq!(result.index_of("tbbav").unwrap(), 9);
            assert_eq!(result.index_of("tbqav").unwrap(), 10);
            assert_eq!(result.index_of("ignore").unwrap(), 11);
        }

        /// This unit test checks if the enum types for the OHLC data by `Test` matches with our id for the column names.
        /// The `.csv` format is
        ///
        /// ```
        /// // idx = Index
        /// // id = Uniqe identifier
        /// // val = Example value inside a .csv file
        /// idx:    0               ,1          ,2          ,3          ,4          ,5              ,6      ,7      ,8      ,9      ,10     ,11
        /// id:     ots             ,open       ,high       ,low        ,close      ,vol            ,cts    ,qav    ,not    ,tbbav  ,tbqav  ,ignore
        /// val:    1643673600000   ,38466.90   ,38627.35   ,38276.43   ,38342.36   ,1058.42599000  ,...    ,...    ,...    ,...    ,...    ,0
        /// ```
        #[tokio::test]
        async fn test_column_name_as_str() {
            let test = Test {
                bucket: PathBuf::from(GCS_DATA_BUCKET),
                producer_kind: ProducerKind::Test,
            };
            assert_eq!(
                test.column_name_as_str(&Columns::Ohlcv(OhlcvColumnNames::OpenTime)),
                String::from("ots")
            );
            assert_eq!(
                test.column_name_as_str(&Columns::Ohlcv(OhlcvColumnNames::Open)),
                String::from("open")
            );
            assert_eq!(
                test.column_name_as_str(&Columns::Ohlcv(OhlcvColumnNames::High)),
                String::from("high")
            );
            assert_eq!(
                test.column_name_as_str(&Columns::Ohlcv(OhlcvColumnNames::Low)),
                String::from("low")
            );
            assert_eq!(
                test.column_name_as_str(&Columns::Ohlcv(OhlcvColumnNames::Close)),
                String::from("close")
            );
            assert_eq!(
                test.column_name_as_str(&Columns::Ohlcv(OhlcvColumnNames::Volume)),
                String::from("vol")
            );
            assert_eq!(
                test.column_name_as_str(&Columns::Ohlcv(OhlcvColumnNames::CloseTime)),
                String::from("cts")
            );
            // assert_eq!(
            //     Ohlcv::column_name_as_str(&OhlcvColumnNames::inance.QuoteAssetVol),
            //     String::from("qav")
            // );
            // assert_eq!(
            //     Ohlcv::column_name_as_str(&OhlcvColumnNames::inance.NumberOfTrades),
            //     String::from("not")
            // );
            // assert_eq!(
            //     Ohlcv::column_name_as_str(&OhlcvColumnNames::inance.TakerBuyBaseAssetVol),
            //     String::from("tbbav")
            // );
            // assert_eq!(
            //     Ohlcv::column_name_as_str(&OhlcvColumnNames::inance.TakerBuyQuoteAssetVol),
            //     String::from("tbqav")
            // );
            // assert_eq!(
            //     Ohlcv::column_name_as_str(&OhlcvColumnNames::inance.Ignore),
            //     String::from("ignore")
            // );
        }

        /// This unit test checks if the order of the column ids actually match with the order of the loaded schema for the Ohlcv data by test.
        /// The `.csv` format is
        ///
        /// ```
        /// // idx = Index
        /// // id = Uniqe identifier
        /// // val = Example value inside a .csv file
        /// idx:    0               ,1          ,2          ,3          ,4          ,5              ,6      ,7      ,8      ,9      ,10     ,11
        /// id:     ots             ,open       ,high       ,low        ,close      ,vol            ,cts    ,qav    ,not    ,tbbav  ,tbqav  ,ignore
        /// val:    1643673600000   ,38466.90   ,38627.35   ,38276.43   ,38342.36   ,1058.42599000  ,...    ,...    ,...    ,...    ,...    ,0
        /// ```
        /// For more information see:
        /// * https://github.com/binance/binance-public-data/#klines
        /// * Data Source: https://data.test.vision/ (for more details: https://github.com/binance/binance-public-data/#where-do-i-access-it)
        #[tokio::test]
        async fn test_column_name_as_int() {
            let test = Test {
                bucket: PathBuf::from(GCS_DATA_BUCKET),
                producer_kind: ProducerKind::Test,
            };
            let result = test.schema(&LeafDir::Ohlcv60m);
            assert_eq!(
                test.column_name_as_int(&Columns::Ohlcv(OhlcvColumnNames::OpenTime)),
                result.index_of("ots").unwrap()
            );
            assert_eq!(
                test.column_name_as_int(&Columns::Ohlcv(OhlcvColumnNames::Open)),
                result.index_of("open").unwrap()
            );
            assert_eq!(
                test.column_name_as_int(&Columns::Ohlcv(OhlcvColumnNames::High)),
                result.index_of("high").unwrap()
            );
            assert_eq!(
                test.column_name_as_int(&Columns::Ohlcv(OhlcvColumnNames::Low)),
                result.index_of("low").unwrap()
            );
            assert_eq!(
                test.column_name_as_int(&Columns::Ohlcv(OhlcvColumnNames::Close)),
                result.index_of("close").unwrap()
            );
            assert_eq!(
                test.column_name_as_int(&Columns::Ohlcv(OhlcvColumnNames::Volume)),
                result.index_of("vol").unwrap()
            );
            assert_eq!(
                test.column_name_as_int(&Columns::Ohlcv(OhlcvColumnNames::CloseTime)),
                result.index_of("cts").unwrap()
            );
            // assert_eq!(
            //     Ohlcv::column_name_as_int(&OhlcvColumnNames::inance.QuoteAssetVol),
            //     result.index_of("qav").unwrap()
            // );
            // assert_eq!(
            //     Ohlcv::column_name_as_int(&OhlcvColumnNames::inance.NumberOfTrades),
            //     result.index_of("not").unwrap()
            // );
            // assert_eq!(
            //     Ohlcv::column_name_as_int(&OhlcvColumnNames::inance.TakerBuyBaseAssetVol),
            //     result.index_of("tbbav").unwrap()
            // );
            // assert_eq!(
            //     Ohlcv::column_name_as_int(&OhlcvColumnNames::inance.TakerBuyQuoteAssetVol),
            //     result.index_of("tbqav").unwrap()
            // );
            // assert_eq!(
            //     Ohlcv::column_name_as_int(&OhlcvColumnNames::inance.Ignore),
            //     result.index_of("ignore").unwrap()
            // );
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
            let test = Test {
                bucket: PathBuf::from(GCS_DATA_BUCKET),
                producer_kind: ProducerKind::Test,
            };
            let result = test.schema(&LeafDir::Vol);
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
            let test = Test {
                bucket: PathBuf::from(GCS_DATA_BUCKET),
                producer_kind: ProducerKind::Test,
            };
            assert_eq!(
                test.column_name_as_str(&Columns::Vol(VolumeProfileColumnNames::Price)),
                String::from("px")
            );
            assert_eq!(
                test.column_name_as_str(&Columns::Vol(VolumeProfileColumnNames::Quantity)),
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
            let test = Test {
                bucket: PathBuf::from(GCS_DATA_BUCKET),
                producer_kind: ProducerKind::Test,
            };
            
            let result = test.schema(&LeafDir::Vol);
            assert_eq!(
                test.column_name_as_int(&Columns::Vol(VolumeProfileColumnNames::Price)),
                result.index_of("px").unwrap()
            );
            assert_eq!(
                test.column_name_as_int(&Columns::Vol(VolumeProfileColumnNames::Quantity)),
                result.index_of("qx").unwrap()
            );
        }
    }
}