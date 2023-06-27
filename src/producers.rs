pub mod binance;
pub mod ninja;
pub mod performance_report;
pub mod profit_loss_report;
pub mod test;

use crate::{
    common::functions::df_from_file,
    config,
    enums::{
        self,
        columns::{
            AggTradeColumnNames, Columns, OhlcColumnNames, OhlcvColumnNames,
            VolumeProfileColumnNames,
        },
        data::{ LeafDir},
        producers::ProducerKind,
    },
};

// Extern crates.
use async_trait::async_trait;
use google_cloud_storage::{client::Client, http::Error};
use polars::prelude::{DataFrame, DataType, Field, Schema};
use std::{path::PathBuf, str::FromStr, sync::Arc};

use self::ninja::Ninja;

#[async_trait]
/// The `DataProducer` crate contains methods that any
/// producer has to implement. They are the minimal set of operations
/// a `DataProducer` is expected to have.
pub trait DataProducer {
    /// **WARNING:** Not implemented. Will be part of an upcoming release.
    ///
    /// # Note
    /// All `.csv` files, but
    /// * `transform_ninja_test_file.csv` - a test file that transform a raw ninja `.csv` to a `DataFrame`
    /// * `"bucket"/data/{producer}/{market}/{year}/{aggTrades | ohlc-{ts} | ...}` - all raw data `.csv` files
    ///
    /// have a header.
    fn has_header(&self);

    // Returns an owning reference of the Google Cloud Storage `Client`.
    // # Example
    // ```
    // // Initialize a `DataProducer`
    // let binance = Binance {
    //    client: Arc::new(config::get_google_cloud_client().await),
    //    bucket: PathBuf::from(GCS_DATA_BUCKET),
    //    producer_kind: ProducerKind::Binance,
    //};
    //
    // // Typically we clone the client, to share some immutable data between threads:
    // for _ in 0..10 {
    // let clone = binance.get_client_clone();
    //
    // tokio::spawn(async move || {
    //     // Do stuff ...
    // });
    //
    // ```
    // fn get_client_clone(&self) -> Arc<Client>;

    /// Returns the Google Cloud Storage bucket name for this `DataProducer`
    /// # Example
    /// ```
    /// // Initialize a `DataProducer`
    /// let binance = Binance {
    ///    client: Arc::new(config::get_google_cloud_client().await),
    ///    bucket: PathBuf::from("bucket_name"),
    ///    producer_kind: ProducerKind::Binance,
    ///};
    ///
    /// let bucket_name = binance.get_bucket_name().to_str().unwrap();
    /// assert_eq!("bucket_name", bucket_name);
    /// ```
    fn get_bucket_name(&self) -> PathBuf;

    /// Returns type `ProducerKind` of `self` to determine what kind of `DataProducer`
    /// is currently used
    /// # Example
    /// ```
    /// // Initialize a `DataProducer`
    /// let binance = Binance {
    ///    client: Arc::new(config::get_google_cloud_client().await),
    ///    bucket: PathBuf::from("bucket_name"),
    ///    producer_kind: ProducerKind::Binance,
    ///};
    ///
    /// let dpk = binance.get_data_producer_kind();
    /// assert!(matches!(dpk, ProducerKind::Binance));
    /// ```
    fn get_data_producer_kind(&self) -> ProducerKind;

    /// Returns the `Schema` for the given `data`. The `LeafDir`s define what kind
    /// of `Schema` we have to load (e.g. to parse a `DataFrame` from a `.csv` file).
    ///
    /// # Arguments
    /// * `data` - defines which `Schema` we want to load
    ///
    /// # Example
    /// If ` data` is set to `LeafDir::Vol`, we obtain the `Schema` for a volume profile.
    /// ```
    /// // Initialize a `DataProducer`
    /// let binance = Binance {
    ///    client: Arc::new(config::get_google_cloud_client().await),
    ///    bucket: PathBuf::from(GCS_DATA_BUCKET),
    ///    producer_kind: ProducerKind::Binance,
    ///};
    ///
    /// // Load `Schema` for `data` = `LeafDir::Vol`
    /// let result = binance.schema(&LeafDir::Vol);
    /// let target = Schema::from(vec![
    ///     Field::new("px", DataType::Float64),
    ///     Field::new("qx", DataType::Float64),
    ///     ].into_iter()
    /// );
    ///
    /// assert_eq!(result, target);
    /// ```
    fn schema(&self, data: &LeafDir) -> Schema;

    /// Returns the `String` value of a `Field` for a `DataFrame` for a given `Schema`.
    ///
    /// # Arguments
    /// * `col` - column we want to get a `String` value for
    ///
    /// # Example
    /// If ` col` is set to `Columns::Vol(VolumeProfileColumnNames::Price)`, we obtain the `String`
    /// value for the price column of a volume profile. For the following example we have
    /// ```
    /// Schema::from(vec![
    ///     Field::new("px", DataType::Float64),
    ///     Field::new("qx", DataType::Float64),
    ///     ].into_iter()
    /// );
    /// ```
    /// as the underlying `Schema`.
    /// ```
    /// // Initialize a `DataProducer`
    /// let binance = Binance {
    ///    client: Arc::new(config::get_google_cloud_client().await),
    ///    bucket: PathBuf::from(GCS_DATA_BUCKET),
    ///    producer_kind: ProducerKind::Binance,
    ///};
    ///
    /// // Get the `String` value of the price column for volume profile `.csv`/`DataFrame`
    /// let col = Columns::Vol(VolumeProfileColumnNames::Price);
    /// let px_col = binance.column_name_as_str(&col);
    /// assert_eq!(String::from("px"), px_col);
    ///
    /// // Note, for this example we have the underlying Schema given above. A `.csv` could look like:
    /// // idx = Index (obtained by `column_as_int()`)
    /// // id = Uniqe identifier (obtained by `column_as_str()`)
    /// // val = Example value inside a `.csv` file
    /// idx:    0       ,1
    /// id:     px      ,qx
    /// val:    100.00  ,38.032
    /// ```
    fn column_name_as_str(&self, col: &Columns) -> String;

    /// Returns the `usize` value of a `Field` for a `DataFrame` for a given `Schema`.
    ///
    /// # Arguments
    /// * `col` - column we want to get a `usize` value for
    ///
    /// # Example
    /// If ` col` is set to `Columns::Vol(VolumeProfileColumnNames::Price)`, we obtain the `usize`
    /// value for the price column of a volume profile. For the following example we have
    /// ```
    /// Schema::from(vec![
    ///     Field::new("px", DataType::Float64),
    ///     Field::new("qx", DataType::Float64),
    ///     ].into_iter()
    /// );
    /// ```
    /// as the underlying `Schema`.
    /// ```
    /// // Initialize a `DataProducer`
    /// let binance = Binance {
    ///    client: Arc::new(config::get_google_cloud_client().await),
    ///    bucket: PathBuf::from(GCS_DATA_BUCKET),
    ///    producer_kind: ProducerKind::Binance,
    /// };
    ///
    /// // Get the `usize` value of the price column for volume profile `.csv`/`DataFrame`
    /// let col = Columns::Vol(VolumeProfileColumnNames::Price);
    /// let px_col_idx = binance.column_name_as_int(&col);
    /// assert_eq!(0, px_col_idx);
    ///
    /// // Note, for this example we have the underlying Schema given above. A `.csv` could look like:
    /// // idx = Index (obtained by `column_as_int()`)
    /// // id = Uniqe identifier (obtained by `column_as_str()`)
    /// // val = Example value inside a `.csv` file
    /// idx:    0       ,1
    /// id:     px      ,qx
    /// val:    100.00  ,38.032
    /// ```
    fn column_name_as_int(&self, col: &Columns) -> usize;

    /// Returns the `String` value of the `{OpenTime | Timestamp | ...}` column
    /// for a `DataFrame` for a given `Schema`.
    ///
    /// # Note
    /// If a `DataFrame` has a opening and closing timestamp, we return the opening timestamp
    /// column name as `String`.
    ///
    /// # Arguments
    /// * `data` - defines which `Schema` to look into
    ///
    /// # Example
    /// If ` data` is set to `Columns::Ohlc(OhlcColumnNames::OpenTime)`, we obtain the `String` value for
    /// the timestamp column. For the following example we have
    /// ```
    /// Schema::from(vec![
    ///     Field::new("ots", DataType::Int64),
    ///     Field::new("open", DataType::Float64),
    ///     // Some more fields, including a `"cts"` field for the closing timestamp
    ///     ].into_iter()
    /// );
    /// ```
    /// as the underlying `Schema`.
    /// ```
    /// // Initialize a `DataProducer`
    /// let binance = Binance {
    ///    client: Arc::new(config::get_google_cloud_client().await),
    ///    bucket: PathBuf::from(GCS_DATA_BUCKET),
    ///    producer_kind: ProducerKind::Binance,
    /// };
    ///
    /// // Get the `String` value of the timestamp column for an OHLC `.csv`/`DataFrame`
    /// let data = LeafDir::Ohlc(KPeriod::M1);
    /// let ts_col = binance.get_ts_col_as_str(&data);
    /// assert_eq!(String::from("ots"), ts_col);
    ///
    /// // Get the `String` value of the timestamp column for an AggTrades `.csv`/`DataFrame`
    /// let data = LeafDir::AggTrades;
    /// let ts_col = binance.get_ts_col_as_str(&data);
    /// assert_eq!(String::from("ts"), ts_col);
    /// ```
    fn get_ts_col_as_str(&self, data: &LeafDir) -> String;

    /// Loads the `.csv` file at the given location to `DataFrame`. The `LeafDir`s define
    /// what kind of `Schema` we have to load.
    ///
    /// # Arguments
    /// * `file` - path to the `.csv` file we want to load into a `DataFrame`
    /// * `data` - defines which `Schema` to choose
    ///
    /// # Example
    /// To load a volume profile `.csv` file into a `DataFrame`, we have to do the following
    /// ```
    /// // Initialize a `DataProducer`
    /// let ninja = Ninja {
    ///    client: Arc::new(config::get_google_cloud_client().await),
    ///    bucket: PathBuf::from(GCS_DATA_BUCKET),
    ///    producer_kind: ProducerKind::Ninja,
    /// };
    ///
    /// let file = PathBuf::from("strategy/ppp/6e/2022/day/vol/354.csv");
    /// let data = LeafDir::Vol;
    ///
    /// let df = ninja.get_df(file, data).unwrap();
    ///
    /// // Do stuff with `df`
    /// ```
    async fn get_df(&self, file: &PathBuf, data: &LeafDir) -> Result<DataFrame, Error>;
}