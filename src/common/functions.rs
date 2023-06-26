// Intern crates
use crate::{
    common::{
        finder::Finder,
        gcs::download_file,
        time_interval::{get_cw_from_ts, get_weekday_from_ts, InInterval, TimeInterval},
        wrappers::unwrap_int64,
    },
    config,
    enums::{
        data::{LeafDir, RootDir},
        markets::{GranularityKind, MarketKind},
    },
    producers::DataProducer,
};

// Extern crates
use chrono::{Duration, NaiveDateTime};
use google_cloud_storage::{client::Client, http::Error};
use polars::{
    lazy::dsl::GetOutput,
    prelude::{
        col, lit, CsvReader, CsvWriter, DataFrame, Float64Chunked, Int64Chunked, IntoLazy,
        IntoSeries, PolarsError, Schema, SerReader, SerWriter, Series,
    },
};
use std::{
    collections::{HashMap, HashSet},
    io::{Cursor, Read, Seek, SeekFrom},
    path::PathBuf,
    sync::{Arc, Mutex},
};

use super::gcs::upload_file;

/// This function computes the `cw` and `weekday` column a `DataFrame`.
///
/// # Arguments
/// * `dp` - DataProducer
/// * `file` - file path to `DataFrame`
/// * `data` - determines which kind of `DataFrame` we handle. The kind is derived from the `LeafDir`
/// * `ts_col` - timestamp column in the respective `DataFrame`
///
/// # Example
/// The header in the result `DataFrame` gets two additional columns `cw` and `weekday`
///   ```
/// // Old header
/// ots    ,open    ,high    ,low    ,close    ,...    ,last_col
///
/// // To new header (extra cw column)
/// ots    ,open    ,high    ,low    ,close    ,...    ,last_col    ,cw     ,weekday
/// ```
pub async fn compute_cw_and_weekday_col(
    dp: Arc<dyn DataProducer + Send + Sync>,
    file: PathBuf,
    data: LeafDir,
    ts_col: Arc<String>,
) -> Result<DataFrame, PolarsError> {
    // Download DataFrame
    let df = dp.get_df(&file, &data).await.unwrap();

    // Compute calendar week from timestamp and add new column
    let df = df
        .lazy()
        .with_columns(vec![
            col(&ts_col)
                .apply(|x| Ok(Some(get_cw_from_ts(&x))), GetOutput::default())
                .alias("cw"),
            col(&ts_col)
                .apply(|x| Ok(Some(get_weekday_from_ts(&x))), GetOutput::default())
                .alias("weekday"),
        ])
        .collect()?;

    // Return result
    Ok(df)
}

/// This function splits a `DataFrame` by calendar week (cw) and returns a `Vec<DataFrame>` grouped by `cw`.
///
/// # Arguments
/// * `dp` - DataProducer
/// * `file` - file path to `DataFrame`
/// * `data` - determines which kind of `DataFrame` we handle. The kind is derived from the `LeafDir`
/// * `ts_col` - timestamp column in the respective `DataFrame`
///
/// # Example
/// The header of the `DataFrames` in the result `Vec<DataFrame>` get an extra column `cw`
///   ```
/// // Old header
/// ots    ,open    ,high    ,low    ,close    ,...    ,last_col
///
/// // To new header (extra cw column)
/// ots    ,open    ,high    ,low    ,close    ,...    ,last_col    ,cw
/// ```
pub async fn split_by_cw(
    dp: &(dyn DataProducer + Send + Sync),
    file: PathBuf,
    data: LeafDir,
    ts_col: Arc<String>,
) -> Result<Vec<DataFrame>, PolarsError> {
    // TODO Use dedicaated Rayon core, because this function can be called frequently
    // Download DataFrame
    let df = dp.get_df(&file, &data).await.unwrap();

    // Compute calendar week from timestamp and add new column
    let cw_df = df
        .lazy()
        .with_column(
            col(&ts_col)
                .apply(|x| Ok(Some(get_cw_from_ts(&x))), GetOutput::default())
                .alias("cw"),
        )
        .collect()?;

    // Partition DataFrame by calendar week
    // Return result
    cw_df.partition_by(["cw"])
}

/// This function splits a `DataFrame` by week day (wd) and returns a `Vec<DataFrame>` grouped by `weekday`.
///
/// # Arguments
/// * `dp` - DataProducer
/// * `file` - file path to `DataFrame`
/// * `data` - determines which kind of `DataFrame` we handle. The kind is derived from the `LeafDir`
/// * `ts_col` - timestamp column in the respective `DataFrame`
///
/// # Example
/// The header of the `DataFrames` in the result `Vec<DataFrame>` get an extra column `weekday`
///   ```
/// // Old header
/// ots    ,open    ,high    ,low    ,close    ,...    ,last_col
///
/// // To new header (extra cw column)
/// ots    ,open    ,high    ,low    ,close    ,...    ,last_col    ,weekday
/// ```
pub async fn split_by_wd(
    dp: &(dyn DataProducer + Send + Sync),
    file: PathBuf,
    data: &LeafDir,
    ts_col: &str,
) -> Result<Vec<DataFrame>, PolarsError> {
    // TODO Use dedicaated Rayon core, because this function can be called frequently
    // Download DataFrame
    let df = dp.get_df(&file, data).await.unwrap();

    // Compute weekday from timestamp and add new column
    let wd_df = df
        .lazy()
        .with_column(
            col(ts_col)
                .apply(|x| Ok(Some(get_weekday_from_ts(&x))), GetOutput::default())
                .alias("weekday"),
        )
        .collect()?;

    // Partition DataFrame by calendar week
    // Return result
    wd_df.partition_by(["weekday"])
}

pub fn _split_by_wd(df: DataFrame, ts_col: &str) -> Result<Vec<DataFrame>, PolarsError> {
    // Compute weekday from timestamp and add new column
    let wd_df = df
        .lazy()
        .with_column(
            col(ts_col)
                .apply(|x| Ok(Some(get_weekday_from_ts(&x))), GetOutput::default())
                .alias("weekday"),
        )
        .collect()?;

    // Partition DataFrame by calendar week
    // Return result
    wd_df.partition_by(["weekday"])
}

/// This function returns the calendar week of this DataFrame appended by `"{cw}.csv"`.
///
/// # Note
/// * We assume that the calendar week column in this DataFrame contains the same value for each row
/// * We only read the value of the calendar week column in the first row
///
/// # Arguments
/// * `df` - `DataFrame`
///
/// # Example
///   ```
/// // Some DataFrame with only one column
/// let df = df!(
///     "cw" => &[1_i64, 1, 1, 1],
/// )
///
/// assert_eq!("1.csv".to_string(), get_cw_filename(df));
/// ```
pub fn get_cw_filename(df: &DataFrame) -> String {
    // Get the cw of the current data frame. We use this cw for the file name {cw}.csv in the last step
    let cw = df.column("cw").unwrap().get(0).unwrap();

    // Unwrap cw to i64
    let cw = unwrap_int64(&cw);

    // Format and return file_name
    format!("{cw}.csv")
}

/// This function returns the calendar week and weekday of this DataFrame appended by `"{cw}{wd}.csv"`.
///
/// # Note
/// * We assume that the calendar week and weekday column in this DataFrame contains the same value for each row
/// * We only read the value of the calendar week column in the first row
///
/// # Arguments
/// * `df` - `DataFrame`
///
/// # Example
///   ```
/// // Some DataFrame with only one column
/// let df = df!(
///     "cw" => &[1_i64, 1, 1, 1],
///     "weekday" => &[4_i64, 4, 4, 4],
/// )
///
/// assert_eq!("14.csv".to_string(), get_cw_filename(df));
/// ```
pub fn get_wd_file_name(df: &DataFrame, cw: &str) -> String {
    // Get the cw of the current data frame. We use this cw for the file name {cw}_{wd}.csv in the last step
    let wd = df.column("weekday").unwrap().get(0).unwrap();

    // Unwrap cw to i64
    let wd = unwrap_int64(&wd);

    // Format file_name
    format!("{cw}{wd}.csv")
}

/// This Function writes a `DataFrame` to a vector of bytes `Vec<u8>` and returns this bytes vector.
///
/// # Arguments
/// * `df` - `DataFrame`
pub fn write_df_to_bytes(mut df: DataFrame) -> Vec<u8> {
    // Initialize cursor to read df
    let mut cursor = Cursor::new(Vec::new());

    // Initialize bytes vec to get df as bytes
    let mut bytes = Vec::new();

    CsvWriter::new(&mut cursor)
        .has_header(true)
        .finish(&mut df)
        .unwrap();

    cursor.seek(SeekFrom::Start(0)).unwrap();
    cursor.read_to_end(&mut bytes).unwrap();
    bytes
}

/// This function filters a `DataFrame` by a given `TimeInterval` and returns a `DataFrame`
/// that contains only the rows, which are within the given `TimeInterval`.
///
/// # Arguments
/// * `df` - `DataFrame`
/// * `ts_col` - timestamp column in the respective `DataFrame`
/// * `granularity` - to choose to filter between `in_weekly_time_interval` or `in_daily_time_interval`
/// * `ti` - the `TimeInterval` that determines the accepted time points in the `DataFrame`
///
/// ```
/// // Old header
/// ots    ,open    ,high    ,low    ,close    ,...    ,last_col
///
/// // To new header (extra cw column)
/// ots    ,open    ,high    ,low    ,close    ,...    ,last_col    ,in_interval
/// ```
pub fn filter_df_by_interval(
    df: DataFrame,
    ts_col: &str,
    granularity: GranularityKind,
    ti: TimeInterval,
) -> Result<DataFrame, PolarsError> {
    df.lazy()
        .with_column(
            col(ts_col)
                .apply(
                    move |x| Ok(Some(ti.in_time_interval(&x, granularity))),
                    GetOutput::default(),
                )
                .alias("in_interval"),
        )
        .filter(col("in_interval").eq(lit(true)))
        .select([col("*")])
        .collect()
}

/// This functions transforms a raw `DataFrame` from the `Ninja` producer to a `OHLC` `DataFrame` and returns
/// the transformed `DataFrame`.
///
/// # Arguments
/// * `df` - raw `DataFrame` from the `Ninja` producer inside `/data/ninja/{market}/{year}/ohlc-{ts}`
/// * `offset` - duration of a K-line
///
/// # Note
/// * The first column in the raw `Ninja` DataFrame is the closing timestamp
/// * To compute the opening timestamp we subtract the duration of a candle (i.e. `offset`) from the closing timestamp
pub fn ninja_raw_to_ohlc_df(df: DataFrame, offset: i64) -> DataFrame {
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

/// This function loads a `DataFrame` from a `.csv` file and returns it.
///
/// # Arguments
/// * `client` - google cloud storage client
/// * `file` - the `.csv` file to load the `DataFrame` from
/// * `s` - schema if we load a `.csv` without headers
/// * `delimiter` - the default is `","`, but we can pass an other delimiter like `";"`
/// * Deprecated: `header` - is `Some(true)` if the `.csv` file has a header
///
/// // TODO make return JSON?
/// 
/// # Note
/// All `.csv` files, but
/// * `transform_ninja_test_file.csv` - a test file that transform a raw ninja `.csv` to a `DataFrame`
/// * `"bucket"/data/{producer}/{market}/{year}/{aggTrades | ohlc-{ts} | ...}` - all raw data `.csv` files
///
/// have a header.
pub async fn df_from_file(
    file: &PathBuf,
    s: Option<Schema>,
    delimiter: Option<u8>,
    // header: Option<bool>
) -> Result<DataFrame, Error> {
    // TODO Fix
    let client = config::get_google_cloud_client().await;
    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    // TODO find better/faster way to determine if we parse a `DataFrame` with header
    // All files but the raw data `.csv` files have headers
    let mut header = file.to_str().unwrap().contains("cw")
        || file.to_str().unwrap().contains("day")
        || file.to_str().unwrap().contains("/test/other")
        || file.to_str().unwrap().contains("performance_report.csv");

    // Stupid hack we have to remove. If we load the files inside test/other/ninja we don't have headers
    header = header
        && !file
            .to_str()
            .unwrap()
            .contains("transform_ninja_test_file.csv");

    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    let bytes = download_file(&client, file).await?;
    let mut reader = CsvReader::new(Cursor::new(bytes));
    if
    /* let Some(true) =  */
    header {
        reader = reader.has_header(true);
        if let Some(d) = delimiter {
            reader = reader.with_delimiter(d);
        }

        Ok(reader.finish().unwrap())
    } else {
        let s = s.unwrap();
        reader = reader.has_header(false).with_schema(Arc::new(s));
        if let Some(d) = delimiter {
            reader = reader.with_delimiter(d);
        }

        Ok(reader.finish().unwrap())
    }
}

/// This function saves a `Vec` of `DataFrame`s into the desired directory.
///
/// # Arguments
/// * `dp` - DataProducer
/// * `dfs` - List of `DataFrame`s we want to save
/// * `finder` - configuration
/// * `root` - directory, where to save files `{data | strategy}`
/// * `leaf` - directory, where to save files
/// * `cache` - to avoid uploading same files in diffrent parts
///
/// # Example
/// Calling `save_files` with the following parameters will save the list of `DataFrame`s to
/// `"bucket"/{root}/{producer}/{market}/{year}/{leaf}/cw`, where
/// * `root = RootDir::Data`
/// * `producer = ProducerKind::Ninja`
/// * `market = MarketKind::EurUsd`
/// * `year = 2022`
/// * `leaf = LeafDir::Ohlc(KPeriod::M1)`
/// ```
/// let finder = Finder::new(
///     "bucket",
///     ProducerKind::Ninja,
///     MarketKind::EurUsd,
///     2022,
///     BotKind::Ppp,
///     GranularityKind::Daily,
/// );
/// let finder = Arc::new(finder);
/// let cache = Arc::new(Mutex::new(HashSet::new()));
///
/// save_files(
///     vec![DataFrame::default(), DataFrame::default()],
///     dp.clone(),
///     RootDir::Data,
///     finder.clone(),
///     db_cache.clone(),
///     LeafDir::Ohlc(KPeriod::M1),
///     .await;
///     )
/// ```
pub async fn save_files(
    dp: Arc<dyn DataProducer + Send + Sync>,
    dfs: Vec<DataFrame>,
    finder: Arc<Finder>,
    root: RootDir,
    leaf: LeafDir,
    cache: Arc<Mutex<HashSet<PathBuf>>>,
) {
    let tasks: Vec<_> = dfs
        .into_iter()
        .map(|df| {
            let dp = dp.clone();
            let db_cache = cache.clone();
            let finder = finder.clone();
            tokio::spawn(async move {
                let file_name = get_cw_filename(&df);
                finder
                    .save_file(
                        finder.get_client_clone(),
                        root,
                        leaf,
                        file_name,
                        df,
                        Some(db_cache),
                        Some(Arc::new(dp.get_ts_col_as_str(&leaf))),
                    )
                    .await;
            })
        })
        .collect();

    for task in tasks {
        task.await.unwrap();
    }
}

pub async fn save_dfs(
    dp: Arc<dyn DataProducer + Send + Sync>,
    dfs: HashMap<(i64, Option<i64>), Option<DataFrame>>,
    finder: Arc<Finder>,
    root: RootDir,
    leaf: LeafDir,
) {
    let mut bytes = HashMap::new();
    for ((cw, wd), df) in dfs {
        // If we have weekly data, weekday is set to -1
        let weekday = if let Some(day) = wd { day } else { -1 };

        // We only store if some df
        if let Some(data) = df {
            bytes.insert(format!("{cw},{weekday}"), write_df_to_bytes(data));
        }
    }
    let ser_bytes = serde_json::to_string(&bytes).unwrap();

    let ap = finder.path_to_leaf(&root, &leaf, None).join("dfs.csv");

    // Upload
    upload_file(&*finder.get_client_clone(), &ap, ser_bytes.into_bytes()).await;
}

pub async fn load_dfs(
    dp: Arc<dyn DataProducer + Send + Sync>,
    finder: Arc<Finder>,
    root: RootDir,
    leaf: LeafDir,
) -> HashMap<(i64, Option<i64>), DataFrame> {
    let file = finder.path_to_leaf(&root, &leaf, None).join("dfs.csv");
    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    // TODO find better/faster way to determine if we parse a `DataFrame` with header
    // All files but the raw data `.csv` files have headers
    let mut header = file.to_str().unwrap().contains("cw")
        || file.to_str().unwrap().contains("day")
        || file.to_str().unwrap().contains("/test/other");

    // Stupid hack we have to remove. If we load the files inside test/other/ninja we don't have headers
    header = header
        && !file
            .to_str()
            .unwrap()
            .contains("transform_ninja_test_file.csv");

    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    let bytes = download_file(&*finder.get_client_clone(), &file)
        .await
        .unwrap();
    let de_bytes: HashMap<String, Vec<u8>> =
        serde_json::from_str(std::str::from_utf8(&bytes).expect("bytes are not valid utf-8..."))
            .unwrap();
    let mut map_of_dfs: HashMap<(i64, Option<i64>), DataFrame> = HashMap::new();

    for (s, bytes) in de_bytes {
        let mut reader = CsvReader::new(Cursor::new(bytes));
        let df = if
        /* let Some(true) =  */
        header {
            reader = reader.has_header(true);
            // if let Some(d) = delimiter {
            //     reader = reader.with_delimiter(d);
            // }

            reader.finish().unwrap()
        } else {
            panic!("We should have a header !!!")
            // let s = s.unwrap();
            // reader = reader.has_header(false).with_schema(&s);
            // if let Some(d) = delimiter {
            //     reader = reader.with_delimiter(d);
            // }

            // reader.finish().unwrap()
        };

        let split: Vec<_> = s.split(',').collect();
        let (cw, wd) = (
            split[0].parse::<i64>().unwrap(),
            split[1].parse::<i64>().unwrap(),
        );

        if wd == -1 {
            // We have weekly data
            // TODO is clear from Filepath
            map_of_dfs.insert((cw, None), df);
        } else {
            map_of_dfs.insert((cw, Some(wd)), df);
        }
    }

    map_of_dfs
}

/// This function transforms a series that contains `"%d.%m.%Y %H:%M:%S"` timestamps
/// to a series that contains the equivalent POSIX timestamp subtracted by a given `Duration`.
fn sub_time(val: Series, duration: Duration) -> Series {
    val.utf8()
        .unwrap()
        .into_iter()
        .map(|o| {
            o.map(|ts| {
                NaiveDateTime::parse_from_str(ts, "%d.%m.%Y %H:%M:%S")
                    .unwrap()
                    .checked_sub_signed(duration)
                    .unwrap()
                    .timestamp_millis()
            })
        })
        .collect::<Int64Chunked>()
        .into_series()
}

/// This function transforms a series that contains comma separted strings to a series
/// that contains floats.
fn comma_separated_string_to_f64(val: Series) -> Series {
    val.utf8()
        .unwrap()
        .into_iter()
        .map(|o| o.map(|x| x.replace(",", ".").parse::<f64>().unwrap()))
        .collect::<Float64Chunked>()
        .into_series()
}

#[cfg(test)]
mod tests {
    // use polars::df;
    // use polars::prelude::NamedFrom;
    use super::*;
    use crate::{config::GCS_DATA_BUCKET, producers::test::Test};
    use google_cloud_default::WithAuthExt;
    use google_cloud_storage::client::ClientConfig;

    #[tokio::test]
    async fn test_compute_cw_and_weekday_col() {
        let config = ClientConfig::default().with_auth().await.unwrap();
        // Initialize data provider
        let dp: Arc<dyn DataProducer + Send + Sync> =
            Arc::new(Test::new(std::path::PathBuf::from(GCS_DATA_BUCKET)));

        // Test file name
        let test_file =
            PathBuf::from("data/test/other/common/test_file_compute_cw_and_weekday_col.csv");

        // Target file name
        let trgt_file =
            PathBuf::from("data/test/other/common/target_file_compute_cw_and_weekday_col.csv");

        // Parameters used in `compute_cw_and_weekday_col`
        let ts_col = Arc::new(String::from("ots"));
        // TODO manueller client call
        let client = crate::config::get_google_cloud_client().await;

        // Compute df from `compute_cw_and_weekday_col`
        let result = compute_cw_and_weekday_col(dp, test_file, LeafDir::Ohlcv60m, ts_col)
            .await
            .unwrap();

        // Load target DataFrame
        let target = df_from_file(&trgt_file, None, None).await.unwrap();

        // The computed result data frame must equal the target data frame
        assert_eq!(result.frame_equal(&target), true);
    }

    #[test]
    fn test_split_by_cw() {
        // let df = df!(
        //     "ts" => &[1670337658956_i64, 1670337658956, 1670337658956],
        //     "ts1" => &[1670337658956_i64, 1670337658956, 1670337658956],
        //     "ts2" => &[1_i64, 2, 3],
        //     "ts3" => &[-1_i64, -2, -3],
        //     "low" => &[1_i64, 2, 30],
        //     "high" => &[3_i64, 5, 35],
        // );

        // let cw_df = df
        //     .lazy()
        //     .with_columns(vec![
        //         col("ts").apply(|x| Ok(get_cw_from_ts(&x)), GetOutput::default()), //.alias("cw"),
        //         col("ts1").apply(|x| Ok(get_cw_from_ts(&x)), GetOutput::default()), //.alias("cw1"),
        //     ])
        //     .collect();
        // let df = df.unwrap();

        // let cw_df = df
        //     .lazy()
        //     .select([col("ts2").filter(col("low").lt_eq(lit(33)).and(col("high").gt_eq(lit(33))))]) //.first()
        //     .collect()
        //     .unwrap();

        // let cw_df = df
        //     .lazy()
        //     .with_column(
        //         col("ts")
        //             .apply(|x| Ok(get_cw_from_ts(&x)), GetOutput::default())
        //             .alias("cw")
        //     )
        //     .collect();

        // let cw_df = df
        //     .lazy()
        //     .with_column(
        //         col("ts")
        //             .apply(|x| Ok(get_cw_from_ts(&x)), GetOutput::default())
        //             .alias("foo"),
        //     )
        //     .filter(col("foo").eq(lit(49)))
        //     .select([col("*")])
        //     .collect();

        // let cw_df = df
        //     .lazy()
        //     .with_column(
        //         col("ts")
        //             .apply(|x| {
        //                 let res = x.i64()
        //                 .unwrap()
        //                 .into_iter()
        //                 .map(|o: Option<i64>| {
        //                     o.map(|ts: i64| {
        //                         i64::try_from(
        //                             NaiveDateTime::from_timestamp_opt(ts / 1000, 0).unwrap()
        //                                 .iso_week()
        //                                 .week(),
        //                         )
        //                         .unwrap()
        //                     })
        //                 })
        //                 .collect::<Int64Chunked>()
        //                 .into_series();
        //                 Ok(dbg!(res))
        //             }, GetOutput::default())
        //             .alias("cw"),
        //     )
        //     .collect();
    }
}
