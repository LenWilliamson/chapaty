use std::collections::HashMap;

use rayon::iter::IntoParallelIterator;
use rayon::prelude::ParallelIterator;

use super::*;
use crate::common::functions::{_split_by_wd, save_dfs};
use crate::{
    common::{
        functions::{filter_df_by_interval, split_by_cw},
        wrappers::unwrap_int64,
    },
    math::volume_profile::volume_profile,
};

/// This function processes a batch of `.csv` files and populates the respective directories with
/// the processed raw data files.
///
/// # Steps
/// * Load the raw data and split it into `{cw}.csv`. Store inside a subdirectory `"/cw"`
/// * Depending on the strategy we prepare the data from the `"/cw"` for a
///   * Weekly analysis
///   * Daily analysis
/// * Compute volume profiles
///
/// # Arguments
/// * `year` - year we want to backtest against
/// * `strategy` - strategy we want to backtest against
/// * `market` - market we want to backtest againgst
/// * `granularity` granulairy we want to backtest against
/// * `time_interval` - time interval for which we want to filter the raw data
pub async fn process_batch(
    dp: Arc<dyn DataProducer + Send + Sync>,
    year: u32,
    bot: BotKind,
    market: MarketKind,
    granularity: GranularityKind,
    directory: LeafDir,
    time_interval: TimeInterval,
    data: LeafDir,
    job: JobKind,
) -> (
    HashMap<(i64, Option<i64>), Option<DataFrame>>,
    Option<HashMap<(i64, Option<i64>), Option<DataFrame>>>,
) {
    let finder = Finder::new(
        dp.get_bucket_name(),
        dp.get_data_producer_kind(),
        market,
        year,
        bot,
        granularity.clone(),
    ).await;
    let dfs =
        split_raw_data_into_calendar_weeks(dp.clone(), finder.clone(), directory.clone()).await;
    let dfs = match granularity {
        GranularityKind::Weekly => prepare_weekly_data(
            dp.clone(),
            finder.clone(),
            time_interval,
            directory,
            dfs,
        ),
        GranularityKind::Daily => prepare_daily_data(
            dp.clone(),
            finder.clone(),
            time_interval,
            directory, // directory == data
            dfs,
        ),
    };

    let vols = match job {
        JobKind::Volume => Some(
            compute_volume_from_agg_trades(dp.clone(), finder.clone(), data, dfs.clone()).await,
        ),
        JobKind::Chart => None, // we are done
    };

    // Save dfs
    save_dfs(
        dp.clone(),
        dfs.clone(),
        Arc::new(finder.clone()),
        RootDir::Strategy,
        data,
    )
    .await;

    // Save vols
    if let Some(dfs) = vols.clone() {
        save_dfs(
            dp.clone(),
            dfs.clone(),
            Arc::new(finder.clone()),
            RootDir::Strategy,
            LeafDir::Vol,
        )
        .await;
    }

    // Return, so we don't have to download the files if pl_only == false

    (dfs, vols)
}

/// This function splits the `.csv` data inside `{EDS}/data/{data_provider}/{market}/{year}/ohlcv-*` by calendar week
/// and stores it inside the `{EDS}/data/{data_provider}/{market}/{year}/ohlcv-*/cw` directory.
/// The `.csv` files get renamed into `{cw}.csv`, where `{cw}` is replaced by `1, 2, ..., 52, 53`.
async fn split_raw_data_into_calendar_weeks(
    dp: Arc<dyn DataProducer + Send + Sync>,
    finder: Finder,
    data: LeafDir,
) -> HashMap<i64, DataFrame> {
    let ts_col = Arc::new(dp.get_ts_col_as_str(&data));
    let files = finder
        .list_files(finder.get_client_clone(), RootDir::Data, data, Some(false))
        .await
        .unwrap();
    let cache = Arc::new(Mutex::new(HashMap::new()));

    let tasks: Vec<_> = files
        .into_iter()
        .map(|file| {
            let dp = dp.clone();
            let db_cache = cache.clone();
            let ts = ts_col.clone();
            tokio::spawn(async move {
                let data_frames_by_cw = split_by_cw(&*dp, file, data, ts.clone()).await.unwrap();
                data_frames_by_cw.into_par_iter().for_each(|mut df| {
                    // Get the cw of the current data frame
                    let cw = df.column("cw").unwrap().get(0).unwrap();

                    // Unwrap cw to i64
                    let cw = unwrap_int64(&cw);

                    let mut lock = db_cache.lock().unwrap();
                    if let Some(cahced_df) = lock.get(&cw) {
                        df.extend(cahced_df).unwrap();
                        df.align_chunks();
                        let sorted = df.sort([&*ts], false).unwrap();
                        lock.insert(cw, sorted);
                    } else {
                        lock.insert(cw, df);
                    }
                })
            })
        })
        .collect();

    for task in tasks {
        task.await.unwrap();
    }

    Arc::try_unwrap(cache).unwrap().into_inner().unwrap()
}

/// This function filteres the `.csv` data inside `{EDS}/data/{data_provider}/{market}/{year}/ohlcv-*/cw`
/// by the respective `time_interval` and stores it inside `{EDS}/strategy/{strategy}/{market}/{year}/{cw | day}/ohlcv-*` directory.
/// The `.csv` files do not get renamed.
fn prepare_weekly_data(
    dp: Arc<dyn DataProducer + Send + Sync>,
    finder: Finder,
    time_interval: TimeInterval,
    data: LeafDir,
    dfs: HashMap<i64, DataFrame>,
) -> HashMap<(i64, Option<i64>), Option<DataFrame>> {
    let ts_col = dp.get_ts_col_as_str(&data);

    let res: HashMap<(i64, Option<i64>), Option<DataFrame>> = dfs
        .into_par_iter()
        .map(|(cw, df)| {
            //let ts_col = ts_handle.clone();
            let filtered =
                filter_df_by_interval(df, &ts_col, finder.get_granularity(), time_interval.clone())
                    .unwrap();

            // Skip empty frames
            let (row, _) = filtered.shape();
            if row > 0 {
                // Store Some df, no day
                ((cw, None), Some(filtered))
            } else {
                ((cw, None), None)
            }
        })
        .collect();

    res
}

fn prepare_daily_data(
    dp: Arc<dyn DataProducer + Send + Sync>,
    finder: Finder,
    time_interval: TimeInterval,
    data: LeafDir,
    dfs: HashMap<i64, DataFrame>,
) -> HashMap<(i64, Option<i64>), Option<DataFrame>> {
    let ts_col = dp.get_ts_col_as_str(&data);

    let res: HashMap<(i64, Option<i64>), Option<DataFrame>> = dfs
        .into_par_iter()
        .map(|(cw, df)| {
            // let ts_col = ts_handle.clone();
            // let ts_col = ts_col.clone();
            // Split df by weekday
            let data_frames_by_wd = _split_by_wd(df, &ts_col).unwrap();
            let tmp: HashMap<(i64, Option<i64>), Option<DataFrame>> = data_frames_by_wd
                .into_par_iter()
                .map(|df_wd| {
                    // Get the cw of the current data frame
                    // let cw = df_wd.column("cw").unwrap().get(0).unwrap();
                    let wd = df_wd.column("weekday").unwrap().get(0).unwrap();

                    // Unwrap cw to i64
                    // let cw = unwrap_int64(&cw);
                    let wd = unwrap_int64(&wd);

                    let filtered = filter_df_by_interval(
                        df_wd,
                        &ts_col,
                        finder.get_granularity(),
                        time_interval.clone(),
                    )
                    .unwrap();

                    // Skip empty frames
                    let (row, _) = filtered.shape();
                    if row > 0 {
                        // Store Some df, some day
                        ((cw, Some(wd)), Some(filtered))
                    } else {
                        ((cw, Some(wd)), None)
                    }
                })
                .collect();

            tmp
        })
        .flatten()
        .collect();

    res
}


/// This function computes the volume profile from the `.csv` data inside `{EDS}/strategy/{strategy}/{market}/{year}/{cw | day}/aggTrades`
/// by the respective `time_interval` and stores it inside `{EDS}/strategy/{strategy}/{market}/{year}/{cw | day}/vol` directory.
/// The `.csv` files do not get renamed.
async fn compute_volume_from_agg_trades(
    dp: Arc<dyn DataProducer + Send + Sync>,
    finder: Finder,
    data: LeafDir,
    dfs: HashMap<(i64, Option<i64>), Option<DataFrame>>,
) -> HashMap<(i64, Option<i64>), Option<DataFrame>> {
    let finder_handle = Arc::new(finder);

    let tasks: Vec<_> = dfs
        .into_iter()
        .map(|((cw, wd), df)| {
            let finder = finder_handle.clone();
            let max_digits = finder.get_market().number_of_digits();
            let dp = dp.clone();
            tokio::spawn(async move {
                let vol = if let Some(df) = df {
                    Some(
                        volume_profile(dp.clone(), df, data, false, max_digits)
                            .await
                            .unwrap(),
                    )
                } else {
                    None
                };
                ((cw, wd), vol)
            })
        })
        .collect();

    let mut res: HashMap<(i64, Option<i64>), Option<DataFrame>> = HashMap::new();
    for task in tasks {
        let (k, v) = task.await.unwrap();
        res.insert(k, v);
    }

    res
}

#[cfg(test)]
mod tests {
    // Intern crates
    use super::*;
    use crate::{
        config::{self, GCS_DATA_BUCKET},
        producers::test::Test,
        streams::tests::load_csv,
    };

    // Extern crates
    use std::time::Instant;

    // BEGIN: Static Test Variables
    // Configure time_interval for the strategy
    static TIME_INTERVAL: TimeInterval = TimeInterval {
        start_day: chrono::Weekday::Mon,
        start_h: 1,
        end_day: chrono::Weekday::Fri,
        end_h: 23,
    };

    /// This unit test checks if the test batch in `/ohlc/1h` gets processed correctly.
    ///
    /// # Note:
    /// * The batch files are split by calendar weeks
    /// * Each calendar week group gets stored into a separate file
    /// * The header of the processed `.csv` file that is stored in the `/cw` subdirectory changes from
    /// * Polars truncates trailing zeros.
    /// ```
    /// // Old header
    /// ots    ,open    ,high    ,low    ,close    ,vol    ,cts    ,qav    ,not    ,tbbav    ,tbqav    ,ignore
    ///
    /// // To new header (extra cw column)
    /// ots    ,open    ,high    ,low    ,close    ,vol    ,cts    ,qav    ,not    ,tbbav    ,tbqav    ,ignore    ,cw
    /// ```
    #[tokio::test]
    async fn test_ohlcv_split_raw_data_into_calendar_weeks() {
        let start = Instant::now();
        let data_provider = Test::new(
            std::path::PathBuf::from(GCS_DATA_BUCKET),
        );
        let dp = Arc::new(data_provider);
        let bot = BotKind::Ppp;
        let market = MarketKind::BtcUsdt;
        let year = 2022;
        let granularity = GranularityKind::Weekly;
        let directory = LeafDir::Ohlcv60m;
        let finder = super::Finder::new(
            dp.get_bucket_name(),
            dp.get_data_producer_kind(),
            market,
            year,
            bot,
            granularity,
        ).await;
        finder
            .delete_files(finder.get_client_clone(), RootDir::Data, directory.clone())
            .await;
        let dfs = split_raw_data_into_calendar_weeks(dp, finder.clone(), directory).await;
        // let out_dir = finder.path_to_leaf(&RootDir::Data, &directory, Some(true));
        let target_dir = finder._path_to_target(&RootDir::Data, &directory);

        // BEGIN: Test CW 5
        let mut target = load_csv(&target_dir, "5.csv").await.unwrap();
        // let mut result = load_csv(&out_dir, "5.csv").await.unwrap();
        assert_eq!(dfs.get(&5).unwrap().frame_equal(&target), true);
        // END: Test CW 5

        // BEGIN: Test CW 6
        target = load_csv(&target_dir, "6.csv").await.unwrap();
        // result = load_csv(&out_dir, "6.csv").await.unwrap();
        assert_eq!(dfs.get(&6).unwrap().frame_equal(&target), true);
        // END: Test CW 6

        // BEGIN: Test CW 7
        target = load_csv(&target_dir, "7.csv").await.unwrap();
        // result = load_csv(&out_dir, "7.csv").await.unwrap();
        assert_eq!(dfs.get(&7).unwrap().frame_equal(&target), true);
        // END: Test CW 7

        // BEGIN: Test CW 8
        target = load_csv(&target_dir, "8.csv").await.unwrap();
        // result = load_csv(&out_dir, "8.csv").await.unwrap();
        assert_eq!(dfs.get(&8).unwrap().frame_equal(&target), true);
        // END: Test CW 8

        // BEGIN: Test CW 9
        target = load_csv(&target_dir, "9.csv").await.unwrap();
        // result = load_csv(&out_dir, "9.csv").await.unwrap();
        assert_eq!(dfs.get(&9).unwrap().frame_equal(&target), true);
        // END: Test CW 9

        // BEGIN: Test CW 10
        target = load_csv(&target_dir, "10.csv").await.unwrap();
        // result = load_csv(&out_dir, "10.csv").await.unwrap();
        assert_eq!(dfs.get(&10).unwrap().frame_equal(&target), true);
        // END: Test CW 10

        // BEGIN: Test CW 11
        target = load_csv(&target_dir, "11.csv").await.unwrap();
        // result = load_csv(&out_dir, "11.csv").await.unwrap();
        assert_eq!(dfs.get(&11).unwrap().frame_equal(&target), true);
        // END: Test CW 11

        // BEGIN: Test CW 12
        target = load_csv(&target_dir, "12.csv").await.unwrap();
        // result = load_csv(&out_dir, "12.csv").await.unwrap();
        assert_eq!(dfs.get(&12).unwrap().frame_equal(&target), true);
        // END: Test CW 12

        // BEGIN: Test CW 13
        target = load_csv(&target_dir, "13.csv").await.unwrap();
        // result = load_csv(&out_dir, "13.csv").await.unwrap();
        assert_eq!(dfs.get(&13).unwrap().frame_equal(&target), true);
        // END: Test CW 13
        //finder.delete_files(&dp.get_client(),&RootDir::Data,  &directory).await;

        let duration = start.elapsed();
        println!("Time elapsed: {duration:?}");
    }

    /// This unit test checks if the test batch in `{EDS}/data/test/2022/raw/btcusdt/ohlc/{1h | ...}/cw` gets processed correctly.
    ///
    /// TODO: Add more test files
    ///
    /// # Note:
    /// * The batch files are filterd by the `TIME_INTERVAL`
    /// * The files get stored in `"{EDS}/data/test/2022/ppp/btcusdt/binance/cw/ohlc/{1h | ...}"`
    /// * The header of the processed `.csv` file that is stored in the `/cw` subdirectory changes from
    ///
    /// ```
    /// // Old header
    /// ots    ,open    ,high    ,low    ,close    ,vol    ,cts    ,qav    ,not    ,tbbav    ,tbqav    ,ignore    ,cw
    ///
    /// // To new header (extra cw column)
    /// ots    ,open    ,high    ,low    ,close    ,vol    ,cts    ,qav    ,not    ,tbbav    ,tbqav    ,ignore    ,cw    ,in_interval
    /// ```
    #[tokio::test]
    async fn test_ohlcv_prepare_weekly_data() {
        let dp: Arc<dyn DataProducer + Send + Sync> = Arc::new(Test::new(
            std::path::PathBuf::from(GCS_DATA_BUCKET),
        ));
        let bot = BotKind::Ppp;
        let market = MarketKind::BtcUsdt;
        let year = 2022;
        let granularity = GranularityKind::Weekly;
        let directory = LeafDir::Ohlcv60m;
        let finder = super::Finder::new(
            dp.get_bucket_name(),
            dp.get_data_producer_kind(),
            market,
            year,
            bot,
            granularity,
        ).await;
        finder
            .delete_files(finder.get_client_clone(), RootDir::Strategy, directory.clone())
            .await;

        // Results to proceed
        let dfs = split_raw_data_into_calendar_weeks(dp.clone(), finder.clone(), directory).await;

        let res = prepare_weekly_data(
            dp.clone(),
            finder.clone(),
            TIME_INTERVAL,
            directory,
            dfs,
        );
        // .await
        // .unwrap();
        // let out_dir = finder.path_to_leaf(&RootDir::Strategy, &directory, None);
        let target_dir = finder._path_to_target(&RootDir::Strategy, &directory);

        // BEGIN: Test CW 8
        let mut target = load_csv(&target_dir, "8.csv").await.unwrap();
        // let mut result = load_csv(&out_dir, "8.csv").await.unwrap();
        assert_eq!(
            res.get(&(8, None))
                .unwrap()
                .clone()
                .unwrap()
                .frame_equal(&target),
            true
        );
        // END: Test CW 8

        // BEGIN: Test CW 9
        target = load_csv(&target_dir, "9.csv").await.unwrap();
        // result = load_csv(&out_dir, "9.csv").await.unwrap();
        assert_eq!(
            res.get(&(9, None))
                .unwrap()
                .clone()
                .unwrap()
                .frame_equal(&target),
            true
        );
        // END: Test CW 9

        // BEGIN: Test CW 10
        target = load_csv(&target_dir, "10.csv").await.unwrap();
        // result = load_csv(&out_dir, "10.csv").await.unwrap();
        assert_eq!(
            res.get(&(10, None))
                .unwrap()
                .clone()
                .unwrap()
                .frame_equal(&target),
            true
        );
        // END: Test CW 10
        finder
            .delete_files(finder.get_client_clone(), RootDir::Strategy, directory.clone())
            .await;
    }

    #[tokio::test]
    async fn test_ohlcv_prepare_daily_data() {
        let dp: Arc<dyn DataProducer + Send + Sync> = Arc::new(Test::new(
            std::path::PathBuf::from(GCS_DATA_BUCKET),
        ));
        let bot = BotKind::Ppp;
        let market = MarketKind::BtcUsdt;
        let year = 2022;
        let granularity = GranularityKind::Daily;
        let directory = LeafDir::Ohlcv60m;
        let finder = super::Finder::new(
            dp.get_bucket_name(),
            dp.get_data_producer_kind(),
            market,
            year,
            bot,
            granularity,
        ).await;
        finder
            .delete_files(finder.get_client_clone(), RootDir::Strategy, directory.clone())
            .await;

        // Results to proceed
        let dfs = split_raw_data_into_calendar_weeks(dp.clone(), finder.clone(), directory).await;

        let res = prepare_daily_data(
            dp.clone(),
            finder.clone(),
            TIME_INTERVAL,
            directory,
            dfs,
        );
        // .await
        // .unwrap();
        // let out_dir = finder.path_to_leaf(&RootDir::Strategy, &directory, None);
        let target_dir = finder._path_to_target(&RootDir::Strategy, &directory);

        // BEGIN: Test CW 8
        let mut target = load_csv(&target_dir, "8_1.csv").await.unwrap();
        // let mut result = load_csv(&out_dir, "81.csv").await.unwrap();
        assert_eq!(
            res.get(&(8, Some(1)))
                .unwrap()
                .clone()
                .unwrap()
                .frame_equal(&target),
            true
        );
        // END: Test CW 8

        // BEGIN: Test CW 9
        target = load_csv(&target_dir, "9_1.csv").await.unwrap();
        // result = load_csv(&out_dir, "91.csv").await.unwrap();
        assert_eq!(
            res.get(&(9, Some(1)))
                .unwrap()
                .clone()
                .unwrap()
                .frame_equal(&target),
            true
        );

        target = load_csv(&target_dir, "9_2.csv").await.unwrap();
        // result = load_csv(&out_dir, "92.csv").await.unwrap();
        assert_eq!(
            res.get(&(9, Some(2)))
                .unwrap()
                .clone()
                .unwrap()
                .frame_equal(&target),
            true
        );

        target = load_csv(&target_dir, "9_3.csv").await.unwrap();
        // result = load_csv(&out_dir, "93.csv").await.unwrap();
        assert_eq!(
            res.get(&(9, Some(3)))
                .unwrap()
                .clone()
                .unwrap()
                .frame_equal(&target),
            true
        );

        target = load_csv(&target_dir, "9_4.csv").await.unwrap();
        // result = load_csv(&out_dir, "94.csv").await.unwrap();
        assert_eq!(
            res.get(&(9, Some(4)))
                .unwrap()
                .clone()
                .unwrap()
                .frame_equal(&target),
            true
        );

        target = load_csv(&target_dir, "9_5.csv").await.unwrap();
        // result = load_csv(&out_dir, "95.csv").await.unwrap();
        assert_eq!(
            res.get(&(9, Some(5)))
                .unwrap()
                .clone()
                .unwrap()
                .frame_equal(&target),
            true
        );
        // END: Test CW 9

        // BEGIN: Test CW 10
        target = load_csv(&target_dir, "10_3.csv").await.unwrap();
        // result = load_csv(&out_dir, "103.csv").await.unwrap();
        assert_eq!(
            res.get(&(10, Some(3)))
                .unwrap()
                .clone()
                .unwrap()
                .frame_equal(&target),
            true
        );
        // END: Test CW 10
        finder
            .delete_files(finder.get_client_clone(), RootDir::Strategy, directory.clone())
            .await;
    }

    // TODO: async fn test_ohlcv_process_batch() {

    // BEGIN Test OHLC
    /// This unit test checks if the test batch in `/ohlc/1h` gets processed correctly.
    ///
    /// # Note:
    /// * The batch files are split by calendar weeks
    /// * Each calendar week group gets stored into a separate file
    /// * The header of the processed `.csv` file that is stored in the `/cw` subdirectory changes from
    ///
    /// # Improvements
    /// * TODO: Figure out why assert_eq!(result.frame_equal(&target), true); fails. Somehow floats change
    ///
    /// ```
    /// // Old header
    /// ots    ,open    ,high    ,low    ,close    ,vol    ,cts    ,qav    ,not    ,tbbav    ,tbqav    ,ignore
    ///
    /// // To new header (extra cw column)
    /// ots    ,open    ,high    ,low    ,close    ,vol    ,cts    ,qav    ,not    ,tbbav    ,tbqav    ,ignore    ,cw
    /// ```
    ///
    /// # Note
    /// Polars truncates trailing zeros. Therefore you have to remove them by calling this code for example
    /// ```
    /// let schema = data_config::config::csv_schema::binance::ohlc::schema();
    /// for i in 8..11 {
    ///     let data_frame = GcpCsvReader::from_path(format!("/media/len/ExterneFestplateLenCewa/DataBase/data/test/2022/ppp/btcusdt/binance/cw/ohlc/1h/target_target_{x}.csv", x = i)).unwrap()
    ///     .has_header(false)
    ///     .with_schema(&schema)
    ///     .finish()
    ///     .unwrap();
    ///
    ///     super::super::save_file(data_frame, &base_path, &format!("_target_target_{x}.csv", x = i));
    /// }
    /// ```
    #[tokio::test]
    async fn test_ohlc_split_raw_data_into_calendar_weeks() {
        let dp: Arc<dyn DataProducer + Send + Sync> = Arc::new(Test::new(
            std::path::PathBuf::from(GCS_DATA_BUCKET),
        ));
        let bot = BotKind::Ppp;
        let market = MarketKind::BtcUsdt;
        let year = 2022;
        let granularity = GranularityKind::Weekly;
        let directory = LeafDir::Ohlc60m;
        let finder = super::Finder::new(
            dp.get_bucket_name(),
            dp.get_data_producer_kind(),
            market,
            year,
            bot,
            granularity,
        ).await;
        finder
            .delete_files(finder.get_client_clone(), RootDir::Data, directory.clone())
            .await;
        let dfs = split_raw_data_into_calendar_weeks(dp, finder.clone(), directory).await;
        // .unwrap();
        let target_dir = finder._path_to_target(&RootDir::Data, &directory);

        // BEGIN: Test CW 5
        let mut target = load_csv(&target_dir, "5.csv").await.unwrap();
        // let mut result = load_csv(&out_dir, "5.csv").await.unwrap();
        assert_eq!(dfs.get(&5).unwrap().frame_equal(&target), true);
        // END: Test CW 5

        // BEGIN: Test CW 6
        target = load_csv(&target_dir, "6.csv").await.unwrap();
        // result = load_csv(&out_dir, "6.csv").await.unwrap();
        assert_eq!(dfs.get(&6).unwrap().frame_equal(&target), true);
        // END: Test CW 6

        // BEGIN: Test CW 7
        target = load_csv(&target_dir, "7.csv").await.unwrap();
        // result = load_csv(&out_dir, "7.csv").await.unwrap();
        assert_eq!(dfs.get(&7).unwrap().frame_equal(&target), true);
        // END: Test CW 7

        // BEGIN: Test CW 8
        target = load_csv(&target_dir, "8.csv").await.unwrap();
        // result = load_csv(&out_dir, "8.csv").await.unwrap();
        assert_eq!(dfs.get(&8).unwrap().frame_equal(&target), true);
        // END: Test CW 8

        // BEGIN: Test CW 9
        target = load_csv(&target_dir, "9.csv").await.unwrap();
        // result = load_csv(&out_dir, "9.csv").await.unwrap();
        assert_eq!(dfs.get(&9).unwrap().frame_equal(&target), true);
        // END: Test CW 9

        // BEGIN: Test CW 10
        target = load_csv(&target_dir, "10.csv").await.unwrap();
        // result = load_csv(&out_dir, "10.csv").await.unwrap();
        assert_eq!(dfs.get(&10).unwrap().frame_equal(&target), true);
        // END: Test CW 10

        // BEGIN: Test CW 11
        target = load_csv(&target_dir, "11.csv").await.unwrap();
        // result = load_csv(&out_dir, "11.csv").await.unwrap();
        assert_eq!(dfs.get(&11).unwrap().frame_equal(&target), true);
        // END: Test CW 11

        // BEGIN: Test CW 12
        target = load_csv(&target_dir, "12.csv").await.unwrap();
        // result = load_csv(&out_dir, "12.csv").await.unwrap();
        assert_eq!(dfs.get(&12).unwrap().frame_equal(&target), true);
        // END: Test CW 12

        // BEGIN: Test CW 13
        target = load_csv(&target_dir, "13.csv").await.unwrap();
        // result = load_csv(&out_dir, "13.csv").await.unwrap();
        assert_eq!(dfs.get(&13).unwrap().frame_equal(&target), true);
        // END: Test CW 13
        // finder.delete_files(&dp.get_client(),&RootDir::Data,  &directory).await;
    }

    /// This unit test checks if the test batch in `{EDS}/data/test/2022/raw/btcusdt/ohlc/{1h | ...}/cw` gets processed correctly.
    ///
    /// TODO: Add more test files
    ///
    /// # Note:
    /// * The batch files are filterd by the `TIME_INTERVAL`
    /// * The files get stored in `"{EDS}/data/test/2022/ppp/btcusdt/binance/cw/ohlc/{1h | ...}"`
    /// * The header of the processed `.csv` file that is stored in the `/cw` subdirectory changes from
    ///
    /// ```
    /// // Old header
    /// ots    ,open    ,high    ,low    ,close    ,vol    ,cts    ,qav    ,not    ,tbbav    ,tbqav    ,ignore    ,cw
    ///
    /// // To new header (extra cw column)
    /// ots    ,open    ,high    ,low    ,close    ,vol    ,cts    ,qav    ,not    ,tbbav    ,tbqav    ,ignore    ,cw    ,in_interval
    /// ```
    #[tokio::test]
    async fn test_ohlc_prepare_weekly_data() {
        let dp: Arc<dyn DataProducer + Send + Sync> = Arc::new(Test::new(
            std::path::PathBuf::from(GCS_DATA_BUCKET),
        ));
        let bot = BotKind::Ppp;
        let market = MarketKind::BtcUsdt;
        let year = 2022;
        let granularity = GranularityKind::Weekly;
        let directory = LeafDir::Ohlc60m;
        let finder = super::Finder::new(
            dp.get_bucket_name(),
            dp.get_data_producer_kind(),
            market,
            year,
            bot,
            granularity,
        ).await;
        finder
            .delete_files(finder.get_client_clone(), RootDir::Strategy, directory.clone())
            .await;
        // Results to proceed
        let dfs = split_raw_data_into_calendar_weeks(dp.clone(), finder.clone(), directory).await;

        let res = prepare_weekly_data(
            dp.clone(),
            finder.clone(),
            TIME_INTERVAL,
            directory,
            dfs,
        );
        // .await
        // .unwrap();
        // let out_dir = finder.path_to_leaf(&RootDir::Strategy, &directory, None);
        let target_dir = finder._path_to_target(&RootDir::Strategy, &directory);

        // BEGIN: Test CW 8
        let mut target = load_csv(&target_dir, "8.csv").await.unwrap();
        // let mut result = load_csv(&out_dir, "8.csv").await.unwrap();
        assert_eq!(
            res.get(&(8, None))
                .unwrap()
                .clone()
                .unwrap()
                .frame_equal(&target),
            true
        );
        // END: Test CW 8

        // BEGIN: Test CW 9
        target = load_csv(&target_dir, "9.csv").await.unwrap();
        // result = load_csv(&out_dir, "9.csv").await.unwrap();
        assert_eq!(
            res.get(&(9, None))
                .unwrap()
                .clone()
                .unwrap()
                .frame_equal(&target),
            true
        );
        // END: Test CW 9

        // BEGIN: Test CW 10
        target = load_csv(&target_dir, "10.csv").await.unwrap();
        // result = load_csv(&out_dir, "10.csv").await.unwrap();
        assert_eq!(
            res.get(&(10, None))
                .unwrap()
                .clone()
                .unwrap()
                .frame_equal(&target),
            true
        );
        // END: Test CW 10
        finder
            .delete_files(finder.get_client_clone(), RootDir::Strategy, directory.clone())
            .await;
    }

    #[tokio::test]
    async fn test_ohlc_prepare_daily_data() {
        let dp: Arc<dyn DataProducer + Send + Sync> = Arc::new(Test::new(
            std::path::PathBuf::from(GCS_DATA_BUCKET),
        ));
        let bot = BotKind::Ppp;
        let market = MarketKind::BtcUsdt;
        let year = 2022;
        let granularity = GranularityKind::Daily;
        let directory = LeafDir::Ohlc60m;
        let finder = super::Finder::new(
            dp.get_bucket_name(),
            dp.get_data_producer_kind(),
            market,
            year,
            bot,
            granularity,
        ).await;
        finder
            .delete_files(finder.get_client_clone(), RootDir::Strategy, directory.clone())
            .await;

        // Results to proceed
        let dfs = split_raw_data_into_calendar_weeks(dp.clone(), finder.clone(), directory).await;

        let res = prepare_daily_data(
            dp.clone(),
            finder.clone(),
            TIME_INTERVAL,
            directory,
            dfs,
        );
        // .await
        // .unwrap();
        // let out_dir = finder.path_to_leaf(&RootDir::Strategy, &directory, None);
        let target_dir = finder._path_to_target(&RootDir::Strategy, &directory);

        // BEGIN: Test CW 8
        let mut target = load_csv(&target_dir, "8_1.csv").await.unwrap();
        // let mut result = load_csv(&out_dir, "81.csv").await.unwrap();
        assert_eq!(
            res.get(&(8, Some(1)))
                .unwrap()
                .clone()
                .unwrap()
                .frame_equal(&target),
            true
        );
        // END: Test CW 8

        // BEGIN: Test CW 9
        target = load_csv(&target_dir, "9_1.csv").await.unwrap();
        // result = load_csv(&out_dir, "91.csv").await.unwrap();
        assert_eq!(
            res.get(&(9, Some(1)))
                .unwrap()
                .clone()
                .unwrap()
                .frame_equal(&target),
            true
        );

        target = load_csv(&target_dir, "9_2.csv").await.unwrap();
        // result = load_csv(&out_dir, "92.csv").await.unwrap();
        assert_eq!(
            res.get(&(9, Some(2)))
                .unwrap()
                .clone()
                .unwrap()
                .frame_equal(&target),
            true
        );

        target = load_csv(&target_dir, "9_3.csv").await.unwrap();
        // result = load_csv(&out_dir, "93.csv").await.unwrap();
        assert_eq!(
            res.get(&(9, Some(3)))
                .unwrap()
                .clone()
                .unwrap()
                .frame_equal(&target),
            true
        );

        target = load_csv(&target_dir, "9_4.csv").await.unwrap();
        // result = load_csv(&out_dir, "94.csv").await.unwrap();
        assert_eq!(
            res.get(&(9, Some(4)))
                .unwrap()
                .clone()
                .unwrap()
                .frame_equal(&target),
            true
        );

        target = load_csv(&target_dir, "9_5.csv").await.unwrap();
        // result = load_csv(&out_dir, "95.csv").await.unwrap();
        assert_eq!(
            res.get(&(9, Some(5)))
                .unwrap()
                .clone()
                .unwrap()
                .frame_equal(&target),
            true
        );
        // END: Test CW 9

        // BEGIN: Test CW 10
        target = load_csv(&target_dir, "10_3.csv").await.unwrap();
        // result = load_csv(&out_dir, "103.csv").await.unwrap();
        assert_eq!(
            res.get(&(10, Some(3)))
                .unwrap()
                .clone()
                .unwrap()
                .frame_equal(&target),
            true
        );
        // END: Test CW 10
        finder
            .delete_files(finder.get_client_clone(), RootDir::Strategy, directory.clone())
            .await;
    }

    // TODO: async fn test_ohlc_process_batch() {

    // Begin Test Volume
    /// This unit test checks if the test batch in `/aggTrades/tick` gets processed correctly.
    ///
    /// # Note:
    /// * The batch files are split by calendar weeks
    /// * Each calendar week group gets stored into a separate file
    /// * The header of the processed `.csv` file that is stored in the `/cw` subdirectory changes from
    ///
    /// ```
    /// // Old header
    /// atid    ,px    ,qx    ,ftid    ,ltid    ,ts    ,bm    ,btpm
    ///
    /// // To new header (extra cw column)
    /// atid    ,px    ,qx    ,ftid    ,ltid    ,ts    ,bm    ,btpm    ,cw
    /// ```
    #[tokio::test]
    async fn test_split_raw_data_into_calendar_weeks() {
        let dp: Arc<dyn DataProducer + Send + Sync> = Arc::new(Test::new(
            std::path::PathBuf::from(GCS_DATA_BUCKET),
        ));
        let bot = BotKind::Ppp;
        let market = MarketKind::BtcUsdt;
        let year = 2022;
        let directory = LeafDir::AggTrades;
        let granularity = GranularityKind::Weekly;
        let finder = super::Finder::new(
            dp.get_bucket_name(),
            dp.get_data_producer_kind(),
            market,
            year,
            bot,
            granularity,
        ).await;
        finder
            .delete_files(finder.get_client_clone(), RootDir::Data, directory.clone())
            .await;
        let dfs = split_raw_data_into_calendar_weeks(dp, finder.clone(), directory).await;
        // .unwrap();
        let target_dir = finder._path_to_target(&RootDir::Data, &directory);

        // BEGIN: Test CW 7
        let mut target = load_csv(&target_dir, "7.csv").await.unwrap();
        //  let mut result = load_csv(&out_dir, "7.csv").await.unwrap();
        assert_eq!(dfs.get(&7).unwrap().frame_equal(&target), true);
        // END: Test CW 7

        // BEGIN: Test CW 8
        target = load_csv(&target_dir, "8.csv").await.unwrap();
        // result = load_csv(&out_dir, "8.csv").await.unwrap();
        assert_eq!(dfs.get(&8).unwrap().frame_equal(&target), true);
        // END: Test CW 8

        // BEGIN: Test CW 9
        target = load_csv(&target_dir, "9.csv").await.unwrap();
        // result = load_csv(&out_dir, "9.csv").await.unwrap();
        assert_eq!(dfs.get(&9).unwrap().frame_equal(&target), true);
        // END: Test CW 9

        // BEGIN: Test CW 10
        target = load_csv(&target_dir, "10.csv").await.unwrap();
        // result = load_csv(&out_dir, "10.csv").await.unwrap();
        assert_eq!(dfs.get(&10).unwrap().frame_equal(&target), true);
        // END: Test CW 10

        // BEGIN: Test CW 12
        target = load_csv(&target_dir, "12.csv").await.unwrap();
        // result = load_csv(&out_dir, "12.csv").await.unwrap();
        assert_eq!(dfs.get(&12).unwrap().frame_equal(&target), true);
        // END: Test CW 12

        //google_cloud_storage::Data::delete_files(&finder, &directory, true);
    }

    /// This unit test checks if the test batch in `{EDS}/data/test/2022/raw/btcusdt/aggTrades/tick/cw` gets processed correctly.
    ///
    /// # Note:
    /// * The batch files are filterd by the `TIME_INTERVAL`
    /// * The files get stored in `"{EDS}/data/test/2022/ppp/btcusdt/binance/cw/aggTrades/tick"`
    /// * The header of the processed `.csv` file that is stored in the `/cw` subdirectory changes from
    ///
    /// ```
    /// // Old header
    /// atid    ,px    ,qx    ,ftid    ,ltid    ,ts    ,bm    ,btpm    ,cw
    ///
    /// // To new header (extra cw column)
    /// atid    ,px    ,qx    ,ftid    ,ltid    ,ts    ,bm    ,btpm    ,cw    ,in_interval
    /// ```
    #[tokio::test]
    async fn test_prepare_weekly_data() {
        let dp: Arc<dyn DataProducer + Send + Sync> = Arc::new(Test::new(
            std::path::PathBuf::from(GCS_DATA_BUCKET),
        ));
        let bot = BotKind::Ppp;
        let market = MarketKind::BtcUsdt;
        let year = 2022;
        let directory = LeafDir::AggTrades;
        let granularity = GranularityKind::Weekly;
        let finder = super::Finder::new(
            dp.get_bucket_name(),
            dp.get_data_producer_kind(),
            market,
            year,
            bot,
            granularity,
        ).await;
        finder
            .delete_files(finder.get_client_clone(), RootDir::Strategy, directory.clone())
            .await;
        let dfs = split_raw_data_into_calendar_weeks(dp.clone(), finder.clone(), directory).await;
        let res = prepare_weekly_data(dp, finder.clone(), TIME_INTERVAL, directory, dfs);
        //.unwrap();
        let target_dir = finder._path_to_target(&RootDir::Strategy, &directory);

        // BEGIN: Test CW 8
        let mut target = load_csv(&target_dir, "8.csv").await.unwrap();
        // let mut result = load_csv(&out_dir, "8.csv").await.unwrap();
        assert_eq!(
            res.get(&(8, None))
                .unwrap()
                .clone()
                .unwrap()
                .frame_equal(&target),
            true
        );
        // END: Test CW 8

        // BEGIN: Test CW 9
        target = load_csv(&target_dir, "9.csv").await.unwrap();
        // result = load_csv(&out_dir, "9.csv").await.unwrap();
        assert_eq!(
            res.get(&(9, None))
                .unwrap()
                .clone()
                .unwrap()
                .frame_equal(&target),
            true
        );
        // END: Test CW 9

        // BEGIN: Test CW 10
        target = load_csv(&target_dir, "10.csv").await.unwrap();
        //result = load_csv(&out_dir, "10.csv").await.unwrap();
        assert_eq!(
            res.get(&(10, None))
                .unwrap()
                .clone()
                .unwrap()
                .frame_equal(&target),
            true
        );
        // END: Test CW 10

        // BEGIN: Test CW 12
        target = load_csv(&target_dir, "12.csv").await.unwrap();
        // result = load_csv(&out_dir, "12.csv").await.unwrap();
        assert_eq!(
            res.get(&(12, None))
                .unwrap()
                .clone()
                .unwrap()
                .frame_equal(&target),
            true
        );
        // END: Test CW 12

        //google_cloud_storage::Strategy::delete_files(&finder,&dp.get_client(), &directory);
    }

    /// This unit test checks if the test batch in `"{EDS}/data/test/2022/ppp/btcusdt/binance/cw/aggTrades/tick"` gets processed correctly.
    ///
    /// # Note:
    /// * The batch files are filterd by the `TIME_INTERVAL`
    /// * The files get stored in `"{EDS}/strategy/ppp/btcusdt/2022/cw/vol"`
    /// * The header of the processed `.csv` file that is stored in the `"{EDS}/data/test/2022/ppp/btcusdt/binance/cw/aggTrades/tick"` directory changes from
    /// * The resulting file is sorted by price in ascending order
    /// ```
    /// // Old header
    /// atid    ,px    ,qx    ,ftid    ,ltid    ,ts    ,bm    ,btpm    ,cw    ,in_interval
    ///
    /// // To new header
    /// px      ,qx
    /// ```
    #[tokio::test]
    async fn test_compute_weekly_volume_from_agg_trades() {
        let dp: Arc<dyn DataProducer + Send + Sync> = Arc::new(Test::new(
            std::path::PathBuf::from(GCS_DATA_BUCKET),
        ));
        let bot = BotKind::Ppp;
        let market = MarketKind::BtcUsdt;
        let year = 2022;
        let granularity = GranularityKind::Weekly;
        let finder = super::Finder::new(
            dp.get_bucket_name(),
            dp.get_data_producer_kind(),
            market,
            year,
            bot,
            granularity,
        ).await;
        finder
            .delete_files(finder.get_client_clone(), RootDir::Strategy, LeafDir::Vol)
            .await;

        // Load data
        let dfs =
            split_raw_data_into_calendar_weeks(dp.clone(), finder.clone(), LeafDir::AggTrades)
                .await;
        // Prepare data
        let dfs = prepare_weekly_data(
            dp.clone(),
            finder.clone(),
            TIME_INTERVAL,
            LeafDir::AggTrades,
            dfs,
        );

        let res =
            compute_volume_from_agg_trades(dp.clone(), finder.clone(), LeafDir::AggTrades, dfs)
                .await;
        let target_dir = finder._path_to_target(&RootDir::Strategy, &LeafDir::Vol);

        // BEGIN: Test CW 8
        let mut target = load_csv(&target_dir, "8.csv").await.unwrap();
        // let mut result = load_csv(&out_dir, "8.csv").await.unwrap();
        assert_eq!(
            res.get(&(8, None))
                .unwrap()
                .clone()
                .unwrap()
                .frame_equal(&target),
            true
        );
        // END: Test CW 8

        // BEGIN: Test CW 9
        target = load_csv(&target_dir, "9.csv").await.unwrap();
        // result = load_csv(&out_dir, "9.csv").await.unwrap();
        assert_eq!(
            res.get(&(9, None))
                .unwrap()
                .clone()
                .unwrap()
                .frame_equal(&target),
            true
        );
        // END: Test CW 9

        // BEGIN: Test CW 10
        target = load_csv(&target_dir, "10.csv").await.unwrap();
        // result = load_csv(&out_dir, "10.csv").await.unwrap();
        assert_eq!(
            res.get(&(10, None))
                .unwrap()
                .clone()
                .unwrap()
                .frame_equal(&target),
            true
        );
        // END: Test CW 10

        // BEGIN: Test CW 12
        target = load_csv(&target_dir, "12.csv").await.unwrap();
        // result = load_csv(&out_dir, "12.csv").await.unwrap();
        assert_eq!(
            res.get(&(12, None))
                .unwrap()
                .clone()
                .unwrap()
                .frame_equal(&target),
            true
        );
        // END: Test CW 12

        finder
            .delete_files(finder.get_client_clone(), RootDir::Strategy, LeafDir::Vol)
            .await;
    }

    #[tokio::test]
    async fn test_prepare_daily_data() {
        let dp: Arc<dyn DataProducer + Send + Sync> = Arc::new(Test::new(
            std::path::PathBuf::from(GCS_DATA_BUCKET),
        ));
        let bot = BotKind::Ppp;
        let market = MarketKind::BtcUsdt;
        let year = 2022;
        let directory = LeafDir::AggTrades;
        let granularity = GranularityKind::Daily;
        let finder = super::Finder::new(
            dp.get_bucket_name(),
            dp.get_data_producer_kind(),
            market,
            year,
            bot,
            granularity,
        ).await;
        finder
            .delete_files(finder.get_client_clone(), RootDir::Strategy, directory.clone())
            .await;
        // Results to proceed
        let dfs = split_raw_data_into_calendar_weeks(dp.clone(), finder.clone(), directory).await;
        let res = prepare_daily_data(dp, finder.clone(), TIME_INTERVAL, directory, dfs);
        // .await
        // .unwrap();
        let target_dir = finder._path_to_target(&RootDir::Strategy, &directory);

        // BEGIN: Test CW 8
        let mut target = load_csv(&target_dir, "8_1.csv").await.unwrap();
        // let mut result = load_csv(&out_dir, "81.csv").await.unwrap();
        assert_eq!(
            res.get(&(8, Some(1)))
                .unwrap()
                .clone()
                .unwrap()
                .frame_equal(&target),
            true
        );

        target = load_csv(&target_dir, "8_4.csv").await.unwrap();
        // result = load_csv(&out_dir, "84.csv").await.unwrap();
        assert_eq!(
            res.get(&(8, Some(4)))
                .unwrap()
                .clone()
                .unwrap()
                .frame_equal(&target),
            true
        );

        target = load_csv(&target_dir, "8_5.csv").await.unwrap();
        // result = load_csv(&out_dir, "85.csv").await.unwrap();
        assert_eq!(
            res.get(&(8, Some(5)))
                .unwrap()
                .clone()
                .unwrap()
                .frame_equal(&target),
            true
        );
        // END: Test CW 8

        // BEGIN: Test CW 9
        target = load_csv(&target_dir, "9_1.csv").await.unwrap();
        // result = load_csv(&out_dir, "91.csv").await.unwrap();
        assert_eq!(
            res.get(&(9, Some(1)))
                .unwrap()
                .clone()
                .unwrap()
                .frame_equal(&target),
            true
        );

        target = load_csv(&target_dir, "9_2.csv").await.unwrap();
        // result = load_csv(&out_dir, "92.csv").await.unwrap();
        assert_eq!(
            res.get(&(9, Some(2)))
                .unwrap()
                .clone()
                .unwrap()
                .frame_equal(&target),
            true
        );
        // END: Test CW 9

        // BEGIN: Test CW 10
        target = load_csv(&target_dir, "10_4.csv").await.unwrap();
        // result = load_csv(&out_dir, "104.csv").await.unwrap();
        assert_eq!(
            res.get(&(10, Some(4)))
                .unwrap()
                .clone()
                .unwrap()
                .frame_equal(&target),
            true
        );

        target = load_csv(&target_dir, "10_5.csv").await.unwrap();
        // result = load_csv(&out_dir, "105.csv").await.unwrap();
        assert_eq!(
            res.get(&(10, Some(5)))
                .unwrap()
                .clone()
                .unwrap()
                .frame_equal(&target),
            true
        );
        // END: Test CW 10

        // BEGIN: Test CW 12
        target = load_csv(&target_dir, "12_3.csv").await.unwrap();
        // result = load_csv(&out_dir, "123.csv").await.unwrap();
        assert_eq!(
            res.get(&(12, Some(3)))
                .unwrap()
                .clone()
                .unwrap()
                .frame_equal(&target),
            true
        );
        // END: Test CW 12

        //google_cloud_storage::Strategy::delete_files(&finder,&dp.get_client(), &directory);
    }

    #[tokio::test]
    async fn test_compute_daily_volume_from_agg_trades() {
        let dp: Arc<dyn DataProducer + Send + Sync> = Arc::new(Test::new(
            std::path::PathBuf::from(GCS_DATA_BUCKET),
        ));
        let bot = BotKind::Ppp;
        let market = MarketKind::BtcUsdt;
        let year = 2022;
        let granularity = GranularityKind::Daily;
        let finder = super::Finder::new(
            dp.get_bucket_name(),
            dp.get_data_producer_kind(),
            market,
            year,
            bot,
            granularity,
        ).await;
        finder
            .delete_files(finder.get_client_clone(), RootDir::Strategy, LeafDir::Vol)
            .await;

        // Load data
        let dfs =
            split_raw_data_into_calendar_weeks(dp.clone(), finder.clone(), LeafDir::AggTrades)
                .await;
        // Prepare data
        let dfs = prepare_daily_data(
            dp.clone(),
            finder.clone(),
            TIME_INTERVAL,
            LeafDir::AggTrades,
            dfs,
        );

        let res =
            compute_volume_from_agg_trades(dp.clone(), finder.clone(), LeafDir::AggTrades, dfs)
                .await;
        //.unwrap();
        let target_dir = finder._path_to_target(&RootDir::Strategy, &LeafDir::Vol);

        // BEGIN: Test CW 8
        let mut target = load_csv(&target_dir, "8_1.csv").await.unwrap();
        // let mut result = load_csv(&out_dir, "81.csv").await.unwrap();
        assert_eq!(
            res.get(&(8, Some(1)))
                .unwrap()
                .clone()
                .unwrap()
                .frame_equal(&target),
            true
        );

        target = load_csv(&target_dir, "8_4.csv").await.unwrap();
        // result = load_csv(&out_dir, "84.csv").await.unwrap();
        assert_eq!(
            res.get(&(8, Some(4)))
                .unwrap()
                .clone()
                .unwrap()
                .frame_equal(&target),
            true
        );

        target = load_csv(&target_dir, "8_5.csv").await.unwrap();
        // result = load_csv(&out_dir, "85.csv").await.unwrap();
        assert_eq!(
            res.get(&(8, Some(5)))
                .unwrap()
                .clone()
                .unwrap()
                .frame_equal(&target),
            true
        );
        // END: Test CW 8

        // BEGIN: Test CW 9
        target = load_csv(&target_dir, "9_1.csv").await.unwrap();
        // result = load_csv(&out_dir, "91.csv").await.unwrap();
        assert_eq!(
            res.get(&(9, Some(1)))
                .unwrap()
                .clone()
                .unwrap()
                .frame_equal(&target),
            true
        );

        target = load_csv(&target_dir, "9_2.csv").await.unwrap();
        // result = load_csv(&out_dir, "92.csv").await.unwrap();
        assert_eq!(
            res.get(&(9, Some(2)))
                .unwrap()
                .clone()
                .unwrap()
                .frame_equal(&target),
            true
        );
        // END: Test CW 9

        // BEGIN: Test CW 10
        target = load_csv(&target_dir, "10_4.csv").await.unwrap();
        // result = load_csv(&out_dir, "104.csv").await.unwrap();
        assert_eq!(
            res.get(&(10, Some(4)))
                .unwrap()
                .clone()
                .unwrap()
                .frame_equal(&target),
            true
        );

        target = load_csv(&target_dir, "10_5.csv").await.unwrap();
        // result = load_csv(&out_dir, "105.csv").await.unwrap();
        assert_eq!(
            res.get(&(10, Some(5)))
                .unwrap()
                .clone()
                .unwrap()
                .frame_equal(&target),
            true
        );
        // END: Test CW 10

        // BEGIN: Test CW 12
        target = load_csv(&target_dir, "12_3.csv").await.unwrap();
        //result = load_csv(&out_dir, "123.csv").await.unwrap();
        assert_eq!(
            res.get(&(12, Some(3)))
                .unwrap()
                .clone()
                .unwrap()
                .frame_equal(&target),
            true
        );
        // END: Test CW 12

        finder
            .delete_files(finder.get_client_clone(), RootDir::Strategy, LeafDir::Vol)
            .await;
    }

    // TODO: for volume async fn test_process_batch() {
}
