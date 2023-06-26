// Intern crates
use crate::{
    common::{
        functions::{df_from_file, write_df_to_bytes},
        gcs::{delete_file, get_files_in_bucket, upload_file},
    },
    enums::{
        bots::BotKind,
        data::*,
        markets::{GranularityKind, MarketKind},
        producers::ProducerKind,
    }, config,
};

// Extern crates
use google_cloud_storage::client::Client;
use polars::prelude::DataFrame;
use rayon::{iter::ParallelIterator, prelude::IntoParallelRefIterator};
use regex::Regex;
use std::{
    collections::HashSet,
    io,
    path::PathBuf,
    sync::{Arc, Mutex},
};

// Types
type FileCache = Arc<Mutex<HashSet<PathBuf>>>;

/// The consumer and bot need to know where to find the respective files in our hierachy. Upon start we configure our `Finder` on the variable parameters
/// * `bucket` - (i.e. my-bucket, but currently not used: `#[allow(dead_code)]`)
/// * `producer` - (i.e. binance, ninja, test, ...)
/// * `market` - (i.e. 6e, btcusdt, ...)
/// * `year` - (i.e. 2023, 2022, ...)
/// * `bot` - (i.e. ppp, magneto, gap, ...)
/// * `granularity` - (currently only: weekly or daily)
///
/// Hence, to get the path to a directory we only say if we want to look inside `data` or `strategy` and what target directory we want to find (`ohlc-1m`, `tick`, etc.).
#[derive(Clone)]
pub struct Finder {
    #[allow(dead_code)]
    bucket: PathBuf,
    producer: ProducerKind,
    market: MarketKind,
    year: u32,
    bot: BotKind,
    granularity: GranularityKind,
    client: Arc<Client>,
}
impl Finder {
    /// Creates a new `Finder`to locate files inside the directory hierachy. Hence, to get the path to a directory
    /// we only say if we want to look inside `data` or `strategy` and what target directory we want to find (`ohlc-1m`, `tick`, etc.).
    ///
    /// # Arguments
    /// * `bucket` - bucket name
    /// * `producer` - binance, ninja, test, ...
    /// * `market` - 6e, btcusdt, ...
    /// * `year` - 2023, 2022, ...
    /// * `bot` - ppp, magneto, gap, ...
    /// * `granularity` - currently only: weekly or daily
    ///
    /// # Examples
    /// * `{bucket}/data/{producer}/{market}/{year}/ohlc-1h`
    /// * `{bucket}/strategy/{bot}/{market}/{year}/{granularity}/ohlc-1h`
    pub async fn new(
        bucket: PathBuf,
        producer: ProducerKind,
        market: MarketKind,
        year: u32,
        bot: BotKind,
        granularity: GranularityKind,
    ) -> Self {
        Finder {
            bucket,
            producer,
            market,
            year,
            bot,
            granularity,
            client: Arc::new(config::get_google_cloud_client().await)
        }
    }

    pub fn set_year(&mut self, year: u32) {
        self.year = year;
    }

    /// TODO makes sense?
    pub fn get_client_clone(&self) -> Arc<Client> {
        self.client.clone()
    }

    /// Returns the year of the current `Finder` configuration
    pub fn get_year(&self) -> u32 {
        self.year
    }

    /// Returns the market of the current `Finder` configuration
    pub fn get_market(&self) -> MarketKind {
        self.market
    }

    /// Returns the granularity of the current `Finder` configuration
    pub fn get_granularity(&self) -> GranularityKind {
        self.granularity
    }

    /// Returns the base path to the given leaf directory in the given root
    ///
    /// # Arguments
    /// * `root` - root directory `{ strategy | data }`
    /// * `leaf`- leaf directory, i.e. `tick`, `ohlc-1m`, etc.
    /// * `cw` - the directory where the raw data is split into calendar weeks
    /// # Example
    /// ```
    /// let finder = Finder {
    ///     bucket: PathBuf::from("my-bucket")
    ///     producer: ProducerKind::Ninja,
    ///     market: MarketKind::EurUsd,
    ///     year: 2022,
    ///     bot: BotKind::Ppp,
    ///     granularity: GranularityKind::Daily,
    /// }
    ///
    /// assert_eq!(PathBuf::from("my-bucket/strategy/ppp/6e/2022/day/ohlc-1m"), finder.find(RootDir::Strategy, LeafDir::Ohlc(KPeriod::M1), None))
    /// ```
    pub fn path_to_leaf(&self, root: &RootDir, leaf: &LeafDir, cw: Option<bool>) -> PathBuf {
        match root {
            RootDir::Data => self.find_in_data(leaf, cw.unwrap()),
            RootDir::Strategy => self.find_in_strategy(leaf),
        }
    }

    /// This function should only be used for unit tests. Returns the base path to the given leaf directory that contains the target `.csv` files for testing.
    /// Only `ProducerKind::Test` contains those directories.
    ///
    /// # Arguments
    /// * `root` - root directory `{ strategy | data }`
    /// * `leaf`- leaf directory, i.e. `tick`, `ohlc-1m`, etc.
    ///
    /// # Example
    /// ```
    /// let finder = Finder {
    ///     bucket: PathBuf::from("my-bucket")
    ///     producer: ProducerKind::Test,
    ///     market: MarketKind::BtcUsdt,
    ///     year: 2022,
    ///     bot: BotKind::Ppp,
    ///     granularity: GranularityKind::Daily,
    /// }
    ///
    /// assert_eq!(PathBuf::from("my-bucket/strategy/ppp/btcusdt/2022/day/_ohlc-1m"), finder._find_target(RootDir::Strategy, LeafDir::Ohlc(KPeriod::M1), None))
    /// ```
    pub fn _path_to_target(&self, root: &RootDir, leaf: &LeafDir) -> PathBuf {
        match root {
            RootDir::Data => self.find_target_in_data(leaf),
            RootDir::Strategy => self.find_target_in_strategy(leaf),
        }
    }

    /// This function deletes all files in the given directory. It spawns multiple threads, therefore we own client.
    ///
    /// # Arguments
    /// * `client` - google cloud storage client
    /// * `root` - root directory `{ strategy | data }`
    /// * `leaf`- leaf directory, i.e. `tick`, `ohlc-1m`, etc.
    ///
    /// # Example
    /// The following finder deletes all `.csv` files inside `PathBuf::from("my-bucket/strategy/ppp/btcusdt/2022/day/ohlc-1m")`,
    /// if we call `finder.delete_files(Client::new(config), RootDir::Strategy, LeafDir::Ohlc(KPeriod::M1))`
    /// ```
    /// let finder = Finder {
    ///     bucket: PathBuf::from("my-bucket")
    ///     producer: ProducerKind::Test,
    ///     market: MarketKind::BtcUsdt,
    ///     year: 2022,
    ///     bot: BotKind::Ppp,
    ///     granularity: GranularityKind::Daily,
    /// }
    ///
    /// // Delete all files in my-bucket/strategy/ppp/btcusdt/2022/day/ohlc-1m
    /// finder.delete_files(Client::new(config), RootDir::Strategy, LeafDir::Ohlc(KPeriod::M1));
    /// ```
    pub async fn delete_files(&self, client: Arc<Client>, root: RootDir, leaf: LeafDir) {
        // Get files that we want to delete. We never want to delete raw data files, hence we set `cw = Some(true)`
        let files_to_delete = self
            .list_files(client.clone(), root, leaf, Some(true))
            .await
            .unwrap();

        // Delete files
        let tasks: Vec<_> = files_to_delete
            .into_iter()
            .map(|file| {
                let client = client.clone();
                tokio::spawn(async move { delete_file(&*client, &file).await })
            })
            .collect();

        for task in tasks {
            task.await.unwrap();
        }
    }

    /// This function saves the DataFrame to the given leaf directory.
    ///
    /// # Arguments
    /// * `client` - google cloud storage client
    /// * `root` - root directory `{ strategy | data }`
    /// * `leaf`- leaf directory, i.e. `tick`, `ohlc-1m`, etc.
    /// * `file_name` - file name
    /// * `df` - DataFrame we want to save
    /// * `cache` - If we will save parts of a DataFrame we can add a cache to quickly lookup if a part is already saved
    /// * `alighn_col` - It can happen, that cached `DataFrame`s are not uploaded correctly. Therfore we sort by the column we want to align
    ///
    /// # Example
    /// The following finder uploads the DataFrame to `PathBuf::from("my-bucket/strategy/ppp/btcusdt/2022/day/ohlc-1m")`,
    /// if we call `finder.save_file(Client::new(config), RootDir::Strategy, LeafDir::Ohlc(KPeriod::M1), "foo.csv".to_string(), df, None)`
    /// ```
    /// let finder = Finder {
    ///     bucket: PathBuf::from("my-bucket")
    ///     producer: ProducerKind::Test,
    ///     market: MarketKind::BtcUsdt,
    ///     year: 2022,
    ///     bot: BotKind::Ppp,
    ///     granularity: GranularityKind::Daily,
    /// }
    ///
    /// let df = DataFrame::default();
    ///
    /// // Upload files to my-bucket/strategy/ppp/btcusdt/2022/day/ohlc-1m
    /// finder.save_file(Client::new(config), RootDir::Strategy, LeafDir::Ohlc(KPeriod::M1), "foo.csv".to_string(), df, None, Arc::new("ots".to_string()));
    /// ```
    pub async fn save_file(
        &self,
        client: Arc<Client>,
        root: RootDir,
        leaf: LeafDir,
        file_name: String,
        df: DataFrame,
        cache: Option<FileCache>,
        align_ts_col: Option<Arc<String>>,
    ) {
        match root {
            RootDir::Data => {
                self.save_file_in_data( &leaf, &file_name, df, cache.unwrap(), align_ts_col.unwrap())
                    .await
            }
            RootDir::Strategy => {
                self.save_file_in_strategy(client, &leaf, &file_name, df)
                    .await
            }
        }
    }

    pub async fn save_performance_report(&self, client: Arc<Client>, file_name: String, df: DataFrame) {
        let market = self.market.to_string().to_lowercase();
        let ap = PathBuf::from(format!("strategy/ppp/{market}/{file_name}"));

        // Write df to bytes
        let bytes = write_df_to_bytes(df);

        // Upload df
        upload_file(&*client, &ap, bytes).await;
    }

    /// Returns all files with `.csv` extension in given path. This function does not
    /// filter directories recursively.
    ///
    /// # Attributes
    /// * `client` - google cloud storage client
    /// * `root` - root directory `{ strategy | data }`
    /// * `leaf`- leaf directory, i.e. `tick`, `ohlc-1m`, etc.
    /// * `cw` - the directory where the raw data is split into calendar weeks
    ///
    /// # Example
    /// Suppose inisde `"data/test/btcusdt/2022/aggTrades/"` are two files:
    /// 1. `BTCUSDT-aggTrades-2022-02.csv`
    /// 2. `BTCUSDT-aggTrades-2022-03.csv`
    /// ```
    /// let finder = Finder {
    ///     bucket: PathBuf::from("my-bucket")
    ///     producer: ProducerKind::Test,
    ///     market: MarketKind::BtcUsdt,
    ///     year: 2022,
    ///     bot: BotKind::Ppp,
    ///     granularity: GranularityKind::Daily,
    /// }
    ///
    /// let result = finder.list_files(Client::new(config), RootDir::Data, LeafDir::AggTrades).await.unwrap();
    ///
    /// assert_eq!(result[0], "data/test/btcusdt/2022/aggTrades/BTCUSDT-aggTrades-2022-02.csv")
    /// assert_eq!(result[1], "data/test/btcusdt/2022/aggTrades/BTCUSDT-aggTrades-2022-03.csv")
    /// ```
    pub async fn list_files(
        &self,
        client: Arc<Client>,
        root: RootDir,
        leaf: LeafDir,
        cw: Option<bool>,
    ) -> std::result::Result<Vec<PathBuf>, io::Error> {
        let files_in_bucket = get_files_in_bucket(&*client, "trust-data").await;

        // Get absolute path
        let dir = self.path_to_leaf(&root, &leaf, cw);

        // Filter all `.csv` files by regular expression
        let mut regex = dir.to_str().unwrap().to_string();
        regex.push_str("/[^/]+.csv");
        let regex = Regex::new(&regex).unwrap();

        // Filter files parallel using rayon
        let mut files: Vec<PathBuf> = files_in_bucket
            .par_iter()
            .filter(|x| regex.is_match(&x.name))
            .map(|x| PathBuf::from(&x.name))
            .collect();

        // Currently files are sorted as strings "../11.csv". This results into "11.csv" -> "111.csv" -> 112.csv -> ... -> "12.csv".
        // Therefore we parse the file name as integer values and return the files sorted ascendingly by integers
        if let (RootDir::Data, Some(false)) = (root, cw) {
            // We can sort all files but the raw data files, as they don't follow our naming conventions {cw}{day}.csv
            return Ok(files);
        }

        if let (RootDir::Strategy, LeafDir::ProfitAndLoss) = (root, leaf) {
            return Ok(files);
        }

        // Sort files
        files.sort_by_key(|x| {
            PathBuf::from(x)
                .file_stem()
                .unwrap()
                .to_str()
                .unwrap()
                .parse::<u32>()
                .unwrap()
        });

        Ok(files)
    }

    /// This function saves the DataFrame to the `Data` directory.
    async fn save_file_in_data(
        &self,
        leaf: &LeafDir,
        file_name: &str,
        df: DataFrame,
        cache: FileCache,
        align_ts_col: Arc<String>,
    ) {
        // Absolute file path
        let ap = self.find_in_data(leaf, true).join(file_name);

        // Initialize bytes vec to get df as bytes
        #[allow(unused_assignments)]
        let mut bytes = Vec::new();

        // By default a value is not cached
        #[allow(unused_assignments)]
        let mut cached = false;

        // The `std::sync::MutexGuard` type is not Send. This means that we can't send a mutex lock to another thread. This is because the Tokio runtime can move a task
        // between threads at every .await. To avoid this, we restructure our code such that the mutex lock's destructor runs before the .await.
        // See: https://tokio.rs/tokio/tutorial/shared-state#holding-a-mutexguard-across-an-await
        {
            // Aquire cache to check if part of this df already exists
            let mut lock = cache.lock().unwrap();
            cached = lock.insert(ap.clone());
        }

        // Part of this df is already uploaded, because lock.insert returns `false` if element exists in cache
        if !cached {
            // Download DataFrame and append to this df
            let mut df_part = df_from_file( &ap, None, None).await;
            while let Err(e) = df_part {
                eprintln!("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ Error: {e} \n Trying again to download file.");
                df_part = df_from_file( &ap, None, None).await;
            }

            // df_part is Ok(_)
            let mut df_part = df_part.unwrap();
            
            df_part.extend(&df).unwrap();

            // Sort df by ts_col (currently align_ts_col is the timestamp col we want to align) ascendingly
            let sorted = df_part.sort([&*align_ts_col], false).unwrap();

            // Write df to bytes
            bytes = write_df_to_bytes(sorted);
        } else {
            // Don't need to glue two df's together as this is the first part we are uploading
            // Write it to bytes
            bytes = write_df_to_bytes(df);
        }

        // Upload df
        upload_file(&*self.client, &ap, bytes).await;
    }

    /// This function saves the DataFrame to the `Strategy` directory.
    async fn save_file_in_strategy(
        &self,
        client: Arc<Client>,
        leaf: &LeafDir,
        file_name: &str,
        df: DataFrame,
    ) {
        // Absolute file path
        let ap = self.find_in_strategy(leaf).join(file_name);

        // Write df to bytes
        let bytes = write_df_to_bytes(df);

        // Upload df
        upload_file(&*client, &ap, bytes).await;
    }

    /// This function contructs the following paths, if
    /// * `cw = false` - The path `{bucket}/data/{bot}/{market}/{year}/{granularity}/{leaf}` for a given leaf is contructed.
    /// * `cw = true` - The path `{bucket}/data/{bot}/{market}/{year}/{granularity}/{leaf}/cw` for a given leaf is contructed.
    fn find_in_data(&self, leaf: &LeafDir, cw: bool) -> PathBuf {
        // Root directory is data
        let mut result = PathBuf::from("data");

        // Determine the producer and add to path
        match self.producer {
            ProducerKind::Binance => result.push("binance"),
            ProducerKind::Test => result.push("test"),
            ProducerKind::Ninja => result.push("ninja"),
        };

        // Determine the market and add to path
        match self.market {
            MarketKind::BtcUsdt => result.push("btcusdt"),
            MarketKind::EurUsd => result.push("6e"),
            MarketKind::AudUsd => result.push("6a"),
            MarketKind::GbpUsd => result.push("6b"),
            MarketKind::CadUsd => result.push("6c"),
            MarketKind::YenUsd => result.push("6j"),
            MarketKind::NzdUsd => result.push("6n"),
            MarketKind::BtcUsdFuture => result.push("6btc"),

        };

        // Add year to path
        result.push(self.year.to_string());

        // TODO Hier strum verwenden

        // Determine the leaf and add to path
        match leaf {
            LeafDir::AggTrades => result.push("aggTrades"),
            LeafDir::Ohlc1m => result.push("ohlc-1m"),
            LeafDir::Ohlc30m => result.push("ohlc-30m"),
            LeafDir::Ohlc60m => result.push("ohlc-1h"),
            LeafDir::Ohlcv1m => result.push("ohlcv-1m"),
            LeafDir::Ohlcv30m => result.push("ohlcv-30m"),
            LeafDir::Ohlcv60m => result.push("ohlcv-1h"),
            LeafDir::Vol => panic!("no volume directory in trust-data/data"),
            LeafDir::ProfitAndLoss => panic!("no pl directory in trust-data/data"),
            LeafDir::Tick => panic!("Tick directory not yet supported in trust-data/data"),
        };

        // If we want to get files from the `cw` subdirectory, than add it to the path
        if cw {
            result.push("cw");
        }

        // Return result
        result
    }

    /// The path `{bucket}/strategy/{bot}/{market}/{year}/{granularity}/{leaf}` for a given leaf is constructed
    fn find_in_strategy(&self, leaf: &LeafDir) -> PathBuf {
        // Root directory is strategy
        let mut result = PathBuf::from("strategy");

        // Determine the bot and add to path
        match self.bot {
            BotKind::Ppp => result.push("ppp"),
            BotKind::Magneto => result.push("magneto"),
        };

        // Determine the market and add to path
        match self.market {
            MarketKind::BtcUsdt => result.push("btcusdt"),
            MarketKind::EurUsd => result.push("6e"),
            MarketKind::AudUsd => result.push("6a"),
            MarketKind::GbpUsd => result.push("6b"),
            MarketKind::CadUsd => result.push("6c"),
            MarketKind::YenUsd => result.push("6j"),
            MarketKind::NzdUsd => result.push("6n"),
            MarketKind::BtcUsdFuture => result.push("6btc"),
        };

        // Add year to path
        result.push(self.year.to_string());

        // Determine granularity and add to path
        match self.granularity {
            GranularityKind::Weekly => result.push("cw"),
            GranularityKind::Daily => result.push("day"),
        };

        // Determine the leaf directory and add to path
        match leaf {
            LeafDir::AggTrades => result.push("aggTrades"),
            LeafDir::Ohlc1m => result.push("ohlc-1m"),
            LeafDir::Ohlc30m => result.push("ohlc-30m"),
            LeafDir::Ohlc60m => result.push("ohlc-1h"),
            LeafDir::Ohlcv1m => result.push("ohlcv-1m"),
            LeafDir::Ohlcv30m => result.push("ohlcv-30m"),
            LeafDir::Ohlcv60m => result.push("ohlcv-1h"),
            LeafDir::Vol => result.push("vol"),
            LeafDir::ProfitAndLoss => result.push("pl"),
            LeafDir::Tick => panic!("Tick directory not yet supported in trust-data/data"),
        };

        // Return result
        result
    }

    #[allow(dead_code)]
    /// This function returns the directory inside `data` containing the target files for unit tests. `{bucket}/data/{bot}/{market}/{year}/{granularity}/{leaf}/_cw`
    /// This is function is only used in unit tests.
    fn find_target_in_data(&self, directory: &LeafDir) -> PathBuf {
        // Get base `{bucket}/data/{bot}/{market}/{year}/{granularity}/{leaf}`
        let mut result = self.find_in_data(directory, false);

        // Add target directory to result
        result.push("_cw");

        // Return result
        result
    }

    #[allow(dead_code)]
    /// This function returns the directory inside `strategy` containing the target files for unit tests. `{bucket}/strategy/{bot}/{market}/{year}/{granularity}/_{leaf}`
    /// /// This is function is only used in unit tests.
    fn find_target_in_strategy(&self, directory: &LeafDir) -> PathBuf {
        // Get base `{bucket}/strategy/{bot}/{market}/{year}/{granularity}/{leaf}`
        let mut result = self.find_in_strategy(directory);

        // Remove `{leaf}`, the result is `{bucket}/strategy/{bot}/{market}/{year}/{granularity}`
        result.pop();

        // Append target directory, the result is `_ + {leaf}`: `{bucket}/strategy/{bot}/{market}/{year}/{granularity}/_{leaf}`
        match directory {
            LeafDir::AggTrades => result.push("_aggTrades"),
            LeafDir::Ohlc1m => result.push("_ohlc-1m"),
            LeafDir::Ohlc30m => result.push("_ohlc-30m"),
            LeafDir::Ohlc60m => result.push("_ohlc-1h"),
            LeafDir::Ohlcv1m => result.push("_ohlcv-1m"),
            LeafDir::Ohlcv30m => result.push("_ohlcv-30m"),
            LeafDir::Ohlcv60m => result.push("_ohlcv-1h"),
            LeafDir::ProfitAndLoss => result.push("_pl"),
            LeafDir::Vol => result.push("_vol"),
            LeafDir::Tick => panic!("Tick directory not yet supported in trust-data/data"),
        };

        // Return result
        result
    }

    // /// Deletes all files in the given `leaf` directory.
    // /// `{bucket}/data/{bot}/{market}/{year}/{granularity}/{leaf}/cw`
    // async fn delete_files_in_data(&self, client: Arc<Client>, leaf: &LeafDir) {
    //     delete_files(client, &self.find_in_data(leaf, true)).await;
    // }

    // /// Deletes all files in the given `leaf` directory.
    // /// `{bucket}/strategy/{bot}/{market}/{year}/{granularity}/{leaf}`
    // async fn delete_files_in_strategy(&self, client: Arc<Client>, leaf: &LeafDir) {
    //     delete_files(client, &self.find_in_strategy(leaf)).await;
    // }
}

/// Unit tests to check if the paths are configured correctly. We only cover a small
/// subset of tests. More tests can be added.
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_find_in_data() {
        let bot = BotKind::Ppp;
        let market = MarketKind::BtcUsdt;
        let year = 2022;
        let granularity = GranularityKind::Weekly;
        let finder = super::Finder::new(
            PathBuf::from("trust-data"),
            ProducerKind::Test,
            market,
            year,
            bot,
            granularity,
        ).await;

        assert_eq!(
            PathBuf::from("data/test/btcusdt/2022/aggTrades"),
            finder.find_in_data(&LeafDir::AggTrades, false)
        );
        assert_eq!(
            PathBuf::from("data/test/btcusdt/2022/aggTrades/cw"),
            finder.find_in_data(&LeafDir::AggTrades, true)
        );
    }

    #[tokio::test]
    async fn test_find_in_strategy() {
        let bot = BotKind::Ppp;
        let market = MarketKind::BtcUsdt;
        let year = 2022;
        let granularity = GranularityKind::Weekly;
        let finder = super::Finder::new(
            PathBuf::from("trust-data"),
            ProducerKind::Test,
            market,
            year,
            bot,
            granularity,
        ).await;

        assert_eq!(
            PathBuf::from("strategy/ppp/btcusdt/2022/cw/aggTrades"),
            finder.find_in_strategy(&LeafDir::AggTrades)
        );
    }

    #[tokio::test]
    async fn test_find_target_in_data() {
        let bot = BotKind::Ppp;
        let market = MarketKind::BtcUsdt;
        let year = 2022;
        let granularity = GranularityKind::Weekly;
        let finder = super::Finder::new(
            PathBuf::from("trust-data"),
            ProducerKind::Test,
            market,
            year,
            bot,
            granularity,
        ).await;

        assert_eq!(
            PathBuf::from("data/test/btcusdt/2022/aggTrades/_cw"),
            finder.find_target_in_data(&LeafDir::AggTrades)
        );
    }

    #[tokio::test]
    async fn test_find_target_in_strategy() {
        let bot = BotKind::Ppp;
        let market = MarketKind::BtcUsdt;
        let year = 2022;
        let granularity = GranularityKind::Weekly;
        let finder = super::Finder::new(
            PathBuf::from("trust-data"),
            ProducerKind::Test,
            market,
            year,
            bot,
            granularity,
        ).await;

        assert_eq!(
            PathBuf::from("strategy/ppp/btcusdt/2022/cw/_aggTrades"),
            finder.find_target_in_strategy(&LeafDir::AggTrades)
        );
    }

    #[tokio::test]
    async fn test_files_in_directory() {
        let client = Client::default();
        // let dir = PathBuf::from("data/test/btcusdt/2022/aggTrades");
        let finder = Finder {
            bucket: PathBuf::from("trust-data"),
            producer: ProducerKind::Test,
            market: MarketKind::BtcUsdt,
            year: 2022,
            bot: BotKind::Ppp,
            granularity: GranularityKind::Daily,
            client: Arc::new(config::get_google_cloud_client().await)
        };
        let result = finder
            .list_files(
                Arc::new(client),
                RootDir::Data,
                LeafDir::AggTrades,
                Some(false),
            )
            .await
            .unwrap();

        // Check if result vector contains target files
        let target = vec![
            PathBuf::from("data/test/btcusdt/2022/aggTrades/BTCUSDT-aggTrades-2022-02.csv"),
            PathBuf::from("data/test/btcusdt/2022/aggTrades/BTCUSDT-aggTrades-2022-03.csv"),
            // PathBuf::from("strategy/ppp/6e/2022/day/vol/35_5.csv")
        ];
        assert_eq!(result.iter().any(|e| e == &target[0]), true);
        assert_eq!(result.iter().any(|e| e == &target[1]), true);
    }

    #[tokio::test]
    async fn _test_files_in_directory() {
        let client = Client::default();
        // let dir = PathBuf::from("strategy/ppp/6e/2022/day/vol");

        let finder = Finder {
            bucket: PathBuf::from("trust-data"),
            producer: ProducerKind::Test,
            market: MarketKind::EurUsd,
            year: 2022,
            bot: BotKind::Ppp,
            granularity: GranularityKind::Daily,
            client: Arc::new(config::get_google_cloud_client().await)
        };
        let result = finder
            .list_files(Arc::new(client), RootDir::Strategy, LeafDir::Vol, None)
            .await
            .unwrap();

        let target = vec![
            PathBuf::from("strategy/ppp/6e/2022/day/vol/354.csv"),
            PathBuf::from("strategy/ppp/6e/2022/day/vol/355.csv"),
        ];
        assert_eq!(result.iter().any(|e| e == &target[0]), true);
        assert_eq!(result.iter().any(|e| e == &target[1]), true);
    }
}
