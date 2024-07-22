use super::{
    backtesting_batch_data::BacktestingBatchData,
    execution_data::ExecutionData,
    indicator_data_pair::IndicatorDataPair,
    pre_trade_data::{PreTradeData, PreTradeDataBuilder},
    time_frame_snapshot::{TimeFrameSnapshot, TimeFrameSnapshotBuilder},
    Bot,
};
use crate::{
    calculator::pnl_report_data_row_calculator::{
        PnLReportDataRow, PnLReportDataRowCalculatorBuilder,
    },
    chapaty,
    cloud_api::{
        cloud_storage_wrapper::CloudStorageClientBuilder,
        file_name_resolver::FileNameResolver,
        path_finder::{PathFinder, PathFinderBuilder},
    },
    enums::{
        bot::TimeFrameKind, error::ChapatyErrorKind, indicator::TradingIndicatorKind,
        markets::MarketKind,
    },
    MarketSimulationDataKind,
};
use chrono::NaiveDate;
use polars::prelude::DataFrame;
use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::task::JoinHandle;

#[derive(Clone)]
pub struct TradingSession {
    pub bot: Arc<Bot>,
    pub indicator_data_pair: Arc<HashSet<IndicatorDataPair>>,
    pub market: MarketKind,
    pub year: u32,
    pub data: ExecutionData,
    pub market_sim_data_kind: MarketSimulationDataKind,
    pub cache_computations: bool,
}

impl TradingSession {
    pub async fn compute_pnl_report(self) -> DataFrame {
        let (tx, rx) = tokio::sync::oneshot::channel();
        rayon::spawn(move || {
            let _ = tx.send(self.run_backtesting());
        });
        rx.await.unwrap()
    }

    fn run_backtesting(&self) -> DataFrame {
        match self.bot.time_frame {
            TimeFrameKind::Daily => self.run_backtesting_daily(),
            TimeFrameKind::Weekly => self.run_backtesting_weekly(),
        }
    }

    fn run_backtesting_daily(&self) -> DataFrame {
        // TODO Segfault can happen when you have an empty DF
        let pnl_report_data_rows: Vec<_> = (1..=52_i64)
            .into_par_iter()
            .flat_map(|cw| (1..=7).into_par_iter().map(move |wd| (cw, wd)))
            .map(|(cw, wd)| build_time_frame_snapshot(cw, Some(wd), None, None))
            .filter(|snapshot| self.is_backtest_on_news(snapshot))
            .filter_map(|snapshot| self.get_daily_backtesting_batch_data(snapshot).ok())
            .map(|batch| self.compute_pnl_data_row(batch))
            .collect();

        pnl_report_data_rows.into_iter().collect()
    }

    /// Determines if the time frame snapshot should be dropped. A strategy is evaluated
    /// on a set of news dates, or on the complement of that set of news dates.
    ///
    /// # Returns
    /// - `true`
    ///     - if the snapshot is on a news event and the bot is only trading on news
    ///     - if the snapshot is **not** on a news event and the bot is **not** only trading on news.
    /// - `false` in all other cases
    fn is_backtest_on_news(&self, snapshot: &TimeFrameSnapshot) -> bool {
        let year = i32::try_from(self.year).unwrap();
        let week = u32::try_from(snapshot.get_calendar_week_as_int()).unwrap();
        let snapshot_date = NaiveDate::from_isoywd_opt(year, week, snapshot.get_weekday());
        let strategy = &self.bot.strategy;
        let is_snapshot_on_news =
            snapshot_date.map_or(false, |date| strategy.get_news().contains(&date));

        if strategy.is_only_trading_on_news() {
            is_snapshot_on_news
        } else {
            !is_snapshot_on_news
        }
    }

    fn run_backtesting_weekly(&self) -> DataFrame {
        panic!("Not implemented weekly backtesting yet...")
    }

    fn get_daily_backtesting_batch_data(
        &self,
        snapshot: TimeFrameSnapshot,
    ) -> Result<BacktestingBatchData, ChapatyErrorKind> {
        Ok(BacktestingBatchData {
            time_frame_snapshot: snapshot,
            market_sim_data: self.get_market_sim_data_data(&snapshot)?,
            pre_trade_data: self.get_pre_trade_data(&snapshot)?,
        })
    }

    fn get_pre_trade_data(
        &self,
        snapshot: &TimeFrameSnapshot,
    ) -> Result<PreTradeData, ChapatyErrorKind> {
        let builder = PreTradeDataBuilder::new();
        if self.bot.strategy.is_pre_trade_day_equal_to_trade_day() {
            Ok(builder
                .with_market_sim_data(self.get_market_sim_data_data(snapshot)?)
                .with_indicators(self.get_trading_indicator(snapshot)?)
                .build())
        } else {
            if is_on_monday(snapshot) {
                let last_friday = snapshot.last_friday();
                Ok(builder
                    .with_market_sim_data(self.get_market_sim_data_data(&last_friday)?)
                    .with_indicators(self.get_trading_indicator(&last_friday)?)
                    .build())
            } else {
                let yesterday = snapshot.shift_back_by_n_weekdays(1);
                Ok(builder
                    .with_market_sim_data(self.get_market_sim_data_data(&yesterday)?)
                    .with_indicators(self.get_trading_indicator(&yesterday)?)
                    .build())
            }
        }
    }

    fn get_market_sim_data_data(
        &self,
        snapshot: &TimeFrameSnapshot,
    ) -> Result<DataFrame, ChapatyErrorKind> {
        Ok(self
            .data
            .market_sim_data
            .get(snapshot)
            .ok_or_else(|| {
                ChapatyErrorKind::FailedToFetchDataFrameFromMap(format!(
                    "DataFrame for <{snapshot:?}> is not available in map"
                ))
            })?
            .clone())
    }

    fn get_trading_indicator(
        &self,
        time_frame_snapshot: &TimeFrameSnapshot,
    ) -> Result<HashMap<TradingIndicatorKind, DataFrame>, ChapatyErrorKind> {
        self.data
            .trading_indicators
            .iter()
            .map(|(indicator, data_frame_map)| {
                (
                    indicator,
                    data_frame_map.get(time_frame_snapshot).map(|df| df.clone()),
                )
            })
            .map(|(indicator, df)| (indicator, df_to_result(df)))
            .map(|(key, value)| value.map(|dataframe| (*key, dataframe)))
            .collect()
    }

    fn compute_pnl_data_row(&self, batch: BacktestingBatchData) -> PnLReportDataRow {
        PnLReportDataRowCalculatorBuilder::new()
            .with_market_sim_data(batch.market_sim_data)
            .with_strategy(self.bot.strategy.clone())
            .with_pre_trade_data(batch.pre_trade_data)
            .with_year(self.year)
            .with_market(self.market)
            .with_time_frame_snapshot(batch.time_frame_snapshot)
            .with_market_sim_data_kind(self.market_sim_data_kind)
            .build_and_compute()
    }
}

fn build_time_frame_snapshot(
    cw: i64,
    wd: Option<i64>,
    h: Option<i64>,
    m: Option<i64>,
) -> TimeFrameSnapshot {
    let mut builder = TimeFrameSnapshotBuilder::new(cw);
    builder = wd.map_or_else(|| builder.clone(), |weekday| builder.with_weekday(weekday));
    builder = h.map_or_else(|| builder.clone(), |hour| builder.with_hour(hour));
    builder = m.map_or_else(|| builder.clone(), |minute| builder.with_minute(minute));

    builder.build()
}

fn is_on_monday(snapshot: &TimeFrameSnapshot) -> bool {
    snapshot.get_weekday_as_int() == 1
}

fn df_to_result(df: Option<DataFrame>) -> Result<DataFrame, ChapatyErrorKind> {
    df.ok_or_else(|| {
        ChapatyErrorKind::FailedToFetchDataFrameFromMap(
            "Missing DataFrame for trading indicator".to_string(),
        )
    })
}

#[derive(Clone)]
pub struct TradingSessionBuilder {
    bot: Option<Arc<Bot>>,
    indicator_data_pair: Option<Arc<HashSet<IndicatorDataPair>>>,
    market: Option<MarketKind>,
    year: Option<u32>,
    market_sim_data_kind: Option<MarketSimulationDataKind>,
    cache_computations: bool,
}

impl TradingSessionBuilder {
    pub fn new() -> Self {
        Self {
            bot: None,
            indicator_data_pair: None,
            market: None,
            year: None,
            market_sim_data_kind: None,
            cache_computations: false,
        }
    }

    pub fn with_bot(self, bot: Arc<Bot>) -> Self {
        Self {
            bot: Some(bot),
            ..self
        }
    }

    pub fn with_indicator_data_pair(self, data: Arc<HashSet<IndicatorDataPair>>) -> Self {
        Self {
            indicator_data_pair: Some(data),
            ..self
        }
    }

    pub fn with_market(self, market: MarketKind) -> Self {
        Self {
            market: Some(market),
            ..self
        }
    }

    pub fn with_year(self, year: u32) -> Self {
        Self {
            year: Some(year),
            ..self
        }
    }

    pub fn with_market_sim_data_kind(self, market_sim_data_kind: MarketSimulationDataKind) -> Self {
        Self {
            market_sim_data_kind: Some(market_sim_data_kind),
            ..self
        }
    }

    pub fn with_cache_computations(self, cache_computations: bool) -> Self {
        Self {
            cache_computations,
            ..self
        }
    }

    pub async fn build(self) -> TradingSession {
        let data = self.populate_trading_session_data().await;
        TradingSession {
            bot: self.bot.unwrap(),
            indicator_data_pair: self.indicator_data_pair.unwrap(),
            market: self.market.unwrap(),
            year: self.year.unwrap(),
            market_sim_data_kind: self.market_sim_data_kind.unwrap(),
            data,
            cache_computations: self.cache_computations,
        }
    }

    async fn populate_trading_session_data(&self) -> ExecutionData {
        let bot = self.bot.clone().unwrap();
        let market = self.market.unwrap();
        let year = self.year.unwrap();

        let path_finder = PathFinderBuilder::new()
            .with_data_provider(bot.data_provider.get_name())
            .with_strategy_name(bot.strategy.get_name())
            .with_market(market)
            .with_year(year)
            .with_time_interval(bot.time_interval)
            .with_time_frame(bot.time_frame.to_string())
            .build();

        let trading_indicators_df_map = self.get_trading_indicators_df_map(&path_finder).await;
        let market_simulation_df_map = self.get_market_simulation_df_map(&path_finder).await;

        ExecutionData {
            market_sim_data: market_simulation_df_map,
            trading_indicators: trading_indicators_df_map,
        }
    }

    async fn get_trading_indicators_df_map(
        &self,
        path_finder: &PathFinder,
    ) -> HashMap<TradingIndicatorKind, chapaty::types::DataFrameMap> {
        let tasks: Vec<_> = self
            .indicator_data_pair
            .clone()
            .unwrap()
            .iter()
            .map(|indicator_data_pair| self.fetch_df_map(path_finder, indicator_data_pair.clone()))
            .collect();

        futures::future::join_all(tasks)
            .await
            .into_iter()
            .map(Result::unwrap)
            .fold(HashMap::new(), |mut trading_indicators_df_map, val| {
                let indicator = val.0;
                let df_map = val.1;
                trading_indicators_df_map.insert(indicator, df_map);

                trading_indicators_df_map
            })
    }

    fn fetch_df_map(
        &self,
        path_finder: &PathFinder,
        indicator_data_pair: IndicatorDataPair,
    ) -> JoinHandle<(TradingIndicatorKind, HashMap<TimeFrameSnapshot, DataFrame>)> {
        let bot = self.bot.clone().unwrap();
        let file_name = FileNameResolver::new(bot.market_simulation_data.into())
            .with_indicator_data_pair(indicator_data_pair.clone())
            .get_filename();

        let file_path_with_fallback =
            path_finder.get_file_path_with_fallback(file_name, &indicator_data_pair.data);

        let cloud_storage_client = self
            .initalize_cloud_storage_client_builder()
            .with_file_path_with_fallback(file_path_with_fallback)
            .with_indicator_data_pair(Some(indicator_data_pair.clone()))
            .build();

        tokio::spawn(async move {
            let indicator = indicator_data_pair.indicator;
            let df_map = cloud_storage_client.download_df_map().await;
            (indicator, df_map)
        })
    }

    async fn get_market_simulation_df_map(
        &self,
        path_finder: &PathFinder,
    ) -> chapaty::types::DataFrameMap {
        let bot = self.bot.clone().unwrap();

        let market_sim_data = bot.market_simulation_data.into();
        let file_name_resolver = FileNameResolver::new(market_sim_data);

        let file_name = file_name_resolver.get_filename();
        let file_path_with_fallback =
            path_finder.get_file_path_with_fallback(file_name, &market_sim_data);

        let cloud_storage_client = self
            .initalize_cloud_storage_client_builder()
            .with_file_path_with_fallback(file_path_with_fallback)
            .build();

        cloud_storage_client.download_df_map().await
    }

    fn initalize_cloud_storage_client_builder(&self) -> CloudStorageClientBuilder {
        let bot = self.bot.clone().unwrap();
        CloudStorageClientBuilder::new(bot.clone())
            .with_simulation_data(bot.market_simulation_data.into())
            .with_market(self.market.unwrap())
            .with_year(self.year.unwrap())
            .with_cache_computations(self.cache_computations)
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use crate::{
        cloud_api::api_for_unit_tests::{download_df, download_df_map},
        config,
        data_provider::{binance::Binance, MockDataProvider},
        enums::indicator::PriceHistogramKind,
        strategy::{MockStrategy, RequriedPreTradeValues},
        BotBuilder, MarketSimulationDataKind, NewsKind, TimeInterval,
    };
    use std::sync::Arc;

    #[tokio::test]
    async fn test_populate_daily_trading_session_data() {
        // Test Setup
        let mut mock_strategy = MockStrategy::new();
        let trading_indicators = vec![TradingIndicatorKind::Poc(PriceHistogramKind::VolAggTrades)];
        let required_pre_trade_values = RequriedPreTradeValues {
            market_values: Vec::new(),
            trading_indicators,
        };
        mock_strategy
            .expect_get_required_pre_trade_values()
            .return_const(required_pre_trade_values.clone());
        mock_strategy.expect_get_name().return_const("ppp");
        let data_provider = Arc::new(Binance);
        let cloud_storage_client = config::get_google_cloud_storage_client().await;
        let bucket = config::GoogleCloudBucket {
            historical_market_data_bucket_name: "chapaty-ai-hdb-test".to_string(),
            cached_bot_data_bucket_name: "chapaty-ai-test".to_string(),
        };
        let time_interval = TimeInterval {
            start_day: chrono::Weekday::Mon,
            start_h: 1,
            end_day: chrono::Weekday::Fri,
            end_h: 23,
        };

        // Test Initialization
        let bot = BotBuilder::new(Arc::new(mock_strategy), data_provider)
            .with_google_cloud_storage_client(cloud_storage_client)
            .with_google_cloud_bucket(bucket)
            .with_time_interval(time_interval)
            .with_market_simulation_data(MarketSimulationDataKind::Ohlcv1h)
            .build()
            .unwrap();

        let session = TradingSessionBuilder::new()
            .with_bot(Arc::new(bot.clone()))
            .with_indicator_data_pair(bot.determine_indicator_data_pair())
            .with_market(MarketKind::BtcUsdt)
            .with_year(2022);

        // Test Evaluation
        let execution_data = session.populate_trading_session_data().await;
        let base_path = "ppp/btcusdt/2022/Mon1h0m-Fri23h0m/1d/target_ohlcv-1h_dataframes";

        // Test Evaluation "market_sim_data"
        let market_sim_data = execution_data.market_sim_data;
        let mut cw = 8;
        let mut wd = 1;
        let snapshot = build_time_frame_snapshot(cw, Some(wd), None, None);
        let target = download_df(
            "chapaty-ai-test".to_string(),
            format!("{base_path}/{cw}_{wd}.csv"),
        )
        .await;
        assert_eq!(&target, market_sim_data.get(&snapshot).unwrap());

        cw = 8;
        wd = 1;
        let snapshot = build_time_frame_snapshot(cw, Some(wd), None, None);
        let target = download_df(
            "chapaty-ai-test".to_string(),
            format!("{base_path}/{cw}_{wd}.csv"),
        )
        .await;
        assert_eq!(&target, market_sim_data.get(&snapshot).unwrap());
        cw = 8;
        wd = 2;
        let snapshot = build_time_frame_snapshot(cw, Some(wd), None, None);
        let target = download_df(
            "chapaty-ai-test".to_string(),
            format!("{base_path}/{cw}_{wd}.csv"),
        )
        .await;
        assert_eq!(&target, market_sim_data.get(&snapshot).unwrap());
        cw = 8;
        wd = 3;
        let snapshot = build_time_frame_snapshot(cw, Some(wd), None, None);
        let target = download_df(
            "chapaty-ai-test".to_string(),
            format!("{base_path}/{cw}_{wd}.csv"),
        )
        .await;
        assert_eq!(&target, market_sim_data.get(&snapshot).unwrap());
        cw = 8;
        wd = 4;
        let snapshot = build_time_frame_snapshot(cw, Some(wd), None, None);
        let target = download_df(
            "chapaty-ai-test".to_string(),
            format!("{base_path}/{cw}_{wd}.csv"),
        )
        .await;
        assert_eq!(&target, market_sim_data.get(&snapshot).unwrap());
        cw = 8;
        wd = 5;
        let snapshot = build_time_frame_snapshot(cw, Some(wd), None, None);
        let target = download_df(
            "chapaty-ai-test".to_string(),
            format!("{base_path}/{cw}_{wd}.csv"),
        )
        .await;
        assert_eq!(&target, market_sim_data.get(&snapshot).unwrap());
        cw = 9;
        wd = 1;
        let snapshot = build_time_frame_snapshot(cw, Some(wd), None, None);
        let target = download_df(
            "chapaty-ai-test".to_string(),
            format!("{base_path}/{cw}_{wd}.csv"),
        )
        .await;
        assert_eq!(&target, market_sim_data.get(&snapshot).unwrap());
        cw = 9;
        wd = 2;
        let snapshot = build_time_frame_snapshot(cw, Some(wd), None, None);
        let target = download_df(
            "chapaty-ai-test".to_string(),
            format!("{base_path}/{cw}_{wd}.csv"),
        )
        .await;
        assert_eq!(&target, market_sim_data.get(&snapshot).unwrap());
        cw = 9;
        wd = 3;
        let snapshot = build_time_frame_snapshot(cw, Some(wd), None, None);
        let target = download_df(
            "chapaty-ai-test".to_string(),
            format!("{base_path}/{cw}_{wd}.csv"),
        )
        .await;
        assert_eq!(&target, market_sim_data.get(&snapshot).unwrap());
        cw = 9;
        wd = 4;
        let snapshot = build_time_frame_snapshot(cw, Some(wd), None, None);
        let target = download_df(
            "chapaty-ai-test".to_string(),
            format!("{base_path}/{cw}_{wd}.csv"),
        )
        .await;
        assert_eq!(&target, market_sim_data.get(&snapshot).unwrap());
        cw = 9;
        wd = 5;
        let snapshot = build_time_frame_snapshot(cw, Some(wd), None, None);
        let target = download_df(
            "chapaty-ai-test".to_string(),
            format!("{base_path}/{cw}_{wd}.csv"),
        )
        .await;
        assert_eq!(&target, market_sim_data.get(&snapshot).unwrap());
        cw = 10;
        wd = 1;
        let snapshot = build_time_frame_snapshot(cw, Some(wd), None, None);
        let target = download_df(
            "chapaty-ai-test".to_string(),
            format!("{base_path}/{cw}_{wd}.csv"),
        )
        .await;
        assert_eq!(&target, market_sim_data.get(&snapshot).unwrap());
        cw = 10;
        wd = 2;
        let snapshot = build_time_frame_snapshot(cw, Some(wd), None, None);
        let target = download_df(
            "chapaty-ai-test".to_string(),
            format!("{base_path}/{cw}_{wd}.csv"),
        )
        .await;
        assert_eq!(&target, market_sim_data.get(&snapshot).unwrap());
        cw = 10;
        wd = 3;
        let snapshot = build_time_frame_snapshot(cw, Some(wd), None, None);
        let target = download_df(
            "chapaty-ai-test".to_string(),
            format!("{base_path}/{cw}_{wd}.csv"),
        )
        .await;
        assert_eq!(&target, market_sim_data.get(&snapshot).unwrap());
        cw = 10;
        wd = 4;
        let snapshot = build_time_frame_snapshot(cw, Some(wd), None, None);
        let target = download_df(
            "chapaty-ai-test".to_string(),
            format!("{base_path}/{cw}_{wd}.csv"),
        )
        .await;
        assert_eq!(&target, market_sim_data.get(&snapshot).unwrap());
        cw = 10;
        wd = 5;
        let snapshot = build_time_frame_snapshot(cw, Some(wd), None, None);
        let target = download_df(
            "chapaty-ai-test".to_string(),
            format!("{base_path}/{cw}_{wd}.csv"),
        )
        .await;
        assert_eq!(&target, market_sim_data.get(&snapshot).unwrap());

        // Test Evaluation "trading_indicators"
        let trading_indicators = execution_data.trading_indicators;
        let path = "ppp/btcusdt/2022/Mon1h0m-Fri23h0m/1d/target_vol-aggTrades.json";
        let target = download_df_map(path.to_string()).await;
        assert_eq!(
            &target,
            trading_indicators
                .get(&required_pre_trade_values.trading_indicators[0])
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_populate_weekly_trading_session_data() {
        // Test Setup
        let mut mock_strategy = MockStrategy::new();
        let trading_indicators = vec![TradingIndicatorKind::Poc(PriceHistogramKind::VolAggTrades)];
        let required_pre_trade_values = RequriedPreTradeValues {
            market_values: Vec::new(),
            trading_indicators,
        };
        mock_strategy
            .expect_get_required_pre_trade_values()
            .return_const(required_pre_trade_values.clone());
        mock_strategy.expect_get_name().return_const("ppp");
        let data_provider = Arc::new(Binance);
        let cloud_storage_client = config::get_google_cloud_storage_client().await;
        let bucket = config::GoogleCloudBucket {
            historical_market_data_bucket_name: "chapaty-ai-hdb-test".to_string(),
            cached_bot_data_bucket_name: "chapaty-ai-test".to_string(),
        };
        let time_interval = TimeInterval {
            start_day: chrono::Weekday::Mon,
            start_h: 1,
            end_day: chrono::Weekday::Fri,
            end_h: 23,
        };

        // Test Initialization
        let bot = BotBuilder::new(Arc::new(mock_strategy), data_provider)
            .with_google_cloud_storage_client(cloud_storage_client)
            .with_google_cloud_bucket(bucket)
            .with_time_interval(time_interval)
            .with_time_frame(TimeFrameKind::Weekly)
            .with_market_simulation_data(MarketSimulationDataKind::Ohlcv1h)
            .build()
            .unwrap();

        let session = TradingSessionBuilder::new()
            .with_bot(Arc::new(bot.clone()))
            .with_indicator_data_pair(bot.determine_indicator_data_pair())
            .with_market(MarketKind::BtcUsdt)
            .with_year(2022);

        // Test Evaluation
        let execution_data = session.populate_trading_session_data().await;
        let base_path = "ppp/btcusdt/2022/Mon1h0m-Fri23h0m/1w/target_ohlcv-1h_dataframes";

        // Test Evaluation "market_sim_data"
        let market_sim_data = execution_data.market_sim_data;
        let mut cw = 8;
        let snapshot = build_time_frame_snapshot(cw, None, None, None);
        let target = download_df(
            "chapaty-ai-test".to_string(),
            format!("{base_path}/{cw}.csv"),
        )
        .await;
        assert_eq!(&target, market_sim_data.get(&snapshot).unwrap());

        cw = 9;
        let snapshot = build_time_frame_snapshot(cw, None, None, None);
        let target = download_df(
            "chapaty-ai-test".to_string(),
            format!("{base_path}/{cw}.csv"),
        )
        .await;
        assert_eq!(&target, market_sim_data.get(&snapshot).unwrap());

        cw = 10;
        let snapshot = build_time_frame_snapshot(cw, None, None, None);
        let target = download_df(
            "chapaty-ai-test".to_string(),
            format!("{base_path}/{cw}.csv"),
        )
        .await;
        assert_eq!(&target, market_sim_data.get(&snapshot).unwrap());

        // Test Evaluation "trading_indicators"
        let trading_indicators = execution_data.trading_indicators;
        let path = "ppp/btcusdt/2022/Mon1h0m-Fri23h0m/1w/target_vol-aggTrades.json";
        let target = download_df_map(path.to_string()).await;
        assert_eq!(
            &target,
            trading_indicators
                .get(&required_pre_trade_values.trading_indicators[0])
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_is_backtest_on_news() {
        // Test Setup
        let mut mock_strategy = MockStrategy::new();
        mock_strategy
            .expect_is_only_trading_on_news()
            .return_const(true);
        mock_strategy
            .expect_get_news()
            .return_const(NewsKind::UsaNFP.get_news_dates());
        let data_provider = MockDataProvider::new();
        let cloud_storage_client = config::get_google_cloud_storage_client().await;
        let bucket = config::GoogleCloudBucket {
            historical_market_data_bucket_name: "chapaty-ai-hdb-test".to_string(),
            cached_bot_data_bucket_name: "chapaty-ai-test".to_string(),
        };

        // Test Initialization with news trading
        let bot = BotBuilder::new(Arc::new(mock_strategy), Arc::new(data_provider))
            .with_google_cloud_storage_client(cloud_storage_client.clone())
            .with_google_cloud_bucket(bucket.clone())
            .with_time_frame(TimeFrameKind::Weekly)
            .with_market_simulation_data(MarketSimulationDataKind::Ohlcv1h)
            .build()
            .unwrap();

        let session = TradingSession {
            bot: Arc::new(bot),
            indicator_data_pair: Arc::new(HashSet::new()),
            market: MarketKind::EurUsdFuture,
            year: 2022,
            data: ExecutionData::default(),
            market_sim_data_kind: MarketSimulationDataKind::Ohlc1m,
            cache_computations: false,
        };

        let snapshot = TimeFrameSnapshotBuilder::new(13).with_weekday(5).build();
        assert!(session.is_backtest_on_news(&snapshot));

        let snapshot = TimeFrameSnapshotBuilder::new(18).with_weekday(5).build();
        assert!(session.is_backtest_on_news(&snapshot));

        let snapshot = TimeFrameSnapshotBuilder::new(13).with_weekday(4).build();
        assert!(!session.is_backtest_on_news(&snapshot));

        let snapshot = TimeFrameSnapshotBuilder::new(18).with_weekday(4).build();
        assert!(!session.is_backtest_on_news(&snapshot));

        let snapshot = TimeFrameSnapshotBuilder::new(14);
        assert!(!session.is_backtest_on_news(&snapshot.with_weekday(1).build()));
        assert!(!session.is_backtest_on_news(&snapshot.with_weekday(2).build()));
        assert!(!session.is_backtest_on_news(&snapshot.with_weekday(3).build()));
        assert!(!session.is_backtest_on_news(&snapshot.with_weekday(4).build()));
        assert!(!session.is_backtest_on_news(&snapshot.with_weekday(5).build()));

        // Test Initialization no news trading
        let mut mock_strategy = MockStrategy::new();
        mock_strategy
            .expect_is_only_trading_on_news()
            .return_const(false);
        mock_strategy
            .expect_get_news()
            .return_const(NewsKind::UsaNFP.get_news_dates());
        let data_provider = MockDataProvider::new();

        let bot = BotBuilder::new(Arc::new(mock_strategy), Arc::new(data_provider))
            .with_google_cloud_storage_client(cloud_storage_client)
            .with_google_cloud_bucket(bucket)
            .with_time_frame(TimeFrameKind::Weekly)
            .with_market_simulation_data(MarketSimulationDataKind::Ohlcv1h)
            .build()
            .unwrap();

        let session = TradingSession {
            bot: Arc::new(bot),
            indicator_data_pair: Arc::new(HashSet::new()),
            market: MarketKind::EurUsdFuture,
            year: 2022,
            data: ExecutionData::default(),
            market_sim_data_kind: MarketSimulationDataKind::Ohlc1m,
            cache_computations: false,
        };

        let snapshot = TimeFrameSnapshotBuilder::new(13).with_weekday(5).build();
        assert!(!session.is_backtest_on_news(&snapshot));

        let snapshot = TimeFrameSnapshotBuilder::new(18).with_weekday(5).build();
        assert!(!session.is_backtest_on_news(&snapshot));

        let snapshot = TimeFrameSnapshotBuilder::new(13).with_weekday(4).build();
        assert!(session.is_backtest_on_news(&snapshot));

        let snapshot = TimeFrameSnapshotBuilder::new(18).with_weekday(4).build();
        assert!(session.is_backtest_on_news(&snapshot));

        let snapshot = TimeFrameSnapshotBuilder::new(14);
        assert!(session.is_backtest_on_news(&snapshot.with_weekday(1).build()));
        assert!(session.is_backtest_on_news(&snapshot.with_weekday(2).build()));
        assert!(session.is_backtest_on_news(&snapshot.with_weekday(3).build()));
        assert!(session.is_backtest_on_news(&snapshot.with_weekday(4).build()));
        assert!(session.is_backtest_on_news(&snapshot.with_weekday(5).build()));
    }
}
