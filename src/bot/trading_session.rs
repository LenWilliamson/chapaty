use super::{
    backtesting_batch_data::BacktestingBatchData,
    execution_data::ExecutionData,
    indicator_data_pair::IndicatorDataPair,
    pre_trade_data::{PreTradeData, PreTradeDataBuilder},
    time_frame_snapshot::{TimeFrameSnapshot, TimeFrameSnapshotBuilder},
    Bot,
};
use crate::{
    calculator::pnl_report_data_row_calculator::PnLReportDataRowCalculatorBuilder,
    chapaty,
    cloud_api::{
        cloud_storage_wrapper::CloudStorageClientBuilder,
        file_name_resolver::FileNameResolver,
        path_finder::{PathFinder, PathFinderBuilder},
    },
    enums::{
        bot::TimeFrameKind, error::ChapatyErrorKind, indicator::TradingIndicatorKind,
        markets::MarketKind, strategy::StrategyKind,
    },
    lazy_frame_operations::trait_extensions::MyLazyFrameVecOperations,
    strategy::Strategy,
    MarketSimulationDataKind, NewsKind, PnLReportColumnKind,
};
use chrono::NaiveDate;
use polars::prelude::{DataFrame, LazyFrame};
use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};
use strum::IntoEnumIterator;
use tokio::task::JoinHandle;

#[derive(Clone)]
pub struct TradingSession {
    pub bot: Arc<Bot>,
    pub indicator_data_pair: Arc<HashMap<StrategyKind, HashSet<IndicatorDataPair>>>,
    pub market: MarketKind,
    pub year: u32,
    pub data: HashMap<StrategyKind, ExecutionData>,
    pub market_sim_data_kind: HashMap<StrategyKind, MarketSimulationDataKind>,
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
            // TODO implement, as some news arrive end of day
            TimeFrameKind::Weekly => self.run_backtesting_weekly(),
        }
    }

    fn run_backtesting_daily(&self) -> DataFrame {
        // TODO Segfault can happen when you have an empty DF
        let pnl_report_data_rows: Vec<_> = (1..=52_i64)
            .into_par_iter()
            .flat_map(|cw| (1..=7).into_par_iter().map(move |wd| (cw, wd)))
            .map(|(cw, wd)| build_time_frame_snapshot(cw, Some(wd), None, None))
            // TODO The strategy can actually to the job decide
            .filter(|snapshot| self.is_backtest_on_news(snapshot))
            .filter_map(|snapshot| self.get_daily_backtesting_batch_data(snapshot).ok())
            .flat_map(|batch| self.compute_pnl_data_row(batch).into_par_iter())
            .collect();
        pnl_report_data_rows
            .concatenate_to_data_frame()
            .with_row_index(PnLReportColumnKind::Id.to_string().into(), Some(1))
            .unwrap()
            .with_row_index(PnLReportColumnKind::Uid.to_string().into(), Some(1))
            .unwrap()
    }

    /// TODO Make it not too restrictive, you may want to trade EURUSD FX on Australien CPI ???
    ///
    /// Determines if the time frame snapshot should be dropped. A strategy is evaluated
    /// on a set of news dates, or on all dates that do not fall into any major economic
    /// news event.
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
        let bot_news_dates =
            self.bot
                .strategies
                .iter()
                .fold(HashSet::new(), |mut acc, strategy| {
                    let dates = strategy.filter_on_economic_news_event().map_or(
                        HashSet::new(),
                        |news_vec| {
                            news_vec.iter().fold(HashSet::new(), |mut acc, news_kind| {
                                acc.extend(news_kind.get_news_dates());
                                acc
                            })
                        },
                    );
                    acc.extend(dates);
                    acc
                });

        if self
            .bot
            .strategies
            .iter()
            .any(|s| s.filter_on_economic_news_event().is_some())
        {
            let is_snapshot_on_news =
                snapshot_date.map_or(false, |date| bot_news_dates.contains(&date));
            is_snapshot_on_news
        } else {
            let all_news = NewsKind::iter().fold(HashSet::new(), |mut acc, news_kind| {
                acc.extend(news_kind.get_news_dates());
                acc
            });

            !snapshot_date.map_or(false, |date| all_news.contains(&date))
        }
    }

    fn run_backtesting_weekly(&self) -> DataFrame {
        panic!("Not implemented weekly backtesting yet...")
    }

    fn get_daily_backtesting_batch_data(
        &self,
        snapshot: TimeFrameSnapshot,
    ) -> Result<HashMap<StrategyKind, BacktestingBatchData>, ChapatyErrorKind> {
        let mut res = HashMap::new();
        for strategy in self.bot.strategies.iter() {
            let strategy_kind = strategy.get_strategy_kind();
            let batch = BacktestingBatchData {
                time_frame_snapshot: snapshot,
                market_sim_data: self.get_market_sim_data_data(&snapshot, &strategy_kind)?,
                pre_trade_data: self.get_pre_trade_data(&snapshot, &strategy_kind)?,
            };
            res.insert(strategy_kind, batch);
        }
        Ok(res)
    }

    fn get_pre_trade_data(
        &self,
        snapshot: &TimeFrameSnapshot,
        strategy_kind: &StrategyKind,
    ) -> Result<PreTradeData, ChapatyErrorKind> {
        let builder = PreTradeDataBuilder::new();
        // TODO Fix, as we might not need to ask if pre_trade equals trade day due to new setup
        // if self.bot.strategies.iter().any(|s| s.is_pre_trade_day_equal_to_trade_day()) {
        //     Ok(builder.with_market_sim_data(self.get_market_sim_data_data(snapshot)?).with_indicators(self.get_trading_indicator(snapshot)?).build())
        // } else {}

        if is_on_monday(snapshot) {
            let last_friday = snapshot.last_friday();
            Ok(builder
                .with_market_sim_data(self.get_market_sim_data_data(&last_friday, strategy_kind)?)
                .with_indicators(self.get_trading_indicator(&last_friday, strategy_kind)?)
                .build())
        } else {
            let yesterday = snapshot.shift_back_by_n_weekdays(1);
            Ok(builder
                .with_market_sim_data(self.get_market_sim_data_data(&yesterday, strategy_kind)?)
                .with_indicators(self.get_trading_indicator(&yesterday, strategy_kind)?)
                .build())
        }
    }

    fn get_market_sim_data_data(
        &self,
        snapshot: &TimeFrameSnapshot,
        strategy_kind: &StrategyKind,
    ) -> Result<DataFrame, ChapatyErrorKind> {
        Ok(self
            .data
            .get(strategy_kind)
            .unwrap()
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
        strategy_kind: &StrategyKind,
    ) -> Result<HashMap<TradingIndicatorKind, DataFrame>, ChapatyErrorKind> {
        self.data
            .get(strategy_kind)
            .unwrap()
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

    fn compute_pnl_data_row(
        &self,
        batch: HashMap<StrategyKind, BacktestingBatchData>,
    ) -> Vec<LazyFrame> {
        PnLReportDataRowCalculatorBuilder::new()
            .with_backtesting_batch_data(batch)
            .with_strategy(self.bot.strategies.clone())
            .with_decision_policy(self.bot.decision_policy.clone())
            .with_year(self.year)
            .with_market(self.market)
            .with_market_sim_data_kind(self.market_sim_data_kind.clone())
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
    builder = wd.map_or(builder.clone(), |weekday| builder.with_weekday(weekday));
    builder = h.map_or(builder.clone(), |hour| builder.with_hour(hour));
    builder = m.map_or(builder.clone(), |minute| builder.with_minute(minute));

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
    indicator_data_pair: Option<Arc<HashMap<StrategyKind, HashSet<IndicatorDataPair>>>>,
    market: Option<MarketKind>,
    year: Option<u32>,
    market_sim_data_kind: Option<HashMap<StrategyKind, MarketSimulationDataKind>>,
    cache_computations: bool,
    session_cache:
        Option<Arc<Mutex<HashMap<MarketKind, HashMap<u32, HashMap<StrategyKind, ExecutionData>>>>>>,
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
            session_cache: None,
        }
    }

    pub fn with_bot(self, bot: Arc<Bot>) -> Self {
        Self {
            bot: Some(bot),
            ..self
        }
    }

    pub fn with_indicator_data_pair(
        self,
        data: Arc<HashMap<StrategyKind, HashSet<IndicatorDataPair>>>,
    ) -> Self {
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

    pub fn with_market_sim_data_kind(
        self,
        market_sim_data_kind: HashMap<StrategyKind, MarketSimulationDataKind>,
    ) -> Self {
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

    pub fn with_session_cache(
        self,
        session_cache: Option<
            Arc<Mutex<HashMap<MarketKind, HashMap<u32, HashMap<StrategyKind, ExecutionData>>>>>,
        >,
    ) -> Self {
        Self {
            session_cache,
            ..self
        }
    }

    pub async fn build(self) -> TradingSession {
        let mut execution_data = HashMap::new();
        for strategy in self.bot.as_ref().unwrap().strategies.iter() {
            let strategy_kind = strategy.get_strategy_kind();
            let data = match &self.session_cache {
                Some(session_cache) => {
                    // Lock the Mutex to get access to the HashMap
                    let cache = session_cache.lock().unwrap();

                    // Access the nested HashMap for the specific market and year
                    cache
                        .get(&self.market.unwrap())
                        .and_then(|year_map| year_map.get(&self.year.unwrap()))
                        .unwrap()
                        .get(&strategy_kind)
                        .unwrap()
                        .clone() // Clone the ExecutionData if it exists
                }
                None => {
                    // If there's no session_cache, populate the trading session data
                    self.populate_trading_session_data(strategy).await
                }
            };
            execution_data.insert(strategy_kind, data);
        }

        TradingSession {
            bot: self.bot.unwrap(),
            indicator_data_pair: self.indicator_data_pair.unwrap(),
            market: self.market.unwrap(),
            year: self.year.unwrap(),
            market_sim_data_kind: self.market_sim_data_kind.unwrap(),
            data: execution_data,
            cache_computations: self.cache_computations,
        }
    }

    async fn populate_trading_session_data(
        &self,
        strategy: &Arc<dyn Strategy + Send + Sync>,
    ) -> ExecutionData {
        let bot = self.bot.clone().unwrap();
        let market = self.market.unwrap();
        let year = self.year.unwrap();

        let path_finder = PathFinderBuilder::new()
            .with_data_provider(bot.data_provider.get_name())
            // .with_strategy_name("bot.strategy.get_name()".to_string()) // TODO no caching as originally thought
            .with_strategy_name("ppp".to_string()) // TODO no caching as originally thought, this is only for the current unittest in this file
            .with_market(market)
            .with_year(year)
            .with_time_interval(bot.time_interval)
            .with_time_frame(bot.time_frame.to_string())
            .build();

        let trading_indicators_df_map = self
            .get_trading_indicators_df_map(&path_finder, strategy)
            .await;
        let market_simulation_df_map = self
            .get_market_simulation_df_map(&path_finder, strategy)
            .await;

        // TODO market_sim_data -> compute rsi, sma, etc. and store it as a vec<MarketXYZ> where MarketXYZ is a struct containing ohlc data, and other real time indicators
        ExecutionData {
            market_sim_data: market_simulation_df_map,
            trading_indicators: trading_indicators_df_map,
        }
    }

    async fn get_trading_indicators_df_map(
        &self,
        path_finder: &PathFinder,
        strategy: &Arc<dyn Strategy + Send + Sync>,
    ) -> HashMap<TradingIndicatorKind, chapaty::types::DataFrameMap> {
        let some_indicator = self
            .indicator_data_pair
            .as_ref()
            .unwrap()
            .get(&strategy.get_strategy_kind());

        if some_indicator.is_none() {
            return HashMap::new();
        }

        let tasks: Vec<_> = some_indicator
            .unwrap()
            .iter()
            .map(|indicator_data_pair| {
                self.fetch_df_map(path_finder, indicator_data_pair.clone(), strategy)
            })
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
        strategy: &Arc<dyn Strategy + Send + Sync>,
    ) -> JoinHandle<(TradingIndicatorKind, HashMap<TimeFrameSnapshot, DataFrame>)> {
        let bot = self.bot.clone().unwrap();
        // TODO Fix -> need different simulation data for different strategies
        let sim_data = bot
            .strategies
            .get(0)
            .unwrap()
            .get_market_simulation_data_kind();
        let file_name = FileNameResolver::new(sim_data.into())
            .with_indicator_data_pair(indicator_data_pair.clone())
            .get_filename();

        let file_path_with_fallback =
            path_finder.get_file_path_with_fallback(file_name, &indicator_data_pair.data);

        let cloud_storage_client = self
            .initalize_cloud_storage_client_builder(strategy)
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
        strategy: &Arc<dyn Strategy + Send + Sync>,
    ) -> chapaty::types::DataFrameMap {
        let sim_data = strategy.get_market_simulation_data_kind();
        let market_sim_data = sim_data.into();
        let file_name_resolver = FileNameResolver::new(market_sim_data);

        let file_name = file_name_resolver.get_filename();
        let file_path_with_fallback =
            path_finder.get_file_path_with_fallback(file_name, &market_sim_data);

        let cloud_storage_client = self
            .initalize_cloud_storage_client_builder(strategy)
            .with_file_path_with_fallback(file_path_with_fallback)
            .build();

        cloud_storage_client.download_df_map().await
    }

    fn initalize_cloud_storage_client_builder(
        &self,
        strategy: &Arc<dyn Strategy + Send + Sync>,
    ) -> CloudStorageClientBuilder {
        let bot = self.bot.clone().unwrap();
        let sim_data = strategy.get_market_simulation_data_kind();
        CloudStorageClientBuilder::new(bot.clone())
            .with_simulation_data(sim_data.into())
            .with_market(self.market.unwrap())
            .with_year(self.year.unwrap())
            .with_cache_computations(self.cache_computations)
    }
}

#[cfg(test)]
mod test {
    // TODO Fix test not using automock for strategy
    use super::*;
    use crate::{
        cloud_api::api_for_unit_tests::{download_df, download_df_map},
        config,
        data_provider::{binance::Binance, MockDataProvider},
        decision_policy::choose_first_policy::ChooseFirstPolicy,
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
        mock_strategy
            .expect_get_market_simulation_data_kind()
            .return_const(MarketSimulationDataKind::Ohlcv1h);

        let strategy_kind = StrategyKind::Ppp;
        mock_strategy
            .expect_get_strategy_kind()
            .return_const(strategy_kind);

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

        let mock_strategy_arc: Arc<(dyn Strategy + std::marker::Send + Sync)> =
            Arc::new(mock_strategy);
        // Test Initialization
        let bot = BotBuilder::new(vec![mock_strategy_arc.clone()], data_provider)
            .with_google_cloud_storage_client(cloud_storage_client)
            .with_google_cloud_bucket(bucket)
            .with_time_interval(time_interval)
            // .with_market_simulation_data(MarketSimulationDataKind::Ohlcv1h)
            .with_decision_policy(Arc::new(ChooseFirstPolicy))
            .build()
            .unwrap();

        let session = TradingSessionBuilder::new()
            .with_bot(Arc::new(bot.clone()))
            .with_indicator_data_pair(bot.determine_indicator_data_pair())
            .with_market(MarketKind::BtcUsdt)
            .with_year(2022);

        // Test Evaluation
        let execution_data = session
            .populate_trading_session_data(&mock_strategy_arc)
            .await;
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
        mock_strategy
            .expect_get_market_simulation_data_kind()
            .return_const(MarketSimulationDataKind::Ohlcv1h);
        let strategy_kind = StrategyKind::Ppp;
        mock_strategy
            .expect_get_strategy_kind()
            .return_const(strategy_kind);

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

        let mock_strategy_arc: Arc<(dyn Strategy + std::marker::Send + Sync)> =
            Arc::new(mock_strategy);
        // Test Initialization
        let bot = BotBuilder::new(vec![mock_strategy_arc.clone()], data_provider)
            .with_google_cloud_storage_client(cloud_storage_client)
            .with_google_cloud_bucket(bucket)
            .with_time_interval(time_interval)
            .with_time_frame(TimeFrameKind::Weekly)
            .with_decision_policy(Arc::new(ChooseFirstPolicy))
            // .with_market_simulation_data(MarketSimulationDataKind::Ohlcv1h)
            .build()
            .unwrap();

        let session = TradingSessionBuilder::new()
            .with_bot(Arc::new(bot.clone()))
            .with_indicator_data_pair(bot.determine_indicator_data_pair())
            .with_market(MarketKind::BtcUsdt)
            .with_year(2022);

        // Test Evaluation
        let execution_data = session
            .populate_trading_session_data(&mock_strategy_arc)
            .await;
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
        // mock_strategy
        //     .expect_is_only_trading_on_news()
        //     .return_const(true);
        mock_strategy
            .expect_filter_on_economic_news_event()
            .return_const(Some(vec![NewsKind::UsaNFP]));
        let data_provider = MockDataProvider::new();
        let cloud_storage_client = config::get_google_cloud_storage_client().await;
        let bucket = config::GoogleCloudBucket {
            historical_market_data_bucket_name: "chapaty-ai-hdb-test".to_string(),
            cached_bot_data_bucket_name: "chapaty-ai-test".to_string(),
        };

        let strategy_kind = StrategyKind::Ppp;
        mock_strategy
            .expect_get_strategy_kind()
            .return_const(strategy_kind);

        // Test Initialization with news trading
        let bot = BotBuilder::new(vec![Arc::new(mock_strategy)], Arc::new(data_provider))
            .with_google_cloud_storage_client(cloud_storage_client.clone())
            .with_google_cloud_bucket(bucket.clone())
            .with_time_frame(TimeFrameKind::Weekly)
            .with_decision_policy(Arc::new(ChooseFirstPolicy))
            // .with_market_simulation_data(MarketSimulationDataKind::Ohlcv1h)
            .build()
            .unwrap();

        let indicator_data_pair_map = HashMap::from([(strategy_kind, HashSet::new())]);
        let data_map = HashMap::from([(strategy_kind, ExecutionData::default())]);
        let market_sim_data_kind_map =
            HashMap::from([(strategy_kind, MarketSimulationDataKind::Ohlc1m)]);

        let session = TradingSession {
            bot: Arc::new(bot),
            indicator_data_pair: Arc::new(indicator_data_pair_map.clone()),
            market: MarketKind::EurUsdFuture,
            year: 2022,
            data: data_map.clone(),
            market_sim_data_kind: market_sim_data_kind_map.clone(),
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
        // mock_strategy
        //     .expect_is_only_trading_on_news()
        //     .return_const(false);
        mock_strategy
            .expect_filter_on_economic_news_event()
            .return_const(None);
        let data_provider = MockDataProvider::new();

        let bot = BotBuilder::new(vec![Arc::new(mock_strategy)], Arc::new(data_provider))
            .with_google_cloud_storage_client(cloud_storage_client)
            .with_google_cloud_bucket(bucket)
            .with_time_frame(TimeFrameKind::Weekly)
            .with_decision_policy(Arc::new(ChooseFirstPolicy))
            // .with_market_simulation_data(MarketSimulationDataKind::Ohlcv1h)
            .build()
            .unwrap();

        let session = TradingSession {
            bot: Arc::new(bot),
            indicator_data_pair: Arc::new(indicator_data_pair_map),
            market: MarketKind::EurUsdFuture,
            year: 2022,
            data: data_map,
            market_sim_data_kind: market_sim_data_kind_map,
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
