use super::{
    backtesting_batch_data::BacktestingBatchData,
    execution_data::ExecutionData,
    indicator_data_pair::IndicatorDataPair,
    pre_trade_data::{PreTradeData, PreTradeDataBuilder},
    time_frame_snapshot::{TimeFrameSnapshot, TimeFrameSnapshotBuilder},
    Bot,
};
use crate::{
    backtest_result::pnl_report::PnLReport,
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
};
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
    pub cache_computations: bool,
}

impl TradingSession {
    pub async fn compute_pnl_report(self) -> PnLReport {
        let (tx, rx) = tokio::sync::oneshot::channel();
        rayon::spawn(move || {
            let _ = tx.send(self.run_backtesting());
        });
        rx.await.unwrap()
    }

    fn run_backtesting(&self) -> PnLReport {
        match self.bot.time_frame {
            TimeFrameKind::Daily => self.run_backtesting_daily(),
            TimeFrameKind::Weekly => self.run_backtesting_weekly(),
        }
    }

    fn run_backtesting_daily(&self) -> PnLReport {
        let pnl_report_data_rows: Vec<_> = (1..=52_i64)
            .into_par_iter()
            .flat_map(|cw| (1..=7).into_par_iter().map(move |wd| (cw, wd)))
            .map(|(cw, wd)| build_time_frame_snapshot(cw, Some(wd), None, None))
            .filter_map(|snapshot| self.get_daily_backtesting_batch_data(snapshot).ok())
            .map(|batch| self.compute_pnl_data_row(batch))
            .collect();

        pnl_report_data_rows.into_iter().collect()
    }

    fn run_backtesting_weekly(&self) -> PnLReport {
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
        let mut builder = PreTradeDataBuilder::new();

        builder = if is_on_monday(snapshot) {
            let last_friday = snapshot.last_friday();
            builder
                .with_market_sim_data(self.get_market_sim_data_data(&last_friday)?)
                .with_indicators(self.get_trading_indicator(&last_friday)?)
        } else {
            let yesterday = snapshot.shift_back_by_n_weekdays(1);
            builder
                .with_market_sim_data(self.get_market_sim_data_data(&yesterday)?)
                .with_indicators(self.get_trading_indicator(&yesterday)?)
        };

        Ok(builder.build())
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
            .with_data_provider(self.bot.data_provider.clone())
            .with_market_sim_data(batch.market_sim_data)
            .with_strategy(self.bot.strategy.clone())
            .with_pre_trade_data(batch.pre_trade_data)
            .with_year(self.year)
            .with_market(self.market)
            .with_time_frame_snapshot(batch.time_frame_snapshot)
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
    cache_computations: bool,
}

impl TradingSessionBuilder {
    pub fn new() -> Self {
        Self {
            bot: None,
            indicator_data_pair: None,
            market: None,
            year: None,
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
            data,
            cache_computations: self.cache_computations,
        }
    }

    async fn populate_trading_session_data(&self) -> ExecutionData {
        let bot = self.bot.clone().unwrap();
        let market = self.market.unwrap();
        let year = self.year.unwrap();

        let path_finder = PathFinderBuilder::new()
            .with_data_provider(bot.data_provider.get_data_producer_kind())
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
        let cloud_storage_client_builder = self.initalize_cloud_storage_client_builder();
        let bot = self.bot.clone().unwrap();

        let market_sim_data = bot.market_simulation_data.into();
        let file_name_resolver = FileNameResolver::new(market_sim_data);

        let file_name = file_name_resolver.get_filename();
        let file_path_with_fallback =
            path_finder.get_file_path_with_fallback(file_name, &market_sim_data);

        let cloud_storage_client = cloud_storage_client_builder
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
        data_provider::binance::Binance,
        enums::indicator::PriceHistogramKind,
        strategy::{MockStrategy, RequriedPreTradeValues},
        BotBuilder, MarketSimulationDataKind, TimeInterval,
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
            .expect_get_required_pre_trade_vales()
            .return_const(required_pre_trade_values.clone());
        mock_strategy.expect_get_name().return_const("ppp");
        let data_provider = Arc::new(Binance::new());
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
            .expect_get_required_pre_trade_vales()
            .return_const(required_pre_trade_values.clone());
        mock_strategy.expect_get_name().return_const("ppp");
        let data_provider = Arc::new(Binance::new());
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
}
