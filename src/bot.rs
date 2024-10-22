mod backtesting_batch_data;
pub mod execution_data;
pub mod indicator_data_pair;
pub mod pre_trade_data;
pub mod time_frame_snapshot;
pub mod time_interval;
pub mod trading_session;
pub mod transformer;
use self::{
    indicator_data_pair::IndicatorDataPair, time_interval::TimeInterval,
    trading_session::TradingSessionBuilder,
};
use crate::{
    backtest_result::{BacktestResult, MarketAndYearBacktestResult},
    config::GoogleCloudBucket,
    data_provider::DataProvider,
    decision_policy::DecisionPolicy,
    enums::{
        bot::TimeFrameKind, data::HdbSourceDirKind, error::ChapatyErrorKind, markets::MarketKind,
        strategy,
    },
    pnl::{
        pnl_report::{PnLReport, PnLReports},
        pnl_statement::PnLStatement,
    },
    strategy::Strategy,
};
use execution_data::ExecutionData;
use google_cloud_storage::client::Client;
use mockall::automock;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

/*
- Create the possiblity to compute Vec<Agent> and at the end collect the result, which is a merged PnL
*/
#[derive(Clone)]
pub struct Bot {
    client: Option<Client>,
    name: String,
    bucket: GoogleCloudBucket,
    strategies: Vec<Arc<dyn Strategy + Send + Sync>>,
    data_provider: Arc<dyn DataProvider + Send + Sync>,
    markets: Vec<MarketKind>,
    years: Vec<u32>,
    time_interval: Option<TimeInterval>,
    time_frame: TimeFrameKind,
    save_result_as_csv: bool,
    cache_computations: bool,
    session_cache: Option<Arc<Mutex<HashMap<MarketKind, HashMap<u32, ExecutionData>>>>>,
    decision_policy: Arc<dyn DecisionPolicy + Send + Sync>,
}
pub struct BotBuilder {
    client: Option<Client>,
    name: String,
    bucket: GoogleCloudBucket,
    strategies: Vec<Arc<dyn Strategy + Send + Sync>>,
    data_provider: Arc<dyn DataProvider + Send + Sync>,
    markets: Vec<MarketKind>,
    years: Vec<u32>,
    time_interval: Option<TimeInterval>,
    time_frame: TimeFrameKind,
    save_result_as_csv: bool,
    cache_computations: bool,
    session_cache: Option<Arc<Mutex<HashMap<MarketKind, HashMap<u32, ExecutionData>>>>>,
    decision_policy: Option<Arc<dyn DecisionPolicy + Send + Sync>>,
}

#[automock]
impl Bot {
    pub async fn backtest(
        &self,
    ) -> (
        BacktestResult,
        Arc<Mutex<HashMap<MarketKind, HashMap<u32, ExecutionData>>>>,
    ) {
        let (pnl_statement, session_cache) = self.compute_pnl_statement().await;

        let performance_report = pnl_statement.compute_performance_report();
        let trade_breakdown_report = pnl_statement.compute_trade_breakdown_report();
        let equity_curves = pnl_statement.compute_equity_curves();

        let market_and_year_backtest_result = MarketAndYearBacktestResult {
            pnl_statement,
            performance_reports: performance_report,
            trade_breakdown_reports: trade_breakdown_report,
            equity_curves,
        };

        let res: BacktestResult = market_and_year_backtest_result.into();

        if self.save_result_as_csv {
            res.save_as_csv(&self.name);
        }

        (res, session_cache)
    }

    pub fn get_shared_pointer(&self) -> Arc<Bot> {
        Arc::new(self.clone())
    }

    pub fn get_client_ref(&self) -> &Option<Client> {
        &self.client
    }

    pub fn get_historical_data_bucket_name_ref(&self) -> &str {
        &self.bucket.historical_market_data_bucket_name
    }

    pub fn get_historical_data_bucket_name_owned(&self) -> String {
        self.bucket.historical_market_data_bucket_name.clone()
    }

    pub fn get_cached_data_bucket_name_ref(&self) -> &str {
        &self.bucket.cached_bot_data_bucket_name
    }

    pub fn get_data_provider(&self) -> Arc<dyn DataProvider + Send + Sync> {
        self.data_provider.clone()
    }

    pub fn get_strategies(&self) -> Vec<Arc<dyn Strategy + Send + Sync>> {
        self.strategies.clone()
    }

    pub fn get_time_frame_ref(&self) -> &TimeFrameKind {
        &self.time_frame
    }

    pub fn get_time_interval_optional_ref(&self) -> &Option<TimeInterval> {
        &self.time_interval
    }

    async fn compute_pnl_statement(
        &self,
    ) -> (
        PnLStatement,
        Arc<Mutex<HashMap<MarketKind, HashMap<u32, ExecutionData>>>>,
    ) {
        let session_cache = Arc::new(Mutex::new(HashMap::new()));
        let tasks: Vec<_> = self
            .markets
            .clone()
            .into_iter()
            .map(|market| {
                let _self = self.clone();
                let session_cache = Arc::clone(&session_cache);
                tokio::spawn(async move { _self.compute_pnl_reports(market, session_cache).await })
            })
            .collect();

        let pnl_data = futures::future::join_all(tasks).await.into_iter().fold(
            HashMap::new(),
            |mut pnl_data, pnl_report| {
                let pnl_report = pnl_report.unwrap();
                pnl_data.insert(pnl_report.market, pnl_report);
                pnl_data
            },
        );

        (
            PnLStatement {
                strategy_name: self.name.clone(),
                markets: self.markets.clone(),
                pnl_data,
            },
            session_cache,
        )
    }

    async fn compute_pnl_reports(
        &self,
        market: MarketKind,
        session_cache: Arc<Mutex<HashMap<MarketKind, HashMap<u32, ExecutionData>>>>,
    ) -> PnLReports {
        let trading_session_builder = TradingSessionBuilder::new()
            .with_bot(self.get_shared_pointer())
            .with_indicator_data_pair(self.determine_indicator_data_pair())
            .with_cache_computations(self.cache_computations)
            .with_market(market)
            // TODO fix, as diffrent strategies need different market sim data
            .with_market_sim_data_kind(
                self.strategies
                    .get(0)
                    .unwrap()
                    .get_market_simulation_data_kind(),
            );

        let tasks: Vec<_> = self
            .years
            .clone()
            .into_iter()
            .map(|year| {
                let builder = trading_session_builder.clone();
                // TODO rename
                let strategy = "TODO_RENAME_self.strategy.get_name();".to_string();
                let cache_computations = self.cache_computations;
                let session_cache = Arc::clone(&session_cache);
                let some_session_cache = self.session_cache.clone();
                tokio::spawn(async move {
                    let session = builder
                        .with_year(year)
                        .with_session_cache(some_session_cache)
                        .build()
                        .await;
                    if cache_computations {
                        let mut cache = session_cache.lock().unwrap();

                        cache
                            .entry(market)
                            .or_insert_with(HashMap::new)
                            .insert(year, session.data.clone());
                    }
                    PnLReport {
                        market,
                        year,
                        strategy,
                        pnl: session.compute_pnl_report().await,
                    }
                })
            })
            .collect();

        futures::future::join_all(tasks)
            .await
            .into_iter()
            .map(Result::unwrap)
            .collect::<Vec<_>>()
            .into_iter()
            .collect()
    }

    fn determine_indicator_data_pair(&self) -> Arc<HashSet<IndicatorDataPair>> {
        let mut map = HashSet::new();

        for strategy in &self.strategies {
            if let Some(required) = strategy.get_required_pre_trade_values() {
                map.extend(required.trading_indicators.iter().fold(
                    HashSet::new(),
                    |mut acc, trading_indicator| {
                        let indicator_data_pair = IndicatorDataPair::new(
                            *trading_indicator,
                            HdbSourceDirKind::from(*trading_indicator),
                        );
                        acc.insert(indicator_data_pair);
                        acc
                    },
                ));
            }
        }

        Arc::new(map)
    }
}

impl BotBuilder {
    pub fn new(
        strategies: Vec<Arc<dyn Strategy + Send + Sync>>,
        data_provider: Arc<dyn DataProvider + Send + Sync>,
    ) -> Self {
        Self {
            client: None,
            name: "chapaty".to_string(),
            bucket: GoogleCloudBucket {
                historical_market_data_bucket_name: "".to_string(),
                cached_bot_data_bucket_name: "".to_string(),
            },
            strategies,
            data_provider,
            markets: vec![],
            years: vec![],
            time_interval: None,
            time_frame: TimeFrameKind::Daily,
            save_result_as_csv: false,
            cache_computations: false,
            session_cache: None,
            decision_policy: None,
        }
    }

    pub fn with_name(self, name: String) -> Self {
        Self { name, ..self }
    }

    pub fn with_years(self, years: Vec<u32>) -> Self {
        Self { years, ..self }
    }

    pub fn with_markets(self, markets: Vec<MarketKind>) -> Self {
        Self { markets, ..self }
    }

    pub fn with_decision_policy(
        self,
        decision_policy: Arc<dyn DecisionPolicy + Send + Sync>,
    ) -> Self {
        Self {
            decision_policy: Some(decision_policy),
            ..self
        }
    }

    pub fn with_time_interval(self, time_interval: TimeInterval) -> Self {
        Self {
            time_interval: Some(time_interval),
            ..self
        }
    }

    pub fn with_time_frame(self, time_frame: TimeFrameKind) -> Self {
        Self { time_frame, ..self }
    }

    pub fn with_google_cloud_storage_client(self, client: Client) -> Self {
        Self {
            client: Some(client),
            ..self
        }
    }

    pub fn with_save_result_as_csv(self, save_result_as_csv: bool) -> Self {
        Self {
            save_result_as_csv,
            ..self
        }
    }

    pub fn with_cache_computations(self, cache_computations: bool) -> Self {
        Self {
            cache_computations,
            ..self
        }
    }

    pub fn with_session_cache_computations(
        self,
        session_cache: Arc<Mutex<HashMap<MarketKind, HashMap<u32, ExecutionData>>>>,
    ) -> Self {
        Self {
            session_cache: Some(session_cache),
            ..self
        }
    }

    pub fn with_google_cloud_bucket(self, bucket: GoogleCloudBucket) -> Self {
        Self { bucket, ..self }
    }

    pub fn build(self) -> Result<Bot, ChapatyErrorKind> {
        // let client = self.client.ok_or(
        //     ChapatyErrorKind::BuildBotError("Google Cloud Client is not initalized. Use BotBuilder::with_google_cloud_client for initalization"
        //     .to_string()))?;

        Ok(Bot {
            client: self.client,
            name: self.name,
            bucket: self.bucket,
            strategies: self.strategies,
            data_provider: self.data_provider,
            markets: self.markets,
            years: self.years,
            time_interval: self.time_interval,
            time_frame: self.time_frame,
            save_result_as_csv: self.save_result_as_csv,
            cache_computations: self.cache_computations,
            session_cache: self.session_cache,
            decision_policy: self.decision_policy.unwrap(),
        })
    }
}

#[cfg(test)]
mod test {
    use crate::{
        bot::IndicatorDataPair,
        config,
        data_provider::cme::Cme,
        decision_policy::choose_first_policy::ChooseFirstPolicy,
        enums::{
            data::HdbSourceDirKind,
            indicator::{PriceHistogramKind, TradingIndicatorKind},
        },
        strategy::{MockStrategy, RequriedPreTradeValues},
        BotBuilder,
    };
    use std::{collections::HashSet, sync::Arc};

    #[tokio::test]
    async fn test_determine_indicator_data_pair() {
        let data_provider = Arc::new(Cme);
        let cloud_storage_client = config::get_google_cloud_storage_client().await;
        let mut mock_strategy = MockStrategy::new();
        let trading_indicators = vec![
            TradingIndicatorKind::Poc(PriceHistogramKind::VolAggTrades),
            TradingIndicatorKind::Poc(PriceHistogramKind::Tpo1m),
            TradingIndicatorKind::Poc(PriceHistogramKind::VolTick),
            TradingIndicatorKind::ValueAreaHigh(PriceHistogramKind::VolTick),
            TradingIndicatorKind::ValueAreaLow(PriceHistogramKind::VolAggTrades),
        ];
        mock_strategy
            .expect_get_required_pre_trade_values()
            .return_const(RequriedPreTradeValues {
                market_values: Vec::new(),
                trading_indicators,
            });

        let bot = BotBuilder::new(vec![Arc::new(mock_strategy)], data_provider)
            .with_google_cloud_storage_client(cloud_storage_client)
            .with_decision_policy(Arc::new(ChooseFirstPolicy))
            .build()
            .unwrap();

        let required_data = bot.determine_indicator_data_pair();
        let expected = HashSet::from([
            IndicatorDataPair::new(
                TradingIndicatorKind::Poc(PriceHistogramKind::VolAggTrades),
                HdbSourceDirKind::AggTrades,
            ),
            IndicatorDataPair::new(
                TradingIndicatorKind::Poc(PriceHistogramKind::VolTick),
                HdbSourceDirKind::Tick,
            ),
            IndicatorDataPair::new(
                TradingIndicatorKind::Poc(PriceHistogramKind::Tpo1m),
                HdbSourceDirKind::Ohlc1m,
            ),
            IndicatorDataPair::new(
                TradingIndicatorKind::ValueAreaHigh(PriceHistogramKind::VolTick),
                HdbSourceDirKind::Tick,
            ),
            IndicatorDataPair::new(
                TradingIndicatorKind::ValueAreaLow(PriceHistogramKind::VolAggTrades),
                HdbSourceDirKind::AggTrades,
            ),
        ]);

        assert_eq!(*required_data, expected);
    }
}
