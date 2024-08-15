use std::sync::Arc;

use chapaty::{
    config::GoogleCloudBucket, data_provider::DataProvider, strategy::Strategy, BotBuilder,
    MarketKind, MarketSimulationDataKind, TimeFrameKind, TimeInterval,
};
use google_cloud_storage::client::Client;

#[derive(Clone)]
pub struct BotConfig {
    pub client: Client,
    pub bucket: GoogleCloudBucket,
    pub strategy: Arc<dyn Strategy + Send + Sync>,
    pub data_provider: Arc<dyn DataProvider + Send + Sync>,
    pub market: MarketKind,
    pub year: u32,
    pub market_simulation_data: MarketSimulationDataKind,
    pub time_interval: Option<TimeInterval>,
    pub time_frame: TimeFrameKind,
}

impl From<BotConfig> for BotBuilder {
    fn from(value: BotConfig) -> Self {
        let builder = BotBuilder::new(value.strategy, value.data_provider)
            .with_years(vec![value.year])
            .with_markets(vec![value.market])
            .with_market_simulation_data(value.market_simulation_data)
            .with_time_frame(value.time_frame)
            .with_google_cloud_storage_client(value.client)
            .with_google_cloud_bucket(value.bucket)
            .with_save_result_as_csv(false)
            .with_cache_computations(false);

        if let Some(time_interval) = value.time_interval {
            builder.with_time_interval(time_interval)
        } else {
            builder
        }
    }
}
