use std::{collections::HashSet, fmt::Debug, io::Read, sync::Arc};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    data::event::{
        EconomicCalendarId, EmaId, MarketEvent, MarketId, OhlcvId, RsiId, SmaId, StreamId, TpoId,
        TradesId, VolumeProfileId,
    },
    error::{ChapatyResult, IoError, SystemError},
    gym::trading::config::EnvConfig,
    io::{SerdeFormat, StorageLocation},
    sorted_vec_map::SortedVecMap,
};

pub type EventMap<S> = SortedVecMap<S, Box<[<S as StreamId>::Event]>>;
pub type OhlcvEventMap = EventMap<OhlcvId>;
pub type TradeEventMap = EventMap<TradesId>;
pub type EconomicCalEventMap = EventMap<EconomicCalendarId>;
pub type VolumeProfileEventMap = EventMap<VolumeProfileId>;
pub type TpoEventMap = EventMap<TpoId>;
pub type EmaEventMap = EventMap<EmaId>;
pub type SmaEventMap = EventMap<SmaId>;
pub type RsiEventMap = EventMap<RsiId>;

#[derive(Debug, Serialize, Deserialize)]
pub struct SimulationData {
    ohlcv: OhlcvEventMap,
    trade: TradeEventMap,
    economic_cal: EconomicCalEventMap,
    volume_profile: VolumeProfileEventMap,
    tpo: TpoEventMap,
    ema: EmaEventMap,
    sma: SmaEventMap,
    rsi: RsiEventMap,
    market_ids: Arc<[MarketId]>,
    global_availability_start: DateTime<Utc>,
    global_open_start: DateTime<Utc>,
    hash: String,
}

impl SimulationData {
    pub fn ohlcv(&self) -> &OhlcvEventMap {
        &self.ohlcv
    }

    pub fn trade(&self) -> &TradeEventMap {
        &self.trade
    }

    pub fn economic_cal(&self) -> &EconomicCalEventMap {
        &self.economic_cal
    }

    pub fn volume_profile(&self) -> &VolumeProfileEventMap {
        &self.volume_profile
    }

    pub fn tpo(&self) -> &TpoEventMap {
        &self.tpo
    }

    pub fn ema(&self) -> &EmaEventMap {
        &self.ema
    }

    pub fn sma(&self) -> &SmaEventMap {
        &self.sma
    }

    pub fn rsi(&self) -> &RsiEventMap {
        &self.rsi
    }

    pub fn market_ids(&self) -> Arc<[MarketId]> {
        self.market_ids.clone()
    }

    /// Returns the absolute earliest moment any data becomes available.
    /// Use this to initialize the global clock at the very start of the simulation.
    pub fn global_availability_start(&self) -> DateTime<Utc> {
        self.global_availability_start
    }

    /// Returns the absolute earliest moment any market activity begins (Window Open).
    ///
    /// Use this to initialize the Simulation's internal clock or "Episode" tracking.
    pub fn global_open_start(&self) -> DateTime<Utc> {
        self.global_open_start
    }

    /// Serializes and writes the SimulationData to a given storage location.
    ///
    /// # Arguments
    ///
    /// * `location` - The storage location where the data will be written
    /// * `format` - The serialization format to use (Postcard or Pickle)
    /// * `buffer_size` - Size of the internal write buffer, in bytes.
    ///
    ///   This controls how much data is buffered in memory before being flushed
    ///   to the underlying storage. Larger values generally improve throughput
    ///   for large writes at the cost of higher memory usage, while smaller values
    ///   reduce memory usage but may result in more frequent I/O operations.
    ///
    ///   If unsure, a good default is `128 * 1024` (128 KiB).
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on successful write, or an error if serialization or I/O fails.
    #[tracing::instrument(skip(self, location), fields(hash = %self.hash, format = ?format))]
    pub async fn write(
        self: Arc<Self>,
        location: &StorageLocation<'_>,
        format: SerdeFormat,
        buffer_size: usize,
    ) -> ChapatyResult<()> {
        let filename = format!("{}.{format}", self.hash);

        tracing::debug!(
            filename = %filename,
            "Writing simulation data to storage"
        );

        let mut writer = location.writer(&filename, buffer_size).await?;

        let result = tokio::task::spawn_blocking(move || {
            let res = match format {
                SerdeFormat::Postcard => postcard::to_io(&*self, &mut writer)
                    .map(|_| {})
                    .map_err(|e| IoError::WriteFailed(e.to_string()).into()),
            };

            if res.is_ok() {
                let _ = writer.flush();
            }
            res
        })
        .await
        .map_err(|e| SystemError::Generic(e.to_string()))?;

        match &result {
            Ok(_) => tracing::info!(
                filename = %filename,
                "Successfully wrote simulation data"
            ),
            Err(e) => tracing::error!(
                filename = %filename,
                error = %e,
                "Failed to write simulation data"
            ),
        }

        result
    }

    /// Reads and deserializes SimulationData from a given storage location.
    ///
    /// # Cache Behavior
    ///
    /// This function attempts to read cached simulation data based on the hash derived from
    /// `env_cfg`. If the file doesn't exist or deserialization fails, it returns an error
    /// that should typically be handled as a **cache miss**.
    ///
    /// # Important Limitations
    ///
    /// **Schema-less formats (Postcard, Pickle) have no versioning:**
    /// - If the `SimulationData` struct definition changes between writes and reads,
    ///   deserialization will fail or produce corrupt data
    /// - The hash is based on `EnvConfig`, not on the struct schema
    /// - **Cache misses can occur even with identical `EnvConfig` if the code changed**
    ///
    /// This is a convenience caching mechanism, not a production-grade solution.
    ///
    /// # Arguments
    ///
    /// * `env_cfg` - The environment configuration (used to derive the filename hash)
    /// * `location` - The storage location to read from
    /// * `format` - The serialization format to use (must match the format used to write)
    /// * `buffer_size` - Size of the internal read buffer, in bytes. A good default is: `128 * 1024` (128 KiB).
    ///
    /// # Returns
    ///
    /// Returns the deserialized `SimulationData` on success, or an error on:
    /// - File not found (cache miss)
    /// - Deserialization failure (schema mismatch or corrupt data)
    /// - I/O errors
    ///
    /// # Errors
    ///
    /// This function returns an error on cache miss or deserialization failure.
    /// **These errors should typically be caught and treated as cache misses** rather
    /// than fatal errors, allowing the system to regenerate the data.
    #[tracing::instrument(skip(location, env_cfg), fields(format = ?format))]
    pub async fn read(
        env_cfg: &EnvConfig,
        location: &StorageLocation<'_>,
        format: SerdeFormat,
        buffer_size: usize,
    ) -> ChapatyResult<Self> {
        let hash = env_cfg.hash()?;
        let filename = format!("{hash}.{format}");

        tracing::debug!(
            filename = %filename,
            hash = %hash,
            "Attempting to read cached simulation data"
        );

        let (mut reader, file_size) = match location.reader_with_size(&filename, buffer_size).await
        {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(
                    filename = %filename,
                    error = %e,
                    "Cache miss: simulation data not found"
                );
                return Err(e);
            }
        };

        let result = tokio::task::spawn_blocking(move || match format {
            SerdeFormat::Postcard => {
                const MB: u64 = 1024 * 1024;
                let capacity = file_size.unwrap_or(100 * MB) as usize;
                let mut data = Vec::with_capacity(capacity);

                reader
                    .read_to_end(&mut data)
                    .map_err(|e| IoError::ReadFailed(e.to_string()))?;

                postcard::from_bytes(&data).map_err(|e| IoError::ReadFailed(e.to_string()).into())
            }
        })
        .await
        .map_err(|e| SystemError::Generic(e.to_string()))?;

        match &result {
            Ok(_) => tracing::info!(
                filename = %filename,
                "Successfully loaded cached simulation data"
            ),
            Err(e) => tracing::warn!(
                filename = %filename,
                error = %e,
                "Cache miss: deserialization failed (possible schema mismatch)"
            ),
        }

        result
    }
}

/// Object-safe trait for querying time properties of a data stream.
pub trait StreamTimeInfo {
    /// The absolute earliest point in time any data becomes immutable and available.
    fn min_availability(&self) -> Option<DateTime<Utc>>;

    /// The absolute earliest timestamp any data window opens.
    fn min_open_time(&self) -> Option<DateTime<Utc>>;
}

impl<S: StreamId> StreamTimeInfo for EventMap<S> {
    fn min_availability(&self) -> Option<DateTime<Utc>> {
        self.iter()
            .filter_map(|(_, events)| events.first().map(MarketEvent::point_in_time))
            .min()
    }

    fn min_open_time(&self) -> Option<DateTime<Utc>> {
        self.iter()
            .filter_map(|(_, events)| events.first().map(MarketEvent::opened_at))
            .min()
    }
}

// ================================================================================================
// SimulationData Builder
// ================================================================================================
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct SimulationDataBuilder {
    ohlcv: OhlcvEventMap,
    trade: TradeEventMap,
    economic_cal: EconomicCalEventMap,
    volume_profile: VolumeProfileEventMap,
    tpo: TpoEventMap,
    ema: EmaEventMap,
    sma: SmaEventMap,
    rsi: RsiEventMap,
}

impl Default for SimulationDataBuilder {
    fn default() -> Self {
        Self {
            ohlcv: OhlcvEventMap::new(),
            trade: TradeEventMap::new(),
            economic_cal: EconomicCalEventMap::new(),
            volume_profile: VolumeProfileEventMap::new(),
            tpo: TpoEventMap::new(),
            ema: EmaEventMap::new(),
            sma: SmaEventMap::new(),
            rsi: RsiEventMap::new(),
        }
    }
}

impl SimulationDataBuilder {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn build(self, env_cfg: EnvConfig) -> ChapatyResult<SimulationData> {
        let hash = env_cfg.hash()?;
        let global_availability_start = self.global_availability_start();
        let global_open_start = self.global_open_start();
        let market_ids = self.collect_sorted_market_ids();

        Ok(SimulationData {
            ohlcv: self.ohlcv,
            trade: self.trade,
            economic_cal: self.economic_cal,
            volume_profile: self.volume_profile,
            tpo: self.tpo,
            ema: self.ema,
            sma: self.sma,
            rsi: self.rsi,
            market_ids: market_ids.into(),
            global_availability_start,
            global_open_start,
            hash,
        })
    }

    pub(crate) fn with_ohlcv(self, ohlcv: OhlcvEventMap) -> Self {
        Self { ohlcv, ..self }
    }

    pub(crate) fn with_trade(self, trade: TradeEventMap) -> Self {
        Self { trade, ..self }
    }

    pub(crate) fn with_economic_news(self, economic_cal: EconomicCalEventMap) -> Self {
        Self {
            economic_cal,
            ..self
        }
    }

    pub(crate) fn with_volume_profile(self, volume_profile: VolumeProfileEventMap) -> Self {
        Self {
            volume_profile,
            ..self
        }
    }

    pub(crate) fn with_tpo(self, tpo: TpoEventMap) -> Self {
        Self { tpo, ..self }
    }

    pub(crate) fn with_ema(self, ema: EmaEventMap) -> Self {
        Self { ema, ..self }
    }

    pub(crate) fn with_sma(self, sma: SmaEventMap) -> Self {
        Self { sma, ..self }
    }

    pub(crate) fn with_rsi(self, rsi: RsiEventMap) -> Self {
        Self { rsi, ..self }
    }

    /// Returns a unified list of all data streams as trait objects.
    /// This allows generic iteration over "Time" without worrying about the underlying Types.
    fn all_streams(&self) -> [&dyn StreamTimeInfo; 8] {
        [
            &self.ohlcv,
            &self.trade,
            &self.economic_cal,
            &self.volume_profile,
            &self.tpo,
            &self.ema,
            &self.sma,
            &self.rsi,
        ]
    }

    /// Returns the absolute earliest moment any data becomes available.
    /// Use this to initialize the global clock at the very start of the simulation.
    fn global_availability_start(&self) -> DateTime<Utc> {
        self.all_streams()
            .iter()
            .filter_map(|stream| stream.min_availability())
            .min()
            .unwrap_or(DateTime::<Utc>::MIN_UTC)
    }

    /// Returns the absolute earliest moment any market activity begins (Window Open).
    ///
    /// Use this to initialize the Simulation's internal clock or "Episode" tracking.
    fn global_open_start(&self) -> DateTime<Utc> {
        self.all_streams()
            .iter()
            .filter_map(|stream| stream.min_open_time())
            .min()
            .unwrap_or(DateTime::<Utc>::MIN_UTC)
    }

    /// Returns a deterministic (sorted) list of unique MarketIds.
    fn collect_sorted_market_ids(&self) -> Vec<MarketId> {
        let mut unique_markets = HashSet::new();

        // Extract from Price-Authoritative sources
        for id in self.ohlcv.keys() {
            unique_markets.insert(MarketId::from(id));
        }
        for id in self.trade.keys() {
            unique_markets.insert(MarketId::from(id));
        }

        let mut sorted_markets = unique_markets.into_iter().collect::<Vec<_>>();
        sorted_markets.sort();

        sorted_markets
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        DataSource,
        data::{
            config::{EconomicCalendarConfig, OhlcvSpotConfig},
            domain::{
                CountryCode, DataBroker, EconomicCategory, EconomicEventImpact, Exchange, Period,
                Price, Quantity, SpotPair, Symbol,
            },
            event::{EconomicCalendarId, EconomicEvent, Ohlcv, OhlcvId, Trade, TradesId},
        },
    };

    fn make_ohlcv(open_ts: &str, close_ts: &str) -> Ohlcv {
        Ohlcv {
            open_timestamp: DateTime::parse_from_rfc3339(open_ts)
                .unwrap()
                .with_timezone(&Utc),
            close_timestamp: DateTime::parse_from_rfc3339(close_ts)
                .unwrap()
                .with_timezone(&Utc),
            open: Price(100.0),
            high: Price(105.0),
            low: Price(95.0),
            close: Price(102.0),
            volume: Quantity(1000.0),
            quote_asset_volume: None,
            number_of_trades: None,
            taker_buy_base_asset_volume: None,
            taker_buy_quote_asset_volume: None,
        }
    }

    fn make_trade(ts: &str) -> Trade {
        Trade {
            timestamp: DateTime::parse_from_rfc3339(ts)
                .unwrap()
                .with_timezone(&Utc),
            price: Price(100.0),
            quantity: crate::data::domain::Quantity(1.0),
            trade_id: None,
            quote_asset_volume: None,
            is_buyer_maker: None,
            is_best_match: None,
        }
    }

    fn make_ohlcv_id(symbol: Symbol) -> OhlcvId {
        OhlcvId {
            broker: DataBroker::Binance,
            exchange: Exchange::Binance,
            symbol,
            period: Period::Minute(1),
        }
    }

    fn make_trade_id(symbol: Symbol) -> TradesId {
        TradesId {
            broker: DataBroker::Binance,
            exchange: Exchange::Binance,
            symbol,
        }
    }

    fn make_economic_calendar_id() -> EconomicCalendarId {
        EconomicCalendarId {
            broker: DataBroker::InvestingCom,
            data_source: None,
            country_code: Some(CountryCode::Us),
            category: Some(EconomicCategory::Employment),
            importance: None,
        }
    }

    fn make_economic_event(ts: &str) -> EconomicEvent {
        EconomicEvent {
            timestamp: DateTime::parse_from_rfc3339(ts)
                .unwrap()
                .with_timezone(&Utc),
            data_source: "investingcom".to_string(),
            category: "Employment".to_string(),
            news_name: "Nonfarm Payrolls".to_string(),
            country_code: CountryCode::Us,
            currency_code: "USD".to_string(),
            economic_impact: EconomicEventImpact::High,
            news_type: Some("NFP".to_string()),
            news_type_confidence: Some(0.95),
            news_type_source: Some("classifier".to_string()),
            period: Some("mom".to_string()),
            actual: None,
            forecast: None,
            previous: None,
        }
    }

    #[test]
    fn returns_earliest_availability_from_multiple_streams() {
        let symbol = Symbol::Spot(crate::data::domain::SpotPair::BtcUsdt);
        let ohlcv_id = make_ohlcv_id(symbol);
        let trade_id = make_trade_id(symbol);

        // OHLCV closes at 10:01, Trade at 09:30
        let ohlcv = make_ohlcv("2024-01-01T10:00:00Z", "2024-01-01T10:01:00Z");
        let trade = make_trade("2024-01-01T09:30:00Z");

        let mut ohlcv_map = OhlcvEventMap::new();
        ohlcv_map.insert(ohlcv_id, Box::new([ohlcv]));

        let mut trade_map = TradeEventMap::new();
        trade_map.insert(trade_id, Box::new([trade]));

        let builder = SimulationDataBuilder::default()
            .with_ohlcv(ohlcv_map)
            .with_trade(trade_map);

        let result = builder.global_availability_start();

        // Trade availability (09:30) is earlier than OHLCV close (10:01)
        let expected = DateTime::parse_from_rfc3339("2024-01-01T09:30:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(result, expected);
    }

    #[test]
    fn returns_earliest_from_single_stream() {
        let symbol = Symbol::Spot(crate::data::domain::SpotPair::BtcUsdt);
        let ohlcv_id = make_ohlcv_id(symbol);

        let ohlcv1 = make_ohlcv("2024-01-01T10:00:00Z", "2024-01-01T10:01:00Z");
        let ohlcv2 = make_ohlcv("2024-01-01T09:00:00Z", "2024-01-01T09:01:00Z");

        let mut ohlcv_map = OhlcvEventMap::new();
        // Note: events are stored as-is, min_availability finds the min close_timestamp
        ohlcv_map.insert(ohlcv_id, Box::new([ohlcv2, ohlcv1]));

        let builder = SimulationDataBuilder::default().with_ohlcv(ohlcv_map);

        let result = builder.global_availability_start();

        let expected = DateTime::parse_from_rfc3339("2024-01-01T09:01:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(result, expected);
    }

    #[test]
    fn global_availability_returns_min_utc_when_no_events() {
        let builder = SimulationDataBuilder::default();
        let result = builder.global_availability_start();
        assert_eq!(result, DateTime::<Utc>::MIN_UTC);
    }

    #[test]
    fn returns_earliest_open_from_multiple_streams() {
        let symbol = Symbol::Spot(crate::data::domain::SpotPair::BtcUsdt);
        let ohlcv_id = make_ohlcv_id(symbol);
        let trade_id = make_trade_id(symbol);

        // OHLCV opens at 09:00, Trade at 09:30 (trade opened_at == point_in_time)
        let ohlcv = make_ohlcv("2024-01-01T09:00:00Z", "2024-01-01T09:01:00Z");
        let trade = make_trade("2024-01-01T09:30:00Z");

        let mut ohlcv_map = OhlcvEventMap::new();
        ohlcv_map.insert(ohlcv_id, Box::new([ohlcv]));

        let mut trade_map = TradeEventMap::new();
        trade_map.insert(trade_id, Box::new([trade]));

        let builder = SimulationDataBuilder::default()
            .with_ohlcv(ohlcv_map)
            .with_trade(trade_map);

        let result = builder.global_open_start();

        // OHLCV open (09:00) is earlier than Trade (09:30)
        let expected = DateTime::parse_from_rfc3339("2024-01-01T09:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(result, expected);
    }

    #[test]
    fn global_open_returns_min_utc_when_no_events() {
        let builder = SimulationDataBuilder::default();
        let result = builder.global_open_start();
        assert_eq!(result, DateTime::<Utc>::MIN_UTC);
    }

    #[test]
    fn returns_sorted_unique_market_ids() {
        let symbol_a = Symbol::Spot(SpotPair::BtcUsdt);
        let symbol_b = Symbol::Spot(SpotPair::EthUsdt);

        let ohlcv_id_a = make_ohlcv_id(symbol_a);
        let ohlcv_id_b = make_ohlcv_id(symbol_b);

        let ohlcv = make_ohlcv("2024-01-01T10:00:00Z", "2024-01-01T10:01:00Z");

        let mut ohlcv_map = OhlcvEventMap::new();
        // Insert in non-sorted order (B before A by symbol if B > A)
        ohlcv_map.insert(ohlcv_id_b, Box::new([ohlcv]));
        ohlcv_map.insert(ohlcv_id_a, Box::new([ohlcv]));

        let builder = SimulationDataBuilder::default().with_ohlcv(ohlcv_map);

        let result = builder.collect_sorted_market_ids();

        // Should be sorted
        assert_eq!(result.len(), 2);
        for i in 0..result.len() - 1 {
            assert!(result[i] < result[i + 1], "MarketIds should be sorted");
        }
    }

    #[test]
    fn deduplicates_market_ids_from_ohlcv_and_trade() {
        let symbol = Symbol::Spot(SpotPair::BtcUsdt);
        let ohlcv_id = make_ohlcv_id(symbol);
        let trade_id = make_trade_id(symbol);

        let ohlcv = make_ohlcv("2024-01-01T10:00:00Z", "2024-01-01T10:01:00Z");
        let trade = make_trade("2024-01-01T09:30:00Z");

        let mut ohlcv_map = OhlcvEventMap::new();
        ohlcv_map.insert(ohlcv_id, Box::new([ohlcv]));

        let mut trade_map = TradeEventMap::new();
        trade_map.insert(trade_id, Box::new([trade]));

        let builder = SimulationDataBuilder::default()
            .with_ohlcv(ohlcv_map)
            .with_trade(trade_map);

        let result = builder.collect_sorted_market_ids();

        // Both OHLCV and Trade have the same symbol, so only 1 MarketId
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].symbol, symbol);
    }

    #[test]
    fn returns_empty_when_no_price_authoritative_sources() {
        let builder = SimulationDataBuilder::default();

        let result = builder.collect_sorted_market_ids();

        assert!(result.is_empty());
    }

    // ============================================================================================
    // Postcard Serialization Roundtrip
    // ============================================================================================

    /// Creates a minimal EnvConfig for testing serialization.
    fn make_test_env_config() -> EnvConfig {
        // Minimal config - just needs to produce a consistent hash
        EnvConfig::default()
            .add_ohlcv_spot(
                DataSource::Rpc {
                    endpoint: crate::transport::source::Url::from("http://test:50051"),
                    api_key: None,
                },
                OhlcvSpotConfig {
                    broker: DataBroker::Binance,
                    symbol: Symbol::Spot(SpotPair::BtcUsdt),
                    exchange: Some(Exchange::Binance),
                    period: Period::Minute(1),
                    batch_size: 100,
                    indicators: vec![],
                },
            )
            .add_economic_calendar(
                DataSource::Rpc {
                    endpoint: crate::transport::source::Url::from("http://test:50051"),
                    api_key: None,
                },
                EconomicCalendarConfig {
                    broker: DataBroker::InvestingCom,
                    data_source: None,
                    country_code: Some(CountryCode::Us),
                    category: Some(EconomicCategory::Employment),
                    importance: None,
                    batch_size: 1000,
                },
            )
    }

    /// Creates SimulationData with some test OHLCV and economic calendar events.
    fn make_test_simulation_data(env_cfg: EnvConfig) -> SimulationData {
        let symbol = Symbol::Spot(SpotPair::BtcUsdt);
        let ohlcv_id = make_ohlcv_id(symbol);

        let ohlcv1 = make_ohlcv("2024-01-01T09:00:00Z", "2024-01-01T09:01:00Z");
        let ohlcv2 = make_ohlcv("2024-01-01T09:01:00Z", "2024-01-01T09:02:00Z");
        let ohlcv3 = make_ohlcv("2024-01-01T09:02:00Z", "2024-01-01T09:03:00Z");

        let mut ohlcv_map = OhlcvEventMap::new();
        ohlcv_map.insert(ohlcv_id, Box::new([ohlcv1, ohlcv2, ohlcv3]));

        // Add economic calendar events
        let eco_cal_id = make_economic_calendar_id();
        let eco_event1 = make_economic_event("2024-01-01T08:30:00Z");
        let eco_event2 = make_economic_event("2024-01-01T10:00:00Z");

        let mut eco_cal_map = EconomicCalEventMap::new();
        eco_cal_map.insert(eco_cal_id, Box::new([eco_event1, eco_event2]));

        SimulationDataBuilder::default()
            .with_ohlcv(ohlcv_map)
            .with_economic_news(eco_cal_map)
            .build(env_cfg)
            .expect("Failed to build SimulationData")
    }

    #[tokio::test]
    async fn file_based_roundtrip_succeeds() {
        // 1. Create test data
        let env_cfg = make_test_env_config();
        let sim_data = Arc::new(make_test_simulation_data(env_cfg.clone()));

        // 2. Set up temp directory for cache
        let temp_dir = std::env::temp_dir().join("chapaty_test_cache");
        std::fs::create_dir_all(&temp_dir).expect("Failed to create temp dir");

        let storage = StorageLocation::Local(&temp_dir);
        let buffer_size = 128 * 1024; // 128 KiB

        // 3. Write to file using SimulationData::write()
        sim_data
            .clone()
            .write(&storage, SerdeFormat::Postcard, buffer_size)
            .await
            .expect("write() failed");

        let hash = env_cfg.hash().expect("Failed to hash env config");
        let cache_path = temp_dir.join(format!("{hash}.postcard"));

        assert!(cache_path.exists(), "Cache file was not created");
        // let file_size = std::fs::metadata(&cache_path).expect("Failed to get file metadata").len();
        // println!("Cache file written: {} ({} bytes)", cache_path.display(), file_size);

        // 4. Read back using SimulationData::read()
        let loaded = SimulationData::read(&env_cfg, &storage, SerdeFormat::Postcard, buffer_size)
            .await
            .expect("read() failed");

        // 5. Verify data integrity
        assert_eq!(sim_data.market_ids().len(), loaded.market_ids().len());
        assert_eq!(sim_data.ohlcv().len(), loaded.ohlcv().len());
        assert_eq!(sim_data.economic_cal().len(), loaded.economic_cal().len());
        assert_eq!(sim_data.global_open_start(), loaded.global_open_start());
        assert_eq!(
            sim_data.global_availability_start(),
            loaded.global_availability_start()
        );
        // println!("File-based roundtrip succeeded (OHLCV + EconomicCalendar)");

        // 6. Cleanup
        std::fs::remove_file(&cache_path).expect("Failed to remove cache file");
        // Try to remove temp dir (may fail if not empty, that's ok)
        let _ = std::fs::remove_dir(&temp_dir);
        // println!("Cleanup complete");
    }
}
