use crate::{
    data::config::{
        ConfigId, EconomicCalendarConfig, OhlcvFutureConfig, OhlcvSpotConfig, TpoFutureConfig,
        TpoSpotConfig, TradeSpotConfig, VolumeProfileSpotConfig,
    },
    error::ChapatyResult,
    generated::chapaty::{
        bq_exporter::v1::{
            BaseMarketParams, EconomicCalendarRequest, EconomicCalendarResponse, EconomicCategory,
            EconomicImportance, OhlcvFutureRequest, OhlcvFutureResponse, OhlcvSpotRequest,
            OhlcvSpotResponse, ProfileAggregation, TpoFutureRequest, TpoFutureResponse,
            TpoSpotRequest, TpoSpotResponse, TradesSpotRequest, TradesSpotResponse,
            VolumeProfileSpotRequest, VolumeProfileSpotResponse,
        },
        data::v1::DataBroker,
    },
    transport::{
        codec::ProtoBatch,
        schema::{
            economic_calendar_schema, ohlcv_future_schema, ohlcv_spot_schema, tpo_future_schema,
            tpo_spot_schema, trades_spot_schema, volume_profile_spot_schema,
        },
        source::ChapatyClient,
    },
};
use polars::prelude::SchemaRef;
use std::fmt::Debug;
use tonic::async_trait;

/// Defines how a specific Config/Spec fetches its data.
#[async_trait]
pub trait Fetchable: ConfigId + Clone + Send + Sync + Debug + 'static {
    /// The Protobuf Response type (e.g., OhlcvSpotResponse)
    type Response: ProtoBatch + Send;
    /// The Protobuf Request type (e.g., OhlcvSpotRequest)
    type Request: Send;

    /// How to build the request for a specific year
    fn make_request(&self, year: i32) -> ChapatyResult<Self::Request>;

    fn schema_ref() -> SchemaRef;

    /// The specific gRPC call
    async fn fetch(
        client: &mut ChapatyClient,
        req: Self::Request,
    ) -> Result<tonic::Response<tonic::Streaming<Self::Response>>, tonic::Status>;
}

// ================================================================================================
// OHLCV Spot
// ================================================================================================

#[async_trait]
impl Fetchable for OhlcvSpotConfig {
    type Response = OhlcvSpotResponse;
    type Request = OhlcvSpotRequest;

    fn schema_ref() -> SchemaRef {
        ohlcv_spot_schema()
    }

    fn make_request(&self, year: i32) -> ChapatyResult<Self::Request> {
        Ok(OhlcvSpotRequest {
            params: Some(BaseMarketParams {
                data_broker: DataBroker::from(self.broker) as i32,
                symbol: self.symbol.to_string(),
                year,
                exchange: self
                    .exchange
                    .as_ref()
                    .map(|e| e.to_string())
                    .unwrap_or_default(),
                batch_size: self.batch_size,
            }),
            period: self.period.to_string(),
        })
    }

    async fn fetch(
        client: &mut ChapatyClient,
        req: Self::Request,
    ) -> Result<tonic::Response<tonic::Streaming<Self::Response>>, tonic::Status> {
        client.ohlcv_spot(req).await
    }
}

// ================================================================================================
// OHLCV Future
// ================================================================================================

#[async_trait]
impl Fetchable for OhlcvFutureConfig {
    type Response = OhlcvFutureResponse;
    type Request = OhlcvFutureRequest;

    fn schema_ref() -> SchemaRef {
        ohlcv_future_schema()
    }

    fn make_request(&self, year: i32) -> ChapatyResult<Self::Request> {
        Ok(OhlcvFutureRequest {
            params: Some(BaseMarketParams {
                data_broker: DataBroker::from(self.broker) as i32,
                symbol: self.symbol.to_string(),
                year,
                exchange: self
                    .exchange
                    .as_ref()
                    .map(|e| e.to_string())
                    .unwrap_or_default(),
                batch_size: self.batch_size,
            }),
            period: self.period.to_string(),
        })
    }

    async fn fetch(
        client: &mut ChapatyClient,
        req: Self::Request,
    ) -> Result<tonic::Response<tonic::Streaming<Self::Response>>, tonic::Status> {
        client.ohlcv_future(req).await
    }
}

// ================================================================================================
// Trade Spot
// ================================================================================================

#[async_trait]
impl Fetchable for TradeSpotConfig {
    type Response = TradesSpotResponse;
    type Request = TradesSpotRequest;

    fn schema_ref() -> SchemaRef {
        trades_spot_schema()
    }

    fn make_request(&self, year: i32) -> ChapatyResult<Self::Request> {
        Ok(TradesSpotRequest {
            params: Some(BaseMarketParams {
                data_broker: DataBroker::from(self.broker) as i32,
                symbol: self.symbol.to_string(),
                year,
                exchange: self
                    .exchange
                    .as_ref()
                    .map(|e| e.to_string())
                    .unwrap_or_default(),
                batch_size: self.batch_size,
            }),
        })
    }

    async fn fetch(
        client: &mut ChapatyClient,
        req: Self::Request,
    ) -> Result<tonic::Response<tonic::Streaming<Self::Response>>, tonic::Status> {
        client.trades_spot(req).await
    }
}

// ================================================================================================
// TPO Spot
// ================================================================================================

#[async_trait]
impl Fetchable for TpoSpotConfig {
    type Response = TpoSpotResponse;
    type Request = TpoSpotRequest;

    fn schema_ref() -> SchemaRef {
        tpo_spot_schema()
    }

    fn make_request(&self, year: i32) -> ChapatyResult<Self::Request> {
        let aggregation = self
            .aggregation
            .as_ref()
            .map(|agg| -> ChapatyResult<ProfileAggregation> {
                Ok(ProfileAggregation {
                    time_frame: agg
                        .time_frame
                        .as_ref()
                        .map(|tf| tf.to_string())
                        .unwrap_or_default(),
                    price_bin: agg.actual_price_bin_string(&self.symbol)?,
                })
            })
            .transpose()?;

        Ok(TpoSpotRequest {
            params: Some(BaseMarketParams {
                data_broker: DataBroker::from(self.broker) as i32,
                symbol: self.symbol.to_string(),
                year,
                exchange: self
                    .exchange
                    .as_ref()
                    .map(|e| e.to_string())
                    .unwrap_or_default(),
                batch_size: self.batch_size,
            }),
            aggregation,
        })
    }

    async fn fetch(
        client: &mut ChapatyClient,
        req: Self::Request,
    ) -> Result<tonic::Response<tonic::Streaming<Self::Response>>, tonic::Status> {
        client.tpo_spot(req).await
    }
}

// ================================================================================================
// TPO Future
// ================================================================================================

#[async_trait]
impl Fetchable for TpoFutureConfig {
    type Response = TpoFutureResponse;
    type Request = TpoFutureRequest;

    fn schema_ref() -> SchemaRef {
        tpo_future_schema()
    }

    fn make_request(&self, year: i32) -> ChapatyResult<Self::Request> {
        let aggregation = self
            .aggregation
            .as_ref()
            .map(|agg| -> ChapatyResult<ProfileAggregation> {
                Ok(ProfileAggregation {
                    time_frame: agg
                        .time_frame
                        .as_ref()
                        .map(|tf| tf.to_string())
                        .unwrap_or_default(),
                    price_bin: agg.actual_price_bin_string(&self.symbol)?,
                })
            })
            .transpose()?;

        Ok(TpoFutureRequest {
            params: Some(BaseMarketParams {
                data_broker: DataBroker::from(self.broker) as i32,
                symbol: self.symbol.to_string(),
                year,
                exchange: self
                    .exchange
                    .as_ref()
                    .map(|e| e.to_string())
                    .unwrap_or_default(),
                batch_size: self.batch_size,
            }),
            aggregation,
        })
    }

    async fn fetch(
        client: &mut ChapatyClient,
        req: Self::Request,
    ) -> Result<tonic::Response<tonic::Streaming<Self::Response>>, tonic::Status> {
        client.tpo_future(req).await
    }
}

// ================================================================================================
// Volume Profile Spot
// ================================================================================================

#[async_trait]
impl Fetchable for VolumeProfileSpotConfig {
    type Response = VolumeProfileSpotResponse;
    type Request = VolumeProfileSpotRequest;

    fn schema_ref() -> SchemaRef {
        volume_profile_spot_schema()
    }

    fn make_request(&self, year: i32) -> ChapatyResult<Self::Request> {
        let aggregation = self
            .aggregation
            .as_ref()
            .map(|agg| -> ChapatyResult<ProfileAggregation> {
                Ok(ProfileAggregation {
                    time_frame: agg
                        .time_frame
                        .as_ref()
                        .map(|tf| tf.to_string())
                        .unwrap_or_default(),
                    price_bin: agg.actual_price_bin_string(&self.symbol)?,
                })
            })
            .transpose()?;

        Ok(VolumeProfileSpotRequest {
            params: Some(BaseMarketParams {
                data_broker: DataBroker::from(self.broker) as i32,
                symbol: self.symbol.to_string(),
                year,
                exchange: self
                    .exchange
                    .as_ref()
                    .map(|e| e.to_string())
                    .unwrap_or_default(),
                batch_size: self.batch_size,
            }),
            aggregation,
        })
    }

    async fn fetch(
        client: &mut ChapatyClient,
        req: Self::Request,
    ) -> Result<tonic::Response<tonic::Streaming<Self::Response>>, tonic::Status> {
        client.volume_profile_spot(req).await
    }
}

// ================================================================================================
// Economic Calendar
// ================================================================================================

#[async_trait]
impl Fetchable for EconomicCalendarConfig {
    type Response = EconomicCalendarResponse;
    type Request = EconomicCalendarRequest;

    fn schema_ref() -> SchemaRef {
        economic_calendar_schema()
    }

    fn make_request(&self, year: i32) -> ChapatyResult<Self::Request> {
        Ok(EconomicCalendarRequest {
            data_broker: DataBroker::from(self.broker) as i32,
            year,
            data_source: self
                .data_source
                .map_or(String::default(), |ds| ds.to_string()),
            country_code: self
                .country_code
                .map_or(String::default(), |cc| cc.to_string()),
            category: self
                .category
                .map(|ec| EconomicCategory::from(ec) as i32)
                .unwrap_or(0),
            importance: self
                .importance
                .map(|ei| EconomicImportance::from(ei) as i32)
                .unwrap_or(0),
            batch_size: self.batch_size,
        })
    }

    async fn fetch(
        client: &mut ChapatyClient,
        req: Self::Request,
    ) -> Result<tonic::Response<tonic::Streaming<Self::Response>>, tonic::Status> {
        client.economic_calendar(req).await
    }
}
