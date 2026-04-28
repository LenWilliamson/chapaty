use std::time::Duration;

use serde::{Deserialize, Serialize};
use tonic::{
    Request, Status,
    metadata::MetadataValue,
    service::{Interceptor, interceptor::InterceptedService},
    transport::Channel,
};
use tracing::info;

use crate::{
    error::{ChapatyResult, TransportError},
    generated::chapaty::bq_exporter::v1::exporter_service_client::ExporterServiceClient,
    impl_from_primitive,
};

// Define the concrete type of your authenticated client
pub type ChapatyClient = ExporterServiceClient<InterceptedService<Channel, ApiKeyInterceptor>>;

/// Represents an API Endpoint URL.
///
/// This struct ensures that URLs are handled explicitly as API endpoints,
/// preventing confusion with generic strings.
///
/// # Examples
///
/// ```rust
/// use chapaty::prelude::*;
/// let url = EndpointUrl::from("https://api.example.com".to_string());
/// assert_eq!(url.0, "https://api.example.com");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EndpointUrl(pub String); // <-- RENAMED HERE
impl_from_primitive!(EndpointUrl, String);

impl From<&str> for EndpointUrl {
    fn from(value: &str) -> Self {
        EndpointUrl(value.to_string())
    }
}

/// Represents an API key for authentication.
///
/// This struct ensures that API keys are treated explicitly,
/// making function signatures more self-documenting.
///
/// # Examples
///
/// ```rust
/// use chapaty::prelude::*;
/// let key = ApiKey::from("my-secret-key".to_string());
/// assert_eq!(key.0, "my-secret-key");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ApiKey(pub String);
impl_from_primitive!(ApiKey, String);

impl From<&str> for ApiKey {
    fn from(value: &str) -> Self {
        ApiKey(value.to_string())
    }
}

// ================================================================================================
// Connect Trait
// ================================================================================================

/// Trait to establish a connection to a Chapaty-compatible gRPC endpoint.
///
/// Implementing this trait allows users to define custom connection logic,
/// such as custom TLS settings, timeouts, or load balancing configurations.
///
/// All implementors must return a [`ChapatyClient`], which wraps the gRPC
/// channel through an [`ApiKeyInterceptor`]. Implementations that don't use
/// API keys can pass `None` to the interceptor.
pub trait Connect {
    fn connect(&self) -> impl Future<Output = ChapatyResult<ChapatyClient>> + Send;
}

// ================================================================================================
// Data Sources
// ================================================================================================

/// Use Chapaty's hosted API.
///
/// Reads `CHAPATY_API_KEY` from environment variables.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostedApi;

impl Connect for HostedApi {
    #[tracing::instrument(skip(self), err)]
    async fn connect(&self) -> ChapatyResult<ChapatyClient> {
        let endpoint = "https://grpc.chapaty.com".to_string();
        let api_key = std::env::var("CHAPATY_API_KEY").ok().map(ApiKey);
        create_default_client(endpoint, api_key).await
    }
}

/// Use a custom RPC endpoint with default connection parameters.
///
/// For full control over channel configuration (TLS, timeouts, etc.),
/// implement the `Connect` trait directly on your own struct.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SelfHostedApi {
    pub endpoint: EndpointUrl,
    pub api_key: Option<ApiKey>,
}

impl Connect for SelfHostedApi {
    #[tracing::instrument(skip(self), fields(endpoint = %self.endpoint.0), err)]
    async fn connect(&self) -> ChapatyResult<ChapatyClient> {
        create_default_client(self.endpoint.0.clone(), self.api_key.clone()).await
    }
}

// ================================================================================================
// Source Group
// ================================================================================================

/// Configuration for connecting to a data source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum DataSource {
    /// Use Chapaty's hosted API.
    ///
    /// Reads `CHAPATY_API_KEY` from environment variables.
    #[default]
    Hosted,

    SelfHosted(SelfHostedApi),
}

impl Connect for DataSource {
    async fn connect(&self) -> ChapatyResult<ChapatyClient> {
        match self {
            DataSource::Hosted => HostedApi.connect().await,
            DataSource::SelfHosted(rpc) => rpc.connect().await,
        }
    }
}

/// Groups items to be fetched from the same data source.
///
/// `T` is the item type.
/// `S` is the connection source, defaulting to the standard `DataSource` enum.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceGroup<T, S: Connect = DataSource> {
    pub source: S,
    pub items: Vec<T>,
}

impl<T, S: Connect> SourceGroup<T, S> {
    pub fn new(source: S) -> Self {
        Self {
            source,
            items: Vec::new(),
        }
    }

    pub fn add(&mut self, item: T) {
        self.items.push(item);
    }
}

// ================================================================================================
// Default Client Builder
// ================================================================================================

/// Internal helper to create the opinionated default gRPC channel.
///
/// Custom implementations of [`Connect`] can ignore this and build their own.
async fn create_default_client(
    endpoint: String,
    api_key: Option<ApiKey>,
) -> ChapatyResult<ChapatyClient> {
    info!(%endpoint, has_api_key = api_key.is_some(), "Establishing gRPC connection");

    let channel = Channel::from_shared(endpoint.clone())
        .map_err(|_| TransportError::Connection("Invalid URI".into()))?
        // HTTP/2 keepalive: ping every 30s to keep connection alive
        .http2_keep_alive_interval(Duration::from_secs(30))
        // Timeout if no keepalive response within 10s
        .keep_alive_timeout(Duration::from_secs(10))
        // Allow keepalive pings even when there are no active streams
        .keep_alive_while_idle(true)
        // Overall connection timeout: 10 minutes for long-running operations
        // This is the timeout for individual RPC calls
        .timeout(Duration::from_secs(600))
        // TCP keepalive to detect broken connections at TCP level
        .tcp_keepalive(Some(Duration::from_secs(60)))
        // Connection timeout: how long to wait for initial connection
        .connect_timeout(Duration::from_secs(30))
        // Initial connection window size for flow control
        .initial_connection_window_size(Some(1024 * 1024)) // 1MB
        .initial_stream_window_size(Some(1024 * 1024)) // 1MB
        .connect()
        .await
        .map_err(|e| TransportError::Connection(e.to_string()))?;

    // Always create the interceptor (it might contain None)
    let interceptor = ApiKeyInterceptor::new(api_key);

    // Always use with_interceptor
    let client = ExporterServiceClient::with_interceptor(channel, interceptor);

    info!(%endpoint, "gRPC connection established with long-running configuration");
    Ok(client)
}

// ================================================================================================
// Interceptor
// ================================================================================================

/// Interceptor that adds API key to gRPC request metadata.
#[derive(Clone)]
pub struct ApiKeyInterceptor {
    api_key: Option<MetadataValue<tonic::metadata::Ascii>>,
}

impl ApiKeyInterceptor {
    pub fn new(api_key: Option<ApiKey>) -> Self {
        let metadata_value = api_key.map(|key| {
            key.0
                .parse()
                .expect("API key contains invalid characters for metadata")
        });

        Self {
            api_key: metadata_value,
        }
    }
}

impl Interceptor for ApiKeyInterceptor {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        if let Some(key) = &self.api_key {
            req.metadata_mut().insert("api-key", key.clone());
        }
        Ok(req)
    }
}
