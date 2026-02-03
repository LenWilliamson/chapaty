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
pub(super) type ChapatyClient =
    ExporterServiceClient<InterceptedService<Channel, ApiKeyInterceptor>>;

/// Represents a validated API URL.
///
/// This struct ensures that URLs are handled explicitly as API endpoints,
/// preventing confusion with generic strings.
///
/// # Examples
///
/// ```
/// # use chapaty::prelude::*;
/// let url = Url::from("https://api.example.com".to_string());
/// assert_eq!(url.0, "https://api.example.com");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Url(pub String);
impl_from_primitive!(Url, String);

impl From<&str> for Url {
    fn from(value: &str) -> Self {
        Url(value.to_string())
    }
}

/// Represents an API key for authentication.
///
/// This struct ensures that API keys are treated explicitly,
/// making function signatures more self-documenting.
///
/// # Examples
///
/// ```
/// # use chapaty::prelude::*;
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

/// Groups items to be fetched from the same data source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceGroup<T> {
    pub source: DataSource,
    pub items: Vec<T>,
}

impl<T> SourceGroup<T> {
    pub fn new(source: DataSource) -> Self {
        Self {
            source,
            items: Vec::new(),
        }
    }

    pub fn add(&mut self, item: T) {
        self.items.push(item);
    }
}

/// Configuration for connecting to a data source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum DataSource {
    /// Use Chapaty's hosted API.
    ///
    /// Reads `CHAPATY_API_KEY` from environment variables.
    #[default]
    Chapaty,

    /// Use a custom RPC endpoint.
    Rpc {
        endpoint: Url,
        api_key: Option<ApiKey>,
    },
}

impl DataSource {
    #[tracing::instrument(skip(self), fields(endpoint), err)]
    pub(crate) async fn connect(&self) -> ChapatyResult<ChapatyClient> {
        let (endpoint, api_key) = self.resolve_connection_params();

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
}

impl DataSource {
    /// Extract endpoint and API key from configuration.
    fn resolve_connection_params(&self) -> (String, Option<ApiKey>) {
        match self {
            Self::Chapaty => {
                let endpoint = "grpc.chapaty.com".to_string();
                let api_key = std::env::var("CHAPATY_API_KEY").ok().map(ApiKey);
                (endpoint, api_key)
            }
            Self::Rpc { endpoint, api_key } => (endpoint.0.clone(), api_key.clone()),
        }
    }
}

/// Interceptor that adds API key to gRPC request metadata.
#[derive(Clone)]
pub(crate) struct ApiKeyInterceptor {
    api_key: Option<MetadataValue<tonic::metadata::Ascii>>,
}

impl ApiKeyInterceptor {
    fn new(api_key: Option<ApiKey>) -> Self {
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
