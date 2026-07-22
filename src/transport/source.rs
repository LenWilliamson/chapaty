use std::time::Duration;

use serde::{Deserialize, Serialize};
use tonic::{
    Request, Status, async_trait,
    metadata::{MetadataKey, MetadataValue},
    service::{Interceptor, interceptor::InterceptedService},
    transport::Channel,
};
use tracing::info;

use crate::{
    error::{ChapatyResult, TransportError},
    generated::chapaty::bq_exporter::v1::exporter_service_client::ExporterServiceClient,
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
/// # use chapaty::prelude::*;
/// let url = EndpointUrl::from("https://api.example.com".to_string());
/// assert_eq!(url.0, "https://api.example.com");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EndpointUrl(pub String); // <-- RENAMED HERE
impl_from_primitive!(EndpointUrl, String);

impl From<&str> for EndpointUrl {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

/// Represents a credential for authentication.
///
/// This struct ensures that credentials are treated explicitly,
/// making function signatures more self-documenting.
///
/// # Examples
///
/// ```rust
/// # use chapaty::prelude::*;
/// let key = Credential::from("my-secret-key".to_string());
/// assert_eq!(key.0, "my-secret-key");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Credential(pub String);
impl_from_primitive!(Credential, String);

impl From<&str> for Credential {
    fn from(value: &str) -> Self {
        Self(value.to_string())
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
#[async_trait]
pub trait Connect {
    async fn connect(&self) -> ChapatyResult<ChapatyClient>;
}

// ================================================================================================
// Data Sources
// ================================================================================================

/// Use Chapaty's hosted API.
///
/// Reads `CHAPATY_BQEXPORTER_URL`, `CHAPATY_METADATA_KEY`, and
/// `CHAPATY_CREDENTIAL` from environment variables.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostedApi;

#[async_trait]
impl Connect for HostedApi {
    #[tracing::instrument(skip(self), err)]
    async fn connect(&self) -> ChapatyResult<ChapatyClient> {
        let endpoint = std::env::var("CHAPATY_BQEXPORTER_URL")
            .unwrap_or_else(|_| "https://bqexporter.chapaty.com".to_string());
        let metadata_key = std::env::var("CHAPATY_METADATA_KEY").ok();
        let credential = std::env::var("CHAPATY_CREDENTIAL").ok().map(Credential);
        create_default_client(endpoint, metadata_key, credential).await
    }
}

/// Configuration for connecting to a self-hosted gRPC endpoint using default
/// settings.
///
/// Uses the SDK's opinionated gRPC channel configuration (30s HTTP/2 keepalive,
/// 10m RPC timeouts, 1MB window sizes).
///
/// # Custom Channel Logic
/// If you need full control over channel parameters (custom TLS certificates,
/// dynamic proxies, custom interceptors), do **not** use this struct. Instead,
/// implement the [`Connect`] trait directly on your own custom struct.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DefaultGrpcEndpoint {
    /// The gRPC endpoint URL (e.g., `"https://grpc.my-company.internal:50051"`).
    pub endpoint: EndpointUrl,

    /// Optional credential (API key or auth token) sent with each gRPC request.
    pub credential: Option<Credential>,

    /// Custom metadata header key used to transmit the credential.
    ///
    /// Must be a valid gRPC metadata key:
    /// - **ASCII characters only**
    /// - **Lowercase only** (e.g., `"x-api-key"`, not `"X-API-Key"`)
    /// - Alphanumeric characters, hyphens (`-`), or underscores (`_`)
    ///
    /// Defaults to `"api-key"` if omitted or if an invalid string is provided.
    pub metadata_key: Option<String>,
}

#[async_trait]
impl Connect for DefaultGrpcEndpoint {
    #[tracing::instrument(skip(self), fields(endpoint = %self.endpoint.0), err)]
    async fn connect(&self) -> ChapatyResult<ChapatyClient> {
        create_default_client(
            self.endpoint.0.clone(),
            self.metadata_key.clone(),
            self.credential.clone(),
        )
        .await
    }
}

// ================================================================================================
// Source Group
// ================================================================================================

/// Configuration for connecting to a data source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Default)]
pub enum DataSource {
    /// Use Chapaty's hosted API.
    ///
    /// Reads `CHAPATY_API_KEY` from environment variables.
    #[default]
    Hosted,
    SelfHosted(DefaultGrpcEndpoint),
}

#[async_trait]
impl Connect for DataSource {
    async fn connect(&self) -> ChapatyResult<ChapatyClient> {
        match self {
            Self::Hosted => HostedApi.connect().await,
            Self::SelfHosted(rpc) => rpc.connect().await,
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
    pub const fn new(source: S) -> Self {
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
    metadata_key: Option<String>,
    credential: Option<Credential>,
) -> ChapatyResult<ChapatyClient> {
    info!(%endpoint, has_api_key = credential.is_some(), "Establishing gRPC connection");

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
        .timeout(Duration::from_mins(10))
        // TCP keepalive to detect broken connections at TCP level
        .tcp_keepalive(Some(Duration::from_mins(1)))
        // Connection timeout: how long to wait for initial connection
        .connect_timeout(Duration::from_secs(30))
        // Initial connection window size for flow control
        .initial_connection_window_size(Some(1024 * 1024)) // 1MB
        .initial_stream_window_size(Some(1024 * 1024)) // 1MB
        .connect()
        .await
        .map_err(|e| TransportError::Connection(e.to_string()))?;

    // Always create the interceptor (it might contain None)
    let interceptor = ApiKeyInterceptor::new(metadata_key, credential);

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
    metadata_key: MetadataKey<tonic::metadata::Ascii>,
    metadata_value: Option<MetadataValue<tonic::metadata::Ascii>>,
}

impl ApiKeyInterceptor {
    /// Creates an interceptor that injects the optional API key into request
    /// metadata.
    ///
    /// # Metadata Key Rules
    /// The `metadata_key` must be a valid lowercased ASCII header name (e.g.,
    /// `"x-api-key"`). If `None` is provided, or if the string fails to
    /// parse (e.g. contains uppercase characters, non-ASCII, or spaces), it
    /// defaults to `"api-key"`.
    ///
    /// # Panics
    /// Panics if the provided API key contains non-ASCII characters or control
    /// characters that cannot be parsed into ASCII metadata values.
    #[must_use]
    #[expect(
        clippy::expect_used,
        reason = "a non-ascii token API key is a configuration error. Failing fast here surfaces it immediately at setup"
    )]
    pub fn new(metadata_key: Option<String>, credential: Option<Credential>) -> Self {
        let metadata_key = metadata_key
            .and_then(|key| key.parse().ok())
            .unwrap_or_else(|| MetadataKey::from_static("api-key"));

        let metadata_value = credential.map(|value| {
            value
                .0
                .parse()
                .expect("API key contains invalid characters for metadata")
        });

        Self {
            metadata_key,
            metadata_value,
        }
    }
}

impl Interceptor for ApiKeyInterceptor {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        if let Some(key) = &self.metadata_value {
            req.metadata_mut()
                .insert(self.metadata_key.clone(), key.clone());
        }
        Ok(req)
    }
}
