use std::num::ParseIntError;

use indicatif::style::TemplateError;
use thiserror::Error;

pub type ChapatyResult<T> = Result<T, ChapatyError>;

#[derive(Debug, Error)]
pub enum ChapatyError {
    #[error(transparent)]
    Agent(#[from] AgentError),

    #[error(transparent)]
    Data(#[from] DataError),

    #[error(transparent)]
    Env(#[from] EnvError),

    #[error(transparent)]
    Io(#[from] IoError),

    #[error(transparent)]
    Transport(#[from] TransportError),

    #[error(transparent)]
    System(#[from] SystemError),
}

/// Errors occurring within Agent logic or execution.
#[derive(Debug, Error)]
pub enum AgentError {
    #[error("Agent logic error: {0}")]
    Logic(String),

    #[error("Invalid input to agent: {0}")]
    InvalidInput(String),

    #[error("Missing resource in agent: {0}")]
    MissingResource(String),

    #[error("Agent execution failure: {0}")]
    Execution(String),

    #[error("Uncategorized agent error: {0}")]
    Other(String),
}

/// Errors related to data loading, parsing, domain types, and availability.
#[derive(Debug, Error)]
pub enum DataError {
    #[error("Invalid OHLCV period length: {0}")]
    InvalidPeriodLength(String),

    #[error("Invalid OHLCV period time unit: {0}")]
    InvalidPeriodTimeUnit(String),

    #[error("Missing '{event}' news event for country '{country}': {msg}")]
    MissingNewsEvent {
        event: String,
        country: String,
        msg: String,
    },

    #[error("No events found: {0}")]
    NoEventsFound(String),

    #[error(
        "Causality violation: Found market open at {open} but no corresponding availability data. Stream ID: {stream}"
    )]
    CausalityViolation { open: String, stream: String },

    #[error("Invalid symbol string: '{0}'")]
    InvalidSymbol(String),

    #[error("Invalid news kind string: '{0}'")]
    InvalidNewsKind(String),

    #[error("Invalid group key value: '{0}'")]
    InvalidGroupKeyValue(String),

    #[error("Key not found: {0}")]
    KeyNotFound(String),

    #[error("Data frame error: {0}")]
    DataFrame(String),

    #[error("Failed timestamp conversion: {0}")]
    TimestampConversion(String),

    #[error("Failed to parse integer: {0}")]
    ParseInt(#[from] ParseIntError),

    #[error("Failed to parse float: {0}")]
    ParseFloat(#[from] std::num::ParseFloatError),

    #[error("Failed to parse enum: {0}")]
    ParseEnum(#[from] strum::ParseError),

    #[error("Unexpected enum variant: {0}")]
    UnexpectedEnumVariant(String),
}

/// Errors related to the Gym Environment configuration and execution loop.
#[derive(Debug, Error)]
pub enum EnvError {
    #[error("Environment was not built successfully (`Environment` is None)")]
    NotBuilt,

    #[error("Invalid environment state: {0}")]
    InvalidState(String),

    #[error("Missing episode length: {0}")]
    MissingEpisodeLength(String),

    #[error("Missing market context: {0}")]
    MissingMarketCtx(String),

    #[error("Missing API key: {0}")]
    MissingApiKey(String),

    #[error("Invalid trading window (start: {start}, end: {end}): {msg}")]
    InvalidTradingWindow { start: u8, end: u8, msg: String },

    #[error("Invalid risk metrics config: {0}")]
    InvalidRiskMetricsConfig(String),

    #[error("Invalid environment configuration: {0}")]
    InvalidConfig(String),

    #[error("Failed to encode EnvConfig")]
    Encoding(#[from] postcard::Error),

    #[error("Progress bar error")]
    ProgressBar(#[from] TemplateError),
}

/// Errors related to File I/O, Serialization, and Object Storage.
#[derive(Debug, Error)]
pub enum IoError {
    #[error("IO operation failed")]
    Io(#[from] std::io::Error),

    #[error("Serialization failed")]
    Json(#[from] serde_json::Error),

    #[error("File system error: {0}")]
    FileSystem(String),

    #[error("Failed to create writer: {0}")]
    WriterCreation(String),

    #[error("Failed to create reader: {0}")]
    ReaderCreation(String),

    #[error("Failed to write data: {0}")]
    WriteFailed(String),

    #[error("Failed to read data: {0}")]
    ReadFailed(String),

    #[error("Failed to read bytes: {0}")]
    ReadBytesFailed(String),

    #[error("Failed to build object store: {0}")]
    ObjectStoreBuild(String),

    #[error("Failed to build object path: {0}")]
    ObjectPathBuild(String),

    #[error("Unsupported file format: {0}")]
    UnsupportedFormat(String),
}

/// Errors related to Network Transport (gRPC, HTTP).
#[derive(Debug, Error)]
pub enum TransportError {
    #[error("Connection failed: {0}")]
    Connection(String),

    #[error("Stream error: {0}")]
    Stream(String),

    #[error("RPC type not found: '{0}'")]
    RpcTypeNotFound(String),
}

/// Errors related to internal system invariants, access control, and bugs.
#[derive(Debug, Error)]
pub enum SystemError {
    #[error("Access denied: {0}")]
    AccessDenied(String),

    #[error("System error: {0}")]
    Generic(String),

    #[error("Missing internal field: {0}")]
    MissingField(String),

    #[error("Index out of bounds: {0}")]
    IndexOutOfBounds(String),

    #[error("Invariant violation: {0}")]
    InvariantViolation(String),
}
