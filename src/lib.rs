/// The current version of the chapaty crate
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

// === Public Modules (The Canonical Paths) ===
pub mod data;
pub mod error;
pub mod gym;
pub mod math;
pub mod report;

// === Private Implementation Details ===
mod generated;
mod io;
mod macros;
mod sim;
mod transport;

// === Facades (Re-exporting internals) ===
// Expose specific IO items without making the whole module public
pub use crate::io::{SerdeFormat, StorageLocation};

// Expose specific Transport items
pub use crate::transport::source::{
    ApiKey, DataSource, EndpointUrl, HostedApi, SelfHostedApi, SourceGroup,
};

// === Convenience ===
pub mod prelude;
pub mod sorted_vec_map;
pub use crate::gym::trading::factory::{load, make};
