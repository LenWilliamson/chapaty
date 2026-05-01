// 1. Report, I/O & Transport
pub use crate::io::*;
pub use crate::report::io::*;
pub use crate::transport::source::*;

// 2. The Core "Loop", Agents & States
pub use crate::data::episode::*;
pub use crate::gym::trading::{
    Env, action::*, agent::*, config::*, env::*, observation::*, state::*, types::*,
};
pub use crate::gym::{
    AgentIdentifier, EnvStatus, GridAxis, InvalidActionPenalty, Reward, StepOutcome,
};

// 3. Financial Domain Types (Primitives & Classifications)
// Safely pulls in Price, Quantity, Tick, Volume, TradeId, SpotPair, etc.
pub use crate::data::domain::*;

// 4. Events & Views
// Pulls in Ohlcv, Trade, Tpo, MarketView, StreamView, ClosePriceProvider, etc.
pub use crate::data::event::*;
pub use crate::data::view::*;

// 5. Data Configurations & Filters
// Pulls in TechnicalAnalysis, FilterConfig, TradingWindow, OhlcvSpotConfig, etc.
pub use crate::data::common::*;
pub use crate::data::config::*;
pub use crate::data::filter::*;

// 6. Technical Indicators
// Automatically exposes StreamingSma, StreamingEma, StreamingRsi, StreamingIndicator, etc.
pub use crate::data::indicator::*;
pub use crate::math::indicator::*;

// 7. Errors
pub use crate::error::*;

// 8. Factories
pub use crate::gym::trading::{load, make};
