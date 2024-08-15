pub mod backtest_result;
mod bot;
mod calculator;
mod chapaty;
mod cloud_api;
pub mod config;
pub mod converter;
mod data_frame_operations;
pub mod data_provider;
mod enums;
pub mod equity_curve;
mod lazy_frame_operations;
pub mod performance_report;
pub mod pnl;
mod price_histogram;
mod serde;
pub mod strategy;
pub mod trade_breakdown_report;
mod trading_indicator;
mod types;

pub use bot::time_interval::TimeInterval;
pub use bot::{Bot, BotBuilder};
pub use enums::{
    bot::{StopLossKind, TakeProfitKind, TimeFrameKind},
    column_names::{
        DataProviderColumnKind, PerformanceReportColumnKind, PnLReportColumnKind,
        TradeBreakDownReportColumnKind,
    },
    data::MarketSimulationDataKind,
    error,
    indicator::{PriceHistogramKind, TradingIndicatorKind},
    markets::MarketKind,
    news::NewsKind,
};
pub use polars::prelude::DataFrame;

// "rust-analyzer.procMacro.server": "$(rustc --print sysroot)/lib/rustlib/src/rust/library/proc_macro/src/bridge/server.rs"
