pub mod backtest_result;
mod bot;
mod calculator;
mod chapaty;
mod cloud_api;
pub mod config;
mod converter;
mod data_frame_operations;
pub mod data_provider;
mod enums;
mod lazy_frame_operations;
pub mod performance_report;
pub mod pnl;
mod price_histogram;
mod serde;
pub mod equity_curve;
pub mod strategy;
pub mod trade_breakdown_report;
mod trading_indicator;

pub use bot::time_interval::TimeInterval;
pub use bot::{BotBuilder, Bot};
pub use enums::{
    bot::{StopLossKind, TakeProfitKind, TimeFrameKind},
    column_names::{DataProviderColumnKind, PnLReportColumnKind, PerformanceReportColumnKind, TradeBreakDownReportColumnKind},
    data::MarketSimulationDataKind,
    indicator::{PriceHistogramKind, TradingIndicatorKind},
    markets::MarketKind,
};
pub use polars::prelude::DataFrame;