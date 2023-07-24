mod backtest_result;
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
mod price_histogram;
mod serde;
pub mod strategy;
mod trading_indicator;

pub use bot::time_interval::TimeInterval;
pub use bot::BotBuilder;
pub use enums::{
    bot::{StopLossKind, TakeProfitKind, TimeFrameKind},
    data::MarketSimulationDataKind,
    markets::MarketKind,
};


/*
- PPP Entry flexibel setzen, am besten über Struct
- PPP / Strategy Trait aufrümen
- Offset in Dollar angeben und dann umrechnen
- Test schreiben
- Time Frames umsetzen (siehe warning)
- StopLoss PrevHigh Namen verbessern, da verwirrend bzw. abgänig ob Long oder Short
- Prüfe, wenn bei SL PriceUponEntry gewählt, dass man keinen Unfug macht
- Data Provider aufräumen... Irgendwie komsch das die gar keinen attribute haben
- Bugfix siehe Zettel

*/