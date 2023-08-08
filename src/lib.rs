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
    column_names::DataProviderColumnKind,
    indicator::{PriceHistogramKind, TradingIndicatorKind},
    markets::MarketKind,
};

/*
- Offset in Dollar angeben und dann umrechnen
- Initial Balance von Trade Day bekommen
- Time Interval anpassen => Flexibler setzen: Wochentage, ganze Woche, Gar nicht und Zeitinterval für Wochentage oder ganze woche
- Time Frames umsetzen
- bot/metrics -> Effizienter bestimmen &&& TODO compute for all_years and all_markets & make parallel &&&
- Fehler in PnL Berechnung fixen (Wann ist TradeDirection::None)
- Warum ist die PnL eine andere, wenn man die Daten direkt lädt
- MERGE IN MASTER + UPLOAD CRATE with License
- Prüfe, wenn bei SL PriceUponEntry gewählt, dass man keinen Unfug macht
- Bugfix siehe Zettel (und eigene Tests mit unterschiedlichen P&L Werten, code läuft manchmal auf Fehler)
    - CRV NULL wenn PrevHigh -0.002 bei 6E => Was passiert wenn SL unterhalb//oberhalb des Entry?
    - Bei TP/SL PrevMax ist number timeout winner größer als number winner
- MERGE IN MASTER + UPLOAD CRATE with License

- Better error handling when using PreTradeValues which were not registered in Strategy::get_required_pre_trade_vales
- Was ist bei zwei POC's? -> Aktuell der kleinere (performt besser) aber flexibel setzen lassen können
*/
