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
- Test schreiben
- PPP Entry flexibel setzen, am besten über Struct
- PPP / Strategy Trait aufrümen
- Importe aufräumen
- Time Interval anpassen => Flexibler setzen: Wochentage, ganze Woche, Gar nicht und Zeitinterval für Wochentage oder ganze woche
- Performance <59sek
- Offset in Dollar angeben und dann umrechnen
- bot/metrics -> Effizienter bestimmen
- Time Frames umsetzen (siehe warning)
- StopLoss PrevHigh Namen verbessern, da verwirrend bzw. abgänig ob Long oder Short
- Prüfe, wenn bei SL PriceUponEntry gewählt, dass man keinen Unfug macht
- Data Provider aufräumen... Irgendwie komsch das die gar keinen attribute haben
- Bugfix siehe Zettel (und eigene Tests mit unterschiedlichen P&L Werten, code läuft manchmal auf Fehler)
- Warum ist die PnL eine andere, wenn man die Daten direkt lädt
- Volume Area LOW / HIGH berechnen und dann SL/TP Kriterien erweitern
- Was ist bei zwei POC's? -> Aktuell der kleinere (performt besser) aber flexibel setzen lassen können
- Volumen Profil Auch angeben wie man rundne soll VolumenProfile(Precision::{Genau, 1EUR, 10EUR, usw})
*/