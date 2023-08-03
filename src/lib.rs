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
- Volume Area LOW / HIGH berechnen und dann SL/TP Kriterien erweitern
- PPP Entry flexibel setzen, am besten über Struct
- PPP / Strategy Trait aufrümen
- Time Interval anpassen => Flexibler setzen: Wochentage, ganze Woche, Gar nicht und Zeitinterval für Wochentage oder ganze woche
- Offset in Dollar angeben und dann umrechnen
- bot/metrics -> Effizienter bestimmen &&& TODO compute for all_years and all_markets & make parallel
- Time Frames umsetzen
- StopLoss PrevHigh Namen verbessern, da verwirrend bzw. abgänig ob Long oder Short
- Data Provider aufräumen... Irgendwie komsch das die gar keinen attribute haben

- Prüfe, wenn bei SL PriceUponEntry gewählt, dass man keinen Unfug macht
- Bugfix siehe Zettel (und eigene Tests mit unterschiedlichen P&L Werten, code läuft manchmal auf Fehler)
    - CRV NULL wenn PrevHigh -0.002 bei 6E => Was passiert wenn SL unterhalb//oberhalb des Entry?
    - Bei TP/SL PrevMax ist number timeout winner größer als number winner
- Warum ist die PnL eine andere, wenn man die Daten direkt lädt
- Was ist bei zwei POC's? -> Aktuell der kleinere (performt besser) aber flexibel setzen lassen können
- Volumen Profil Auch angeben wie man rundne soll VolumenProfile(Precision::{Genau, 1EUR, 10EUR, usw})
*/
