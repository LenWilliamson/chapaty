use serde::{Deserialize, Serialize};
use strum_macros::{Display, EnumString};

#[derive(Copy, Clone, Debug, Display, EnumString, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MarketKind {
    #[strum(serialize = "btcusdt")]
    BtcUsdt,

    #[strum(serialize = "6a")]
    AudUsdFuture,

    #[strum(serialize = "6e")]
    EurUsdFuture,

    #[strum(serialize = "6b")]
    GbpUsdFuture,

    #[strum(serialize = "6c")]
    CadUsdFuture,

    #[strum(serialize = "6j")]
    YenUsdFuture,

    #[strum(serialize = "6n")]
    NzdUsdFuture,

    #[strum(serialize = "6btc")]
    BtcUsdFuture,
}

impl MarketKind {
    pub fn decimal_places(&self) -> i32 {
        match self {
            MarketKind::BtcUsdt => 2,
            MarketKind::EurUsdFuture => 5,
            MarketKind::AudUsdFuture => 5,
            MarketKind::GbpUsdFuture => 4,
            MarketKind::CadUsdFuture => 5,
            MarketKind::YenUsdFuture => 7,
            MarketKind::NzdUsdFuture => 5,
            MarketKind::BtcUsdFuture => 0,
        }
    }

    pub fn tick_step_size(&self) -> Option<f64> {
        match self {
            MarketKind::BtcUsdt => None,
            MarketKind::EurUsdFuture => Some(0.00005),
            MarketKind::AudUsdFuture => Some(0.00005),
            MarketKind::GbpUsdFuture => Some(0.0001),
            MarketKind::CadUsdFuture => Some(0.00005),
            MarketKind::YenUsdFuture => Some(0.0000005),
            MarketKind::NzdUsdFuture => Some(0.00005),
            MarketKind::BtcUsdFuture => Some(5.0),
        }
    }

    pub fn tik_to_dollar_conversion_factor(&self) -> Option<f64> {
        match self {
            MarketKind::BtcUsdt => None,
            MarketKind::EurUsdFuture => Some(6.25),
            MarketKind::AudUsdFuture => Some(5.0),
            MarketKind::GbpUsdFuture => Some(6.25),
            MarketKind::CadUsdFuture => Some(5.0),
            MarketKind::YenUsdFuture => Some(6.25),
            MarketKind::NzdUsdFuture => Some(5.0),
            MarketKind::BtcUsdFuture => Some(25.0),
        }
    }

    pub fn try_offset_in_tick(&self, offset: f64) -> f64 {
        match self {
            MarketKind::BtcUsdt => offset,
            MarketKind::EurUsdFuture => offset / 6.25 * self.tick_step_size().unwrap(),
            MarketKind::AudUsdFuture => offset / 5.0 * self.tick_step_size().unwrap(),
            MarketKind::GbpUsdFuture => offset / 6.25 * self.tick_step_size().unwrap(),
            MarketKind::CadUsdFuture => offset / 5.0 * self.tick_step_size().unwrap(),
            MarketKind::YenUsdFuture => offset / 6.25 * self.tick_step_size().unwrap(),
            MarketKind::NzdUsdFuture => offset / 5.0 * self.tick_step_size().unwrap(),
            MarketKind::BtcUsdFuture => offset / 25.0 * self.tick_step_size().unwrap(),
        }
    }
}
