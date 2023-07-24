use strum_macros::{Display, EnumString};

#[derive(Copy, Clone, Debug, Display, EnumString, PartialEq, Eq, Hash)]
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
    /// Returns the number of decimal places for the `market`.
    ///
    /// # Arguments
    /// * `market` - where we want to know the number of decimal places
    ///
    /// # Example
    /// * `MarketKind::BtcUsdt` for Binance has two decimal digits for cents, e.g. `1258.33`
    /// * `MarketKind::EurUsd` for Ninja has five decimal digits for ticks, e.g. `1.39455`
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

    /// This function returns the tik to dollar conversion factor for a market that uses tiks as units. Otherwise we return `None`.
    ///
    /// # Arguments
    /// * `market` - we want to get tik step size
    ///
    /// # Examples
    /// ```
    /// // BtcUsdt does not use tiks as unit
    /// assert_eq!(tik_to_dollar_conversion_factor(MarketKind::BtcUsdt).is_some(), false)
    /// // EurUsd uses tiks as unit
    /// assert_eq!(tik_to_dollar_conversion_factor(MarketKind::EurUsd).is_some(), true)
    /// ```
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
}