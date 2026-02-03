use crate::error::{ChapatyResult, EnvError};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use strum::{Display, EnumString};

/// Configuration for filtering market data based on time and economic events.
///
/// # Usage
/// This struct uses `Option` to represent active filters. If a field is `None`,
/// that specific filter is **disabled**, meaning all data passes through.
///
/// # Example
/// ```
/// # use std::collections::BTreeMap;
/// # use chapaty::prelude::*;
/// let config = FilterConfig {
///     // 1. Restrict to specific years
///     allowed_years: Some(vec![2020, 2021, 2022].into_iter().collect()),
///     
///     // 2. Define trading hours (e.g., Mon-Fri, 9am - 5pm)
///     allowed_trading_hours: Some(BTreeMap::from([
///         (Weekday::Monday, vec![TradingWindow::new(9, 17).unwrap()]),
///         (Weekday::Tuesday, vec![TradingWindow::new(9, 17).unwrap()]),
///     ])),
///
///     // 3. Default economic policy (Unrestricted)
///     economic_news_policy: None,
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FilterConfig {
    /// Policy for trading around economic news events.
    ///
    /// Defaults to `Unrestricted` (all days allowed) if `None`.
    pub economic_news_policy: Option<EconomicCalendarPolicy>,

    /// Allowlist of calendar years (e.g., 2023).
    ///
    /// - `None`: All years are permitted.
    /// - `Some(years)`: Only data from these years is retained.
    pub allowed_years: Option<BTreeSet<u16>>,

    /// Allowlist of trading hours by weekday.
    ///
    /// - `None`: Trading is unrestricted by time of day (24/7).
    /// - `Some(map)`: Trading is only allowed during the specified windows for the specified days.
    ///   Days missing from the map will have **no allowed trading hours**.
    pub allowed_trading_hours: Option<BTreeMap<Weekday, Vec<TradingWindow>>>,
}

impl FilterConfig {
    /// Returns true if no filters are active (all data allowed).
    pub fn is_unrestricted(&self) -> bool {
        self.economic_news_policy.is_none()
            && self.allowed_years.is_none()
            && self.allowed_trading_hours.is_none()
    }
}

/// Defines how the environment filters trading days based on the Economic Calendar.
///
/// This policy controls which simulation timeframes (e.g., Days, Weeks) are eligible for training
/// based on the presence or absence of economic events (e.g., NFP, CPI releases).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize, Display, EnumString,
)]
#[strum(serialize_all = "snake_case")]
pub enum EconomicCalendarPolicy {
    /// **Default.** No calendar-based filtering is applied.
    ///
    /// Trading is allowed during all valid market hours, regardless of whether economic
    /// events are occurring. The agent sees all days.
    #[default]
    Unrestricted,

    /// **Volatility Seeking.** Restrict trading *only* to timeframes containing economic events.
    ///
    /// Use this to train agents specifically on how to handle high-volatility news events
    /// (e.g., only train on days where "Non-Farm Payrolls" or "CPI" are released).
    /// Timeframes without events are dropped.
    OnlyWithEvents,

    /// **Volatility Avoidance.** Restrict trading to timeframes *without* economic events.
    ///
    /// Use this to train agents that operate in "normal" market conditions and should
    /// sit out during high-risk/unpredictable news releases.
    /// Timeframes containing events are dropped.
    ExcludeEvents,
}

impl EconomicCalendarPolicy {
    pub fn is_unrestricted(&self) -> bool {
        matches!(self, Self::Unrestricted)
    }

    pub fn is_only_with_events(&self) -> bool {
        matches!(self, Self::OnlyWithEvents)
    }

    pub fn is_exclude_events(&self) -> bool {
        matches!(self, Self::ExcludeEvents)
    }
}

/// A specific **UTC** time interval within a day where trading is allowed.
///
/// # Semantics
/// The interval is **half-open**: `[start, end)`.
/// * `start` is inclusive (0-23).
/// * `end` is exclusive (1-24).
///
/// # MVP Constraints
/// * **UTC Only:** Users must manually convert local times to UTC.
/// * **No Wrapping:** Windows cannot wrap around midnight (e.g., 22:00 to 02:00 is invalid).
///   To define an overnight session, define two windows: `[22, 24)` on Day A and `[0, 2)` on Day B.
///
/// # Example
/// `start=9, end=17` means 09:00:00 UTC up to (but not including) 17:00:00 UTC.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TradingWindow {
    /// Start hour (0-23).
    start: u8,
    /// End hour (1-24).
    end: u8,
}

impl TradingWindow {
    /// Creates a new UTC trading window.
    ///
    /// # Errors
    /// Returns an error if:
    /// - `start` is not in `0..=23`.
    /// - `end` is not in `1..=24`.
    /// - `start >= end` (Invalid duration or midnight wrapping).
    pub fn new(start: u8, end: u8) -> ChapatyResult<Self> {
        if start > 23 {
            return Err(EnvError::InvalidTradingWindow {
                start,
                end,
                msg: "start hour must be in the range 0-23".to_string(),
            }
            .into());
        }

        if end == 0 || end > 24 {
            return Err(EnvError::InvalidTradingWindow {
                start,
                end,
                msg: "end hour must be in the range 1-24".to_string(),
            }
            .into());
        }

        if start >= end {
            return Err(EnvError::InvalidTradingWindow {
                start,
                end,
                msg:
                    "start hour must be strictly less than end hour (wrapping not supported in MVP)"
                        .to_string(),
            }
            .into());
        }

        Ok(Self { start, end })
    }

    /// Helper for the full 24-hour day [0, 24).
    pub fn full_day() -> Self {
        Self { start: 0, end: 24 }
    }

    /// Returns the inclusive start hour (UTC).
    pub fn start(&self) -> u8 {
        self.start
    }

    /// Returns the exclusive end hour (UTC).
    pub fn end(&self) -> u8 {
        self.end
    }
}

/// Days of the week.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Display,
    EnumString,
    PartialOrd,
    Ord,
)]
#[strum(serialize_all = "snake_case")]
pub enum Weekday {
    Monday,
    Tuesday,
    Wednesday,
    Thursday,
    Friday,
    Saturday,
    Sunday,
}

impl From<chrono::Weekday> for Weekday {
    fn from(weekday: chrono::Weekday) -> Self {
        match weekday {
            chrono::Weekday::Mon => Weekday::Monday,
            chrono::Weekday::Tue => Weekday::Tuesday,
            chrono::Weekday::Wed => Weekday::Wednesday,
            chrono::Weekday::Thu => Weekday::Thursday,
            chrono::Weekday::Fri => Weekday::Friday,
            chrono::Weekday::Sat => Weekday::Saturday,
            chrono::Weekday::Sun => Weekday::Sunday,
        }
    }
}

impl From<Weekday> for chrono::Weekday {
    fn from(weekday: Weekday) -> Self {
        match weekday {
            Weekday::Monday => chrono::Weekday::Mon,
            Weekday::Tuesday => chrono::Weekday::Tue,
            Weekday::Wednesday => chrono::Weekday::Wed,
            Weekday::Thursday => chrono::Weekday::Thu,
            Weekday::Friday => chrono::Weekday::Fri,
            Weekday::Saturday => chrono::Weekday::Sat,
            Weekday::Sunday => chrono::Weekday::Sun,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_config_unrestricted_logic() {
        let mut config = FilterConfig::default();
        assert!(
            config.is_unrestricted(),
            "Default config should be unrestricted"
        );

        config.economic_news_policy = Some(EconomicCalendarPolicy::OnlyWithEvents);
        assert!(
            !config.is_unrestricted(),
            "Config with policy should not be unrestricted"
        );

        let mut config = FilterConfig::default();
        config.allowed_years = Some(BTreeSet::from([2023]));
        assert!(
            !config.is_unrestricted(),
            "Config with years should not be unrestricted"
        );

        let mut config = FilterConfig::default();
        config.allowed_trading_hours = Some(BTreeMap::from([(Weekday::Monday, vec![])]));
        assert!(
            !config.is_unrestricted(),
            "Config with hours should not be unrestricted"
        );
    }

    #[test]
    fn test_economic_default() {
        assert_eq!(
            EconomicCalendarPolicy::default(),
            EconomicCalendarPolicy::Unrestricted
        );
    }

    #[test]
    fn test_economic_policy_helpers() {
        let policy = EconomicCalendarPolicy::Unrestricted;
        assert!(policy.is_unrestricted());
        assert!(!policy.is_only_with_events());
        assert!(!policy.is_exclude_events());

        let policy = EconomicCalendarPolicy::OnlyWithEvents;
        assert!(!policy.is_unrestricted());
        assert!(policy.is_only_with_events());
        assert!(!policy.is_exclude_events());

        let policy = EconomicCalendarPolicy::ExcludeEvents;
        assert!(!policy.is_unrestricted());
        assert!(!policy.is_only_with_events());
        assert!(policy.is_exclude_events());
    }

    #[test]
    fn test_trading_window_validation() {
        // Valid cases
        assert!(TradingWindow::new(0, 1).is_ok());
        assert!(TradingWindow::new(9, 17).is_ok());
        assert!(TradingWindow::new(23, 24).is_ok());

        // Invalid start
        assert!(TradingWindow::new(24, 25).is_err());

        // Invalid end
        assert!(TradingWindow::new(9, 0).is_err());
        assert!(TradingWindow::new(9, 25).is_err());

        // Invalid range
        assert!(TradingWindow::new(10, 10).is_err());
        assert!(TradingWindow::new(17, 9).is_err());
    }

    #[test]
    fn test_trading_window_full_day() {
        let window = TradingWindow::full_day();
        assert_eq!(window.start, 0);
        assert_eq!(window.end, 24);
    }

    #[test]
    fn test_weekday_chrono_conversion() {
        let days = vec![
            (Weekday::Monday, chrono::Weekday::Mon),
            (Weekday::Tuesday, chrono::Weekday::Tue),
            (Weekday::Wednesday, chrono::Weekday::Wed),
            (Weekday::Thursday, chrono::Weekday::Thu),
            (Weekday::Friday, chrono::Weekday::Fri),
            (Weekday::Saturday, chrono::Weekday::Sat),
            (Weekday::Sunday, chrono::Weekday::Sun),
        ];

        for (w, c) in days {
            assert_eq!(chrono::Weekday::from(w), c);
            assert_eq!(Weekday::from(c), w);
        }
    }
}
