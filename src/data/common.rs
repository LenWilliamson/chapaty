use rust_decimal::{
    Decimal,
    prelude::{FromPrimitive, ToPrimitive},
};
use serde::{Deserialize, Serialize};

use crate::{
    data::domain::{Instrument, Period, Price},
    error::{ChapatyResult, EnvError, SystemError},
};

// ================================================================================================
// Market Profile Aggregation Configuration
// ================================================================================================

/// Rules for expanding the Value Area (VA) from the Point of Control (POC).
///
/// The Value Area represents the price range containing a specific percentage
/// (usually 70%) of the total volume/TPOs.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default,
)]
pub enum ValueAreaRule {
    /// Standard Market Profile algorithm (Steidlmayer).
    ///
    /// - **Logic:** "Greedy" expansion. At each step, compare the volume of the
    ///   price bin immediately above vs. immediately below the current range.
    ///   Include the neighbor with the **higher** volume.
    /// - **Tie-Breaker:** If volumes are equal, favor the **Higher** price (Up/Resistance).
    #[default]
    HighestVolume,

    /// Same as `HighestVolume`, but resolves ties differently.
    ///
    /// - **Logic:** Greedy expansion.
    /// - **Tie-Breaker:** If volumes are equal, favor the **Lower** price (Down/Support).
    HighestVolumePreferLower,

    /// Expands strictly by price proximity, ignoring volume density.
    ///
    /// - **Logic:** Expands one tick Up and one tick Down simultaneously at each step,
    ///   maintaining a symmetric range around the POC (as much as data allows).
    /// - **Use Case:** When assuming a normal (Gaussian) distribution or when
    ///   volume data is sparse/unreliable (e.g., TPO on low liquidity).
    Symmetric,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default,
)]
pub enum PocRule {
    /// Selects the lowest price level among the maxima.
    ///
    /// Standard behavior for many platforms. Conservative for finding support.
    #[default]
    LowestPrice,

    /// Selects the highest price level among the maxima.
    ///
    /// Conservative for finding resistance.
    HighestPrice,

    /// Selects the price level closest to the arithmetic mean of all maxima.
    ///
    /// Attempts to find the "center of gravity" of the high-volume node.
    ClosestToCenter,
}

/// Aggregation parameters for profile-based market data (TPO and Volume Profile).
///
/// Profile data aggregates price and volume information into price levels,
/// providing insights into market structure and trader behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ProfileAggregation {
    /// The time period for profile aggregation (e.g., "5m", "1h", "1d").
    ///
    /// If `None`, defaults to "1m" (one minute).
    pub time_frame: Option<Period>,

    /// The size of price bins expressed as a multiple of the instrument's tick size.
    ///
    /// Instead of a raw float, this defines the bin size in "ticks".
    /// This guarantees strict ordering (`Ord`) and hashing (`Hash`).
    ///
    /// # Logic
    /// `Bin Size = ticks_per_bin * Symbol::price_increment()`
    ///
    /// # Examples
    /// - `1`: The bin size is exactly 1 tick (e.g., 0.01 for btc-usdt, 0.00005 for 6e Future).
    /// - `10`: The bin size is 10 ticks.
    /// - `100`: The bin size is 100 ticks.
    ///
    /// If `None`, defaults to `1` (the finest available granularity).
    pub ticks_per_bin: Option<u32>,

    /// The Value Area threshold in **Basis Points** (bps).
    ///
    /// - `10000` = 100%
    /// - `7000`  = 70% (Default)
    /// - `100`   = 1%
    pub value_area_bps: Option<u16>,

    /// How to determine the POC in case of a tie.
    /// Defaults to `PocRule::LowestPrice`.
    pub poc_rule: Option<PocRule>,

    /// How to expand the Value Area from the POC.
    /// Defaults to `ValueAreaRule::HighestVolume`.
    pub value_area_rule: Option<ValueAreaRule>,
}

impl Default for ProfileAggregation {
    fn default() -> Self {
        Self {
            time_frame: Some(Period::Minute(1)),
            ticks_per_bin: Some(1),
            // Default to 70% (7000 bps)
            value_area_bps: Some(7000),
            poc_rule: Some(PocRule::default()),
            value_area_rule: Some(ValueAreaRule::default()),
        }
    }
}

impl ProfileAggregation {
    /// Returns the size of the bin in quote currency as a mathematically exact string.
    pub fn actual_price_bin_string<I: Instrument>(&self, instrument: &I) -> ChapatyResult<String> {
        self.calculate_bin_decimal(instrument)
            .map(|d| d.normalize().to_string())
    }

    /// Returns the size of the bin in quote currency as an f64.
    ///
    /// # Errors
    /// Returns `SystemError::InvariantViolation` if the calculated decimal cannot be
    /// represented as an f64 (e.g. overflow), which implies corrupted inputs.
    pub fn actual_price_bin<I: Instrument>(&self, instrument: &I) -> ChapatyResult<f64> {
        self.calculate_bin_decimal(instrument)?
            .to_f64()
            .ok_or_else(|| {
                SystemError::InvariantViolation(
                    "Calculated price bin Decimal is too large or invalid to convert to f64"
                        .to_string(),
                )
                .into()
            })
    }

    /// Helper to get the value area as a normalized float (e.g., 0.70).
    pub fn value_area_pct(&self) -> f64 {
        let bps = self.value_area_bps.unwrap_or(7000);
        f64::from(bps) / 10_000.0
    }
}

impl ProfileAggregation {
    /// Internal helper: Calculates bin size as Decimal.
    /// Fails if the instrument tick size is NaN or Infinity.
    fn calculate_bin_decimal<I: Instrument>(&self, instrument: &I) -> ChapatyResult<Decimal> {
        let multiplier = self.ticks_per_bin.unwrap_or(1);
        let tick_size = instrument.tick_size();

        let tick_dec = Decimal::from_f64(tick_size).ok_or_else(|| {
            SystemError::InvariantViolation(format!(
                "Instrument tick size is invalid (NaN or Infinity): {}",
                tick_size
            ))
        })?;

        Ok(tick_dec * Decimal::from(multiplier))
    }
}

#[derive(Copy, Clone, Serialize, Deserialize)]
pub struct MarketProfileStats {
    pub poc: Price,
    pub value_area_high: Price,
    pub value_area_low: Price,
}

/// Trait for profile bins (Volume or TPO) to allow generic calculation of
/// Value Area and Point of Control.
pub trait ProfileBinStats {
    /// Returns the metric used for importance (Volume or TPO Count).
    fn get_value(&self) -> f64;

    /// Returns the price level of this bin.
    fn get_price(&self) -> Price;
}

// ================================================================================================
// Risk & Performance Metrics Configuration
// ================================================================================================

/// Configuration for calculating portfolio risk and performance metrics.
///
/// These parameters determine how the environment computes ex-post statistics
/// such as Sharpe Ratio, Sortino Ratio, and Maximum Drawdown.
///
/// # Numeric Representation
/// - **Rates** are stored in **Basis Points (bps)** (`1 bps = 0.01%`).
/// - **Capital** is stored as `u32` (representing whole units of quote currency).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RiskMetricsConfig {
    /// The initial portfolio value (capital) at the start of the episode.
    ///
    /// This value is used to normalize returns and calculate percentage-based metrics.
    /// Must be strictly positive (> 0).
    ///
    /// # Example
    /// - `10_000` represents $10,000.
    initial_portfolio_value: u32,

    /// The annualized risk-free rate in **Basis Points** (bps).
    ///
    /// This represents the theoretical return of an investment with zero risk.
    ///
    /// # Conversions
    /// - `200` bps = 2.0% (`0.02`)
    /// - `0` bps   = 0.0% (`0.0`)
    /// - `10000` bps = 100.0% (`1.0`)
    annual_risk_free_rate_bps: u16,
}

impl Default for RiskMetricsConfig {
    fn default() -> Self {
        Self {
            initial_portfolio_value: 10_000,
            // A conservative default: 200 bps = 2% (approx. T-Bill rate)
            annual_risk_free_rate_bps: 200,
        }
    }
}

impl RiskMetricsConfig {
    /// Creates a new config with the mandatory initial capital.
    ///
    /// # Validation
    /// Returns error if `initial_portfolio_value` is 0.
    pub fn new(initial_portfolio_value: u32) -> ChapatyResult<Self> {
        if initial_portfolio_value == 0 {
            return Err(EnvError::InvalidRiskMetricsConfig(
                "Initial portfolio value must be positive (> 0)".to_string(),
            )
            .into());
        }

        Ok(Self {
            initial_portfolio_value,
            ..Default::default()
        })
    }

    /// Set Risk Free Rate.
    ///
    /// # Example
    /// `200` = 2.0%.
    pub fn with_annual_risk_free_rate_bps(self, bps: u16) -> Self {
        Self {
            annual_risk_free_rate_bps: bps,
            ..self
        }
    }

    pub fn initial_portfolio_value(&self) -> u32 {
        self.initial_portfolio_value
    }

    /// Helper to convert the BPS rate to a normalized `f64` (e.g., `200` -> `0.02`).
    pub fn risk_free_rate_f64(&self) -> f64 {
        f64::from(self.annual_risk_free_rate_bps) / 10_000.0
    }

    pub fn risk_free_rate_bps(&self) -> u16 {
        self.annual_risk_free_rate_bps
    }
}
