use polars::prelude::PlSmallStr;
use serde::{Deserialize, Serialize};
use strum::{Display, EnumCount, EnumIter, EnumString, IntoStaticStr};

use crate::{
    data::domain::{Instrument, Price, Quantity, Symbol},
    error::{AgentError, ChapatyResult},
};

#[derive(
    Copy,
    Clone,
    Debug,
    EnumString,
    Display,
    PartialEq,
    Eq,
    Hash,
    Deserialize,
    Serialize,
    PartialOrd,
    Ord,
    IntoStaticStr,
)]
#[strum(serialize_all = "lowercase")]
pub enum TradeType {
    Long,
    Short,
}

impl From<TradeType> for PlSmallStr {
    fn from(value: TradeType) -> Self {
        value.as_str().into()
    }
}

impl TradeType {
    pub fn name(&self) -> PlSmallStr {
        (*self).into()
    }

    pub fn as_str(&self) -> &'static str {
        self.into()
    }
}

impl TradeType {
    pub fn price_ordering_validation(
        &self,
        stop_loss: Option<Price>,
        entry: Option<Price>,
        take_profit: Option<Price>,
    ) -> ChapatyResult<()> {
        fn err_msg(msg: &str) -> ChapatyResult<()> {
            Err(AgentError::InvalidInput(msg.to_string()).into())
        }

        use TradeType::*;
        match (self, stop_loss, entry, take_profit) {
            // No prices: trivially valid
            (_, None, None, None) => Ok(()),

            // Only one provided: trivially valid (explicit Nones ensure exclusivity)
            (_, None, None, Some(_)) | (_, None, Some(_), None) | (_, Some(_), None, None) => {
                Ok(())
            }

            // Entry + TP (Explicitly NO Stop Loss)
            (Long, None, Some(en), Some(tp)) if en.0 < tp.0 => Ok(()),
            (Short, None, Some(en), Some(tp)) if tp.0 < en.0 => Ok(()),

            // SL + TP (Explicitly NO Entry)
            (Long, Some(sl), None, Some(tp)) if sl.0 < tp.0 => Ok(()),
            (Short, Some(sl), None, Some(tp)) if tp.0 < sl.0 => Ok(()),

            // SL + Entry (Explicitly NO Take Profit)
            (Long, Some(sl), Some(en), None) if sl.0 < en.0 => Ok(()),
            (Short, Some(sl), Some(en), None) if en.0 < sl.0 => Ok(()),

            // All three
            (Long, Some(sl), Some(en), Some(tp)) if sl.0 < en.0 && en.0 < tp.0 => Ok(()),
            (Short, Some(sl), Some(en), Some(tp)) if tp.0 < en.0 && en.0 < sl.0 => Ok(()),

            // === ERROR HANDLING ===
            // If we reach here, one of the above patterns matched structurally
            // but failed the `if` guard (e.g. values were out of order).
            (Long, Some(_), Some(_), Some(_)) => {
                err_msg("For long trades: stop_loss < entry < take_profit")
            }
            (Short, Some(_), Some(_), Some(_)) => {
                err_msg("For short trades: take_profit < entry < stop_loss")
            }
            // Explicitly match the pairs to give specific errors
            (Long, None, Some(_), Some(_)) => err_msg("For long trades: entry < take_profit"),
            (Short, None, Some(_), Some(_)) => err_msg("For short trades: take_profit < entry"),
            (Long, Some(_), None, Some(_)) => err_msg("For long trades: stop_loss < take_profit"),
            (Short, Some(_), None, Some(_)) => err_msg("For short trades: take_profit < stop_loss"),
            (Long, Some(_), Some(_), None) => err_msg("For long trades: stop_loss < entry"),
            (Short, Some(_), Some(_), None) => err_msg("For short trades: entry < stop_loss"),
        }
    }

    pub fn price_diff(&self, entry: Price, exit: Price) -> Price {
        match self {
            TradeType::Long => exit - entry,
            TradeType::Short => entry - exit,
        }
    }

    /// Calculates the "Clean" PnL for a trade.
    ///
    /// This method guarantees that the result is a multiple of the tick value.
    /// It eliminates floating point drift by routing through discrete Ticks.
    pub fn calculate_pnl(&self, entry: Price, exit: Price, qty: Quantity, symbol: &Symbol) -> f64 {
        let price_dist = self.price_diff(entry, exit);

        // 2. Snap to Grid (Price -> Ticks)
        let ticks = symbol.price_to_ticks(price_dist);

        // 3. Convert to Money (Ticks -> USD)
        let unit_pnl = symbol.ticks_to_usd(ticks);

        // 4. Scale
        unit_pnl * qty.0
    }
}

#[derive(
    Copy,
    Clone,
    Debug,
    EnumString,
    Display,
    PartialEq,
    Eq,
    Hash,
    Deserialize,
    Serialize,
    PartialOrd,
    Ord,
    IntoStaticStr,
)]
#[strum(serialize_all = "snake_case")]
pub enum TerminationReason {
    StopLoss,
    TakeProfit,
    MarketClose,
    Canceled,
}

impl From<TerminationReason> for PlSmallStr {
    fn from(value: TerminationReason) -> Self {
        value.as_str().into()
    }
}

impl TerminationReason {
    pub fn name(&self) -> PlSmallStr {
        (*self).into()
    }

    pub fn as_str(&self) -> &'static str {
        self.into()
    }
}

/// Represents a Risk-Reward Ratio (RRR) in trading and financial contexts.
///
/// The Risk-Reward Ratio quantifies the relationship between potential loss and potential gain
/// for a trade, helping assess whether the trade setup is favorable.
///
/// # Fields
/// - `risk`: Expected loss (absolute value)
/// - `reward`: Expected gain (absolute value)
/// - `ratio`: The computed ratio, `risk / reward`
///
/// # Interpretation
/// - `ratio = 2.0` -> Risking $2 to gain $1 (risk outweighs reward)
/// - `ratio = 0.5` -> Risking $0.50 to gain $1 (reward outweighs risk)
/// - `reward = 0.0` and `risk > 0.0` -> Infinite risk, no reward => `ratio = f64::INFINITY`
/// - `reward = 0.0` and `risk = 0.0` -> No risk, no reward => `ratio = 0.0`
/// - `risk = 0.0` and `reward > 0.0` -> No-loss scenario => `ratio = 0.0`
///
/// # Notes
/// - Both `risk` and `reward` are expected to be absolute values.
/// - A lower ratio generally indicates a more favorable trade.
/// - The `ratio` field is computed during construction and is not stored externally.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RiskRewardRatio {
    risk: f64,
    reward: f64,
    ratio: f64,
}

impl RiskRewardRatio {
    pub fn new(risk_usd: f64, reward_usd: f64) -> Self {
        let risk = risk_usd.abs();
        let reward = reward_usd.abs();

        let ratio = if reward == 0.0 {
            if risk == 0.0 {
                0.0 // No risk, no reward
            } else {
                f64::INFINITY // All risk, no reward
            }
        } else if risk == 0.0 {
            0.0 // No risk (free money)
        } else {
            risk / reward
        };

        Self {
            risk,
            reward,
            ratio,
        }
    }

    pub fn ratio(&self) -> f64 {
        self.ratio
    }
}

impl std::fmt::Display for RiskRewardRatio {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:.2}:1", self.ratio)
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    PartialOrd,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    EnumString,
    EnumIter,
    EnumCount,
    Display,
    IntoStaticStr,
)]
#[strum(serialize_all = "lowercase")]
pub enum StateKind {
    Active,
    Closed,
    Pending,
    Canceled,
}

impl From<StateKind> for PlSmallStr {
    fn from(value: StateKind) -> Self {
        value.as_str().into()
    }
}

impl StateKind {
    pub fn name(&self) -> PlSmallStr {
        (*self).into()
    }

    pub fn as_str(&self) -> &'static str {
        self.into()
    }
}

#[cfg(test)]
mod tests {
    use crate::data::domain::{ContractMonth, ContractYear, FutureContract, FutureRoot};

    use super::*;

    // Convenience constructors
    fn sl(v: f64) -> Price {
        Price(v)
    }
    fn en(v: f64) -> Price {
        Price(v)
    }
    fn tp(v: f64) -> Price {
        Price(v)
    }

    #[test]
    fn test_long_valid_cases() {
        // stop_loss < entry < take_profit
        assert!(
            TradeType::Long
                .price_ordering_validation(Some(sl(90.0)), Some(en(100.0)), Some(tp(110.0)))
                .is_ok()
        );

        // only entry + take_profit, entry < tp
        assert!(
            TradeType::Long
                .price_ordering_validation(None, Some(en(100.0)), Some(tp(120.0)))
                .is_ok()
        );

        // stop_loss + entry, sl < entry
        assert!(
            TradeType::Long
                .price_ordering_validation(Some(sl(80.0)), Some(en(100.0)), None)
                .is_ok()
        );

        // stop_loss + take_profit, sl < tp
        assert!(
            TradeType::Long
                .price_ordering_validation(Some(sl(80.0)), None, Some(tp(120.0)))
                .is_ok()
        );
    }

    #[test]
    fn test_long_invalid_cases() {
        // entry >= take_profit
        assert!(
            TradeType::Long
                .price_ordering_validation(None, Some(en(120.0)), Some(tp(100.0)))
                .is_err()
        );

        // stop_loss >= entry
        assert!(
            TradeType::Long
                .price_ordering_validation(Some(sl(100.0)), Some(en(90.0)), None)
                .is_err()
        );

        // stop_loss >= take_profit
        assert!(
            TradeType::Long
                .price_ordering_validation(Some(sl(120.0)), None, Some(tp(100.0)))
                .is_err()
        );

        // full triple but wrong ordering
        assert!(
            TradeType::Long
                .price_ordering_validation(Some(sl(100.0)), Some(en(110.0)), Some(tp(105.0)))
                .is_err()
        );
    }

    #[test]
    fn test_short_valid_cases() {
        // take_profit < entry < stop_loss
        assert!(
            TradeType::Short
                .price_ordering_validation(Some(sl(120.0)), Some(en(110.0)), Some(tp(100.0)))
                .is_ok()
        );

        // only entry + take_profit, tp < entry
        assert!(
            TradeType::Short
                .price_ordering_validation(None, Some(en(110.0)), Some(tp(100.0)))
                .is_ok()
        );

        // stop_loss + entry, entry < sl
        assert!(
            TradeType::Short
                .price_ordering_validation(Some(sl(120.0)), Some(en(100.0)), None)
                .is_ok()
        );

        // stop_loss + take_profit, tp < sl
        assert!(
            TradeType::Short
                .price_ordering_validation(Some(sl(120.0)), None, Some(tp(100.0)))
                .is_ok()
        );
    }

    #[test]
    fn test_short_invalid_cases() {
        // take_profit >= entry
        assert!(
            TradeType::Short
                .price_ordering_validation(None, Some(en(90.0)), Some(tp(110.0)))
                .is_err()
        );

        // entry >= stop_loss
        assert!(
            TradeType::Short
                .price_ordering_validation(Some(sl(90.0)), Some(en(100.0)), None)
                .is_err()
        );

        // take_profit >= stop_loss
        assert!(
            TradeType::Short
                .price_ordering_validation(Some(sl(100.0)), None, Some(tp(110.0)))
                .is_err()
        );

        // full triple but wrong ordering
        assert!(
            TradeType::Short
                .price_ordering_validation(Some(sl(100.0)), Some(en(90.0)), Some(tp(95.0)))
                .is_err()
        );
    }

    #[test]
    fn test_three_legged_invariants() {
        // These tests specifically target the "Silent Success" bug.
        // We ensure that having a valid partial match (e.g., Entry < TP)
        // does not mask an invalid third parameter (e.g., SL > Entry).

        // === LONG CASES (Target: SL < Entry < TP) ===

        // Case 1: Entry < TP is Valid (100 < 110), but SL > Entry (105 > 100) -> INVALID
        // This was the specific bug case.
        assert!(
            TradeType::Long
                .price_ordering_validation(Some(sl(105.0)), Some(en(100.0)), Some(tp(110.0)))
                .is_err(),
            "Long: Valid Entry/TP should not mask invalid SL/Entry"
        );

        // Case 2: SL < Entry is Valid (90 < 100), but Entry > TP (100 > 95) -> INVALID
        assert!(
            TradeType::Long
                .price_ordering_validation(Some(sl(90.0)), Some(en(100.0)), Some(tp(95.0)))
                .is_err(),
            "Long: Valid SL/Entry should not mask invalid Entry/TP"
        );

        // === SHORT CASES (Target: TP < Entry < SL) ===

        // Case 3: TP < Entry is Valid (90 < 100), but Entry > SL (100 > 95) -> INVALID
        // (Remember Short SL must be > Entry)
        assert!(
            TradeType::Short
                .price_ordering_validation(Some(sl(95.0)), Some(en(100.0)), Some(tp(90.0)))
                .is_err(),
            "Short: Valid TP/Entry should not mask invalid Entry/SL"
        );

        // Case 4: Entry < SL is Valid (100 < 110), but TP > Entry (105 > 100) -> INVALID
        assert!(
            TradeType::Short
                .price_ordering_validation(Some(sl(110.0)), Some(en(100.0)), Some(tp(105.0)))
                .is_err(),
            "Short: Valid Entry/SL should not mask invalid TP/Entry"
        );
    }

    #[test]
    fn test_price_diff_logic() {
        // 1. Long: Profit if Exit > Entry
        assert_eq!(
            TradeType::Long.price_diff(Price(100.0), Price(110.0)),
            Price(10.0),
            "Long should be positive when price goes up"
        );
        assert_eq!(
            TradeType::Long.price_diff(Price(110.0), Price(100.0)),
            Price(-10.0),
            "Long should be negative when price goes down"
        );

        // 2. Short: Profit if Entry > Exit
        assert_eq!(
            TradeType::Short.price_diff(Price(110.0), Price(100.0)),
            Price(10.0),
            "Short should be positive when price goes down"
        );
        assert_eq!(
            TradeType::Short.price_diff(Price(100.0), Price(110.0)),
            Price(-10.0),
            "Short should be negative when price goes up"
        );
    }

    #[test]
    fn test_pnl_cleans_dirty_inputs() {
        // Setup: EUR/USD Future
        // Tick Size: 0.00005
        // Tick Value: $6.25
        let eur = Symbol::Future(FutureContract {
            root: FutureRoot::EurUsd,
            month: ContractMonth::December,
            year: ContractYear::Y5,
        });

        let entry = Price(1.10000);

        // Target: 10 ticks of profit (10 * 0.00005 = 0.0005)
        // Expected PnL: 10 ticks * $6.25 = $62.50

        // CASE 1: Dirty Long Exit (Price is slightly too high: 1.10050 + 0.00000001)
        let dirty_exit = Price(1.10050001);

        let pnl = TradeType::Long.calculate_pnl(entry, dirty_exit, Quantity(1.0), &eur);

        // Without grid snapping, this would result in ~$62.50125
        // With snapping, it must be exactly 62.5
        assert_eq!(pnl, 62.5, "Long PnL failed to snap dirty input to grid");

        // CASE 2: Dirty Short Entry (Price is slightly too low: 1.10050 - 0.00000001)
        // Shorting from here down to 1.10000 should still yield 10 ticks
        let dirty_entry = Price(1.09949999); // Target 10 ticks below is ~1.09900
        let clean_exit = Price(1.09900);

        let pnl_short =
            TradeType::Short.calculate_pnl(dirty_entry, clean_exit, Quantity(1.0), &eur);

        assert_eq!(
            pnl_short, 62.5,
            "Short PnL failed to snap dirty input to grid"
        );
    }

    // ============================================================================================
    // RiskRewardRatio Logic Tests
    // ============================================================================================

    #[test]
    fn calculates_standard_ratios() {
        // Case 1: Favorable trade (Risk $50, Reward $100)
        // Ratio = 50 / 100 = 0.5
        let favorable = RiskRewardRatio::new(50.0, 100.0);
        assert_eq!(favorable.ratio(), 0.5);

        // Case 2: Unfavorable trade (Risk $100, Reward $50)
        // Ratio = 100 / 50 = 2.0
        let unfavorable = RiskRewardRatio::new(100.0, 50.0);
        assert_eq!(unfavorable.ratio(), 2.0);

        // Case 3: Break-even setup (Risk $100, Reward $100)
        let neutral = RiskRewardRatio::new(100.0, 100.0);
        assert_eq!(neutral.ratio(), 1.0);
    }

    #[test]
    fn handles_negative_inputs_gracefully() {
        // Inputs should be absolute-valued automatically
        let rrr = RiskRewardRatio::new(-50.0, -100.0);
        assert_eq!(rrr.risk, 50.0);
        assert_eq!(rrr.reward, 100.0);
        assert_eq!(rrr.ratio(), 0.5);
    }

    #[test]
    fn handles_edge_cases() {
        // 1. Zero Risk (Free money) -> Ratio 0.0
        let zero_risk = RiskRewardRatio::new(0.0, 100.0);
        assert_eq!(zero_risk.ratio(), 0.0);

        // 2. Zero Reward (All risk) -> Ratio Infinity
        let zero_reward = RiskRewardRatio::new(100.0, 0.0);
        assert_eq!(zero_reward.ratio(), f64::INFINITY);

        // 3. Zero Risk AND Zero Reward -> Ratio 0.0 (Undefined, treated as neutral/safe)
        let zero_zero = RiskRewardRatio::new(0.0, 0.0);
        assert_eq!(zero_zero.ratio(), 0.0);
    }

    #[test]
    fn test_display_formatting() {
        // 1. Standard float formatting
        let rrr = RiskRewardRatio::new(50.0, 100.0);
        assert_eq!(rrr.to_string(), "0.50:1");

        // 2. Whole numbers should still have decimals
        let rrr_whole = RiskRewardRatio::new(200.0, 100.0);
        assert_eq!(rrr_whole.to_string(), "2.00:1");

        // 3. Infinity
        let rrr_inf = RiskRewardRatio::new(100.0, 0.0);
        assert_eq!(rrr_inf.to_string(), "inf:1");
    }
}
