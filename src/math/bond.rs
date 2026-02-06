use chrono::{Datelike, NaiveDate};
use serde::{Deserialize, Serialize};

// ============================================================================
// 1. Enums & Conventions
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum DayCount {
    /// **30/360 (US Agency / Corporate Standard).**
    /// Assumes every month has 30 days and a year has 360 days.
    Thirty360,

    /// **Actual/365 (Government Standard in UK/Japan).**
    /// Uses actual days difference divided by fixed 365.
    Actual365,

    /// **Actual/Actual ISDA (US Treasuries).**
    /// Accounts for leap years accurately.
    ActualActual,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum Frequency {
    Annual = 1,
    SemiAnnual = 2,
    Quarterly = 4,
}

impl Frequency {
    /// Returns the number of months between payments.
    fn months(&self) -> u32 {
        12 / (*self as u32)
    }
}

// ============================================================================
// 2. The Bond Definition (Static Data)
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BondStaticData {
    /// The fixed interest rate paid per year (e.g., 0.05 for 5%).
    pub coupon_rate: f64,

    /// The date when the principal is repaid.
    pub maturity_date: NaiveDate,

    /// How often coupons are paid.
    pub frequency: Frequency,

    /// The convention used to calculate year fractions.
    pub day_count: DayCount,

    /// The redemption value (usually 100.0).
    pub face_value: f64,
}

impl BondStaticData {
    // ========================================================================
    // Core Pricing Logic
    // ========================================================================

    /// Calculates the **Dirty Price** (Clean Price + Accrued Interest) from a Yield.
    ///
    /// Uses the Discounted Cash Flow (DCF) method:
    /// $$ P = \sum \frac{C}{(1+y/f)^t} $$
    pub fn dirty_price_from_yield(&self, yield_pct: f64, settlement: NaiveDate) -> f64 {
        if settlement >= self.maturity_date {
            return 0.0; // Expired bond
        }

        let (prev_coupon, next_coupon) = self.get_coupon_dates(settlement);

        // 1. Calculate fraction of the current period remaining
        // "tau" represents the time from settlement to next coupon in fractional periods.
        let days_in_period = self.days_between(prev_coupon, next_coupon);
        let days_remaining = self.days_between(settlement, next_coupon);

        // Safety check for division by zero
        if days_in_period == 0.0 {
            return self.face_value;
        }

        let fraction = days_remaining / days_in_period; // Value between 0.0 and 1.0

        // 2. Setup Cashflow parameters
        let freq_val = self.frequency as i32 as f64;
        let coupon_amt = (self.coupon_rate * self.face_value) / freq_val;
        let y_per_period = yield_pct / freq_val;
        let discount_factor = 1.0 + y_per_period;

        let mut pv = 0.0;
        let mut current_coupon_date = next_coupon;
        let mut period_idx = 0;

        // 3. Discount all future cashflows
        while current_coupon_date <= self.maturity_date {
            // Time t (in periods) = fraction + period_index
            let t = fraction + (period_idx as f64);

            // Add Coupon PV
            pv += coupon_amt / discount_factor.powf(t);

            // Add Principal PV (only at maturity)
            if current_coupon_date == self.maturity_date {
                pv += self.face_value / discount_factor.powf(t);
                break; // Done
            }

            // Move to next coupon
            current_coupon_date = self.next_coupon_date(current_coupon_date);
            period_idx += 1;

            // Safety break for infinite loops
            if period_idx > 1000 {
                break;
            }
        }

        pv
    }

    /// Calculates the **Yield to Maturity (YTM)** from a **Clean Price**.
    ///
    /// Since traders quote Clean Price, but math works on Dirty Price,
    /// this function handles the conversion internally.
    pub fn yield_from_clean_price(&self, clean_price: f64, settlement: NaiveDate) -> Option<f64> {
        let accrued = self.accrued_interest(settlement);
        let target_dirty = clean_price + accrued;

        self.solve_yield_internal(target_dirty, settlement)
    }

    /// Calculates **Accrued Interest**.
    ///
    /// Interest earned by the seller since the last coupon payment.
    /// Formula: `Coupon * (DaysSinceLast / DaysInPeriod)`
    pub fn accrued_interest(&self, settlement: NaiveDate) -> f64 {
        if settlement >= self.maturity_date {
            return 0.0;
        }

        let (prev_coupon, next_coupon) = self.get_coupon_dates(settlement);

        let days_in_period = self.days_between(prev_coupon, next_coupon);
        let days_accrued = self.days_between(prev_coupon, settlement);

        if days_in_period == 0.0 {
            return 0.0;
        }

        let coupon_amt = (self.coupon_rate * self.face_value) / (self.frequency as i32 as f64);

        coupon_amt * (days_accrued / days_in_period)
    }

    // ========================================================================
    // Helpers & Numerical Methods
    // ========================================================================

    /// Newton-Raphson Solver for Yield.
    fn solve_yield_internal(&self, target_dirty: f64, settlement: NaiveDate) -> Option<f64> {
        let max_iter = 50;
        let tolerance = 1e-7;
        let mut y = self.coupon_rate; // Good initial guess (trading near par)

        for _ in 0..max_iter {
            let price = self.dirty_price_from_yield(y, settlement);
            let diff = price - target_dirty;

            if diff.abs() < tolerance {
                return Some(y);
            }

            // Finite Difference derivative (Secant method approximation step)
            let h = 1e-5;
            let p_up = self.dirty_price_from_yield(y + h, settlement);
            let p_down = self.dirty_price_from_yield(y - h, settlement);
            let derivative = (p_up - p_down) / (2.0 * h);

            if derivative.abs() < 1e-9 {
                return None;
            } // Flat curve error

            y = y - (diff / derivative);

            // Sanity clamp (-50% to +100% yield)
            if y <= -0.5 {
                y = -0.5;
            }
            if y >= 1.0 {
                y = 1.0;
            }
        }

        None
    }

    /// Helper to calculate Year Fraction / Days based on convention.
    fn days_between(&self, start: NaiveDate, end: NaiveDate) -> f64 {
        match self.day_count {
            DayCount::ActualActual | DayCount::Actual365 => (end - start).num_days() as f64,
            DayCount::Thirty360 => {
                // Standard NASD 30/360
                let d1 = start.day().min(30);
                let d2 = if d1 == 30 && end.day() == 31 {
                    30
                } else {
                    end.day()
                };

                let y_diff = (end.year() - start.year()) as f64;
                let m_diff = (end.month() as i32 - start.month() as i32) as f64;
                let d_diff = (d2 as i32 - d1 as i32) as f64;

                (y_diff * 360.0) + (m_diff * 30.0) + d_diff
            }
        }
    }

    /// Finds the previous and next coupon dates relative to a settlement date.
    /// (Simplified logic: walks back from maturity).
    fn get_coupon_dates(&self, settlement: NaiveDate) -> (NaiveDate, NaiveDate) {
        let mut next = self.maturity_date;
        let months = self.frequency.months();

        // Walk backwards until we find the interval containing settlement
        while next > settlement {
            let prev_calc = self.prev_coupon_date(next);
            if prev_calc <= settlement {
                return (prev_calc, next);
            }
            next = prev_calc;
        }

        // Fallback (should not happen if check strictly < maturity)
        (settlement, self.maturity_date)
    }

    /// Subtracts frequency months from a date.
    fn prev_coupon_date(&self, date: NaiveDate) -> NaiveDate {
        let months = self.frequency.months();
        let new_month = date.month() as i32 - months as i32;

        let (y, m) = if new_month <= 0 {
            (date.year() - 1, (new_month + 12) as u32)
        } else {
            (date.year(), new_month as u32)
        };

        // Handle end-of-month logic (e.g. Feb 30 -> Feb 28)
        Self::get_valid_date(y, m, date.day())
    }

    /// Adds frequency months to a date.
    fn next_coupon_date(&self, date: NaiveDate) -> NaiveDate {
        let months = self.frequency.months();
        let new_month = date.month() + months;

        let (y, m) = if new_month > 12 {
            (date.year() + 1, new_month - 12)
        } else {
            (date.year(), new_month)
        };

        Self::get_valid_date(y, m, date.day())
    }

    /// Safe date constructor (handles Feb 29 etc)
    fn get_valid_date(year: i32, month: u32, day: u32) -> NaiveDate {
        // Try exact day, if fail, subtract days until valid (e.g. 31 -> 30 -> 29)
        let mut d = day;
        loop {
            if let Some(date) = NaiveDate::from_ymd_opt(year, month, d) {
                return date;
            }
            d -= 1;
            if d == 0 {
                panic!("Date calc failed");
            }
        }
    }
}

// ============================================================================
// Unit Tests (Proof of Correctness)
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_accrued_interest_mid_period() {
        let bond = BondStaticData {
            coupon_rate: 0.10, // 10%
            maturity_date: NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
            frequency: Frequency::Annual,
            day_count: DayCount::Thirty360,
            face_value: 100.0,
        };

        // Exact middle of the year (July 1st)
        let settlement = NaiveDate::from_ymd_opt(2024, 7, 1).unwrap();

        let accrued = bond.accrued_interest(settlement);
        // 10% of 100 = 10. Half year = 5.0.
        assert!(
            (accrued - 5.0).abs() < 1e-2,
            "Accrued should be approx 5.0, got {}",
            accrued
        );
    }

    #[test]
    fn test_yield_price_roundtrip() {
        let bond = BondStaticData {
            coupon_rate: 0.05, // 5%
            maturity_date: NaiveDate::from_ymd_opt(2030, 1, 1).unwrap(),
            frequency: Frequency::Annual,
            day_count: DayCount::Thirty360,
            face_value: 100.0,
        };

        let settlement = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(); // Clean integer years left

        // 1. Calc Price from Yield of 5% (Par)
        let dirty = bond.dirty_price_from_yield(0.05, settlement);
        // Should be exactly 100 + 0 accrued
        assert!(
            (dirty - 100.0).abs() < 0.01,
            "Par yield should give par price"
        );

        // 2. Calc Price from Yield of 4% (Premium)
        let dirty_premium = bond.dirty_price_from_yield(0.04, settlement);
        assert!(
            dirty_premium > 100.0,
            "Lower yield should imply premium price"
        );

        // 3. Roundtrip: Price -> Yield
        let implied_yield = bond
            .solve_yield_internal(dirty_premium, settlement)
            .unwrap();
        assert!((implied_yield - 0.04).abs() < 1e-6, "Roundtrip failed");
    }
}
