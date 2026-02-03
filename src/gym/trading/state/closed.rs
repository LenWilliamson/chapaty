use chrono::TimeDelta;

use crate::gym::trading::state::{Closed, Trade};

impl Trade<Closed> {
    /// Calculates the duration the trade was active (in milliseconds/seconds depending on TS basis).
    pub fn duration(&self) -> TimeDelta {
        self.state.exit_ts - self.state.entry_ts
    }

    /// Calculates the Return on Investment (ROI) percentage.
    ///
    /// Formula: `PnL / (Entry Price * Quantity)`
    /// Returns `0.0` if the cost basis is zero (should not happen in valid trades).
    pub fn roi(&self) -> f64 {
        let cost_basis = self.state.entry_price.0 * self.quantity.0;
        if cost_basis.abs() < f64::EPSILON {
            0.0
        } else {
            self.state.realized_pnl / cost_basis
        }
    }
}
