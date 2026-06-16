use chrono::TimeDelta;

use crate::gym::trading::state::{Canceled, Trade};

impl Trade<Canceled> {
    /// Calculates how long the order was pending before cancellation.
    #[must_use]
    pub fn time_in_force(&self) -> TimeDelta {
        self.state.cancel_ts - self.state.created_at
    }
}
