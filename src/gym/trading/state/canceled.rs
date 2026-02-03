use chrono::TimeDelta;

use crate::gym::trading::state::{Canceled, Trade};

impl Trade<Canceled> {
    /// Calculates how long the order was pending before cancellation.
    pub fn time_in_force(&self) -> TimeDelta {
        self.state.canceled_at - self.state.created_at
    }
}
