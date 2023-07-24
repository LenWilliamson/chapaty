use std::collections::HashMap;

use crate::enums::{bot::StrategyKind, markets::MarketKind};

use super::pnl_report::PnLReports;

#[derive(Clone, Debug)]
pub struct PnLStatement {
    pub strategy: StrategyKind,
    pub markets: Vec<MarketKind>,
    pub pnl_data: HashMap<MarketKind, PnLReports>,
}

impl PnLStatement {
    pub fn save_as_csv(&self, file_name: &str) {
        self.pnl_data
            .iter()
            .for_each(|(_, data)| data.save_as_csv(file_name))
    }
}
