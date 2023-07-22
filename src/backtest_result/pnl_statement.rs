use std::collections::HashMap;


use crate::enums::{bots::StrategyKind, markets::MarketKind};

use super::pnl_report::PnLReports;

#[derive(Clone, Debug)]
pub struct PnLStatement {
    pub strategy: StrategyKind,
    pub markets: Vec<MarketKind>,
    pub pnl_data: HashMap<MarketKind, PnLReports>,
}

