use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{
    pnl::pnl_statement_agg_markets::{PnLSnapshotAggMarkets, PnLStatementAggMarkets},
    MarketKind,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EquityCurvesAggMarkets {
    pub markets: Vec<MarketKind>,
    pub years: Vec<u32>,
    pub curves: HashMap<u32, Vec<f64>>,
}

impl From<PnLStatementAggMarkets> for EquityCurvesAggMarkets {
    fn from(value: PnLStatementAggMarkets) -> Self {
        PnLSnapshotAggMarkets {
            pnl_reports: value.pnl_data,
            strategy_name: value.strategy_name.clone(),
        }
        .into()
    }
}
