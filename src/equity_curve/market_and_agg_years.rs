use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::MarketKind;

#[derive(Debug, Serialize, Deserialize)]
pub struct EquityCurvesAggYears {
    pub markets: Vec<MarketKind>,
    pub years: Vec<u32>,
    pub curves: HashMap<MarketKind, Vec<f64>>,
}
