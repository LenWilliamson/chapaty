use crate::enums::markets::MarketKind;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EquityCurvesReport {
    pub markets: Vec<MarketKind>,
    pub curves: HashMap<MarketKind, EquityCurves>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EquityCurves {
    pub market: MarketKind,
    pub years: Vec<u32>,
    pub curves: HashMap<u32, Vec<f64>>,
}
