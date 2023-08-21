use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::MarketKind;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EquityCurvesAggMarkets {
    pub markets: Vec<MarketKind>,
    pub years: Vec<u32>,
    pub curves: HashMap<u32, Vec<f64>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EquityCurvesAggYears {
    pub markets: Vec<MarketKind>,
    pub years: Vec<u32>,
    pub curves: HashMap<MarketKind, Vec<f64>>,
}

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