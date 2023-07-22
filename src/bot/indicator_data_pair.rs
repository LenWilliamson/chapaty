use crate::enums::{bots::TradingIndicatorKind, data::LeafDir};

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct IndicatorDataPair {
    pub indicator: TradingIndicatorKind,
    pub data: LeafDir,
}

impl IndicatorDataPair {
    pub fn new(indicator: TradingIndicatorKind, data: LeafDir) -> Self {
        Self { indicator, data }
    }
}
