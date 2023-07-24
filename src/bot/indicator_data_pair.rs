use crate::enums::{bots::TradingIndicatorKind, data::HdbSourceDir};

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct IndicatorDataPair {
    pub indicator: TradingIndicatorKind,
    pub data: HdbSourceDir,
}

impl IndicatorDataPair {
    pub fn new(indicator: TradingIndicatorKind, data: HdbSourceDir) -> Self {
        Self { indicator, data }
    }
}
