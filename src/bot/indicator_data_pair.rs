use crate::enums::{data::HdbSourceDirKind, indicator::TradingIndicatorKind};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndicatorDataPair {
    pub indicator: TradingIndicatorKind,
    pub data: HdbSourceDirKind,
}

impl IndicatorDataPair {
    pub fn new(indicator: TradingIndicatorKind, data: HdbSourceDirKind) -> Self {
        Self { indicator, data }
    }
}
