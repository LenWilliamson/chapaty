use std::collections::HashMap;

use crate::{chapaty, enums::indicator::TradingIndicatorKind};

#[derive(Debug, Clone, Default)]
pub struct ExecutionData {
    pub market_sim_data: chapaty::types::DataFrameMap,
    pub trading_indicators: HashMap<TradingIndicatorKind, chapaty::types::DataFrameMap>,
}
