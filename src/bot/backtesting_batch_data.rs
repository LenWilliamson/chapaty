use super::{pre_trade_data::PreTradeData, time_frame_snapshot::TimeFrameSnapshot};
use polars::prelude::DataFrame;

pub struct BacktestingBatchData {
    pub time_frame_snapshot: TimeFrameSnapshot,
    pub market_sim_data: DataFrame,
    pub pre_trade_data: PreTradeData,
}
