use std::{convert::identity, sync::Arc};

use polars::prelude::LazyFrame;

use crate::{
    bot::trade::Trade, data_provider::DataProvider,
    enums::bots::TradeDataKind, lazy_frame_operations::trait_extensions::MyLazyFrameOperations,
    strategy::Strategy,
};

use super::pnl_report_data_row_calculator::TradeAndPreTradeValues;

pub struct TradePnLCalculator {
    data_provider: Arc<dyn DataProvider>,
    strategy: Arc<dyn Strategy>,
    entry_ts: i64,
    trade: Trade,
    market_sim_data_since_entry: LazyFrame,
    trade_and_pre_trade_values: TradeAndPreTradeValues,
}

#[derive(Debug, Clone)]
pub struct TradePnL {
    pub trade_entry_ts: i64,
    pub stop_loss: Option<PnL>,
    pub take_profit: Option<PnL>,
    pub timeout: Option<PnL>,
}

impl TradePnL {
    pub fn trade_outcome(&self) -> String {
        if self.is_trade_timeout() {
            self.handle_timeout_trade()
        } else {
            self.handle_regular_trade_outcome()
        }
    }

    pub fn exit_price(&self) -> f64 {
        if self.is_trade_timeout() {
            self.timeout.clone().unwrap().price
        } else {
            self.handle_regular_trade_exit()
        }
    }

    pub fn profit(&self) -> f64 {
        if self.is_trade_timeout() {
            self.timeout.clone().unwrap().profit.clone().unwrap()
        } else {
            self.handle_regular_profit()
        }
    }

    fn handle_regular_trade_exit(&self) -> f64 {
        if self.is_stop_loss_entry_before_take_profit_entry() {
            self.stop_loss.clone().unwrap().price
        } else if self.is_stop_loss_entry_after_take_profit_entry() {
            self.take_profit.clone().unwrap().price
        } else {
            // If trade outcome not clear, be conservative and assume loser trade
            self.stop_loss.clone().unwrap().price
        }
    }

    fn handle_regular_profit(&self) -> f64 {
        if self.is_stop_loss_entry_before_take_profit_entry() {
            self.stop_loss.clone().unwrap().profit.clone().unwrap()
        } else if self.is_stop_loss_entry_after_take_profit_entry() {
            self.take_profit.clone().unwrap().profit.clone().unwrap()
        } else {
            // If trade outcome not clear, be conservative and assume loser trade
            self.stop_loss.clone().unwrap().profit.clone().unwrap()
        }
    }

    fn is_trade_timeout(&self) -> bool {
        let is_stop_loss_timeout = self.stop_loss.is_none();
        let is_take_profit_timeout = self.take_profit.is_none();

        is_stop_loss_timeout && is_take_profit_timeout
    }

    fn handle_timeout_trade(&self) -> String {
        if self.is_timeout_trade_winner() {
            "Winner".to_string()
        } else {
            "Loser".to_string()
        }
    }

    fn handle_regular_trade_outcome(&self) -> String {
        if self.is_stop_loss_entry_before_take_profit_entry() {
            "Loser".to_string()
        } else if self.is_stop_loss_entry_after_take_profit_entry() {
            "Winner".to_string()
        } else {
            "Not Clear".to_string()
        }
    }

    fn is_timeout_trade_winner(&self) -> bool {
        self.timeout.clone().unwrap().profit.clone().unwrap() > 0.0
    }

    fn is_stop_loss_entry_before_take_profit_entry(&self) -> bool {
        let sl_ts = get_entry_ts(&self.stop_loss);
        let tp_ts = get_entry_ts(&self.take_profit);
        sl_ts < tp_ts
    }

    fn is_stop_loss_entry_after_take_profit_entry(&self) -> bool {
        let sl_ts = get_entry_ts(&self.stop_loss);
        let tp_ts = get_entry_ts(&self.take_profit);
        sl_ts > tp_ts
    }
}

fn get_entry_ts(trade_pnl: &Option<PnL>) -> i64 {
    trade_pnl.clone().map_or_else(no_entry_timestamp, |pnl| {
        pnl.ts.map_or_else(no_entry_timestamp, identity)
    })
}

fn no_entry_timestamp() -> i64 {
    i64::MAX
}

#[derive(Debug, Clone)]
pub struct PnL {
    pub price: f64,
    pub ts: Option<i64>,
    pub profit: Option<f64>,
}

impl PnL {
    fn or_none(self) -> Option<Self> {
        if is_order_open(self.ts) {
            None
        } else {
            Some(self)
        }
    }
}

impl TradePnLCalculator {
    pub fn compute(&self) -> TradePnL {
        let stop_loss = self.handle_exit(self.trade.stop_loss);
        let take_profit = self.handle_exit(self.trade.take_prift);
        let timeout = if is_limit_order_open(stop_loss.clone(), take_profit.clone()) {
            Some(self.handle_timeout())
        } else {
            None
        };

        TradePnL {
            trade_entry_ts: self.entry_ts,
            stop_loss: stop_loss.or_none(),
            take_profit: take_profit.or_none(),
            timeout,
        }
    }

    fn handle_exit(&self, exit_px: f64) -> PnL {
        let ts = self.trade_exit_ts(exit_px);
        let profit = ts.map_or_else(|| None, |_| Some(self.trade.profit(exit_px)));

        PnL {
            price: exit_px,
            ts,
            profit,
        }
    }

    fn handle_timeout(&self) -> PnL {
        let exit_px = self
            .trade_and_pre_trade_values
            .trade
            .get(&TradeDataKind::LastTradePrice)
            .unwrap().clone()
            .unwrap_float64();

        PnL {
            price: exit_px,
            ts: Some(0),
            profit: Some(self.trade.profit(exit_px)),
        }
    }

    fn trade_exit_ts(&self, exit_px: f64) -> Option<i64> {
        self.market_sim_data_since_entry
            .clone()
            .find_timestamp_when_price_reached(exit_px, self.data_provider.clone())
    }
}

fn is_limit_order_open(sl: PnL, tp: PnL) -> bool {
    let is_sl_order_open = is_order_open(sl.ts);
    let is_tp_order_open = is_order_open(tp.ts);

    is_sl_order_open && is_tp_order_open
}

fn is_order_open(timestamp: Option<i64>) -> bool {
    timestamp.is_none()
}

pub struct TradePnLCalculatorBuilder {
    data_provider: Option<Arc<dyn DataProvider>>,
    strategy: Option<Arc<dyn Strategy>>,
    entry_ts: Option<i64>,
    trade: Option<Trade>,
    market_sim_data_since_entry: Option<LazyFrame>,
    trade_and_pre_trade_values: Option<TradeAndPreTradeValues>,
}

impl TradePnLCalculatorBuilder {
    pub fn new() -> Self {
        Self {
            data_provider: None,
            strategy: None,
            entry_ts: None,
            trade: None,
            market_sim_data_since_entry: None,
            trade_and_pre_trade_values: None,
        }
    }

    pub fn with_data_provider(self, data_provider: Arc<dyn DataProvider>) -> Self {
        Self {
            data_provider: Some(data_provider),
            ..self
        }
    }

    pub fn with_strategy(self, strategy: Arc<dyn Strategy>) -> Self {
        Self {
            strategy: Some(strategy),
            ..self
        }
    }

    pub fn with_market_sim_data_since_entry(self, market_sim_data_since_entry: LazyFrame) -> Self {
        Self {
            market_sim_data_since_entry: Some(market_sim_data_since_entry),
            ..self
        }
    }

    pub fn with_entry_ts(self, ts: i64) -> Self {
        Self {
            entry_ts: Some(ts),
            ..self
        }
    }

    pub fn with_trade(self, trade: Trade) -> Self {
        Self {
            trade: Some(trade),
            ..self
        }
    }

    pub fn with_trade_and_pre_trade_values(
        self,
        trade_and_pre_trade_values: TradeAndPreTradeValues,
    ) -> Self {
        Self {
            trade_and_pre_trade_values: Some(trade_and_pre_trade_values),
            ..self
        }
    }

    pub fn build(self) -> TradePnLCalculator {
        TradePnLCalculator {
            data_provider: self.data_provider.clone().unwrap(),
            strategy: self.strategy.clone().unwrap(),
            entry_ts: self.entry_ts.clone().unwrap(),
            trade: self.trade.clone().unwrap(),
            market_sim_data_since_entry: self.market_sim_data_since_entry.clone().unwrap(),
            trade_and_pre_trade_values: self.trade_and_pre_trade_values.clone().unwrap(),
        }
    }

    pub fn build_and_compute(self) -> TradePnL {
        self.build().compute()
    }
}
