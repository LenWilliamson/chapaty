use super::{
    pre_trade_values_calculator::{
        PreTradeValuesCalculatorBuilder, RequiredPreTradeValuesWithData,
    },
    trade_pnl_calculator::{TradePnL, TradePnLCalculatorBuilder},
};
use crate::{
    bot::{pre_trade_data::PreTradeData, time_frame_snapshot::TimeFrameSnapshot},
    compose,
    decision_policy::DecisionPolicy,
    dfa::{
        market_simulation_data::{Market, MarketDataFrame, SimulationData, SimulationDataBuilder},
        states::{Active, Close, CloseEvent, Trade, TradeResult},
    },
    enums::trade_and_pre_trade::TradeCloseKind,
    lazy_frame_operations::trait_extensions::MyLazyFrameOperations,
    strategy::Strategy,
    MarketKind, MarketSimulationDataKind,
};
use polars::prelude::{DataFrame, IntoLazy, LazyFrame};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct PnLReportDataRow<'a> {
    pub market: MarketKind,
    pub year: u32,
    pub strategy_name: String,
    pub time_frame_snapshot: TimeFrameSnapshot,
    pub trade: &'a Trade<'a, Close>,
    pub trade_pnl: Option<TradePnL>,
}

pub struct PnLReportDataRowCalculator {
    pub strategies: Vec<Arc<dyn Strategy + Send + Sync>>,
    pub decision_policy: Arc<dyn DecisionPolicy + Send + Sync>,
    pub sim_data: SimulationData,
    pub market_sim_df: DataFrame,
    pub year: u32,
    pub time_frame_snapshot: TimeFrameSnapshot,
    pub market: Vec<Market>,
}

impl PnLReportDataRowCalculator {
    pub fn compute(self) -> Vec<LazyFrame> {
        let mut pnl_report_data_rows = Vec::new();
        if self.market.is_empty() {
            return pnl_report_data_rows;
        }

        let mut trade = Trade::new();
        // let mut sim_event = SimulationEvent::new(vec![&self.market[0]], &self.sim_data);
        let mut market_trajectory = Box::new(Vec::new());
        let sim_data_box = Box::new(self.sim_data.clone());
        for market_event in self.market.iter() {
            // sim_event.update_on_market_event(market_event);
            trade.update_on_market_event(&market_event);
            market_trajectory.push(*market_event);

            // TODOs
            // 1. Verfeinern / Refactor mit Hilfe meines Automaten Diagrams in Notability
            // 2. Update initial_balance when possible
            // sim_event.initial_balance.get_or_insert_with(|| {
            //     InitialBalanceCalculator::try_compute().unwrap_or_else(|| return None)
            // });
            /*

            DONE: 2. PnLDataRow Element richtig berechnen
            DONE: 3. Strategiesn anpassen
            4. Decision Policy implementieren: Wenn nur eine Strategie, und für Rassler mit Conf und Counter
            */
            trade = match trade {
                TradeResult::Idle(idle_trade) => {
                    // TODO simplify, the decision policy takes care about choosing the strategy, remove: else if activation_events.len() == 1
                    let activation_events: Vec<_> = self
                        .strategies
                        .iter()
                        .filter_map(|strategy| {
                            strategy.check_activation_event(&market_trajectory, &sim_data_box)
                        })
                        .collect();
                    if activation_events.is_empty() {
                        TradeResult::Idle(idle_trade)
                    } else if activation_events.len() == 1 {
                        idle_trade.activation_event(&activation_events[0])
                    } else {
                        match self.decision_policy.choose_strategy(&activation_events) {
                            Some(strategy) => {
                                // TODO refine if you have the same strategy with diffrent parameter configuration
                                let activation_event = activation_events
                                    .iter()
                                    .find(|e| e.strategy.get_strategy_kind() == strategy)
                                    .unwrap();
                                idle_trade.activation_event(activation_event)
                            }
                            None => TradeResult::Idle(idle_trade), // remain idle
                        }
                    }
                }
                TradeResult::Active(active_trade) => {
                    // Check for activation events other than current strategy running trade
                    let activation_events: Vec<_> = self
                        .strategies
                        .iter()
                        .filter_map(|strategy| {
                            strategy.check_activation_event(&market_trajectory, &sim_data_box)
                        })
                        .filter(|event| {
                            event.strategy.get_strategy_kind()
                                != active_trade.strategy.as_ref().unwrap().get_strategy_kind()
                        })
                        .collect();

                    // check exit condition for strategy who activated the trade
                    let trade_result = if active_trade
                        .strategy
                        .as_ref()
                        .unwrap()
                        .check_cancelation_event(&market_trajectory, &sim_data_box, &active_trade)
                        .is_some()
                    {
                        let close_event = active_trade
                            .strategy
                            .as_ref()
                            .unwrap()
                            .check_cancelation_event(
                                &market_trajectory,
                                &sim_data_box,
                                &active_trade,
                            )
                            .unwrap();
                        let mut composed = compose!(
                            {|active_trade: Trade<Active>| active_trade.close_event(&close_event)}
                            {|trade_result: TradeResult| {
                                if let TradeResult::Close(mut closed_trade) = trade_result {
                                    closed_trade.curate_precision(&self.sim_data.market_kind);
                                    self.add_pnl_report_data_row(&mut pnl_report_data_rows, closed_trade)
                                } else {
                                    panic!("Closed a trade but got not TradeResult::Close(...)")
                                }
                            }}
                            {|closed_trade: Trade<Close>| closed_trade.reset()}
                        );
                        // active_trade.close_event(&close_event)
                        composed(active_trade) // -> returns TradeResultIdle after closing trade
                    } else {
                        TradeResult::Active(active_trade)
                    };

                    let activation_event =
                        match self.decision_policy.choose_strategy(&activation_events) {
                            Some(strategy) => {
                                // TODO refine if you have the same strategy with diffrent parameter configuration
                                Some(
                                    activation_events
                                        .iter()
                                        .find(|e| e.strategy.get_strategy_kind() == strategy)
                                        .unwrap(),
                                )
                            }
                            None => None, // no activation event
                        };
                    if activation_event.is_some() {
                        match trade_result {
                            TradeResult::Active(active_trade) => {
                                let mut compose = compose!(
                                {|active_trade: Trade<Active>| active_trade.pivot_event(&activation_event.unwrap())}
                                    {|trade_result: TradeResult| {
                                    if let TradeResult::Close(mut closed_trade) = trade_result {
                                        closed_trade.curate_precision(&self.sim_data.market_kind);
                                        self.add_pnl_report_data_row(&mut pnl_report_data_rows, closed_trade)
                                    } else {
                                        panic!("Closed a trade but got not TradeResult::Close(...)")
                                    }
                                }}
                                {|closed_trade: Trade<Close>| closed_trade.pivot_event(&activation_event.unwrap())}
                                    );
                                compose(active_trade)
                            }
                            TradeResult::Idle(idle_trade) => {
                                idle_trade.activation_event(activation_event.unwrap())
                            }
                            TradeResult::Close(_) => panic!("Invalid, why?"),
                        }
                    } else {
                        trade_result
                    }
                }

                _ => panic!("Closed trade is not an accepting state, only idle and active are accepting states"),
            };

            trade = if market_trajectory.last().unwrap().is_end_of_day {
                let timeout = CloseEvent {
                    exit_ts: market_trajectory.last().unwrap().ohlc.close_ts.unwrap(),
                    exit_price: market_trajectory.last().unwrap().ohlc.close.unwrap(),
                    close_event_kind: TradeCloseKind::Timeout,
                };
                match trade {
                    TradeResult::Active(trade) => {
                        let mut composed = compose!(
                            {|active_trade: Trade<Active>| active_trade.close_event(&timeout)}
                            {|trade_result: TradeResult| {
                                if let TradeResult::Close(mut closed_trade) = trade_result {
                                    closed_trade.curate_precision(&self.sim_data.market_kind);
                                    self.add_pnl_report_data_row(&mut pnl_report_data_rows, closed_trade)
                                } else {
                                    panic!("Closed a trade but got not TradeResult::Close(...)")
                                }
                            }}
                            {|closed_trade: Trade<Close>| closed_trade.reset()}
                        );
                        // active_trade.close_event(&close_event)
                        composed(trade) // -> returns TradeResultIdle after closing trade
                    }
                    _ => trade,
                }
            } else {
                trade
            };
        }

        pnl_report_data_rows
    }

    // fn handle_trade(&self, mut closed_trade: Trade<Close>) -> PnLReportDataRow {
    //     let entry_ts = closed_trade.entry_ts.unwrap();
    //     closed_trade.curate_precision(&self.sim_data.market_kind);
    //     let trade_pnl = TradePnLCalculatorBuilder::new()
    //         .with_entry_ts(entry_ts)
    //         .with_trade(&closed_trade)
    //         .with_market_sim_data_since_entry(self.market_sim_data_since_entry_ts(entry_ts))
    //         .with_trade_and_pre_trade_values(self.sim_data.pre_trade_values.clone())
    //         .build_and_compute();

    //     PnLReportDataRow {
    //         market: self.sim_data.market_kind,
    //         year: self.year,
    //         strategy_name: closed_trade.strategy.as_ref().unwrap().get_name(),
    //         time_frame_snapshot: self.time_frame_snapshot,
    //         trade: closed_trade,
    //         trade_pnl: Some(trade_pnl),
    //     }
    // }

    fn add_pnl_report_data_row<'a>(
        &self,
        pnl_report_data_rows: &mut Vec<LazyFrame>,
        closed_trade: Trade<'a, Close>,
    ) -> Trade<'a, Close> {
        let entry_ts = closed_trade.entry_ts.as_ref().unwrap();
        let trade_pnl = TradePnLCalculatorBuilder::new()
            .with_entry_ts(*entry_ts)
            .with_trade(&closed_trade)
            .with_market_sim_data_since_entry(self.market_sim_data_since_entry_ts(*entry_ts))
            .with_trade_and_pre_trade_values(self.sim_data.pre_trade_values.clone())
            .build_and_compute();

        let pnl_report_data_row = PnLReportDataRow {
            market: self.sim_data.market_kind,
            year: self.year,
            strategy_name: closed_trade.strategy.as_ref().unwrap().get_name(),
            time_frame_snapshot: self.time_frame_snapshot,
            // TODO pass reference here
            trade: &closed_trade,
            trade_pnl: Some(trade_pnl),
        };
        pnl_report_data_rows.push(pnl_report_data_row.into());
        closed_trade
    }

    fn market_sim_data_since_entry_ts(&self, entry_ts: i64) -> LazyFrame {
        self.market_sim_df
            .clone()
            .lazy()
            .drop_rows_before_entry_ts(entry_ts)
    }

    // fn trade_object_request(&self, values: &TradeAndPreTradeValuesWithData) -> TradeRequestObject {
    //     let initial_balance = values
    //         .trade
    //         .as_ref()
    //         .and_then(|trade| Some(trade.initial_balance()));
    //     TradeRequestObject {
    //         pre_trade_values: values.pre_trade.clone(),
    //         initial_balance,
    //         market: self.market,
    //     }
    // }

    // fn compute_trade_values(
    //     &self,
    //     pre_trade_values: &RequiredPreTradeValuesWithData,
    // ) -> Option<TradeValuesWithData> {
    //     let calculator_builder: TradeValuesCalculatorBuilder = self.into();
    //     let (entry_ts, compute_entry_ts_if_none) = self.strategies.get_entry_ts(&pre_trade_values);
    //     if should_skip_computation(&entry_ts, compute_entry_ts_if_none) {
    //         // compute_entry_ts_if_none == false => if we don't have an entry_ts we don't have a trade
    //         // therefore we don't need to compute anything
    //         None
    //     } else {
    //         self.strategies
    //             .get_entry_price(&pre_trade_values)
    //             .and_then(|entry_price| {
    //                 calculator_builder
    //                     .with_entry_price(entry_price)
    //                     .with_entry_ts(entry_ts)
    //                     .build_and_compute()
    //             })
    //     }
    // }
}

// fn should_skip_computation(entry_ts: &Option<i64>, compute_entry_ts_if_none: bool) -> bool {
//     entry_ts.is_none() && !compute_entry_ts_if_none
// }

pub struct PnLReportDataRowCalculatorBuilder {
    strategy: Option<Vec<Arc<dyn Strategy + Send + Sync>>>,
    decision_policy: Option<Arc<dyn DecisionPolicy + Send + Sync>>,
    market_sim_data: Option<DataFrame>,
    pub pre_trade_data: Option<PreTradeData>,
    market: Option<MarketKind>,
    pub year: Option<u32>,
    pub time_frame_snapshot: Option<TimeFrameSnapshot>,
    pub market_sim_data_kind: Option<MarketSimulationDataKind>,
}

impl PnLReportDataRowCalculatorBuilder {
    pub fn new() -> Self {
        Self {
            strategy: None,
            decision_policy: None,
            market_sim_data: None,
            pre_trade_data: None,
            market: None,
            year: None,
            time_frame_snapshot: None,
            market_sim_data_kind: None,
        }
    }

    pub fn with_strategy(self, strategy: Vec<Arc<dyn Strategy + Send + Sync>>) -> Self {
        Self {
            strategy: Some(strategy),
            ..self
        }
    }

    pub fn with_decision_policy(
        self,
        decision_policy: Arc<dyn DecisionPolicy + Send + Sync>,
    ) -> Self {
        Self {
            decision_policy: Some(decision_policy),
            ..self
        }
    }

    pub fn with_market_sim_data(self, market_sim_data: DataFrame) -> Self {
        Self {
            market_sim_data: Some(market_sim_data),
            ..self
        }
    }

    pub fn with_pre_trade_data(self, pre_trade_data: PreTradeData) -> Self {
        Self {
            pre_trade_data: Some(pre_trade_data),
            ..self
        }
    }

    pub fn with_market(self, market: MarketKind) -> Self {
        Self {
            market: Some(market),
            ..self
        }
    }

    pub fn with_year(self, year: u32) -> Self {
        Self {
            year: Some(year),
            ..self
        }
    }

    pub fn with_time_frame_snapshot(self, snapshot: TimeFrameSnapshot) -> Self {
        Self {
            time_frame_snapshot: Some(snapshot),
            ..self
        }
    }

    pub fn with_market_sim_data_kind(self, market_sim_data_kind: MarketSimulationDataKind) -> Self {
        Self {
            market_sim_data_kind: Some(market_sim_data_kind),
            ..self
        }
    }

    fn compute_pre_trade_values(&self) -> RequiredPreTradeValuesWithData {
        let calculator_builder: PreTradeValuesCalculatorBuilder = self.into();
        let required_pre_trade_values = self
            .strategy
            .as_ref()
            .unwrap()
            .iter()
            .filter_map(|s| s.get_required_pre_trade_values())
            .collect();
        calculator_builder
            .with_required_pre_trade_values(required_pre_trade_values)
            .build_and_compute()
    }

    pub fn build(self) -> PnLReportDataRowCalculator {
        let pre_trade_values_with_data = self.compute_pre_trade_values();
        let market_sim_df = self.market_sim_data.unwrap();
        let sim_data = SimulationDataBuilder::new()
            // .with_ohlc_candle(market_sim_df.clone())
            .with_pre_trade_values_with_data(pre_trade_values_with_data)
            .with_market_kind(self.market.unwrap())
            .with_market_sim_data_kind(self.market_sim_data_kind.unwrap())
            .build()
            .unwrap();
        PnLReportDataRowCalculator {
            strategies: self.strategy.unwrap(),
            decision_policy: self.decision_policy.unwrap(),
            sim_data,
            market: MarketDataFrame(market_sim_df.clone()).try_into().unwrap(),
            market_sim_df,
            year: self.year.unwrap(),
            time_frame_snapshot: self.time_frame_snapshot.unwrap(),
        }
    }

    pub fn build_and_compute(self) -> Vec<LazyFrame> {
        self.build().compute()
    }
}
