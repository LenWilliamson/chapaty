pub mod news_counter;
pub mod news_rassler;
pub mod news_rassler_conf;
pub mod ppp;

use crate::{
    calculator::pre_trade_values_calculator::RequiredPreTradeValuesWithData,
    dfa::{
        market_simulation_data::{Market, SimulationData},
        states::{ActivationEvent, Active, CloseEvent, Trade},
    },
    enums::{
        bot::{StopLossKind, TakeProfitKind},
        error::ChapatyErrorKind,
        indicator::TradingIndicatorKind,
        strategy::StrategyKind,
        trade_and_pre_trade::{PreTradeDataKind, TradeDirectionKind},
    },
    MarketSimulationDataKind, NewsKind,
};
use chrono::NaiveDate;
use mockall::automock;
use std::{collections::HashSet, fmt::Debug, str::FromStr};

#[derive(Debug, Clone, Copy)]
pub struct StopLoss {
    pub kind: StopLossKind,
    /// Offset is used by bot diffrently, can be USD or percentage, etc..
    pub offset: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct TakeProfit {
    pub kind: TakeProfitKind,
    /// Offset is used by bot diffrently, can be USD or percentage, etc..
    pub offset: f64,
}

#[derive(Clone, Default)]
pub struct RequriedPreTradeValues {
    pub market_values: Vec<PreTradeDataKind>,
    pub trading_indicators: Vec<TradingIndicatorKind>,
}

impl FromIterator<RequriedPreTradeValues> for RequriedPreTradeValues {
    fn from_iter<T: IntoIterator<Item = RequriedPreTradeValues>>(iter: T) -> Self {
        let init = RequriedPreTradeValues {
            market_values: Vec::new(),
            trading_indicators: Vec::new(),
        };
        iter.into_iter().fold(init, |mut acc, item| {
            acc.market_values.extend(item.market_values);
            acc.trading_indicators.extend(item.trading_indicators);
            acc
        })
    }
}

/// Represents a trading strategy that can be used in a market simulation environment.
/// The strategy defines the logic for trade activation, cancellation, and various
/// required preconditions and metadata for simulation.
#[automock]
pub trait Strategy: Debug {
    /// Computes the necessary pre-trade values over a defined time horizon.
    ///
    /// This function calculates relevant pre-trade data based on the specific needs
    /// of the strategy. These values may include metrics from the previous day, week,
    /// or month, depending on the strategy’s time frame. The purpose is to provide
    /// convenience for strategy developers so they don’t need to manually compute
    /// these values during simulation.
    ///
    /// The returned `RequiredPreTradeValues` contains any historical market information
    /// or other indicators the strategy depends on to make trading decisions.
    fn get_required_pre_trade_values(&self) -> Option<RequriedPreTradeValues>;

    /// Specifies the type of market simulation data required by the strategy.
    ///
    /// This function determines what kind of market data the strategy will operate on.
    /// The data could be minute-level OHLCV candles (`ohlcv1m`), 5-minute candles
    /// (`ohlcv5m`), or tick data, among others. The chosen data format influences
    /// the granularity of the simulation and how the strategy processes the incoming
    /// market events during backtesting.
    ///
    /// # Warning
    ///
    /// If the chosen time frame is too large (e.g., using `ohlcv1m` or larger) and
    /// the strategy uses very tight stop-loss or take-profit margins relative to the
    /// entry price, the backtesting software may be unable to accurately determine
    /// whether trades are winners or losers if both the entry and exit signals occur
    /// within the same candle.
    ///
    /// In such cases, it's recommended to manually verify trade results afterward
    /// or, if available, use more granular data such as tick data to improve accuracy.
    fn get_market_simulation_data_kind(&self) -> MarketSimulationDataKind;

    /// # TODO Think about passing the `Trade<State>` as well?
    /// Checks for a trade activation event based on the provided simulation event.
    ///
    /// This function is the core of the strategy's logic for trade entry. It evaluates
    /// each `SimulationEvent`, which includes the current market state, pre-trade values,
    /// and other relevant data. When the conditions for entering a trade are met,
    /// the function returns an `ActivationEvent`, signaling the software to open a
    /// position. If no entry signal is detected, it returns `None`, meaning no action
    /// should be taken at this time.
    ///
    /// This function is called continuously as market events (e.g., price candles)
    /// are passed to the strategy.
    fn check_activation_event<'a>(
        &'a self,
        market_trajectory: &Box<Vec<Market>>,
        sim_data: &Box<SimulationData>,
    ) -> Option<ActivationEvent<'a>>;

    /// Checks for a trade cancellation or closure event based on the provided simulation event.
    ///
    /// This function is responsible for evaluating whether an active trade should be closed,
    /// either due to hitting a predefined condition (e.g., take profit, stop loss, or reversal)
    /// or other strategy-specific exit logic. If such a condition is met, it returns a
    /// `CloseEvent`, signaling the software to close the position. Otherwise, it returns
    /// `None`, indicating the trade should remain open.
    ///
    /// This function operates in a similar manner to `check_activation_event`, running
    /// continuously as market events are passed to the strategy.
    fn check_cancelation_event<'a>(
        &self,
        market_trajectory: &Box<Vec<Market>>,
        sim_data: &Box<SimulationData>,
        trade: &Trade<'a, Active>,
    ) -> Option<CloseEvent>;

    /// Filters trading based on specific economic news event dates.
    ///
    /// This function allows the strategy to define whether it should execute trades only
    /// on certain economic news event dates. If the strategy is designed to trade around
    /// specific economic events (such as Non-Farm Payrolls or interest rate decisions),
    /// it returns `Some(HashSet<Vec<NewsKind>>)` with the dates of those events. If the strategy
    /// does not depend on such events, it returns `None`, the strategy will not execute during major
    /// economic news events (e.g., FED Interest Rate releases), and backtesting will proceed
    /// without considering these dates.
    ///
    /// This allows for backtesting strategies under specific economic news events, helping
    /// evaluate how strategies behave under significant market-moving conditions like
    /// Non-Farm Payrolls (NFP) or other major economic announcements.
    fn filter_on_economic_news_event(&self) -> Option<Vec<NewsKind>>;

    /// # TODO join with get_name ???
    fn get_strategy_kind(&self) -> StrategyKind;
    /// # TODO join with get_strategy_kind ???
    fn get_name(&self) -> String;
}
