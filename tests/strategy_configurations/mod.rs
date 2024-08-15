pub mod setup;
use chapaty::{
    data_provider::{cme::Cme, DataProvider}, strategy::{news::NewsBuilder, ppp::PppBuilder, StopLoss, Strategy, TakeProfit}, NewsKind, PriceHistogramKind, StopLossKind, TakeProfitKind, TimeInterval, TradingIndicatorKind
};
use std::sync::Arc;

pub fn setup_ppp_strategy() -> Arc<dyn Strategy + Send + Sync> {
    let ppp_builder = PppBuilder::new();
    let sl = StopLoss {
        kind: StopLossKind::PrevHighOrLow,
        offset: 0.0,
    };
    let tp = TakeProfit {
        kind: TakeProfitKind::PrevClose,
        offset: 0.0,
    };
    // let sl = StopLoss {
    //     kind: StopLossKind::PrevHighOrLow, // MAIN
    //     offset: 125_000.0, // MAIN previous 1.0
    // };
    // let tp = TakeProfit {
    //     kind: TakeProfitKind::PrevClose, // MAIN
    //     offset: 125.0, // MAIN previous 0.00005 * 20.0
    // };

    let strategy = ppp_builder
        .with_stop_loss(sl)
        .with_take_profit(tp)
        .with_entry(TradingIndicatorKind::Poc(PriceHistogramKind::Tpo1m))
        .build();
    Arc::new(strategy)
}

pub fn setup_news_strategy() -> Arc<dyn Strategy + Send + Sync> {
    let news_builder = NewsBuilder::new();
    let sl = StopLoss {
        kind: StopLossKind::PriceUponTradeEntry,
        offset: 0.3,
    };
    let tp = TakeProfit {
        kind: TakeProfitKind::PriceUponTradeEntry,
        offset: 0.9,
    };

    let strategy = news_builder
        .with_stop_loss(sl)
        .with_take_profit(tp)
        .with_news_kind(NewsKind::UsaNFP)
        .with_is_counter_trade(true)
        .with_number_candles_to_wait(5)
        .with_loss_to_win_ratio(2.0)
        .build();
    Arc::new(strategy)
}

pub fn setup_data_provider() -> Arc<dyn DataProvider + Send + Sync> {
    Arc::new(Cme)
}


pub fn setup_time_interval() -> TimeInterval {
    TimeInterval {
        start_day: chrono::Weekday::Mon,
        start_h: 1,
        end_day: chrono::Weekday::Fri,
        end_h: 23,
    }
}
