use chapaty::{
    data_provider::{cme::Cme, DataProvider},
    strategy::{ppp::PppBuilder, StopLoss, Strategy, TakeProfit},
    PriceHistogramKind, StopLossKind, TakeProfitKind, TimeInterval, TradingIndicatorKind,
};
use std::sync::Arc;

pub fn setup_strategy() -> Arc<dyn Strategy + Send + Sync> {
    let ppp_builder = PppBuilder::new();
    // let sl = StopLoss {
    //     kind: StopLossKind::PrevHighOrLow,
    //     offset: 0.0,
    // };
    // let tp = TakeProfit {
    //     kind: TakeProfitKind::PrevClose,
    //     offset: 0.0,
    // };
    let sl = StopLoss {
        kind: StopLossKind::PrevHighOrLow, // MAIN
        offset: 125_000.0, // MAIN previous 1.0
    };
    let tp = TakeProfit {
        kind: TakeProfitKind::PrevClose, // MAIN
        offset: 125.0, // MAIN previous 0.00005 * 20.0
    };

    let strategy = ppp_builder
        .with_stop_loss(sl)
        .with_take_profit(tp)
        .with_entry(TradingIndicatorKind::Poc(PriceHistogramKind::Tpo1m))
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
