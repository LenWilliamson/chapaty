use std::sync::Arc;

use chapaty::{
    TimeInterval,
    data_provider::{cme::Cme, DataProvider},

    strategy::{ppp::Ppp, StopLoss, Strategy, TakeProfit}, StopLossKind, TakeProfitKind,
};

pub fn setup_strategy() -> Arc<dyn Strategy + Send + Sync> {
    let mut strategy = Ppp::new();
    let sl = StopLoss {
        condition: StopLossKind::PrevLow,
        offset: 1.0,
    };
    let tp = TakeProfit {
        condition: TakeProfitKind::PrevClose,
        offset: 0.00005 * 20.0,
    };
    strategy.set_stop_loss(sl);
    strategy.set_take_profit(tp);

    Arc::new(strategy)
}

pub fn setup_data_provider() -> Arc<dyn DataProvider + Send + Sync> {
    Arc::new(Cme::new())
}

pub fn setup_time_interval() -> TimeInterval {
    TimeInterval {
        start_day: chrono::Weekday::Mon,
        start_h: 1,
        end_day: chrono::Weekday::Fri,
        end_h: 23,
    }
}
