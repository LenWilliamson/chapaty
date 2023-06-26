use std::sync::Arc;

use chapaty::{
    common::time_interval::TimeInterval,
    enums::{
        markets::{GranularityKind, MarketKind},
    },
    producers::{
        ninja::Ninja,
    },
    streams::backtester::Backtester,
};

pub fn setup() -> Backtester {
    // Initialze parameter for backtesting the strategy
    let data_provider = Ninja::new(std::path::PathBuf::from("trust-data"));
    let dp = Arc::new(data_provider);
    let years = vec![2022];
    let market = MarketKind::EurUsd;
    let granularity = GranularityKind::Daily;
    let ti: Option<TimeInterval> = None;


    let backtester = Backtester {
        dp,
        years,
        market: vec![market],
        granularity: vec![granularity],
        ti,
    };
    backtester
}
