use std::path::PathBuf;

use regex::Regex;

use crate::{
    bot::time_interval::TimeInterval,
    enums::{
        bot::{DataProviderKind, StrategyKind},
        data::HdbSourceDirKind,
        markets::MarketKind,
    },
};

use super::file_path_with_fallback::FilePathWithFallback;

pub struct PathFinder {
    data_provider: DataProviderKind,
    strategy: StrategyKind,
    market: MarketKind,
    year: u32,
    time_interval: Option<TimeInterval>,
    time_frame: String,
}

impl PathFinder {
    pub fn get_file_path_with_fallback(
        &self,
        file_name: String,
        fallback_dir: &HdbSourceDirKind,
    ) -> FilePathWithFallback {
        let abs_file_path = self.get_absolute_file_path(file_name);
        let fallback_file_name = self.get_fallback_file_name(fallback_dir);
        FilePathWithFallback::new(abs_file_path, Regex::new(&fallback_file_name).unwrap())
    }

    pub fn get_absolute_file_path(&self, file_name: String) -> String {
        let mut base_path = self.get_base_path_to_cached_data();
        base_path.push(format!("{file_name}.csv"));
        base_path.to_str().unwrap().to_string()
    }

    fn get_base_path_to_cached_data(&self) -> PathBuf {
        let time_interval = self
            .time_interval
            .map_or_else(|| "none".to_string(), |v| v.to_string());
        let mut file_path = PathBuf::from(self.strategy.to_string());
        file_path.push(self.market.to_string());
        file_path.push(self.year.to_string());
        file_path.push(time_interval);
        file_path.push(self.time_frame.clone());
        file_path
    }

    fn get_fallback_file_name(&self, leaf_dir_kind: &HdbSourceDirKind) -> String {
        let data_provider = self.data_provider;
        let market = self.market;
        let year = self.year;
        match leaf_dir_kind {
            HdbSourceDirKind::AggTrades => {
                let leaf_dir = HdbSourceDirKind::AggTrades.to_string();
                format!(
                    r"{data_provider}/{leaf_dir}/{market}-aggTrades-{year}(-\d{{1,2}}){{0,2}}\.csv"
                )
            }
            HdbSourceDirKind::Tick => {
                let leaf_dir = HdbSourceDirKind::Tick.to_string();
                format!(r"{data_provider}/{leaf_dir}/{market}-tick-{year}(-\d{{1,2}}){{0,2}}\.csv")
            }
            ohlc_variant => {
                let (leaf_dir, ts) = ohlc_variant.split_ohlc_dir_in_parts();
                format!(r"{data_provider}/{leaf_dir}/{market}-{ts}-{year}(-\d{{1,2}}){{0,2}}\.csv")
            }
        }
    }
}

pub struct PathFinderBuilder {
    data_provider: Option<DataProviderKind>,
    strategy: Option<StrategyKind>,
    market: Option<MarketKind>,
    year: Option<u32>,
    time_interval: Option<TimeInterval>,
    time_frame: Option<String>,
}

impl PathFinderBuilder {
    pub fn new() -> Self {
        Self {
            data_provider: None,
            strategy: None,
            market: None,
            year: None,
            time_interval: None,
            time_frame: None,
        }
    }

    pub fn with_data_provider(self, data_provider: DataProviderKind) -> Self {
        Self {
            data_provider: Some(data_provider),
            ..self
        }
    }
    pub fn with_strategy(self, strategy: StrategyKind) -> Self {
        Self {
            strategy: Some(strategy),
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
    pub fn with_time_interval(self, time_interval: Option<TimeInterval>) -> Self {
        Self {
            time_interval,
            ..self
        }
    }
    pub fn with_time_frame(self, time_frame: String) -> Self {
        Self {
            time_frame: Some(time_frame),
            ..self
        }
    }

    pub fn build(self) -> PathFinder {
        PathFinder {
            data_provider: self.data_provider.unwrap(),
            strategy: self.strategy.unwrap(),
            market: self.market.unwrap(),
            year: self.year.unwrap(),
            time_interval: self.time_interval,
            time_frame: self.time_frame.unwrap(),
        }
    }
}
