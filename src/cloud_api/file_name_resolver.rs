use crate::{
    bot::indicator_data_pair::IndicatorDataPair,
    enums::{indicator::TradingIndicatorKind, data::HdbSourceDirKind},
};
#[derive(Clone)]
pub struct FileNameResolver {
    indicator_data_pair: Option<IndicatorDataPair>,
    simulation_data: HdbSourceDirKind,
}

impl FileNameResolver {
    pub fn new(simulation_data: HdbSourceDirKind) -> Self {
        Self {
            indicator_data_pair: None,
            simulation_data,
        }
    }

    pub fn with_indicator_data_pair(self, indicator_data_pair: IndicatorDataPair) -> Self {
        Self {
            indicator_data_pair: Some(indicator_data_pair),
            ..self
        }
    }

    pub fn get_filename(&self) -> String {
        self.indicator_data_pair.clone().map_or_else(
            || self.simulation_data.to_string(),
            |_| self.generate_file_name(),
        )
    }


    fn generate_file_name(&self) -> String {
        match self.indicator_data_pair.as_ref().unwrap().data {
            HdbSourceDirKind::Tick => self.trading_indicator_from_tick_data(),
            HdbSourceDirKind::AggTrades => self.trading_indicator_from_agg_trades_data(),
            ohlc_variant => self.trading_indicator_from_ohlc_variant(&ohlc_variant),
        }
    }

    fn trading_indicator_from_tick_data(&self) -> String {
        match self.indicator_data_pair.clone().unwrap().indicator {
            TradingIndicatorKind::Poc(_)
            | TradingIndicatorKind::VolumeAreaHigh(_)
            | TradingIndicatorKind::VolumeAreaLow(_) => format!("vol-tick"),
        }
    }

    fn trading_indicator_from_agg_trades_data(&self) -> String {
        match self.indicator_data_pair.clone().unwrap().indicator {
            TradingIndicatorKind::Poc(_)
            | TradingIndicatorKind::VolumeAreaHigh(_)
            | TradingIndicatorKind::VolumeAreaLow(_) => format!("vol-aggTrades"),
        }
    }

    fn trading_indicator_from_ohlc_variant(&self, ohlc_variant: &HdbSourceDirKind) -> String {
        match self.indicator_data_pair.clone().unwrap().indicator {
            TradingIndicatorKind::Poc(_)
            | TradingIndicatorKind::VolumeAreaHigh(_)
            | TradingIndicatorKind::VolumeAreaLow(_) => {
                format!("tpo-{}", ohlc_variant.split_ohlc_dir_in_parts().1)
            }
        }
    }
}
