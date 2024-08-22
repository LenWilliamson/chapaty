use super::pnl_report_data_row_calculator::PnLReportDataRowCalculator;
use crate::{
    bot::{pre_trade_data::PreTradeData, time_frame_snapshot::TimeFrameSnapshot},
    converter::any_value::AnyValueConverter,
    enums::{
        column_names::DataProviderColumnKind, indicator::TradingIndicatorKind, news::NewsKind,
        trade_and_pre_trade::PreTradeDataKind,
    },
    lazy_frame_operations::trait_extensions::MyLazyFrameOperations,
    strategy::RequriedPreTradeValues,
    trading_indicator::price_histogram::PriceHistogram,
    types::ohlc::OhlcCandle,
    MarketSimulationDataKind, PriceHistogramKind,
};
use chrono::{Duration, NaiveDate, NaiveTime};
use polars::prelude::{col, IntoLazy};
use std::collections::HashMap;

#[derive(Clone)]
pub struct RequiredPreTradeValuesWithData {
    pub market_values: HashMap<PreTradeDataKind, Option<OhlcCandle>>,
    pub indicator_values: HashMap<TradingIndicatorKind, f64>,
}

impl RequiredPreTradeValuesWithData {
    pub fn lowest_trade_price(&self) -> f64 {
        let ohlc_candle_opt = match self.market_values.get(&PreTradeDataKind::LowestTradePrice) {
            Some(ohlc_candle_opt) => ohlc_candle_opt,
            None => panic!("Pre Trade Value of Kind LowestTradePrice was not requested"),
        };

        ohlc_candle_opt
            .as_ref()
            .and_then(OhlcCandle::get_lowest_trade_price)
            .unwrap()
    }
    pub fn highest_trade_price(&self) -> f64 {
        let ohlc_candle_opt = match self.market_values.get(&PreTradeDataKind::HighestTradePrice) {
            Some(ohlc_candle_opt) => ohlc_candle_opt,
            None => panic!("Pre Trade Value of Kind HighestTradePrice was not requested"),
        };

        ohlc_candle_opt
            .as_ref()
            .and_then(OhlcCandle::get_highest_trade_price)
            .unwrap()
    }
    pub fn last_trade_price(&self) -> f64 {
        let ohlc_candle_opt = match self.market_values.get(&PreTradeDataKind::LastTradePrice) {
            Some(ohlc_candle_opt) => ohlc_candle_opt,
            None => panic!("Pre Trade Value of Kind LastTradePrice was not requested"),
        };

        ohlc_candle_opt
            .as_ref()
            .and_then(OhlcCandle::get_last_trade_price)
            .unwrap()
    }
    pub fn news_candle(&self, news_kind: &NewsKind, n: u32) -> Option<&OhlcCandle> {
        (0..=n).find_map(|offset| {
            self.market_values
                .get(&PreTradeDataKind::News(*news_kind, n - offset))
                .and_then(|candle| candle.as_ref())
        })
    }

    pub fn value_area_high(&self, ph: PriceHistogramKind) -> f64 {
        *self
            .indicator_values
            .get(&TradingIndicatorKind::ValueAreaHigh(ph))
            .unwrap()
    }
    pub fn value_area_low(&self, ph: PriceHistogramKind) -> f64 {
        *self
            .indicator_values
            .get(&TradingIndicatorKind::ValueAreaLow(ph))
            .unwrap()
    }
    pub fn poc(&self, ph: PriceHistogramKind) -> f64 {
        *self
            .indicator_values
            .get(&TradingIndicatorKind::Poc(ph))
            .unwrap()
    }
}

pub struct PreTradeValuesCalculator {
    year: u32,
    snapshot: TimeFrameSnapshot,
    pre_trade_data: PreTradeData,
    market_sim_data_kind: MarketSimulationDataKind,
    required_pre_trade_values: RequriedPreTradeValues,
}

impl PreTradeValuesCalculator {
    pub fn compute(&self) -> RequiredPreTradeValuesWithData {
        RequiredPreTradeValuesWithData {
            market_values: self.compute_market_values(),
            indicator_values: self.compute_indicator_values(),
        }
    }

    fn compute_market_values(&self) -> HashMap<PreTradeDataKind, Option<OhlcCandle>> {
        self.required_pre_trade_values
            .market_values
            .iter()
            .fold(HashMap::new(), |acc, val| {
                self.update_market_value_map(acc, val)
            })
    }

    fn update_market_value_map(
        &self,
        mut map: HashMap<PreTradeDataKind, Option<OhlcCandle>>,
        val: &PreTradeDataKind,
    ) -> HashMap<PreTradeDataKind, Option<OhlcCandle>> {
        match val {
            PreTradeDataKind::LastTradePrice => {
                let res = self.compute_last_trade_price();
                map.insert(PreTradeDataKind::LastTradePrice, Some(res));
            }
            PreTradeDataKind::LowestTradePrice => {
                let res = self.compute_lowest_trade_price();
                map.insert(PreTradeDataKind::LowestTradePrice, Some(res));
            }
            PreTradeDataKind::HighestTradePrice => {
                let res = self.compute_highest_trade_price();
                map.insert(PreTradeDataKind::HighestTradePrice, Some(res));
            }
            PreTradeDataKind::News(news_kind, n) => {
                let res = self.get_news_candle(news_kind, *n);
                map.insert(PreTradeDataKind::News(*news_kind, *n), res);
            }
        };

        map
    }

    fn compute_indicator_values(&self) -> HashMap<TradingIndicatorKind, f64> {
        self.required_pre_trade_values
            .trading_indicators
            .iter()
            .fold(HashMap::new(), |acc, val| {
                self.update_indicator_value_map(acc, val)
            })
    }

    fn update_indicator_value_map(
        &self,
        mut map: HashMap<TradingIndicatorKind, f64>,
        val: &TradingIndicatorKind,
    ) -> HashMap<TradingIndicatorKind, f64> {
        match val {
            TradingIndicatorKind::Poc(ph) => {
                map.insert(TradingIndicatorKind::Poc(*ph), self.get_poc(val));
            }
            TradingIndicatorKind::ValueAreaHigh(ph) | TradingIndicatorKind::ValueAreaLow(ph) => {
                let (value_area_low, value_area_high) = self.get_value_area(val);
                map.insert(TradingIndicatorKind::ValueAreaHigh(*ph), value_area_low);
                map.insert(TradingIndicatorKind::ValueAreaLow(*ph), value_area_high);
            }
        };

        map
    }

    fn get_poc(&self, indicator: &TradingIndicatorKind) -> f64 {
        let df = self
            .pre_trade_data
            .indicators
            .get(&indicator)
            .unwrap()
            .clone();
        let ph = PriceHistogram::new(df);
        ph.poc()
    }

    fn get_value_area(&self, indicator: &TradingIndicatorKind) -> (f64, f64) {
        let df = self
            .pre_trade_data
            .indicators
            .get(&indicator)
            .unwrap()
            .clone();
        let ph = PriceHistogram::new(df);
        ph.value_area(0.63)
    }

    fn get_news_candle(&self, news_kind: &NewsKind, n: u32) -> Option<OhlcCandle> {
        let df = self.pre_trade_data.market_sim_data.clone();
        let year = i32::try_from(self.year).unwrap();
        let week = u32::try_from(self.snapshot.get_calendar_week_as_int()).unwrap();
        let date = NaiveDate::from_isoywd_opt(year, week, self.snapshot.get_weekday()).unwrap();
        let ots = self.get_ots_of_n_candles_after_news_event(news_kind, n.into(), &date);
        df.lazy().find_ohlc_candle_by_ots(&date.and_time(ots))
    }

    fn get_ots_of_n_candles_after_news_event(
        &self,
        news_kind: &NewsKind,
        n: u32,
        date: &NaiveDate,
    ) -> NaiveTime {
        let n: i64 = n.into();
        match self.market_sim_data_kind {
            MarketSimulationDataKind::Ohlc1m | MarketSimulationDataKind::Ohlcv1m => {
                news_kind
                    .utc_time_daylight_saving_adjusted(date)
                    .overflowing_add_signed(Duration::minutes(n))
                    .0
            }
            MarketSimulationDataKind::Ohlc5m => {
                news_kind
                    .utc_time_daylight_saving_adjusted(date)
                    .overflowing_add_signed(Duration::minutes(n * 5))
                    .0
            }
            MarketSimulationDataKind::Ohlc15m => {
                news_kind
                    .utc_time_daylight_saving_adjusted(date)
                    .overflowing_add_signed(Duration::minutes(n * 15))
                    .0
            }
            MarketSimulationDataKind::Ohlc30m | MarketSimulationDataKind::Ohlcv30m => {
                news_kind
                    .utc_time_daylight_saving_adjusted(date)
                    .overflowing_add_signed(Duration::minutes(n * 30))
                    .0
            }
            MarketSimulationDataKind::Ohlc1h | MarketSimulationDataKind::Ohlcv1h => {
                news_kind
                    .utc_time_daylight_saving_adjusted(date)
                    .overflowing_add_signed(Duration::hours(n))
                    .0
            }
        }
    }

    fn compute_last_trade_price(&self) -> OhlcCandle {
        let df = self.pre_trade_data.market_sim_data.clone();

        let close = DataProviderColumnKind::Close.to_string();
        let filt = df.lazy().select([col(&close).last()]).collect().unwrap();

        let v = filt.get(0).unwrap();
        let last_trade_price = v[0].unwrap_float64();
        OhlcCandle::new().with_close(last_trade_price)
    }

    fn compute_lowest_trade_price(&self) -> OhlcCandle {
        let df = self.pre_trade_data.market_sim_data.clone();

        let low = DataProviderColumnKind::Low.to_string();
        let filt = df.lazy().select([col(&low).min()]).collect().unwrap();

        let v = filt.get(0).unwrap();
        let lowest_trade_price = v[0].unwrap_float64();
        OhlcCandle::new().with_low(lowest_trade_price)
    }

    fn compute_highest_trade_price(&self) -> OhlcCandle {
        let df = self.pre_trade_data.market_sim_data.clone();

        let high = DataProviderColumnKind::High.to_string();
        let filt = df.lazy().select([col(&high).max()]).collect().unwrap();

        let v = filt.get(0).unwrap();
        let highest_trade_price = v[0].unwrap_float64();
        OhlcCandle::new().with_high(highest_trade_price)
    }
}

pub struct PreTradeValuesCalculatorBuilder {
    year: Option<u32>,
    snapshot: Option<TimeFrameSnapshot>,
    pre_trade_data: Option<PreTradeData>,
    market_sim_data_kind: Option<MarketSimulationDataKind>,
    required_pre_trade_values: Option<RequriedPreTradeValues>,
}

impl From<&PnLReportDataRowCalculator> for PreTradeValuesCalculatorBuilder {
    fn from(value: &PnLReportDataRowCalculator) -> Self {
        Self {
            year: Some(value.year),
            snapshot: Some(value.time_frame_snapshot),
            pre_trade_data: Some(value.pre_trade_data.clone()),
            market_sim_data_kind: Some(value.market_sim_data_kind),
            required_pre_trade_values: None,
        }
    }
}

impl PreTradeValuesCalculatorBuilder {
    pub fn with_required_pre_trade_values(
        self,
        required_pre_trade_values: RequriedPreTradeValues,
    ) -> Self {
        Self {
            required_pre_trade_values: Some(required_pre_trade_values),
            ..self
        }
    }

    pub fn build(self) -> PreTradeValuesCalculator {
        PreTradeValuesCalculator {
            year: self.year.unwrap(),
            snapshot: self.snapshot.unwrap(),
            pre_trade_data: self.pre_trade_data.unwrap(),
            market_sim_data_kind: self.market_sim_data_kind.unwrap(),
            required_pre_trade_values: self.required_pre_trade_values.unwrap(),
        }
    }

    pub fn build_and_compute(self) -> RequiredPreTradeValuesWithData {
        self.build().compute()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        bot::time_frame_snapshot::TimeFrameSnapshotBuilder,
        calculator::pre_trade_values_calculator::PreTradeData,
        cloud_api::api_for_unit_tests::download_df,
    };
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_compute_last_trade_price() {
        let df = download_df(
            "chapaty-ai-test".to_string(),
            "ppp/_test_data_files/pre_trade_data.csv".to_string(),
        )
        .await;

        let pre_trade_data = PreTradeData {
            market_sim_data: df,
            indicators: HashMap::new(),
        };

        let required_pre_trade_values = RequriedPreTradeValues {
            market_values: Vec::new(),
            trading_indicators: Vec::new(),
        };

        let caclulator = PreTradeValuesCalculator {
            year: 0,
            snapshot: TimeFrameSnapshot::default(),
            market_sim_data_kind: MarketSimulationDataKind::Ohlc30m,
            pre_trade_data,
            required_pre_trade_values,
        };

        assert_eq!(
            43_578.87,
            caclulator
                .compute_last_trade_price()
                .get_last_trade_price()
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_compute_lowest_trade_price() {
        let df = download_df(
            "chapaty-ai-test".to_string(),
            "ppp/_test_data_files/pre_trade_data.csv".to_string(),
        )
        .await;

        let pre_trade_data = PreTradeData {
            market_sim_data: df,
            indicators: HashMap::new(),
        };

        let required_pre_trade_values = RequriedPreTradeValues {
            market_values: Vec::new(),
            trading_indicators: Vec::new(),
        };

        let caclulator = PreTradeValuesCalculator {
            year: 0,
            snapshot: TimeFrameSnapshot::default(),
            market_sim_data_kind: MarketSimulationDataKind::Ohlc30m,
            pre_trade_data,
            required_pre_trade_values,
        };

        assert_eq!(
            37_934.89,
            caclulator
                .compute_lowest_trade_price()
                .get_lowest_trade_price()
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_compute_highest_trade_price() {
        let df = download_df(
            "chapaty-ai-test".to_string(),
            "ppp/_test_data_files/pre_trade_data.csv".to_string(),
        )
        .await;

        let pre_trade_data = PreTradeData {
            market_sim_data: df,
            indicators: HashMap::new(),
        };

        let required_pre_trade_values = RequriedPreTradeValues {
            market_values: Vec::new(),
            trading_indicators: Vec::new(),
        };

        let caclulator = PreTradeValuesCalculator {
            year: 0,
            snapshot: TimeFrameSnapshot::default(),
            market_sim_data_kind: MarketSimulationDataKind::Ohlc30m,
            pre_trade_data,
            required_pre_trade_values,
        };

        assert_eq!(
            44_225.84,
            caclulator
                .compute_highest_trade_price()
                .get_highest_trade_price()
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_get_ots_of_n_candles_after_news_event() {
        let df = download_df(
            "chapaty-ai-hdb-test".to_string(),
            "cme/ohlc/unittest/6e-1h-nfp-testdata.csv".to_string(),
        )
        .await;

        let pre_trade_data = PreTradeData {
            market_sim_data: df,
            indicators: HashMap::new(),
        };

        let required_pre_trade_values = RequriedPreTradeValues {
            market_values: Vec::new(),
            trading_indicators: Vec::new(),
        };

        let caclulator = PreTradeValuesCalculator {
            year: 2022,
            snapshot: TimeFrameSnapshot::default(),
            market_sim_data_kind: MarketSimulationDataKind::Ohlc1m,
            pre_trade_data: pre_trade_data.clone(),
            required_pre_trade_values: required_pre_trade_values.clone(),
        };

        let date = NaiveDate::from_ymd_opt(2022, 12, 2).unwrap();

        assert_eq!(
            NaiveTime::from_hms_opt(13, 35, 0).unwrap(),
            caclulator.get_ots_of_n_candles_after_news_event(&NewsKind::UsaNFP, 5, &date)
        );

        assert_eq!(
            NaiveTime::from_hms_opt(13, 34, 0).unwrap(),
            caclulator.get_ots_of_n_candles_after_news_event(&NewsKind::UsaNFP, 4, &date)
        );

        assert_eq!(
            NaiveTime::from_hms_opt(13, 33, 0).unwrap(),
            caclulator.get_ots_of_n_candles_after_news_event(&NewsKind::UsaNFP, 3, &date)
        );

        assert_eq!(
            NaiveTime::from_hms_opt(13, 32, 0).unwrap(),
            caclulator.get_ots_of_n_candles_after_news_event(&NewsKind::UsaNFP, 2, &date)
        );

        assert_eq!(
            NaiveTime::from_hms_opt(13, 31, 0).unwrap(),
            caclulator.get_ots_of_n_candles_after_news_event(&NewsKind::UsaNFP, 1, &date)
        );

        assert_eq!(
            NaiveTime::from_hms_opt(13, 30, 0).unwrap(),
            caclulator.get_ots_of_n_candles_after_news_event(&NewsKind::UsaNFP, 0, &date)
        );

        let caclulator = PreTradeValuesCalculator {
            year: 2022,
            snapshot: TimeFrameSnapshot::default(),
            market_sim_data_kind: MarketSimulationDataKind::Ohlc1h,
            pre_trade_data: pre_trade_data.clone(),
            required_pre_trade_values: required_pre_trade_values.clone(),
        };

        assert_eq!(
            NaiveTime::from_hms_opt(18, 30, 0).unwrap(),
            caclulator.get_ots_of_n_candles_after_news_event(&NewsKind::UsaNFP, 5, &date)
        );

        assert_eq!(
            NaiveTime::from_hms_opt(17, 30, 0).unwrap(),
            caclulator.get_ots_of_n_candles_after_news_event(&NewsKind::UsaNFP, 4, &date)
        );

        assert_eq!(
            NaiveTime::from_hms_opt(16, 30, 0).unwrap(),
            caclulator.get_ots_of_n_candles_after_news_event(&NewsKind::UsaNFP, 3, &date)
        );

        assert_eq!(
            NaiveTime::from_hms_opt(15, 30, 0).unwrap(),
            caclulator.get_ots_of_n_candles_after_news_event(&NewsKind::UsaNFP, 2, &date)
        );

        assert_eq!(
            NaiveTime::from_hms_opt(14, 30, 0).unwrap(),
            caclulator.get_ots_of_n_candles_after_news_event(&NewsKind::UsaNFP, 1, &date)
        );

        assert_eq!(
            NaiveTime::from_hms_opt(13, 30, 0).unwrap(),
            caclulator.get_ots_of_n_candles_after_news_event(&NewsKind::UsaNFP, 0, &date)
        );
    }

    #[tokio::test]
    async fn test_get_get_news_candle() {
        let df = download_df(
            "chapaty-ai-hdb-test".to_string(),
            "cme/ohlc/unittest/6e-1h-nfp-testdata.csv".to_string(),
        )
        .await;

        let pre_trade_data = PreTradeData {
            market_sim_data: df,
            indicators: HashMap::new(),
        };

        let required_pre_trade_values = RequriedPreTradeValues {
            market_values: Vec::new(),
            trading_indicators: Vec::new(),
        };

        let snapshot = TimeFrameSnapshotBuilder::new(48).with_weekday(5).build();

        let caclulator = PreTradeValuesCalculator {
            year: 2022,
            snapshot,
            market_sim_data_kind: MarketSimulationDataKind::Ohlc1h,
            pre_trade_data: pre_trade_data.clone(),
            required_pre_trade_values: required_pre_trade_values.clone(),
        };

        // Not possible to get 13:30 News Candle on 1h charts
        assert_eq!(None, caclulator.get_news_candle(&NewsKind::UsaNFP, 0));

        assert_eq!(None, caclulator.get_news_candle(&NewsKind::UsaNFP, 5));

        let df = download_df(
            "chapaty-ai-hdb-test".to_string(),
            "cme/ohlc/unittest/6e-1m-nfp-testdata.csv".to_string(),
        )
        .await;

        let pre_trade_data = PreTradeData {
            market_sim_data: df,
            indicators: HashMap::new(),
        };

        let caclulator = PreTradeValuesCalculator {
            year: 2022,
            snapshot,
            market_sim_data_kind: MarketSimulationDataKind::Ohlc1m,
            pre_trade_data,
            required_pre_trade_values,
        };

        let ohlc_candle = OhlcCandle {
            open_ts: Some(1669987800000),
            open: Some(1.0616),
            high: Some(1.06175),
            low: Some(1.06135),
            close: Some(1.0614),
            close_ts: Some(1669987859999),
        };

        assert_eq!(
            Some(ohlc_candle),
            caclulator.get_news_candle(&NewsKind::UsaNFP, 0)
        );

        let ohlc_candle = OhlcCandle {
            open_ts: Some(1669987860000),
            open: Some(1.06135),
            high: Some(1.0614),
            low: Some(1.0613),
            close: Some(1.0614),
            close_ts: Some(1669987919999),
        };

        assert_eq!(
            Some(ohlc_candle),
            caclulator.get_news_candle(&NewsKind::UsaNFP, 1)
        );

        let ohlc_candle = OhlcCandle {
            open_ts: Some(1669987920000),
            open: Some(1.06135),
            high: Some(1.06155),
            low: Some(1.06125),
            close: Some(1.06135),
            close_ts: Some(1669987979999),
        };

        assert_eq!(
            Some(ohlc_candle),
            caclulator.get_news_candle(&NewsKind::UsaNFP, 2)
        );
    }
}
