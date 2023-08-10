use crate::{
    chapaty,
    converter::{any_value::AnyValueConverter, market_decimal_places::MyDecimalPlaces},
    enums::{column_names::DataProviderColumnKind, markets::MarketKind},
};

use polars::prelude::{df, AnyValue, DataFrame, IntoLazy, NamedFrom};
use rayon::{iter::ParallelIterator, prelude::IntoParallelIterator};
use std::{collections::HashMap, convert::identity};

pub struct Tpo {
    market: MarketKind,
}

impl Tpo {
    pub fn from_df_map(
        &self,
        df_map: chapaty::types::DataFrameMap,
    ) -> chapaty::types::DataFrameMap {
        df_map
            .into_par_iter()
            .map(|(key, df)| (key, self.tpo(df)))
            .collect()
    }

    fn tpo(&self, df: DataFrame) -> DataFrame {
        // Get index of respective columns in `DataFrame`
        let high_idx = df
            .find_idx_by_name(DataProviderColumnKind::High.to_string().as_str())
            .unwrap();
        let low_idx = df
            .find_idx_by_name(DataProviderColumnKind::Low.to_string().as_str())
            .unwrap();

        // Get a reference to the respective columns
        let highs = &df.get_columns()[high_idx];
        let lows = &df.get_columns()[low_idx];

        // Create a `Hashmap` to compute the time price opportunities (tpos)
        let tpos = std::iter::zip(lows.iter(), highs.iter())
            .fold(HashMap::<String, (f64, f64)>::new(), |tpos, interval| {
                self.compute_tpo_for_interval(tpos, interval)
            });

        // Create volume profile `DataFrame`
        let (px, qx): (Vec<_>, Vec<_>) = tpos.values().cloned().unzip();
        let result = df!(
            "px" => &px,
            "qx" => &qx,
        );
        result
            .unwrap()
            .lazy()
            .sort("px", Default::default())
            .collect()
            .unwrap()
    }

    fn compute_tpo_for_interval<'a>(
        &self,
        mut tpos: HashMap<String, (f64, f64)>,
        interval: (AnyValue<'a>, AnyValue<'a>),
    ) -> HashMap<String, (f64, f64)> {
        let mut x = initalize_start_value(&interval);
        let end = upper_bound_from_interval(&interval);
        while is_current_value_still_in_inteval(x, end) {
            tpos.entry(self.create_key(x))
                .and_modify(|(_, qx)| *qx += 1.0)
                .or_insert((x.round_to_n_decimal_places(self.max_digits()), 1.0));
            x += self.market.tick_step_size().map_or_else(|| 0.01, identity);
        }

        // add possible last entry
        tpos.entry(self.create_key(x))
            .and_modify(|(_, qx)| *qx += 1.0)
            .or_insert((x.round_to_n_decimal_places(self.max_digits()), 1.0));

        tpos
    }

    fn max_digits(&self) -> i32 {
        self.market.decimal_places()
    }
    fn create_key(&self, x: f64) -> String {
        let res = match self.market {
            MarketKind::BtcUsdt => format!("{:.2}", x),
            MarketKind::EurUsdFuture => format!("{:.5}", x),
            MarketKind::AudUsdFuture => format!("{:.5}", x),
            MarketKind::GbpUsdFuture => format!("{:.4}", x),
            MarketKind::CadUsdFuture => format!("{:.5}", x),
            MarketKind::YenUsdFuture => format!("{:.7}", x),
            MarketKind::NzdUsdFuture => format!("{:.5}", x),
            MarketKind::BtcUsdFuture => format!("{:.0}", x),
        };
        res
    }
}

fn initalize_start_value<'a>(interval: &(AnyValue<'a>, AnyValue<'a>)) -> f64 {
    interval.0.unwrap_float64()
}

fn upper_bound_from_interval<'a>(interval: &(AnyValue<'a>, AnyValue<'a>)) -> f64 {
    interval.1.unwrap_float64()
}

fn is_current_value_still_in_inteval(current: f64, upper_bound: f64) -> bool {
    current <= upper_bound
}

#[derive(Clone)]
pub struct TpoBuilder {
    market: Option<MarketKind>,
}

impl TpoBuilder {
    pub fn new() -> Self {
        Self { market: None }
    }

    pub fn with_market(self, market: MarketKind) -> Self {
        Self {
            market: Some(market),
            ..self
        }
    }

    pub fn build(self) -> Tpo {
        Tpo {
            market: self.market.unwrap(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cloud_api::api_for_unit_tests::download_df;

    #[tokio::test]
    async fn test_tpo_cme() {
        let df_ohlc_data = download_df(
            "chapaty-ai-hdb-test".to_string(),
            "cme/ohlc/ohlc_data_for_tpo_test.csv".to_string(),
        )
        .await;

        let target = download_df(
            "chapaty-ai-test".to_string(),
            "ppp/_test_data_files/target_ohlc_tpo_for_tpo_test.csv".to_string(),
        )
        .await;

        let tpo = Tpo {
            market: MarketKind::EurUsdFuture,
        };
        assert_eq!(target, tpo.tpo(df_ohlc_data))
    }

    #[tokio::test]
    async fn test_tpo_binance() {
        let df_ohlc_data = download_df(
            "chapaty-ai-hdb-test".to_string(),
            "binance/ohlcv/ohlc_data_for_tpo_test.csv".to_string(),
        )
        .await;

        let target = download_df(
            "chapaty-ai-test".to_string(),
            "ppp/btcusdt/2022/Mon1h0m-Fri23h0m/1d/target_binance_tpo_from_ohlc.csv".to_string(),
        )
        .await;

        let tpo = Tpo {
            market: MarketKind::BtcUsdt,
        };
        assert_eq!(target, tpo.tpo(df_ohlc_data))
    }
}
