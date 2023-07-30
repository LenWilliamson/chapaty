use crate::{
    chapaty,
    converter::any_value::AnyValueConverter,
    data_provider::DataProvider,
    enums::{column_names::DataProviderColumnKind, markets::MarketKind},
};

use polars::prelude::{df, DataFrame, NamedFrom};
use rayon::{iter::ParallelIterator, prelude::IntoParallelIterator};
use std::{collections::HashMap, sync::Arc};

pub struct Tpo {
    data_provider: Arc<dyn DataProvider + Send + Sync>,
    max_digits: i32,
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

    /// TODO wrong step size. For 6E we move in 0.00005 steps and not 0.00001 steps.
    ///
    /// This function computes the market profile described in https://www.vtad.de/lexikon/market-profile/,
    /// for the given DataFrame and sorts it by price in ascending order. The values for the price columns are not rounded.
    ///
    /// # Arguments
    /// * `df` - DataFrame we want to compute the volume profile for
    /// * `max_digits` - accuracy of available trade price.
    ///    * BtcUsdt (two digits for cents, e.g. 1258.33)
    ///    * 6e (five digits for ticks, e.g. 1.39450)
    fn tpo(&self, df: DataFrame) -> DataFrame {
        let dp = self.data_provider.clone();
        let max_digits = self.max_digits;

        // Get index of respective columns in `DataFrame`
        let high_idx = dp.column_name_as_int(&DataProviderColumnKind::High);
        let low_idx = dp.column_name_as_int(&DataProviderColumnKind::Low);

        // Get a reference to the respective columns
        let highs = &df.get_columns()[high_idx];
        let lows = &df.get_columns()[low_idx];

        // Create a `Hashmap` to compute the time price opportunities (tpos)
        let mut tpos = HashMap::<i32, (f64, f64)>::new();

        std::iter::zip(highs.iter(), lows.iter()).for_each(|(highw, loww)| {
            // Unwrap Anyvalue h
            let h = highw.unwrap_float64();
            // Unwrap Anyvalue l
            let l = loww.unwrap_float64();

            // Multiply h, l * 10^max-digits and transform to i32
            let high = (h * 10.0_f64.powi(max_digits)) as i32;
            let low = (l * 10.0_f64.powi(max_digits)) as i32;

            // Iterate for x in low..=high and add to hashmap
            // TODO https://docs.rs/ordered-float/latest/ordered_float/struct.OrderedFloat.html (improvement?)
            for x in low..=high {
                tpos.entry(x)
                    .and_modify(|(_, qx)| *qx += 1.0)
                    .or_insert((f64::try_from(x).unwrap() * 10.0_f64.powi(-max_digits), 1.0));
            }
        });

        // Create volume profile `DataFrame`
        let (px, qx): (Vec<_>, Vec<_>) = tpos.values().cloned().unzip();
        let result = df!(
            "px" => &px,
            "qx" => &qx,
        );
        result.unwrap().sort(["px"], false, false).unwrap()
    }
}

pub struct TpoBuilder {
    data_provider: Option<Arc<dyn DataProvider + Send + Sync>>,
    market: Option<MarketKind>,
}

impl TpoBuilder {
    pub fn new() -> Self {
        Self {
            data_provider: None,
            market: None,
        }
    }

    pub fn with_data_provider(self, data_provider: Arc<dyn DataProvider + Send + Sync>) -> Self {
        Self {
            data_provider: Some(data_provider),
            ..self
        }
    }

    pub fn with_market(self, market: MarketKind) -> Self {
        Self {
            market: Some(market),
            ..self
        }
    }

    pub fn build(self) -> Tpo {
        Tpo {
            data_provider: self.data_provider.unwrap(),
            max_digits: self.market.unwrap().decimal_places(),
        }
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn tpo_test() {
        
    }
}
