use polars::prelude::{col, DataFrame, IntoLazy};

use crate::{
    calculator::trade_values_calculator::TradeValuesCalculator,
    converter::any_value::AnyValueConverter, DataProviderColumnKind, MarketSimulationDataKind,
};

#[derive(PartialEq, Debug, Clone, Copy)]
pub struct InitialBalance {
    pub high: f64,
    pub low: f64,
}

pub struct InitialBalanceCalculator {
    pub df: DataFrame,
    pub market_simulation_data_kind: MarketSimulationDataKind,
}

impl From<&TradeValuesCalculator> for InitialBalanceCalculator {
    fn from(value: &TradeValuesCalculator) -> Self {
        Self {
            df: value.market_sim_data.clone(),
            market_simulation_data_kind: value.market_sim_data_kind,
        }
    }
}

impl InitialBalanceCalculator {
    /// Computes the initial balance described in <https://www.vtad.de/lexikon/market-profile/>
    pub fn initial_balance(&self) -> InitialBalance {
        match self.market_simulation_data_kind {
            MarketSimulationDataKind::Ohlc1h | MarketSimulationDataKind::Ohlcv1h => {
                self.initial_balance_from_first_k_candles(1)
            }
            MarketSimulationDataKind::Ohlc30m | MarketSimulationDataKind::Ohlcv30m => {
                self.initial_balance_from_first_k_candles(2)
            }
            MarketSimulationDataKind::Ohlc1m | MarketSimulationDataKind::Ohlcv1m => {
                self.initial_balance_from_first_k_candles(60)
            }
        }
    }

    fn initial_balance_from_first_k_candles(&self, k: u32) -> InitialBalance {
        let ots = DataProviderColumnKind::OpenTime.to_string();
        let high = DataProviderColumnKind::High.to_string();
        let low = DataProviderColumnKind::Low.to_string();
        let res = self
            .df
            .clone()
            .lazy()
            .top_k(k, &[col(&ots)], [true], true, true)
            .select(&[col(&high).max(), col(&low).min()])
            .collect()
            .unwrap();
        let row = res.get(0).unwrap();
        InitialBalance {
            high: row[0].unwrap_float64(),
            low: row[1].unwrap_float64(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{cloud_api::api_for_unit_tests::download_df, MarketSimulationDataKind};

    #[tokio::test]
    async fn test_initial_balance_ohlc30m() {
        let df = download_df(
            "chapaty-ai-hdb-test".to_string(),
            "cme/ohlc/ohlc_data_for_tpo_test.csv".to_string(),
        )
        .await;

        let calculator = InitialBalanceCalculator {
            df,
            market_simulation_data_kind: MarketSimulationDataKind::Ohlc30m,
        };
        let initial_balance = calculator.initial_balance();
        let target = InitialBalance {
            high: 1.16275,
            low: 1.16095,
        };
        assert_eq!(target, initial_balance);
    }
    
    #[tokio::test]
    async fn test_initial_balance_from_first_k_candles() {
        let df = download_df(
            "chapaty-ai-hdb-test".to_string(),
            "cme/ohlc/ohlc_data_for_tpo_test.csv".to_string(),
        )
        .await;

        let calculator = InitialBalanceCalculator {
            df,
            market_simulation_data_kind: MarketSimulationDataKind::Ohlc30m,
        };
        let initial_balance = calculator.initial_balance_from_first_k_candles(10);
        let target = InitialBalance {
            high: 1.16275,
            low: 1.15835,
        };
        assert_eq!(target, initial_balance);
    }

    #[tokio::test]
    async fn test_initial_balance_ohlc1h() {
        let df = download_df(
            "chapaty-ai-test".to_string(),
            "ppp/_test_data_files/trade_data.csv".to_string(),
        ).await;
        let calculator = InitialBalanceCalculator {
            df,
            market_simulation_data_kind: MarketSimulationDataKind::Ohlc1h
        };

        let target = InitialBalance {
            high: 38484.84,
            low: 38127.76,
        };
        assert_eq!(target, calculator.initial_balance())
    }
}
