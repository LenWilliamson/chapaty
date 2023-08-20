use std::collections::HashMap;

use polars::prelude::{DataFrame, IntoLazy, LazyFrame};
use serde::{Deserialize, Serialize};

use crate::{
    data_frame_operations::io_operations::save_df_as_csv,
    lazy_frame_operations::trait_extensions::{MyLazyFrameOperations, MyLazyFrameVecOperations},
    MarketKind,
};

use super::pnl_report::PnLReports;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PnLReportsAggMarkets {
    pub markets: Vec<MarketKind>,
    pub years: Vec<u32>,
    pub reports: HashMap<u32, DataFrame>,
}

impl PnLReportsAggMarkets {
    pub fn save_as_csv(&self, file_name: &str) {
        self.reports.iter().for_each(|(year, pnl)| {
            save_df_as_csv(
                &mut pnl.clone(),
                &format!("{file_name}_all_markets_{year}_pnl"),
            )
        })
    }
}

pub struct PnLReportsAggMarketsBuilder {
    pub markets: Option<Vec<MarketKind>>,
    pub years: Option<Vec<u32>>,
    pub pnl_data: Option<HashMap<MarketKind, PnLReports>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PnLReportAggMarkets {
    pub markets: Vec<MarketKind>,
    pub year: u32,
    pub strategy: String,
    pub pnl: DataFrame,
}

impl PnLReportsAggMarketsBuilder {
    fn new() -> Self {
        Self {
            markets: None,
            years: None,
            pnl_data: None,
        }
    }

    fn with_markets(self, markets: Vec<MarketKind>) -> Self {
        Self {
            markets: Some(markets),
            ..self
        }
    }

    fn with_years(self, years: Vec<u32>) -> Self {
        Self {
            years: Some(years),
            ..self
        }
    }

    fn with_pnl_data_to_be_aggregated(self, pnl_data: HashMap<MarketKind, PnLReports>) -> Self {
        Self {
            pnl_data: Some(pnl_data),
            ..self
        }
    }

    fn build(self) -> PnLReportsAggMarkets {
        let reports = self.aggregate_pnl_data_by_markets();
        PnLReportsAggMarkets {
            markets: self.markets.unwrap(),
            years: self.years.unwrap(),
            reports,
        }
    }

    fn aggregate_pnl_data_by_markets(&self) -> HashMap<u32, DataFrame> {
        self.years
            .as_ref()
            .unwrap()
            .iter()
            .map(|year| (*year, self.get_agg_pnl_report_in_year(year)))
            .collect()
    }

    fn get_agg_pnl_report_in_year(&self, year: &u32) -> DataFrame {
        let pnl_data = self.pnl_data.as_ref().unwrap();
        pnl_data
            .values()
            .map(|pnl_reports| pnl_reports.reports.get(year).unwrap().clone().lazy())
            .collect::<Vec<LazyFrame>>()
            .concatenate_to_lazy_frame()
            .sort_by_date()
            .collect()
            .unwrap()
    }
}

impl From<HashMap<MarketKind, PnLReports>> for PnLReportsAggMarkets {
    fn from(value: HashMap<MarketKind, PnLReports>) -> Self {
        let markets: Vec<MarketKind> = value.keys().map(|k| *k).collect();
        PnLReportsAggMarketsBuilder::new()
            .with_markets(markets.clone())
            .with_years(value.get(&markets[0]).unwrap().years.clone())
            .with_pnl_data_to_be_aggregated(value)
            .build()
    }
}
