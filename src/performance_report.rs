use std::collections::HashMap;

use polars::prelude::DataFrame;
use serde::{Deserialize, Serialize};

use crate::{data_frame_operations::io_operations::save_df_as_csv, MarketKind};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PerformanceReportAggMarkets {
    pub markets: Vec<MarketKind>,
    pub report: DataFrame,
}

impl PerformanceReportAggMarkets {
    pub fn save_as_csv(&self, file_name: &str) {
        save_df_as_csv(
            &mut self.report.clone(),
            &format!("{file_name}_all_markets_performance_report"),
        )
    }
}


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PerformanceReportsAggYears {
    pub markets: Vec<MarketKind>,
    pub reports: HashMap<MarketKind, DataFrame>,
}

impl PerformanceReportsAggYears {
    pub fn save_as_csv(&self, file_name: &str) {
        self.reports
            .iter()
            .for_each(|(market, performance_report)| {
                save_df_as_csv(
                    &mut performance_report.clone(),
                    &format!("{file_name}_{market}_all_years_performance_report"),
                )
            })
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PerformanceReports {
    pub markets: Vec<MarketKind>,
    pub reports: HashMap<MarketKind, DataFrame>,
}

impl PerformanceReports {
    pub fn save_as_csv(&self, file_name: &str) {
        self.reports
            .iter()
            .for_each(|(market, performance_report)| {
                save_df_as_csv(
                    &mut performance_report.clone(),
                    &format!("{file_name}_{market}_performance_report"),
                )
            })
    }
}
