use crate::{data_frame_operations::io_operations::save_df_as_csv, enums::markets::MarketKind};
use polars::prelude::DataFrame;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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
