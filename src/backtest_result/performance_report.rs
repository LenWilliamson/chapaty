use super::pnl_statement::{PnLSnapshot, PnLStatement};
use crate::data_frame_operations::save_df_as_csv;
use crate::enums::markets::MarketKind;
use polars::prelude::DataFrame;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize)]
pub struct PerformanceReports {
    pub markets: Vec<MarketKind>,
    pub reports: HashMap<MarketKind, PerformanceReport>,
}

impl PerformanceReports {
    pub fn save_as_csv(&self, file_name: &str) {
        self.reports
            .iter()
            .for_each(|(market, performance_report)| {
                save_df_as_csv(
                    &mut performance_report.report.clone(),
                    &format!("{file_name}_{market}_performance_report"),
                )
            })
    }
}
#[derive(Debug, Serialize, Deserialize)]
pub struct PerformanceReport {
    pub market: MarketKind,
    pub report: DataFrame,
}

impl From<PnLStatement> for PerformanceReports {
    fn from(value: PnLStatement) -> Self {
        value
            .pnl_data
            .into_iter()
            .map(|(_, pnl_reports)| PnLSnapshot {
                pnl_reports,
                bot: value.strategy,
            })
            .map(|pnl_snapshot| pnl_snapshot.into())
            .collect()
    }
}

struct PerformanceReportsBuilder {
    markets: Vec<MarketKind>,
    reports: HashMap<MarketKind, PerformanceReport>,
}

impl PerformanceReportsBuilder {
    fn new() -> Self {
        Self {
            markets: Vec::new(),
            reports: HashMap::new(),
        }
    }

    fn append(self, report: PerformanceReport) -> Self {
        let market = report.market;
        let mut markets = self.markets;
        markets.push(market);

        let mut reports = self.reports;
        reports.insert(market, report);

        Self { markets, reports }
    }

    fn build(self) -> PerformanceReports {
        PerformanceReports {
            markets: self.markets,
            reports: self.reports,
        }
    }
}

impl FromIterator<PerformanceReport> for PerformanceReports {
    fn from_iter<T: IntoIterator<Item = PerformanceReport>>(iter: T) -> Self {
        iter.into_iter()
            .fold(PerformanceReportsBuilder::new(), |builder, i| {
                builder.append(i)
            })
            .build()
    }
}
