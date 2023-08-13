use std::collections::HashMap;

use polars::prelude::DataFrame;
use serde::{Deserialize, Serialize};

use crate::{
    pnl::pnl_statement_agg_years::{PnLSnapshotAggYears, PnLStatementAggYears},
    MarketKind, data_frame_operations::io_operations::save_df_as_csv,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PerformanceReportsAggYears {
    pub markets: Vec<MarketKind>,
    pub reports: HashMap<MarketKind, PerformanceReportAggYears>,
}

impl PerformanceReportsAggYears {
    pub fn save_as_csv(&self, file_name: &str) {
        self.reports
            .iter()
            .for_each(|(market, performance_report)| {
                save_df_as_csv(
                    &mut performance_report.report.clone(),
                    &format!("{file_name}_{market}_all_years_performance_report"),
                )
            })
    }
}

struct PerformanceReportsAggYearsBuilder {
    pub markets: Vec<MarketKind>,
    pub reports: HashMap<MarketKind, PerformanceReportAggYears>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PerformanceReportAggYears {
    pub market: MarketKind,
    pub report: DataFrame,
}

impl From<PnLStatementAggYears> for PerformanceReportsAggYears {
    fn from(value: PnLStatementAggYears) -> Self {
        value
            .pnl_data
            .into_iter()
            .map(|(_, pnl_report)| PnLSnapshotAggYears {
                pnl_report,
                strategy_name: value.strategy_name.clone(),
            })
            .map(|pnl_snapshot| pnl_snapshot.into())
            .collect()
    }
}

impl PerformanceReportsAggYearsBuilder {
    fn new() -> Self {
        Self {
            markets: Vec::new(),
            reports: HashMap::new(),
        }
    }

    fn append(self, report: PerformanceReportAggYears) -> Self {
        let market = report.market;
        let mut markets = self.markets;
        markets.push(market);

        let mut reports = self.reports;
        reports.insert(market, report);

        Self { markets, reports }
    }

    fn build(self) -> PerformanceReportsAggYears {
        PerformanceReportsAggYears {
            markets: self.markets,
            reports: self.reports,
        }
    }
}

impl FromIterator<PerformanceReportAggYears> for PerformanceReportsAggYears {
    fn from_iter<T: IntoIterator<Item = PerformanceReportAggYears>>(iter: T) -> Self {
        iter.into_iter()
            .fold(PerformanceReportsAggYearsBuilder::new(), |builder, i| {
                builder.append(i)
            })
            .build()
    }
}
