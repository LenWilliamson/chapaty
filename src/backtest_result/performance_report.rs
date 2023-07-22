use crate::enums::markets::MarketKind;
use std::collections::HashMap;

use polars::prelude::DataFrame;

use super::pnl_statement::PnLStatement;
use super::trade_breakdown_report::PnLSnapshot;
#[derive(Debug)]
pub struct PerformanceReports {
    pub markets: Vec<MarketKind>,
    pub reports: HashMap<MarketKind, PerformanceReport>,
}
#[derive(Debug)]
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
