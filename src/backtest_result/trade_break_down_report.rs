use crate::data_frame_operations::save_df_as_csv;

use std::collections::HashMap;

use super::pnl_statement::PnLSnapshot;
use super::pnl_statement::PnLStatement;
use crate::enums::markets::MarketKind;
use polars::prelude::DataFrame;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct TradeBreakDownReports {
    pub markets: Vec<MarketKind>,
    pub reports: HashMap<MarketKind, TradeBreakDownReport>,
}

impl TradeBreakDownReports {
    pub fn save_as_csv(&self, file_name: &str) {
        self.reports
            .iter()
            .for_each(|(market, trade_break_down_report)| {
                save_df_as_csv(
                    &mut trade_break_down_report.report.clone(),
                    &format!("{file_name}_{market}_trade_break_down_report"),
                )
            })
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TradeBreakDownReport {
    pub market: MarketKind,
    pub report: DataFrame,
}

impl From<PnLStatement> for TradeBreakDownReports {
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

struct TradeBreakDownReportsBuilder {
    markets: Vec<MarketKind>,
    reports: HashMap<MarketKind, TradeBreakDownReport>,
}

impl TradeBreakDownReportsBuilder {
    fn new() -> Self {
        Self {
            markets: Vec::new(),
            reports: HashMap::new(),
        }
    }

    fn append(self, report: TradeBreakDownReport) -> Self {
        let market = report.market;
        let mut markets = self.markets;
        markets.push(market);

        let mut reports = self.reports;
        reports.insert(market, report);

        Self { markets, reports }
    }

    fn build(self) -> TradeBreakDownReports {
        TradeBreakDownReports {
            markets: self.markets,
            reports: self.reports,
        }
    }
}

impl FromIterator<TradeBreakDownReport> for TradeBreakDownReports {
    fn from_iter<T: IntoIterator<Item = TradeBreakDownReport>>(iter: T) -> Self {
        iter.into_iter()
            .fold(TradeBreakDownReportsBuilder::new(), |builder, i| {
                builder.append(i)
            })
            .build()
    }
}
