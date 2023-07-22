use std::collections::HashMap;

use crate::enums::markets::MarketKind;

use super::{pnl_statement::PnLStatement, trade_breakdown_report::PnLSnapshot};

impl From<PnLStatement> for EquityCurvesReport {
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

#[derive(Debug)]
pub struct EquityCurvesReport {
    pub markets: Vec<MarketKind>,
    pub curves: HashMap<MarketKind, EquityCurves>,
}
pub struct EquityCurvesReportBuilder {
    markets: Vec<MarketKind>,
    curves: HashMap<MarketKind, EquityCurves>,
}

impl EquityCurvesReportBuilder {
    fn new() -> Self {
        Self {
            markets: Vec::new(),
            curves: HashMap::new(),
        }
    }

    fn append(self, equity_curves: EquityCurves) -> Self {
        let market = equity_curves.market;
        let mut markets = self.markets;
        markets.push(market);

        let mut curves = self.curves;
        curves.insert(market, equity_curves);

        Self { markets, curves }
    }

    fn build(self) -> EquityCurvesReport {
        EquityCurvesReport {
            markets: self.markets,
            curves: self.curves,
        }
    }
}

impl FromIterator<EquityCurves> for EquityCurvesReport {
    fn from_iter<T: IntoIterator<Item = EquityCurves>>(iter: T) -> Self {
        iter.into_iter()
            .fold(EquityCurvesReportBuilder::new(), |builder, i| {
                builder.append(i)
            })
            .build()
    }
}

#[derive(Debug)]
pub struct EquityCurves {
    pub market: MarketKind,
    pub years: Vec<u32>,
    pub curves: HashMap<u32, EquityCurve>,
}

#[derive(Debug)]
pub struct EquityCurve {
    pub market: MarketKind,
    pub year: u32,
    pub curve: Vec<f64>,
}
