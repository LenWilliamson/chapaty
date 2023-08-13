use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{
    pnl::pnl_statement_agg_years::{PnLSnapshotAggYears, PnLStatementAggYears},
    MarketKind,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct EquityCurvesAggYears {
    pub markets: Vec<MarketKind>,
    pub years: Vec<u32>,
    pub curves: HashMap<MarketKind, EquityCurveAggYears>,
}

struct EquityCurvesAggYearsBuilder {
    pub markets: Vec<MarketKind>,
    pub years: Vec<u32>,
    pub curves: HashMap<MarketKind, EquityCurveAggYears>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EquityCurveAggYears {
    pub market: MarketKind,
    pub years: Vec<u32>,
    pub curve: Vec<f64>,
}

impl From<PnLStatementAggYears> for EquityCurvesAggYears {
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

impl EquityCurvesAggYearsBuilder {
    fn new() -> Self {
        Self {
            markets: Vec::new(),
            years: Vec::new(),
            curves: HashMap::new(),
        }
    }

    fn append(self, equity_curves: EquityCurveAggYears) -> Self {
        let years = equity_curves.years.clone();
        let market = equity_curves.market.clone();
        let mut markets = self.markets;
        markets.push(market);

        let mut curves = self.curves;
        curves.insert(market, equity_curves);

        Self {
            markets,
            years,
            curves,
        }
    }

    fn build(self) -> EquityCurvesAggYears {
        EquityCurvesAggYears {
            markets: self.markets,
            years: self.years,
            curves: self.curves,
        }
    }
}

impl FromIterator<EquityCurveAggYears> for EquityCurvesAggYears {
    fn from_iter<T: IntoIterator<Item = EquityCurveAggYears>>(iter: T) -> Self {
        iter.into_iter()
            .fold(EquityCurvesAggYearsBuilder::new(), |builder, i| {
                builder.append(i)
            })
            .build()
    }
}
