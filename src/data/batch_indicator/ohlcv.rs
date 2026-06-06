use chrono::Duration;
use polars::{
    prelude::{
        EWMOptions, Expr, LazyFrame, RollingOptionsFixedWindow, SortMultipleOptions, col, lit,
        max_horizontal, when,
    },
    series::ops::NullBehavior,
};
use serde::{Deserialize, Serialize};

use crate::{
    data::batch_indicator::convert_err, error::ChapatyResult, transport::schema::CanonicalCol,
};

use super::{SessionConfig, VwapConfig};

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EmaWindow(pub u16);

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SmaWindow(pub u16);

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RsiWindow(pub u16);

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct AtrWindow(pub u16);

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RocWindow(pub u16);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BatchOhlcvIndicator {
    Ema(EmaWindow),
    Sma(SmaWindow),
    Rsi(RsiWindow),
    Atr(AtrWindow),
    Roc(RocWindow),
    Vwap(VwapConfig),
    Session(SessionConfig),
}

impl BatchOhlcvIndicator {
    pub fn pre_compute(&self, lf: LazyFrame) -> ChapatyResult<LazyFrame> {
        match self {
            BatchOhlcvIndicator::Ema(ema) => ema.pre_compute_ema(lf),
            BatchOhlcvIndicator::Sma(sma) => sma.pre_compute_sma(lf),
            BatchOhlcvIndicator::Rsi(rsi) => rsi.pre_compute_rsi(lf),
            BatchOhlcvIndicator::Atr(atr) => atr.pre_compute_atr(lf),
            BatchOhlcvIndicator::Roc(roc) => roc.pre_compute_roc(lf),
            BatchOhlcvIndicator::Vwap(vwap) => vwap.pre_compute_vwap(lf),
            BatchOhlcvIndicator::Session(session) => session.pre_compute_session(lf),
        }
    }
}

// === Implementations ===

impl EmaWindow {
    fn pre_compute_ema(&self, lf: LazyFrame) -> ChapatyResult<LazyFrame> {
        let window = self.0;
        let alpha = 2.0 / (window as f64 + 1.0);

        let options = EWMOptions {
            alpha,
            adjust: false,
            bias: false,
            min_periods: window as usize,
            ignore_nulls: true,
        };

        Ok(lf
            .sort(
                [CanonicalCol::Timestamp],
                SortMultipleOptions::default().with_maintain_order(false),
            )
            .with_column(
                col(CanonicalCol::Close)
                    .ewm_mean(options)
                    .alias(CanonicalCol::Ema),
            ))
    }
}

impl SmaWindow {
    fn pre_compute_sma(&self, lf: LazyFrame) -> ChapatyResult<LazyFrame> {
        let window = self.0;
        let options = RollingOptionsFixedWindow {
            window_size: window as usize,
            min_periods: window as usize,
            weights: None,
            center: false,
            fn_params: None,
        };

        Ok(lf
            .sort(
                [CanonicalCol::Timestamp],
                SortMultipleOptions::default().with_maintain_order(false),
            )
            .with_column(
                col(CanonicalCol::Close)
                    .rolling_mean(options)
                    .alias(CanonicalCol::Sma),
            ))
    }
}

impl RsiWindow {
    fn pre_compute_rsi(&self, lf: LazyFrame) -> ChapatyResult<LazyFrame> {
        let window = self.0;
        let alpha = 1.0 / (window as f64);
        let options = EWMOptions {
            alpha,
            adjust: false,
            bias: false,
            min_periods: window as usize,
            ignore_nulls: true,
        };

        let rsi_expr = {
            let delta = col(CanonicalCol::Close).diff(lit(1), NullBehavior::Ignore);
            let gain = delta.clone().clip(lit(0), lit(f64::MAX));
            let loss = delta.clip(lit(f64::MIN), lit(0)).abs();

            let avg_gain = gain.ewm_mean(options.clone());
            let avg_loss = loss.ewm_mean(options);

            let rs = avg_gain / avg_loss;
            lit(100.0) - (lit(100.0) / (lit(1.0) + rs))
        };

        Ok(lf
            .sort(
                [CanonicalCol::Timestamp],
                SortMultipleOptions::default().with_maintain_order(false),
            )
            .with_column(rsi_expr.alias(CanonicalCol::Rsi)))
    }
}

impl AtrWindow {
    fn pre_compute_atr(&self, lf: LazyFrame) -> ChapatyResult<LazyFrame> {
        let window = self.0;
        let alpha = 1.0 / (window as f64);
        let options = EWMOptions {
            alpha,
            adjust: false,
            bias: false,
            min_periods: window as usize,
            ignore_nulls: true,
        };

        let tr_expr = {
            let prev_close = col(CanonicalCol::Close).shift(lit(1));
            max_horizontal([
                col(CanonicalCol::High) - col(CanonicalCol::Low),
                (col(CanonicalCol::High) - prev_close.clone()).abs(),
                (col(CanonicalCol::Low) - prev_close).abs(),
            ])
            .expect("Valid arguments supplied to max_horizontal for True Range")
        };

        Ok(lf
            .sort(
                [CanonicalCol::Timestamp],
                SortMultipleOptions::default().with_maintain_order(false),
            )
            .with_column(tr_expr.ewm_mean(options).alias(CanonicalCol::Atr)))
    }
}

impl RocWindow {
    fn pre_compute_roc(&self, lf: LazyFrame) -> ChapatyResult<LazyFrame> {
        let window = self.0;
        let roc_expr = ((col(CanonicalCol::Close) - col(CanonicalCol::Close).shift(lit(window)))
            / col(CanonicalCol::Close).shift(lit(window)))
            * lit(100.0);

        Ok(lf
            .sort(
                [CanonicalCol::Timestamp],
                SortMultipleOptions::default().with_maintain_order(false),
            )
            .with_column(roc_expr.alias(CanonicalCol::Roc)))
    }
}

impl VwapConfig {
    fn pre_compute_vwap(&self, lf: LazyFrame) -> ChapatyResult<LazyFrame> {
        let hlc3 = (col(CanonicalCol::High) + col(CanonicalCol::Low) + col(CanonicalCol::Close))
            / lit(3.0);
        let price_x_volume = hlc3 * col(CanonicalCol::Volume);
        let valid_volume_mask = col(CanonicalCol::Volume).gt(lit(0.0));

        let cumulative_pv = price_x_volume
            .filter(valid_volume_mask.clone())
            .cum_sum(false);
        let cumulative_v = col(CanonicalCol::Volume)
            .filter(valid_volume_mask)
            .cum_sum(false);

        let vwap_expr = cumulative_pv / cumulative_v;

        Ok(lf
            .sort(
                [CanonicalCol::Timestamp],
                SortMultipleOptions::default().with_maintain_order(false),
            )
            .with_column(vwap_expr.alias(CanonicalCol::Vwap)))
    }
}

impl SessionConfig {
    fn pre_compute_session(&self, lf: LazyFrame) -> ChapatyResult<LazyFrame> {
        let start_mins = (self.start_h as u32) * 60 + (self.start_m as u32);
        let end_mins = (self.end_h as u32) * 60 + (self.end_m as u32);
        let is_intraday = start_mins < end_mins;

        // 1. Convert to local timezone and extract raw time
        let local_ts = col(CanonicalCol::Timestamp)
            .dt()
            .convert_time_zone(lit(self.timezone.clone()));
        let time_mins = local_ts.clone().dt().hour() * lit(60u32) + local_ts.clone().dt().minute();
        let local_date = local_ts.clone().dt().date();

        // 2. Classify Session Date
        let session_date_expr = if is_intraday {
            when(
                time_mins
                    .clone()
                    .gt_eq(lit(start_mins))
                    .and(time_mins.clone().lt(lit(end_mins))),
            )
            .then(local_date.clone())
            .otherwise(lit(Expr::Literal(polars::prelude::LiteralValue::Null)))
        } else {
            when(time_mins.clone().gt_eq(lit(start_mins)))
                .then(local_date.clone())
                .when(time_mins.lt(lit(end_mins)))
                // Pull morning leg into the previous day's session
                .then(local_date - lit(Duration::days(1)))
                .otherwise(lit(Expr::Literal(polars::prelude::LiteralValue::Null)))
        };

        let hlc3 = (col(CanonicalCol::High) + col(CanonicalCol::Low) + col(CanonicalCol::Close))
            / lit(3.0);
        let pv = hlc3 * col(CanonicalCol::Volume);

        // 3. Compute running cumulative session extremes (Stateless Vectorized Mapping)
        let out_lf = lf
            .sort(
                [CanonicalCol::Timestamp],
                SortMultipleOptions::default().with_maintain_order(false),
            )
            .with_column(session_date_expr.alias(CanonicalCol::SessionDate))
            .with_columns([
                col(CanonicalCol::High)
                    .cum_max(false)
                    .over([CanonicalCol::SessionDate])
                    .map_err(convert_err)?
                    .alias(CanonicalCol::SessionHigh),
                col(CanonicalCol::Low)
                    .cum_min(false)
                    .over([CanonicalCol::SessionDate])
                    .map_err(convert_err)?
                    .alias(CanonicalCol::SessionLow),
                col(CanonicalCol::Close)
                    .cum_max(false)
                    .over([CanonicalCol::SessionDate])
                    .map_err(convert_err)?
                    .alias(CanonicalCol::SessionHighestClose),
                col(CanonicalCol::Close)
                    .cum_min(false)
                    .over([CanonicalCol::SessionDate])
                    .map_err(convert_err)?
                    .alias(CanonicalCol::SessionLowestClose),
                col(CanonicalCol::Volume)
                    .cum_sum(false)
                    .over([CanonicalCol::SessionDate])
                    .map_err(convert_err)?
                    .alias(CanonicalCol::SessionVolume),
                (pv.cum_sum(false).over([CanonicalCol::SessionDate])
                    / col(CanonicalCol::Volume)
                        .cum_sum(false)
                        .over([CanonicalCol::SessionDate]))
                .map_err(convert_err)?
                .alias(CanonicalCol::SessionVwap),
            ]);

        Ok(out_lf)
    }
}
