use chrono::Duration;
use polars::{
    lazy::dsl::max_horizontal,
    prelude::{
        EWMOptions, LazyFrame, NULL, RollingOptionsFixedWindow, SortMultipleOptions, col, lit, when,
    },
    series::ops::NullBehavior,
};
use serde::{Deserialize, Serialize};

use crate::{
    data::{
        batch_indicator::{convert_err, finalize_scalar},
        domain::AggregatedPrice,
    },
    error::ChapatyResult,
    transport::schema::CanonicalCol,
};

use super::config::SessionConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VwapConfig(pub AggregatedPrice);

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EmaWindow(pub u16);

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SmaWindow(pub u16);

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RsiWindow(pub u16);

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct AtrWindow(pub u16);

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RateOfChangeWindow(pub u16);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BatchOhlcvIndicator {
    Ema(EmaWindow),
    Sma(SmaWindow),
    Rsi(RsiWindow),
    Atr(AtrWindow),
    RateOfChange(RateOfChangeWindow),
    Vwap(VwapConfig),
    OvernightRange(SessionConfig),
}

impl BatchOhlcvIndicator {
    pub(crate) fn pre_compute(&self, lf: LazyFrame) -> ChapatyResult<LazyFrame> {
        match self {
            BatchOhlcvIndicator::Ema(ema) => ema.pre_compute_ema(lf),
            BatchOhlcvIndicator::Sma(sma) => sma.pre_compute_sma(lf),
            BatchOhlcvIndicator::Rsi(rsi) => rsi.pre_compute_rsi(lf),
            BatchOhlcvIndicator::Atr(atr) => atr.pre_compute_atr(lf),
            BatchOhlcvIndicator::RateOfChange(roc) => roc.pre_compute_roc(lf),
            BatchOhlcvIndicator::Vwap(vwap) => vwap.pre_compute_ohlcv_vwap(lf),
            BatchOhlcvIndicator::OvernightRange(session) => session.pre_compute_ohlcv_session(lf),
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

        Ok(finalize_scalar(
            lf,
            col(CanonicalCol::Close).ewm_mean(options),
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

        Ok(finalize_scalar(
            lf,
            col(CanonicalCol::Close).rolling_mean(options),
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

            // Mirror the streaming RSI's degenerate-case handling: a flat series
            // (no gains and no losses) is defined as 50 rather than the 0/0 NaN the
            // raw formula would produce. A pure up-trend (avg_loss == 0, avg_gain > 0)
            // still resolves to 100 naturally, since rs -> +inf.
            let is_flat = avg_gain
                .clone()
                .eq(lit(0.0))
                .and(avg_loss.clone().eq(lit(0.0)));
            let rs = avg_gain / avg_loss;
            when(is_flat)
                .then(lit(50.0))
                .otherwise(lit(100.0) - (lit(100.0) / (lit(1.0) + rs)))
        };

        Ok(finalize_scalar(lf, rsi_expr))
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

        Ok(finalize_scalar(lf, tr_expr.ewm_mean(options)))
    }
}

impl RateOfChangeWindow {
    fn pre_compute_roc(&self, lf: LazyFrame) -> ChapatyResult<LazyFrame> {
        let window = self.0;

        // Reference price `window` bars back (null during warm-up).
        let reference = col(CanonicalCol::Close).shift(lit(window));
        let absolute = col(CanonicalCol::Close) - reference.clone();
        let roc = (absolute.clone() / reference.clone()) * lit(100.0);

        // Match the streaming guard: a (near-)zero reference price would divide by
        // zero, so the whole observation (both absolute and roc) is suppressed.
        let has_reference = reference.abs().gt(lit(f64::EPSILON));

        // Mirrors the streaming `MomentumOutput { absolute, roc }`, keyed by timestamp.
        Ok(lf
            .sort(
                [CanonicalCol::Timestamp],
                SortMultipleOptions::default().with_maintain_order(false),
            )
            .select([
                col(CanonicalCol::Timestamp),
                when(has_reference.clone())
                    .then(absolute)
                    .otherwise(lit(NULL))
                    .alias(CanonicalCol::RocAbsolute),
                when(has_reference)
                    .then(roc)
                    .otherwise(lit(NULL))
                    .alias(CanonicalCol::Roc),
            ])
            .filter(col(CanonicalCol::Roc).is_not_null()))
    }
}

impl VwapConfig {
    fn pre_compute_ohlcv_vwap(&self, lf: LazyFrame) -> ChapatyResult<LazyFrame> {
        let hlc3 = (col(CanonicalCol::High) + col(CanonicalCol::Low) + col(CanonicalCol::Close))
            / lit(3.0);

        // Non-positive volume contributes nothing to either running sum, mirroring
        // the streaming accumulator. Use a conditional (rather than `filter`) so the
        // series keeps the frame's length and stays alignable with the timestamp.
        let has_volume = col(CanonicalCol::Volume).gt(lit(0.0));
        let price_x_volume = when(has_volume.clone())
            .then(hlc3 * col(CanonicalCol::Volume))
            .otherwise(lit(0.0));
        let volume = when(has_volume)
            .then(col(CanonicalCol::Volume))
            .otherwise(lit(0.0));

        let cumulative_v = volume.cum_sum(false);
        // VWAP is undefined until the first positive-volume bar arrives.
        let vwap_expr = when(cumulative_v.clone().gt(lit(0.0)))
            .then(price_x_volume.cum_sum(false) / cumulative_v)
            .otherwise(lit(NULL));

        Ok(finalize_scalar(lf, vwap_expr))
    }
}

impl SessionConfig {
    fn pre_compute_ohlcv_session(&self, lf: LazyFrame) -> ChapatyResult<LazyFrame> {
        let start_mins = (self.start_h as u32) * 60 + (self.start_m as u32);
        let end_mins = (self.end_h as u32) * 60 + (self.end_m as u32);
        let is_intraday = start_mins < end_mins;

        // 1. Convert to local timezone and extract raw time
        let local_ts = col(CanonicalCol::Timestamp)
            .dt()
            .convert_time_zone(self.polars_tz()?);
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
            .otherwise(lit(NULL))
        } else {
            when(time_mins.clone().gt_eq(lit(start_mins)))
                .then(local_date.clone())
                .when(time_mins.lt(lit(end_mins)))
                // Pull morning leg into the previous day's session
                .then(local_date - lit(Duration::days(1)))
                .otherwise(lit(NULL))
        };

        let hlc3 = (col(CanonicalCol::High) + col(CanonicalCol::Low) + col(CanonicalCol::Close))
            / lit(3.0);
        let pv = hlc3 * col(CanonicalCol::Volume);

        // Session VWAP = running sum(price * volume) / running sum(volume), both
        // partitioned by session. `over` is fallible, so build the two cumulative
        // legs first and only then divide.
        let session_pv_cum = pv
            .cum_sum(false)
            .over([col(CanonicalCol::SessionDate)])
            .map_err(convert_err)?;
        let session_volume_cum = col(CanonicalCol::Volume)
            .cum_sum(false)
            .over([col(CanonicalCol::SessionDate)])
            .map_err(convert_err)?;
        let session_vwap = session_pv_cum / session_volume_cum;

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
                    .over([col(CanonicalCol::SessionDate)])
                    .map_err(convert_err)?
                    .alias(CanonicalCol::SessionHigh),
                col(CanonicalCol::Low)
                    .cum_min(false)
                    .over([col(CanonicalCol::SessionDate)])
                    .map_err(convert_err)?
                    .alias(CanonicalCol::SessionLow),
                col(CanonicalCol::Close)
                    .cum_max(false)
                    .over([col(CanonicalCol::SessionDate)])
                    .map_err(convert_err)?
                    .alias(CanonicalCol::SessionHighestClose),
                col(CanonicalCol::Close)
                    .cum_min(false)
                    .over([col(CanonicalCol::SessionDate)])
                    .map_err(convert_err)?
                    .alias(CanonicalCol::SessionLowestClose),
                col(CanonicalCol::Volume)
                    .cum_sum(false)
                    .over([col(CanonicalCol::SessionDate)])
                    .map_err(convert_err)?
                    .alias(CanonicalCol::SessionVolume),
                session_vwap.alias(CanonicalCol::SessionVwap),
            ])
            // Project to the columns of the `OhlcvSessionData` struct (keyed by
            // timestamp) and keep only rows that belong to a session.
            .select([
                col(CanonicalCol::Timestamp),
                col(CanonicalCol::SessionDate),
                col(CanonicalCol::SessionHigh),
                col(CanonicalCol::SessionLow),
                col(CanonicalCol::SessionHighestClose),
                col(CanonicalCol::SessionLowestClose),
                col(CanonicalCol::SessionVolume),
                col(CanonicalCol::SessionVwap),
            ])
            .filter(col(CanonicalCol::SessionDate).is_not_null());

        Ok(out_lf)
    }
}
