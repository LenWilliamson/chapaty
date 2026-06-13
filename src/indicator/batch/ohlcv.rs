use polars::{
    lazy::dsl::max_horizontal,
    prelude::{
        EWMOptions, Expr, JoinArgs, JoinType, LazyFrame, NULL, RollingOptionsFixedWindow,
        SortMultipleOptions, col, lit, when,
    },
    series::ops::NullBehavior,
};
use serde::{Deserialize, Serialize};

use crate::{
    data::domain::{AggregatedPrice, SessionWindow},
    error::ChapatyResult,
    indicator::{
        batch::{BatchCompute, IndicatorExprExt, LazyFrameIndicatorExt, convert_err},
        config::{AtrConfig, EmaWindow, LookbackWindow, RsiWindow, SmaWindow},
    },
    transport::schema::CanonicalCol,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SessionCfg {
    window: SessionWindow,
    price_aggregation: AggregatedPrice,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BatchOhlcvIndicator {
    Ema(EmaWindow),
    Sma(SmaWindow),
    Rsi(RsiWindow),
    Atr(AtrConfig),
    RateOfChange(LookbackWindow),
    Vwap(AggregatedPrice),
    OvernightRange(SessionCfg),
}

impl BatchCompute for BatchOhlcvIndicator {
    fn pre_compute(&self, lf: LazyFrame) -> ChapatyResult<LazyFrame> {
        match self {
            BatchOhlcvIndicator::Ema(ema) => pre_compute_ema(*ema, lf),
            BatchOhlcvIndicator::Sma(sma) => pre_compute_sma(*sma, lf),
            BatchOhlcvIndicator::Rsi(rsi) => pre_compute_rsi(*rsi, lf),
            BatchOhlcvIndicator::Atr(atr) => pre_compute_atr(*atr, lf),
            BatchOhlcvIndicator::RateOfChange(lb) => pre_compute_rate_of_change(*lb, lf),
            BatchOhlcvIndicator::Vwap(vwap) => pre_compute_vwap(*vwap, lf),
            BatchOhlcvIndicator::OvernightRange(session) => {
                pre_compute_overnight_range(*session, lf)
            }
        }
    }
}

// ================================================================================================
// LazyFrame Pre-Computations
// ================================================================================================

trait OhlcvIndicatorExprExt {
    /// Computes the Relative Strength Index (RSI).
    fn rsi(self, window: RsiWindow) -> Expr;

    /// Computes the True Range using High, Low, and self (as Close).
    fn true_range(self, high: Expr, low: Expr) -> ChapatyResult<Expr>;

    /// Computes the absolute point change: $Close_{current} - Close_{current - n}$
    /// Returns Null if the reference price is 0.0 to prevent division-by-zero downstream.
    fn momentum_absolute(self, reference: Expr) -> Expr;

    /// Computes the percentage rate of change.
    /// Returns Null if the reference price is 0.0.
    fn momentum_roc(self, reference: Expr) -> Expr;
}

impl OhlcvIndicatorExprExt for Expr {
    fn rsi(self, window: RsiWindow) -> Expr {
        let window = window.0;
        let alpha = 1.0 / (window as f64);
        let options = EWMOptions {
            alpha,
            adjust: false,
            bias: false,
            min_periods: window as usize,
            ignore_nulls: true,
        };

        let delta = self.diff(lit(1), NullBehavior::Ignore);
        let gain = delta.clone().clip(lit(0), lit(f64::MAX));
        let loss = delta.clip(lit(f64::MIN), lit(0)).abs();

        let avg_gain = gain.ewm_mean(options.clone());
        let avg_loss = loss.ewm_mean(options);

        let is_flat = avg_gain
            .clone()
            .eq(lit(0.0))
            .and(avg_loss.clone().eq(lit(0.0)));
        let rs = avg_gain / avg_loss;

        when(is_flat)
            .then(lit(50.0))
            .otherwise(lit(100.0) - (lit(100.0) / (lit(1.0) + rs)))
    }

    fn true_range(self, high: Expr, low: Expr) -> ChapatyResult<Expr> {
        let prev_close = self.shift(lit(1));
        max_horizontal([
            high.clone() - low.clone(),
            (high - prev_close.clone()).abs(),
            (low - prev_close).abs(),
        ])
        .map_err(convert_err)
    }

    fn momentum_absolute(self, reference: Expr) -> Expr {
        let has_ref = reference.clone().abs().gt(lit(f64::EPSILON));

        when(has_ref)
            .then(self - reference.clone())
            .otherwise(lit(NULL))
    }

    fn momentum_roc(self, reference: Expr) -> Expr {
        let has_ref = reference.clone().abs().gt(lit(f64::EPSILON));
        let absolute = self.clone() - reference.clone();

        when(has_ref)
            .then((absolute / reference) * lit(100.0))
            .otherwise(lit(NULL))
    }
}

fn pre_compute_ema(ema: EmaWindow, lf: LazyFrame) -> ChapatyResult<LazyFrame> {
    let window = ema.0;
    let alpha = 2.0 / (window as f64 + 1.0);

    let options = EWMOptions {
        alpha,
        adjust: false,
        bias: false,
        min_periods: window as usize,
        ignore_nulls: true,
    };

    Ok(lf.finalize_scalar(col(CanonicalCol::Close).ewm_mean(options)))
}

fn pre_compute_sma(sma: SmaWindow, lf: LazyFrame) -> ChapatyResult<LazyFrame> {
    let window = sma.0;
    let options = RollingOptionsFixedWindow {
        window_size: window as usize,
        min_periods: window as usize,
        weights: None,
        center: false,
        fn_params: None,
    };

    Ok(lf.finalize_scalar(col(CanonicalCol::Close).rolling_mean(options)))
}

fn pre_compute_rsi(rsi: RsiWindow, lf: LazyFrame) -> ChapatyResult<LazyFrame> {
    Ok(lf.finalize_scalar(col(CanonicalCol::Close).rsi(rsi)))
}

fn pre_compute_atr(atr: AtrConfig, lf: LazyFrame) -> ChapatyResult<LazyFrame> {
    let alpha = 1.0 / (atr.window as f64);
    let options = EWMOptions {
        alpha,
        adjust: false,
        bias: false,
        min_periods: atr.window as usize,
        ignore_nulls: true,
    };

    let tr_expr =
        col(CanonicalCol::Close).true_range(col(CanonicalCol::High), col(CanonicalCol::Low))?;

    Ok(lf.finalize_scalar(tr_expr.ewm_mean(options)))
}

fn pre_compute_vwap(agg: AggregatedPrice, lf: LazyFrame) -> ChapatyResult<LazyFrame> {
    Ok(lf.finalize_scalar(agg.to_expr().vwap(col(CanonicalCol::Volume))))
}

fn pre_compute_rate_of_change(window: LookbackWindow, lf: LazyFrame) -> ChapatyResult<LazyFrame> {
    match window {
        LookbackWindow::Bars(n) => {
            // Row-based lookback is natively supported by Polars
            let reference = col(CanonicalCol::Close).shift(lit(n as u32));

            Ok(lf
                .sort(
                    [CanonicalCol::PointInTime],
                    SortMultipleOptions::default().with_maintain_order(false),
                )
                .select([
                    col(CanonicalCol::PointInTime),
                    col(CanonicalCol::Close)
                        .momentum_absolute(reference.clone())
                        .alias(CanonicalCol::RocAbsolute),
                    col(CanonicalCol::Close)
                        .momentum_roc(reference)
                        .alias(CanonicalCol::Roc),
                ])
                .filter(col(CanonicalCol::Roc).is_not_null()))
        }

        LookbackWindow::Time(duration) => {
            // To mimic the exact time-boundary guard of your streaming buffer:
            // 1. Create an exact historical timestamp.
            // 2. Perform a left join onto itself.
            // If the exact historical time doesn't exist (e.g., gaps), it yields null,
            // which perfectly matches the streaming buffer's `None` output.

            let duration_ms = duration.num_milliseconds();

            // Define the time exactly `duration` ago
            let lookback_target = col(CanonicalCol::PointInTime) - lit(duration_ms);

            let history_lf = lf.clone().select([
                col(CanonicalCol::PointInTime).alias("hist_ts"),
                col(CanonicalCol::Close).alias("hist_close"),
            ]);

            Ok(lf
                .with_column(lookback_target.alias("lookback_target"))
                .join(
                    history_lf,
                    [col("lookback_target")],
                    [col("hist_ts")],
                    JoinArgs::new(JoinType::Left),
                )
                .select([
                    col(CanonicalCol::PointInTime),
                    col(CanonicalCol::Close)
                        .momentum_absolute(col("hist_close"))
                        .alias(CanonicalCol::RocAbsolute),
                    col(CanonicalCol::Close)
                        .momentum_roc(col("hist_close"))
                        .alias(CanonicalCol::Roc),
                ])
                .filter(col(CanonicalCol::Roc).is_not_null()))
        }
    }
}

fn pre_compute_overnight_range(cfg: SessionCfg, lf: LazyFrame) -> ChapatyResult<LazyFrame> {
    let SessionCfg {
        window,
        price_aggregation,
    } = cfg;

    let session_date_col = col(CanonicalCol::PointInTime).session_date(window);

    let out_lf = lf
        .with_column(session_date_col.alias(CanonicalCol::Date))
        .filter(col(CanonicalCol::Date).is_not_null())
        .group_by([col(CanonicalCol::Date)])
        .agg([
            col(CanonicalCol::OpenTimestamp)
                .first()
                .alias("OpenTimestamp"),
            col(CanonicalCol::PointInTime)
                .last()
                .alias(CanonicalCol::PointInTime),
            col(CanonicalCol::High)
                .max()
                .alias(CanonicalCol::SessionHigh),
            col(CanonicalCol::Low).min().alias(CanonicalCol::SessionLow),
            col(CanonicalCol::Close)
                .max()
                .alias(CanonicalCol::SessionHighestClose),
            col(CanonicalCol::Close)
                .min()
                .alias(CanonicalCol::SessionLowestClose),
            col(CanonicalCol::Volume)
                .sum()
                .alias(CanonicalCol::SessionVolume),
            // --- Session VWAP ---
            price_aggregation
                .to_expr()
                .agg_vwap(col(CanonicalCol::Volume))
                .alias(CanonicalCol::SessionVwap),
        ])
        .sort([CanonicalCol::PointInTime], SortMultipleOptions::default());

    Ok(out_lf)
}
