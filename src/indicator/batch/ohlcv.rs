use std::sync::Arc;

use polars::{
    lazy::dsl::max_horizontal,
    prelude::{
        DataType, EWMOptions, Expr, JoinArgs, JoinType, LazyFrame, NULL, RollingOptionsFixedWindow,
        Schema, SortMultipleOptions, col, lit, when,
    },
    series::ops::NullBehavior,
};
use serde::{Deserialize, Serialize};

use crate::{
    data::domain::{AggregatedPrice, SessionWindow},
    error::{ChapatyResult, DataError},
    indicator::{
        batch::{BatchCompute, IndicatorExprExt, LazyFrameIndicatorExt, convert_err},
        config::{AtrConfig, EmaWindow, LookbackWindow, RsiWindow, SmaWindow},
    },
    transport::schema::CanonicalCol,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SessionCfg {
    pub window: SessionWindow,
    pub price_aggregation: AggregatedPrice,
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
            Self::Ema(ema) => Ok(pre_compute_ema(*ema, lf)),
            Self::Sma(sma) => Ok(pre_compute_sma(*sma, lf)),
            Self::Rsi(rsi) => Ok(pre_compute_rsi(*rsi, lf)),
            Self::Atr(atr) => pre_compute_atr(*atr, lf),
            Self::RateOfChange(lb) => pre_compute_rate_of_change(*lb, lf),
            Self::Vwap(vwap) => Ok(pre_compute_vwap(*vwap, lf)),
            Self::OvernightRange(session) => Ok(pre_compute_overnight_range(*session, lf)),
        }
    }

    fn output_schema(&self) -> Arc<Schema> {
        match self {
            Self::Ema(_) | Self::Sma(_) | Self::Rsi(_) | Self::Atr(_) | Self::Vwap(_) => {
                Arc::new(Schema::from_iter(vec![
                    CanonicalCol::PointInTime.field(),
                    CanonicalCol::Price.field(),
                ]))
            }
            Self::RateOfChange(_) => Arc::new(Schema::from_iter(vec![
                CanonicalCol::OpenTimestamp.field(),
                CanonicalCol::PointInTime.field(),
                CanonicalCol::RocAbsolute.field(),
                CanonicalCol::Roc.field(),
            ])),
            Self::OvernightRange(_) => Arc::new(Schema::from_iter(vec![
                CanonicalCol::Date.field(),
                CanonicalCol::OpenTimestamp.field(),
                CanonicalCol::PointInTime.field(),
                CanonicalCol::SessionHigh.field(),
                CanonicalCol::SessionLow.field(),
                CanonicalCol::SessionHighestClose.field(),
                CanonicalCol::SessionLowestClose.field(),
                CanonicalCol::SessionVolume.field(),
                CanonicalCol::SessionVwap.field(),
            ])),
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

    /// Computes the absolute point change: $Close_{current} - Close_{current -
    /// n}$ Returns Null if the reference price is 0.0 to prevent
    /// division-by-zero downstream.
    fn momentum_absolute(self, reference: Expr) -> Expr;

    /// Computes the percentage rate of change.
    /// Returns Null if the reference price is 0.0.
    fn momentum_roc(self, reference: Expr) -> Expr;
}

impl OhlcvIndicatorExprExt for Expr {
    fn rsi(self, window: RsiWindow) -> Expr {
        let window = window.0;
        let alpha = 1.0 / f64::from(window);
        let options = EWMOptions {
            alpha,
            adjust: false,
            bias: false,
            min_periods: window as usize,
            ignore_nulls: true,
        };

        let delta = self.diff(lit(1), NullBehavior::Ignore);
        let gain = delta.clone().clip(lit(0.0), lit(f64::MAX));
        let loss = delta.clip(lit(f64::MIN), lit(0.0)).abs();

        let avg_gain = gain.ewm_mean(options);
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

        when(has_ref).then(self - reference).otherwise(lit(NULL))
    }

    fn momentum_roc(self, reference: Expr) -> Expr {
        let absolute = self.momentum_absolute(reference.clone());
        (absolute / reference) * lit(100.0)
    }
}

fn pre_compute_ema(ema: EmaWindow, lf: LazyFrame) -> LazyFrame {
    let window = ema.0;
    let alpha = 2.0 / (f64::from(window) + 1.0);

    let options = EWMOptions {
        alpha,
        adjust: false,
        bias: false,
        min_periods: window as usize,
        ignore_nulls: true,
    };

    lf.into_price_timeseries(col(CanonicalCol::Close).ewm_mean(options))
}

fn pre_compute_sma(sma: SmaWindow, lf: LazyFrame) -> LazyFrame {
    let window = sma.0;
    let options = RollingOptionsFixedWindow {
        window_size: window as usize,
        min_periods: window as usize,
        weights: None,
        center: false,
        fn_params: None,
    };

    lf.into_price_timeseries(col(CanonicalCol::Close).rolling_mean(options))
}

fn pre_compute_rsi(rsi: RsiWindow, lf: LazyFrame) -> LazyFrame {
    lf.into_price_timeseries(col(CanonicalCol::Close).rsi(rsi))
}

fn pre_compute_atr(atr: AtrConfig, lf: LazyFrame) -> ChapatyResult<LazyFrame> {
    let alpha = 1.0 / f64::from(atr.window);
    let options = EWMOptions {
        alpha,
        adjust: false,
        bias: false,
        min_periods: atr.window as usize,
        ignore_nulls: true,
    };

    let tr_expr =
        col(CanonicalCol::Close).true_range(col(CanonicalCol::High), col(CanonicalCol::Low))?;

    Ok(lf.into_price_timeseries(tr_expr.ewm_mean(options)))
}

fn pre_compute_vwap(agg: AggregatedPrice, lf: LazyFrame) -> LazyFrame {
    lf.into_price_timeseries(agg.to_expr().vwap_with_volume(col(CanonicalCol::Volume)))
}

fn pre_compute_rate_of_change(window: LookbackWindow, lf: LazyFrame) -> ChapatyResult<LazyFrame> {
    match window {
        LookbackWindow::Bars(n) => {
            let reference_close = col(CanonicalCol::Close).shift(lit(n as u64));
            let reference_time = col(CanonicalCol::PointInTime)
                .shift(lit(n as u64))
                .alias(CanonicalCol::OpenTimestamp);

            Ok(lf
                .sort(
                    [CanonicalCol::PointInTime],
                    SortMultipleOptions::default().with_maintain_order(false),
                )
                .select([
                    reference_time,
                    col(CanonicalCol::PointInTime),
                    col(CanonicalCol::Close)
                        .momentum_absolute(reference_close.clone())
                        .alias(CanonicalCol::RocAbsolute),
                    col(CanonicalCol::Close)
                        .momentum_roc(reference_close)
                        .alias(CanonicalCol::Roc),
                ])
                .filter(col(CanonicalCol::Roc).is_not_null()))
        }

        LookbackWindow::Time(duration) => {
            const TMP_LOOKBACK_TARGET_TS: &str = "_lookback_target_ts";
            const TMP_HISTORY_TS: &str = "_history_ts";
            const TMP_HISTORY_CLOSE: &str = "_history_close";
            const TMP_HISTORY_WINDOW_START: &str = "_history_window_start";

            let duration_us = duration.num_microseconds().ok_or_else(|| {
                DataError::TimestampConversion("Lookback duration overflow: The requested time window is too large to be represented in microseconds, or requires unsupported nanosecond precision.".to_string())
            })?;

            // 1. Calculate the target timestamp we want to find in the past
            let ts_micros = col(CanonicalCol::PointInTime)
                .ts_as_microseconds()
                .cast(DataType::Int64);

            let lookback_target = (ts_micros - lit(duration_us)).alias(TMP_LOOKBACK_TARGET_TS);

            // 2. Build the historical lookup table
            let history_lf = lf.clone().select([
                col(CanonicalCol::PointInTime)
                    .ts_as_microseconds()
                    .cast(DataType::Int64)
                    .alias(TMP_HISTORY_TS),
                col(CanonicalCol::PointInTime).alias(TMP_HISTORY_WINDOW_START),
                col(CanonicalCol::Close).alias(TMP_HISTORY_CLOSE),
            ]);

            // 3. Join the history onto the current frame and compute rate of
            //    change
            Ok(lf
                .with_column(lookback_target)
                .join(
                    history_lf,
                    [col(TMP_LOOKBACK_TARGET_TS)],
                    [col(TMP_HISTORY_TS)],
                    JoinArgs::new(JoinType::Left),
                )
                .select([
                    col(TMP_HISTORY_WINDOW_START).alias(CanonicalCol::OpenTimestamp),
                    col(CanonicalCol::PointInTime),
                    col(CanonicalCol::Close)
                        .momentum_absolute(col(TMP_HISTORY_CLOSE))
                        .alias(CanonicalCol::RocAbsolute),
                    col(CanonicalCol::Close)
                        .momentum_roc(col(TMP_HISTORY_CLOSE))
                        .alias(CanonicalCol::Roc),
                ])
                .filter(col(CanonicalCol::Roc).is_not_null()))
        }
    }
}

fn pre_compute_overnight_range(cfg: SessionCfg, lf: LazyFrame) -> LazyFrame {
    let SessionCfg {
        window,
        price_aggregation,
    } = cfg;

    let session_date_col = col(CanonicalCol::PointInTime).session_date(window);

    lf.with_column(session_date_col.alias(CanonicalCol::Date))
        .filter(col(CanonicalCol::Date).is_not_null())
        .group_by([col(CanonicalCol::Date)])
        .agg([
            col(CanonicalCol::OpenTimestamp).min(),
            col(CanonicalCol::PointInTime).max(),
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
            price_aggregation
                .to_expr()
                .agg_vwap_with_volume(col(CanonicalCol::Volume))
                .alias(CanonicalCol::SessionVwap),
        ])
        .sort([CanonicalCol::PointInTime], SortMultipleOptions::default())
        .select([
            col(CanonicalCol::Date),
            col(CanonicalCol::OpenTimestamp),
            col(CanonicalCol::PointInTime),
            col(CanonicalCol::SessionHigh),
            col(CanonicalCol::SessionLow),
            col(CanonicalCol::SessionHighestClose),
            col(CanonicalCol::SessionLowestClose),
            col(CanonicalCol::SessionVolume),
            col(CanonicalCol::SessionVwap),
        ])
}

#[cfg(test)]
mod tests {
    #![expect(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::print_stdout,
        clippy::similar_names,
        reason = "tests assert against known-valid fixtures; unwrap and expect surface failures as panics that fail the test"
    )]
    use std::path::PathBuf;

    use polars::prelude::{
        DataType, LazyCsvReader, LazyFileListReader, PlRefPath, SchemaExt, TimeUnit,
    };

    use super::*;

    // ============================================================================
    // Test Fixtures & Helpers
    // ============================================================================

    /// Returns the absolute path to the test fixtures directory.
    fn fixtures_path() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/gym")
    }

    /// Loads an OHLCV CSV fixture and maps column names to the canonical
    /// schema.
    fn load_ohlcv_fixture(filename: &str) -> LazyFrame {
        let path = fixtures_path().join("input").join(filename);

        LazyCsvReader::new(PlRefPath::new(path.as_os_str().to_str().expect("filepath")))
            .with_has_header(true)
            .finish()
            .expect("Failed to parse fixture CSV")
            .select([
                col("open_timestamp")
                    .cast(DataType::Datetime(
                        TimeUnit::Microseconds,
                        Some(polars::prelude::TimeZone::UTC),
                    ))
                    .alias(CanonicalCol::OpenTimestamp.as_str()),
                col("close_timestamp")
                    .cast(DataType::Datetime(
                        TimeUnit::Microseconds,
                        Some(polars::prelude::TimeZone::UTC),
                    ))
                    .alias(CanonicalCol::PointInTime.as_str()),
                // Rename the metrics (Implicitly drops 'exchange', 'symbol', etc.)
                col("open").alias(CanonicalCol::Open.as_str()),
                col("high").alias(CanonicalCol::High.as_str()),
                col("low").alias(CanonicalCol::Low.as_str()),
                col("close").alias(CanonicalCol::Close.as_str()),
                col("volume").alias(CanonicalCol::Volume.as_str()),
                col("quote_asset_volume").alias(CanonicalCol::QuoteAssetVolume.as_str()),
                col("number_of_trades").alias(CanonicalCol::NumberOfTrades.as_str()),
                col("taker_buy_base_asset_volume")
                    .alias(CanonicalCol::TakerBuyBaseAssetVolume.as_str()),
                col("taker_buy_quote_asset_volume")
                    .alias(CanonicalCol::TakerBuyQuoteAssetVolume.as_str()),
            ])
    }

    struct IndicatorTestCase {
        name: &'static str,
        indicator: BatchOhlcvIndicator,
        expected_file: &'static str,
    }

    // ============================================================================
    // Indicator Regression Tests
    // ============================================================================

    /// **REGRESSION TEST**
    ///
    /// Checks that the current indicator logic produces the same output as
    /// previously recorded snapshots.
    ///
    /// **NOTE:** The `expected/*.csv` files were generated by this library
    /// itself. They serve to catch accidental changes in logic
    /// (regressions), but they do NOT guarantee mathematical correctness
    /// against an external standard (like TA-Lib or `TradingView`).
    #[test]
    fn test_indicators_regression_consistency() {
        let test_cases = vec![
            IndicatorTestCase {
                name: "EMA-20",
                indicator: BatchOhlcvIndicator::Ema(EmaWindow(20)),
                expected_file: "ema_20_daily.csv",
            },
            IndicatorTestCase {
                name: "SMA-14",
                indicator: BatchOhlcvIndicator::Sma(SmaWindow(14)),
                expected_file: "sma_14_daily.csv",
            },
            IndicatorTestCase {
                name: "RSI-14",
                indicator: BatchOhlcvIndicator::Rsi(RsiWindow(14)),
                expected_file: "rsi_14_daily.csv",
            },
            IndicatorTestCase {
                name: "ATR-14",
                indicator: BatchOhlcvIndicator::Atr(AtrConfig::new(14)),
                expected_file: "atr_14_daily.csv",
            },
            IndicatorTestCase {
                name: "ROC-20",
                indicator: BatchOhlcvIndicator::RateOfChange(LookbackWindow::Bars(20)),
                expected_file: "roc_20_daily.csv",
            },
            IndicatorTestCase {
                name: "VWAP-HLC3",
                indicator: BatchOhlcvIndicator::Vwap(AggregatedPrice::Hlc3),
                expected_file: "vwap_hlc3_daily.csv",
            },
            IndicatorTestCase {
                name: "Overnight-Range-Core-HLC3",
                indicator: BatchOhlcvIndicator::OvernightRange(SessionCfg {
                    window: SessionWindow::us_core_session(),
                    price_aggregation: AggregatedPrice::Hlc3,
                }),
                expected_file: "overnight_range_core_hlc3_daily.csv",
            },
            IndicatorTestCase {
                name: "Overnight-Range-Night-HLC3",
                indicator: BatchOhlcvIndicator::OvernightRange(SessionCfg {
                    window: SessionWindow::us_overnight(),
                    price_aggregation: AggregatedPrice::Hlc3,
                }),
                expected_file: "overnight_range_night_hlc3_daily.csv",
            },
        ];

        for case in test_cases {
            println!("Running indicator test: {}", case.name);

            // 1. Load Input
            let input_lf = load_ohlcv_fixture("binance-btc-usdt-8h.csv");

            // 2. Compute (Simulating internal build step)
            let result_lf = case
                .indicator
                .pre_compute(input_lf)
                .unwrap_or_else(|_| panic!("Failed to compute {}", case.name));

            // 3. Assert
            let result_df = result_lf.collect().unwrap();
            let schema = case.indicator.output_schema();

            // Verify that the result matches the promised schema
            assert_eq!(
                result_df.schema().as_ref(),
                schema.as_ref(),
                "Result schema mismatch for test case: {}",
                case.name
            );

            let expected_file = fixtures_path().join("expected").join(case.expected_file);
            let mut expected_lf = LazyCsvReader::new(PlRefPath::new(
                expected_file.as_os_str().to_str().expect("filepath"),
            ))
            .with_has_header(true)
            .finish()
            .unwrap();

            let expected_schema = expected_lf.collect_schema().unwrap();

            // Dynamically align expected DataFrame with the required schema
            for field in schema.iter_fields() {
                let name = field.name();
                let dtype = field.dtype();

                if expected_schema.contains(name.as_str()) {
                    expected_lf = expected_lf.with_column(col(name.as_str()).cast(dtype.clone()));
                } else if name.as_str() == CanonicalCol::PointInTime.as_str()
                    && expected_schema.contains("timestamp")
                {
                    // Handle "timestamp" -> "point_in_time" legacy alias
                    expected_lf = expected_lf
                        .with_column(col("timestamp").cast(dtype.clone()).alias(name.as_str()));
                }
            }

            let expected_df = expected_lf
                .select(
                    result_df
                        .get_column_names()
                        .iter()
                        .map(|n| col(n.as_str()))
                        .collect::<Vec<_>>(),
                )
                .collect()
                .unwrap();

            assert_eq!(
                result_df, expected_df,
                "DataFrame mismatch for test case: {}",
                case.name
            );
        }
    }
}
