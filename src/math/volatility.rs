use std::{fmt::Debug, marker::PhantomData};

use serde::{Deserialize, Serialize};

use crate::{
    data::{
        domain::Price,
        event::{ClosePriceProvider, Ohlcv, TradeEvent},
    },
    math::StreamingIndicator,
};

// ================================================================================================
// ATR
// ================================================================================================

// ================================================================================================
// Configuration & Degrees of Freedom
// ================================================================================================

/// Defines the smoothing algorithm used to average the True Range.
/// Traders often experiment with different smoothing types depending on their
/// responsiveness requirements.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
pub enum AtrSmoothingType {
    /// J. Welles Wilder's original smoothing method (Running Moving Average / RMA).
    /// Formula: alpha = 1 / window_size
    #[default]
    Wilders,
    /// Simple Moving Average (SMA). Gives equal weight to all TRs in the window.
    Sma,
    /// Exponential Moving Average (EMA). Faster reaction to recent volatility spikes.
    Ema,
}

// ================================================================================================
// Internal Smoother State Machine
// ================================================================================================

/// An internal wrapper to cleanly dispatch the update calls to the
/// selected moving average implementation without using dynamic dispatch (`Box<dyn>`).
#[derive(Debug, Clone, Serialize, Deserialize)]
enum AtrSmoother {
    Wilders(StreamingEwm),
    Sma(StreamingSma),
    Ema(StreamingEma),
}

impl StreamingIndicator for AtrSmoother {
    type Input = f64;
    type Output<'a> = Option<f64>;

    fn update(&mut self, value: Self::Input) -> Self::Output<'_> {
        match self {
            Self::Wilders(ewm) => ewm.update(value),
            Self::Sma(sma) => sma.update(value),
            Self::Ema(ema) => ema.update(value),
        }
    }

    fn reset(&mut self) {
        match self {
            Self::Wilders(ewm) => ewm.reset(),
            Self::Sma(sma) => sma.reset(),
            Self::Ema(ema) => ema.reset(),
        }
    }
}

// ================================================================================================
// ATR: Average True Range
// ================================================================================================

/// Average True Range (ATR) indicator.
/// Measures market volatility by decomposing the entire range of an asset price for that period.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingAtr {
    window_size: u16,
    smoothing_type: AtrSmoothingType,
    smoother: AtrSmoother,
    prev_close: Option<f64>,
}

impl StreamingAtr {
    /// Creates a new ATR indicator using the industry standard: Wilder's Smoothing.
    pub fn new(window_size: u16) -> Self {
        Self::with_smoothing(window_size, AtrSmoothingType::default())
    }

    /// Creates a new ATR indicator with a custom smoothing type.
    pub fn with_smoothing(window_size: u16, smoothing_type: AtrSmoothingType) -> Self {
        let smoother = match smoothing_type {
            AtrSmoothingType::Wilders => {
                // Wilder's original formula is an Exponential Weighted Mean with alpha = 1 / N
                let alpha = 1.0 / (window_size as f64);
                AtrSmoother::Wilders(StreamingEwm::new(alpha, window_size as usize))
            }
            AtrSmoothingType::Sma => AtrSmoother::Sma(StreamingSma::new(window_size)),
            AtrSmoothingType::Ema => AtrSmoother::Ema(StreamingEma::new(window_size)),
        };

        Self {
            window_size,
            smoothing_type,
            smoother,
            prev_close: None,
        }
    }

    /// Helper to isolate the True Range (TR) math.
    fn calculate_true_range(&self, current: &AtrInput) -> f64 {
        let hl_range = current.high - current.low;

        match self.prev_close {
            Some(prev_c) => {
                let hc_range = (current.high - prev_c).abs();
                let lc_range = (current.low - prev_c).abs();

                // TR = max(H - L, |H - C_prev|, |L - C_prev|)
                hl_range.max(hc_range).max(lc_range)
            }
            None => {
                // First candle: we don't have a previous close, so TR is just the High-Low range.
                hl_range
            }
        }
    }
}

impl StreamingIndicator for StreamingAtr {
    type Input = Ohlcv;
    type Output<'a> = Option<f64>;

    fn update(&mut self, current: Self::Input) -> Self::Output<'_> {
        // 1. Calculate the True Range for the current period
        let true_range = self.calculate_true_range(&current);

        // 2. Advance the state (remember the close for the next tick)
        self.prev_close = Some(current.close);

        // 3. Smooth the True Range
        self.smoother.update(true_range)
    }

    fn reset(&mut self) {
        self.smoother.reset();
        self.prev_close = None;
    }
}

// ================================================================================================
// VWAP
// ================================================================================================

/// Selects which price of a bar feeds into the volume-weighted average.
///
/// Only meaningful for bar-like data that spans a range (e.g. [`Ohlcv`]).
/// Point-like data such as a [`TradeEvent`] has a single execution price and
/// does not use this setting.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
pub enum VwapPriceSource {
    /// `(High + Low + Close) / 3`. The industry-standard VWAP price.
    #[default]
    Hlc3,
    /// `(High + Low) / 2`. Weights the bar by its extremes only.
    Hl2,
    /// `(Open + High + Low + Close) / 4`. Equal weight to all four prices.
    Ohlc4,
    /// `Close` only. Ignores intra-bar movement entirely.
    Close,
}

// ================================================================================================
// Indicator: VWAP (Volume Weighted Average Price)
// ================================================================================================

/// Shared accumulator for the `Σ(price·volume) / Σ(volume)` core.
///
/// Both VWAP variants compose one of these; the only thing they differ on is
/// how they derive the `(price, volume)` pair they feed in.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct VwapAccumulator {
    sum_price_x_volume: f64,
    sum_volume: f64,
}

impl VwapAccumulator {
    /// Folds one observation into the running totals.
    ///
    /// Non-positive volume is skipped: it contributes nothing and a zero-volume
    /// bar must not pull the average or risk a 0/0 once it's the only input.
    fn add(&mut self, price: f64, volume: f64) {
        if volume > 0.0 {
            self.sum_price_x_volume += price * volume;
            self.sum_volume += volume;
        }
    }

    /// Current VWAP, or `None` before any positive-volume input has arrived.
    fn value(&self) -> Option<f64> {
        (self.sum_volume > 0.0).then(|| self.sum_price_x_volume / self.sum_volume)
    }

    fn reset(&mut self) {
        *self = Self::default();
    }
}

/// A streaming, *anchored* Volume-Weighted Average Price over [`Ohlcv`] bars.
///
/// Accumulates `price * volume` and `volume` from the anchor onward and never
/// discards past data, so — unlike a moving average — it has no fixed lookback
/// window. The anchor is (re)set by [`reset`](StreamingIndicator::reset), which
/// the caller (e.g. a session manager) invokes at each new anchor, typically
/// the RTH open at 09:30 NY.
///
/// The per-bar price fed into the average is chosen via [`VwapPriceSource`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingOhlcvVwap {
    source: VwapPriceSource,
    // later: volume_source: OhlcvVolumeSource (volume vs taker_buy_base vs quote)
    acc: VwapAccumulator,
}

impl StreamingOhlcvVwap {
    pub fn new(source: VwapPriceSource) -> Self {
        Self {
            source,
            acc: VwapAccumulator::default(),
        }
    }

    /// Current VWAP, or `None` before any positive-volume input has arrived.
    pub fn value(&self) -> Option<f64> {
        self.acc.value()
    }

    fn weighting_price(&self, c: &Ohlcv) -> f64 {
        match self.source {
            VwapPriceSource::Hlc3 => (c.high.0 + c.low.0 + c.close.0) / 3.0,
            VwapPriceSource::Hl2 => (c.high.0 + c.low.0) / 2.0,
            VwapPriceSource::Ohlc4 => (c.open.0 + c.high.0 + c.low.0 + c.close.0) / 4.0,
            VwapPriceSource::Close => c.close.0,
        }
    }
}

impl Default for StreamingOhlcvVwap {
    fn default() -> Self {
        Self::new(VwapPriceSource::default())
    }
}

impl StreamingIndicator for StreamingOhlcvVwap {
    type Input = Ohlcv;
    type Output<'a>
        = Option<f64>
    where
        Self::Input: 'a;

    fn update(&mut self, current: Self::Input) -> Self::Output<'_> {
        let price = self.weighting_price(&current);
        self.acc.add(price, current.volume.0);
        self.acc.value()
    }

    fn reset(&mut self) {
        self.acc.reset();
    }
}

/// A streaming, *anchored* Volume-Weighted Average Price over [`TradeEvent`]s.
///
/// Same accumulation semantics and anchoring as [`StreamingOhlcvVwap`], but a
/// trade carries a single execution price, so there is no [`VwapPriceSource`]
/// to configure. The trade's `quantity` is the volume.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StreamingTradesVwap {
    // later: side filter / split buy-sell accumulators derived from is_buyer_maker
    acc: VwapAccumulator,
}

impl StreamingTradesVwap {
    pub fn new() -> Self {
        Self::default()
    }

    /// Current VWAP, or `None` before any positive-volume input has arrived.
    pub fn value(&self) -> Option<f64> {
        self.acc.value()
    }
}

impl StreamingIndicator for StreamingTradesVwap {
    type Input = TradeEvent;
    type Output<'a>
        = Option<f64>
    where
        Self::Input: 'a;

    fn update(&mut self, current: Self::Input) -> Self::Output<'_> {
        self.acc.add(current.price.0, current.quantity.0);
        self.acc.value()
    }

    fn reset(&mut self) {
        self.acc.reset();
    }
}
