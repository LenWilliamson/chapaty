use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use crate::{
    data::{
        domain::{Price, Volume},
        event::{Ohlcv, TradeEvent},
    },
    math::{
        StreamingIndicator, accumulators::KahanSum, moving_averages::{StreamingEma, StreamingEwm, StreamingSma}
    },
};

// ================================================================================================
// ATR
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
    /// Simple Moving Average (SMA).
    Sma,
    /// Exponential Moving Average (EMA).
    Ema,
}

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

/// Average True Range (ATR) indicator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingAtr {
    window_size: u16,
    smoother: AtrSmoother,
    prev_close: Option<Price>,
}

impl Default for StreamingAtr {
    fn default() -> Self {
        let window_size = 14;
        let alpha = 1.0 / (window_size as f64);
        let smoother = AtrSmoother::Wilders(StreamingEwm::new(alpha, window_size as usize));
        Self {
            window_size,
            smoother,
            prev_close: None,
        }
    }
}

impl StreamingAtr {
    /// Creates a new ATR indicator.
    ///
    /// Panics if `window_size` is not strictly positive (i.e., `window_size` must be > 0).
    pub fn new(window_size: u16, smoothing_type: AtrSmoothingType) -> Self {
        assert!(
            window_size > 0,
            "window_size must be > 0, but got {window_size} <= 0"
        );
        let smoother = match smoothing_type {
            AtrSmoothingType::Wilders => {
                let alpha = 1.0 / (window_size as f64);
                AtrSmoother::Wilders(StreamingEwm::new(alpha, window_size as usize))
            }
            AtrSmoothingType::Sma => AtrSmoother::Sma(StreamingSma::new(window_size)),
            AtrSmoothingType::Ema => AtrSmoother::Ema(StreamingEma::new(window_size)),
        };

        Self {
            window_size,
            smoother,
            ..Default::default()
        }
    }
}

impl StreamingAtr {
    /// Helper to isolate the True Range (TR) math.
    fn calculate_true_range(&self, ohlcv: Ohlcv) -> f64 {
        let hl_range = ohlcv.high - ohlcv.low;

        match self.prev_close {
            Some(prev_c) => {
                let hc_range = (ohlcv.high - prev_c).abs();
                let lc_range = (ohlcv.low - prev_c).abs();

                // TR = max(H - L, |H - C_prev|, |L - C_prev|)
                hl_range.max(hc_range).max(lc_range).0
            }
            None => {
                // First candle: we don't have a previous close, so TR is just the High-Low range.
                hl_range.0
            }
        }
    }
}

impl StreamingIndicator for StreamingAtr {
    type Input = Ohlcv;
    type Output<'a> = Option<f64>;

    fn update(&mut self, current: Self::Input) -> Self::Output<'_> {
        // 1. Calculate the True Range for the current period
        let true_range = self.calculate_true_range(current);

        // 2. Advance the state
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

/// Shared accumulator for the `sum(price * volume) / sum(volume)` core.
///
/// VWAP variants only differ on how they derive the `(price, volume)` pair they feed in.
/// Now implemented using Kahan summation for precision lossless high-frequency accumulation.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
struct KahanAccumulator {
    sum_price_x_volume: KahanSum,
    sum_volume: KahanSum,
}

impl KahanAccumulator {
    /// Folds one observation into the running totals using move semantics.
    ///
    /// Non-positive or non-finite volume is skipped: it contributes nothing and a zero-volume
    /// bar must not pull the average or risk a zero-division once it's the only input.
    #[inline]
    fn add(self, price: Price, volume: Volume) -> Self {
        let v = volume.0;
        if !v.is_finite() || v <= 0.0 {
            return self;
        }

        Self {
            sum_price_x_volume: self.sum_price_x_volume.add(price.0 * v),
            sum_volume: self.sum_volume.add(v),
        }
    }

    /// Current VWAP, or `None` before any positive-volume input has arrived.
    #[inline]
    fn value(self) -> Option<f64> {
        let total_v = self.sum_volume.value();
        (total_v > 0.0).then(|| self.sum_price_x_volume.value() / total_v)
    }
}

// ================================================================================================
// VWAP: OHLCV
// ================================================================================================

/// A streaming Volume-Weighted Average Price over [`Ohlcv`] bars.
///
/// Accumulates `price * volume` and `volume` from the anchor onward and never
/// discards past data. The anchor is reset by [`reset`](StreamingIndicator::reset).
///
/// The per-bar price fed into the average is chosen via [`VwapPriceSource`].
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct StreamingOhlcvVwap {
    source: VwapPriceSource,
    acc: KahanAccumulator,
}

impl StreamingOhlcvVwap {
    pub fn new(source: VwapPriceSource) -> Self {
        Self {
            source,
            acc: KahanAccumulator::default(),
        }
    }

    /// Current VWAP, or `None` before any positive-volume input has arrived.
    pub fn value(&self) -> Option<f64> {
        self.acc.value()
    }

    fn weighting_price(&self, ohlcv: Ohlcv) -> Price {
        match self.source {
            VwapPriceSource::Hlc3 => Price((ohlcv.high + ohlcv.low + ohlcv.close).0 / 3.0),
            VwapPriceSource::Hl2 => Price((ohlcv.high + ohlcv.low).0 / 2.0),
            VwapPriceSource::Ohlc4 => {
                Price((ohlcv.open + ohlcv.high + ohlcv.low + ohlcv.close).0 / 4.0)
            }
            VwapPriceSource::Close => ohlcv.close,
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
    type Output<'a> = Option<f64>;

    fn update(&mut self, current: Self::Input) -> Self::Output<'_> {
        let price = self.weighting_price(current);
        self.acc = self.acc.add(price, current.volume);
        self.acc.value()
    }

    fn reset(&mut self) {
        self.acc = KahanAccumulator::default();
    }
}

// ================================================================================================
// VWAP: Trades
// ================================================================================================

/// A streaming, Volume-Weighted Average Price over [`TradeEvent`]s.
///
/// Same accumulation semantics and anchoring as [`StreamingOhlcvVwap`], but a
/// trade carries a single execution price, so there is no [`VwapPriceSource`]
/// to configure. The trade's `quantity` is the volume.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct StreamingTradesVwap {
    acc: KahanAccumulator,
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
    type Output<'a> = Option<f64>;

    fn update(&mut self, current: Self::Input) -> Self::Output<'_> {
        self.acc = self.acc.add(current.price, current.quantity);
        self.acc.value()
    }

    fn reset(&mut self) {
        self.acc = KahanAccumulator::default();
    }
}

#[cfg(test)]
mod tests {
    use crate::data::domain::Quantity;

    use super::*;
    use chrono::{DateTime, Utc};

    fn mock_candle(open: f64, high: f64, low: f64, close: f64, vol: f64) -> Ohlcv {
        Ohlcv {
            open_timestamp: DateTime::<Utc>::MIN_UTC,
            close_timestamp: DateTime::<Utc>::MIN_UTC,
            open: Price(open),
            high: Price(high),
            low: Price(low),
            close: Price(close),
            volume: Quantity(vol),
            quote_asset_volume: None,
            number_of_trades: None,
            taker_buy_base_asset_volume: None,
            taker_buy_quote_asset_volume: None,
        }
    }

    fn mock_trade(price: f64, qty: f64) -> TradeEvent {
        TradeEvent {
            timestamp: DateTime::<Utc>::MIN_UTC,
            price: Price(price),
            quantity: Quantity(qty),
            trade_id: None,
            quote_asset_volume: None,
            is_buyer_maker: None,
            is_best_match: None,
        }
    }

    // ============================================================================================
    // ATR TESTS
    // ============================================================================================

    #[test]
    fn atr_calculates_true_range_correctly_across_edge_cases() {
        // By using an SMA of length 1, the smoother just outputs the exact True Range of the current candle.
        // This isolates the TR math from the smoothing math.
        let mut atr = StreamingAtr::new(1, AtrSmoothingType::Sma);

        // 1. First Candle: No previous close. TR should be High - Low (15.0 - 5.0 = 10.0)
        let candle1 = mock_candle(10., 15., 5., 12., 100.);
        assert_eq!(atr.update(candle1), Some(10.0));

        // 2. Normal Inside/Regular Candle: High/Low range is completely contained or slightly overlaps.
        // Prev Close = 12.0. High = 14.0, Low = 10.0.
        // TR = max(14 - 10, |14 - 12|, |10 - 12|) = max(4, 2, 2) = 4.0
        let candle2 = mock_candle(12., 14., 10., 13., 100.);
        assert_eq!(atr.update(candle2), Some(4.0));

        // 3. Massive Gap Up: Prev Close is significantly lower than current Low.
        // Prev Close = 13.0. High = 25.0, Low = 20.0.
        // TR = max(25 - 20, |25 - 13|, |20 - 13|) = max(5, 12, 7) = 12.0
        let candle3 = mock_candle(20., 25., 20., 24., 100.);
        assert_eq!(atr.update(candle3), Some(12.0));

        // 4. Massive Gap Down: Prev Close is significantly higher than current High.
        // Prev Close = 24.0. High = 10.0, Low = 5.0.
        // TR = max(10 - 5, |10 - 24|, |5 - 24|) = max(5, 14, 19) = 19.0
        let candle4 = mock_candle(10., 10., 5., 8., 100.);
        assert_eq!(atr.update(candle4), Some(19.0));
    }

    // ============================================================================================
    // VWAP TESTS
    // ============================================================================================

    #[test]
    fn ohlcv_vwap_accumulates_and_ignores_bad_volume() {
        let mut vwap = StreamingOhlcvVwap::new(VwapPriceSource::Hlc3);

        // 1. VWAP should be None before any data is fed
        assert_eq!(vwap.value(), None);

        // 2. First candle: H=10, L=8, C=9 => Hlc3 = 9.0
        // Volume = 100. VWAP = 9.0
        let c1 = mock_candle(0., 10., 8., 9., 100.);
        assert_eq!(vwap.update(c1), Some(9.0));

        // 3. Second candle: H=20, L=10, C=15 => Hlc3 = 15.0
        // Volume = 200. Total Vol = 300.
        // Sum(Price * Vol) = (9 * 100) + (15 * 200) = 900 + 3000 = 3900.
        // VWAP = 3900 / 300 = 13.0
        let c2 = mock_candle(0., 20., 10., 15., 200.);
        assert_eq!(vwap.update(c2), Some(13.0));

        // 4. Zero Volume: Should NOT affect the VWAP or cause division by zero.
        let c3 = mock_candle(0., 50., 40., 45., 0.);
        assert_eq!(vwap.update(c3), Some(13.0));

        // 5. Negative Volume: Should be discarded safely.
        let c4 = mock_candle(0., 50., 40., 45., -50.);
        assert_eq!(vwap.update(c4), Some(13.0));
    }

    #[test]
    fn ohlcv_vwap_respects_price_sources() {
        let candle = mock_candle(10., 20., 10., 18., 100.); // O=10, H=20, L=10, C=18

        let mut vwap_hlc3 = StreamingOhlcvVwap::new(VwapPriceSource::Hlc3);
        assert_eq!(vwap_hlc3.update(candle), Some((20. + 10. + 18.) / 3.0)); // 16.0

        let mut vwap_hl2 = StreamingOhlcvVwap::new(VwapPriceSource::Hl2);
        assert_eq!(vwap_hl2.update(candle), Some((20. + 10.) / 2.0)); // 15.0

        let mut vwap_ohlc4 = StreamingOhlcvVwap::new(VwapPriceSource::Ohlc4);
        assert_eq!(
            vwap_ohlc4.update(candle),
            Some((10. + 20. + 10. + 18.) / 4.0)
        ); // 14.5

        let mut vwap_close = StreamingOhlcvVwap::new(VwapPriceSource::Close);
        assert_eq!(vwap_close.update(candle), Some(18.0)); // 18.0
    }

    #[test]
    fn trades_vwap_accumulates_correctly() {
        let mut vwap = StreamingTradesVwap::new();

        // 1. Feed a trade: Price = 100, Qty = 2
        // VWAP = 100
        assert_eq!(vwap.update(mock_trade(100.0, 2.0)), Some(100.0));

        // 2. Feed a second trade: Price = 110, Qty = 8
        // Total Volume = 10.
        // Sum(Price * Vol) = (100 * 2) + (110 * 8) = 200 + 880 = 1080.
        // VWAP = 1080 / 10 = 108.0
        assert_eq!(vwap.update(mock_trade(110.0, 8.0)), Some(108.0));

        // 3. Reset behavior
        vwap.reset();
        assert_eq!(vwap.value(), None);

        // Ensure standard behavior resumes cleanly after reset
        assert_eq!(vwap.update(mock_trade(50.0, 10.0)), Some(50.0));
    }
}
