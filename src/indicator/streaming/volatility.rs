use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use crate::{
    data::{
        domain::{AggregatedPrice, Price, Volume},
        event::{Ohlcv, TradeEvent},
    },
    indicator::{
        config::{AtrConfig, AtrSmoothingType, EmaWindow, SmaWindow},
        streaming::{
            StreamingIndicator,
            moving_averages::{StreamingEma, StreamingEwm, StreamingSma},
        },
    },
    math::accumulators::KahanSum,
};

// ================================================================================================
// ATR
// ================================================================================================

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
        let alpha = 1.0 / f64::from(window_size);
        let smoother = AtrSmoother::Wilders(StreamingEwm::new(alpha, window_size as usize));
        Self {
            window_size,
            smoother,
            prev_close: None,
        }
    }
}

impl StreamingAtr {
    #[must_use]
    pub fn new(cfg: AtrConfig) -> Self {
        let AtrConfig { window, smoothing } = cfg;
        let smoother = match smoothing {
            AtrSmoothingType::Wilders => {
                let alpha = 1.0 / f64::from(window);
                AtrSmoother::Wilders(StreamingEwm::new(alpha, window as usize))
            }
            AtrSmoothingType::Sma => AtrSmoother::Sma(StreamingSma::new(SmaWindow(window))),
            AtrSmoothingType::Ema => AtrSmoother::Ema(StreamingEma::new(EmaWindow(window))),
        };

        Self {
            window_size: window,
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
    fn value(self) -> Option<f64> {
        let total_v = self.sum_volume.value();
        (total_v > 0.0).then(|| self.sum_price_x_volume.value() / total_v)
    }

    fn reset(&mut self) {
        *self = Self::default();
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
    source: AggregatedPrice,
    acc: KahanAccumulator,
}

impl StreamingOhlcvVwap {
    #[must_use]
    pub fn new(source: AggregatedPrice) -> Self {
        Self {
            source,
            acc: KahanAccumulator::default(),
        }
    }

    /// Current VWAP, or `None` before any positive-volume input has arrived.
    #[must_use]
    pub fn value(&self) -> Option<f64> {
        self.acc.value()
    }

    fn weighting_price(&self, ohlcv: Ohlcv) -> Price {
        match self.source {
            AggregatedPrice::Hlc3 => Price((ohlcv.high + ohlcv.low + ohlcv.close).0 / 3.0),
            AggregatedPrice::Hl2 => Price((ohlcv.high + ohlcv.low).0 / 2.0),
            AggregatedPrice::Ohlc4 => {
                Price((ohlcv.open + ohlcv.high + ohlcv.low + ohlcv.close).0 / 4.0)
            }
            AggregatedPrice::Close => ohlcv.close,
        }
    }
}

impl Default for StreamingOhlcvVwap {
    fn default() -> Self {
        Self::new(AggregatedPrice::default())
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
        self.acc.reset();
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
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Current VWAP, or `None` before any positive-volume input has arrived.
    #[must_use]
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
        let mut atr = StreamingAtr::new(AtrConfig {
            window: 1,
            smoothing: AtrSmoothingType::Sma,
        });

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

    /// With a constant True Range, every smoothing type must converge to (and stay
    /// at) that exact value — a smoother-agnostic sanity check that the dispatch is
    /// wired correctly and nothing drifts. Each bar here has H-L = 2 and no gap
    /// large enough to exceed it, so TR == 2 on every bar including the first.
    #[test]
    fn atr_constant_true_range_is_stable_for_all_smoothers() {
        for smoothing in [
            AtrSmoothingType::Wilders,
            AtrSmoothingType::Sma,
            AtrSmoothingType::Ema,
        ] {
            let mut atr = StreamingAtr::new(AtrConfig {
                window: 3,
                smoothing,
            });
            let mut last = None;
            for _ in 0..6 {
                last = atr.update(mock_candle(10., 11., 9., 10., 100.));
            }
            assert_eq!(
                last,
                Some(2.0),
                "smoother {smoothing:?} should settle at TR"
            );
        }
    }

    /// `reset` must clear `prev_close` so the next bar is treated as a fresh first
    /// bar (TR = High - Low), not as a gap from the stale close.
    #[test]
    fn atr_reset_clears_previous_close() {
        let mut atr = StreamingAtr::new(AtrConfig {
            window: 1,
            smoothing: AtrSmoothingType::Sma,
        });
        atr.update(mock_candle(10., 15., 5., 12., 100.)); // prev_close becomes 12

        atr.reset();

        // Without the reset, TR would be max(5, |25-12|, |20-12|) = 13.
        let after = atr.update(mock_candle(20., 25., 20., 24., 100.));
        assert_eq!(after, Some(5.0)); // 25 - 20, treated as a first bar
    }

    /// The documented default is a 14-period Wilder's ATR.
    #[test]
    fn atr_default_is_wilders_window_14() {
        let atr = StreamingAtr::default();
        assert_eq!(atr.window_size, 14);
        assert!(matches!(atr.smoother, AtrSmoother::Wilders(_)));
    }

    // ============================================================================================
    // VWAP TESTS
    // ============================================================================================

    #[test]
    fn ohlcv_vwap_accumulates_and_ignores_bad_volume() {
        let mut vwap = StreamingOhlcvVwap::new(AggregatedPrice::Hlc3);

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

        let mut vwap_hlc3 = StreamingOhlcvVwap::new(AggregatedPrice::Hlc3);
        assert_eq!(vwap_hlc3.update(candle), Some((20. + 10. + 18.) / 3.0)); // 16.0

        let mut vwap_hl2 = StreamingOhlcvVwap::new(AggregatedPrice::Hl2);
        assert_eq!(vwap_hl2.update(candle), Some(f64::midpoint(20., 10.))); // 15.0

        let mut vwap_ohlc4 = StreamingOhlcvVwap::new(AggregatedPrice::Ohlc4);
        assert_eq!(
            vwap_ohlc4.update(candle),
            Some((10. + 20. + 10. + 18.) / 4.0)
        ); // 14.5

        let mut vwap_close = StreamingOhlcvVwap::new(AggregatedPrice::Close);
        assert_eq!(vwap_close.update(candle), Some(18.0)); // 18.0
    }

    /// Before any positive-volume bar arrives, the VWAP is undefined (`None`), even
    /// after zero/negative-volume bars have been fed.
    #[test]
    fn ohlcv_vwap_is_none_until_positive_volume() {
        let mut vwap = StreamingOhlcvVwap::new(AggregatedPrice::Hlc3);
        assert_eq!(vwap.update(mock_candle(0., 10., 8., 9., 0.)), None);
        assert_eq!(vwap.update(mock_candle(0., 10., 8., 9., -5.)), None);
        // First valid bar establishes the average.
        assert_eq!(vwap.update(mock_candle(0., 10., 8., 9., 100.)), Some(9.0));
    }

    /// `reset` re-anchors the average: prior accumulation is dropped entirely.
    #[test]
    fn ohlcv_vwap_reset_re_anchors() {
        let mut vwap = StreamingOhlcvVwap::new(AggregatedPrice::Hlc3);
        vwap.update(mock_candle(0., 10., 8., 9., 100.));
        vwap.update(mock_candle(0., 20., 10., 15., 200.));
        assert_eq!(vwap.value(), Some(13.0));

        vwap.reset();
        assert_eq!(vwap.value(), None);

        // Fresh anchor: only the post-reset bar counts.
        assert_eq!(vwap.update(mock_candle(0., 20., 10., 15., 50.)), Some(15.0));
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

    /// Trades carry their own quantity as volume; non-positive quantities are
    /// discarded just like bar volumes are.
    #[test]
    fn trades_vwap_ignores_non_positive_quantity() {
        let mut vwap = StreamingTradesVwap::new();
        assert_eq!(vwap.update(mock_trade(100.0, 0.0)), None);
        assert_eq!(vwap.update(mock_trade(100.0, -3.0)), None);
        assert_eq!(vwap.update(mock_trade(100.0, 2.0)), Some(100.0));
        // A later zero-quantity trade leaves the average untouched.
        assert_eq!(vwap.update(mock_trade(999.0, 0.0)), Some(100.0));
    }

    /// The Kahan core recovers low-order bits that naive `f64` summation drops
    /// when a long tail of tiny terms is added to a larger running sum. (This
    /// really belongs beside `KahanSum` in the accumulators module; included here
    /// as it underpins VWAP precision.)
    #[test]
    fn kahan_sum_recovers_precision_lost_by_naive_summation() {
        use crate::math::accumulators::KahanSum;

        // A leading 1.0 followed by many terms below its ULP: naive addition drops
        // each one, Kahan accumulates them.
        let mut values = vec![1.0];
        values.extend(std::iter::repeat_n(1e-16, 100));
        let truth = 1.0 + 100.0 * 1e-16;

        let kahan = values
            .iter()
            .fold(KahanSum::default(), |acc, &v| acc.add(v))
            .value();
        let naive: f64 = values.iter().sum();

        assert_f64_eq!(kahan, truth); // exact
        assert_f64_eq!(naive, 1.0); // every tiny term lost
        assert!((kahan - truth).abs() < (naive - truth).abs());
    }
}
