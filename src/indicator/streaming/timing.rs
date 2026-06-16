use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

use crate::{
    data::event::Ohlcv, indicator::streaming::StreamingIndicator, ring_buffer::RingBuffer,
};

// ================================================================================================
// TD X Sequential
// ================================================================================================

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum TdDirection {
    /// "Buy Setup": Triggered when a sequence of lower closes completes. Expect
    /// a bounce.
    BullishReversal,
    /// "Sell Setup": Triggered when a sequence of higher closes completes.
    /// Expect a drop.
    BearishReversal,
}

/// Represents the relationship between the current close and the historical
/// close N bars ago.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
pub enum PriceRelationship {
    /// The current close is strictly higher than the historical close.
    Higher,
    /// The current close is strictly lower than the historical close.
    Lower,
    /// The close is perfectly equal, or the indicator has just been initialized
    /// (neutral state). If an invalid float (`NaN`) is encountered, it
    /// gracefully defaults to this state.
    #[default]
    Flat,
}

impl From<Option<Ordering>> for PriceRelationship {
    fn from(cmp: Option<Ordering>) -> Self {
        match cmp {
            Some(Ordering::Less) => Self::Lower,
            Some(Ordering::Greater) => Self::Higher,
            Some(Ordering::Equal) | None => Self::Flat,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
enum InternalSetupState {
    /// No active sequence is being tracked.
    #[default]
    Neutral,
    /// Actively tracking a sequence of higher or lower closes.
    Tracking {
        direction: TdDirection,
        count: usize,
    },
}

impl InternalSetupState {
    const fn new_bullish() -> Self {
        Self::Tracking {
            direction: TdDirection::BullishReversal,
            count: 1,
        }
    }

    const fn new_bearish() -> Self {
        Self::Tracking {
            direction: TdDirection::BearishReversal,
            count: 1,
        }
    }

    const fn increment(self) -> Self {
        match self {
            Self::Tracking { direction, count } => Self::Tracking {
                direction,
                count: count + 1,
            },
            Self::Neutral => Self::Neutral,
        }
    }
}

// ================================================================================================
// Indicator: TD X Sequential Setup
// ================================================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingTdXSequential {
    target_count: usize,
    buffer: RingBuffer<f64>,
    state: InternalSetupState,
    last_cmp: PriceRelationship,
}

impl Default for StreamingTdXSequential {
    fn default() -> Self {
        Self::td9()
    }
}

impl StreamingTdXSequential {
    /// Creates a TD setup detector with the given lookback and target count.
    ///
    /// # Panics
    /// Panics if `target_count == 0`.
    #[must_use]
    pub fn new(lookback: usize, target_count: usize) -> Self {
        assert!(
            target_count > 0,
            "TD setup target_count must be strictly greater than 0"
        );
        Self {
            target_count,
            buffer: RingBuffer::new(lookback),
            state: InternalSetupState::default(),
            last_cmp: PriceRelationship::default(),
        }
    }

    /// The canonical TD Sequential Setup: nine closes, each beyond the close
    /// four bars earlier. This is the standard. Prefer it unless you
    /// specifically want a non-standard variant.
    #[must_use]
    pub fn td9() -> Self {
        Self::td(9)
    }

    /// A TD Setup with a custom completion count, keeping the canonical 4-bar
    /// lookback. `td(9)` is the standard. Other counts are deliberate
    /// variations and are *not* the TD Countdown (which is a separate,
    /// unmodeled phase).
    #[must_use]
    pub fn td(target_count: usize) -> Self {
        Self::new(4, target_count)
    }
}

impl StreamingTdXSequential {
    /// Evaluates if the current sequence count constitutes a completed TD
    /// Setup.
    ///
    /// In standard TD methodology, a setup completes at exactly `target_count`
    /// (e.g., 9). By checking exact multiples (e.g., 18, 27), we handle
    /// "Setup Recycling", ensuring extreme trend breakouts continue to emit
    /// valid exhaustion signals rather than being silently ignored.
    const fn is_setup_completion(&self, count: usize) -> bool {
        count > 0 && count.is_multiple_of(self.target_count)
    }
}

impl StreamingIndicator for StreamingTdXSequential {
    type Input = Ohlcv;
    type Output<'a> = Option<TdDirection>;

    fn update(&mut self, candle: Self::Input) -> Self::Output<'_> {
        let close = candle.close.0;
        let historical_close = self.buffer.push(close)?;
        let current_cmp = PriceRelationship::from(close.partial_cmp(&historical_close));

        self.state = match current_cmp {
            PriceRelationship::Lower => match self.state {
                InternalSetupState::Tracking {
                    direction: TdDirection::BullishReversal,
                    ..
                } => self.state.increment(),
                _ if self.last_cmp == PriceRelationship::Higher => {
                    InternalSetupState::new_bullish()
                }
                _ => InternalSetupState::Neutral,
            },
            PriceRelationship::Higher => match self.state {
                InternalSetupState::Tracking {
                    direction: TdDirection::BearishReversal,
                    ..
                } => self.state.increment(),
                _ if self.last_cmp == PriceRelationship::Lower => InternalSetupState::new_bearish(),
                _ => InternalSetupState::Neutral,
            },
            PriceRelationship::Flat => InternalSetupState::Neutral,
        };

        self.last_cmp = current_cmp;

        if let InternalSetupState::Tracking { direction, count } = self.state
            && self.is_setup_completion(count)
        {
            return Some(direction);
        }

        None
    }

    fn reset(&mut self) {
        self.buffer.clear();
        self.state = InternalSetupState::default();
        self.last_cmp = PriceRelationship::default();
    }
}

// ================================================================================================
// Indicator: TD Sequential (Setup + Countdown)
// ================================================================================================

/// Standard number of qualifying bars that complete a TD Countdown.
const COUNTDOWN_TARGET_STANDARD: usize = 13;

/// The Countdown compares the close to the low/high this many bars earlier.
const COUNTDOWN_LOOKBACK: usize = 2;

/// The Countdown's final bar is validated against the close of this earlier
/// count (`DeMark`'s "8th bar" rule). Ignored for targets at or below this
/// value.
const COUNTDOWN_QUALIFIER_BAR: usize = 8;

/// Selects which bar the Countdown (phase 2) starts counting on, once a Setup
/// (phase 1) completes.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
pub enum CountdownStart {
    /// Inclusive convention (the default): The Setup-completion bar is itself
    /// eligible as Countdown bar 1. If that bar's close already satisfies the
    /// Countdown comparison, it is counted immediately. The bar still emits
    /// [`TdSignal::Setup`]. The Countdown completion can only land on a later
    /// bar.
    #[default]
    SetupBar,
    /// Exclusive convention: the Countdown begins on the bar **after** the
    /// Setup completes. The completion bar emits only [`TdSignal::Setup`]
    /// and is never counted toward the Countdown.
    NextBar,
}

impl CountdownStart {
    #[must_use]
    pub const fn is_next_bar(&self) -> bool {
        matches!(self, Self::NextBar)
    }
}

/// The signal emitted by [`StreamingTdSequential`], identifying which phase of
/// the two-phase sequence just completed.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum TdSignal {
    /// Phase 1 complete: a TD Setup of this direction finished (the classic
    /// "9"). A Countdown of the same direction begins immediately
    /// afterwards.
    Setup(TdDirection),
    /// Phase 2 complete: a TD Countdown of this direction finished (the classic
    /// "13"). This is the terminal, high-conviction exhaustion signal.
    Countdown(TdDirection),
}

/// Internal state for an in-progress Countdown (phase 2).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
struct Countdown {
    direction: TdDirection,
    count: usize,
    /// Close of the qualifier bar (the 8th counted bar), referenced by the
    /// final bar.
    qualifier_close: Option<f64>,
}

/// Represents the outcome of evaluating a bar against a running Countdown.
#[derive(Debug, Clone, Copy, PartialEq)]
enum CountdownResult {
    /// The countdown is still in progress. Returns the updated state.
    Pending(Countdown),
    /// The countdown has reached its target and completed, yielding the final
    /// signal.
    Completed(TdDirection),
}

/// Context containing all necessary price data and targets to evaluate a bar
/// against a running Countdown.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CountdownEvalContext {
    /// The target number of qualifying bars required to complete the Countdown
    /// (standard is 13).
    target: usize,
    /// The current evaluated OHLCV bar.
    candle: Ohlcv,
    /// The lowest price from `COUNTDOWN_LOOKBACK` bars prior. Evaluated against
    /// the current `close` for bullish signals.
    low_n_back: Option<f64>,
    /// The highest price from `COUNTDOWN_LOOKBACK` bars prior. Evaluated
    /// against the current `close` for bearish signals.
    high_n_back: Option<f64>,
}

impl Countdown {
    const fn new(direction: TdDirection) -> Self {
        Self {
            direction,
            count: 0,
            qualifier_close: None,
        }
    }

    /// Evaluates the current bar against the Countdown rules, advancing the
    /// count when the bar qualifies. Qualifying bars need NOT be
    /// consecutive. Bars that do not qualify are simply skipped.
    ///
    /// Consumes the current state by value and yields back either a newly
    /// constructed, advanced state or the final completion signal.
    ///
    /// # Returns
    ///
    /// Returns a [`CountdownResult`]. If the countdown completes on this bar,
    /// returns `Completed(TdDirection)`. Otherwise, returns
    /// `Pending(Countdown)` yielding a new instance with the potentially
    /// updated state.
    fn evaluate(self, ctx: CountdownEvalContext) -> CountdownResult {
        let CountdownEvalContext {
            target,
            candle,
            low_n_back,
            high_n_back,
        } = ctx;
        let close = candle.close.0;
        let low = candle.low.0;
        let high = candle.high.0;

        let qualifies = match self.direction {
            // Buy Countdown: close at or below the low `COUNTDOWN_LOOKBACK` bars earlier.
            TdDirection::BullishReversal => matches!(low_n_back, Some(l) if close <= l),
            // Sell Countdown: close at or above the high `COUNTDOWN_LOOKBACK` bars earlier.
            TdDirection::BearishReversal => matches!(high_n_back, Some(h) if close >= h),
        };

        if !qualifies {
            return CountdownResult::Pending(self);
        }

        let tentative = self.count + 1;

        // Not the final bar yet: advance, remembering the qualifier-bar close.
        if tentative < target {
            let new_qualifier = if tentative == COUNTDOWN_QUALIFIER_BAR {
                Some(close)
            } else {
                self.qualifier_close
            };

            return CountdownResult::Pending(Self {
                count: tentative,
                qualifier_close: new_qualifier,
                ..self
            });
        }

        // Final bar check: DeMark requires the final bar's extreme to exceed the
        // 8th bar's close. If the target is smaller than 8 (no qualifier recorded),
        // this rule is safely bypassed.
        let is_final_ok = match (self.direction, self.qualifier_close) {
            (TdDirection::BullishReversal, Some(c8)) => low <= c8,
            (TdDirection::BearishReversal, Some(c8)) => high >= c8,
            (_, None) => true,
        };

        if is_final_ok {
            CountdownResult::Completed(self.direction)
        } else {
            // Defer: stay one short and wait for a fully-qualifying bar.
            CountdownResult::Pending(self)
        }
    }
}

/// Full two-phase **TD Sequential**: a TD Setup (phase 1) followed by a TD
/// Countdown (phase 2).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingTdSequential {
    setup: StreamingTdXSequential,
    countdown_target: usize,
    countdown_start: CountdownStart,
    low_buf: RingBuffer<f64>,
    high_buf: RingBuffer<f64>,
    countdown: Option<Countdown>,
}

impl Default for StreamingTdSequential {
    fn default() -> Self {
        Self::td9_13()
    }
}

impl StreamingTdSequential {
    /// Composes a phase-1 `setup` detector with a `countdown_target`, using the
    /// canonical 2-bar low/high comparison for phase 2 and the default
    /// [`CountdownStart`].
    ///
    /// The result is non-standard unless `setup` is
    /// [`StreamingTdXSequential::td9`] and `countdown_target` is 13
    /// (see [`Self::td9_13`]). Override the start convention with
    /// [`Self::with_countdown_start`].
    ///
    /// # Panics
    ///
    /// Panics if `countdown_target` is <= 0.
    #[must_use]
    pub fn new(setup: StreamingTdXSequential, countdown_target: usize) -> Self {
        assert!(
            countdown_target > 0,
            "TD countdown_target must be strictly greater than 0. Got {countdown_target} <= 0."
        );
        Self {
            setup,
            countdown_target,
            countdown_start: CountdownStart::default(),
            low_buf: RingBuffer::new(COUNTDOWN_LOOKBACK),
            high_buf: RingBuffer::new(COUNTDOWN_LOOKBACK),
            countdown: None,
        }
    }

    /// Sets the [`CountdownStart`] convention.
    #[must_use]
    pub fn with_countdown_start(self, start: CountdownStart) -> Self {
        Self {
            countdown_start: start,
            ..self
        }
    }

    /// The configured [`CountdownStart`] convention.
    #[must_use]
    pub const fn countdown_start(&self) -> CountdownStart {
        self.countdown_start
    }

    /// The canonical TD Sequential: a 9-bar Setup followed by a 13-bar
    /// Countdown. This is the standard configuration traders expect on
    /// `TradingView`, and the constructor to reach for unless you have a
    /// specific reason not to.
    #[must_use]
    pub fn td9_13() -> Self {
        Self::new(StreamingTdXSequential::td9(), COUNTDOWN_TARGET_STANDARD)
    }

    /// The current Countdown count, if a Countdown (phase 2) is in progress.
    #[must_use]
    pub fn countdown_progress(&self) -> Option<usize> {
        self.countdown.map(|c| c.count)
    }
}

impl StreamingTdSequential {
    /// Executes Phase 1: TD Setup.
    ///
    /// Analyzes the td9 setup signal. If a setup completes that contradicts the
    /// currently running countdown (or if no countdown is active), it
    /// establishes a new trend, resets the Phase 2 engine, and emits the
    /// Setup signal.
    ///
    /// Setup completions in the same direction ("recycles") are ignored.
    fn process_phase_1(&mut self, td9_setup_signal: Option<TdDirection>) -> Option<TdSignal> {
        td9_setup_signal.and_then(|direction| {
            let is_new_trend = self
                .countdown
                .as_ref()
                .is_none_or(|active| active.direction != direction);

            if is_new_trend {
                // Initialize a fresh Phase 2 engine for the new trend.
                self.countdown = Some(Countdown::new(direction));
                Some(TdSignal::Setup(direction))
            } else {
                // Recycle: Setup direction matches running countdown. Ignore it.
                None
            }
        })
    }

    /// Executes Phase 2: TD Countdown.
    ///
    /// Advances the active countdown using the rolling lookback buffers.
    /// Returns the terminal Countdown signal if the target is reached on this
    /// bar.
    fn process_phase_2(
        &mut self,
        candle: Ohlcv,
        low_n_back: Option<f64>,
        high_n_back: Option<f64>,
    ) -> Option<TdSignal> {
        match self.countdown.take() {
            Some(active) => {
                let ctx = CountdownEvalContext {
                    target: self.countdown_target,
                    candle,
                    low_n_back,
                    high_n_back,
                };

                match active.evaluate(ctx) {
                    CountdownResult::Completed(direction) => {
                        // Countdown is exhausted. Emit the climax signal.
                        Some(TdSignal::Countdown(direction))
                    }
                    CountdownResult::Pending(updated_countdown) => {
                        // Still counting. Put the updated state back into the struct.
                        self.countdown = Some(updated_countdown);
                        None
                    }
                }
            }
            None => None,
        }
    }
}

impl StreamingIndicator for StreamingTdSequential {
    type Input = Ohlcv;
    type Output<'a> = Option<TdSignal>;

    fn update(&mut self, candle: Self::Input) -> Self::Output<'_> {
        // 1. Maintain the macro state
        let td9_setup_signal = self.setup.update(candle);
        let low_n_back = self.low_buf.push(candle.low.0);
        let high_n_back = self.high_buf.push(candle.high.0);

        // 2. Evaluate Phase 1
        let new_setup_signal = self.process_phase_1(td9_setup_signal);

        // 3. Early exit if configured to wait a bar before starting Phase 2
        if new_setup_signal.is_some() && self.countdown_start.is_next_bar() {
            return new_setup_signal;
        }

        // 4. Evaluate Phase 2
        let countdown_signal = self.process_phase_2(candle, low_n_back, high_n_back);

        // 5. Tie-breaker and Signal resolution
        // If a new setup occurred, emit it.
        // Otherwise, emit the countdown signal (which will be None if still pending).
        new_setup_signal.or(countdown_signal)
    }

    fn reset(&mut self) {
        self.setup.reset();
        self.low_buf.clear();
        self.high_buf.clear();
        self.countdown = None;
    }
}

#[cfg(test)]
mod tests {
    #![expect(
        clippy::unwrap_used,
        clippy::expect_used,
        reason = "tests assert against known-valid fixtures; unwrap and expect surface failures as panics that fail the test"
    )]
    use chrono::{DateTime, Utc};

    use super::*;
    use crate::data::domain::{Price, Quantity};

    /// Parse RFC3339 timestamp string to `DateTime`<Utc>.
    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    /// Builds a flat OHLCV candle whose only meaningful field for this
    /// indicator is `close`. Timestamps are fixed.
    fn candle(close: f64) -> Ohlcv {
        Ohlcv {
            open_timestamp: ts("2026-06-11T17:30:00Z"),
            close_timestamp: ts("2026-06-11T17:31:00Z"),
            open: Price(close),
            high: Price(close),
            low: Price(close),
            close: Price(close),
            volume: Quantity(0.0),
            quote_asset_volume: None,
            number_of_trades: None,
            taker_buy_base_asset_volume: None,
            taker_buy_quote_asset_volume: None,
        }
    }

    /// Like [`candle`] but with explicit high/low, for Countdown tests that
    /// need the close compared against prior lows/highs.
    fn candle_hlc(high: f64, low: f64, close: f64) -> Ohlcv {
        Ohlcv {
            open_timestamp: ts("2026-06-11T17:30:00Z"),
            close_timestamp: ts("2026-06-11T17:31:00Z"),
            open: Price(close),
            high: Price(high),
            low: Price(low),
            close: Price(close),
            volume: Quantity(0.0),
            quote_asset_volume: None,
            number_of_trades: None,
            taker_buy_base_asset_volume: None,
            taker_buy_quote_asset_volume: None,
        }
    }

    fn feed_tdx(td: &mut StreamingTdXSequential, closes: &[f64]) -> Vec<Option<TdDirection>> {
        closes.iter().map(|&c| td.update(candle(c))).collect()
    }

    fn feed_td(td: &mut StreamingTdSequential, closes: &[f64]) -> Vec<Option<TdSignal>> {
        closes.iter().map(|&c| td.update(candle(c))).collect()
    }

    /// A test helper that evaluates one bar against a Countdown.
    /// It asserts that the Countdown is still running (Pending) and returns the
    /// updated Countdown state.
    fn step(cd: Countdown, ctx: CountdownEvalContext) -> Countdown {
        match cd.evaluate(ctx) {
            CountdownResult::Pending(next) => next,
            CountdownResult::Completed(dir) => panic!("unexpected completion: {dir:?}"),
        }
    }

    /// A buy-side [`CountdownEvalContext`]. `high` is irrelevant to buy logic,
    /// so it mirrors the close.
    fn buy_ctx(target: usize, close: f64, low: f64, low_n_back: f64) -> CountdownEvalContext {
        CountdownEvalContext {
            target,
            candle: candle_hlc(close, low, close),
            low_n_back: Some(low_n_back),
            high_n_back: None,
        }
    }

    /// A sell-side [`CountdownEvalContext`]. `low` is irrelevant to sell logic,
    /// so it mirrors the close.
    fn sell_ctx(target: usize, close: f64, high: f64, high_n_back: f64) -> CountdownEvalContext {
        CountdownEvalContext {
            target,
            candle: candle_hlc(high, close, close),
            low_n_back: None,
            high_n_back: Some(high_n_back),
        }
    }

    /// A pure decline never produces a bearish price flip, so per `DeMark` no
    /// buy setup ever begins. (Lookback 1 used so each bar compares to the
    /// prior close.)
    #[test]
    fn monotonic_decline_without_flip_emits_nothing() {
        let mut td = StreamingTdXSequential::new(1, 3);
        let out = feed_tdx(&mut td, &[50.0, 40.0, 30.0, 20.0, 10.0]);
        assert!(out.iter().all(std::option::Option::is_none));
        assert_eq!(td.state, InternalSetupState::Neutral);
    }

    /// A higher close followed by a lower close is the bearish flip that opens
    /// a buy setup at count 1.
    #[test]
    fn bearish_flip_starts_buy_setup() {
        let mut td = StreamingTdXSequential::new(1, 9);
        feed_tdx(&mut td, &[10.0, 20.0, 19.0]); // up, then the first lower close = flip
        assert_eq!(
            td.state,
            InternalSetupState::Tracking {
                direction: TdDirection::BullishReversal,
                count: 1
            }
        );
    }

    /// Flip + two more consecutive lower closes reaches target 3 and emits.
    #[test]
    fn completes_buy_setup_and_emits_bullish() {
        let mut td = StreamingTdXSequential::new(1, 3);
        let out = feed_tdx(&mut td, &[10.0, 20.0, 19.0, 18.0, 17.0]);
        assert_eq!(
            out,
            vec![None, None, None, None, Some(TdDirection::BullishReversal)]
        );
    }

    /// Symmetric sell-setup case: a bullish flip then rising closes.
    #[test]
    fn completes_sell_setup_and_emits_bearish() {
        let mut td = StreamingTdXSequential::new(1, 3);
        let out = feed_tdx(&mut td, &[20.0, 10.0, 11.0, 12.0, 13.0]);
        assert_eq!(
            out,
            vec![None, None, None, None, Some(TdDirection::BearishReversal)]
        );
    }

    /// A flip in the opposite direction destroys the in-progress setup and
    /// starts the other side at count 1.
    #[test]
    fn opposite_flip_resets_to_other_direction() {
        let mut td = StreamingTdXSequential::new(1, 5);
        feed_tdx(&mut td, &[10.0, 20.0, 19.0, 18.0]); // bullish, count 2
        assert_eq!(
            td.state,
            InternalSetupState::Tracking {
                direction: TdDirection::BullishReversal,
                count: 2
            }
        );

        // 25.0 > 18.0 (higher) right after a lower close = bullish flip -> sell setup.
        assert_eq!(td.update(candle(25.0)), None);
        assert_eq!(
            td.state,
            InternalSetupState::Tracking {
                direction: TdDirection::BearishReversal,
                count: 1
            }
        );
    }

    /// An exactly-equal close fails the strict comparison and breaks the run.
    #[test]
    fn flat_close_breaks_sequence() {
        let mut td = StreamingTdXSequential::new(1, 5);
        feed_tdx(&mut td, &[10.0, 20.0, 19.0]); // bullish, count 1
        assert_eq!(td.update(candle(19.0)), None); // 19 == 19 -> Flat
        assert_eq!(td.state, InternalSetupState::Neutral);
    }

    /// The `count % target == 0` rule re-emits on each multiple ("Setup
    /// Recycling").
    #[test]
    fn recycles_and_re_emits_at_second_multiple() {
        let mut td = StreamingTdXSequential::new(1, 3);
        let out = feed_tdx(&mut td, &[10.0, 20.0, 19.0, 18.0, 17.0, 16.0, 15.0, 14.0]);
        let signal_count = out.iter().filter(|o| o.is_some()).count();
        assert_eq!(signal_count, 2);
        assert_eq!(out[4], Some(TdDirection::BullishReversal)); // count 3
        assert_eq!(out[7], Some(TdDirection::BullishReversal)); // count 6
    }

    /// Until the ring buffer is full there is no historical close to compare
    /// to, and the very first comparison has no prior relationship to flip
    /// from.
    #[test]
    fn warmup_blocks_comparison_until_buffer_full() {
        let mut td = StreamingTdXSequential::td9(); // lookback 4
        let out = feed_tdx(&mut td, &[10.0, 10.0, 10.0, 10.0, 11.0]);
        assert!(out.iter().all(std::option::Option::is_none));
        assert_eq!(td.state, InternalSetupState::Neutral);
    }

    /// A `NaN` close compares as `None` -> `Flat`; it must not panic and must
    /// break any active sequence.
    #[test]
    fn nan_close_is_treated_as_flat_and_does_not_panic() {
        let mut td = StreamingTdXSequential::new(1, 3);
        let out = feed_tdx(&mut td, &[10.0, 20.0, f64::NAN]);
        assert_eq!(out, vec![None, None, None]);
        assert_eq!(td.state, InternalSetupState::Neutral);
    }

    /// `reset` clears the buffer (so warm-up restarts) and the state machine.
    #[test]
    fn reset_restores_warmup_and_neutral_state() {
        let mut td = StreamingTdXSequential::new(1, 3);
        feed_tdx(&mut td, &[10.0, 20.0, 19.0, 18.0]);
        td.reset();
        assert_eq!(td.state, InternalSetupState::Neutral);
        assert_eq!(td.last_cmp, PriceRelationship::Flat);
        // Buffer empty again: the next candle is just a warm-up bar.
        assert_eq!(td.update(candle(5.0)), None);
    }

    // ============================================================================================
    // Two-phase TD Sequential (Setup + Countdown)
    // ============================================================================================

    /// A buy setup completes (phase 1), then four qualifying down-bars complete
    /// a (shortened) countdown (phase 2). Uses a tiny setup/countdown so
    /// the whole two-phase life cycle fits in nine bars. `NextBar`
    /// convention: the countdown starts counting the bar after the setup
    /// completes.
    #[test]
    fn two_phase_completes_setup_then_countdown_bullish() {
        // setup: lookback 1, target 3; countdown target 4 (below the 8th-bar rule).
        let mut td = StreamingTdSequential::new(StreamingTdXSequential::new(1, 3), 4)
            .with_countdown_start(CountdownStart::NextBar);
        let out = feed_td(&mut td, &[10.0, 12.0, 11.0, 10.0, 9.0, 8.0, 7.0, 6.0, 5.0]);

        let mut expected = vec![None; 9];
        expected[4] = Some(TdSignal::Setup(TdDirection::BullishReversal));
        expected[8] = Some(TdSignal::Countdown(TdDirection::BullishReversal));
        assert_eq!(out, expected);
        assert_eq!(td.countdown_progress(), None); // consumed on completion
    }

    /// Same input under the `SetupBar` convention: the setup-completion bar
    /// (idx 4) is itself counted as countdown bar 1, so completion lands
    /// one bar earlier.
    #[test]
    fn two_phase_setup_bar_convention_counts_completion_bar() {
        let mut td = StreamingTdSequential::new(StreamingTdXSequential::new(1, 3), 4)
            .with_countdown_start(CountdownStart::SetupBar);
        let out = feed_td(&mut td, &[10.0, 12.0, 11.0, 10.0, 9.0, 8.0, 7.0, 6.0, 5.0]);

        let mut expected = vec![None; 9];
        expected[4] = Some(TdSignal::Setup(TdDirection::BullishReversal));
        expected[7] = Some(TdSignal::Countdown(TdDirection::BullishReversal));
        assert_eq!(out, expected);
    }

    /// The default convention is `SetupBar`, and `td9_13` inherits it.
    #[test]
    fn default_countdown_start_is_setup_bar() {
        assert_eq!(CountdownStart::default(), CountdownStart::SetupBar);
        assert_eq!(
            StreamingTdSequential::td9_13().countdown_start(),
            CountdownStart::SetupBar
        );
    }

    /// The two conventions are otherwise identical: on the same input the
    /// countdown completes exactly one bar earlier under `SetupBar`.
    #[test]
    fn setup_bar_completes_one_bar_before_next_bar() {
        let series = [10.0, 12.0, 11.0, 10.0, 9.0, 8.0, 7.0, 6.0, 5.0];
        let find_countdown = |start| {
            let mut td = StreamingTdSequential::new(StreamingTdXSequential::new(1, 3), 4)
                .with_countdown_start(start);
            feed_td(&mut td, &series)
                .iter()
                .position(|o| matches!(o, Some(TdSignal::Countdown(_))))
                .unwrap()
        };
        let inclusive = find_countdown(CountdownStart::SetupBar);
        let exclusive = find_countdown(CountdownStart::NextBar);
        assert_eq!(exclusive, inclusive + 1);
    }

    /// Symmetric sell-side path: sell setup then four qualifying up-bars
    /// (`NextBar` convention).
    #[test]
    fn two_phase_completes_setup_then_countdown_bearish() {
        let mut td = StreamingTdSequential::new(StreamingTdXSequential::new(1, 3), 4)
            .with_countdown_start(CountdownStart::NextBar);
        let out = feed_td(
            &mut td,
            &[10.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0],
        );

        let mut expected = vec![None; 9];
        expected[4] = Some(TdSignal::Setup(TdDirection::BearishReversal));
        expected[8] = Some(TdSignal::Countdown(TdDirection::BearishReversal));
        assert_eq!(out, expected);
    }

    /// An opposite setup completing mid-countdown cancels the running countdown
    /// and emits the new setup instead — the buy countdown never reaches 13.
    #[test]
    fn opposite_setup_cancels_active_countdown() {
        let mut td = StreamingTdSequential::new(StreamingTdXSequential::new(1, 3), 13)
            .with_countdown_start(CountdownStart::NextBar);
        // Buy setup completes at idx 4; then three rising closes complete a sell
        // setup at idx 7 while the buy countdown is still far from 13.
        let out = feed_td(&mut td, &[10.0, 12.0, 11.0, 10.0, 9.0, 20.0, 21.0, 22.0]);

        assert_eq!(out[4], Some(TdSignal::Setup(TdDirection::BullishReversal)));
        assert_eq!(out[7], Some(TdSignal::Setup(TdDirection::BearishReversal)));
        // No countdown ever fired.
        assert!(
            out.iter()
                .all(|o| !matches!(o, Some(TdSignal::Countdown(_))))
        );
        // The active countdown is now the freshly-opened bearish one.
        assert_eq!(td.countdown_progress(), Some(0));
    }

    /// The standard config emits nothing during warm-up.
    #[test]
    fn td9_13_silent_during_warmup() {
        let mut td = StreamingTdSequential::td9_13();
        let out = feed_td(&mut td, &[10.0, 10.0, 10.0, 10.0, 11.0]);
        assert!(out.iter().all(std::option::Option::is_none));
        assert_eq!(td.countdown_progress(), None);
    }

    /// The Countdown's non-consecutive rule: a bar that does not qualify leaves
    /// the state untouched; a qualifying one advances the count by one.
    #[test]
    fn countdown_skips_non_qualifying_bars() {
        let cd = Countdown::new(TdDirection::BullishReversal);

        // close 100 > low_n_back 10 -> does not qualify; count stays 0.
        let cd = step(cd, buy_ctx(13, 100.0, 100.0, 10.0));
        assert_eq!(cd.count, 0);

        // close 5 <= low_n_back 10 -> qualifies; count advances to 1.
        let cd = step(cd, buy_ctx(13, 5.0, 5.0, 10.0));
        assert_eq!(cd.count, 1);
    }

    /// The qualifier close is recorded exactly when the count reaches bar 8.
    /// Not before, not after. Guards the `tentative == COUNTDOWN_QUALIFIER_BAR`
    /// condition against off-by-one refactors.
    #[test]
    fn countdown_records_qualifier_close_exactly_at_bar_eight() {
        let mut cd = Countdown::new(TdDirection::BullishReversal);

        // Counts 1..=7 carry distinct closes, but the qualifier must stay unset.
        for count in 1..=7_u64 {
            let count_f64 =
                f64::from(u32::try_from(count).expect("countdown count exceeds u32 range"));
            cd = step(cd, buy_ctx(13, count_f64, 0.0, 1000.0));
            assert_eq!(cd.count as u64, count);
            assert_eq!(cd.qualifier_close, None, "qualifier set before bar 8");
        }

        // The 8th qualifying bar records its close — and only it.
        cd = step(cd, buy_ctx(13, 42.0, 0.0, 1000.0));
        assert_eq!(cd.count, 8);
        assert_eq!(cd.qualifier_close, Some(42.0));
    }

    /// Regression guard for the `..self` carry-over in `evaluate`: once the
    /// qualifier close is recorded at bar 8, every later advance (and every
    /// skipped bar) must preserve it. A refactor that drops the struct spread
    /// or resets the field on non-qualifier bars would be caught here.
    #[test]
    fn countdown_preserves_qualifier_close_after_it_is_set() {
        let mut cd = Countdown::new(TdDirection::BullishReversal);
        for _ in 0..7 {
            cd = step(cd, buy_ctx(13, 0.0, 0.0, 1000.0));
        }
        cd = step(cd, buy_ctx(13, 42.0, 0.0, 1000.0)); // bar 8 records qualifier = 42
        assert_eq!(cd.qualifier_close, Some(42.0));

        // Advancing bars 9..=12 (closes deliberately != 42) must carry it unchanged.
        for expected in 9..=12 {
            cd = step(cd, buy_ctx(13, 7.0, 0.0, 1000.0));
            assert_eq!(cd.count, expected);
            assert_eq!(
                cd.qualifier_close,
                Some(42.0),
                "qualifier_close must persist after bar 8"
            );
        }

        // A non-qualifying bar (no advance) must also keep it.
        let cd = step(cd, buy_ctx(13, 999.0, 0.0, 10.0)); // close 999 > 10 -> skipped
        assert_eq!(cd.count, 12);
        assert_eq!(cd.qualifier_close, Some(42.0));
    }

    /// The 8th-bar deferral: a would-be final bar whose low is above the close
    /// of bar 8 is deferred (state unchanged) until a fully-qualifying bar
    /// arrives.
    #[test]
    fn countdown_defers_thirteenth_until_qualifier_met() {
        let mut cd = Countdown::new(TdDirection::BullishReversal);

        // Counts 1..=7 (low_n_back high enough that the close always qualifies).
        for _ in 0..7 {
            cd = step(cd, buy_ctx(13, 0.0, 0.0, 1000.0));
        }
        assert_eq!(cd.count, 7);

        // Count 8 records the qualifier close (50.0).
        cd = step(cd, buy_ctx(13, 50.0, 0.0, 1000.0));
        assert_eq!(cd.count, 8);
        assert_eq!(cd.qualifier_close, Some(50.0));

        // Counts 9..=12.
        for _ in 0..4 {
            cd = step(cd, buy_ctx(13, 0.0, 0.0, 1000.0));
        }
        assert_eq!(cd.count, 12);

        // 13th attempt: close qualifies, but low 60 > qualifier 50 -> deferred.
        let deferred = cd.evaluate(buy_ctx(13, 0.0, 60.0, 1000.0));
        assert_eq!(
            deferred,
            CountdownResult::Pending(cd),
            "deferral must not advance"
        );

        // 13th attempt: low 40 <= qualifier 50 -> completes.
        let completed = cd.evaluate(buy_ctx(13, 0.0, 40.0, 1000.0));
        assert_eq!(
            completed,
            CountdownResult::Completed(TdDirection::BullishReversal)
        );
    }

    /// Sell-side qualification (`close >= high_n_back`) and completion with a
    /// sub-8 target (the qualifier rule is inert).
    #[test]
    fn countdown_completes_sell_side() {
        let mut cd = Countdown::new(TdDirection::BearishReversal);

        // target 4: counts 1..=3 pending, then the 4th qualifying bar completes.
        for _ in 0..3 {
            cd = step(cd, sell_ctx(4, 100.0, 100.0, 10.0));
        }
        assert_eq!(cd.count, 3);
        assert_eq!(
            cd.evaluate(sell_ctx(4, 100.0, 100.0, 10.0)),
            CountdownResult::Completed(TdDirection::BearishReversal)
        );
    }

    /// Sell-side final-bar rule (`high >= close[8]`), exercised by constructing
    /// the pre-final state directly so the test stays compact.
    #[test]
    fn countdown_final_bar_respects_qualifier_for_sell() {
        let cd = Countdown {
            direction: TdDirection::BearishReversal,
            count: 12,
            qualifier_close: Some(50.0),
        };

        // high 45 < qualifier 50 -> deferred (state unchanged).
        let deferred = cd.evaluate(sell_ctx(13, 100.0, 45.0, 10.0));
        assert_eq!(deferred, CountdownResult::Pending(cd));

        // high 55 >= qualifier 50 -> completes.
        let completed = cd.evaluate(sell_ctx(13, 100.0, 55.0, 10.0));
        assert_eq!(
            completed,
            CountdownResult::Completed(TdDirection::BearishReversal)
        );
    }

    /// `reset` clears both phases.
    #[test]
    fn two_phase_reset_clears_everything() {
        let mut td = StreamingTdSequential::new(StreamingTdXSequential::new(1, 3), 4);
        feed_td(&mut td, &[10.0, 12.0, 11.0, 10.0, 9.0, 8.0]); // setup done, countdown counting
        assert!(td.countdown_progress().is_some());

        td.reset();
        assert_eq!(td.countdown_progress(), None);
        // Warm-up restarts: the next candle cannot emit anything.
        assert_eq!(td.update(candle(5.0)), None);
    }
}
