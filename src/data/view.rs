use chrono::{DateTime, Utc};
use std::{fmt::Debug, sync::Arc};

use crate::{
    data::{
        domain::{Price, Symbol},
        event::{
            ClosePriceProvider, EconomicCalendarId, EmaId, MarketEvent, MarketId, Ohlcv, OhlcvId,
            PriceReachable, RsiId, SmaId, StreamId, SymbolProvider, TpoId, TradesId,
            VolumeProfileId,
        },
    },
    error::{ChapatyError, ChapatyResult, DataError, SystemError},
    sim::{
        cursor::{Cursor, StreamEntity},
        cursor_group::CursorGroup,
        data::{EventMap, SimulationData},
    },
    sorted_vec_map::SortedVecMap,
};

// ================================================================================================
// Market View
// ================================================================================================

pub trait StreamView<'env> {
    type Id: StreamId;
    type Event: MarketEvent + 'env;

    /// Returns the slice of events visible at the current time step.
    fn get_slice(&self, id: &Self::Id) -> Option<&'env [Self::Event]>;

    #[inline]
    fn last_event(&self, id: &Self::Id) -> Option<&'env Self::Event> {
        self.get_slice(id).and_then(|s| s.last())
    }

    /// Returns an iterator over events in reverse chronological order (Newest -> Oldest).
    /// This is the primary access pattern for RL agents reacting to recent news.
    #[inline]
    fn rev_iter(
        &self,
        id: &Self::Id,
    ) -> Option<std::iter::Rev<std::slice::Iter<'env, Self::Event>>> {
        self.get_slice(id).map(|s| s.iter().rev())
    }

    /// Returns all *new* events with point in time after `since_ts`.
    /// Useful for agents to react to the latest news or ticks.
    #[inline]
    fn new_events_since(
        &self,
        id: &Self::Id,
        since_ts: DateTime<Utc>,
    ) -> Option<impl Iterator<Item = &'env Self::Event>> {
        // We take_while on the reverse iterator.
        // This is efficient because we stop as soon as we hit old data.
        self.rev_iter(id)
            .map(|iter| iter.take_while(move |e| e.point_in_time() > since_ts))
    }
}

/// Trait for Views that contain events capable of checking if a price was hit.
pub trait PriceCheckableView {
    /// Checks if any *new* event (since `since_ts`) matching `target_symbol` hit the `price`.
    fn reached_price_since(
        &self,
        target_symbol: &Symbol,
        price: Price,
        since_ts: DateTime<Utc>,
    ) -> bool;
}

/// Trait for Views that contain events capable of providing a close price.
pub trait ClosePriceView {
    /// Finds the timestamp and price of the most recent event for the target symbol.
    fn latest_price_for_symbol(&self, target_symbol: &Symbol) -> Option<(DateTime<Utc>, Price)>;
}

/// A strictly typed view into a slice of simulation data.
#[derive(Debug, Clone)]
pub struct View<'env, S: StreamId> {
    data: SortedVecMap<S, &'env [S::Event]>,
}

pub type OhlcvView<'env> = View<'env, OhlcvId>;
pub type TradeView<'env> = View<'env, TradesId>;
pub type EconomicCalendarView<'env> = View<'env, EconomicCalendarId>;
pub type VolumeProfileView<'env> = View<'env, VolumeProfileId>;
pub type TpoView<'env> = View<'env, TpoId>;
pub type EmaView<'env> = View<'env, EmaId>;
pub type SmaView<'env> = View<'env, SmaId>;
pub type RsiView<'env> = View<'env, RsiId>;

impl<'env, S: StreamId + 'env> StreamView<'env> for View<'env, S> {
    type Id = S;
    type Event = S::Event;

    #[inline]
    fn get_slice(&self, id: &S) -> Option<&'env [S::Event]> {
        self.data.get(id).copied()
    }
}

impl<'env, S> PriceCheckableView for View<'env, S>
where
    S: StreamId + SymbolProvider,
    S::Event: PriceReachable,
{
    fn reached_price_since(
        &self,
        target_symbol: &Symbol,
        price: Price,
        since_ts: DateTime<Utc>,
    ) -> bool {
        // Linear scan of all streams in this view is cheap (M < 100).
        self.data
            .iter()
            .filter(|(id, _)| id.symbol() == target_symbol)
            .any(|(_, events)| {
                // REVERSE ITERATION (Optimization):
                // We check the most recent events first.
                // We manually inline the logic of `new_events_since` to avoid
                // performing a second lookup for `id`.
                events
                    .iter()
                    .rev()
                    .take_while(|e| e.point_in_time() > since_ts)
                    .any(|e| e.price_reached(price))
            })
    }
}

impl<'env, S> ClosePriceView for View<'env, S>
where
    S: StreamId + SymbolProvider,
    S::Event: ClosePriceProvider,
{
    fn latest_price_for_symbol(&self, target_symbol: &Symbol) -> Option<(DateTime<Utc>, Price)> {
        self.data
            .iter()
            .filter(|(id, _)| id.symbol() == target_symbol)
            .filter_map(|(_, events)| events.last())
            .map(|e| (e.close_timestamp(), e.close_price()))
            .max_by_key(|(ts, _)| *ts)
    }
}

#[derive(Debug, Clone)]
pub struct MarketView<'env> {
    ohlcv: OhlcvView<'env>,
    trade: TradeView<'env>,
    economic_calendar: EconomicCalendarView<'env>,
    volume_profile: VolumeProfileView<'env>,
    tpo: TpoView<'env>,
    ema: EmaView<'env>,
    sma: SmaView<'env>,
    rsi: RsiView<'env>,

    // Tradable Markets
    market_ids: Arc<[MarketId]>,

    // Time State
    previous_ts: Option<DateTime<Utc>>,
    current_ts: DateTime<Utc>,
}

impl<'env> MarketView<'env> {
    pub(crate) fn new(sim_data: &'env SimulationData, cursor: &CursorGroup) -> ChapatyResult<Self> {
        Ok(Self {
            ohlcv: slice_map(sim_data.ohlcv(), cursor.ohlcv())?,
            trade: slice_map(sim_data.trade(), cursor.trade())?,
            economic_calendar: slice_map(sim_data.economic_cal(), cursor.economic_cal())?,
            volume_profile: slice_map(sim_data.volume_profile(), cursor.vp())?,
            tpo: slice_map(sim_data.tpo(), cursor.tpo())?,
            ema: slice_map(sim_data.ema(), cursor.ema())?,
            sma: slice_map(sim_data.sma(), cursor.sma())?,
            rsi: slice_map(sim_data.rsi(), cursor.rsi())?,
            previous_ts: cursor.previous_ts(),
            current_ts: cursor.current_ts(),
            market_ids: sim_data.market_ids(),
        })
    }

    pub fn ohlcv(&self) -> &OhlcvView<'env> {
        &self.ohlcv
    }
    pub fn trade(&self) -> &TradeView<'env> {
        &self.trade
    }
    pub fn economic_news(&self) -> &EconomicCalendarView<'env> {
        &self.economic_calendar
    }
    pub fn volume_profile(&self) -> &VolumeProfileView<'env> {
        &self.volume_profile
    }
    pub fn tpo(&self) -> &TpoView<'env> {
        &self.tpo
    }
    pub fn ema(&self) -> &EmaView<'env> {
        &self.ema
    }
    pub fn sma(&self) -> &SmaView<'env> {
        &self.sma
    }
    pub fn rsi(&self) -> &RsiView<'env> {
        &self.rsi
    }
    pub fn current_timestamp(&self) -> DateTime<Utc> {
        self.current_ts
    }
    pub fn previous_timestamp(&self) -> DateTime<Utc> {
        self.previous_ts.unwrap_or(DateTime::<Utc>::MIN_UTC)
    }
    pub fn market_ids(&self) -> Arc<[MarketId]> {
        self.market_ids.clone()
    }

    /// Returns a stack-allocated array of all views that support price checking.
    #[inline]
    fn all_price_checkable_views(&self) -> [&dyn PriceCheckableView; 5] {
        [&self.ohlcv, &self.trade, &self.ema, &self.sma, &self.rsi]
    }

    /// Returns a stack-allocated array of all views that provide a canonical market "Close" price.
    #[inline]
    fn all_close_price_views(&self) -> [&dyn ClosePriceView; 5] {
        [&self.ohlcv, &self.trade, &self.ema, &self.sma, &self.rsi]
    }

    /// Finds the candle active at the specific timestamp (Search: Newest to Oldest).
    pub fn find_candle(&self, id: &OhlcvId, ts: DateTime<Utc>) -> Option<Ohlcv> {
        self.ohlcv
            .rev_iter(id)?
            .find(|e| e.open_timestamp <= ts && ts < e.close_timestamp)
            .copied()
    }

    /// Returns `true` if `price` was reached by any *new* event since the last step.
    pub fn reached_price(&self, price: Price, target_symbol: &Symbol) -> bool {
        let prev = self.previous_timestamp();
        self.all_price_checkable_views()
            .into_iter()
            .any(|view| view.reached_price_since(target_symbol, price, prev))
    }

    /// Resolves the most recent, non-leaky close price.
    pub fn try_resolved_close_price(&self, target_symbol: &Symbol) -> ChapatyResult<Price> {
        let best_price = self
            .all_close_price_views()
            .into_iter()
            .filter_map(|view| view.latest_price_for_symbol(target_symbol))
            .max_by_key(|(ts, _)| *ts)
            .map(|(_, price)| price);

        best_price.ok_or_else(|| {
            ChapatyError::Data(DataError::KeyNotFound(format!(
                "No price events found for symbol {:?}",
                target_symbol
            )))
        })
    }
}

// ================================================================================================
// Helper Functions
// ================================================================================================

/// Efficiently zips a Cursor with Data to produce a View.
fn slice_map<'env, S>(data: &'env S::Storage, cursor: &Cursor<S>) -> ChapatyResult<View<'env, S>>
where
    S: StreamEntity<Storage = EventMap<S>>,
{
    // OPTIMIZATION: Lockstep Iteration (Zip)
    // We avoid looking up keys in `data` by relying on the construction invariant
    // that `Cursor` and `Data` have identical keys in identical order.
    let sliced_data = cursor.0.iter().zip(data.iter()).try_fold(
        SortedVecMap::with_capacity(cursor.0.len()),
        |mut acc, pair| {
            let ((cursor_id, range), (data_id, events)) = pair;
            debug_assert_eq!(cursor_id, data_id, "Cursor desynchronized from Storage!");

            if range.end > events.len() {
                return Err(ChapatyError::System(SystemError::IndexOutOfBounds(
                    format!(
                        "Cursor {:?} out of bounds: range end {} > len {}",
                        cursor_id,
                        range.end,
                        events.len()
                    ),
                )));
            }

            // Insert slice.
            // Since we insert in sorted order, this is effectively an O(1) push.
            acc.insert(*cursor_id, &events[0..range.end]);
            Ok(acc)
        },
    )?;

    Ok(View { data: sliced_data })
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::data::domain::{DataBroker, Exchange, Period, Price, Quantity, SpotPair};

    // ============================================================================
    // Test Helpers
    // ============================================================================

    /// Parse RFC3339 timestamp string to DateTime<Utc>.
    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    /// Create an OHLCV event with full control over price levels.
    fn ohlcv(
        open_ts: DateTime<Utc>,
        close_ts: DateTime<Utc>,
        open: f64,
        high: f64,
        low: f64,
        close: f64,
    ) -> Ohlcv {
        Ohlcv {
            open_timestamp: open_ts,
            close_timestamp: close_ts,
            open: Price(open),
            high: Price(high),
            low: Price(low),
            close: Price(close),
            volume: Quantity(1000.0),
            quote_asset_volume: None,
            number_of_trades: None,
            taker_buy_base_asset_volume: None,
            taker_buy_quote_asset_volume: None,
        }
    }

    /// Create a simple OHLCV with default prices.
    fn ohlcv_simple(open_ts: DateTime<Utc>, close_ts: DateTime<Utc>) -> Ohlcv {
        ohlcv(open_ts, close_ts, 100.0, 110.0, 90.0, 105.0)
    }

    /// Create an OhlcvId for testing.
    fn ohlcv_id(symbol: SpotPair, period: Period) -> OhlcvId {
        OhlcvId {
            broker: DataBroker::Binance,
            exchange: Exchange::Binance,
            symbol: Symbol::Spot(symbol),
            period,
        }
    }

    /// Helper to create a MarketView with specified OHLCV data and time state.
    fn market_view_with_ohlcv<'a>(
        ohlcv_data: SortedVecMap<OhlcvId, &'a [Ohlcv]>,
        previous_ts: Option<DateTime<Utc>>,
        current_ts: DateTime<Utc>,
    ) -> MarketView<'a> {
        MarketView {
            ohlcv: OhlcvView { data: ohlcv_data },
            trade: TradeView {
                data: SortedVecMap::new(),
            },
            economic_calendar: EconomicCalendarView {
                data: SortedVecMap::new(),
            },
            volume_profile: VolumeProfileView {
                data: SortedVecMap::new(),
            },
            tpo: TpoView {
                data: SortedVecMap::new(),
            },
            ema: EmaView {
                data: SortedVecMap::new(),
            },
            sma: SmaView {
                data: SortedVecMap::new(),
            },
            rsi: RsiView {
                data: SortedVecMap::new(),
            },
            previous_ts,
            current_ts,
            market_ids: Arc::new([]),
        }
    }

    // ============================================================================
    // Part 2: view.rs Tests (Business Logic)
    // Focus: PriceCheckableView and ClosePriceView implementations
    // ============================================================================

    // ==========================================================================
    // Test: reached_price (Reverse Iteration)
    // Constraint: Must iterate in reverse
    // The view checks if any NEW event (prev_ts < event_ts <= current_ts)
    // hit the price.
    // ==========================================================================

    #[test]
    fn test_reached_price_basic_hit() {
        // Simple case: price is within high-low range of a new candle
        let id = ohlcv_id(SpotPair::BtcUsdt, Period::Minute(3));
        let symbol = Symbol::Spot(SpotPair::BtcUsdt);

        // Candle: opens 2025-01-01 00:00, closes 00:03 (available at 00:03)
        // Range: [90, 110]
        let events = vec![ohlcv(
            ts("2025-01-01T00:00:00Z"),
            ts("2025-01-01T00:03:00Z"),
            100.0,
            110.0,
            90.0,
            105.0,
        )];

        let mut view_data = SortedVecMap::new();
        view_data.insert(id, events.as_slice());

        // previous_ts = 00:00:00, current_ts = 00:03:00
        // The candle point_in_time (00:03) > previous_ts (00:00), so it's "new"
        let market_view = market_view_with_ohlcv(
            view_data,
            Some(ts("2025-01-01T00:00:00Z")),
            ts("2025-01-01T00:03:00Z"),
        );

        assert!(
            market_view.reached_price(Price(110.0), &symbol),
            "High (110.0) should be reached"
        );
        assert!(
            market_view.reached_price(Price(90.0), &symbol),
            "Low (90.0) should be reached"
        );
        assert!(
            market_view.reached_price(Price(100.0), &symbol),
            "Price in range (100.0) should be reached"
        );
        assert!(
            !market_view.reached_price(Price(120.0), &symbol),
            "Price above high (120.0) should NOT be reached"
        );
        assert!(
            !market_view.reached_price(Price(80.0), &symbol),
            "Price below low (80.0) should NOT be reached"
        );
    }

    #[test]
    fn test_reached_price_ignores_old_data() {
        // Edge Case: Old data (events <= last_close_ts) hitting the price must be ignored.
        // This tests the "reverse iteration" logic with `since_ts` filtering.
        let id = ohlcv_id(SpotPair::BtcUsdt, Period::Minute(3));
        let symbol = Symbol::Spot(SpotPair::BtcUsdt);

        let events = vec![
            // OLD candle: available at 00:03:00, contains price 150
            ohlcv(
                ts("2025-02-01T00:00:00Z"),
                ts("2025-02-01T00:03:00Z"),
                100.0,
                150.0, // high = 150
                90.0,
                105.0,
            ),
            // NEW candle: available at 00:06:00, does NOT contain price 150
            ohlcv(
                ts("2025-02-01T00:03:00Z"),
                ts("2025-02-01T00:06:00Z"),
                105.0,
                115.0, // high = 115, low = 95, so 150 is NOT reached
                95.0,
                110.0,
            ),
        ];

        let mut view_data = SortedVecMap::new();
        view_data.insert(id, events.as_slice());

        // previous_ts = 00:03:00, current_ts = 00:06:00
        // Only the second candle (point_in_time 00:06) is "new" (00:06 > 00:03)
        // The first candle (point_in_time 00:03) is "old" (00:03 <= 00:03)
        let market_view = market_view_with_ohlcv(
            view_data,
            Some(ts("2025-02-01T00:03:00Z")),
            ts("2025-02-01T00:06:00Z"),
        );

        assert!(
            !market_view.reached_price(Price(150.0), &symbol),
            "Price 150 is in OLD candle (<=previous_ts), should be IGNORED"
        );
        assert!(
            market_view.reached_price(Price(115.0), &symbol),
            "Price 115 is in NEW candle, should be reached"
        );
    }

    #[test]
    fn test_reached_price_overlapping_candles_3m_vs_5m() {
        // Test: Multiple Timeframe Candles (3m vs 5m) Both Valid
        // Scenario: current_ts is 10:00. Previous was 08:59.
        //
        // 3m Candle: opens 09:00, closes 09:03 (available at 09:03)
        //            Since 09:03 > 08:59:00 (previous_ts), this candle IS new.
        //            Contains price 500.
        //
        // 5m Candle: opens 09:55, closes 10:00 (available at 10:00)
        //            Since 10:00 > 08:59:00 (previous_ts), this candle IS new.
        //            Contains price 200.
        //
        // Both candles have point_in_time <= current_ts, so both are visible.
        // Both candles have point_in_time > previous_ts, so both are "new".
        //
        // NOTE: Future data prevention is tested separately in
        // `test_view_contract_no_future_data_in_slice`.

        let id_3m = ohlcv_id(SpotPair::BtcUsdt, Period::Minute(3));
        let id_5m = ohlcv_id(SpotPair::BtcUsdt, Period::Minute(5));
        let symbol = Symbol::Spot(SpotPair::BtcUsdt);

        // 3m candle: [09:00:00, 09:03:00) - half-open interval, 3 minute duration
        // point_in_time = close_timestamp = 09:03:00
        // Since previous_ts = 08:59 and point_in_time = 09:03:00 > 08:59, it IS new
        // Contains price 500
        let events_3m = vec![ohlcv(
            ts("2025-03-01T09:00:00Z"),
            ts("2025-03-01T09:03:00Z"),
            100.0,
            500.0, // high = 500
            90.0,
            105.0,
        )];

        // 5m candle: [09:55:00, 10:00:00) - half-open interval, 5 minute duration
        // point_in_time = close_timestamp = 10:00:00
        // Since previous_ts = 08:59 and point_in_time = 10:00:00 > 08:59, it IS new
        // Contains price 200
        let events_5m = vec![ohlcv(
            ts("2025-03-01T09:55:00Z"),
            ts("2025-03-01T10:00:00Z"),
            100.0,
            200.0, // high = 200
            90.0,
            105.0,
        )];

        let mut view_data = SortedVecMap::new();
        view_data.insert(id_3m, events_3m.as_slice());
        view_data.insert(id_5m, events_5m.as_slice());

        let market_view = market_view_with_ohlcv(
            view_data,
            Some(ts("2025-03-01T08:59:00Z")),
            ts("2025-03-01T10:00:00Z"), // current_ts at 10:00:00 to include 5m candle
        );

        // Both candles are "new" (point_in_time > previous_ts), so both should be checked
        assert!(
            market_view.reached_price(Price(500.0), &symbol),
            "3m candle (new) contains 500, should be reached"
        );
        assert!(
            market_view.reached_price(Price(200.0), &symbol),
            "5m candle (new) contains 200, should be reached"
        );
    }

    #[test]
    fn test_reached_price_time_boundaries_exact() {
        // Test exact boundary conditions for `since_ts` comparison
        // The filter is: point_in_time() > since_ts (strictly greater)
        let id = ohlcv_id(SpotPair::EthUsdt, Period::Minute(1));
        let symbol = Symbol::Spot(SpotPair::EthUsdt);

        let events = vec![
            // Candle 1: available at EXACTLY previous_ts - should be IGNORED (not strictly greater)
            // Range: [90, 111] - contains price 111
            ohlcv(
                ts("2025-04-01T00:02:00Z"),
                ts("2025-04-01T00:03:00Z"), // point_in_time = 00:03:00
                100.0,
                111.0, // high = 111 (unique price only in this candle)
                90.0,
                105.0,
            ),
            // Candle 2: available at 1 second AFTER previous_ts - should be included
            // Range: [200, 250] - does NOT contain price 111, but contains 222
            ohlcv(
                ts("2025-04-01T00:03:00Z"),
                ts("2025-04-01T00:03:01Z"), // point_in_time = 00:03:01
                210.0,
                250.0, // high = 250
                200.0, // low = 200 (so 111 is NOT in [200, 250])
                222.0,
            ),
        ];

        let mut view_data = SortedVecMap::new();
        view_data.insert(id, events.as_slice());

        // previous_ts = 00:03:00
        // Candle 1: point_in_time (00:03:00) > previous_ts (00:03:00)? NO -> excluded
        // Candle 2: point_in_time (00:03:01) > previous_ts (00:03:00)? YES -> included
        let market_view = market_view_with_ohlcv(
            view_data,
            Some(ts("2025-04-01T00:03:00Z")),
            ts("2025-04-01T00:04:00Z"),
        );

        assert!(
            !market_view.reached_price(Price(111.0), &symbol),
            "Price 111 is in candle at EXACTLY previous_ts, should be EXCLUDED (not > since_ts)"
        );
        assert!(
            market_view.reached_price(Price(222.0), &symbol),
            "Price 222 is in candle 1 second after previous_ts, should be INCLUDED"
        );
    }

    // ==========================================================================
    // Test: try_resolved_close_price
    // Must prioritize recency of the close time, not granularity
    // ==========================================================================

    #[test]
    fn test_try_resolved_close_price_most_recent_wins() {
        // Scenario: 3m candle closes at 08:59. 5m candle closes at 09:59. Current TS is 09:59.
        // The function must return the 5m close price (09:59), ignoring the 3m candle (08:59).
        let id_3m = ohlcv_id(SpotPair::BtcUsdt, Period::Minute(3));
        let id_5m = ohlcv_id(SpotPair::BtcUsdt, Period::Minute(5));
        let symbol = Symbol::Spot(SpotPair::BtcUsdt);

        // 3m closes at 08:59 with close price 950.0
        let events_3m = vec![ohlcv(
            ts("2025-05-01T08:56:00Z"),
            ts("2025-05-01T08:59:00Z"), // close_timestamp = 08:59
            100.0,
            110.0,
            90.0,
            950.0, // close = 950
        )];

        // 5m closes at 09:59 with close price 1000.0 (more recent)
        let events_5m = vec![ohlcv(
            ts("2025-05-01T09:54:00Z"),
            ts("2025-05-01T09:59:00Z"), // close_timestamp = 09:59
            100.0,
            110.0,
            90.0,
            1000.0, // close = 1000
        )];

        let mut view_data = SortedVecMap::new();
        view_data.insert(id_3m, events_3m.as_slice());
        view_data.insert(id_5m, events_5m.as_slice());

        let market_view = market_view_with_ohlcv(
            view_data,
            Some(ts("2025-05-01T08:59:00Z")),
            ts("2025-05-01T09:59:00Z"),
        );

        let price = market_view.try_resolved_close_price(&symbol).unwrap();
        assert_eq!(
            price,
            Price(1000.0),
            "Should select 5m price (1000.0) because it closed more recently at 09:59"
        );
    }

    #[test]
    fn test_try_resolved_close_price_same_symbol_different_periods() {
        // Multiple candles for the same symbol, different periods, all visible
        // The one with the LATEST close_timestamp wins
        let id_1m = ohlcv_id(SpotPair::BtcUsdt, Period::Minute(1));
        let id_3m = ohlcv_id(SpotPair::BtcUsdt, Period::Minute(3));
        let id_5m = ohlcv_id(SpotPair::BtcUsdt, Period::Minute(5));
        let symbol = Symbol::Spot(SpotPair::BtcUsdt);

        // 1m closes at 09:57 with price 100
        let events_1m = vec![ohlcv(
            ts("2025-06-01T09:56:00Z"),
            ts("2025-06-01T09:57:00Z"),
            100.0,
            110.0,
            90.0,
            100.0,
        )];

        // 3m closes at 09:59 with price 300 (WINNER - most recent)
        let events_3m = vec![ohlcv(
            ts("2025-06-01T09:56:00Z"),
            ts("2025-06-01T09:59:00Z"),
            100.0,
            110.0,
            90.0,
            300.0,
        )];

        // 5m closes at 09:55 with price 500
        let events_5m = vec![ohlcv(
            ts("2025-06-01T09:50:00Z"),
            ts("2025-06-01T09:55:00Z"),
            100.0,
            110.0,
            90.0,
            500.0,
        )];

        let mut view_data = SortedVecMap::new();
        view_data.insert(id_1m, events_1m.as_slice());
        view_data.insert(id_3m, events_3m.as_slice());
        view_data.insert(id_5m, events_5m.as_slice());

        let market_view = market_view_with_ohlcv(
            view_data,
            Some(ts("2025-06-01T09:50:00Z")),
            ts("2025-06-01T09:59:00Z"),
        );

        let price = market_view.try_resolved_close_price(&symbol).unwrap();
        assert_eq!(
            price,
            Price(300.0),
            "3m candle closing at 09:59 is most recent, should return 300.0"
        );
    }

    #[test]
    fn test_try_resolved_close_price_no_data_error() {
        // Test error when no data available for the symbol
        let symbol = Symbol::Spot(SpotPair::SolUsdt); // No data for this symbol

        let market_view = market_view_with_ohlcv(
            SortedVecMap::new(), // empty data
            Some(ts("2025-07-01T00:00:00Z")),
            ts("2025-07-01T00:10:00Z"),
        );

        assert!(
            market_view.try_resolved_close_price(&symbol).is_err(),
            "Should error when no data available for the symbol"
        );
    }

    // ==========================================================================
    // Test: find_candle
    // ==========================================================================

    #[test]
    fn test_find_candle_exact_boundaries() {
        // Test finding candle with exact timestamp boundaries
        // Candle is [open, close) - open is inclusive, close is exclusive
        let id = ohlcv_id(SpotPair::BtcUsdt, Period::Minute(3));

        let events = vec![
            ohlcv(
                ts("2025-09-01T00:00:00Z"),
                ts("2025-09-01T00:03:00Z"),
                100.0,
                110.0,
                90.0,
                105.0,
            ),
            ohlcv(
                ts("2025-09-01T00:03:00Z"),
                ts("2025-09-01T00:06:00Z"),
                105.0,
                115.0,
                95.0,
                110.0,
            ),
            ohlcv(
                ts("2025-09-01T00:06:00Z"),
                ts("2025-09-01T00:09:00Z"),
                110.0,
                120.0,
                100.0,
                115.0,
            ),
        ];

        let mut view_data = SortedVecMap::new();
        view_data.insert(id, events.as_slice());

        let market_view = market_view_with_ohlcv(view_data, None, ts("2025-09-01T00:09:00Z"));

        // Timestamp at open should find the candle
        let candle = market_view
            .find_candle(&id, ts("2025-09-01T00:03:00Z"))
            .unwrap();
        assert_eq!(
            candle.close,
            Price(110.0),
            "Should find second candle at open time"
        );

        // Timestamp in middle of candle
        let candle = market_view
            .find_candle(&id, ts("2025-09-01T00:04:30Z"))
            .unwrap();
        assert_eq!(
            candle.close,
            Price(110.0),
            "Should find second candle in middle"
        );

        // Timestamp at close-1ns should still find the candle
        let candle = market_view
            .find_candle(&id, ts("2025-09-01T00:05:59Z"))
            .unwrap();
        assert_eq!(
            candle.close,
            Price(110.0),
            "Should find second candle just before close"
        );

        // Timestamp exactly at close should find NEXT candle (close is exclusive)
        let candle = market_view
            .find_candle(&id, ts("2025-09-01T00:06:00Z"))
            .unwrap();
        assert_eq!(
            candle.close,
            Price(115.0),
            "Close timestamp is exclusive, should find third candle"
        );
    }

    #[test]
    fn test_find_candle_no_match() {
        let id = ohlcv_id(SpotPair::BtcUsdt, Period::Minute(3));

        let events = vec![ohlcv(
            ts("2025-10-01T00:03:00Z"),
            ts("2025-10-01T00:06:00Z"),
            100.0,
            110.0,
            90.0,
            105.0,
        )];

        let mut view_data = SortedVecMap::new();
        view_data.insert(id, events.as_slice());

        let market_view = market_view_with_ohlcv(view_data, None, ts("2025-10-01T00:06:00Z"));

        // Query before any candle
        let result = market_view.find_candle(&id, ts("2025-10-01T00:00:00Z"));
        assert!(
            result.is_none(),
            "Should return None when no candle covers the timestamp"
        );
    }

    // ==========================================================================
    // Test: StreamView trait methods
    // ==========================================================================

    #[test]
    fn test_stream_view_last_event() {
        let id = ohlcv_id(SpotPair::BtcUsdt, Period::Minute(3));

        let events = vec![
            ohlcv_simple(ts("2025-11-01T00:00:00Z"), ts("2025-11-01T00:03:00Z")),
            ohlcv_simple(ts("2025-11-01T00:03:00Z"), ts("2025-11-01T00:06:00Z")),
            ohlcv(
                ts("2025-11-01T00:06:00Z"),
                ts("2025-11-01T00:09:00Z"),
                100.0,
                110.0,
                90.0,
                999.0, // unique close price
            ),
        ];

        let mut view_data = SortedVecMap::new();
        view_data.insert(id, events.as_slice());

        let market_view = market_view_with_ohlcv(view_data, None, ts("2025-11-01T00:09:00Z"));

        let last = market_view.ohlcv.last_event(&id).unwrap();
        assert_eq!(
            last.close,
            Price(999.0),
            "Should return the last event in the slice"
        );
    }

    #[test]
    fn test_stream_view_rev_iter() {
        let id = ohlcv_id(SpotPair::BtcUsdt, Period::Minute(3));

        let events = vec![
            ohlcv(
                ts("2025-12-01T00:00:00Z"),
                ts("2025-12-01T00:03:00Z"),
                100.0,
                110.0,
                90.0,
                111.0,
            ),
            ohlcv(
                ts("2025-12-01T00:03:00Z"),
                ts("2025-12-01T00:06:00Z"),
                100.0,
                110.0,
                90.0,
                222.0,
            ),
            ohlcv(
                ts("2025-12-01T00:06:00Z"),
                ts("2025-12-01T00:09:00Z"),
                100.0,
                110.0,
                90.0,
                333.0,
            ),
        ];

        let mut view_data = SortedVecMap::new();
        view_data.insert(id, events.as_slice());

        let market_view = market_view_with_ohlcv(view_data, None, ts("2025-12-01T00:09:00Z"));

        let close_prices: Vec<f64> = market_view
            .ohlcv
            .rev_iter(&id)
            .unwrap()
            .map(|e| e.close.0)
            .collect();

        assert_eq!(
            close_prices,
            vec![333.0, 222.0, 111.0],
            "rev_iter should return events in reverse order (newest first)"
        );
    }

    #[test]
    fn test_new_events_since() {
        let id = ohlcv_id(SpotPair::BtcUsdt, Period::Minute(3));

        let events = vec![
            // Old: available at 00:03
            ohlcv(
                ts("2026-01-01T00:00:00Z"),
                ts("2026-01-01T00:03:00Z"),
                100.0,
                110.0,
                90.0,
                111.0,
            ),
            // New: available at 00:06
            ohlcv(
                ts("2026-01-01T00:03:00Z"),
                ts("2026-01-01T00:06:00Z"),
                100.0,
                110.0,
                90.0,
                222.0,
            ),
            // New: available at 00:09
            ohlcv(
                ts("2026-01-01T00:06:00Z"),
                ts("2026-01-01T00:09:00Z"),
                100.0,
                110.0,
                90.0,
                333.0,
            ),
        ];

        let mut view_data = SortedVecMap::new();
        view_data.insert(id, events.as_slice());

        let market_view = market_view_with_ohlcv(view_data, None, ts("2026-01-01T00:09:00Z"));

        // Query events since 00:03:00 (should exclude the first candle)
        let new_events: Vec<f64> = market_view
            .ohlcv
            .new_events_since(&id, ts("2026-01-01T00:03:00Z"))
            .unwrap()
            .map(|e| e.close.0)
            .collect();

        assert_eq!(
            new_events,
            vec![333.0, 222.0],
            "new_events_since should return only events with point_in_time > since_ts, in reverse"
        );
    }
}
