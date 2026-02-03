use std::{fmt::Debug, ops::Range};

use chrono::{DateTime, Utc};

use crate::{
    data::event::{
        EconomicCalendarId, EmaId, MarketEvent, OhlcvId, RsiId, SmaId, StreamId, TpoId, TradesId,
        VolumeProfileId,
    },
    sim::data::EventMap,
    sorted_vec_map::SortedVecMap,
};

/// Binds a StreamId to its physical storage container in the simulation.
///
/// This "Type Family" pattern allows generic components (like Cursors) to know
/// exactly what data structure they operate on without explicit generic parameters.
pub trait StreamEntity: StreamId {
    /// The physical storage container for this stream's history.
    type Storage;
}

impl StreamEntity for OhlcvId {
    type Storage = EventMap<Self>;
}

impl StreamEntity for TradesId {
    type Storage = EventMap<Self>;
}

impl StreamEntity for EconomicCalendarId {
    type Storage = EventMap<Self>;
}

impl StreamEntity for VolumeProfileId {
    type Storage = EventMap<Self>;
}

impl StreamEntity for TpoId {
    type Storage = EventMap<Self>;
}

impl StreamEntity for EmaId {
    type Storage = EventMap<Self>;
}

impl StreamEntity for SmaId {
    type Storage = EventMap<Self>;
}

impl StreamEntity for RsiId {
    type Storage = EventMap<Self>;
}

/// Defines how a cursor interacts with its specific storage.
pub trait StreamCursor {
    /// The specific data structure this cursor knows how to navigate.
    type Storage;

    /// Initialize a cursor at index 0 for all streams found in storage.
    fn new(data: &Self::Storage) -> Self;

    /// Moves the cursor forward to `ts`.
    /// Handles both small steps (t -> t+1) and large jumps (Episode -> Episode) efficiently.
    fn advance(&mut self, data: &Self::Storage, ts: DateTime<Utc>);

    /// Resets all indices to 0.
    fn rewind(&mut self);

    /// Moves all indices to the end (exhausted).
    fn to_end(&mut self, data: &Self::Storage);

    /// Scans future events (starting from current cursor position)
    /// to find the first event that OPENS at or after `ts`.
    /// Use this to align the environment start time to the beginning of an
    /// episode.
    fn find_first_open_at_or_after(
        &self,
        data: &Self::Storage,
        ts: DateTime<Utc>,
    ) -> Option<DateTime<Utc>>;

    /// Scans future events (starting from current cursor position)
    /// to find the first event that becomes AVAILABLE at or after `ts`.
    /// Use this to determine the exact timestamp when the environment loop should resume.
    fn find_first_availability_at_or_after(
        &self,
        data: &Self::Storage,
        ts: DateTime<Utc>,
    ) -> Option<DateTime<Utc>>;

    fn is_done(&self, data: &Self::Storage) -> bool;
    fn next_availability(&self, data: &Self::Storage) -> Option<DateTime<Utc>>;
}
#[derive(Debug, Clone)]
pub struct Cursor<S: StreamId>(pub SortedVecMap<S, Range<usize>>);

impl<S: StreamId> Default for Cursor<S> {
    fn default() -> Self {
        Self(SortedVecMap::new())
    }
}

pub type OhlcvCursor = Cursor<OhlcvId>;
pub type TradeCursor = Cursor<TradesId>;
pub type EconomicCalendarCursor = Cursor<EconomicCalendarId>;
pub type VolumeProfileCursor = Cursor<VolumeProfileId>;
pub type TpoCursor = Cursor<TpoId>;
pub type EmaCursor = Cursor<EmaId>;
pub type SmaCursor = Cursor<SmaId>;
pub type RsiCursor = Cursor<RsiId>;

impl<S> StreamCursor for Cursor<S>
where
    S: StreamEntity<Storage = EventMap<S>>,
{
    type Storage = S::Storage;

    fn new(data: &EventMap<S>) -> Self {
        Self(data.iter().map(|(id, _)| (*id, 0..0)).collect())
    }

    fn advance(&mut self, data: &EventMap<S>, ts: DateTime<Utc>) {
        // OPTIMIZATION: Lockstep Iteration (Zip).
        // Since Cursor is created from Data, and Data is structurally immutable during simulation,
        // the memory layout of keys is identical.
        // We zip the iterators to access the event vector in O(1) relative to the cursor.
        //
        // Prerequisite: `Cursor` and `EventMap` must have the exact same keys in the same order.
        // This is enforced by <S as StreamCursor>::new()
        self.0
            .iter_mut()
            .zip(data.iter())
            .for_each(|((cursor_id, range), (data_id, events))| {
                // SAFETY: Low-cost assertion in debug builds to ensure our invariant holds.
                // Since StreamId is copy, this check is essentially free.
                debug_assert_eq!(cursor_id, data_id, "Cursor desynchronized from Storage!");

                let future_events = &events[range.end..];

                // OPTIMIZATION: Quick check to avoid `position` call overhead if we are already up to date.
                // This handles the "waiting" case.
                if future_events
                    .first()
                    .is_some_and(|e| e.point_in_time() <= ts)
                {
                    let advance_by = future_events
                        .iter()
                        .position(|e| e.point_in_time() > ts)
                        .unwrap_or(future_events.len());

                    range.end += advance_by;
                }
            })
    }

    fn rewind(&mut self) {
        self.0.iter_mut().for_each(|(_, range)| range.end = 0);
    }

    fn to_end(&mut self, data: &EventMap<S>) {
        self.0
            .iter_mut()
            .zip(data.iter())
            .for_each(|((cursor_id, range), (data_id, events))| {
                debug_assert_eq!(cursor_id, data_id, "Cursor desynchronized from Storage!");
                range.end = events.len();
            });
    }

    fn find_first_open_at_or_after(
        &self,
        data: &EventMap<S>,
        ts: DateTime<Utc>,
    ) -> Option<DateTime<Utc>> {
        self.0
            .iter()
            .zip(data.iter())
            .filter_map(|((cursor_id, range), (data_id, events))| {
                debug_assert_eq!(cursor_id, data_id, "Cursor desynchronized from Storage!");
                events[range.end..]
                    .iter()
                    .find(|e| e.opened_at() >= ts)
                    .map(|e| e.opened_at())
            })
            .min()
    }

    fn find_first_availability_at_or_after(
        &self,
        data: &EventMap<S>,
        ts: DateTime<Utc>,
    ) -> Option<DateTime<Utc>> {
        self.0
            .iter()
            .zip(data.iter())
            .filter_map(|((cursor_id, range), (data_id, events))| {
                debug_assert_eq!(cursor_id, data_id, "Cursor desynchronized from Storage!");

                events[range.end..]
                    .iter()
                    .find(|e| e.point_in_time() >= ts)
                    .map(|e| e.point_in_time())
            })
            .min()
    }

    fn next_availability(&self, data: &EventMap<S>) -> Option<DateTime<Utc>> {
        self.0
            .iter()
            .zip(data.iter())
            .filter_map(|((cursor_id, range), (data_id, events))| {
                debug_assert_eq!(cursor_id, data_id, "Cursor desynchronized from Storage!");
                events.get(range.end).map(|e| e.point_in_time())
            })
            .min()
    }

    fn is_done(&self, data: &EventMap<S>) -> bool {
        self.0
            .iter()
            .zip(data.iter())
            .all(|((cursor_id, range), (data_id, events))| {
                debug_assert_eq!(cursor_id, data_id, "Cursor desynchronized from Storage!");
                range.end >= events.len()
            })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::data::{
        domain::{
            CountryCode, DataBroker, EconomicEventImpact, EconomicValue, Exchange, Period, Price,
            Quantity, SpotPair, Symbol,
        },
        event::{EconomicCalendarId, EconomicEvent, Ohlcv, OhlcvId, Trade, TradesId},
    };

    // ============================================================================
    // Test Helpers
    // ============================================================================

    /// Parse RFC3339 timestamp string to DateTime<Utc>.
    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    /// Create an OHLCV event with specified open and close timestamps.
    /// The `point_in_time` is determined by `close_timestamp` per the MarketEvent trait.
    fn ohlcv(open_ts: DateTime<Utc>, close_ts: DateTime<Utc>) -> Ohlcv {
        Ohlcv {
            open_timestamp: open_ts,
            close_timestamp: close_ts,
            open: Price(100.0),
            high: Price(110.0),
            low: Price(90.0),
            close: Price(105.0),
            volume: Quantity(1000.0),
            quote_asset_volume: None,
            number_of_trades: None,
            taker_buy_base_asset_volume: None,
            taker_buy_quote_asset_volume: None,
        }
    }

    /// Create an OHLCV ID for testing.
    fn ohlcv_id(period: Period) -> OhlcvId {
        OhlcvId {
            broker: DataBroker::Binance,
            exchange: Exchange::Binance,
            symbol: Symbol::Spot(SpotPair::BtcUsdt),
            period,
        }
    }

    /// Create a Trade event at the specified timestamp.
    fn trade(timestamp: DateTime<Utc>) -> Trade {
        Trade {
            timestamp,
            price: Price(100.0),
            quantity: Quantity(1.0),
            trade_id: None,
            quote_asset_volume: None,
            is_buyer_maker: None,
            is_best_match: None,
        }
    }

    /// Create a TradeId for testing.
    fn trade_id() -> TradesId {
        TradesId {
            broker: DataBroker::Binance,
            exchange: Exchange::Binance,
            symbol: Symbol::Spot(SpotPair::BtcUsdt),
        }
    }

    /// Create an EconomicEvent (news release) at the specified timestamp.
    fn economic_event(timestamp: DateTime<Utc>, name: &str) -> EconomicEvent {
        EconomicEvent {
            timestamp,
            data_source: "investingcom".to_string(),
            category: "Inflation".to_string(),
            news_name: name.to_string(),
            country_code: CountryCode::Us,
            currency_code: "USD".to_string(),
            economic_impact: EconomicEventImpact::High,
            news_type: Some("CPI".to_string()),
            news_type_confidence: Some(0.95),
            news_type_source: Some("manual".to_string()),
            period: Some("yoy".to_string()),
            actual: Some(EconomicValue(2.5)),
            forecast: Some(EconomicValue(2.3)),
            previous: Some(EconomicValue(2.4)),
        }
    }

    /// Create an EconomicCalendarId for testing.
    fn econ_id() -> EconomicCalendarId {
        EconomicCalendarId {
            broker: DataBroker::InvestingCom,
            data_source: None,
            country_code: Some(CountryCode::Us),
            category: None,
            importance: None,
        }
    }

    // ============================================================================
    // Part 1: Cursor Tests (The Engine)
    // Focus: Slicing mechanics and time advancement
    // ============================================================================

    #[test]
    fn test_cursor_new_initializes_at_zero() {
        // Cursor should start with empty ranges (0..0)
        let id = ohlcv_id(Period::Minute(3));
        let events = vec![
            // Candle: opens 2025-01-01 00:00, closes 00:03 (available at 00:03)
            ohlcv(ts("2025-01-01T00:00:00Z"), ts("2025-01-01T00:03:00Z")),
            ohlcv(ts("2025-01-01T00:03:00Z"), ts("2025-01-01T00:06:00Z")),
        ];

        let mut data = SortedVecMap::new();
        data.insert(id, events.into_boxed_slice());

        let cursor = OhlcvCursor::new(&data);

        assert_eq!(
            cursor.0.get(&id).unwrap(),
            &(0..0),
            "Cursor should initialize with empty range"
        );
    }

    #[test]
    fn test_cursor_simultaneous_events_news_case() {
        // THE CRITICAL TEST: "CPI YoY", "CPI MoM", "CPI QoQ" all released at ts=100
        // Fourth event at ts=101 should NOT be included when advancing to ts=100.
        //
        // The advance logic uses `position(|e| e.point_in_time() > ts)` which is STRICTLY GREATER.
        // This ensures ALL events at exactly ts=100 are consumed.
        let id = econ_id();

        // All three news items released simultaneously at 08:30:00
        let events = vec![
            economic_event(ts("2025-06-15T08:30:00Z"), "CPI YoY"),
            economic_event(ts("2025-06-15T08:30:00Z"), "CPI MoM"),
            economic_event(ts("2025-06-15T08:30:00Z"), "CPI QoQ"),
            // Fourth event at 08:31:00 - should NOT be consumed
            economic_event(ts("2025-06-15T08:31:00Z"), "Core CPI"),
        ];

        let mut data = SortedVecMap::new();
        data.insert(id, events.into_boxed_slice());

        let mut cursor = EconomicCalendarCursor::new(&data);

        // Advance to exactly 08:30:00
        cursor.advance(&data, ts("2025-06-15T08:30:00Z"));

        assert_eq!(
            cursor.0.get(&id).unwrap(),
            &(0..3),
            "All three simultaneous news events at 08:30:00 should be visible, \
             but NOT the event at 08:31:00. Logic: position(|e| e.point_in_time() > ts)"
        );
    }

    #[test]
    fn test_cursor_simultaneous_events_ohlcv_case() {
        // Multiple OHLCV events with the same close_timestamp (point_in_time)
        // This simulates different candle periods (1m, 3m, 5m) all closing at the same
        // wall-clock time, which is a realistic scenario.
        //
        // HALF-OPEN INTERVAL RULE: [open, close) where open < close
        // - 1m candle: [00:02:00, 00:03:00) - 1 minute duration
        // - 3m candle: [00:00:00, 00:03:00) - 3 minute duration
        // - Another 1m candle: [00:02:00, 00:03:00) - duplicate (realistic scenario)
        //
        // All three close at exactly 00:03:00, so all should be consumed at ts=00:03:00.

        let id = ohlcv_id(Period::Minute(1)); // Using same id for simplicity in test

        // Three candles all closing at exactly 00:03:00, with VALID durations
        let events = vec![
            // 1-minute candle: [00:02, 00:03)
            ohlcv(ts("2025-03-01T00:02:00Z"), ts("2025-03-01T00:03:00Z")),
            // 3-minute candle: [00:00, 00:03) - shares same close_timestamp
            ohlcv(ts("2025-03-01T00:00:00Z"), ts("2025-03-01T00:03:00Z")),
            // Another 1-minute candle at same close (e.g., duplicate or different source)
            ohlcv(ts("2025-03-01T00:02:00Z"), ts("2025-03-01T00:03:00Z")),
            // This one closes at 00:04:00 - should NOT be consumed when advancing to 00:03
            ohlcv(ts("2025-03-01T00:03:00Z"), ts("2025-03-01T00:04:00Z")),
        ];

        let mut data = SortedVecMap::new();
        data.insert(id, events.into_boxed_slice());

        let mut cursor = OhlcvCursor::new(&data);

        cursor.advance(&data, ts("2025-03-01T00:03:00Z"));

        assert_eq!(
            cursor.0.get(&id).unwrap(),
            &(0..3),
            "Should consume ALL THREE events at 00:03:00, but NOT the one at 00:04:00"
        );
    }

    #[test]
    fn test_cursor_strict_forward_only_slice_grows() {
        // As time advances, the slice grows but NEVER shrinks
        let id = ohlcv_id(Period::Minute(3));
        let events = vec![
            ohlcv(ts("2025-02-01T00:00:00Z"), ts("2025-02-01T00:03:00Z")),
            ohlcv(ts("2025-02-01T00:03:00Z"), ts("2025-02-01T00:06:00Z")),
            ohlcv(ts("2025-02-01T00:06:00Z"), ts("2025-02-01T00:09:00Z")),
            ohlcv(ts("2025-02-01T00:09:00Z"), ts("2025-02-01T00:12:00Z")),
        ];

        let mut data = SortedVecMap::new();
        data.insert(id, events.into_boxed_slice());

        let mut cursor = OhlcvCursor::new(&data);

        // Step 1: Advance to 00:03:00
        cursor.advance(&data, ts("2025-02-01T00:03:00Z"));
        assert_eq!(
            cursor.0.get(&id).unwrap(),
            &(0..1),
            "Step 1: 1 event visible"
        );

        // Step 2: Advance to 00:06:00 - slice GROWS
        cursor.advance(&data, ts("2025-02-01T00:06:00Z"));
        assert_eq!(
            cursor.0.get(&id).unwrap(),
            &(0..2),
            "Step 2: slice grew to 2 events"
        );

        // Step 3: Advance to 00:12:00 - large jump, slice GROWS
        cursor.advance(&data, ts("2025-02-01T00:12:00Z"));
        assert_eq!(
            cursor.0.get(&id).unwrap(),
            &(0..4),
            "Step 3: slice grew to all 4 events"
        );

        // Step 4: Advance to an earlier time - slice should NOT shrink
        cursor.advance(&data, ts("2025-02-01T00:06:00Z"));
        assert_eq!(
            cursor.0.get(&id).unwrap(),
            &(0..4),
            "Step 4: slice MUST NOT shrink on earlier timestamp"
        );
    }

    #[test]
    fn test_cursor_advance_before_any_events() {
        // Advancing to a time before any events are available should leave cursor at 0..0
        let id = ohlcv_id(Period::Minute(3));
        let events = vec![
            // First event available at 00:03:00
            ohlcv(ts("2025-04-01T00:00:00Z"), ts("2025-04-01T00:03:00Z")),
        ];

        let mut data = SortedVecMap::new();
        data.insert(id, events.into_boxed_slice());

        let mut cursor = OhlcvCursor::new(&data);

        // Advance to 00:02:00 - before the first event is available
        cursor.advance(&data, ts("2025-04-01T00:02:00Z"));
        assert_eq!(
            cursor.0.get(&id).unwrap(),
            &(0..0),
            "Should not consume events before they're available"
        );
    }

    #[test]
    fn test_cursor_advance_exactly_at_availability() {
        // Advancing to the exact availability timestamp should include that event
        let id = ohlcv_id(Period::Minute(5));
        let events = vec![ohlcv(
            ts("2025-05-01T00:00:00Z"),
            ts("2025-05-01T00:05:00Z"),
        )];

        let mut data = SortedVecMap::new();
        data.insert(id, events.into_boxed_slice());

        let mut cursor = OhlcvCursor::new(&data);

        // Advance to exactly 00:05:00 - matches point_in_time()
        cursor.advance(&data, ts("2025-05-01T00:05:00Z"));
        assert_eq!(
            cursor.0.get(&id).unwrap(),
            &(0..1),
            "Event should be consumed when ts == point_in_time()"
        );
    }

    #[test]
    fn test_cursor_large_jump_consumes_all_intermediate() {
        // Test episode-to-episode jumps: advancing far ahead should consume all intermediate events
        let id = ohlcv_id(Period::Minute(3));
        let events = vec![
            ohlcv(ts("2025-07-01T00:00:00Z"), ts("2025-07-01T00:03:00Z")),
            ohlcv(ts("2025-07-01T00:03:00Z"), ts("2025-07-01T00:06:00Z")),
            ohlcv(ts("2025-07-01T00:06:00Z"), ts("2025-07-01T00:09:00Z")),
            ohlcv(ts("2025-07-01T00:09:00Z"), ts("2025-07-01T00:12:00Z")),
            ohlcv(ts("2025-07-01T00:12:00Z"), ts("2025-07-01T00:15:00Z")),
        ];

        let mut data = SortedVecMap::new();
        data.insert(id, events.into_boxed_slice());

        let mut cursor = OhlcvCursor::new(&data);

        // Large jump: skip from start to 00:12:00
        cursor.advance(&data, ts("2025-07-01T00:12:00Z"));

        assert_eq!(
            cursor.0.get(&id).unwrap(),
            &(0..4),
            "Should consume all events up to and including timestamp"
        );
    }

    #[test]
    fn test_cursor_multiple_streams_different_availability() {
        // Two streams with different periods have different availability times
        let id_3m = OhlcvId {
            broker: DataBroker::Binance,
            exchange: Exchange::Binance,
            symbol: Symbol::Spot(SpotPair::BtcUsdt),
            period: Period::Minute(3),
        };
        let id_5m = OhlcvId {
            broker: DataBroker::Binance,
            exchange: Exchange::Binance,
            symbol: Symbol::Spot(SpotPair::BtcUsdt),
            period: Period::Minute(5),
        };

        // 3m candles: available at 00:03, 00:06
        let events_3m = vec![
            ohlcv(ts("2025-08-01T00:00:00Z"), ts("2025-08-01T00:03:00Z")),
            ohlcv(ts("2025-08-01T00:03:00Z"), ts("2025-08-01T00:06:00Z")),
        ];
        // 5m candles: available at 00:05, 00:10
        let events_5m = vec![
            ohlcv(ts("2025-08-01T00:00:00Z"), ts("2025-08-01T00:05:00Z")),
            ohlcv(ts("2025-08-01T00:05:00Z"), ts("2025-08-01T00:10:00Z")),
        ];

        let mut data = SortedVecMap::new();
        data.insert(id_3m, events_3m.into_boxed_slice());
        data.insert(id_5m, events_5m.into_boxed_slice());

        let mut cursor = OhlcvCursor::new(&data);

        // Advance to 00:05:00
        cursor.advance(&data, ts("2025-08-01T00:05:00Z"));

        assert_eq!(
            cursor.0.get(&id_3m).unwrap(),
            &(0..1),
            "3m stream: only event at 00:03 is consumed (00:06 > 00:05)"
        );
        assert_eq!(
            cursor.0.get(&id_5m).unwrap(),
            &(0..1),
            "5m stream: only event at 00:05 is consumed (00:10 > 00:05)"
        );
    }

    #[test]
    fn test_cursor_is_done() {
        let id = ohlcv_id(Period::Minute(3));
        let events = vec![ohlcv(
            ts("2025-09-01T00:00:00Z"),
            ts("2025-09-01T00:03:00Z"),
        )];

        let mut data = SortedVecMap::new();
        data.insert(id, events.into_boxed_slice());

        let mut cursor = OhlcvCursor::new(&data);

        assert!(!cursor.is_done(&data), "Should not be done at start");

        cursor.advance(&data, ts("2025-09-01T00:03:00Z"));
        assert!(
            cursor.is_done(&data),
            "Should be done after consuming all events"
        );
    }

    #[test]
    fn test_cursor_rewind() {
        let id = ohlcv_id(Period::Minute(3));
        let events = vec![
            ohlcv(ts("2025-10-01T00:00:00Z"), ts("2025-10-01T00:03:00Z")),
            ohlcv(ts("2025-10-01T00:03:00Z"), ts("2025-10-01T00:06:00Z")),
        ];

        let mut data = SortedVecMap::new();
        data.insert(id, events.into_boxed_slice());

        let mut cursor = OhlcvCursor::new(&data);

        cursor.advance(&data, ts("2025-10-01T00:06:00Z"));
        assert_eq!(cursor.0.get(&id).unwrap().end, 2);

        cursor.rewind();
        assert_eq!(
            cursor.0.get(&id).unwrap(),
            &(0..0),
            "Rewind should reset to zero"
        );
    }

    #[test]
    fn test_cursor_to_end() {
        let id = ohlcv_id(Period::Minute(3));
        let events = vec![
            ohlcv(ts("2025-11-01T00:00:00Z"), ts("2025-11-01T00:03:00Z")),
            ohlcv(ts("2025-11-01T00:03:00Z"), ts("2025-11-01T00:06:00Z")),
            ohlcv(ts("2025-11-01T00:06:00Z"), ts("2025-11-01T00:09:00Z")),
        ];

        let mut data = SortedVecMap::new();
        data.insert(id, events.into_boxed_slice());

        let mut cursor = OhlcvCursor::new(&data);

        cursor.to_end(&data);
        assert_eq!(
            cursor.0.get(&id).unwrap(),
            &(0..3),
            "to_end should move cursor to include all events"
        );
    }

    #[test]
    fn test_cursor_next_availability() {
        let id = ohlcv_id(Period::Minute(3));
        let events = vec![
            ohlcv(ts("2025-12-01T00:00:00Z"), ts("2025-12-01T00:03:00Z")),
            ohlcv(ts("2025-12-01T00:03:00Z"), ts("2025-12-01T00:06:00Z")),
        ];

        let mut data = SortedVecMap::new();
        data.insert(id, events.into_boxed_slice());

        let cursor = OhlcvCursor::new(&data);

        assert_eq!(
            cursor.next_availability(&data),
            Some(ts("2025-12-01T00:03:00Z")),
            "Should return the first unconsumed event's availability"
        );
    }

    #[test]
    fn test_cursor_find_first_open_at_or_after() {
        let id = ohlcv_id(Period::Minute(3));
        let events = vec![
            ohlcv(ts("2026-01-01T00:00:00Z"), ts("2026-01-01T00:03:00Z")),
            ohlcv(ts("2026-01-01T00:03:00Z"), ts("2026-01-01T00:06:00Z")),
            ohlcv(ts("2026-01-01T00:06:00Z"), ts("2026-01-01T00:09:00Z")),
        ];

        let mut data = SortedVecMap::new();
        data.insert(id, events.into_boxed_slice());

        let cursor = OhlcvCursor::new(&data);

        // Find first candle opening at or after 00:02:00
        let result = cursor.find_first_open_at_or_after(&data, ts("2026-01-01T00:02:00Z"));
        assert_eq!(
            result,
            Some(ts("2026-01-01T00:03:00Z")),
            "Should find candle opening at 00:03:00"
        );

        // Find first candle opening at exact timestamp
        let result = cursor.find_first_open_at_or_after(&data, ts("2026-01-01T00:03:00Z"));
        assert_eq!(
            result,
            Some(ts("2026-01-01T00:03:00Z")),
            "Should find candle opening exactly at requested time"
        );
    }

    #[test]
    fn test_cursor_find_first_availability_at_or_after() {
        let id = ohlcv_id(Period::Minute(3));
        let events = vec![
            ohlcv(ts("2026-01-05T00:00:00Z"), ts("2026-01-05T00:03:00Z")),
            ohlcv(ts("2026-01-05T00:03:00Z"), ts("2026-01-05T00:06:00Z")),
            ohlcv(ts("2026-01-05T00:06:00Z"), ts("2026-01-05T00:09:00Z")),
        ];

        let mut data = SortedVecMap::new();
        data.insert(id, events.into_boxed_slice());

        let cursor = OhlcvCursor::new(&data);

        // Query 00:05:00 - should return 00:06:00
        let result = cursor.find_first_availability_at_or_after(&data, ts("2026-01-05T00:05:00Z"));
        assert_eq!(
            result,
            Some(ts("2026-01-05T00:06:00Z")),
            "Should find next available event after the query time"
        );
    }

    #[test]
    fn test_cursor_lockstep_invariant() {
        // Verify cursor and data maintain synchronized key order
        let id1 = OhlcvId {
            broker: DataBroker::Binance,
            exchange: Exchange::Binance,
            symbol: Symbol::Spot(SpotPair::BtcUsdt),
            period: Period::Minute(3),
        };
        let id2 = OhlcvId {
            broker: DataBroker::Binance,
            exchange: Exchange::Binance,
            symbol: Symbol::Spot(SpotPair::EthUsdt),
            period: Period::Minute(3),
        };

        let events1 = vec![ohlcv(
            ts("2026-01-10T00:00:00Z"),
            ts("2026-01-10T00:03:00Z"),
        )];
        let events2 = vec![ohlcv(
            ts("2026-01-10T00:00:00Z"),
            ts("2026-01-10T00:03:00Z"),
        )];

        let mut data = SortedVecMap::new();
        data.insert(id1, events1.into_boxed_slice());
        data.insert(id2, events2.into_boxed_slice());

        let mut cursor = OhlcvCursor::new(&data);

        assert_eq!(
            cursor.0.len(),
            data.len(),
            "Cursor should have same number of keys as data"
        );

        cursor.advance(&data, ts("2026-01-10T00:03:00Z"));

        assert_eq!(cursor.0.get(&id1).unwrap(), &(0..1));
        assert_eq!(cursor.0.get(&id2).unwrap(), &(0..1));
    }

    #[test]
    fn test_cursor_lockstep_invariant_mixed_types() {
        // Verify cursor and data maintain synchronized key order across DIFFERENT stream types.
        // This ensures the generic implementation of Cursor works for any StreamId.

        // 1. Setup OHLCV Stream
        let ohlcv_id = OhlcvId {
            broker: DataBroker::Binance,
            exchange: Exchange::Binance,
            symbol: Symbol::Spot(SpotPair::BtcUsdt),
            period: Period::Minute(3),
        };
        let ohlcv_events = vec![ohlcv(
            ts("2026-01-10T00:00:00Z"),
            ts("2026-01-10T00:03:00Z"),
        )];

        // 2. Setup Trade Stream (using previously unused helper)
        let trade_id = trade_id();
        let trade_events = vec![trade(ts("2026-01-10T00:02:00Z"))];

        // 3. Insert into their respective maps
        let mut ohlcv_map = SortedVecMap::new();
        ohlcv_map.insert(ohlcv_id, ohlcv_events.into_boxed_slice());

        let mut trade_map = SortedVecMap::new();
        trade_map.insert(trade_id, trade_events.into_boxed_slice());

        // 4. Initialize Cursors
        let mut ohlcv_cursor = OhlcvCursor::new(&ohlcv_map);
        let mut trade_cursor = TradeCursor::new(&trade_map);

        // 5. Verify Structure
        assert_eq!(ohlcv_cursor.0.len(), ohlcv_map.len());
        assert_eq!(trade_cursor.0.len(), trade_map.len());

        // 6. Advance both (Trade at 00:02, Candle at 00:03)
        // Advance Trade to 00:02
        trade_cursor.advance(&trade_map, ts("2026-01-10T00:02:00Z"));
        assert_eq!(trade_cursor.0.get(&trade_id).unwrap(), &(0..1));

        // Advance OHLCV to 00:03
        ohlcv_cursor.advance(&ohlcv_map, ts("2026-01-10T00:03:00Z"));
        assert_eq!(ohlcv_cursor.0.get(&ohlcv_id).unwrap(), &(0..1));
    }
}
