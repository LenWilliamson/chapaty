use std::fmt::Debug;

use chrono::{DateTime, Utc};

use crate::{
    data::episode::Episode,
    error::{ChapatyResult, DataError},
    sim::{
        cursor::{
            EconomicCalendarCursor, EmaCursor, OhlcvCursor, RsiCursor, SmaCursor, StreamCursor,
            TpoCursor, TradeCursor, VolumeProfileCursor,
        },
        data::SimulationData,
    },
};

#[derive(Debug, Clone)]
pub struct CursorGroup {
    ohlcv: OhlcvCursor,
    trade: TradeCursor,
    economic_cal: EconomicCalendarCursor,
    vp: VolumeProfileCursor,
    tpo: TpoCursor,
    ema: EmaCursor,
    sma: SmaCursor,
    rsi: RsiCursor,

    // === Time State ===
    previous_ts: Option<DateTime<Utc>>,
    current_ts: DateTime<Utc>,
}

impl CursorGroup {
    pub fn new(sim_data: &SimulationData) -> ChapatyResult<Self> {
        let start_ts = sim_data.global_availability_start();

        let mut group = Self {
            ohlcv: OhlcvCursor::new(sim_data.ohlcv()),
            trade: TradeCursor::new(sim_data.trade()),
            economic_cal: EconomicCalendarCursor::new(sim_data.economic_cal()),
            vp: VolumeProfileCursor::new(sim_data.volume_profile()),
            tpo: TpoCursor::new(sim_data.tpo()),
            ema: EmaCursor::new(sim_data.ema()),
            sma: SmaCursor::new(sim_data.sma()),
            rsi: RsiCursor::new(sim_data.rsi()),
            previous_ts: None,
            current_ts: start_ts,
        };
        group.advance_all(sim_data, start_ts);
        Ok(group)
    }

    pub fn current_ts(&self) -> DateTime<Utc> {
        self.current_ts
    }

    pub fn previous_ts(&self) -> Option<DateTime<Utc>> {
        self.previous_ts
    }

    pub fn ohlcv(&self) -> &OhlcvCursor {
        &self.ohlcv
    }

    pub fn trade(&self) -> &TradeCursor {
        &self.trade
    }

    pub fn economic_cal(&self) -> &EconomicCalendarCursor {
        &self.economic_cal
    }

    pub fn vp(&self) -> &VolumeProfileCursor {
        &self.vp
    }

    pub fn tpo(&self) -> &TpoCursor {
        &self.tpo
    }

    pub fn ema(&self) -> &EmaCursor {
        &self.ema
    }

    pub fn sma(&self) -> &SmaCursor {
        &self.sma
    }

    pub fn rsi(&self) -> &RsiCursor {
        &self.rsi
    }

    /// Advances the cursor to the next chronological available event in the simulation data.
    ///
    /// # Idempotency
    ///
    /// This method is idempotent when called at the end of available data or at an episode boundary:
    /// - When there are no more events, calling this method multiple times has no effect.
    /// - When at the episode's end, further calls will not advance beyond the episode boundary.
    pub fn step(&mut self, sim_data: &SimulationData, ep: &Episode) -> ChapatyResult<()> {
        let next_ts = [
            self.ohlcv.next_availability(sim_data.ohlcv()),
            self.trade.next_availability(sim_data.trade()),
            self.economic_cal.next_availability(sim_data.economic_cal()),
            self.vp.next_availability(sim_data.volume_profile()),
            self.tpo.next_availability(sim_data.tpo()),
            self.ema.next_availability(sim_data.ema()),
            self.sma.next_availability(sim_data.sma()),
            self.rsi.next_availability(sim_data.rsi()),
        ]
        .into_iter()
        .flatten()
        .min();

        let Some(mut next_ts) = next_ts else {
            return Ok(());
        };

        // Don't step past episode boundary
        next_ts = next_ts.min(ep.end());

        if next_ts <= self.current_ts {
            return Ok(());
        }

        self.previous_ts = Some(self.current_ts);
        self.current_ts = next_ts;
        self.advance_all(sim_data, next_ts);
        Ok(())
    }

    /// Resets the cursor to the beginning of the next chronological episode.
    ///
    /// This function correctly handles sparse data by finding the first available
    /// event at or after the theoretical start of the next episode.
    pub fn advance_to_next_episode(
        &mut self,
        sim_data: &SimulationData,
        ep: Episode,
    ) -> ChapatyResult<Option<Episode>> {
        let current_ep_end = ep.end();

        // 1. Try to find the next episode start
        let next_start = [
            self.ohlcv
                .find_first_open_at_or_after(sim_data.ohlcv(), current_ep_end),
            self.trade
                .find_first_open_at_or_after(sim_data.trade(), current_ep_end),
            self.economic_cal
                .find_first_open_at_or_after(sim_data.economic_cal(), current_ep_end),
            self.vp
                .find_first_open_at_or_after(sim_data.volume_profile(), current_ep_end),
            self.tpo
                .find_first_open_at_or_after(sim_data.tpo(), current_ep_end),
            self.ema
                .find_first_open_at_or_after(sim_data.ema(), current_ep_end),
            self.sma
                .find_first_open_at_or_after(sim_data.sma(), current_ep_end),
            self.rsi
                .find_first_open_at_or_after(sim_data.rsi(), current_ep_end),
        ]
        .into_iter()
        .flatten()
        .min();

        let Some(next_start) = next_start else {
            // If no future episode exists, explicitly mark all streams as exhausted.
            self.advance_all_to_end(sim_data);
            return Ok(None);
        };

        // 2. Find data availability for that start time
        let start_availability_candidate = [
            self.ohlcv
                .find_first_availability_at_or_after(sim_data.ohlcv(), next_start),
            self.trade
                .find_first_availability_at_or_after(sim_data.trade(), next_start),
            self.economic_cal
                .find_first_availability_at_or_after(sim_data.economic_cal(), next_start),
            self.vp
                .find_first_availability_at_or_after(sim_data.volume_profile(), next_start),
            self.tpo
                .find_first_availability_at_or_after(sim_data.tpo(), next_start),
            self.ema
                .find_first_availability_at_or_after(sim_data.ema(), next_start),
            self.sma
                .find_first_availability_at_or_after(sim_data.sma(), next_start),
            self.rsi
                .find_first_availability_at_or_after(sim_data.rsi(), next_start),
        ]
        .into_iter()
        .flatten()
        .min();

        // LOGICAL ASSERTION:
        // If an event opened at `next_start`, it MUST be available at >= `next_start`.
        // If this unwrap fails, the SimulationData is corrupt or the Cursor logic is broken.
        let start_availability =
            start_availability_candidate.ok_or_else(|| DataError::CausalityViolation {
                open: next_start.to_string(),
                stream: "Unknown (Aggregation)".to_string(),
            })?;

        // 3. Update State
        let next_ep = ep.next(next_start);
        self.previous_ts = None;
        self.current_ts = start_availability;

        // 4. Advances all cursors to the new start time (Preserving history 0..start)
        self.advance_all(sim_data, self.current_ts);

        Ok(Some(next_ep))
    }

    /// Resets the cursor group to the initial state of the simulation.
    ///
    /// This encapsulates the logic of rewinding streams and re-aligning
    /// the timestamp to the global start.
    pub fn reset(&mut self, sim_data: &SimulationData) {
        self.rewind();
        let start_ts = sim_data.global_availability_start();
        self.advance_all(sim_data, start_ts);
        self.current_ts = start_ts;
        self.previous_ts = None;
    }

    /// Checks if all data streams in the simulation have been fully consumed.
    ///
    /// # Returns
    ///
    /// `true` if every cursor for every data type has reached the end of its
    /// corresponding event array in `SimulationData`.
    /// `false` if there is any data left to be processed in any stream.
    pub fn is_end_of_data(&self, sim_data: &SimulationData) -> bool {
        self.ohlcv.is_done(sim_data.ohlcv())
            && self.trade.is_done(sim_data.trade())
            && self.economic_cal.is_done(sim_data.economic_cal())
            && self.vp.is_done(sim_data.volume_profile())
            && self.tpo.is_done(sim_data.tpo())
            && self.ema.is_done(sim_data.ema())
            && self.sma.is_done(sim_data.sma())
            && self.rsi.is_done(sim_data.rsi())
    }
}

impl CursorGroup {
    fn advance_all_to_end(&mut self, sim_data: &SimulationData) {
        self.ohlcv.to_end(sim_data.ohlcv());
        self.trade.to_end(sim_data.trade());
        self.economic_cal.to_end(sim_data.economic_cal());
        self.vp.to_end(sim_data.volume_profile());
        self.tpo.to_end(sim_data.tpo());
        self.ema.to_end(sim_data.ema());
        self.sma.to_end(sim_data.sma());
        self.rsi.to_end(sim_data.rsi());
    }

    fn rewind(&mut self) {
        self.ohlcv.rewind();
        self.trade.rewind();
        self.economic_cal.rewind();
        self.vp.rewind();
        self.tpo.rewind();
        self.ema.rewind();
        self.sma.rewind();
        self.rsi.rewind();
    }

    fn advance_all(&mut self, sim_data: &SimulationData, ts: DateTime<Utc>) {
        self.ohlcv.advance(sim_data.ohlcv(), ts);
        self.trade.advance(sim_data.trade(), ts);
        self.economic_cal.advance(sim_data.economic_cal(), ts);
        self.vp.advance(sim_data.volume_profile(), ts);
        self.tpo.advance(sim_data.tpo(), ts);
        self.ema.advance(sim_data.ema(), ts);
        self.sma.advance(sim_data.sma(), ts);
        self.rsi.advance(sim_data.rsi(), ts);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        data::{
            domain::{DataBroker, Exchange, Period, Price, Quantity, SpotPair, Symbol},
            episode::{EpisodeBuilder, EpisodeLength},
            event::{Ohlcv, OhlcvId, Trade, TradesId},
        },
        gym::trading::config::EnvConfig,
        sim::data::SimulationDataBuilder,
        sorted_vec_map::SortedVecMap,
    };

    // ============================================================================
    // Test Helpers
    // ============================================================================

    /// Parse RFC3339 timestamp string to DateTime<Utc>.
    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    /// Create an OHLCV event with specified open and close timestamps.
    /// The `point_in_time()` is determined by `close_timestamp` per the MarketEvent trait.
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

    /// Create an OHLCV ID for testing with configurable period.
    fn ohlcv_id(period: Period) -> OhlcvId {
        OhlcvId {
            broker: DataBroker::Binance,
            exchange: Exchange::Binance,
            symbol: Symbol::Spot(SpotPair::BtcUsdt),
            period,
        }
    }

    /// Create a Trade event at the specified timestamp.
    /// For trades, `point_in_time()` returns `timestamp` per the MarketEvent trait.
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

    /// Create a second TradeId for multi-stream tests (ETH instead of BTC).
    fn trade_id_alt() -> TradesId {
        TradesId {
            broker: DataBroker::Binance,
            exchange: Exchange::Binance,
            symbol: Symbol::Spot(SpotPair::EthUsdt),
        }
    }

    /// Build SimulationData with OHLCV data only.
    fn sim_data_with_ohlcv(id: OhlcvId, events: Vec<Ohlcv>) -> SimulationData {
        let mut ohlcv_map = SortedVecMap::new();
        ohlcv_map.insert(id, events.into_boxed_slice());

        SimulationDataBuilder::new()
            .with_ohlcv(ohlcv_map)
            .build(EnvConfig::default())
            .unwrap()
    }

    /// Build SimulationData with both OHLCV and Trade data for multi-stream tests.
    fn sim_data_multi_stream(
        oid: OhlcvId,
        ohlcv_events: Vec<Ohlcv>,
        tid: TradesId,
        trade_events: Vec<Trade>,
    ) -> SimulationData {
        let mut ohlcv_map = SortedVecMap::new();
        ohlcv_map.insert(oid, ohlcv_events.into_boxed_slice());

        let mut trade_map = SortedVecMap::new();
        trade_map.insert(tid, trade_events.into_boxed_slice());

        SimulationDataBuilder::new()
            .with_ohlcv(ohlcv_map)
            .with_trade(trade_map)
            .build(EnvConfig::default())
            .unwrap()
    }

    /// Build an Episode using EpisodeBuilder.
    fn episode(start: DateTime<Utc>, length: EpisodeLength) -> Episode {
        EpisodeBuilder::new()
            .with_start(start)
            .with_length(length)
            .build()
            .unwrap()
    }

    // ============================================================================
    // Part 3: CursorGroup Tests (The Container)
    // Focus: Synchronization between multiple cursors
    // Note: Individual cursor mechanics (simultaneous events, forward-only, etc.)
    //       are tested in cursor.rs. Here we only test GROUP coordination.
    // ============================================================================

    #[test]
    fn test_cursor_group_synchronization_ohlcv_and_trade() {
        // THE CRITICAL TEST: Verify that when the group advances, BOTH cursors update correctly.
        // This tests the synchronization guarantee of CursorGroup.
        //
        // Scenario:
        // - OHLCV 5m candles: available at 00:05, 00:10
        // - Trades: available at 00:02, 00:07
        //
        // The group should step through: 00:02 -> 00:05 -> 00:07 -> 00:10
        // And BOTH cursors should update correctly at each step.

        let oid = ohlcv_id(Period::Minute(5));
        let tid = trade_id();

        // 5m candles: open at 00:00/00:05, close (available) at 00:05/00:10
        let ohlcv_events = vec![
            ohlcv(ts("2025-03-01T00:00:00Z"), ts("2025-03-01T00:05:00Z")),
            ohlcv(ts("2025-03-01T00:05:00Z"), ts("2025-03-01T00:10:00Z")),
        ];

        // Trades at 00:02 and 00:07
        let trade_events = vec![
            trade(ts("2025-03-01T00:02:00Z")),
            trade(ts("2025-03-01T00:07:00Z")),
        ];

        let sim_data = sim_data_multi_stream(oid, ohlcv_events, tid, trade_events);
        let ep = episode(ts("2025-03-01T00:00:00Z"), EpisodeLength::Day);

        let mut cursor_group = CursorGroup::new(&sim_data).unwrap();

        // INITIAL STATE: First available event is trade at 00:02
        assert_eq!(
            cursor_group.current_ts,
            ts("2025-03-01T00:02:00Z"),
            "Initial: Should start at earliest available event (trade at 00:02)"
        );
        // Trade cursor consumed first event, OHLCV cursor still empty (00:05 > 00:02)
        assert_eq!(cursor_group.trade.0.get(&tid).unwrap(), &(0..1));
        assert_eq!(cursor_group.ohlcv.0.get(&oid).unwrap(), &(0..0));

        // STEP 1: Next event is OHLCV at 00:05
        cursor_group.step(&sim_data, &ep).unwrap();
        assert_eq!(cursor_group.current_ts, ts("2025-03-01T00:05:00Z"));
        // Now OHLCV cursor has consumed first event
        assert_eq!(
            cursor_group.ohlcv.0.get(&oid).unwrap(),
            &(0..1),
            "Step 1: OHLCV cursor should have consumed first candle"
        );
        assert_eq!(
            cursor_group.trade.0.get(&tid).unwrap(),
            &(0..1),
            "Step 1: Trade cursor should remain at 1 (no new trades)"
        );

        // STEP 2: Next event is trade at 00:07
        cursor_group.step(&sim_data, &ep).unwrap();
        assert_eq!(cursor_group.current_ts, ts("2025-03-01T00:07:00Z"));
        assert_eq!(
            cursor_group.trade.0.get(&tid).unwrap(),
            &(0..2),
            "Step 2: Trade cursor should now have consumed both trades"
        );
        assert_eq!(
            cursor_group.ohlcv.0.get(&oid).unwrap(),
            &(0..1),
            "Step 2: OHLCV cursor should remain at 1"
        );

        // STEP 3: Next event is OHLCV at 00:10
        cursor_group.step(&sim_data, &ep).unwrap();
        assert_eq!(cursor_group.current_ts, ts("2025-03-01T00:10:00Z"));
        assert_eq!(
            cursor_group.ohlcv.0.get(&oid).unwrap(),
            &(0..2),
            "Step 3: OHLCV cursor should have consumed both candles"
        );
        assert_eq!(
            cursor_group.trade.0.get(&tid).unwrap(),
            &(0..2),
            "Step 3: Trade cursor should remain at 2"
        );

        // Both streams exhausted
        assert!(cursor_group.is_end_of_data(&sim_data));
    }

    #[test]
    fn test_cursor_group_initialization_advances_to_first_available() {
        // CursorGroup::new() should advance all cursors to the first available timestamp.
        let oid = ohlcv_id(Period::Minute(3));

        // First candle available at 00:03
        let events = vec![
            ohlcv(ts("2025-04-01T00:00:00Z"), ts("2025-04-01T00:03:00Z")),
            ohlcv(ts("2025-04-01T00:03:00Z"), ts("2025-04-01T00:06:00Z")),
        ];

        let sim_data = sim_data_with_ohlcv(oid, events);
        let cursor_group = CursorGroup::new(&sim_data).unwrap();

        // Should start at first available event (00:03:00)
        assert_eq!(
            cursor_group.current_ts,
            ts("2025-04-01T00:03:00Z"),
            "Should initialize at first available event"
        );
        assert_eq!(
            cursor_group.previous_ts, None,
            "previous_ts should be None on init"
        );

        // First event should be consumed
        assert_eq!(
            cursor_group.ohlcv.0.get(&oid).unwrap(),
            &(0..1),
            "First event should be consumed on initialization"
        );
    }

    #[test]
    fn test_cursor_group_rewind_resets_all_cursors() {
        // Verify that rewind() resets ALL cursor types to zero, not just one.
        let oid = ohlcv_id(Period::Minute(3));
        let tid = trade_id();

        let ohlcv_events = vec![
            ohlcv(ts("2025-05-01T00:00:00Z"), ts("2025-05-01T00:03:00Z")),
            ohlcv(ts("2025-05-01T00:03:00Z"), ts("2025-05-01T00:06:00Z")),
        ];
        let trade_events = vec![
            trade(ts("2025-05-01T00:02:00Z")),
            trade(ts("2025-05-01T00:04:00Z")),
        ];

        let sim_data = sim_data_multi_stream(oid, ohlcv_events, tid, trade_events);
        let ep = episode(ts("2025-05-01T00:00:00Z"), EpisodeLength::Day);

        let mut cursor_group = CursorGroup::new(&sim_data).unwrap();

        // Advance past some events
        cursor_group.step(&sim_data, &ep).unwrap();
        cursor_group.step(&sim_data, &ep).unwrap();

        // Verify cursors have advanced
        assert!(cursor_group.ohlcv.0.get(&oid).unwrap().end > 0);
        assert!(cursor_group.trade.0.get(&tid).unwrap().end > 0);

        // Rewind
        cursor_group.rewind();

        // ALL cursors should be reset
        assert_eq!(
            cursor_group.ohlcv.0.get(&oid).unwrap(),
            &(0..0),
            "OHLCV cursor should be reset to 0..0"
        );
        assert_eq!(
            cursor_group.trade.0.get(&tid).unwrap(),
            &(0..0),
            "Trade cursor should be reset to 0..0"
        );
    }

    #[test]
    fn test_cursor_group_is_end_of_data_requires_all_exhausted() {
        // is_end_of_data should return true only when ALL streams are exhausted.
        let oid = ohlcv_id(Period::Minute(3));
        let tid = trade_id();

        // OHLCV: 1 event at 00:03
        // Trade: 1 event at 00:05 (later than OHLCV)
        let ohlcv_events = vec![ohlcv(
            ts("2025-06-01T00:00:00Z"),
            ts("2025-06-01T00:03:00Z"),
        )];
        let trade_events = vec![trade(ts("2025-06-01T00:05:00Z"))];

        let sim_data = sim_data_multi_stream(oid, ohlcv_events, tid, trade_events);
        let ep = episode(ts("2025-06-01T00:00:00Z"), EpisodeLength::Day);

        let mut cursor_group = CursorGroup::new(&sim_data).unwrap();

        // Initial: at 00:03 (OHLCV first)
        assert!(
            !cursor_group.is_end_of_data(&sim_data),
            "Should NOT be at end initially"
        );

        // Step to 00:05 (trade)
        cursor_group.step(&sim_data, &ep).unwrap();
        assert_eq!(cursor_group.current_ts, ts("2025-06-01T00:05:00Z"));

        // Now OHLCV is exhausted but trade was just consumed
        // Both should be exhausted now
        assert!(
            cursor_group.is_end_of_data(&sim_data),
            "Should be at end when ALL streams are exhausted"
        );
    }

    #[test]
    fn test_cursor_group_step_idempotent_at_episode_end() {
        // When at episode boundary, repeated step() calls should be idempotent.
        let oid = ohlcv_id(Period::Minute(3));

        // Events extend past episode boundary
        let events = vec![
            ohlcv(ts("2025-07-01T00:00:00Z"), ts("2025-07-01T00:03:00Z")),
            ohlcv(ts("2025-07-01T00:03:00Z"), ts("2025-07-01T00:06:00Z")),
            // Beyond episode (episode ends at 00:05)
            ohlcv(ts("2025-07-02T00:06:00Z"), ts("2025-07-02T00:09:00Z")),
        ];

        let sim_data = sim_data_with_ohlcv(oid, events);
        // Episode from 00:00 to 00:05 (5 minutes)
        let ep = episode(ts("2025-07-01T00:00:00Z"), EpisodeLength::Day);
        // Note: EpisodeLength::Day is the closest we have to a short test episode
        // Let's use a real Day and just step to the boundary

        let mut cursor_group = CursorGroup::new(&sim_data).unwrap();

        // Step until end of data
        cursor_group.step(&sim_data, &ep).unwrap();
        cursor_group.step(&sim_data, &ep).unwrap();

        let ts_before = cursor_group.current_ts;
        let prev_before = cursor_group.previous_ts;
        let range_before = cursor_group.ohlcv.0.get(&oid).unwrap().clone();

        // Additional steps should be idempotent
        cursor_group.step(&sim_data, &ep).unwrap();
        cursor_group.step(&sim_data, &ep).unwrap();

        assert_eq!(
            cursor_group.current_ts, ts_before,
            "current_ts should not change"
        );
        assert_eq!(
            cursor_group.previous_ts, prev_before,
            "previous_ts should not change"
        );
        assert_eq!(
            cursor_group.ohlcv.0.get(&oid).unwrap(),
            &range_before,
            "cursor range should not change"
        );
    }

    #[test]
    fn test_cursor_group_advance_to_next_episode_resets_previous_ts() {
        // When advancing to a new episode, previous_ts should be reset to None.
        let oid = ohlcv_id(Period::Minute(3));

        let events = vec![
            // Episode 1: 2025-08-01
            ohlcv(ts("2025-08-01T00:00:00Z"), ts("2025-08-01T00:03:00Z")),
            ohlcv(ts("2025-08-01T00:03:00Z"), ts("2025-08-01T00:06:00Z")),
            // Episode 2: 2025-08-02
            ohlcv(ts("2025-08-02T00:00:00Z"), ts("2025-08-02T00:03:00Z")),
        ];

        let sim_data = sim_data_with_ohlcv(oid, events);
        let ep1 = episode(ts("2025-08-01T00:00:00Z"), EpisodeLength::Day);

        let mut cursor_group = CursorGroup::new(&sim_data).unwrap();

        // Step in episode 1 to set previous_ts
        cursor_group.step(&sim_data, &ep1).unwrap();
        assert!(
            cursor_group.previous_ts.is_some(),
            "previous_ts should be set after step"
        );

        // Advance to next episode
        let ep2 = cursor_group
            .advance_to_next_episode(&sim_data, ep1)
            .unwrap();
        assert!(ep2.is_some());

        assert_eq!(
            cursor_group.previous_ts, None,
            "previous_ts should be reset to None for new episode"
        );
        assert_eq!(
            cursor_group.current_ts,
            ts("2025-08-02T00:03:00Z"),
            "current_ts should be at first event of new episode"
        );
    }

    #[test]
    fn test_cursor_group_advance_to_next_episode_returns_none_when_no_more_data() {
        let oid = ohlcv_id(Period::Minute(3));

        // Only one event, one episode
        let events = vec![ohlcv(
            ts("2025-09-01T00:00:00Z"),
            ts("2025-09-01T00:03:00Z"),
        )];

        let sim_data = sim_data_with_ohlcv(oid, events);
        let ep = episode(ts("2025-09-01T00:00:00Z"), EpisodeLength::Day);

        let mut cursor_group = CursorGroup::new(&sim_data).unwrap();

        // Try to advance to next episode when no more data exists
        let next_ep = cursor_group.advance_to_next_episode(&sim_data, ep).unwrap();

        assert!(
            next_ep.is_none(),
            "Should return None when no more data exists"
        );
        assert!(
            cursor_group.is_end_of_data(&sim_data),
            "All cursors should be exhausted"
        );
    }

    #[test]
    fn test_cursor_group_handles_sparse_data_across_episodes() {
        // Test that advance_to_next_episode correctly handles large gaps in data.
        let oid = ohlcv_id(Period::Minute(3));

        let events = vec![
            // Episode 1: 2025-10-01
            ohlcv(ts("2025-10-01T00:00:00Z"), ts("2025-10-01T00:03:00Z")),
            // Large gap - next data on 2025-10-15
            ohlcv(ts("2025-10-15T10:00:00Z"), ts("2025-10-15T10:03:00Z")),
        ];

        let sim_data = sim_data_with_ohlcv(oid, events);
        let ep1 = episode(ts("2025-10-01T00:00:00Z"), EpisodeLength::Day);

        let mut cursor_group = CursorGroup::new(&sim_data).unwrap();

        // Advance to next episode (should skip 14 days of missing data)
        let ep2 = cursor_group
            .advance_to_next_episode(&sim_data, ep1)
            .unwrap();

        assert!(ep2.is_some());
        let ep2 = ep2.unwrap();

        // Episode should start where data actually exists
        assert_eq!(
            ep2.start(),
            ts("2025-10-15T10:00:00Z"),
            "Episode should start at actual data location"
        );
        assert_eq!(
            cursor_group.current_ts,
            ts("2025-10-15T10:03:00Z"),
            "current_ts should be at first available event"
        );
    }

    #[test]
    fn test_cursor_group_step_tracks_time_correctly() {
        // Verify that previous_ts and current_ts are tracked correctly through steps.
        let oid = ohlcv_id(Period::Minute(3));

        let events = vec![
            ohlcv(ts("2025-11-01T00:00:00Z"), ts("2025-11-01T00:03:00Z")),
            ohlcv(ts("2025-11-01T00:03:00Z"), ts("2025-11-01T00:06:00Z")),
            ohlcv(ts("2025-11-01T00:06:00Z"), ts("2025-11-01T00:09:00Z")),
        ];

        let sim_data = sim_data_with_ohlcv(oid, events);
        let ep = episode(ts("2025-11-01T00:00:00Z"), EpisodeLength::Day);

        let mut cursor_group = CursorGroup::new(&sim_data).unwrap();

        // Initial state
        assert_eq!(cursor_group.current_ts, ts("2025-11-01T00:03:00Z"));
        assert_eq!(cursor_group.previous_ts, None);

        // Step 1
        cursor_group.step(&sim_data, &ep).unwrap();
        assert_eq!(cursor_group.current_ts, ts("2025-11-01T00:06:00Z"));
        assert_eq!(cursor_group.previous_ts, Some(ts("2025-11-01T00:03:00Z")));

        // Step 2
        cursor_group.step(&sim_data, &ep).unwrap();
        assert_eq!(cursor_group.current_ts, ts("2025-11-01T00:09:00Z"));
        assert_eq!(cursor_group.previous_ts, Some(ts("2025-11-01T00:06:00Z")));
    }

    #[test]
    fn test_cursor_group_simultaneous_events_across_different_streams() {
        // When OHLCV and Trade have events at the EXACT same timestamp,
        // both should be consumed in a single advance.
        // NOTE: We don't re-test the internal index math (that's cursor.rs's job).
        // We only verify both cursor types update correctly.

        let oid = ohlcv_id(Period::Minute(5));
        let tid = trade_id();

        // OHLCV at 00:05, Trade also at 00:05
        let ohlcv_events = vec![ohlcv(
            ts("2025-12-01T00:00:00Z"),
            ts("2025-12-01T00:05:00Z"),
        )];
        let trade_events = vec![trade(ts("2025-12-01T00:05:00Z"))];

        let sim_data = sim_data_multi_stream(oid, ohlcv_events, tid, trade_events);
        let cursor_group = CursorGroup::new(&sim_data).unwrap();

        // Both events at exactly 00:05 should be consumed on initialization
        assert_eq!(
            cursor_group.current_ts,
            ts("2025-12-01T00:05:00Z"),
            "Should be at the simultaneous timestamp"
        );
        assert_eq!(
            cursor_group.ohlcv.0.get(&oid).unwrap(),
            &(0..1),
            "OHLCV should have consumed its event"
        );
        assert_eq!(
            cursor_group.trade.0.get(&tid).unwrap(),
            &(0..1),
            "Trade should have consumed its event"
        );

        // Both streams should be exhausted
        assert!(cursor_group.is_end_of_data(&sim_data));
    }

    #[test]
    fn test_cursor_group_integration_full_workflow() {
        // Integration test: Complete workflow through multiple episodes with multi-stream data.
        let oid = ohlcv_id(Period::Minute(5));
        let tid = trade_id();

        let ohlcv_events = vec![
            // Episode 1
            ohlcv(ts("2026-01-05T00:00:00Z"), ts("2026-01-05T00:05:00Z")),
            ohlcv(ts("2026-01-05T00:05:00Z"), ts("2026-01-05T00:10:00Z")),
            // Episode 2
            ohlcv(ts("2026-01-06T00:00:00Z"), ts("2026-01-06T00:05:00Z")),
        ];
        let trade_events = vec![
            // Episode 1
            trade(ts("2026-01-05T00:03:00Z")),
            trade(ts("2026-01-05T00:08:00Z")),
            // Episode 2
            trade(ts("2026-01-06T00:02:00Z")),
        ];

        let sim_data = sim_data_multi_stream(oid, ohlcv_events, tid, trade_events);
        let ep1 = episode(ts("2026-01-05T00:00:00Z"), EpisodeLength::Day);

        let mut cursor_group = CursorGroup::new(&sim_data).unwrap();

        // Episode 1: Initial (trade at 00:03)
        assert_eq!(cursor_group.current_ts, ts("2026-01-05T00:03:00Z"));

        // Step through Episode 1
        cursor_group.step(&sim_data, &ep1).unwrap(); // OHLCV 00:05
        assert_eq!(cursor_group.current_ts, ts("2026-01-05T00:05:00Z"));

        cursor_group.step(&sim_data, &ep1).unwrap(); // Trade 00:08
        assert_eq!(cursor_group.current_ts, ts("2026-01-05T00:08:00Z"));

        cursor_group.step(&sim_data, &ep1).unwrap(); // OHLCV 00:10
        assert_eq!(cursor_group.current_ts, ts("2026-01-05T00:10:00Z"));

        // Advance to Episode 2
        let ep2 = cursor_group
            .advance_to_next_episode(&sim_data, ep1)
            .unwrap();
        assert!(ep2.is_some());
        let ep2 = ep2.unwrap();

        // Episode 2: Should start at trade 00:02 (earliest)
        assert_eq!(cursor_group.current_ts, ts("2026-01-06T00:02:00Z"));
        assert_eq!(cursor_group.previous_ts, None);

        // Step to OHLCV 00:05
        cursor_group.step(&sim_data, &ep2).unwrap();
        assert_eq!(cursor_group.current_ts, ts("2026-01-06T00:05:00Z"));
        assert_eq!(cursor_group.previous_ts, Some(ts("2026-01-06T00:02:00Z")));

        // No more episodes
        let ep3 = cursor_group
            .advance_to_next_episode(&sim_data, ep2)
            .unwrap();
        assert!(ep3.is_none());
        assert!(cursor_group.is_end_of_data(&sim_data));
    }

    #[test]
    fn test_cursor_group_multi_symbol_same_type() {
        // Scenario: BTC Trades and ETH Trades occurring interleaved.
        let btc_id = trade_id();
        let eth_id = trade_id_alt(); // The previously unused helper

        let btc_events = vec![trade(ts("2025-01-01T10:00:00Z"))];
        let eth_events = vec![trade(ts("2025-01-01T10:01:00Z"))];

        // Manually build sim data with two trade streams
        let mut trade_map = SortedVecMap::new();
        trade_map.insert(btc_id, btc_events.into_boxed_slice());
        trade_map.insert(eth_id, eth_events.into_boxed_slice());

        let mut ohlcv_map = SortedVecMap::new(); // Empty OHLCV for this test

        let dummy_ohlcv = ohlcv_id(Period::Minute(1));
        ohlcv_map.insert(dummy_ohlcv, vec![].into_boxed_slice());

        let sim_data = SimulationDataBuilder::new()
            .with_ohlcv(ohlcv_map)
            .with_trade(trade_map)
            .build(EnvConfig::default())
            .unwrap();

        let ep = episode(ts("2025-01-01T00:00:00Z"), EpisodeLength::Day);
        let mut cursor_group = CursorGroup::new(&sim_data).unwrap();

        // 1. Init -> 10:00 (BTC)
        assert_eq!(cursor_group.current_ts, ts("2025-01-01T10:00:00Z"));
        assert_eq!(cursor_group.trade.0.get(&btc_id).unwrap(), &(0..1));
        assert_eq!(cursor_group.trade.0.get(&eth_id).unwrap(), &(0..0));

        // 2. Step -> 10:01 (ETH)
        cursor_group.step(&sim_data, &ep).unwrap();
        assert_eq!(cursor_group.current_ts, ts("2025-01-01T10:01:00Z"));

        // Both consumed
        assert_eq!(cursor_group.trade.0.get(&btc_id).unwrap(), &(0..1));
        assert_eq!(cursor_group.trade.0.get(&eth_id).unwrap(), &(0..1));
    }
}
