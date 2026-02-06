use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use chrono::{DateTime, Utc};

use crate::data::domain::Price;
use crate::data::view::MarketView;
use crate::{
    data::episode::Episode,
    error::ChapatyResult,
    gym::flow::{
        domain::RfqId,
        state::{Open, Rfq},
    },
    sim::{cursor_group::CursorGroup, data::SimulationData},
};

// ============================================================================
// 1. The Scheduler (Master Clock)
// ============================================================================

#[derive(Clone, Debug)]
pub struct Scheduler {
    /// Der Cursor, der über die fixen Marktdaten iteriert.
    cursor: CursorGroup,

    /// Zeit-State (für Deltas oder Logging)
    previous_ts: Option<DateTime<Utc>>,
    current_ts: DateTime<Utc>,
}

/// Das Ergebnis eines einzelnen Simulations-Schritts.
/// Das Env muss wissen, WAS passiert ist, um richtig zu reagieren.
pub enum SchedulerOutcome {
    /// Ein Marktdaten-Update hat stattgefunden (Zeit ist fortgeschritten).
    /// Env sollte jetzt prüfen, ob der Generator ein neues RFQ erzeugt.
    MarketTick { timestamp: DateTime<Utc> },

    /// Ein synthetisches Event aus der Queue wurde ausgelöst.
    /// Env muss Ledger/States updaten.
    SyntheticEvent(ScheduledEvent),

    /// Keine Daten mehr (Episode zu Ende).
    Done,
}

impl Scheduler {
    pub fn new(cursor: CursorGroup) -> Self {
        let ts = cursor.current_ts();
        Self {
            cursor,
            previous_ts: None,
            current_ts: ts,
        }
    }
    
    pub fn current_ts(&self) -> DateTime<Utc> {
        self.current_ts
    }

    /// Der Herzschlag der Simulation.
    /// Entscheidet deterministisch, ob Marktdaten oder Queue-Events Vorrang haben.
    pub fn step(
        &mut self,
        sim_data: &SimulationData,
        ep: &Episode,
        queue: &mut BinaryHeap<Reverse<ScheduledEvent>>,
    ) -> ChapatyResult<SchedulerOutcome> {
        // 1. Wann ist der Markt bereit? (Peek in Cursor)
        let t_market = self.cursor.peek(sim_data);

        // 2. Wann ist die Queue bereit? (Peek in Heap)
        let t_queue = queue.peek().map(|e| e.timestamp);

        // 3. Entscheidung (Wer ist früher?)
        match (t_market, t_queue) {
            (Some(tm), Some(tq)) => {
                if tq <= tm {
                    // FALL A: Queue ist früher (oder gleich -> Queue priority für Kausalität)
                    // Wir konsumieren das Event aus der Queue.
                    let event = queue.pop().expect("Peeked item must exist");
                    self.update_time(event.timestamp);
                    Ok(SchedulerOutcome::SyntheticEvent(event))
                } else {
                    // FALL B: Markt ist früher
                    // Wir bewegen den Cursor vorwärts.
                    self.cursor.step(sim_data, ep)?;
                    self.update_time(tm);
                    Ok(SchedulerOutcome::MarketTick { timestamp: tm })
                }
            }
            (Some(tm), None) => {
                // Nur Markt da, Queue leer
                self.cursor.step(sim_data, ep)?;
                self.update_time(tm);
                Ok(SchedulerOutcome::MarketTick { timestamp: tm })
            }
            (None, Some(_)) => {
                // Markt ist leer, aber noch Events in der Queue (z.B. Settlement)
                let event = queue.pop().expect("Peeked item must exist");
                self.update_time(event.timestamp);
                Ok(SchedulerOutcome::SyntheticEvent(event))
            }
            (None, None) => {
                // Alles vorbei
                Ok(SchedulerOutcome::Done)
            }
        }
    }

    fn update_time(&mut self, new_ts: DateTime<Utc>) {
        // Simple Monotonicity Check (Debug Safety)
        if new_ts < self.current_ts {
            // Warnung: In einer DES darf die Zeit nie rückwärts laufen!
            // tracing::warn!("Time Moved Backwards! {:?} -> {:?}", self.current_ts, new_ts);
        }
        self.previous_ts = Some(self.current_ts);
        self.current_ts = new_ts;
    }
    
    // Delegate getter for Observation
    pub fn market_view<'a>(&'a self, data: &'a SimulationData) -> MarketView<'a> {
        MarketView::new(data, &self.cursor).unwrap() // Should not fail if initialized
    }
}

// ============================================================================
// 2. Events & Payloads
// ============================================================================

#[derive(Debug, Clone)]
pub enum RfqEvent {
    /// Generator hat zugeschlagen: Ein neuer Kunde ist da.
    NewRequest(Rfq<Open>),

    /// Der Kunde hat geantwortet (Probabilistic Model Decision).
    CustomerReply {
        rfq_id: RfqId,
        decision: CustomerDecision,
    },

    /// Zeit abgelaufen.
    Expired { rfq_id: RfqId },
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CustomerDecision {
    Accept,
    Reject,
    Counter(Price),
}

#[derive(Clone, Debug)]
pub struct ScheduledEvent {
    pub timestamp: DateTime<Utc>,
    pub event_type: RfqEvent,
}
impl PartialEq for ScheduledEvent {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp == other.timestamp
    }
}

impl Eq for ScheduledEvent {}

impl PartialOrd for ScheduledEvent {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScheduledEvent {
    fn cmp(&self, other: &Self) -> Ordering {
        self.timestamp.cmp(&other.timestamp)
    }
}