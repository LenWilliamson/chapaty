use std::sync::Arc;

use crate::{
    agent::AgentIdentifier,
    data::domain::{Price, Quantity, Symbol},
    flow::{domain::{RfqId, Side}, state::Expirable},
    gym::flow::state::{Countered, Finalized, Quoted, Rfq, RfqOutcome},
};

// ============================================================================
// Implementation: Quoted State (Waiting for Client)
// ============================================================================
impl Rfq<Quoted> {
    /// Die Simulation entscheidet: Der Kunde akzeptiert.
    pub fn customer_accepts(self) -> Rfq<Finalized> {
        let price = self.state.my_quote;
        let quantity = self.header.quantity;

        self.finalize(RfqOutcome::Filled { price, quantity })
    }

    /// Die Simulation entscheidet: Der Kunde lehnt ab.
    pub fn customer_rejects(self) -> Rfq<Finalized> {
        self.finalize(RfqOutcome::Rejected)
    }

    /// Die Simulation entscheidet: Der Kunde verhandelt (Counter Offer).
    pub fn customer_counters(self, counter_price: Price) -> Rfq<Countered> {
        // Header muss modifiziert werden (Revision + 1), da der Kunde antwortet
        let mut new_header = self.header.as_ref().clone();
        new_header.revision_id += 1;

        Rfq {
            header: Arc::new(new_header),
            state: Countered {
                client_price: counter_price,
            },
        }
    }

    fn finalize(self, outcome: RfqOutcome) -> Rfq<Finalized> {
        Rfq {
            header: self.header,
            state: Finalized { outcome },
        }
    }
}

impl Expirable for Rfq<Quoted> {
    fn expire(self) -> Rfq<Finalized> {
        self.finalize(RfqOutcome::Expired)
    }
}

pub struct QuoteDetails {
    pub rfq_id: RfqId,
    pub client_id: AgentIdentifier,
    pub symbol: Symbol,
    pub side: Side,
    pub quantity: Quantity,
    pub price: Price,
}
