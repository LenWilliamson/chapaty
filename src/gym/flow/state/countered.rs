use std::sync::Arc;

use crate::{
    data::domain::Price,
    flow::state::Expirable,
    gym::flow::state::{Countered, Finalized, Quoted, Rfq, RfqOutcome},
};

// ============================================================================
// Implementation: Countered State (Negotiation)
// ============================================================================
impl Rfq<Countered> {
    /// **Action:** Accept the client's counter-offer.
    ///
    /// The trade is executed at the CLIENT's proposed price.
    pub fn accept_counter(self) -> Rfq<Finalized> {
        let price = self.state.client_price;
        let quantity = self.header.quantity;

        Rfq {
            header: self.header, // Header bleibt gleich (keine Änderung der Metadaten)
            state: Finalized {
                outcome: RfqOutcome::Filled { price, quantity },
            },
        }
    }

    /// **Action:** Reject the client's counter-offer.
    ///
    /// The negotiation ends immediately with no trade.
    pub fn reject_counter(self) -> Rfq<Finalized> {
        Rfq {
            header: self.header, // Header metadata (ID, Symbol) remains unchanged
            state: Finalized {
                outcome: RfqOutcome::Rejected, // Explicitly marked as Rejected
            },
        }
    }

    /// Agent schlägt wieder einen neuen Preis vor (Loop zurück zu Quoted!).
    /// Hier erhöht sich die Revision, da wir antworten.
    pub fn requote(self, new_price: Price) -> Rfq<Quoted> {
        // Header muss modifiziert werden (Revision + 1), daher Deep Clone nötig
        let mut new_header = self.header.as_ref().clone();
        new_header.revision_id += 1;

        Rfq {
            header: Arc::new(new_header),
            state: Quoted {
                responder_id: Default::default(), // TODO dummy value needs to be replaced (Agent ID missing in args)
                my_quote: new_price,
            },
        }
    }
}

impl Expirable for Rfq<Countered> {
    fn expire(self) -> Rfq<Finalized> {
        Rfq {
            header: self.header,
            state: Finalized {
                outcome: RfqOutcome::Expired,
            },
        }
    }
}
