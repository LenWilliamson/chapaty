use std::sync::Arc;

use crate::{
    data::domain::Price, flow::state::Expirable, gym::flow::state::{Finalized, Open, Quoted, Rfq, RfqOutcome}
};


// ============================================================================
// Implementation: Open State (Initial Request)
// ============================================================================
impl Rfq<Open> {
    /// Der Agent sendet ein Quote -> Übergang zu "Quoted".
    /// Beachte: Wir konsumieren "self" (move), der alte Zustand existiert nicht mehr.
    pub fn quote(self, price: Price) -> Rfq<Quoted> {
        // Header muss modifiziert werden (Revision + 1), daher Deep Clone nötig
        let mut new_header = self.header.as_ref().clone();
        new_header.revision_id += 1;

        Rfq {
            header: Arc::new(new_header),
            state: Quoted {
                responder_id: Default::default(), // TODO dummy value needs to be replaced (Agent ID missing in args)
                my_quote: price,
            },
        }
    }

    /// Der Agent ignoriert die Anfrage -> Expired.
    pub fn ignore(self) -> Rfq<Finalized> {
        self.finalize(RfqOutcome::Expired)
    }

    // Hilfsfunktion für den Abschluss (Internal Helper)
    fn finalize(self, outcome: RfqOutcome) -> Rfq<Finalized> {
        Rfq {
            header: self.header, // Header bleibt unverändert
            state: Finalized { outcome },
        }
    }
}

impl Expirable for Rfq<Open> {
    fn expire(self) -> Rfq<Finalized> {
        self.finalize(RfqOutcome::Expired)
    }
}